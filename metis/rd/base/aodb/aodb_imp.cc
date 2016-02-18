/*
 * =====================================================================================
 *
 *       Filename:  aodb_imp.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/18/2013 12:10:35 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 *
 * =====================================================================================
 */

#include "aodb_imp.h"
#include "aodb_data.h"
#include "db_imp.h"
#include "mdb.pb.h"

using namespace MdbInterface;

typedef GenericResErr (*callback_t) (GenericReq *req, \
        google::protobuf::Message **resMsg, thread_data_t *pthr);

typedef struct {
    const char *cmd;
    callback_t callback;
}cmd_callback_t;

const static cmd_callback_t cmd_callback[] = {
    {"put", aodb_put},
    {"get", aodb_get},
    {NULL, NULL},
};

static void aodb_reset_res(nshead_t* req_head, nshead_t* res_head)
{
	res_head->id = 0;
	res_head->version = 0;
	res_head->log_id = req_head->log_id;
	strlcpy(res_head->provider, g_conf.aodb.svr_name, sizeof (res_head->provider));
	res_head->magic_num = NSHEAD_MAGICNUM;
	res_head->body_len = 0;
}

static void aodb_reset_threaddata(thread_data_t *pthr) 
{
}

/**
 *
 * aodb的命令处理回调函数
 *
 * @return a int 
 * @return
 *       0  : 成功
 *       -1 : 失败,socket将直接关闭,不给client返回错误信息
 **/
int aodb_cmdproc_callback()
{

	nshead_t* req_head = (nshead_t *) ub_server_get_read_buf();
	nshead_t* res_head = (nshead_t *) ub_server_get_write_buf();

	char* req_buf = (char *) (req_head + 1);
	//size_t req_buf_size = ub_server_get_read_size() - sizeof (nshead_t);
	char* res_buf = (char *) (res_head + 1);
	size_t res_buf_size = ub_server_get_write_size() - sizeof (nshead_t);

	thread_data_t* pthr = (thread_data_t *) ub_server_get_user_data();
	if (NULL == pthr) {
        UB_LOG_WARNING("failed to get thread data!");
        return -1;
	}

    const cmd_callback_t *callback = NULL;
    GenericResErr err;
	int ret = 0;

    aodb_reset_threaddata(pthr);

	ub_log_setbasic(UB_LOG_REQSVR, "%s", req_head->provider);

    // 1, 解析请求
    GenericReq req;
    GenericRes res;
    google::protobuf::Message *res_body = NULL;

    ret = req.ParseFromArray(req_buf, req_head->body_len);
    if (false == ret) {
        res.set_err(ERR_ARG);
        UB_LOG_WARNING("failed to parse request!");
        goto failed;
    }

    ub_log_pushnotice("cmd", "%s", req.cmd().c_str());

    callback = cmd_callback;
    while (callback->cmd) {
        if (0 == strcmp(callback->cmd, req.cmd().c_str()))
            break;
        callback++;
    }

    if (NULL == callback->cmd) {
        res.set_err(ERR_UNSUP);
        UB_LOG_WARNING("unsupport cmd:%s", req.cmd().c_str());
        goto failed;
    }

    err = callback->callback(&req, &res_body, pthr);

    ub_log_pushnotice("cmd_err", "%d", err);
    res.set_err(err);

    if (ERR_OK == err && res_body) {
        // 检查required的字段是否已经赋值
        if (false == res_body->IsInitialized()) {
            UB_LOG_FATAL("IsInitialized failed![cmd:%s]", req.cmd().c_str());
            res.set_err(ERR_INTER);
            goto failed;
        }

        if (res_body->ByteSize() > (ssize_t)res_buf_size) {
            UB_LOG_WARNING("res body exceed res buffer size![cur:%d][max:%ld]",\
                    res_body->ByteSize(), res_buf_size);
            res.set_err(ERR_INTER);
            goto failed;
        }

        // 响应到保存到body中
        ret = res_body->SerializeToArray(res_buf, res_buf_size);
        if (false == ret) {
            UB_LOG_WARNING("SerializeToArray failed!");
            res.set_err(ERR_INTER);
            goto failed;
        }

        res.set_body(res_buf, res_body->ByteSize());
    }

failed:
    // 4, 返回响应
    aodb_reset_res(req_head, res_head);
    if (res.ByteSize() > (ssize_t)res_buf_size) {
        UB_LOG_WARNING("res exceed res buffer size![cur:%d][max:%ld]",
                       res.ByteSize(), res_buf_size);
        res.set_err(ERR_INTER);
        res.set_body(NULL);
    }
    ret = res.SerializeToArray(res_buf, res_buf_size);
    if (false == ret) {
        UB_LOG_WARNING("failed to serialize response!");
        res_head->body_len = 0;
        ret = -1;
    } else {
        res_head->body_len = res.ByteSize();
        ret = 0;
    }
    ub_log_pushnotice("body_len", "%lu", res_head->body_len);
    ub_log_pushnotice("err", "%d", res.err());
    if (res_body) {
        delete res_body;
        res_body = NULL;
    }
	return ret;
}

