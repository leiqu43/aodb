/*
 * =====================================================================================
 *
 *       Filename:  db_imp.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/18/2012 02:48:32 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#include "db_imp.h"
#include <string>

#include "mdb.pb.h"

using namespace std;
using namespace MdbInterface;

//
// 往某个db写入数据。
//
//
GenericResErr aodb_put(GenericReq *req, google::protobuf::Message **res_msg, thread_data_t *pthr) 
{
    assert(req);
    assert(res_msg);
    assert(pthr);
    int ret = -1;

    DbPutRequest request;
    ret = request.ParseFromString(req->body());
    if (false == ret) {
        UB_LOG_WARNING("DbPutRequest::ParseFromString failed![req_body_size:%lu]", req->body().size());
        return ERR_ARG;
    }

    ub_log_pushnotice("keys num", "%lu", request.key_value_list_size());

    if (request.db_name().size()) {
        // 默认db批量写入
        ub_log_pushnotice("default_db", "%s", request.db_name().c_str());
#if 0
        WriteBatch write_batch;

        // 根据请求构造WriteBatch
        for (int i=0; i<request.key_value_list_size(); ++i) {
            const KeyValuePair *pair = &request.key_value_list(i);
            assert(pair);
            if (pair->has_db() && pair->db() != request.db_name()) {
                // 已经指定了db了，并且db名称不是默认的名称
                continue;
            }
            UB_LOG_DEBUG("[key:%s][value_size:%lu]", pair->key().c_str(), pair->value().size());
            write_batch.Put(pair->key(), pair->value());
        }
        ret = aodb::DbHandler::Put(request.db_name(), &write_batch, true);
        if (0 != ret) {
            UB_LOG_WARNING("DbHandler::Put return failed![err:%d]", ret);
            return MDB_ERR_TO_INTERFACE_ERR(ret);
        }
#endif
    }

    for (int i=0; i<request.key_value_list_size(); ++i) {
        const KeyValuePair *pair = &request.key_value_list(i);
        assert(pair);
        if (!pair->has_db()) {
            continue;
        }
#if 0
        WriteBatch write_batch;
        write_batch.Put(pair->key(), pair->value());
        ret = aodb::DbHandler::Put(pair->db(), &write_batch, true);
        if (0 != ret) {
            UB_LOG_WARNING("DbHandler::Put return failed![err:%d]", ret);
            return MDB_ERR_TO_INTERFACE_ERR(ret);
        }
#endif
    }
    return ERR_OK;
}

//
// 从某个db获取数据。
//
//
GenericResErr aodb_get(GenericReq *req, google::protobuf::Message **res_msg, thread_data_t *pthr) 
{
    void *data = NULL;
    size_t length = 0;
    int ret = -1;

    DbGetRequest request;
    ret = request.ParseFromString(req->body());
    if (false == ret) {
        UB_LOG_WARNING("ParseFromString failed!");
        return ERR_ARG;
    }

    static int kMaxDataSize = 1024*2024;

    DbGetResponse *response = new DbGetResponse();
    data = malloc(kMaxDataSize);
    assert(data);

    //
    // 获取默认db的keys
    //
    if (request.db_name().size()) {
        for (int i=0; i<request.key_list_size(); ++i) {
            const char *key = request.key_list(i).c_str();

            length = kMaxDataSize;
#if 0
            ret = aodb::DbHandler::Get(request.db_name(), key, data, &length);
            if (kMDB_ERR_NO_DB == ret) {
                UB_LOG_WARNING("DbHandler::Get failed, because can't find db![db_name:%s][err:%d]", \
                        request.db_name().c_str(), ret);
                goto failed;
            } else if (ret < 0) {
                UB_LOG_WARNING("DbHandler::Get failed![err:%d]", ret);
                goto failed;
            }
#endif

            if (length > 0)  {
                KeyValuePair *key_value = response->add_key_value_list();
                assert(key_value);

                key_value->set_key(key);
                key_value->set_value(data, length);
            } else {
                response->add_failed_list(key);
            }

            UB_LOG_DEBUG("DbHandler::Get success![db_name:%s][key:%s][value_size:%ld]", \
                    request.db_name().c_str(), key, length);
        }
    }

    //
    // 获取指定db的keys
    //
    for (int i=0; i<request.db_key_list_size(); ++i) {
#if 0
        const DbKey &db_key = request.db_key_list(i);
        length = kMaxDataSize;
        ret = aodb::DbHandler::Get(db_key.db(), db_key.key(), data, &length);
        if (kMDB_ERR_NO_DB == ret) {
            UB_LOG_WARNING("DbHandler::Get failed, because can't find db![db_name:%s][err:%d]", \
                    db_key.db().c_str(), ret);
            goto failed;
        } else if (ret < 0) {
            UB_LOG_WARNING("DbHandler::Get failed![err:%d]", ret);
            goto failed;
        }
        if (length > 0)  {
            KeyValuePair *key_value = response->add_key_value_list();
            assert(key_value);
            key_value->set_key(db_key.key());
            key_value->set_value(data, length);
        } else {
            response->add_failed_list(db_key.key());
        }
        UB_LOG_DEBUG("DbHandler::Get success![db:%s][key:%s][value_size:%lu]", \
                db_key.db().c_str(), db_key.key().c_str(), length);
#endif
    }

    assert(data);
    free(data);
    data = NULL;

    *res_msg = response;
    UB_LOG_DEBUG("aodb_get succes!");
    return ERR_OK;

    if (data) {
        free(data);
        data = NULL;
    }

//    return MDB_ERR_TO_INTERFACE_ERR(ret);
    return ERR_OK;
}

