/*
 * =====================================================================================
 *
 *       Filename:  aodb.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2013 03:35:31 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 *
 * =====================================================================================
 */

#include <stdint.h>
#include <stdio.h>

#include "aodb_data.h"
#include "aodb_imp.h"
#include "db_mgr.h"

#define  DEF_CONF_DIR   "./conf/"
#define  DEF_CONF_FILE  (PROJECT_NAME ".conf")

// ȫ��������Ϣ
aodb_conf_t g_conf;
// ȫ������
aodb_data_t g_data;

/**
 *
 * ��ӡ������Ϣ
**/
static void print_help()
{
	printf("\t-d:     conf_dir\n");
	printf("\t-f:     conf_file\n");
	printf("\t-h:     show this page\n");
	printf("\t-v:     show version infomation\n");
}

/**
 *
 * ��ӡģ��İ汾��Ϣ
**/
static void print_version()
{
	printf("Project    :%s\n", PROJECT_NAME);
	printf("Version    :%s\n", VERSION);
}

/**
 *
 * ��ȡ�����ļ�,ͬʱ��ʼ��log��Ϣ
 *
 * @param dir ���õ�Ŀ¼
 * @param file ���õ��ļ���
 * @param build ������ʽ: 0��ʾ��ȡ����;����ֵ��ʾ���������ļ�
 *
 * @return
 *       0  : �ɹ�
 *       -1 : ʧ��
**/
static int serv_loadconf(const char *dir, const char *file, int build)
{
	/**!! ��ȡ�����ļ�
	 *  �����ļ�Ӧ����ʹ��Ĭ��ֵ,���׳�����.�������е���������ļ��ж�ȡ
	 *  һЩֵ�ķ�Χ����������.range�ļ���
	 **/

	int ret = 0;

	ub_conf_data_t *conf = ub_conf_init(dir, file, build);
	if (NULL == conf) {
		UB_LOG_FATAL("load conf file [dir:%s file:%s build:%d ] failed!", dir, file, build);
		return -1;
	}

	//��־�����Ϣ
	UB_CONF_GETNSTR(conf, "log_dir", g_conf.log_dir, sizeof(g_conf.log_dir), "��־�ļ�Ŀ¼");
	UB_CONF_GETNSTR(conf, "log_file", g_conf.log_file, sizeof(g_conf.log_file), "��־�ļ���");
	UB_CONF_GETINT(conf, "log_size", &g_conf.log_size, "��־�ع���С(M)");
	UB_CONF_GETINT(conf, "log_level", &g_conf.log_level, "��־����");
	//����־
	if (0==build) {
		ret = ub_log_init(g_conf.log_dir, g_conf.log_file, g_conf.log_size, g_conf.log_level);
		if (0 != ret) {
			UB_LOG_FATAL
				("ub_log_init failed![dir:%s file:%s size:%d level:%d ]",
				 g_conf.log_dir, g_conf.log_file, g_conf.log_size, g_conf.log_level);
			goto out;
		}
	}
	//����������Ϣ
	UB_CONF_GETSVR(conf, "metis", "aodb", &g_conf.aodb, "����aodb����");

	UB_CONF_GETNSTR(conf, "data_dir", g_conf.data_dir, sizeof(g_conf.data_dir), "�����ļ�Ŀ¼");
	UB_CONF_GETUINT(conf, "reqbuf_size", &g_conf.reqbuf_size, "����buffer�ĳ�");
	UB_CONF_GETUINT(conf, "resbuf_size", &g_conf.resbuf_size, "Ӧ��buffer�ĳ�");

	UB_CONF_GETNSTR(conf, "db_path", g_conf.db_path, sizeof(g_conf.db_path), "db��������Ŀ¼");
	UB_CONF_GETUINT(conf, "max_open_table", &g_conf.max_open_table, "����ܴ򿪵ı������");
	UB_CONF_GETUINT(conf, "devide_table_period", &g_conf.devide_table_period, "�ֱ�����");
    ret = 0;
out:
	if (conf) {
		ub_conf_close(conf);
		conf = NULL;
	}
	return ret;
}

/**
 *
 * ������ĺϷ��Լ��(�����.range�ļ������˼��,���������� do nothing)
 *
 * @return
 *       0  : �ɹ�
 *       -1 : ʧ��
 **/
static int serv_validateconf()
{
	return 0;
}

/**
 *
 * ����load�����ļ��Ƿ�ɹ�
 *
 * @param dir �����ļ���Ŀ¼
 * @param file �����ļ���
 *
 * @return
 *       0  : �ɹ�
 *       -1 : ʧ��
 **/
static int serv_checkconf(const char *dir, const char *file)
{
	int ret = 0;

	ret = serv_loadconf(dir, file, 0);
	if (ret < 0) {
        UB_LOG_FATAL("serv_loadconf failed![ret:%d][dir:%s][file:%s]", ret, dir, file);
		return ret;
	}

	ret = serv_validateconf();
    if (ret < 0) {
        UB_LOG_FATAL("serv_validateconf failed![ret:%d][dir:%s][file:%s]", ret, dir, file);
    }
	return ret;
}

/**
 *
 * �����ʼ��
 *
 * @return
 *       0  : �ɹ�
 *       -1 : ʧ��
 **/
static int serv_init()
{
	int ret = 0;

	signal(SIGPIPE, SIG_IGN);

	//�������
	g_data.aodb_svr = ub_server_create();
	if (NULL == g_data.aodb_svr) {
		UB_LOG_FATAL("ub_server_create failed!");
		ret = -1;
		goto out;
	}

	ret = ub_server_load(g_data.aodb_svr, &g_conf.aodb);
	if (0 != ret) {
		UB_LOG_FATAL("load aodb server failed!");
		ret = -1;
		goto out;
	}

	ub_server_set_buffer(g_data.aodb_svr, g_conf.aodb.thread_num, NULL,  
                         g_conf.reqbuf_size, NULL, g_conf.resbuf_size);

	//ʹ�ü����ͻص�
	ub_server_set_callback(g_data.aodb_svr, aodb_cmdproc_callback, NULL);

    ret = 0;
out:
	if (ret < 0) {
		if (g_data.aodb_svr) {
			ub_server_destroy(g_data.aodb_svr);
			g_data.aodb_svr = NULL;
		}
	}
	return ret;
}

static int init_thread_data() 
{
    g_data.thread_data = (thread_data_t **)calloc(g_conf.aodb.thread_num, \
            sizeof(void *));
    assert(g_data.thread_data);
	for (uint32_t i=0; i<g_conf.aodb.thread_num; ++i) {
		g_data.thread_data[i] = (thread_data_t *) calloc(1, sizeof(thread_data_t));
        assert(g_data.thread_data[i]);
    }
	ub_server_set_user_data(g_data.aodb_svr, (void **)g_data.thread_data, sizeof(thread_data_t));
    return 0;
}

//
// ��ʼ�����ݣ�����ȫ������&�߳�˽�����ݡ�
//
static int init_data() 
{
    g_data.need_quit = false;
    int ret = init_thread_data();
    if (0 != ret) {
        UB_LOG_FATAL("init_thread_data failed!");
        return -1;
    }
    UB_LOG_TRACE("init_data success!");
    return 0;
}


//
// ��ʼ������ģ��
//
static int init_module() 
{
    int ret = aodb::DbMgr::instance()->Initialize(g_conf.db_path, g_conf.max_open_table, g_conf.devide_table_period);
    if (ret < 0) {
        UB_LOG_WARNING("DbMgr::Initialize failed![db_path:%s][max_open_table:%u][devide_table_period:%u]", 
                      g_conf.db_path, g_conf.max_open_table, g_conf.devide_table_period);
        return -1;
    }
    UB_LOG_TRACE("init_module success!");
    return 0;
}

/**
 *
 *���з���
 *
 * @return
 *       0  : �ɹ�
 *       -1 : ʧ��
 **/
static int serv_run() 
{
    ub_server_setoptsock(g_data.aodb_svr, UBSVR_NODELAY);
	//aodb����
	int ret = ub_server_run(g_data.aodb_svr);
	if (0 != ret) {
		UB_LOG_FATAL("run aodb server failed!");
		return -1;
	}
	ub_server_join(g_data.aodb_svr);
	return 0;
}

/**
 *
 *�����������
 *
 **/
static void serv_cleanup() 
{
	if (g_data.aodb_svr) {
		ub_server_destroy(g_data.aodb_svr);
		g_data.aodb_svr = NULL;
	}
}

//
// 
//
void quit_signal_handler(int signum) 
{
    fprintf(stderr, "catch quit signal[signum:%d]!\n", signum);
    if (g_data.aodb_svr) {
        ub_server_stop(g_data.aodb_svr);
    }
}

int main(int argc, char *argv[])
{
	int ret = 0;
	int c = 0;

	signal(SIGTERM, quit_signal_handler);
	signal(SIGINT, quit_signal_handler);
	signal(SIGQUIT, quit_signal_handler);

	strlcpy(g_conf.conf_dir, DEF_CONF_DIR, PATH_SIZE);
	strlcpy(g_conf.conf_file, DEF_CONF_FILE, PATH_SIZE);

	while (-1 != (c = getopt(argc, argv, "d:f:gthv"))) {
		switch (c) {
			case 'd':
				snprintf(g_conf.conf_dir, PATH_SIZE, "%s/", optarg);
				break;
			case 'f':
				snprintf(g_conf.conf_file, PATH_SIZE, "%s", optarg);
				break;
			case 'v':
				print_version();
				return 0;
			case 'h':
				print_help();
				return 0;
			default:
				print_help();
				return 0;
		}
	}

	//��ȡ�����ļ�
    ret = serv_loadconf(g_conf.conf_dir, g_conf.conf_file, 0);
    if (ret < 0) {
        UB_LOG_FATAL("load config [dir:%s file:%s] failed!",
                g_conf.conf_dir, g_conf.conf_file);
        goto out;
    }

    // ��ʼ������
	ret = serv_init();
	if (0 != ret) {
		UB_LOG_FATAL("serv_init failed! [ret:%d]", ret);
		goto out;
	} else {
		UB_LOG_FATAL("serv_init success!");
	}

    // ��ʼ������
    ret = init_data();
    if (0 != ret) {
        UB_LOG_FATAL("init_data failed![ret:%d]", ret);
        goto out;
    } else {
        UB_LOG_FATAL("init_data success!");
    }

    // ��ʼ������ģ��
    ret = init_module();
    if (0 != ret) {
        UB_LOG_FATAL("init_module failed![ret:%d]", ret);
        goto out;
    } else {
        UB_LOG_FATAL("init_module success!");
    }

	//����ʼ��!!
	ret = serv_run();
	if (0 != ret) {
		UB_LOG_FATAL("serv_start failed! [ret:%d]", ret);
		goto out;
	} else {
		UB_LOG_FATAL("serv_start success!");
	}

out:
	//�������
	serv_cleanup();
	return ret;
}

