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

// 全局配置信息
aodb_conf_t g_conf;
// 全局数据
aodb_data_t g_data;

/**
 *
 * 打印帮助信息
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
 * 打印模块的版本信息
**/
static void print_version()
{
	printf("Project    :%s\n", PROJECT_NAME);
	printf("Version    :%s\n", VERSION);
}

/**
 *
 * 读取配置文件,同时初始化log信息
 *
 * @param dir 配置的目录
 * @param file 配置的文件名
 * @param build 工作方式: 0表示读取配置;其它值表示生成配置文件
 *
 * @return
 *       0  : 成功
 *       -1 : 失败
**/
static int serv_loadconf(const char *dir, const char *file, int build)
{
	/**!! 读取配置文件
	 *  配置文件应避免使用默认值,容易出问题.建议所有的配置项都从文件中读取
	 *  一些值的范围建议配置在.range文件中
	 **/

	int ret = 0;

	ub_conf_data_t *conf = ub_conf_init(dir, file, build);
	if (NULL == conf) {
		UB_LOG_FATAL("load conf file [dir:%s file:%s build:%d ] failed!", dir, file, build);
		return -1;
	}

	//日志相关信息
	UB_CONF_GETNSTR(conf, "log_dir", g_conf.log_dir, sizeof(g_conf.log_dir), "日志文件目录");
	UB_CONF_GETNSTR(conf, "log_file", g_conf.log_file, sizeof(g_conf.log_file), "日志文件名");
	UB_CONF_GETINT(conf, "log_size", &g_conf.log_size, "日志回滚大小(M)");
	UB_CONF_GETINT(conf, "log_level", &g_conf.log_level, "日志级别");
	//打开日志
	if (0==build) {
		ret = ub_log_init(g_conf.log_dir, g_conf.log_file, g_conf.log_size, g_conf.log_level);
		if (0 != ret) {
			UB_LOG_FATAL
				("ub_log_init failed![dir:%s file:%s size:%d level:%d ]",
				 g_conf.log_dir, g_conf.log_file, g_conf.log_size, g_conf.log_level);
			goto out;
		}
	}
	//其它配置信息
	UB_CONF_GETSVR(conf, "metis", "aodb", &g_conf.aodb, "正向aodb服务");

	UB_CONF_GETNSTR(conf, "data_dir", g_conf.data_dir, sizeof(g_conf.data_dir), "数据文件目录");
	UB_CONF_GETUINT(conf, "reqbuf_size", &g_conf.reqbuf_size, "请求buffer的长");
	UB_CONF_GETUINT(conf, "resbuf_size", &g_conf.resbuf_size, "应答buffer的长");

	UB_CONF_GETNSTR(conf, "db_path", g_conf.db_path, sizeof(g_conf.db_path), "db数据所在目录");
	UB_CONF_GETUINT(conf, "max_open_table", &g_conf.max_open_table, "最多能打开的表的数量");
	UB_CONF_GETUINT(conf, "devide_table_period", &g_conf.devide_table_period, "分表周期");
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
 * 配置项的合法性检查(如果在.range文件已做了检查,本函数可以 do nothing)
 *
 * @return
 *       0  : 成功
 *       -1 : 失败
 **/
static int serv_validateconf()
{
	return 0;
}

/**
 *
 * 测试load配置文件是否成功
 *
 * @param dir 配置文件的目录
 * @param file 配置文件名
 *
 * @return
 *       0  : 成功
 *       -1 : 失败
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
 * 服务初始化
 *
 * @return
 *       0  : 成功
 *       -1 : 失败
 **/
static int serv_init()
{
	int ret = 0;

	signal(SIGPIPE, SIG_IGN);

	//服务相关
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

	//使用激进型回调
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
// 初始化数据，包括全局数据&线程私有数据。
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
// 初始化各个模块
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
 *运行服务
 *
 * @return
 *       0  : 成功
 *       -1 : 失败
 **/
static int serv_run() 
{
    ub_server_setoptsock(g_data.aodb_svr, UBSVR_NODELAY);
	//aodb服务
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
 *服务的清理工作
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

	//读取配置文件
    ret = serv_loadconf(g_conf.conf_dir, g_conf.conf_file, 0);
    if (ret < 0) {
        UB_LOG_FATAL("load config [dir:%s file:%s] failed!",
                g_conf.conf_dir, g_conf.conf_file);
        goto out;
    }

    // 初始化服务
	ret = serv_init();
	if (0 != ret) {
		UB_LOG_FATAL("serv_init failed! [ret:%d]", ret);
		goto out;
	} else {
		UB_LOG_FATAL("serv_init success!");
	}

    // 初始化数据
    ret = init_data();
    if (0 != ret) {
        UB_LOG_FATAL("init_data failed![ret:%d]", ret);
        goto out;
    } else {
        UB_LOG_FATAL("init_data success!");
    }

    // 初始化其他模块
    ret = init_module();
    if (0 != ret) {
        UB_LOG_FATAL("init_module failed![ret:%d]", ret);
        goto out;
    } else {
        UB_LOG_FATAL("init_module success!");
    }

	//服务开始啦!!
	ret = serv_run();
	if (0 != ret) {
		UB_LOG_FATAL("serv_start failed! [ret:%d]", ret);
		goto out;
	} else {
		UB_LOG_FATAL("serv_start success!");
	}

out:
	//清除工作
	serv_cleanup();
	return ret;
}

