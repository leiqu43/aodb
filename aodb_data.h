/*
 * =====================================================================================
 *
 *       Filename:  aodb_data.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 *
 * =====================================================================================
 */

#ifndef AODB_DATA_H_
#define AODB_DATA_H_

#include <stdint.h>
#include <stdlib.h>

#include "ub.h"

//
// 配置信息
//
typedef struct _aodb_conf_t 
{
	ub_svr_t aodb;					       /**<   正向aodb的配置信息 */

	char conf_dir[PATH_SIZE];			   /**<   配置文件的目录    */
	char conf_file[PATH_SIZE];			   /**<   配置文件名        */
	char data_dir[PATH_SIZE];			   /**<   数据文件的根目录  */

	char log_dir[PATH_SIZE];			   /**<   日志文件的根目录  */
	char log_file[PATH_SIZE];			   /**<   日志文件名        */
	int log_level;					       /**<   日志级别          */
	int log_size;					       /**<   日志回滚的大小(M) */

	uint32_t reqbuf_size;				   /**<   请求buffer的长度  */
	uint32_t resbuf_size;				   /**<   应答buffer的长度  */

    char db_path[PATH_SIZE];               /**<   db数据所在目录  */
    uint32_t max_open_table;               /**<   最大保留table的个数  */
    uint32_t devide_table_period;          /**<   分表周期，单位为小时 */
} aodb_conf_t;

//
// 线程私有数据
//
typedef struct _thread_data_t 
{
} thread_data_t;

//
// 全局相关数据
//
typedef struct _aodb_data_t {
	ub_server_t *aodb_svr;			      /**<  正向aodb服务 */
    thread_data_t **thread_data;          /**<  线程私有数据 */
    bool need_quit;                       /**<  是否退出服务 */
} aodb_data_t;

extern aodb_conf_t g_conf;

#endif

