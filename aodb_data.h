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
// config info
//
typedef struct _aodb_conf_t 
{
	ub_svr_t aodb;					       /**<   aodb config */

	char conf_dir[PATH_SIZE];			   /**<   config dir    */
	char conf_file[PATH_SIZE];			   /**<   config file name        */
	char data_dir[PATH_SIZE];			   /**<   data dir  */

	char log_dir[PATH_SIZE];			   /**<   log dir  */
	char log_file[PATH_SIZE];			   /**<   log name        */
	int log_level;					       /**<   log level          */
	int log_size;					       /**<   log size (M) */

	uint32_t reqbuf_size;				   /**<   length for request buffer  */
	uint32_t resbuf_size;				   /**<   length for response buffer  */

    char db_path[PATH_SIZE];               /**<   data dir  */
    uint32_t max_open_table;               /**<   max open table  */
    uint32_t devide_table_period;          /**<   devide table period (Hour) */
} aodb_conf_t;

//
// thread data
//
typedef struct _thread_data_t 
{
} thread_data_t;

//
// global data
//
typedef struct _aodb_data_t {
	ub_server_t *aodb_svr;			      /**<  aodb service */
    thread_data_t **thread_data;          /**<  thread data */
    bool need_quit;                       /**<  quit service or not */
} aodb_data_t;

extern aodb_conf_t g_conf;

#endif

