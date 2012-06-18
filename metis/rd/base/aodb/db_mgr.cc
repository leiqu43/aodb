/*
 * =====================================================================================
 *
 *       Filename:  db_mgr.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/18/2012 03:36:40 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#include "db_mgr.h"

using namespace aodb;

int DbMgr::Initialize(const std::string& db_path, 
                      int db_max_open_table, 
                      int db_devide_table_period)
{
    db_path_ = db_path;
    db_max_open_table_ = db_max_open_table;
    db_devide_table_period_ = db_devide_table_period;

    return 0;
}

void DbMgr::Release()
{
}

DbMgr::DbMgr()
{
}

DbMgr::~DbMgr()
{
}

