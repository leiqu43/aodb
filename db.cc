/*
 * =====================================================================================
 *
 *       Filename:  db.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2012 02:29:59 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#include "db.h"

#include <iostream>
#include <boost/foreach.hpp>
#include "ub_log.h"
#include "posix_env.h"

using namespace aodb;

inline std::string get_table_name(const struct table_info& table_info)
{
    char buf[64] = "\0";
    snprintf(buf, sizeof(buf), "table_%04d_%02d_%02d", table_info.table_year, 
            table_info.table_month, table_info.table_day);
    return buf;
}

int Db::OpenDb(const std::string& db_path, 
              int max_open_table, 
              int devide_table_period,
              Db** result_db)
{
    assert(result_db);
    assert(!*result_db);

    *result_db = new Db(db_path, max_open_table, devide_table_period);
    assert(*result_db);

    int ret = (*result_db)->Initialize();
    if (ret < 0) {
        UB_LOG_WARNING("Db::Initialize failed![ret:%d]", ret);
        delete *result_db;
        *result_db = NULL;
        return -1;
    }
    UB_LOG_TRACE("Db::OpenDb success![db_path:%s]", db_path.c_str());
    return 0;
}

int Db::Initialize()
{
    std::vector<std::string> tables;
    int ret = ScanTable(&tables);
    if (ret < 0) {
        UB_LOG_WARNING("ScanTable failed![ret:%d]", ret);
        return -1;
    }
    return 0;
}

int Db::ScanTable(std::vector<std::string>* result_tables)
{
    assert(result_tables);
    result_tables->clear();

    std::vector<std::string> files;
    int ret = PosixEnv::GetDirChildren(db_path_, &files);
    if (ret < 0) {
        UB_LOG_WARNING("PosixEnv::GetDirChildren failed![ret:%d][db_path:%s]", 
                        ret, db_path_.c_str());
        return -1;
    }
    if (0 == files.size()) {
        UB_LOG_DEBUG("got 0 table![db_path:%s]", db_path_.c_str());
        return 0;
    }
    std::vector<struct table_info> tables_info_array;
    BOOST_FOREACH(const std::string& file, files) {
        // ±íÃû³Æ£ºtable_yyyy_mm_dd£¬ÀýÈçtable_20120614
        struct table_info table_info;
        ret = GetTableInfoFromTableName(file, &table_info);
        if (ret < 0) {
            UB_LOG_WARNING("GetTableInfoFromTableName failed![ret:%d]", ret);
            return -1;
        } else if (0 == ret) {
            UB_LOG_DEBUG("not table, skip![file:%s]", file.c_str());
            continue;
        }
        tables_info_array.push_back(table_info);
    }
    for (int i=0; i<(int)tables_info_array.size() && i<max_open_table_; ++i) {
        std::cout << get_table_name(tables_info_array[i]) << std::endl;
    }
    return 0;
}

int Db::GetTableInfoFromTableName(const std::string& table_name, struct table_info *result)
{
    int ret = sscanf(table_name.c_str(), "table_%d_%d_%d.ind", &(result->table_year), 
                 &(result->table_month), &(result->table_day));
    if (3 != ret) {
        UB_LOG_DEBUG("can't get table info![table_name:%s]", table_name.c_str());
        return 0;
    }
    return 1;
}

int Db::GetAllTables(std::vector<boost::shared_ptr<Table> > *result_tables) 
{
    assert(result_tables);

    result_tables->clear();
    boost::mutex::scoped_lock primary_table_lock(primary_table_lock_);
    result_tables->push_back(primary_table_);
    {
        boost::mutex::scoped_lock tables_list_lock(tables_list_lock_);
        BOOST_FOREACH(boost::shared_ptr<Table> table, tables_list_) {
            result_tables->push_back(table);
        }
    }
    return 0;
}

