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

#include <sys/time.h>
#include <iostream>
#include <algorithm>
#include <boost/foreach.hpp>
#include "ub_log.h"
#include "snappy.h"

#include "posix_env.h"
#include "table.h"

using namespace aodb;

inline std::string get_table_name(const struct table_info& table_info)
{
    char buf[64] = "\0";
    snprintf(buf, sizeof(buf), "table_%04d_%02d_%02d_%02d", table_info.table_year, 
            table_info.table_month, table_info.table_day, table_info.table_hour);
    return buf;
}

inline std::string get_write_table_name(int devide_table_period)
{
    char buf[64] = "\0";
    time_t table_time = (time(NULL)/(3600*devide_table_period))*(3600*devide_table_period);
    struct tm result;
    strftime(buf, sizeof(buf), "table_%Y_%m_%d_%H", localtime_r(&table_time, &result));
    return buf;
}

int Db::OpenDb(const std::string& path, 
               const std::string& db_name,
               int max_open_table, 
               int devide_table_period,
               Db** result_db)
{
    assert(result_db);
    assert(!*result_db);

    const std::string db_path = path + "/" + db_name + "/";
    int ret = PosixEnv::CreateDir(db_path);
    if (ret < 0) {
        UB_LOG_WARNING("PosixEnv::CreateDir failed![ret:%d][path:%s]", ret, db_path.c_str());
        return -1;
    }

    *result_db = new Db(db_path, db_name, max_open_table, devide_table_period);
    assert(*result_db);

    ret = (*result_db)->Initialize();
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
    // 扫描符合条件的表
    std::vector<std::string> tables;
    int ret = ScanTable(&tables);
    if (ret < 0) {
        UB_LOG_WARNING("ScanTable failed![ret:%d]", ret);
        return -1;
    }

    // 加载符合条件的表
    ret = LoadTables(tables);
    if (ret < 0) {
        UB_LOG_WARNING("LoadTables failed![ret:%d]", ret);
        return -1;
    }

    UB_LOG_TRACE("Db::Initialize success!");
    return 0;
}

int Db::LoadTables(const std::vector<std::string>& tables)
{
    int ret = -1;
    BOOST_FOREACH(const std::string& table_name, tables) {
        // TODO 如何决定一个table是不是read only的？
        Table* table = NULL;
        ret = Table::Open(db_path_, table_name, 0x10000000, true, &table);
        if (ret < 0) {
            UB_LOG_WARNING("Table::Open failed![ret:%d][table:%s]", ret, table_name.c_str());
            return -1;
        }
        {
            boost::mutex::scoped_lock primary_table_lock(primary_table_lock_);
            if (NULL == primary_table_) {
                primary_table_ = boost::shared_ptr<Table>(table);
                UB_LOG_DEBUG("use table %s as primary table!", table_name.c_str());
                continue;
            }
        }
        {
            boost::mutex::scoped_lock tables_list_lock(tables_list_lock_);
            tables_list_.push_back(boost::shared_ptr<Table>(table));
        }
        UB_LOG_DEBUG("append table to table list![table:%s]", table_name.c_str());
    }
    UB_LOG_TRACE("LoadTables success!");
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
        // 表名称：table_yyyy_mm_dd，例如table_20120614
        struct table_info table_info;
        if (file.length() <=4 || file.substr(file.length()-3, 3) != "ind") {
            continue;
        }
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

    std::sort(tables_info_array.begin(), tables_info_array.end(), table_info_less());
    std::reverse(tables_info_array.begin(), tables_info_array.end());
    for (int i=0; i<(int)tables_info_array.size() && i<max_open_table_; ++i) {
        result_tables->push_back(get_table_name(tables_info_array[i]));
    }
    return 0;
}

int Db::GetTableInfoFromTableName(const std::string& table_name, struct table_info *result)
{
    int ret = sscanf(table_name.c_str(), "table_%d_%d_%d_%d.ind", &(result->table_year), 
                 &(result->table_month), &(result->table_day), &(result->table_hour));
    if (4 != ret) {
        UB_LOG_DEBUG("can't get table info![table_name:%s]", table_name.c_str());
        return 0;
    }
    return 1;
}

int Db::GetAllTables(std::vector<boost::shared_ptr<Table> > *result_tables) 
{
    assert(result_tables);

    result_tables->clear();
    {
        boost::mutex::scoped_lock primary_table_lock(primary_table_lock_);
        if (primary_table_) {
            result_tables->push_back(primary_table_);
        }
    }
    {
        boost::mutex::scoped_lock tables_list_lock(tables_list_lock_);
        BOOST_FOREACH(boost::shared_ptr<Table> table, tables_list_) {
            result_tables->push_back(table);
        }
    }
    return 0;
}

int Db::Get(const std::string& key, std::string* value)
{
    assert(key.length());
    assert(value);

    std::vector<boost::shared_ptr<Table> > tables;
    int ret = GetAllTables(&tables);
    if (ret < 0) {
        UB_LOG_WARNING("GetAllTables failed![ret:%d]", ret);
        return -1;
    }
    std::string compress_data;
    BOOST_FOREACH(boost::shared_ptr<Table> table, tables) {
        ret = table->Get(key, &compress_data);
        if (ret < 0) {
            UB_LOG_WARNING("Table::Get failed![ret:%d][table:%s][key:%s]", ret, \
                    table->TableName().c_str(), key.c_str());
            return -1;
        }
        if (compress_data.length() > 0) break;
        UB_LOG_DEBUG("Table::Get nothing![key:%s][table:%s]", key.c_str(), table->TableName().c_str());
    }
    value->clear();
    if (compress_data.length() > 0) {
        snappy::Uncompress(compress_data.data(), compress_data.size(), value);
        ub_log_pushnotice("real_size", "%lu", value->length());
        UB_LOG_DEBUG("Db::Get success![key:%s][value_size:%ld]", \
                     key.c_str(), value->length());
    } else {
        UB_LOG_DEBUG("can't find key in tables![tables:%lu][key:%s]", tables.size(), key.c_str());
    }
    return 0;
}

int Db::Put(const std::string& key, const std::string& value)
{
    assert(key.length());
    assert(value.length());

    // 压缩数据
    std::string compress_data;
    snappy::Compress(value.data(), value.size(), &compress_data);
    ub_log_pushnotice("compress_ratio", "%04f", ((float)compress_data.length())/value.length());

    // 得到要写入的表名
    std::string write_table_name = get_write_table_name(devide_table_period_);
    {
        boost::mutex::scoped_lock primary_table_lock(primary_table_lock_);
        if (!primary_table_ || (primary_table_->TableName() != write_table_name)) {
            // 创建新的表
            Table* table = NULL;
            int ret = Table::Open(db_path_, write_table_name, 0x0, false, &table);
            if (ret < 0) {
                UB_LOG_WARNING("Table::Open failed![ret:%d]", ret);
                return -1;
            }
            if (primary_table_){
                boost::mutex::scoped_lock tables_list_lock(tables_list_lock_);
                tables_list_.push_front(primary_table_);
                if ((int)tables_list_.size() >= max_open_table_) {
                    UB_LOG_TRACE("drop olddest table %s", tables_list_.back()->TableName().c_str());
                    tables_list_.pop_back();
                }
            }
            primary_table_ = boost::shared_ptr<Table>(table);
        }
    }

    // 写入数据
    boost::shared_ptr<Table> write_table;
    {
        boost::mutex::scoped_lock primary_table_lock(primary_table_lock_);
        write_table = primary_table_;
    }
    int ret = write_table->Put(key, compress_data);
    if (ret < 0) {
        UB_LOG_WARNING("Table::Put failed![ret:%d][table:%s]", ret, write_table->TableName().c_str());
        return -1;
    }
    UB_LOG_DEBUG("Db::Put success![key:%s][value_size:%lu]", key.c_str(), value.length());
    return 0;
}

