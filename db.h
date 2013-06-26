/*
 * =====================================================================================
 *
 *       Filename:  db.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2012 11:24:18 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#ifndef AODB_DB_H_
#define AODB_DB_H_

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <list>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

namespace aodb {

class Table;

struct table_info {
    // 创建表的时间
    int32_t table_year;
    int32_t table_month;
    int32_t table_day;
    int32_t table_hour;
};

struct table_info_less {
    bool operator() (struct table_info const& x, struct table_info const& y) { 
        uint64_t x_time = (uint64_t)x.table_year << 48 | (uint64_t)x.table_month << 32 | (uint64_t)x.table_day << 16 | x.table_hour;
        uint64_t y_time = (uint64_t)y.table_year << 48 | (uint64_t)y.table_month << 32 | (uint64_t)y.table_day << 16 | y.table_hour;
        return x_time < y_time;
    }
};

class Db {
public:
    //
    // 打开一个DB
    // db_path  :   数据文件地址
    // max_open_table : 最大打开的表个数，保留时间最近的表
    // devide_table_period : 拆分表的周期【单位为天】
    // result_db : 打开的db句柄
    //
    static int OpenDb(const std::string& path, 
                      const std::string& db_name,
                      int max_open_table, 
                      int devide_table_period,
                      Db** result_db);

    virtual ~Db() {
    }

    //
    // 从DB中获取数据
    //
    int Get(const std::string& key, std::string* value);

    //
    // 写入数据到DB中
    //
    int Put(const std::string& key, const std::string& value);

    //
    // 返回db的名称
    //
    const std::string& DbName() const {
        return db_name_;
    }

private:
    Db(const std::string& db_path, 
       const std::string& db_name, 
       const int max_open_table, 
       const int devide_table_period) 
        : db_path_(db_path), 
          db_name_(db_name),
          max_open_table_(max_open_table),
          devide_table_period_(devide_table_period) {
    }

    // 
    // 初始化Db
    //
    int Initialize();

    //
    // 扫描符合条件的表，并且按照时间从最近到最旧排序。
    //
    int ScanTable(std::vector<std::string>* result_tables);

    //
    // 加载指定的tables。
    //
    int LoadTables(const std::vector<std::string>& tables);

    //
    // 获取所有表，包括主表，按照时间段从最近到最旧排序
    // 注意：得到的所有表只能用来进行读操作。
    //
    int GetAllTables(std::vector<boost::shared_ptr<Table> > *result_tables);

    //
    // 根据表名称得到表的信息
    // retval:
    //      <0  :   失败
    //      0   :   不是表
    //      1   :   成功
    //
    int GetTableInfoFromTableName(const std::string& table_name, struct table_info *result);

private:
    // 主表，所有写操作都写入此表
    boost::shared_ptr<Table> primary_table_;
    boost::mutex primary_table_lock_;

    // 读表，只读
    std::list<boost::shared_ptr<Table> > tables_list_;
    boost::mutex tables_list_lock_;

    // db路径&名称
    const std::string db_path_;
    const std::string db_name_;

    const int max_open_table_;
    const int devide_table_period_;
};

}

#endif

