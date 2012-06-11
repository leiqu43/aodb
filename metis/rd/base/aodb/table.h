/*
 * =====================================================================================
 *
 *       Filename:  table.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2012 02:45:41 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#ifndef AODB_TABLE_H_
#define AODB_TABLE_H_

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <boost/utility.hpp>

namespace aodb {

//
// 一个表是由一个(key,value)字典组成，其中一个表包括如下三部分：磁盘文件、磁盘索引，内存索引。
//
class Table : boost::noncopyable {
public:
    //
    // 打开一个Table。
    //  table_path : 表数据所保存的目录
    //  filename : 表名称
    //  file_size : 最大数据文件大小
    //
    //  如果成功，那么*table返回创建成功的表。如果失败，retval < 0
    //
    static int Open(const std::string& table_path, 
                    const std::string& table_name,
                    uint64_t file_size, 
                    Table **table);
    ~Table();

    //
    // 从表中获取数据
    //
    int Get(const std::string& key, std::string* value);

    //
    // 写入数据到表中
    //
    int Put(const std::string& key, std::string& value);

private:

    // 索引文件句柄
    FILE *aodb_index_fp_;
    // 数据文件句柄
    FILE *aodb_data_fp_;

    explicit Table(FILE *aodb_index_fp, FILE *aodb_data_fp);
    //
    // 初始化db
    //
    int Initialize();
};

}

#endif

