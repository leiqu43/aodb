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
#include <map>
#include <boost/thread/mutex.hpp>
#include <boost/utility.hpp>

#include "db_format.h"

namespace aodb {

class PosixRandomAccessFile;

//
// 一个表是由一个(key,value)字典组成，其中一个表包括如下三部分：磁盘文件、磁盘索引，内存索引。
//
class Table : boost::noncopyable {
public:
    //
    // 打开一个Table。
    //  table_path : 表数据所保存的目录
    //  filename : 表名称
    //  max_file_size : 最大数据文件大小
    //
    //  如果成功，那么*table返回创建成功的表。如果失败，retval < 0
    //
    static int Open(const std::string& table_path, 
                    const std::string& table_name,
                    uint64_t max_file_size, 
                    Table **table);
    ~Table();

    //
    // 从表中获取数据
    //
    int Get(const std::string& key, std::string* value);

    //
    // 写入数据到表中
    //
    int Put(const std::string& key, const std::string& value);

private:

    explicit Table(PosixRandomAccessFile *aodb_index_file, PosixRandomAccessFile *aodb_data_file);

    //
    // 初始化db
    //
    int Initialize();

    //
    // 更新内存索引
    //
    void UpdateIndexDict(const struct aodb_index& aodb_index);

    //
    // 从索引字典中查询一个item
    // retval :
    //      <0  :   失败
    //      0   :   没有找到
    //      1  :   成功
    //
    int GetItemFromIndexDict(const std::string& key, struct aodb_index* aodb_index);

private:
    // 索引文件
    PosixRandomAccessFile *aodb_index_file_;
    // 数据文件
    PosixRandomAccessFile *aodb_data_file_;

    // 写数据锁，保证每个时刻只有一个用户写数据
    boost::mutex table_put_lock_;

    // aodb内存索引
    std::map<uint64_t, struct aodb_index> index_dict_;
    boost::mutex index_dict_lock_;
};

}

#endif
