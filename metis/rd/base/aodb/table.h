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
    //  filename : 表名称
    //  file_size : 最大表数据文件大小
    //  create_if_not_exist : 如果表不存在，那么创建。
    //
    //  如果成功，那么*table返回创建成功的表。如果失败，retval < 0
    //
    static int Open(const std::string& table_name,
                    uint64_t file_size, 
                    bool create_if_not_exist,
                    Table **table);
    ~Table();

private:
};

}

#endif

