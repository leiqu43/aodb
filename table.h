/*
 * =====================================================================================
 *
 *       Filename:  table.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2013 02:45:41 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#ifndef AODB_TABLE_H_
#define AODB_TABLE_H_

#include <stdint.h>
#include <stdio.h>
#include <vector>
#include <string>
#include <map>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/utility.hpp>

#include "db_format.h"

namespace aodb {

class PosixRandomAccessFile;

//
// 一个表是由一个(key,value)字典组成，其中一个表包括如下三部分：磁盘文件、磁盘索引，内存索引(map or sorted array)。
//
class Table : boost::noncopyable {
public:
    //
    // 打开一个Table。
    //  table_path : 表数据所保存的目录
    //  filename : 表名称
    //  max_table_size : 最大表的大小(key数量)
    //  max_file_size : 最大数据文件大小
    //  read_only : 是否只读
    //
    //  如果成功，那么*table返回创建成功的表。如果失败，retval < 0
    //
    static int Open(const std::string& table_path, 
                    const std::string& table_name,
                    uint32_t max_table_size,
                    bool read_only,
                    Table **table);
    ~Table();

    //
    // 得到表名
    //
    std::string TableName() {
        return table_name_;
    }

    bool CheckFullWithLock() {
        assert(index_dict_.size() <= max_table_size_);
        return index_dict_.size() == max_table_size_;
    }

    //
    // 是否已经写满了？
    //
    bool CheckFull() {
        boost::mutex::scoped_lock table_put_lock(table_put_lock_);
        return CheckFullWithLock();
    }

    //
    // 从表中获取数据
    //
    int Get(const std::string& key, std::string* value);

    //
    // 写入数据到表中
    //
    int Put(const std::string& key, const std::string& value);


    // 启动后台处理线程
    void RunBgWorkThread();

private:

    explicit Table(const std::string& table_path,
                   const std::string& table_name, 
                   uint32_t max_table_size,
                   bool read_only);

    //
    // 初始化db
    //
    int Initialize();

    //
    // 更新内存索引
    //
    void UpdateIndexDict(const struct aodb_index& aodb_index);

    //
    // 从索引中查询一个item
    // retval :
    //      <0  :   失败
    //      0   :   没有找到
    //      1  :   成功
    //
    int GetIndex(const std::string& key, struct aodb_index* aodb_index);

    void SortIndex();

    // Background thread
    void BgThread();

    //
    // 标记为只读
    //
    void MarkAsReadOnly();

    // 保存只读索引
    int SaveSortedIndex();

    // 加载自读索引
    int LoadSortedIndex();

    // 加载未排序索引数据
    int LoadTmpIndex();

    // 从dict中查找索引
    int GetIndexFromDict(uint64_t key_sign, struct aodb_index* aodb_index);

    // 从只读索引中查找
    int GetIndexFromSortedIndex(uint64_t key_sign, struct aodb_index* aodb_index);

private:
    // 表路径&名称
    const std::string table_path_;
    const std::string table_name_;

    // 索引文件路径
    std::string index_file_path_;
    // 临时索引文件路劲
    std::string tmp_index_file_path_;
    // 数据文件路径
    std::string data_file_path_;

    // 索引文件
    PosixRandomAccessFile *tmp_index_file_writer_;
    // 数据文件
    PosixRandomAccessFile *data_file_reader_writer_;

    // aodb内存索引
    std::map<uint64_t, struct aodb_index> index_dict_;
    boost::mutex index_dict_lock_;

    // read only sorted index
    std::vector<struct aodb_index> sorted_index_array_;

    // 写数据锁，保证每个时刻只有一个用户写数据
    boost::mutex table_put_lock_;

    // 最大表的大小
    uint32_t max_table_size_;

    // 是否是只读的
    bool read_only_;

    // 是否正在排序中
    bool in_sorting_;

    boost::thread* bg_thread_;
};

}

#endif
