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
// One table includes a dict of (key, value), they are file and index on harddrive, index in memory
//
class Table : boost::noncopyable {
public:
    //
    // Open a table
    //  table_path 
    //  filename 
    //  max_table_size 
    //  max_file_size
    //  read_only
    //
    // 
    //
    static int Open(const std::string& table_path, 
                    const std::string& table_name,
                    uint32_t max_table_size,
                    bool read_only,
                    Table **table);
    ~Table();

    //
    // Talbe Name
    //
    std::string TableName() {
        return table_name_;
    }

    bool CheckFullWithLock() {
        assert(index_dict_.size() <= max_table_size_);
        return index_dict_.size() == max_table_size_;
    }

    //
    // Check is full?
    //
    bool CheckFull() {
        boost::mutex::scoped_lock table_put_lock(table_put_lock_);
        return CheckFullWithLock();
    }

    //
    // Get data from table
    //
    int Get(const std::string& key, std::string* value);

    //
    // write data into table
    //
    int Put(const std::string& key, const std::string& value);


    void RunBgWorkThread();

private:

    explicit Table(const std::string& table_path,
                   const std::string& table_name, 
                   uint32_t max_table_size,
                   bool read_only);

    //
    // Init DB
    //
    int Initialize();

    //
    // Update index in memory
    //
    void UpdateIndexDict(const struct aodb_index& aodb_index);

    //
    // search an item from index
    //
    int GetIndex(const std::string& key, struct aodb_index* aodb_index);

    void SortIndex();

    // Background thread
    void BgThread();

    //
    // mark as read only
    //
    void MarkAsReadOnly();

    // save read-only index
    int SaveSortedIndex();

    // load index
    int LoadSortedIndex();

    // load temporary index
    int LoadTmpIndex();

    // get index from dict
    int GetIndexFromDict(uint64_t key_sign, struct aodb_index* aodb_index);

    // get index from read-only index
    int GetIndexFromSortedIndex(uint64_t key_sign, struct aodb_index* aodb_index);

private:
    const std::string table_path_;
    const std::string table_name_;

    std::string index_file_path_;
    std::string tmp_index_file_path_;
    std::string data_file_path_;

    PosixRandomAccessFile *tmp_index_file_writer_;
    PosixRandomAccessFile *data_file_reader_writer_;

    // aodb index in memory
    std::map<uint64_t, struct aodb_index> index_dict_;
    boost::mutex index_dict_lock_;

    // read only sorted index
    std::vector<struct aodb_index> sorted_index_array_;

    // write lock
    boost::mutex table_put_lock_;

    uint32_t max_table_size_;

    // read only or not
    bool read_only_;

    // is it sorting?
    bool in_sorting_;

    boost::thread* bg_thread_;
};

}

#endif
