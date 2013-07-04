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
// һ��������һ��(key,value)�ֵ���ɣ�����һ����������������֣������ļ��������������ڴ�����(map or sorted array)��
//
class Table : boost::noncopyable {
public:
    //
    // ��һ��Table��
    //  table_path : �������������Ŀ¼
    //  filename : ������
    //  max_table_size : ����Ĵ�С(key����)
    //  max_file_size : ��������ļ���С
    //  read_only : �Ƿ�ֻ��
    //
    //  ����ɹ�����ô*table���ش����ɹ��ı����ʧ�ܣ�retval < 0
    //
    static int Open(const std::string& table_path, 
                    const std::string& table_name,
                    uint32_t max_table_size,
                    bool read_only,
                    Table **table);
    ~Table();

    //
    // �õ�����
    //
    std::string TableName() {
        return table_name_;
    }

    bool CheckFullWithLock() {
        assert(index_dict_.size() <= max_table_size_);
        return index_dict_.size() == max_table_size_;
    }

    //
    // �Ƿ��Ѿ�д���ˣ�
    //
    bool CheckFull() {
        boost::mutex::scoped_lock table_put_lock(table_put_lock_);
        return CheckFullWithLock();
    }

    //
    // �ӱ��л�ȡ����
    //
    int Get(const std::string& key, std::string* value);

    //
    // д�����ݵ�����
    //
    int Put(const std::string& key, const std::string& value);


    // ������̨�����߳�
    void RunBgWorkThread();

private:

    explicit Table(const std::string& table_path,
                   const std::string& table_name, 
                   uint32_t max_table_size,
                   bool read_only);

    //
    // ��ʼ��db
    //
    int Initialize();

    //
    // �����ڴ�����
    //
    void UpdateIndexDict(const struct aodb_index& aodb_index);

    //
    // �������в�ѯһ��item
    // retval :
    //      <0  :   ʧ��
    //      0   :   û���ҵ�
    //      1  :   �ɹ�
    //
    int GetIndex(const std::string& key, struct aodb_index* aodb_index);

    void SortIndex();

    // Background thread
    void BgThread();

    //
    // ���Ϊֻ��
    //
    void MarkAsReadOnly();

    // ����ֻ������
    int SaveSortedIndex();

    // �����Զ�����
    int LoadSortedIndex();

    // ����δ������������
    int LoadTmpIndex();

    // ��dict�в�������
    int GetIndexFromDict(uint64_t key_sign, struct aodb_index* aodb_index);

    // ��ֻ�������в���
    int GetIndexFromSortedIndex(uint64_t key_sign, struct aodb_index* aodb_index);

private:
    // ��·��&����
    const std::string table_path_;
    const std::string table_name_;

    // �����ļ�·��
    std::string index_file_path_;
    // ��ʱ�����ļ�·��
    std::string tmp_index_file_path_;
    // �����ļ�·��
    std::string data_file_path_;

    // �����ļ�
    PosixRandomAccessFile *tmp_index_file_writer_;
    // �����ļ�
    PosixRandomAccessFile *data_file_reader_writer_;

    // aodb�ڴ�����
    std::map<uint64_t, struct aodb_index> index_dict_;
    boost::mutex index_dict_lock_;

    // read only sorted index
    std::vector<struct aodb_index> sorted_index_array_;

    // д����������֤ÿ��ʱ��ֻ��һ���û�д����
    boost::mutex table_put_lock_;

    // ����Ĵ�С
    uint32_t max_table_size_;

    // �Ƿ���ֻ����
    bool read_only_;

    // �Ƿ�����������
    bool in_sorting_;

    boost::thread* bg_thread_;
};

}

#endif
