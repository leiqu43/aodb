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
    //  max_file_size : ��������ļ���С
    //  read_only : �Ƿ�ֻ��
    //
    //  ����ɹ�����ô*table���ش����ɹ��ı����ʧ�ܣ�retval < 0
    //
    static int Open(const std::string& table_path, 
                    const std::string& table_name,
                    uint64_t max_file_size, 
                    bool read_only,
                    Table **table);
    ~Table();

    //
    // �õ�����
    //
    std::string TableName() {
        return table_name_;
    }

    //
    // �ӱ��л�ȡ����
    //
    int Get(const std::string& key, std::string* value);

    //
    // д�����ݵ�����
    //
    int Put(const std::string& key, const std::string& value);

    //
    // ���Ϊֻ��
    //
    void MarkAsReadOnly();

private:

    explicit Table(const std::string& table_name, 
                   PosixRandomAccessFile *aodb_index_file, 
                   PosixRandomAccessFile *aodb_data_file,
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

private:
    const std::string table_name_;
    // �����ļ�
    PosixRandomAccessFile *aodb_index_file_;
    // �����ļ�
    PosixRandomAccessFile *aodb_data_file_;

    // aodb�ڴ�����
    std::map<uint64_t, struct aodb_index> index_dict_;
    boost::mutex index_dict_lock_;

    // read only sorted index
    std::vector<struct aodb_index> sorted_index_array_;

    // д����������֤ÿ��ʱ��ֻ��һ���û�д����
    boost::mutex table_put_lock_;

    // �Ƿ���ֻ����
    bool read_only_;

    // �Ƿ�����������
    bool in_sorting_;
};

}

#endif
