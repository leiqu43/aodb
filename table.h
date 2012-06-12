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
// һ��������һ��(key,value)�ֵ���ɣ�����һ����������������֣������ļ��������������ڴ�������
//
class Table : boost::noncopyable {
public:
    //
    // ��һ��Table��
    //  table_path : �������������Ŀ¼
    //  filename : ������
    //  file_size : ��������ļ���С
    //
    //  ����ɹ�����ô*table���ش����ɹ��ı����ʧ�ܣ�retval < 0
    //
    static int Open(const std::string& table_path, 
                    const std::string& table_name,
                    uint64_t file_size, 
                    Table **table);
    ~Table();

    //
    // �ӱ��л�ȡ����
    //
    int Get(const std::string& key, std::string* value);

    //
    // д�����ݵ�����
    //
    int Put(const std::string& key, std::string& value);

private:

    explicit Table(PosixRandomAccessFile *aodb_index_file, PosixRandomAccessFile *aodb_data_file);

    //
    // ��ʼ��db
    //
    int Initialize();

    //
    // �����ڴ�����
    //
    void UpdateIndexDict(const struct aodb_index& aodb_index);

private:
    // �����ļ�
    PosixRandomAccessFile *aodb_index_file_;
    // �����ļ�
    PosixRandomAccessFile *aodb_data_file_;

    // aodb�ڴ�����
    std::map<uint64_t, struct aodb_index> index_dict_;
    boost::mutex index_dict_lock_;
};

}

#endif
