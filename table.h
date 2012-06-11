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

    // �����ļ����
    FILE *aodb_index_fp_;
    // �����ļ����
    FILE *aodb_data_fp_;

    explicit Table(FILE *aodb_index_fp, FILE *aodb_data_fp);
    //
    // ��ʼ��db
    //
    int Initialize();
};

}

#endif

