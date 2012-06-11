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
    //  filename : ������
    //  file_size : ���������ļ���С
    //  create_if_not_exist : ��������ڣ���ô������
    //
    //  ����ɹ�����ô*table���ش����ɹ��ı����ʧ�ܣ�retval < 0
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

