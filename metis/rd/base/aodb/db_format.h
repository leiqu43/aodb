/*
 * =====================================================================================
 *
 *       Filename:  db_format.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2012 03:05:56 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#ifndef AODB_DB_FORMAT_H_
#define AODB_DB_FORMAT_H_

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <boost/utility.hpp>

#define MAGIC_NUM (0x371f1712)

namespace aodb {

//
// ����ͷ������ͷ�����Ǿ����key,value���ݡ�
// ��һ������ƫ��Ϊ ��ǰƫ��+header_size+key_length+value_length;
//
struct aodb_data_header
{
    uint32_t magic_num;
    uint16_t header_size;
    uint8_t version;
    uint8_t status;
    uint32_t key_length;
    uint32_t value_length;
    uint64_t key_sign;
    uint64_t value_sign;
};

//
// �������������������ع�ʱ�䡣
//
struct aodb_index
{
    uint64_t key_sign;
    uint64_t block_offset;
    uint32_t block_size;
};

}

#endif

