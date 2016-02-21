/*
 * =====================================================================================
 *
 *       Filename:  db_format.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2013 03:05:56 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
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

//#pragma pack(push,1)
//
// Data header
//
struct aodb_data_header
{
    uint32_t magic_num;
    uint32_t key_length;
    uint32_t value_length;
};

//
// Index
//
struct aodb_index
{
    uint64_t key_sign;
    uint64_t block_offset:48;
    uint64_t padding:16;
    bool operator<(struct aodb_index y) const {
        return this->key_sign < y.key_sign;
    }
};
//#pragma pack(pop)

}

#endif
