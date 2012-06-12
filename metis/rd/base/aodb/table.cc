/*
 * =====================================================================================
 *
 *       Filename:  table.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2012 03:53:19 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#include "table.h"

#include "ub_log.h"
#include "posix_env.h"
#include "db_format.h"

using namespace aodb;


int Table::Open(const std::string& table_path, const std::string& table_name, 
        uint64_t file_size, Table **table)
{
    assert(table_path.length() > 0);
    assert(table_name.length() > 0);
    assert(file_size > 0);
    assert(table);
    assert(!(*table));

    std::string table_index_file_path;
    table_index_file_path.append(table_path);
    table_index_file_path.append("/");
    table_index_file_path.append(table_name);
    table_index_file_path.append(".ind");

    std::string table_data_file_path;
    table_data_file_path.append(table_path);
    table_data_file_path.append("/");
    table_data_file_path.append(table_name);
    table_data_file_path.append(".data");

    PosixRandomAccessFile *aodb_index_file = NULL;
    int ret = PosixEnv::NewRandomAccessFile(table_index_file_path, &aodb_index_file);
    if (ret < 0) {
        UB_LOG_WARNING("PosixEnv::NewRandomAccessFile failed![ret:%d]", ret);
        return -1;
    }

    PosixRandomAccessFile *aodb_data_file = NULL;
    ret = PosixEnv::NewRandomAccessFile(table_data_file_path, &aodb_data_file);
    if (ret < 0) {
        UB_LOG_WARNING("PosixEnv::NewRandomAccessFile failed![ret:%d][file:%s]",
                        ret, table_data_file_path.c_str());
        delete aodb_index_file;
        return -1;
    }

    *table = new Table(aodb_index_file, aodb_data_file);
    assert(*table);

    ret = (*table)->Initialize();
    if (ret < 0) {
        UB_LOG_WARNING("Table::Initialize failed![table:%s][ret:%d]", table_name.c_str(), ret);
        delete *table;
        delete aodb_index_file;
        delete aodb_data_file;
        return -1;
    }
    UB_LOG_TRACE("Table::Open success![table:%s]", table_name.c_str());
    return 0;
}

Table::Table(PosixRandomAccessFile* aodb_index_file, PosixRandomAccessFile* aodb_data_file) 
        : aodb_index_file_(aodb_index_file),
          aodb_data_file_(aodb_data_file_)
{
    assert(aodb_index_file_);
    assert(aodb_data_file_);
}

void Table::UpdateIndexDict(const struct aodb_index& aodb_index)
{
    boost::mutex::scoped_lock index_dict_lock(index_dict_lock_);
    index_dict_.insert(std::make_pair(aodb_index.key_sign, aodb_index));
}

int Table::Initialize()
{
    assert(aodb_index_file_);
    assert(aodb_data_file_);

    int ret = -1;
    ssize_t index_file_size = aodb_index_file_->FileSize();
    if (index_file_size < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::FileSize failed!");
        return -1;
    }

    int index_item_num = index_file_size / sizeof(struct aodb_index);
    for (int i=0; i<index_item_num; ++i) {
        // 建立内存索引
        struct aodb_index aodb_index;
        ret = aodb_index_file_->Read(i*sizeof(struct aodb_index), sizeof(struct aodb_index), &aodb_index);
        if (ret < 0) {
            UB_LOG_WARNING("PosixRandomAccessFile::Read failed![ret:%d]", ret);
            return -1;
        }
        UpdateIndexDict(aodb_index);
    }
    return 0;
}

Table::~Table()
{
    if (aodb_index_file_) {
        delete aodb_index_file_;
        aodb_index_file_ = NULL;
    }
    if (aodb_data_file_) {
        delete aodb_data_file_;
        aodb_data_file_ = NULL;
    }
}

int Table::Get(const std::string& key, std::string* value)
{

    return 0;
}

int Table::Put(const std::string& key, std::string& value)
{
    struct aodb_data_header data_header;
    memset(static_cast<void*>(&data_header), 0x0, sizeof(data_header));
    data_header.magic_num = MAGIC_NUM;
    data_header.header_size = sizeof(struct aodb_data_header);
    data_header.version = 1;
    data_header.status = 0;
    data_header.key_length = key.length();
    data_header.value_length = value.length();
    PosixEnv::CalcMd5_64(key, &data_header.key_sign);
    PosixEnv::CalcMd5_64(value, &data_header.value_sign);

    std::string data;
    data.append((const char *)(&data_header), sizeof(aodb_data_header));
    data.append(key);
    data.append(value);

    struct aodb_index aodb_index;
    aodb_index.key_sign = data_header.key_sign;
    aodb_index.block_offset = aodb_data_file_->FileOffset();
    aodb_index.block_size = data.length();

    int ret = aodb_data_file_->Append(data);
    if (ret < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::Append failed!");
        return -1;
    }

    data.clear();
    data.append((const char *)(&aodb_index), sizeof(aodb_index));
    ret = aodb_index_file_->Append(data);
    if (ret < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::Append failed!");
        return -1;
    }

    UpdateIndexDict(aodb_index);
    return 0;
}

