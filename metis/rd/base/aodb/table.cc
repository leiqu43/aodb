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
        uint64_t max_file_size, Table **table)
{
    assert(table_path.length() > 0);
    assert(table_name.length() > 0);
    //assert(max_file_size >= 0);
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

    *table = new Table(table_name, aodb_index_file, aodb_data_file);
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

Table::Table(const std::string& table_name, PosixRandomAccessFile* aodb_index_file, 
             PosixRandomAccessFile* aodb_data_file) 
        : table_name_(table_name),
          aodb_index_file_(aodb_index_file),
          aodb_data_file_(aodb_data_file)
{
    assert(aodb_index_file_);
    assert(aodb_data_file_);
}

void Table::UpdateIndexDict(const struct aodb_index& aodb_index)
{
    boost::mutex::scoped_lock index_dict_lock(index_dict_lock_);
    std::map<uint64_t, struct aodb_index>::iterator iter = index_dict_.find(aodb_index.key_sign);
    if (iter == index_dict_.end()) {
        index_dict_.insert(std::make_pair(aodb_index.key_sign, aodb_index));
        UB_LOG_DEBUG("UpdateIndexDict new![key_sign:%lu][value_sign:%lu]", aodb_index.key_sign, aodb_index.value_sign);
    } else {
        assert(aodb_index.key_sign == iter->second.key_sign);
        iter->second = aodb_index;
        UB_LOG_DEBUG("UpdateIndexDict update![key_sign:%lu][value_sign:%lu]", aodb_index.key_sign, aodb_index.value_sign);
    }
}

int Table::GetItemFromIndexDict(const std::string& key, struct aodb_index* aodb_index)
{
    assert(key.length() > 0);
    assert(aodb_index);

    uint64_t key_sign = 0;
    PosixEnv::CalcMd5_64(key, &key_sign);
    ub_log_pushnotice("key_sign", "%lu", key_sign);
    boost::mutex::scoped_lock index_dict_lock(index_dict_lock_);
    std::map<uint64_t, struct aodb_index>::const_iterator iter = index_dict_.find(key_sign);
    if (iter == index_dict_.end()) {
        return 0;
    }
    *aodb_index = iter->second;
    assert(key_sign == aodb_index->key_sign);
    return 1;
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
    UB_LOG_TRACE("Table::Initialize success![item:%d]", index_item_num);
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
    struct aodb_index aodb_index;
    int ret = GetItemFromIndexDict(key, &aodb_index);
    if (ret < 0) {
        UB_LOG_WARNING("GetItemFromIndexDict failed![ret:%d]", ret);
        return -1;
    }
    value->clear();
    if (0 == ret) {
        return 0;
    }

    std::string data;
    ret = aodb_data_file_->Read(aodb_index.block_offset, aodb_index.block_size, &data);
    if (ret < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::Read failed![ret:%d]", ret);
        return -1;
    }
    struct aodb_data_header *data_header = NULL;
    data_header = (struct aodb_data_header*)const_cast<char *>(data.c_str());
    assert(data_header->key_sign == aodb_index.key_sign);
    value->append(data.c_str() + data_header->header_size + data_header->key_length, data_header->value_length);
    uint64_t value_sign;
    PosixEnv::CalcMd5_64(*value, &value_sign);
    UB_LOG_DEBUG("Table::Get![key:%s][value_sign:%lu][actual_value_size:%lu][block_offset:%lu]", key.c_str(), 
                 data_header->value_sign, value_sign, aodb_index.block_offset);
    assert(value_sign == data_header->value_sign);
    return 0;
}

int Table::Put(const std::string& key, const std::string& value)
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
    aodb_index.block_size = data.length();
    aodb_index.value_sign = data_header.value_sign;

    {
        boost::mutex::scoped_lock table_put_lock(table_put_lock_);
        // 检查数据是否需要写入
        struct aodb_index old_aodb_index;
        int ret = GetItemFromIndexDict(key, &old_aodb_index);
        if (1 == ret && old_aodb_index.value_sign == aodb_index.value_sign) {
            UB_LOG_DEBUG("duplicate (key,value), ignore![key:%s][value_sign:%lu]", 
                         key.c_str(), aodb_index.value_sign);
            return 0;
        }
        //  写入数据
        aodb_index.block_offset = aodb_data_file_->Append(data);
        if (aodb_index.block_offset < 0) {
            UB_LOG_WARNING("PosixRandomAccessFile::Append failed!");
            return -1;
        }
        data.clear();
        data.append((const char *)(&aodb_index), sizeof(aodb_index));
        ssize_t offset = aodb_index_file_->Append(data);
        if (offset < 0) {
            UB_LOG_WARNING("PosixRandomAccessFile::Append failed!");
            return -1;
        }
        UpdateIndexDict(aodb_index);
    }
    UB_LOG_DEBUG("Table::Put[key:%s][value_sign:%lu][block_offset:%lu]", key.c_str(), data_header.value_sign, aodb_index.block_offset);
    return 0;
}

