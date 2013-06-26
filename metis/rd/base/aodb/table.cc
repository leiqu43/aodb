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
#include <algorithm>
#include <boost/foreach.hpp>

#include "ub_log.h"
#include "posix_env.h"
#include "db_format.h"

using namespace aodb;

int Table::Open(const std::string& table_path, 
                const std::string& table_name, 
                uint64_t max_file_size, 
                bool read_only,
                Table **table)
{
    assert(table_path.length() > 0);
    assert(table_name.length() > 0);
    assert(table);
    assert(!(*table));

    // 临时索引文件
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

    *table = new Table(table_name, aodb_index_file, aodb_data_file, read_only);
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

Table::Table(const std::string& table_name, 
             PosixRandomAccessFile* aodb_index_file, 
             PosixRandomAccessFile* aodb_data_file, 
             bool read_only) 
        : table_name_(table_name),
          aodb_index_file_(aodb_index_file),
          aodb_data_file_(aodb_data_file),
          read_only_(read_only),
          in_sorting_(false)
{
    assert(table_name.length() > 0);
    assert(aodb_index_file_);
    assert(aodb_data_file_);
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

void Table::UpdateIndexDict(const struct aodb_index& aodb_index)
{
    boost::mutex::scoped_lock index_dict_lock(index_dict_lock_);
    std::map<uint64_t, struct aodb_index>::iterator iter = index_dict_.find(aodb_index.key_sign);
    if (iter == index_dict_.end()) {
        index_dict_.insert(std::make_pair(aodb_index.key_sign, aodb_index));
        UB_LOG_DEBUG("UpdateIndexDict new![key_sign:%lu]", aodb_index.key_sign);
    } else {
        assert(aodb_index.key_sign == iter->second.key_sign);
        iter->second = aodb_index;
        UB_LOG_DEBUG("UpdateIndexDict update![key_sign:%lu]", aodb_index.key_sign);
    }
}

int Table::GetIndex(const std::string& key, struct aodb_index* aodb_index)
{
    assert(key.length() > 0);
    assert(aodb_index);

    uint64_t key_sign = 0;
    PosixEnv::CalcMd5_64(key, &key_sign);
    ub_log_pushnotice("key_sign", "%lu", key_sign);
    // 如果只读，而且in_sorting_为true，那么此时index_dict_还是可读的。
    if (read_only_) {
        // binary search
        struct aodb_index value;
        value.key_sign = key_sign;
        std::vector<struct aodb_index>::const_iterator iter = 
            std::lower_bound(sorted_index_array_.begin(), 
                             sorted_index_array_.end(),
                             value);
        if (iter == sorted_index_array_.end()) {
            UB_LOG_DEBUG("got nothing![read_only:true][key_sign:%lx]", key_sign);
            return 0;
        }
        *aodb_index = *iter;
        return 1;
    } else {
        boost::mutex::scoped_lock index_dict_lock(index_dict_lock_);
        std::map<uint64_t, struct aodb_index>::const_iterator iter = index_dict_.find(key_sign);
        if (iter == index_dict_.end()) {
            UB_LOG_DEBUG("got nothing![read_only:false][key_sign:%lx]", key_sign);
            return 0;
        }
        *aodb_index = iter->second;
    }
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
    if (read_only_) {
        SortIndex();
        index_dict_.clear();
    }
    UB_LOG_TRACE("Table::Initialize success![item:%d]", index_item_num);
    return 0;
}

void Table::SortIndex() 
{
    assert(0 == sorted_index_array_.size());

    sorted_index_array_.reserve(index_dict_.size());
    typedef std::map<uint64_t, struct aodb_index> aodb_index_dict_t;
    BOOST_FOREACH(const aodb_index_dict_t::value_type& pair, 
                  index_dict_) {
        sorted_index_array_.push_back(pair.second);
    }
    // 上面给出的结果应该是有序的(std::map的实现相关)，std::sort再确认一下。
    std::sort(sorted_index_array_.begin(), sorted_index_array_.end());
}

int Table::Get(const std::string& key, std::string* value)
{
    assert(0 != key.length());
    assert(0 == value->length());

    // 查找索引
    struct aodb_index aodb_index;
    int ret = GetIndex(key, &aodb_index);
    if (ret < 0) {
        UB_LOG_WARNING("GetIndex failed![ret:%d]", ret);
        return -1;
    }
    if (0 == ret) {
        UB_LOG_DEBUG("get nothing![key:%s]", key.c_str());
        return 0;
    }

    // 读取数据头
    aodb_data_header header;
    ret = aodb_data_file_->Read(aodb_index.block_offset, sizeof(header), &header);
    if (ret < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::Read header failed![ret:%d]", ret);
        return -1;
    }

    // 检查数据头
    assert(header.magic_num == MAGIC_NUM);

    // 读value
    ret = aodb_data_file_->Read(aodb_index.block_offset + sizeof(header) + header.key_length, \
                                header.value_length, value);
    if (ret < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::Read failed![ret:%d][size:%u]", ret, header.value_length);
        return -1;
    }
    assert(header.value_length == value->length());
    UB_LOG_DEBUG("Table::Read success![key:%s][value_size:%ld]", key.c_str(), value->length());
    return 1;
}

int Table::Put(const std::string& key, const std::string& value)
{
    // MarkAsReadOnly后，就不能Put了，这个应该是上层来控制的。
    assert(!read_only_);
    assert(!in_sorting_);

    struct aodb_data_header data_header;
    data_header.magic_num = MAGIC_NUM;
    data_header.key_length = key.length();
    data_header.value_length = value.length();

    std::string data;
    data.reserve(sizeof(data_header) + key.length() + value.length());
    data.append(reinterpret_cast<char*>(&data_header), sizeof(data_header));
    data.append(key);
    data.append(value);

    boost::mutex::scoped_lock table_put_lock(table_put_lock_);
    //  写入数据
    ssize_t offset = aodb_data_file_->Append(data);
    if (offset < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::Append failed![ret:%ld]", offset);
        return -1;
    }

    // 写入索引
    struct aodb_index aodb_index;
    PosixEnv::CalcMd5_64(key, &aodb_index.key_sign);
    aodb_index.block_offset = offset;
    aodb_index.padding = 0x0;

    offset = aodb_index_file_->Append(reinterpret_cast<char*>(&aodb_index), \
                                      sizeof(aodb_index));
    if (-1 == offset) {
        UB_LOG_WARNING("PosixRandomAccessFile::Append failed!");
        return -1;
    }
    UpdateIndexDict(aodb_index);
    UB_LOG_DEBUG("Table::Put[key:%s][block_offset:%lu][value_length:%ld]", 
            key.c_str(), aodb_index.block_offset, value.length());
    return 0;
}

void Table::MarkAsReadOnly()
{
    assert(!read_only_);
    assert(!in_sorting_);

    // 注意，不应该影响正常的服务
    in_sorting_ = true;
    SortIndex();
    {
        boost::mutex::scoped_lock index_dict_lock(index_dict_lock_);
        index_dict_.clear();
        read_only_ = true;
    }
    in_sorting_ = false;
}

