/*
 * =====================================================================================
 *
 *       Filename:  table.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2013 03:53:19 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 * 
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
                uint32_t max_table_size,
                bool read_only,
                Table **table)
{
    assert(table_path.length() > 0);
    assert(table_name.length() > 0);
    assert(table);
    assert(!(*table));


    *table = new Table(table_path, table_name, max_table_size, read_only);
    assert(*table);

    int ret = (*table)->Initialize();
    if (ret < 0) {
        UB_LOG_WARNING("Table::Initialize failed![table:%s][ret:%d]", table_name.c_str(), ret);
        delete *table;
        return -1;
    }
    UB_LOG_TRACE("Table::Open success![table:%s]", table_name.c_str());
    return 0;
}

Table::Table(const std::string& table_path,
             const std::string& table_name, 
             uint32_t max_table_size,
             bool read_only) 
        : table_path_(table_path),
          table_name_(table_name),
          tmp_index_file_writer_(NULL),
          data_file_reader_writer_(NULL),
          max_table_size_(max_table_size),
          read_only_(read_only),
          in_sorting_(false),
          bg_thread_(NULL)
{
    assert(table_path_.length() > 0);
    assert(table_name_.length() > 0);
}

Table::~Table()
{
    if (bg_thread_) {
        bg_thread_->join();
        delete bg_thread_;
        bg_thread_ = NULL;
    }
    if (tmp_index_file_writer_) {
        delete tmp_index_file_writer_;
        tmp_index_file_writer_ = NULL;
    }
    if (data_file_reader_writer_) {
        delete data_file_reader_writer_;
        data_file_reader_writer_ = NULL;
    }
}

int Table::LoadSortedIndex()
{
    //
    // TODO 用mmap直接加载到内存效率更高
    //
    PosixRandomAccessFile *index_file_reader = NULL;
    // 加载排序后索引
    int ret = PosixEnv::NewRandomAccessFile(index_file_path_, &index_file_reader);
    if (ret < 0) {
        UB_LOG_WARNING("PosixEnv::NewRandomAccessFile failed![ret:%d]", ret);
        return -1;
    }

    ssize_t index_file_size = index_file_reader->FileSize();
    if (index_file_size < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::FileSize failed!");
        return -1;
    }

    int index_item_num = index_file_size / sizeof(struct aodb_index);
    sorted_index_array_.reserve(index_item_num);

    struct aodb_index prev_aodb_index;
    prev_aodb_index.key_sign = 0x0;
    for (int i=0; i<index_item_num; ++i) {
        // 建立内存索引
        struct aodb_index aodb_index;
        ret = index_file_reader->Read(i*sizeof(struct aodb_index), sizeof(struct aodb_index), &aodb_index);
        if (ret < 0) {
            UB_LOG_WARNING("index_file_reader->Read failed![ret:%d][item:%d]", ret, i);
            return -1;
        }
        assert(prev_aodb_index < aodb_index);
        prev_aodb_index = aodb_index;
        sorted_index_array_.push_back(aodb_index);
    }
    UB_LOG_DEBUG("LoadSortedIndex success![item:%ld]", sorted_index_array_.size());
    return 0;
}

int Table::LoadTmpIndex()
{
    // 加载临时索引文件
    int ret = PosixEnv::NewRandomAccessFile(tmp_index_file_path_, &tmp_index_file_writer_);
    if (ret < 0) {
        UB_LOG_WARNING("PosixEnv::NewRandomAccessFile failed![ret:%d]", ret);
        return -1;
    }

    ssize_t index_file_size = tmp_index_file_writer_->FileSize();
    if (index_file_size < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::FileSize failed!");
        return -1;
    }

    int index_item_num = index_file_size / sizeof(struct aodb_index);
    for (int i=0; i<index_item_num; ++i) {
        // 建立内存索引
        struct aodb_index aodb_index;
        ret = tmp_index_file_writer_->Read(i*sizeof(struct aodb_index), sizeof(struct aodb_index), &aodb_index);
        if (ret < 0) {
            UB_LOG_WARNING("PosixRandomAccessFile::Read failed![ret:%d]", ret);
            return -1;
        }
        UpdateIndexDict(aodb_index);
    }
    UB_LOG_DEBUG("LoadTmpIndex success![item:%ld]", index_dict_.size());
    return 0;
}

int Table::Initialize()
{
    index_file_path_.append(table_path_);
    index_file_path_.append("/");
    index_file_path_.append(table_name_);
    index_file_path_.append(".ind");

    tmp_index_file_path_ = index_file_path_;
    tmp_index_file_path_.append(".tmp");

    data_file_path_.append(table_path_);
    data_file_path_.append("/");
    data_file_path_.append(table_name_);
    data_file_path_.append(".data");

    int ret = -1;

    // 加载索引
    if (read_only_) {
        ret = LoadSortedIndex();
        if (ret < 0) {
            UB_LOG_WARNING("LoadSortedIndex failed![ret:%d]", ret);
            return -1;
        }
    } else {
        ret = LoadTmpIndex();
        if (ret < 0) {
            UB_LOG_WARNING("LoadTmpIndex failed![ret:%d]", ret);
            return -1;
        }
    }

    // 加载数据
    ret = PosixEnv::NewRandomAccessFile(data_file_path_, &data_file_reader_writer_);
    if (ret < 0) {
        UB_LOG_WARNING("PosixEnv::NewRandomAccessFile failed![ret:%d][file:%s]",
                        ret, data_file_path_.c_str());
        return -1;
    }

    UB_LOG_TRACE("Table::Initialize success!");
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
    // 上面给出的结果应该是有序的(std::map的实现相关)，std::sort再确认一下，如果有序不会消耗时间
    std::sort(sorted_index_array_.begin(), sorted_index_array_.end());
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

int Table::GetIndexFromSortedIndex(uint64_t key_sign, struct aodb_index* aodb_index)
{
    // binary search
    struct aodb_index value;
    value.key_sign = key_sign;
    std::vector<struct aodb_index>::const_iterator iter = 
        std::lower_bound(sorted_index_array_.begin(), 
                         sorted_index_array_.end(),
                         value);
    if (iter == sorted_index_array_.end() || iter->key_sign != key_sign) {
        UB_LOG_DEBUG("got nothing![read_only:true][key_sign:%lx]", key_sign);
        return 0;
    }
    *aodb_index = *iter;
    assert(aodb_index->key_sign == key_sign);
    return 1;
}

int Table::GetIndexFromDict(uint64_t key_sign, struct aodb_index* aodb_index)
{
    boost::mutex::scoped_lock index_dict_lock(index_dict_lock_);
    std::map<uint64_t, struct aodb_index>::const_iterator iter = index_dict_.find(key_sign);
    if (iter == index_dict_.end()) {
        UB_LOG_DEBUG("got nothing![read_only:false][key_sign:%lx]", key_sign);
        return 0;
    }
    *aodb_index = iter->second;
    assert(aodb_index->key_sign == key_sign);
    return 1;
}

int Table::GetIndex(const std::string& key, struct aodb_index* aodb_index)
{
    assert(key.length() > 0);
    assert(aodb_index);

    uint64_t key_sign = 0;
    PosixEnv::CalcMd5_64(key, &key_sign);
    ub_log_pushnotice("key_sign", "%lu", key_sign);

    if (read_only_) {
        return GetIndexFromSortedIndex(key_sign, aodb_index);
    }
    return GetIndexFromDict(key_sign, aodb_index);
}

int Table::Get(const std::string& key, std::string* value)
{
    assert(0 != key.length());
    assert(0 == value->length());

    // 查找索引
    struct aodb_index aodb_index;
    PosixEnv::CalcMd5_64(key, &aodb_index.key_sign);
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
    ret = data_file_reader_writer_->Read(aodb_index.block_offset, sizeof(header), &header);
    if (ret < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::Read header failed![ret:%d]", ret);
        return -1;
    }

    // 检查数据头
    assert(header.magic_num == MAGIC_NUM);

    std::string save_key;
    ret = data_file_reader_writer_->Read(aodb_index.block_offset + sizeof(header), \
                                header.key_length, &save_key);
    if (ret < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::Read failed![ret:%d][size:%u]", ret, header.value_length);
        return -1;
    }
    if (save_key != key) {
        UB_LOG_WARNING("hash conflict![key_sign:%lx][key1:%s][key2:%s]", 
                       aodb_index.key_sign, key.c_str(), save_key.c_str());
        return -1;
    }

    // 读value
    ret = data_file_reader_writer_->Read(aodb_index.block_offset + sizeof(header) + header.key_length, \
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
    assert(index_dict_.size() <= max_table_size_);

    //  写入数据
    ssize_t offset = data_file_reader_writer_->Append(data);
    if (offset < 0) {
        UB_LOG_WARNING("PosixRandomAccessFile::Append failed![ret:%ld]", offset);
        return -1;
    }

    // 写入索引
    struct aodb_index aodb_index;
    PosixEnv::CalcMd5_64(key, &aodb_index.key_sign);
    aodb_index.block_offset = offset;
    aodb_index.padding = 0x0;

    offset = tmp_index_file_writer_->Append(reinterpret_cast<char*>(&aodb_index), \
                                      sizeof(aodb_index));
    if (-1 == offset) {
        UB_LOG_WARNING("PosixRandomAccessFile::Append failed!");
        return -1;
    }
    UpdateIndexDict(aodb_index);
    UB_LOG_DEBUG("Table::Put[key:%s][key_sign:%lx][block_offset:%lu][value_length:%ld]", 
            key.c_str(), aodb_index.key_sign, aodb_index.block_offset, value.length());
    return 0;
}

void Table::MarkAsReadOnly()
{
    assert(!read_only_);
    assert(!in_sorting_);

    // 注意，不应该影响正常的服务
    in_sorting_ = true;
    SortIndex();
    read_only_ = true;
    in_sorting_ = false;

    boost::mutex::scoped_lock index_dict_lock(index_dict_lock_);
    index_dict_.clear();
}

int Table::SaveSortedIndex()
{
    PosixRandomAccessFile *aodb_index_file = NULL;
    int ret = PosixEnv::NewRandomAccessFile(index_file_path_, &aodb_index_file);
    if (ret < 0) {
        UB_LOG_WARNING("PosixEnv::NewRandomAccessFile failed![ret:%d][file:%s]", 
                       ret, index_file_path_.c_str());
        return -1;
    }
    BOOST_FOREACH(struct aodb_index& aodb_index, sorted_index_array_) {
        aodb_index_file->Append(reinterpret_cast<char*>(&aodb_index), \
                                      sizeof(aodb_index));
    }
    // 删除临时索引文件
    ret = unlink(tmp_index_file_path_.c_str());
    assert(0 == ret);
    UB_LOG_DEBUG("SaveSortedIndex success!");
    return 0;
}

void Table::BgThread()
{
    ub_log_initthread("table_bg_thread");
    UB_LOG_WARNING("Thread::BgThread start!");
    MarkAsReadOnly();
    int ret = SaveSortedIndex();
    if (ret < 0) {
        UB_LOG_FATAL("SaveSortedIndex failed![ret:%d]", ret);
    }
    UB_LOG_WARNING("Thread::BgThread finish!");
}

void Table::RunBgWorkThread()
{
    // bg thread
    if (bg_thread_) {
        delete bg_thread_;
        bg_thread_ = NULL;
    }
    bg_thread_ = new boost::thread(boost::bind(&Table::BgThread, this));
    assert(bg_thread_);
}

