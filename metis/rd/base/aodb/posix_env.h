/*
 * =====================================================================================
 *
 *       Filename:  posix_env.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/12/2013 10:22:50 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 *
 * =====================================================================================
 */

#ifndef AODB_POSIX_ENV_H_
#define AODB_POSIX_ENV_H_

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <dirent.h>
#include <openssl/md5.h>
#include <vector>
#include <boost/thread/mutex.hpp>
#include "ub_log.h"

namespace aodb {

inline size_t min(size_t x, size_t y)
{
    return x > y ? y : x;
}

class PosixRandomAccessFile {
public:
    const static size_t kRandomAccessBufferSize = 128 * 1024;

    PosixRandomAccessFile(const std::string& filename, int fd)
        : filename_(filename), fd_(fd) {
    }

    virtual ~PosixRandomAccessFile() {
        close(fd_);
    }

    //
    // 从指定偏移读取指定的数据
    //
    int Read(uint64_t offset, size_t n, std::string* result) {
        result->clear();
        size_t total_read = 0;
        char tmp_buf[kRandomAccessBufferSize] = "\0";
        while (total_read < n) {
            ssize_t r = pread(fd_, tmp_buf, min(n - total_read, kRandomAccessBufferSize), 
                              offset + total_read);
            if (r <= 0) {
                UB_LOG_WARNING("pread failed![err:%m][ret:%d]", static_cast<int>(r));
                return -1;
            }
            result->append(tmp_buf, r);
            total_read += r;
        }
        assert(total_read == n);
        return 0;
    }

    int Read(uint64_t offset, size_t n, void *result) {
        assert(result);
        boost::mutex::scoped_lock lock(lock_);
        ssize_t r = pread(fd_, result, n, offset);
        if (r <= 0) {
            UB_LOG_WARNING("pread failed![err:%m]");
            return -1;
        }
        assert(static_cast<size_t>(r) == n);
        return 0;
    }

    //
    // 把数据追加写入到文件末尾
    //
    ssize_t Append(const std::string& data) {
        return Append(data.c_str(), data.length());
    }

    ssize_t Append(const void* data, size_t size) {
        boost::mutex::scoped_lock lock(lock_);
        size_t offset = lseek(fd_, 0, SEEK_END);
        ssize_t w = pwrite(fd_, data, size, offset);
        if (w < 0) {
            UB_LOG_WARNING("pwrite failed![err:%m][ret:%d]", static_cast<int>(w));
            return -1;
        }
        UB_LOG_DEBUG("Append data success![offset:%lu][size:%lu][next_offset:%lu]", 
                     offset, size, offset + size);
        assert(static_cast<size_t>(w) == size);
        return offset;
    }

    //
    // 返回文件的大小
    //
    ssize_t FileSize() {
        return lseek(fd_, 0, SEEK_END);
    }

private:
    std::string filename_;
    int fd_;
    boost::mutex lock_;
};

class PosixEnv {
public:
    PosixEnv() {}
    virtual ~PosixEnv() {}

    static int NewRandomAccessFile(const std::string& filename,
                                    PosixRandomAccessFile** result) {
        assert(result);
        assert(!(*result));
        int fd = open(filename.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd < 0) {
            UB_LOG_WARNING("NewRandomAccessFile faild![filename:%s]", filename.c_str());
            *result = NULL;
            return -1;
        }
        *result = new PosixRandomAccessFile(filename, fd);
        assert(*result);
        return 0;
    }

    static void CalcMd5(const std::string& data, uint64_t* high, uint64_t* low) {
        MD5_CTX ctx;
        unsigned char buf[16] = "\0";
        MD5_Init(&ctx);
        MD5_Update(&ctx, data.c_str(), data.size());
        MD5_Final(buf, &ctx);
        *high = (uint64_t)buf[0]<<56 | (uint64_t)buf[1]<<48 | (uint64_t)buf[2]<<40 | (uint64_t)buf[3]<<32 \
                | (uint64_t)buf[4] << 24 | (uint64_t)buf[5] << 16 | (uint64_t)buf[6] << 8 | (uint64_t)buf[7];
        *low = (uint64_t)buf[8]<<56| (uint64_t)buf[9]<<48 | (uint64_t)buf[10]<<40 | (uint64_t)buf[11]<<32 \
               | (uint64_t)buf[12] << 24 | (uint64_t)buf[13] << 16 | (uint64_t)buf[14] << 8 | (uint64_t)buf[15];
    }

    static void CalcMd5_64(const std::string& data, uint64_t* sign) {
        uint64_t high = 0, low = 0;
        CalcMd5(data, &high, &low);
        *sign = high ^ low;
    }

    static void CalcMd5_32(const std::string& data, uint32_t* sign) {
        uint64_t sign64 = 0;
        CalcMd5_64(data, &sign64);
        *sign = (sign64 >> 32) ^ (sign64 & 0xffffffff);
    }

    static int GetDirChildren(const std::string& dir,
                           std::vector<std::string>* result,
                           uint8_t d_type_filter=0)  {
        assert(result);
        result->clear();
        DIR* d = opendir(dir.c_str());
        if (d == NULL) {
            return 0;
        }   
        struct dirent* entry = NULL;
        while ((entry = readdir(d)) != NULL) {
            if (entry->d_name[0] == '.') continue;  // 去掉临时文件以及".",".."目录
            if (d_type_filter != 0 && entry->d_type != d_type_filter) continue;
            result->push_back(entry->d_name);
        }   
        closedir(d);
        return 0;
    }

    static int CreateDir(const std::string& path) {
        int ret = mkdir(path.c_str(), 0755);
        if (EEXIST == errno) {
            return 0;
        }
        return ret;
    }
};

}

#endif
