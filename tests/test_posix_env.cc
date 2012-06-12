/*
 * =====================================================================================
 *
 *       Filename:  test_posix_env.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/12/2012 10:57:31 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include "posix_env.h"

using namespace aodb;

class PosixEnvTest: public ::testing::Test
{
protected:
    static void SetUpTestCase() {
    }

    static void TearDownTestCase() {
    }
};

TEST_F(PosixEnvTest, NewRandomAccessFile) {
    PosixRandomAccessFile *random_access_file = NULL;
    int ret = PosixEnv::NewRandomAccessFile("/tmp/aodb_unittest_tmp", &random_access_file);
    ASSERT_EQ(ret, 0);

    std::string buf;
    ret = random_access_file->Read(123123, 1024, &buf);
    ASSERT_LE(ret, 0);

    for (int i=0; i<32*1024; ++i) {
        buf.append("hello,world");
    }

    ret = random_access_file->Append(buf);
    ASSERT_EQ(ret, 0);

    std::string tmp;
    for (int i=0; i<32*1024; ++i) {
        ret = random_access_file->Read(i*11, 11, &tmp);
        ASSERT_EQ(ret, (int)0);
        ASSERT_EQ(tmp.length(), (size_t)11);
        ASSERT_EQ(tmp, "hello,world");
    }
    ret = random_access_file->Read(0, 11*32*1024, &tmp);
    ASSERT_EQ(ret, (int)0);
    ASSERT_EQ(tmp.length(), (size_t)11*32*1024);

    delete random_access_file;
    random_access_file = NULL;
}

