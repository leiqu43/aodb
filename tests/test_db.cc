/*
 * =====================================================================================
 *
 *       Filename:  test_db.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/14/2013 02:06:46 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include "db.h"

using namespace aodb;

class DbTest: public ::testing::Test
{
protected:
    static void SetUpTestCase() {
    }

    static void TearDownTestCase() {
    }
};

TEST_F(DbTest, Open) {
    Db *db = NULL;
    int ret = Db::OpenDb("./tmp/", "unittest", 3, 0x10000000, 1, &db);
    ASSERT_EQ(ret, 0);

    delete db;
    db = NULL;
}

TEST_F(DbTest, Put) {
    Db *db = NULL;
    int ret = Db::OpenDb("./tmp/", "unittest", 3, 0x10000000, 1, &db);
    ASSERT_EQ(ret, 0);

    ret = db->Put("hello", "hello,world!");
    ASSERT_EQ(ret, 1);

    delete db;
    db = NULL;
}

TEST_F(DbTest, Get) {
    Db *db = NULL;
    int ret = Db::OpenDb("./tmp/", "unittest", 3, 0x10000000, 1, &db);
    ASSERT_EQ(ret, 0);

    std::string data;
    ret = db->Get("hello", &data);
    ASSERT_EQ(ret, 1);
    ASSERT_EQ(data, "hello,world!");

    delete db;
    db = NULL;
}

TEST_F(DbTest, PressureTest) {
    Db *db = NULL;
    int ret = Db::OpenDb("./tmp/", "unittest", 3, 0x10000000, 1, &db);
    ASSERT_EQ(ret, 0);

    std::string data;
    for (int i=0; i<32*1024; ++i) {
        data.append("hello,world");
    }
    char buf[32] = "\0";
    for (int i=0; i<1000; ++i) {
        snprintf(buf, sizeof(buf), "%d", i);
        ret = db->Put(buf, data);
        ASSERT_EQ(ret, 1);
    }
    for (int i=0; i<1000; ++i) {
        snprintf(buf, sizeof(buf), "%d", i);
        std::string value;
        ret = db->Get(buf, &value);
        ASSERT_EQ(ret, 1);
        ASSERT_EQ(data.length(), value.length());
    }
    delete db;
    db = NULL;
}

