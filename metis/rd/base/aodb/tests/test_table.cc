/*
 * =====================================================================================
 *
 *       Filename:  test_table.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/12/2012 11:50:45 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include "table.h"

using namespace aodb;

class TableTest: public ::testing::Test
{
protected:
    static void SetUpTestCase() {
        unlink("./tmp/test_table.ind");
        unlink("./tmp/test_table.data");
    }

    static void TearDownTestCase() {
    }
};

TEST_F(TableTest, Open) {
    Table* table = NULL;
    int ret = Table::Open("./tmp/", "test_table", 0x10000000, false, &table);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(table);
    delete table;
    table = NULL;
}

TEST_F(TableTest, Put) {
    Table* table = NULL;
    int ret = Table::Open("./tmp/", "test_table", 0x10000000, false, &table);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(table);

    ret = table->Put("hello", "hello,world!");
    ASSERT_EQ(ret, 0);

    std::string data;
    ret = table->Get("hello", &data);
    ASSERT_EQ(ret, 1);
    ASSERT_EQ(data, "hello,world!");

    data.clear();
    ret = table->Put("hello", "hello,world!123123");
    ASSERT_EQ(ret, 0);

    data.clear();
    ret = table->Get("hello", &data);
    ASSERT_EQ(ret, 1);
    ASSERT_EQ(data, "hello,world!123123");

    delete table;
    table = NULL;

    // 重新打开
    ret = Table::Open("./tmp/", "test_table", 0x10000000, true, &table);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(table);

    data.clear();
    ret = table->Get("hello", &data);
    ASSERT_EQ(ret, 1);
    ASSERT_EQ(data, "hello,world!123123");

    delete table;
    table = NULL;
}

TEST_F(TableTest, PressureTest) {
    Table* table = NULL;
    int ret = Table::Open("./tmp/", "test_table", 0x10000000, false, &table);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(table);

    std::string data;
    for (int i=0; i<50*1024; ++i) {
        data.append("c");
    }
    char key[64] = "\0";
    for(int i=0; i<10000; ++i) {
        snprintf(key, sizeof(key), "alskfjalj%d%d", i, i);
        ret = table->Put(key, data);
        ASSERT_EQ(ret, 0);
    }

    for (int i=0; i<10000; ++i) {
        int id = rand() % 10000;
        snprintf(key, sizeof(key), "alskfjalj%d%d", id, id);
        std::string data_read;
        ret = table->Get(key, &data_read);
        ASSERT_EQ(ret, 1);
        ASSERT_EQ(data, data_read);
    }

    // 重复写入
    for(int i=0; i<10000; ++i) {
        snprintf(key, sizeof(key), "alskfjalj%d%d", i, i);
        ret = table->Put(key, data);
        ASSERT_EQ(ret, 0);
    }

    for (int i=0; i<10000; ++i) {
        int id = rand() % 10000;
        snprintf(key, sizeof(key), "alskfjalj%d%d", id, id);
        std::string data_read;
        ret = table->Get(key, &data_read);
        ASSERT_EQ(ret, 1);
        ASSERT_EQ(data, data_read);
    }


    table->MarkAsReadOnly();

    for (int i=0; i<10000; ++i) {
        int id = rand() % 10000;
        snprintf(key, sizeof(key), "alskfjalj%d%d", id, id);
        std::string data_read;
        ret = table->Get(key, &data_read);
        ASSERT_EQ(ret, 1);
        ASSERT_EQ(data, data_read);
    }

    delete table;
    table = NULL;
}
