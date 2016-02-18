/*
 * =====================================================================================
 *
 *       Filename:  test_table.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/12/2013 11:50:45 AM
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
#include "table.h"

using namespace aodb;

class TableTest: public ::testing::Test
{
protected:
    static void SetUpTestCase() {
        unlink("./tmp/test_table.ind");
        unlink("./tmp/test_table.ind.tmp");
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
    ret = Table::Open("./tmp/", "test_table", 0x10000000, false, &table);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(table);

    data.clear();
    ret = table->Get("hello", &data);
    ASSERT_EQ(ret, 1);
    ASSERT_EQ(data, "hello,world!123123");

    delete table;
    table = NULL;
}

TEST_F(TableTest, SaveSortedIndexTest) {
    Table* table = NULL;
    int ret = Table::Open("./tmp/", "test_table", 100000, false, &table);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(table);

    char key[64] = "\0";
    for(int i=0; i<100000; ++i) {
        snprintf(key, sizeof(key), "alskfjalj%d%d", i, i);
        ret = table->Put(key, key);
        ASSERT_EQ(ret, 0);
    }
    table->RunBgWorkThread();

    delete table;
    table = NULL;

    // 重新只读方式加载数据
    ret = Table::Open("./tmp/", "test_table", 100000, true, &table);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(table);
    for(int i=0; i<100000; ++i) {
        snprintf(key, sizeof(key), "alskfjalj%d%d", i, i);
        std::string value;
        ret = table->Get(key, &value);
        ASSERT_EQ(ret, 1);
        ASSERT_EQ(key, value);
    }
}

