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
    }

    static void TearDownTestCase() {
    }
};

TEST_F(TableTest, Open) {
    Table* table = NULL;
    int ret = Table::Open("/tmp/", "test_table", 0x10000000, &table);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(table);
    delete table;
    table = NULL;
}

TEST_F(TableTest, Put) {
    Table* table = NULL;
    int ret = Table::Open("/tmp/", "test_table", 0x10000000, &table);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(table);

    ret = table->Put("hello", "hello,world!");
    ASSERT_EQ(ret, 0);

    std::string data;
    ret = table->Get("hello", &data);
    ASSERT_EQ(ret, 0);
    std::cout << "data:" << data << std::endl;

    delete table;
    table = NULL;
}




