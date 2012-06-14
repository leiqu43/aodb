/*
 * =====================================================================================
 *
 *       Filename:  test_db.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/14/2012 02:06:46 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
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
    int ret = Db::OpenDb("./tmp/", 7, 1, &db);
    ASSERT_EQ(ret, 0);
}

