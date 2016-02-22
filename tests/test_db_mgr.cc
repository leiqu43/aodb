/*
 * =====================================================================================
 *
 *       Filename:  test_db_mgr.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/18/2013 03:54:00 PM
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
#include "db_mgr.h"

using namespace aodb;

class DbMgrTest: public ::testing::Test
{
protected:
    static void SetUpTestCase() {
    }

    static void TearDownTestCase() {
    }
};

TEST_F(DbMgrTest, Initialize) {
    DbMgr::instance()->Initialize("./tmp/", 5, 1);
}

TEST_F(DbMgrTest, GetDb) {
    Db* db = DbMgr::instance()->GetDb("unittest");
    ASSERT_TRUE(db);

    db = DbMgr::instance()->GetDb("unittest123123");
    ASSERT_FALSE(db);

    db = DbMgr::instance()->GetDb("unittest_new", 1);
    ASSERT_TRUE(db);
}
