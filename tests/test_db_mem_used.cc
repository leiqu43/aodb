/*
 * =====================================================================================
 *
 *       Filename:  test_db_mem_used.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2013年06月27日 10时04分51秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#include <ub_log.h>
#include "db.h"

using namespace aodb;

int main(void) 
{
    ub_log_init("./log/", "unittest.", 0x10000000, 8);
    Db* db = NULL;

    int ret = Db::OpenDb("./tmp/", "test_db", 16, 1000000, 1, &db);
    if (ret < 0) {
        UB_LOG_WARNING("Db::OpenDb failed!");
        return -1;
    }

    for (int i=0; i<1; ++i) {
        for (int id=0; id<10000000; ++id) {
            char key[128];
            snprintf(key, sizeof(key), "LKAJLKJLKJLKJLKJLKJLKJLJ%d-%d", i, id);
            ret = db->Put(key, key);
            if (ret <= 0) {
                UB_LOG_WARNING("Db::Put failed![ret:%d]", ret);
                return -1;
            }

            std::string value;
            ret = db->Get(key, &value);
            if (ret <= 0) {
                UB_LOG_WARNING("Db::Get failed![ret:%d][id:%d]", ret, id);
                return -1;
            }
            assert(key == value);
        }
    }
    for (int id=0; id<10000000; ++id) {
        char key[128];
        snprintf(key, sizeof(key), "LKAJLKJLKJLKJLKJLKJLKJLJ%d-%d", 0, id);
        std::string value;
        ret = db->Get(key, &value);
        assert(1 == ret);
        assert(value == key);
    }
    return 0;
}

