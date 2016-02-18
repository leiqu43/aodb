/*
 * =====================================================================================
 *
 *       Filename:  test_db_mem_used.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/18/2013 11:54:00 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company: 
 *
 * =====================================================================================
 */

#include <boost/thread.hpp>
#include <ub_log.h>
#include "db.h"

using namespace aodb;

void random_get_routine(Db* db, int max_id)
{
    ub_log_initthread("random_get_thread");
    while(true) {
        for (int id=0; id<max_id; id++) {
            char key[128];
            snprintf(key, sizeof(key), "%d-%d", id, id);

            std::string value;
            int ret = db->Get(key, &value);
            if (ret < 0) {
                UB_LOG_WARNING("Db::Get failed![ret:%d][id:%d]", ret, id);
                _exit(-1);
            } else if (0 == ret) {
                continue;
            }
            assert(key == value);
        }
    }
}

int main(void) 
{
    ub_log_init("./log/", "unittest.", 0x10000000, 8);
    Db* db = NULL;

    const int key_num = 100000000;

    int ret = Db::OpenDb("./tmp/", "test_db", 16, key_num/10, 1, &db);
    if (ret < 0) {
        UB_LOG_WARNING("Db::OpenDb failed!");
        return -1;
    }

    boost::thread random_get_thread(random_get_routine, db, key_num);

    printf("start finish : %d\n", (int)time(NULL));
    for (int i=0; i<1; ++i) {
        for (int id=0; id<key_num; ++id) {
            char key[128];
            snprintf(key, sizeof(key), "%d-%d", id, id);
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
    printf("insert finish : %d\n", (int)time(NULL));
    for (int id=0; id<key_num; ++id) {
        char key[128];
        snprintf(key, sizeof(key), "%d-%d", id, id);
        std::string value;
        ret = db->Get(key, &value);
        assert(1 == ret);
        if (key != value) {
            UB_LOG_WARNING("invalid value!!![value:%s]", value.c_str());
            return -1;
        }
    }
    printf("query finish: %d\n", (int)time(NULL));
    getchar();
    return 0;
}

