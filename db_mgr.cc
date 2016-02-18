/*
 * =====================================================================================
 *
 *       Filename:  db_mgr.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/18/2013 03:36:40 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 *
 * =====================================================================================
 */

#include "db_mgr.h"
#include <vector>
#include <string>
#include <boost/foreach.hpp>
#include "posix_env.h"
#include "db.h"

using namespace aodb;

DbMgr* DbMgr::instance_ = NULL;
boost::mutex DbMgr::instance_lock_;

DbMgr* DbMgr::instance()
{
    if (!instance_) {
        boost::mutex::scoped_lock instance_lock(instance_lock_);
        if (!instance_) {
            instance_ = new DbMgr;
            assert(instance_);
        }
    }
    assert(instance_);
    return instance_;
}

int DbMgr::Initialize(const std::string& db_path, 
                      int db_max_open_table, 
                      int db_devide_table_period)
{
    db_path_ = db_path;
    db_max_open_table_ = db_max_open_table;
    db_devide_table_period_ = db_devide_table_period;

    std::vector<std::string> db_list;
    int ret = PosixEnv::GetDirChildren(db_path_, &db_list, DT_DIR);
    if (ret < 0) {
        UB_LOG_WARNING("PosixEnv::GetDirChildren failed![ret:%d]", ret);
        return -1;
    }

    BOOST_FOREACH(const std::string& db_name, db_list) {
        if (!AddDb(db_name)) {
            UB_LOG_WARNING("AddDb failed![db_name:%s]", db_name.c_str());
            return -1;
        }
    }
    return 0;
}

void DbMgr::Release()
{
}

Db* DbMgr::AddDb(const std::string& db_name)
{
    Db* db = NULL;
    int ret = Db::OpenDb(db_path_, db_name, db_max_open_table_, 10000000, db_devide_table_period_, &db);
    if (ret < 0) {
        UB_LOG_WARNING("Db::OpenDb failed![ret:%d][db_name:%s]", ret, db_name.c_str());
        return NULL;
    }
    {
        boost::mutex::scoped_lock db_dict_lock(db_dict_lock_);
        db_dict_.insert(std::make_pair(db_name, db));
    }
    return db;
}

Db* DbMgr::GetDb(const std::string& db_name, bool new_if_not_exist)
{
    Db* db = NULL;
    {
        boost::mutex::scoped_lock db_dict_lock(db_dict_lock_);
        DbDictIterator iter = db_dict_.find(db_name);
        if (iter == db_dict_.end()) {
            db = NULL;
        } else {
            db = iter->second;
        }
    }
    if (!db && new_if_not_exist) {
        return AddDb(db_name);
    }
    return db;
}

DbMgr::DbMgr()
{
}

DbMgr::~DbMgr()
{
    boost::mutex::scoped_lock db_dict_lock(db_dict_lock_);
    for (DbDictIterator iter = db_dict_.begin(); iter != db_dict_.end(); ++iter) {
        delete iter->second;
    }
}



