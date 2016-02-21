/*
 * =====================================================================================
 *
 *       Filename:  db_mgr.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/18/2013 03:31:07 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 *
 * =====================================================================================
 */

#define DB_MGR_H_

#include <pthread.h>
#include <string>
#include <set>
#include <map>
#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

namespace aodb {

class Db;

//
// DB manager
//
//
class DbMgr{
public:
    static DbMgr* instance();

    //
    // Init DB Manager
    //
    int Initialize(const std::string& db_path, int db_max_open_table, int db_devide_table_period);

    //
    // Release
    //
    void Release();

    //
    // Get DB based on name
    //
    Db* GetDb(const std::string& db_name, bool new_if_not_exist=false);

private:
    DbMgr();
    virtual ~DbMgr();

    //
    // Add a new DB
    //
    Db* AddDb(const std::string& db_name);

private:
    //DB List
    typedef std::map<std::string, Db*> DbDict;
    typedef DbDict::const_iterator DbDictIterator;
    DbDict db_dict_;
    boost::mutex db_dict_lock_;

    // DbMgr mutex
    boost::mutex lock_;

    // DB path
    std::string db_path_;
    int db_max_open_table_;
    int db_devide_table_period_;

    // singleton
    static DbMgr* instance_;
    static boost::mutex instance_lock_;
};
}

