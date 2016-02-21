/*
 * =====================================================================================
 *
 *       Filename:  db_imp.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/18/2013 02:48:32 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 * =====================================================================================
 */

#include "db_imp.h"
#include <string>
#include "mdb.pb.h"
#include "db_mgr.h"
#include "db.h"

using namespace std;
using namespace MdbInterface;
using namespace aodb;

//
// Put data into DB
//
GenericResErr aodb_put(GenericReq *req, google::protobuf::Message **res_msg, thread_data_t *pthr) 
{
    assert(req);
    assert(res_msg);
    assert(pthr);
    int ret = -1;

    DbPutRequest request;
    ret = request.ParseFromString(req->body());
    if (false == ret) {
        UB_LOG_WARNING("DbPutRequest::ParseFromString failed![req_body_size:%lu]", req->body().size());
        return ERR_ARG;
    }

    ub_log_pushnotice("keys num", "%lu", request.key_value_list_size());

    for (int i=0; i<request.key_value_list_size(); ++i) {
        const KeyValuePair *pair = &request.key_value_list(i);
        if (0 == pair->key().length() || 0 == pair->value().length()) {
            UB_LOG_DEBUG("invalid request!");
            return ERR_ARG;
        }
    }

    if (request.db_name().size()) {
        Db *db = DbMgr::instance()->GetDb(request.db_name(), true);
        if (!db) {
            UB_LOG_WARNING("DbMgr::GetDb failed![db:%s]", request.db_name().c_str());
            return ERR_INTER;
        }
        for (int i=0; i<request.key_value_list_size(); ++i) {
            const KeyValuePair *pair = &request.key_value_list(i);
            assert(pair);
            if (pair->has_db()) {
                continue;
            }
            ret = db->Put(pair->key(), pair->value());
            if (ret < 0) {
                UB_LOG_WARNING("Db::Put failed![ret:%d]", ret);
                return ERR_INTER;
            }
            UB_LOG_DEBUG("put key success![key:%s][value_size:%lu]", pair->key().c_str(), pair->value().size());
        }
    }

    for (int i=0; i<request.key_value_list_size(); ++i) {
        const KeyValuePair *pair = &request.key_value_list(i);
        assert(pair);
        if (!pair->has_db()) {
            continue;
        }
        Db *db = DbMgr::instance()->GetDb(pair->db(), true);
        assert(db);
        ret = db->Put(pair->key(), pair->value());
        if (ret < 0) {
            UB_LOG_WARNING("Db::Put failed![ret:%d]", ret);
            return ERR_INTER;
        }
        UB_LOG_DEBUG("put key success![db:%s][key:%s][value_size:%lu]", pair->db().c_str(), 
                     pair->key().c_str(), pair->value().size());
    }
    return ERR_OK;
}

//
// Get data from DB
//
//
GenericResErr aodb_get(GenericReq *req, google::protobuf::Message **res_msg, thread_data_t *pthr) 
{
    int ret = -1;

    DbGetRequest request;
    ret = request.ParseFromString(req->body());
    if (false == ret) {
        UB_LOG_WARNING("ParseFromString failed!");
        return ERR_ARG;
    }

    DbGetResponse *response = new DbGetResponse();
    assert(response);
    if (request.db_name().size()) {
        Db *db = DbMgr::instance()->GetDb(request.db_name(), false);
        if (!db) {
            UB_LOG_WARNING("can't find db:%s", request.db_name().c_str());
            return ERR_INTER;
        }
        for (int i=0; i<request.key_list_size(); ++i) {
            const std::string& key = request.key_list(i);
            std::string data;
            ret = db->Get(key, &data);
            if (ret < 0) {
                UB_LOG_WARNING("Db:Get failed![ret:%d][db:%s][key:%s]", ret, 
                               request.db_name().c_str(), key.c_str());
                return ERR_INTER;
            }
            if (data.length() > 0)  {
                KeyValuePair *key_value = response->add_key_value_list();
                assert(key_value);
                key_value->set_key(key.c_str());
                key_value->set_value(data);
            } else {
                response->add_failed_list(key.c_str());
            }
            UB_LOG_DEBUG("DbHandler::Get success![db_name:%s][key:%s][value_size:%ld]", \
                    request.db_name().c_str(), key.c_str(), data.length());
        }
        db = NULL;
    }
    for (int i=0; i<request.db_key_list_size(); ++i) {
        const DbKey &db_key = request.db_key_list(i);
        Db *db = DbMgr::instance()->GetDb(db_key.db().c_str(), false);
        if (!db) {
            UB_LOG_DEBUG("can't find db![db:%s]", db_key.db().c_str());
            response->add_failed_list(db_key.key());
            continue;
        }
        std::string data;
        ret = db->Get(db_key.key(), &data);
        if (ret < 0) {
            UB_LOG_WARNING("Db::Get failed![ret:%d][db:%s][key:%s]", ret, db_key.db().c_str(), db_key.key().c_str());
            return ERR_INTER;
        }
        if (data.length() > 0)  {
            KeyValuePair *key_value = response->add_key_value_list();
            assert(key_value);
            key_value->set_key(db_key.key());
            key_value->set_value(data);
        } else {
            response->add_failed_list(db_key.key());
        }
        UB_LOG_DEBUG("DbHandler::Get success![db:%s][key:%s][value_size:%lu]", \
                db_key.db().c_str(), db_key.key().c_str(), data.length());
        db = NULL;
    }

    *res_msg = response;
    UB_LOG_DEBUG("aodb_get succes!");
    return ERR_OK;
}

