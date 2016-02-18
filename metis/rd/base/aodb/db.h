/*
 * =====================================================================================
 *
 *       Filename:  db.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2013 11:24:18 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 *
 * =====================================================================================
 */

#ifndef AODB_DB_H_
#define AODB_DB_H_

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <list>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread.hpp>

namespace aodb {

class Table;

struct table_info {
    int32_t table_time;
    bool read_only;
    bool operator<(const struct table_info& y) const {
        return this->table_time < y.table_time;
    }
};

class Db {
public:
    //
    // ��һ��DB
    // db_path  :   �����ļ���ַ
    // max_open_table : ���򿪵ı����������ʱ������ı�
    // devide_table_period : ��ֱ�����ڡ���λΪ�졿
    // result_db : �򿪵�db���
    //
    static int OpenDb(const std::string& path, 
                      const std::string& db_name,
                      int max_open_table, 
                      uint32_t max_table_size, 
                      int devide_table_period,
                      Db** result_db);

    virtual ~Db() {
    }

    //
    // ��DB�л�ȡ����
    //
    int Get(const std::string& key, std::string* value);

    //
    // д�����ݵ�DB��
    //
    int Put(const std::string& key, const std::string& value);

    //
    // ����db������
    //
    const std::string& DbName() const {
        return db_name_;
    }

private:
    Db(const std::string& db_path, 
       const std::string& db_name, 
       const uint32_t max_open_table, 
       const uint32_t max_table_size, 
       const int devide_table_period) 
        : db_path_(db_path), 
          db_name_(db_name),
          max_open_table_(max_open_table),
          max_table_size_(max_table_size),
          devide_table_period_(devide_table_period) {
    }

    // 
    // ��ʼ��Db
    //
    int Initialize();

    //
    // ɨ����������ı����Ұ���ʱ���������������
    //
    int ScanTable(std::vector<struct table_info>* tables);

    //
    // ����ָ����tables��
    //
    int LoadTables(const std::vector<struct table_info>& tables);

    //
    // ��ȡ���б�������������ʱ��δ�������������
    // ע�⣺�õ������б�ֻ���������ж�������
    //
    int GetAllTables(std::vector<boost::shared_ptr<Table> > *result_tables);

    //
    // ���ݱ����Ƶõ������Ϣ
    // retval:
    //      <0  :   ʧ��
    //      0   :   ���Ǳ�
    //      1   :   �ɹ�
    //
    int GetTableInfoFromTableName(const std::string& table_name, struct table_info *result);

    int NewTableWithTableLock();

private:
    // ��������д������д��˱�
    boost::shared_ptr<Table> primary_table_;
    boost::mutex primary_table_lock_;

    // ����ֻ��
    std::list<boost::shared_ptr<Table> > tables_list_;
    boost::mutex tables_list_lock_;

    // db·��&����
    const std::string db_path_;
    const std::string db_name_;

    const int max_open_table_;
    const uint32_t max_table_size_;
    const int devide_table_period_;
};

}

#endif

