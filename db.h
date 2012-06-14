/*
 * =====================================================================================
 *
 *       Filename:  db.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2012 11:24:18 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zbz (), zhoubaozhou@gmail.com
 *        Company:  
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

namespace aodb {

class Table;

struct table_info {
    bool operator() (struct table_info x,struct table_info y) { 
        if (x.table_year < y.table_year) {
            return true;
        }
        if (x.table_month < y.table_month) {
            return true;
        }
        if (x.table_day < x.table_day) {
            return true;
        }
        return false;
    }
    // �������ʱ��
    int32_t table_year;
    int32_t table_month;
    int32_t table_day;
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
    static int OpenDb(const std::string& db_path, 
                      int max_open_table, 
                      int devide_table_period,
                      Db** result_db);

    virtual ~Db() {
    }

private:
    Db(const std::string& db_path, const int max_open_table, 
       const int devide_table_period) 
        : db_path_(db_path), 
          max_open_table_(max_open_table),
          devide_table_period_(devide_table_period) {
    }

    // 
    // ��ʼ��Db
    //
    int Initialize();

    //
    // ɨ����������ı����Ұ���ʱ���������������
    //
    int ScanTable(std::vector<std::string>* result_tables);

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

private:
    // ��������д������д��˱�
    boost::shared_ptr<Table> primary_table_;
    boost::mutex primary_table_lock_;

    // ����ֻ��
    std::list<boost::shared_ptr<Table> > tables_list_;
    boost::mutex tables_list_lock_;

    const std::string db_path_;
    const int max_open_table_;
    const int devide_table_period_;
};

}

#endif

