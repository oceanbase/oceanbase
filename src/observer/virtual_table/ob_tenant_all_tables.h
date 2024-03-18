/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_ALL_TABLES_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_ALL_TABLES_

#include "share/ob_virtual_table_iterator.h"
#include "common/ob_range.h"
#include "share/ob_autoincrement_param.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_define.h"
using oceanbase::common::OB_APP_MIN_COLUMN_ID;
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace common
{
class ObMySQLProxy;
class ObSqlString;
}
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace observer
{

#define NEW_TABLE_STATUS_SQL  "select /*+ leading(a) no_use_nl(ts)*/" \
                    "a.database_id as DATABASE_ID," \
                    "a.table_name as TABLE_NAME," \
                    "a.table_type as TABLE_TYPE," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE 'oceanbase' END as ENGINE," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE 0 END as VERSION," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE store_format END as ROW_FORMAT," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE cast( coalesce(ts.row_cnt,0) as unsigned) END as ROWS," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE cast( coalesce(ts.avg_row_len, 0) as unsigned) END as AVG_ROW_LENGTH," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE cast( coalesce(ts.data_size,0) as unsigned) END as DATA_LENGTH," \
                    "NULL as MAX_DATA_LENGTH," \
                    "NULL as INDEX_LENGTH," \
                    "NULL as DATA_FREE," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE inner_table_sequence_getter(effective_tenant_id(), a.table_id, a.autoinc_column_id) END as AUTO_INCREMENT," \
                    "a.gmt_create as CREATE_TIME," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE a.gmt_modified END as UPDATE_TIME," \
                    "NULL as CHECK_TIME," \
                    "CASE a.collation_type " \
                    "WHEN 45 THEN 'utf8mb4_general_ci' " \
                    "WHEN 46 THEN 'utf8mb4_bin' " \
                    "WHEN 63 THEN 'binary' " \
                    "ELSE NULL END as COLLATION," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE 0 END as CHECKSUM," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE inner_table_option_printer(effective_tenant_id(), a.database_id, a.table_id) END as CREATE_OPTIONS," \
                    "CASE a.table_type WHEN 4 THEN 'VIEW' ELSE a.comment END as COMMENT " \
                    "from " \
                    "(" \
                    "select tenant_id," \
                    "database_id," \
                    "table_id," \
                    "table_name, " \
                    "table_type," \
                    "gmt_create," \
                    "gmt_modified," \
                    "store_format," \
                    "CASE table_type WHEN 4 THEN NULL ELSE collation_type END as collation_type," \
                    "comment," \
                    "autoinc_column_id " \
                    "from oceanbase.__all_table where table_type in (0, 1, 2, 3, 4, 14)) a " \
                    "join oceanbase.__all_database b " \
                    "on a.database_id = b.database_id " \
                    "and a.tenant_id = b.tenant_id " \
                    "left join (" \
                    "select tenant_id," \
                    "table_id," \
                    "row_cnt," \
                    "avg_row_len," \
                    "row_cnt * avg_row_len as data_size " \
                    "from oceanbase.__all_table_stat " \
                    "where partition_id = -1 or partition_id = table_id) ts " \
                    "on a.table_id = ts.table_id " \
                    "and a.tenant_id = ts.tenant_id " \
                    "and a.table_type in (0, 1, 2, 3, 4, 14) " \
                    "and b.database_name != '__recyclebin' " \
                    "and b.in_recyclebin = 0 " \
                    "and 0 = sys_privilege_check('table_acc', effective_tenant_id(), b.database_name, a.table_name) "

#define NEW_TABLE_STATUS_SQL_ORA  "select /*+ leading(a) no_use_nl(ts)*/" \
                    "a.database_id as DATABASE_ID," \
                    "a.table_name as TABLE_NAME," \
                    "a.table_type as TABLE_TYPE," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE 'oceanbase' END as ENGINE," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE 0 END as VERSION," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE store_format END as ROW_FORMAT," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE coalesce(ts.row_cnt,0) END as \"ROWS\"," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE coalesce(ts.avg_row_len, 0) END as AVG_ROW_LENGTH," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE coalesce(ts.data_size,0) END as DATA_LENGTH," \
                    "NULL as MAX_DATA_LENGTH," \
                    "NULL as INDEX_LENGTH," \
                    "NULL as DATA_FREE," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE inner_table_sequence_getter(effective_tenant_id(), a.table_id, a.autoinc_column_id) END as AUTO_INCREMENT," \
                    "a.gmt_create as CREATE_TIME," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE a.gmt_modified END as UPDATE_TIME," \
                    "NULL as CHECK_TIME," \
                    "CASE a.collation_type " \
                    "WHEN 45 THEN 'utf8mb4_general_ci' " \
                    "WHEN 46 THEN 'utf8mb4_bin' " \
                    "WHEN 63 THEN 'binary' " \
                    "ELSE NULL END as COLLATION," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE 0 END as CHECKSUM," \
                    "CASE a.table_type WHEN 4 THEN NULL ELSE inner_table_option_printer(effective_tenant_id(), a.database_id, a.table_id) END as CREATE_OPTIONS," \
                    "CASE a.table_type WHEN 4 THEN 'VIEW' ELSE a.cmt END as \"COMMENT\" " \
                    "from " \
                    "(" \
                    "select tenant_id," \
                    "database_id," \
                    "table_id," \
                    "table_name, " \
                    "table_type," \
                    "gmt_create," \
                    "gmt_modified," \
                    "store_format," \
                    "CASE table_type WHEN 4 THEN NULL ELSE collation_type END as collation_type," \
                    "\"COMMENT\" as cmt," \
                    "autoinc_column_id " \
                    "from SYS.ALL_VIRTUAL_TABLE_REAL_AGENT where table_type in (0, 1, 2, 3, 4, 14)) a " \
                    "join SYS.ALL_VIRTUAL_DATABASE_REAL_AGENT b " \
                    "on a.database_id = b.database_id " \
                    "and a.tenant_id = b.tenant_id " \
                    "left join (" \
                    "select tenant_id," \
                    "table_id," \
                    "row_cnt," \
                    "avg_row_len," \
                    "row_cnt * avg_row_len as data_size " \
                    "from SYS.ALL_VIRTUAL_TABLE_STAT_REAL_AGENT " \
                    "where partition_id = -1 or partition_id = table_id) ts " \
                    "on a.table_id = ts.table_id " \
                    "and a.tenant_id = ts.tenant_id " \
                    "and a.table_type in (0, 1, 2, 3, 4, 14) " \
                    "and b.database_name != '__recyclebin' " \
                    "and b.in_recyclebin = 0 "


class ObTenantAllTables : public common::ObVirtualTableIterator
{
  enum TENANT_ALL_TABLES_COLUMN
  {
    DATABASE_ID = OB_APP_MIN_COLUMN_ID,
    TABLE_NAME = OB_APP_MIN_COLUMN_ID + 1,
    TABLE_TYPE = OB_APP_MIN_COLUMN_ID + 2,
    ENGINE = OB_APP_MIN_COLUMN_ID + 3,
    TABLE_VERSION = OB_APP_MIN_COLUMN_ID + 4,
    ROW_FORMAT = OB_APP_MIN_COLUMN_ID + 5,
    ROWS = OB_APP_MIN_COLUMN_ID + 6,
    AVG_ROW_LENGTH = OB_APP_MIN_COLUMN_ID + 7,
    DATA_LENGTH = OB_APP_MIN_COLUMN_ID + 8,
    MAX_DATA_LENGTH = OB_APP_MIN_COLUMN_ID + 9,
    INDEX_LENGTH = OB_APP_MIN_COLUMN_ID + 10,
    DATA_FREE = OB_APP_MIN_COLUMN_ID + 11,
    AUTO_INCREMENT = OB_APP_MIN_COLUMN_ID + 12,
    CREATE_TIME = OB_APP_MIN_COLUMN_ID + 13,
    UPDATE_TIME = OB_APP_MIN_COLUMN_ID + 14,
    CHECK_TIME = OB_APP_MIN_COLUMN_ID + 15,
    COLLATION = OB_APP_MIN_COLUMN_ID + 16,
    CHECKSUM = OB_APP_MIN_COLUMN_ID + 17,
    CREATE_OPTIONS = OB_APP_MIN_COLUMN_ID + 18,
    COMMENT = OB_APP_MIN_COLUMN_ID + 19
  };
  class TableStatistics
  {
  public:
    TableStatistics():
        row_count_(0),
        data_size_(0),
        data_version_(0),
        data_checksum_(0),
        create_time_(0),
        update_time_(0)
        {
        }
    ~TableStatistics() {}
    void reset()
    {
      row_count_ = 0;
      data_size_ = 0;
      data_version_ = 0;
      data_checksum_ = 0;
      create_time_ = 0;
      update_time_ = 0;
    }
    inline void set_table_rows(const int64_t &v) { row_count_ = v; }
    inline void set_data_length(const int64_t &v) { data_size_ = v; }
    inline void set_version(const int64_t &v) { data_version_ = v; }
    inline void set_checksum(const int64_t &v) { data_checksum_ = v; }
    inline void set_create_time(const int64_t &v) { create_time_ = v; }
    inline void set_update_time(const int64_t &v) { update_time_ = v; }
    int64_t row_count_;
    int64_t data_size_;
    int64_t data_version_;
    int64_t data_checksum_;
    int64_t create_time_;
    int64_t update_time_;
    TO_STRING_KV(K_(row_count),
                 K_(data_size),
                 K_(data_version),
                 K_(data_checksum),
                 K_(create_time),
                 K_(update_time));
  };
public:
  ObTenantAllTables();
  virtual ~ObTenantAllTables();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_sql_proxy(common::ObMySQLProxy *sql_proxy) { sql_proxy_ = sql_proxy; }
private:
  int inner_get_next_row();
  int get_sequence_value();
  int get_table_stats();
private:
  common::ObMySQLProxy *sql_proxy_;
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObSEArray<const share::schema::ObTableSchema *, 128> table_schemas_;
  int64_t table_schema_idx_;
  common::hash::ObHashMap<share::AutoincKey, uint64_t> seq_values_;
  common::hash::ObHashMap<uint64_t, TableStatistics> tables_statistics_;
  char *option_buf_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantAllTables);
};

}// observer
}// oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_ALL_TABLES_ */
