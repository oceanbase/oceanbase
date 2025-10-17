/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_SQL_STAT_H
#define OB_ALL_VIRTUAL_SQL_STAT_H

#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/row/ob_row.h"
#include "sql/monitor/ob_sql_stat_record.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{
typedef common::hash::ObHashMap<sql::ObSqlStatRecordKey, sql::ObExecutedSqlStatRecord*> TmpSqlStatMap;
struct ObGetAllSqlStatCacheIdOp
{
  ObGetAllSqlStatCacheIdOp(common::ObIArray<uint64_t> *key_array)
    : key_array_(key_array)
  {}
  void reset() { key_array_ = NULL; }
  int operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry);
public:
  common::ObIArray<uint64_t> *key_array_;
};

class ObAllVirtualSqlStat : public common::ObVirtualTableScannerIterator,
                            public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualSqlStat() :
      ipstr_(), port_(0), last_sql_stat_record_(nullptr),
      tmp_sql_stat_map_(),
      sql_stat_cache_id_array_(),
      sql_stat_cache_id_array_idx_(0),
      first_enter_(true) {}
  virtual ~ObAllVirtualSqlStat() { reset(); }

public:
  void reset();
  int inner_get_next_row(common::ObNewRow *&row);
  virtual void release_last_tenant() override;
  bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info);
  virtual bool is_need_process(uint64_t tenant_id) override {
    if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
      return true;
    }
    return false;
  }
private:
  enum STORAGE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    SQL_ID,
    PLAN_ID,
    PLAN_HASH,
    PLAN_TYPE,
    QUERY_SQL,
    SQL_TYPE,
    MODULE,
    ACTION,
    PARSING_DB_ID,
    PARSING_DB_NAME,
    PARSING_USER_ID,
    EXECUTIONS_TOTAL,
    EXECUTIONS_DELTA,
    DISK_READS_TOTAL,
    DISK_READS_DELTA,
    BUFFER_GETS_TOTAL,
    BUFFER_GETS_DELTA,
    ELAPSED_TIME_TOTAL,
    ELAPSED_TIME_DELTA,
    CPU_TIME_TOTAL,
    CPU_TIME_DELTA,
    CCWAIT_TOTAL,
    CCWAIT_DELTA,
    USERIO_WAIT_TOTAL,
    USERIO_WAIT_DELTA,
    APWAIT_TOTAL,
    APWAIT_DELTA,
    PHYSICAL_READ_REQUESTS_TOTAL,
    PHYSICAL_READ_REQUESTS_DELTA,
    PHYSICAL_READ_BYTES_TOTAL,
    PHYSICAL_READ_BYTES_DELTA,
    WRITE_THROTTLE_TOTAL,
    WRITE_THROTTLE_DELTA,
    ROWS_PROCESSED_TOTAL,
    ROWS_PROCESSED_DELTA,
    MEMSTORE_READ_ROWS_TOTAL,
    MEMSTORE_READ_ROWS_DELTA,
    MINOR_SSSTORE_READ_ROWS_TOTAL,
    MINOR_SSSTORE_READ_ROWS_DELTA,
    MAJOR_SSSTORE_READ_ROWS_TOTAL,
    MAJOR_SSSTORE_READ_ROWS_DELTA,
    RPC_TOTAL,
    RPC_DELTA,
    FETCHES_TOTAL,
    FETCHES_DELTA,
    RETRY_TOTAL,
    RETRY_DELTA,
    PARTITION_TOTAL,
    PARTITION_DELTA,
    NESTED_SQL_TOTAL,
    NESTED_SQL_DELTA,
    SOURCE_IP,
    SOURCE_PORT,
    ROUTE_MISS_TOTAL,
    ROUTE_MISS_DELTA,
    FIRST_LOAD_TIME,
    PLAN_CACHE_HIT_TOTAL,
    PLAN_CACHE_HIT_DELTA
  };
  int fill_row(const uint64_t tenant_id,
               const ObExecutedSqlStatRecord *sql_stat_record,
               common::ObNewRow *&row);
  int get_server_ip_and_port();
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  int load_next_batch_sql_stat();
  int get_next_sql_stat(sql::ObExecutedSqlStatRecord &sql_stat_value);
private:
  common::ObString ipstr_;
  int32_t port_;
  ObExecutedSqlStatRecord *last_sql_stat_record_;
  TmpSqlStatMap tmp_sql_stat_map_;
  common::ObSEArray<uint64_t, 1024> sql_stat_cache_id_array_;
  int64_t sql_stat_cache_id_array_idx_;
  bool first_enter_;
};


} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_SQL_STAT_H */
