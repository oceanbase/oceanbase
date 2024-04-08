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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_GV_SQL_AUDIT_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_GV_SQL_AUDIT_

#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"
#include "observer/mysql/ob_ra_queue.h"

namespace oceanbase
{
namespace obmysql
{
class ObMySQLRequestManager;
class ObMySQLRequestRecord;
class ObMySQLGlobalRequestManager;
}
namespace common
{
class ObIAllocator;
}

namespace share
{
class ObTenantSpaceFetcher;
}

namespace observer
{
class ObGvSqlAudit : public common::ObVirtualTableScannerIterator
{
public:
  ObGvSqlAudit ();
  virtual ~ObGvSqlAudit();

  int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  inline void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  virtual int set_ip(common::ObAddr *addr);
  virtual void reset();
  int check_ip_and_port(bool &is_valid);
  void use_index_scan() { is_use_index_ = true; }
  bool is_index_scan() const { return is_use_index_; }

private:
  bool is_perf_event_dep_field(uint64_t col_id);
  int fill_cells(obmysql::ObMySQLRequestRecord &record);
  int extract_tenant_ids();
  int extract_request_ids(const uint64_t tenant_id,
                          int64_t &start_id,
                          int64_t &end_id,
                          bool &is_valid);
private:
  enum WAIT_COLUMN
  {
    SERVER_IP = common::OB_APP_MIN_COLUMN_ID,
    SERVER_PORT,
    TENANT_ID,
    REQUEST_ID,
    TRACE_ID,
    CLIENT_IP,
    CLIENT_PORT,
    TENANT_NAME,
    EFFECTIVE_TENANT_ID,
    USER_ID,
    USER_NAME,
    DB_ID,
    DB_NAME,
    SQL_ID,
    QUERY_SQL,
    PLAN_ID,
    AFFECTED_ROWS,
    RETURN_ROWS,
    PARTITION_CNT,
    RET_CODE,
    QC_ID,
    DFO_ID,
    SQC_ID,
    WORKER_ID,

    EVENT,
    P1TEXT,
    P1,
    P2TEXT,
    P2,
    P3TEXT,
    P3,
    LEVEL,
    WAIT_CLASS_ID,
    WAIT_CLASS_NO,
    WAIT_CLASS,
    STATE,
    WAIT_TIME_MICRO,
    TOTAL_WAIT_TIME,
    TOTAL_WAIT_COUNT,

    RPC_COUNT,
    PLAN_TYPE,

    IS_INNER_SQL,
    IS_EXECUTOR_RPC,
    IS_HIT_PLAN,

    REQUEST_TIMESTAMP,
    ELAPSED_TIME,
    NET_TIME,
    NET_WAIT_TIME,
    QUEUE_TIME,
    DECODE_TIME,
    GET_PLAN_TIME,
    EXECUTE_TIME,
    APPLICATION_WAIT_TIME,
    CONCURRENCY_WAIT_TIME,
    USER_IO_WAIT_TIME,
    SCHEDULE_TIME,
    ROW_CACHE_HIT,
    BLOOM_FILTER_NOT_HIT,
    BLOCK_CACHE_HIT,
    DISK_READS,
    SQL_EXEC_ID,
    SESSION_ID,
    RETRY_CNT,
    TABLE_SCAN,
    CONSISTENCY_LEVEL,
    MEMSTORE_READ_ROW_COUNT,
    SSSTORE_READ_ROW_COUNT,
    DATA_BLOCK_READ_CNT,
    DATA_BLOCK_CACHE_HIT,
    INDEX_BLOCK_READ_CNT,
    INDEX_BLOCK_CACHE_HIT,
    BLOCKSCAN_BLOCK_CNT,
    BLOCKSCAN_ROW_CNT,
    PUSHDOWN_STORAGE_FILTER_ROW_CNT,
    REQUEST_MEMORY_USED,
    EXPECTED_WORKER_COUNT,
    USED_WORKER_COUNT,
    SCHED_INFO,
    FUSE_ROW_CACHE_HIT,
    USER_CLIENT_IP,
    PS_CLIENT_STMT_ID,
    PS_INNER_STMT_ID,
    TRANSACTION_HASH,
    SNAPSHOT_VERSION,
    SNAPSHOT_SOURCE,
    REQUEST_TYPE,
    IS_BATCHED_MULTI_STMT,
    OB_TRACE_INFO,
    PLAN_HASH,
    USER_GROUP,
    LOCK_FOR_READ_TIME,
    PARAMS_VALUE,
    RULE_NAME,
    PROXY_SESSION_ID,
    TX_INTERNAL_ROUTE_FLAG,

    PARTITION_HIT,
    TX_INTERNAL_ROUTE_VERSION,
    FLT_TRACE_ID,

    PL_TRACE_ID,
    PLSQL_EXEC_TIME,
    NETWORK_WAIT_TIME,
    STMT_TYPE,
    SEQ_NUM,
    TOTAL_MEMSTORE_READ_ROW_COUNT,
    TOTAL_SSSTORE_READ_ROW_COUNT,
    PROXY_USER_NAME,
    FORMAT_SQL_ID
  };

  const static int64_t PRI_KEY_IP_IDX        = 0;
  const static int64_t PRI_KEY_PORT_IDX      = 1;
  const static int64_t PRI_KEY_TENANT_ID_IDX = 2;
  const static int64_t PRI_KEY_REQ_ID_IDX    = 3;

  const static int64_t IDX_KEY_TENANT_ID_IDX = 0;
  const static int64_t IDX_KEY_REQ_ID_IDX    = 1;
  const static int64_t IDX_KEY_IP_IDX        = 2;
  const static int64_t IDX_KEY_PORT_IDX      = 3;


  DISALLOW_COPY_AND_ASSIGN(ObGvSqlAudit);
  obmysql::ObMySQLRequestManager *cur_mysql_req_mgr_;
  int64_t start_id_;
  int64_t end_id_;
  int64_t cur_id_;
  common::ObRaQueue::Ref ref_;
  common::ObAddr *addr_;
  common::ObString ipstr_;
  int32_t port_;
  char server_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  char client_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  char user_client_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  char trace_id_[128];
  char pl_trace_id_[128];

  //max wait event columns
  bool is_first_get_;
  bool is_use_index_;

  common::ObSEArray<uint64_t, 16> tenant_id_array_;
  int64_t tenant_id_array_idx_;

  share::ObTenantSpaceFetcher *with_tenant_ctx_;
};
}
}

#endif
