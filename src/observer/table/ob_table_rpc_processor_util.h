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

#ifndef _OB_TABLE_RPC_PROCESSOR_UTIL_H
#define _OB_TABLE_RPC_PROCESSOR_UTIL_H 1
#include "lib/stat/ob_diagnose_info.h"
#include "share/table/ob_table.h"
#include "sql/monitor/ob_exec_stat.h"
#include "share/table/ob_table_config_util.h"

namespace oceanbase
{
namespace observer
{

enum ObTableProccessType
{
  TABLE_API_PROCESS_TYPE_INVALID = 0,
  // table single mutate
  TABLE_API_SINGLE_INSERT,
  TABLE_API_SINGLE_GET,
  TABLE_API_SINGLE_DELETE,
  TABLE_API_SINGLE_UPDATE,
  TABLE_API_SINGLE_INSERT_OR_UPDATE,
  TABLE_API_SINGLE_REPLACE,
  TABLE_API_SINGLE_INCREMENT,
  TABLE_API_SINGLE_APPEND,
  TABLE_API_SINGLE_PUT,

  // table batch mutate
  TABLE_API_MULTI_INSERT,
  TABLE_API_MULTI_GET,
  TABLE_API_MULTI_DELETE,
  TABLE_API_MULTI_UPDATE,
  TABLE_API_MULTI_INSERT_OR_UPDATE,
  TABLE_API_MULTI_REPLACE,
  TABLE_API_MULTI_INCREMENT,
  TABLE_API_MULTI_APPEND,
  TABLE_API_BATCH_RETRIVE,
  TABLE_API_BATCH_HYBRID,
  TABLE_API_MULTI_PUT,

  // hbase mutate
  TABLE_API_HBASE_DELETE,
  TABLE_API_HBASE_PUT,
  TABLE_API_HBASE_CHECK_AND_DELETE,
  TABLE_API_HBASE_CHECK_AND_PUT,
  TABLE_API_HBASE_CHECK_AND_MUTATE,
  TABLE_API_HBASE_INCREMENT,
  TABLE_API_HBASE_APPEND,
  TABLE_API_HBASE_HYBRID,

  // query
  TABLE_API_TABLE_QUERY,
  TABLE_API_HBASE_QUERY,
  TABLE_API_TABLE_QUERY_ASYNC,
  TABLE_API_HBASE_QUERY_ASYNC,

  // query_and_mutate
  TABLE_API_QUERY_AND_MUTATE,
  // redis
  TABLE_API_REDIS_TYPE_OFFSET,
  TABLE_API_REDIS_LINDEX,
  TABLE_API_REDIS_LSET,
  TABLE_API_REDIS_LRANGE,
  TABLE_API_REDIS_LTRIM,
  // include lpush,lpushx,rpush,rpushx
  TABLE_API_REDIS_PUSH,
  // include lpop,rpop
  TABLE_API_REDIS_POP,
  TABLE_API_REDIS_LREM,
  TABLE_API_REDIS_RPOPLPUSH,
  TABLE_API_REDIS_LINSERT,
  TABLE_API_REDIS_LLEN,

  // group commit
  TABLE_API_GROUP_TRIGGER,

  TABLE_API_PROCESS_TYPE_MAX
};

#define SET_AUDIT_SQL_STRING(op_type) \
static const char op_type##_name[] = "table api: " #op_type; \
audit_record.sql_ = const_cast<char *>(op_type##_name); \
audit_record.sql_len_ = sizeof(op_type##_name)


#define COLLECT_RESPONSE_TIME_false(stmt_type, elapsed_us)
#define COLLECT_RESPONSE_TIME_true(stmt_type, elapsed_us)                                                \
do {                                                                                                     \
  observer::ObTenantQueryRespTimeCollector *collector = MTL(observer::ObTenantQueryRespTimeCollector *); \
  if (OB_NOT_NULL(collector)) {                                                                          \
    collector->collect(stmt_type, false/*is_inner_sql*/, elapsed_us);                                                    \
  }                                                                                                      \
} while (0);

#define COLLECT_RESPONSE_TIME(enable, stmt_type, elapsed_us) \
do {                                                         \
  if (enable) {                                              \
    COLLECT_RESPONSE_TIME_true(stmt_type, elapsed_us);       \
  } else {                                                   \
    COLLECT_RESPONSE_TIME_false(stmt_type, elapsed_us);      \
  }                                                          \
} while (0);

class ObTableRpcProcessorUtil
{
public:
  OB_INLINE static bool is_require_rerouting_err(const int err)
  {
    // rerouting: whether client should refresh location cache and retry
    // Now, following the same logic as in ../mysql/ob_query_retry_ctrl.cpp
    return is_master_changed_error(err)
      || is_server_down_error(err)
      || is_partition_change_error(err)
      || is_server_status_error(err)
      || is_unit_migrate(err)
      || is_transaction_rpc_timeout_err(err)
      || is_has_no_readable_replica_err(err)
      || is_select_dup_follow_replic_err(err)
      || is_trans_stmt_need_retry_error(err)
      || is_snapshot_discarded_err(err);
  }

  OB_INLINE static bool need_do_move_response(const int err, const obrpc::ObRpcPacket &rpc_pkt)
  {
    return is_require_rerouting_err(err)
      && ObKVFeatureModeUitl::is_rerouting_enable()
      && rpc_pkt.require_rerouting();
  }

  OB_INLINE static void record_stat(
      const int32_t process_type,
      int64_t elapsed_us,
      int64_t rows,
      bool enable_response_time_stats)
  {
    switch (process_type) {
      // table single mutate
      case ObTableProccessType::TABLE_API_SINGLE_INSERT:
        EVENT_INC(TABLEAPI_INSERT_COUNT);
        EVENT_ADD(TABLEAPI_INSERT_TIME, elapsed_us);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_INSERT, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_SINGLE_GET:
        EVENT_INC(TABLEAPI_RETRIEVE_COUNT);
        EVENT_ADD(TABLEAPI_RETRIEVE_TIME, elapsed_us);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_GET, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_SINGLE_DELETE:
        EVENT_INC(TABLEAPI_DELETE_COUNT);
        EVENT_ADD(TABLEAPI_DELETE_TIME, elapsed_us);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_DELETE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_SINGLE_UPDATE:
        EVENT_INC(TABLEAPI_UPDATE_COUNT);
        EVENT_ADD(TABLEAPI_UPDATE_TIME, elapsed_us);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_UPDATE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_SINGLE_INSERT_OR_UPDATE:
        EVENT_INC(TABLEAPI_INSERT_OR_UPDATE_COUNT);
        EVENT_ADD(TABLEAPI_INSERT_OR_UPDATE_TIME, elapsed_us);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_INSERT_OR_UPDATE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_SINGLE_PUT:
        EVENT_INC(TABLEAPI_PUT_COUNT);
        EVENT_ADD(TABLEAPI_PUT_TIME, elapsed_us);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_PUT, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_SINGLE_REPLACE:
        EVENT_INC(TABLEAPI_REPLACE_COUNT);
        EVENT_ADD(TABLEAPI_REPLACE_TIME, elapsed_us);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_REPLACE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_SINGLE_INCREMENT:
        EVENT_INC(TABLEAPI_INCREMENT_COUNT);
        EVENT_ADD(TABLEAPI_INCREMENT_TIME, elapsed_us);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_INCREMENT, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_SINGLE_APPEND:
        EVENT_INC(TABLEAPI_APPEND_COUNT);
        EVENT_ADD(TABLEAPI_APPEND_TIME, elapsed_us);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_APPEND, elapsed_us);
        break;

      // table batch mutate
      case ObTableProccessType::TABLE_API_MULTI_INSERT:
        EVENT_INC(TABLEAPI_MULTI_INSERT_COUNT);
        EVENT_ADD(TABLEAPI_MULTI_INSERT_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_MULTI_INSERT_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_MULTI_INSERT, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_MULTI_GET:
        EVENT_INC(TABLEAPI_MULTI_RETRIEVE_COUNT);
        EVENT_ADD(TABLEAPI_MULTI_RETRIEVE_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_MULTI_RETRIEVE_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_MULTI_GET, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_MULTI_DELETE:
        EVENT_INC(TABLEAPI_MULTI_DELETE_COUNT);
        EVENT_ADD(TABLEAPI_MULTI_DELETE_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_MULTI_DELETE_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_MULTI_DELETE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_MULTI_UPDATE:
        EVENT_INC(TABLEAPI_MULTI_UPDATE_COUNT);
        EVENT_ADD(TABLEAPI_MULTI_UPDATE_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_MULTI_UPDATE_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_MULTI_UPDATE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_MULTI_INSERT_OR_UPDATE:
        EVENT_INC(TABLEAPI_MULTI_INSERT_OR_UPDATE_COUNT);
        EVENT_ADD(TABLEAPI_MULTI_INSERT_OR_UPDATE_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_MULTI_INSERT_OR_UPDATE_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_MULTI_INSERT_OR_UPDATE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_MULTI_PUT:
        EVENT_INC(TABLEAPI_MULTI_PUT_COUNT);
        EVENT_ADD(TABLEAPI_MULTI_PUT_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_MULTI_PUT_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_MULTI_PUT, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_MULTI_REPLACE:
        EVENT_INC(TABLEAPI_MULTI_REPLACE_COUNT);
        EVENT_ADD(TABLEAPI_MULTI_REPLACE_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_MULTI_REPLACE_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_MULTI_REPLACE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_MULTI_INCREMENT:
        EVENT_INC(TABLEAPI_MULTI_INCREMENT_COUNT);
        EVENT_ADD(TABLEAPI_MULTI_INCREMENT_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_MULTI_INCREMENT_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_MULTI_INCREMENT, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_MULTI_APPEND:
        EVENT_INC(TABLEAPI_MULTI_APPEND_COUNT);
        EVENT_ADD(TABLEAPI_MULTI_APPEND_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_MULTI_APPEND_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_MULTI_APPEND, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_BATCH_RETRIVE:
        EVENT_INC(TABLEAPI_BATCH_RETRIEVE_COUNT);
        EVENT_ADD(TABLEAPI_BATCH_RETRIEVE_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_BATCH_RETRIEVE_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_MULTI_GET, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_BATCH_HYBRID:
        EVENT_INC(TABLEAPI_BATCH_HYBRID_COUNT);
        EVENT_ADD(TABLEAPI_BATCH_HYBRID_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_BATCH_HYBRID_INSERT_OR_UPDATE_ROW, rows); // @todo row count for each type
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_OTHER, elapsed_us);
        break;

      // hbase mutate
      case ObTableProccessType::TABLE_API_HBASE_DELETE:
        EVENT_INC(HBASEAPI_DELETE_COUNT);
        EVENT_ADD(HBASEAPI_DELETE_TIME, elapsed_us);
        EVENT_ADD(HBASEAPI_DELETE_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_HBASE_DELETE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_HBASE_PUT:
        EVENT_INC(HBASEAPI_PUT_COUNT);
        EVENT_ADD(HBASEAPI_PUT_TIME, elapsed_us);
        EVENT_ADD(HBASEAPI_PUT_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_HBASE_PUT, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_HBASE_CHECK_AND_DELETE:
        EVENT_INC(HBASEAPI_CHECK_DELETE_COUNT);
        EVENT_ADD(HBASEAPI_CHECK_DELETE_TIME, elapsed_us);
        EVENT_ADD(HBASEAPI_CHECK_DELETE_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_HBASE_CHECK_AND_DELETE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_HBASE_CHECK_AND_PUT:
        EVENT_INC(HBASEAPI_CHECK_PUT_COUNT);
        EVENT_ADD(HBASEAPI_CHECK_PUT_TIME, elapsed_us);
        EVENT_ADD(HBASEAPI_CHECK_PUT_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_HBASE_CHECK_AND_PUT, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_HBASE_CHECK_AND_MUTATE:
        EVENT_INC(HBASEAPI_CHECK_MUTATE_COUNT);
        EVENT_ADD(HBASEAPI_CHECK_MUTATE_TIME, elapsed_us);
        EVENT_ADD(HBASEAPI_CHECK_MUTATE_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_HBASE_CHECK_AND_MUTATE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_HBASE_INCREMENT:
        EVENT_INC(HBASEAPI_INCREMENT_COUNT);
        EVENT_ADD(HBASEAPI_INCREMENT_TIME, elapsed_us);
        EVENT_ADD(HBASEAPI_INCREMENT_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_HBASE_INCREMENT, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_HBASE_APPEND:
        EVENT_INC(HBASEAPI_APPEND_COUNT);
        EVENT_ADD(HBASEAPI_APPEND_TIME, elapsed_us);
        EVENT_ADD(HBASEAPI_APPEND_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_HBASE_APPEND, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_HBASE_HYBRID:
        EVENT_INC(HBASEAPI_HYBRID_COUNT);
        EVENT_ADD(HBASEAPI_HYBRID_TIME, elapsed_us);
        EVENT_ADD(HBASEAPI_HYBRID_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_HBASE_OTHER, elapsed_us);
        break;

      // table query
      case ObTableProccessType::TABLE_API_TABLE_QUERY:
        EVENT_INC(TABLEAPI_QUERY_COUNT);
        EVENT_ADD(TABLEAPI_QUERY_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_QUERY_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_QUERY, elapsed_us);
        break;

      // hbase query
      case ObTableProccessType::TABLE_API_HBASE_QUERY:
        EVENT_INC(HBASEAPI_SCAN_COUNT);
        EVENT_ADD(HBASEAPI_SCAN_TIME, elapsed_us);
        EVENT_ADD(HBASEAPI_SCAN_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_HBASE_SCAN, elapsed_us);
        break;

      // table query async
      case ObTableProccessType::TABLE_API_TABLE_QUERY_ASYNC:
        EVENT_INC(TABLEAPI_QUERY_COUNT);
        EVENT_ADD(TABLEAPI_QUERY_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_QUERY_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_QUERY, elapsed_us);
        break;

      // hbase query sync
      case ObTableProccessType::TABLE_API_HBASE_QUERY_ASYNC:
        EVENT_INC(HBASEAPI_SCAN_COUNT);
        EVENT_ADD(HBASEAPI_SCAN_TIME, elapsed_us);
        EVENT_ADD(HBASEAPI_SCAN_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_HBASE_SCAN, elapsed_us);
        break;

      // table query_and_mutate
      case ObTableProccessType::TABLE_API_QUERY_AND_MUTATE:
        EVENT_INC(TABLEAPI_QUERY_AND_MUTATE_COUNT);
        EVENT_ADD(TABLEAPI_QUERY_AND_MUTATE_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_QUERY_AND_MUTATE_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_QUERY_AND_MUTATE, elapsed_us);
        break;
      case ObTableProccessType::TABLE_API_GROUP_TRIGGER:
        EVENT_INC(TABLEAPI_GROUP_TRIGGER_COUNT);
        EVENT_ADD(TABLEAPI_GROUP_TRIGGER_TIME, elapsed_us);
        EVENT_ADD(TABLEAPI_GROUP_TRIGGER_ROW, rows);
        COLLECT_RESPONSE_TIME(enable_response_time_stats, sql::stmt::T_KV_OTHER, elapsed_us);
        break;
      default:
        SERVER_LOG_RET(WARN, OB_ERR_UNEXPECTED, "unknow process type", K(process_type), K(elapsed_us), K(rows));
        break;
    }
  }

  static int negate_htable_timestamp(table::ObITableEntity &entity);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableRpcProcessorUtil);
  ObTableRpcProcessorUtil() = delete;
  ~ObTableRpcProcessorUtil() = delete;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_PROCESSOR_UTIL_H */
