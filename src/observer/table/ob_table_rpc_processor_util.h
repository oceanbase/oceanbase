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

  // hbase mutate
  TABLE_API_HBASE_DELETE,
  TABLE_API_HBASE_PUT,
  TABLE_API_HBASE_CHECK_AND_DELETE,
  TABLE_API_HBASE_CHECK_AND_PUT,
  TABLE_API_HBASE_INCREMENT,
  TABLE_API_HBASE_APPEND,
  TABLE_API_HBASE_HYBRID,

  // query
  TABLE_API_TABLE_QUERY,
  TABLE_API_HBASE_QUERY,
  TABLE_API_TABLE_QUERY_SYNC,
  TABLE_API_HBASE_QUERY_SYNC,

  // query_and_mutate
  TABLE_API_QUERY_AND_MUTATE,

  TABLE_API_PROCESS_TYPE_MAX
};

#define SET_AUDIT_SQL_STRING(op_type) \
static const char op_type##_name[] = "table api: " #op_type; \
audit_record.sql_ = const_cast<char *>(op_type##_name); \
audit_record.sql_len_ = sizeof(op_type##_name)

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
      sql::ObAuditRecordData &audit_record,
      const int32_t process_type,
      int64_t elapsed_us,
      int64_t rows)
  {
    switch (process_type) {
      // table single mutate
      case ObTableProccessType::TABLE_API_SINGLE_INSERT:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_INSERT_COUNT);
          EVENT_ADD(TABLEAPI_INSERT_TIME, elapsed_us);
        }
        SET_AUDIT_SQL_STRING(single_insert);
        audit_record.stmt_type_ = sql::stmt::T_KV_INSERT;
        break;
      case ObTableProccessType::TABLE_API_SINGLE_GET:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_RETRIEVE_COUNT);
          EVENT_ADD(TABLEAPI_RETRIEVE_TIME, elapsed_us);
        }
        SET_AUDIT_SQL_STRING(single_get);
        audit_record.stmt_type_ = sql::stmt::T_KV_GET;
        break;
      case ObTableProccessType::TABLE_API_SINGLE_DELETE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_DELETE_COUNT);
          EVENT_ADD(TABLEAPI_DELETE_TIME, elapsed_us);
        }
        SET_AUDIT_SQL_STRING(single_delete);
        audit_record.stmt_type_ = sql::stmt::T_KV_DELETE;
        break;
      case ObTableProccessType::TABLE_API_SINGLE_UPDATE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_UPDATE_COUNT);
          EVENT_ADD(TABLEAPI_UPDATE_TIME, elapsed_us);
        }
        SET_AUDIT_SQL_STRING(single_update);
        audit_record.stmt_type_ = sql::stmt::T_KV_UPDATE;
        break;
      case ObTableProccessType::TABLE_API_SINGLE_INSERT_OR_UPDATE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_INSERT_OR_UPDATE_COUNT);
          EVENT_ADD(TABLEAPI_INSERT_OR_UPDATE_TIME, elapsed_us);
        }
        SET_AUDIT_SQL_STRING(single_insert_or_update);
        audit_record.stmt_type_ = sql::stmt::T_KV_INSERT_OR_UPDATE;
        break;
      case ObTableProccessType::TABLE_API_SINGLE_REPLACE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_REPLACE_COUNT);
          EVENT_ADD(TABLEAPI_REPLACE_TIME, elapsed_us);
        }
        SET_AUDIT_SQL_STRING(single_replace);
        audit_record.stmt_type_ = sql::stmt::T_KV_REPLACE;
        break;
      case ObTableProccessType::TABLE_API_SINGLE_INCREMENT:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_INCREMENT_COUNT);
          EVENT_ADD(TABLEAPI_INCREMENT_TIME, elapsed_us);
        }
        SET_AUDIT_SQL_STRING(single_increment);
        audit_record.stmt_type_ = sql::stmt::T_KV_INCREMENT;
        break;
      case ObTableProccessType::TABLE_API_SINGLE_APPEND:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_APPEND_COUNT);
          EVENT_ADD(TABLEAPI_APPEND_TIME, elapsed_us);
        }
        SET_AUDIT_SQL_STRING(single_append);
        audit_record.stmt_type_ = sql::stmt::T_KV_APPEND;
        break;

      // table batch mutate
      case ObTableProccessType::TABLE_API_MULTI_INSERT:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_MULTI_INSERT_COUNT);
          EVENT_ADD(TABLEAPI_MULTI_INSERT_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_MULTI_INSERT_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(multi_insert);
        audit_record.stmt_type_ = sql::stmt::T_KV_MULTI_INSERT;
        break;
      case ObTableProccessType::TABLE_API_MULTI_GET:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_MULTI_RETRIEVE_COUNT);
          EVENT_ADD(TABLEAPI_MULTI_RETRIEVE_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_MULTI_RETRIEVE_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(multi_get);
        audit_record.stmt_type_ = sql::stmt::T_KV_MULTI_GET;
        break;
      case ObTableProccessType::TABLE_API_MULTI_DELETE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_MULTI_DELETE_COUNT);
          EVENT_ADD(TABLEAPI_MULTI_DELETE_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_MULTI_DELETE_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(multi_delete);
        audit_record.stmt_type_ = sql::stmt::T_KV_MULTI_DELETE;
        break;
      case ObTableProccessType::TABLE_API_MULTI_UPDATE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_MULTI_UPDATE_COUNT);
          EVENT_ADD(TABLEAPI_MULTI_UPDATE_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_MULTI_UPDATE_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(multi_update);
        audit_record.stmt_type_ = sql::stmt::T_KV_MULTI_UPDATE;
        break;
      case ObTableProccessType::TABLE_API_MULTI_INSERT_OR_UPDATE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_MULTI_INSERT_OR_UPDATE_COUNT);
          EVENT_ADD(TABLEAPI_MULTI_INSERT_OR_UPDATE_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_MULTI_INSERT_OR_UPDATE_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(multi_insert_or_update);
        audit_record.stmt_type_ = sql::stmt::T_KV_MULTI_INSERT_OR_UPDATE;
        break;
      case ObTableProccessType::TABLE_API_MULTI_REPLACE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_MULTI_REPLACE_COUNT);
          EVENT_ADD(TABLEAPI_MULTI_REPLACE_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_MULTI_REPLACE_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(multi_replace);
        audit_record.stmt_type_ = sql::stmt::T_KV_MULTI_REPLACE;
        break;
      case ObTableProccessType::TABLE_API_MULTI_INCREMENT:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_MULTI_INCREMENT_COUNT);
          EVENT_ADD(TABLEAPI_MULTI_INCREMENT_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_MULTI_INCREMENT_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(multi_increment);
        audit_record.stmt_type_ = sql::stmt::T_KV_MULTI_INCREMENT;
        break;
      case ObTableProccessType::TABLE_API_MULTI_APPEND:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_MULTI_APPEND_COUNT);
          EVENT_ADD(TABLEAPI_MULTI_APPEND_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_MULTI_APPEND_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(multi_append);
        audit_record.stmt_type_ = sql::stmt::T_KV_MULTI_APPEND;
        break;
      case ObTableProccessType::TABLE_API_BATCH_RETRIVE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_BATCH_RETRIEVE_COUNT);
          EVENT_ADD(TABLEAPI_BATCH_RETRIEVE_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_BATCH_RETRIEVE_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(batch_retrieve);
        audit_record.stmt_type_ = sql::stmt::T_KV_MULTI_GET;
        break;
      case ObTableProccessType::TABLE_API_BATCH_HYBRID:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_BATCH_HYBRID_COUNT);
          EVENT_ADD(TABLEAPI_BATCH_HYBRID_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_BATCH_HYBRID_INSERT_OR_UPDATE_ROW, rows); // @todo row count for each type
        }
        SET_AUDIT_SQL_STRING(batch_hybrid);
        audit_record.stmt_type_ = sql::stmt::T_KV_OTHER;
        break;

      // hbase mutate
      case ObTableProccessType::TABLE_API_HBASE_DELETE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(HBASEAPI_DELETE_COUNT);
          EVENT_ADD(HBASEAPI_DELETE_TIME, elapsed_us);
          EVENT_ADD(HBASEAPI_DELETE_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(hbase_delete);
        audit_record.stmt_type_ = sql::stmt::T_HBASE_DELETE;
        break;
      case ObTableProccessType::TABLE_API_HBASE_PUT:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(HBASEAPI_PUT_COUNT);
          EVENT_ADD(HBASEAPI_PUT_TIME, elapsed_us);
          EVENT_ADD(HBASEAPI_PUT_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(hbase_put);
        audit_record.stmt_type_ = sql::stmt::T_HBASE_PUT;
        break;
      case ObTableProccessType::TABLE_API_HBASE_CHECK_AND_DELETE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(HBASEAPI_CHECK_DELETE_COUNT);
          EVENT_ADD(HBASEAPI_CHECK_DELETE_TIME, elapsed_us);
          EVENT_ADD(HBASEAPI_CHECK_DELETE_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(hbase_check_and_delete);
        audit_record.stmt_type_ = sql::stmt::T_HBASE_CHECK_AND_DELETE;
        break;
      case ObTableProccessType::TABLE_API_HBASE_CHECK_AND_PUT:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(HBASEAPI_CHECK_PUT_COUNT);
          EVENT_ADD(HBASEAPI_CHECK_PUT_TIME, elapsed_us);
          EVENT_ADD(HBASEAPI_CHECK_PUT_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(hbase_check_and_put);
        audit_record.stmt_type_ = sql::stmt::T_HBASE_CHECK_AND_PUT;
        break;
      case ObTableProccessType::TABLE_API_HBASE_INCREMENT:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(HBASEAPI_INCREMENT_COUNT);
          EVENT_ADD(HBASEAPI_INCREMENT_TIME, elapsed_us);
          EVENT_ADD(HBASEAPI_INCREMENT_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(hbase_increment);
        audit_record.stmt_type_ = sql::stmt::T_HBASE_INCREMENT;
        break;
      case ObTableProccessType::TABLE_API_HBASE_APPEND:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(HBASEAPI_APPEND_COUNT);
          EVENT_ADD(HBASEAPI_APPEND_TIME, elapsed_us);
          EVENT_ADD(HBASEAPI_APPEND_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(hbase_append);
        audit_record.stmt_type_ = sql::stmt::T_HBASE_APPEND;
        break;
      case ObTableProccessType::TABLE_API_HBASE_HYBRID:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(HBASEAPI_HYBRID_COUNT);
          EVENT_ADD(HBASEAPI_HYBRID_TIME, elapsed_us);
          EVENT_ADD(HBASEAPI_HYBRID_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(hbase_hybrid);
        audit_record.stmt_type_ = sql::stmt::T_HBASE_OTHER;
        break;

      // table query
      case ObTableProccessType::TABLE_API_TABLE_QUERY:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_QUERY_COUNT);
          EVENT_ADD(TABLEAPI_QUERY_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_QUERY_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(table_query);
        audit_record.stmt_type_ = sql::stmt::T_KV_QUERY;
        break;

      // hbase query
      case ObTableProccessType::TABLE_API_HBASE_QUERY:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(HBASEAPI_SCAN_COUNT);
          EVENT_ADD(HBASEAPI_SCAN_TIME, elapsed_us);
          EVENT_ADD(HBASEAPI_SCAN_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(hbase_scan);
        audit_record.stmt_type_ = sql::stmt::T_HBASE_SCAN;
        break;

      // table query sync
      case ObTableProccessType::TABLE_API_TABLE_QUERY_SYNC:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_QUERY_COUNT);
          EVENT_ADD(TABLEAPI_QUERY_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_QUERY_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(table_query_sync);
        audit_record.stmt_type_ = sql::stmt::T_KV_QUERY;
        break;

      // hbase query sync
      case ObTableProccessType::TABLE_API_HBASE_QUERY_SYNC:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(HBASEAPI_SCAN_COUNT);
          EVENT_ADD(HBASEAPI_SCAN_TIME, elapsed_us);
          EVENT_ADD(HBASEAPI_SCAN_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(hbase_scan_sync);
        audit_record.stmt_type_ = sql::stmt::T_HBASE_SCAN;
        break;

      // table query_and_mutate
      case ObTableProccessType::TABLE_API_QUERY_AND_MUTATE:
        if (OB_SUCCESS == audit_record.status_) {
          EVENT_INC(TABLEAPI_QUERY_AND_MUTATE_COUNT);
          EVENT_ADD(TABLEAPI_QUERY_AND_MUTATE_TIME, elapsed_us);
          EVENT_ADD(TABLEAPI_QUERY_AND_MUTATE_ROW, rows);
        }
        SET_AUDIT_SQL_STRING(table_query_and_mutate);
        audit_record.stmt_type_ = sql::stmt::T_KV_QUERY_AND_MUTATE;
        break;

      default:
        SET_AUDIT_SQL_STRING(unknown);
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
