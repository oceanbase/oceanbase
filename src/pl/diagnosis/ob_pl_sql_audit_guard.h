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

#ifndef OCEANBASE_SRC_PL_SQL_AUDIT_GUARD_OB_PL_H_

#define OCEANBASE_SRC_PL_SQL_AUDIT_GUARD_OB_PL_H_

#include "sql/monitor/ob_exec_stat.h"
#include "observer/ob_inner_sql_connection.h"
#include "sql/resolver/ob_stmt_type.h"
#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{
class ObQueryRetryCtrl;
}
namespace sql
{
struct ObPLSPITraceIdGuard;
}
namespace pl
{

class ObPLTimeRecord : public observer::ObITimeRecord
{
public:
  ObPLTimeRecord()
    : send_timestamp_(0),
      receive_timestamp_(0),
      enqueue_timestamp_(0),
      run_timestamp_(0),
      process_timestamp_(0),
      single_process_timestamp_(0),
      exec_start_timestamp_(0),
      exec_end_timestamp_(0) {}
  virtual ~ObPLTimeRecord() {}

  void set_send_timestamp(int64_t send_timestamp) { send_timestamp_ = send_timestamp; }
  void set_receive_timestamp(int64_t receive_timestamp) { receive_timestamp_ = receive_timestamp; }
  void set_enqueue_timestamp(int64_t enqueue_timestamp) { enqueue_timestamp_ = enqueue_timestamp; }
  void set_run_timestamp(int64_t run_timestamp) { run_timestamp_ = run_timestamp; }
  void set_process_timestamp(int64_t process_timestamp) { process_timestamp_ = process_timestamp; }
  void set_single_process_timestamp(int64_t single_process_timestamp) { single_process_timestamp_ = single_process_timestamp; }
  void set_exec_start_timestamp(int64_t exec_start_timestamp) { exec_start_timestamp_ = exec_start_timestamp; }
  void set_exec_end_timestamp(int64_t exec_end_timestamp) { exec_end_timestamp_ = exec_end_timestamp; }

  int64_t get_send_timestamp() const { return send_timestamp_; }
  int64_t get_receive_timestamp() const { return get_send_timestamp(); }
  int64_t get_enqueue_timestamp() const { return get_send_timestamp(); }
  int64_t get_run_timestamp() const { return get_send_timestamp(); }
  int64_t get_process_timestamp() const { return get_send_timestamp(); }
  int64_t get_single_process_timestamp() const { return get_send_timestamp(); }
  int64_t get_exec_start_timestamp() const { return get_send_timestamp(); }
  int64_t get_exec_end_timestamp() const { return exec_end_timestamp_; }

public:
  int64_t send_timestamp_;
  int64_t receive_timestamp_;
  int64_t enqueue_timestamp_;
  int64_t run_timestamp_;
  int64_t process_timestamp_;
  int64_t single_process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;

};

class ObPLSqlAuditRecord
{
public:
  ObPLSqlAuditRecord(sql::ExecType exec_type_) {
    exec_timestamp_.exec_type_ = exec_type_;
  }
public:
  sql::ObExecRecord exec_record_;
  // sql::ObExecutingSqlStatRecord sqlstat_record_;
  sql::ObExecTimestamp exec_timestamp_;
  ObPLTimeRecord time_record_;
};

class ObPLSqlAuditGuard
{
public:
  ObPLSqlAuditGuard(sql::ObExecContext &exec_ctx,
                    sql::ObSQLSessionInfo &session_info,
                    sql::ObSPIResultSet &spi_result,
                    ObPLSqlAuditRecord &record,
                    int &ret,
                    ObString ps_sql,
                    observer::ObQueryRetryCtrl &retry_ctrl,
                    sql::ObPLSPITraceIdGuard &traceid_guard,
                    sql::stmt::StmtType stmt_type,
                    bool is_ps_cursor_open = false,
                    ObPLCursorInfo *cursor = nullptr);

  ~ObPLSqlAuditGuard();

private:
  bool enable_perf_event_;
  bool enable_sql_audit_;
  bool enable_sql_stat_;
  bool enable_sqlstat_;
  sql::ObExecContext &exec_ctx_;
  sql::ObSQLSessionInfo &session_info_;

  ObWaitEventDesc max_wait_desc_;
  ObWaitEventStat total_wait_desc_;
  ObMaxWaitGuard *max_wait_guard_;
  ObTotalWaitGuard *total_wait_guard_;
  char memory1[sizeof(ObMaxWaitGuard)];
  char memory2[sizeof(ObTotalWaitGuard)];
  sql::ObSPIResultSet &spi_result_;
  ObPLSqlAuditRecord &record_;

  int &ret_;
  ObString ps_sql_;
  observer::ObQueryRetryCtrl &retry_ctrl_;
  sql::ObPLSPITraceIdGuard &traceid_guard_;
  sql::stmt::StmtType stmt_type_;

  sql::ObExecutingSqlStatRecord sqlstat_record_;
  int64_t sql_used_memory_size_;
  observer::ObProcessMallocCallback pmcb_;
  lib::ObMallocCallbackGuard memory_guard_;
  int64_t plsql_compile_time_;
  bool is_ps_cursor_open_;
  ObPLCursorInfo *cursor_;
};

}
}

#endif /*OCEANBASE_SRC_PL_SQL_AUDIT_GUARD_OB_PL_H_*/