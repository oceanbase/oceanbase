
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

#define USING_LOG_PREFIX PL

#include "ob_pl_sql_audit_guard.h"
#include "sql/ob_spi.h"

namespace oceanbase
{
namespace pl
{

ObPLSqlAuditGuard::ObPLSqlAuditGuard(
    sql::ObExecContext &exec_ctx,
    ObSQLSessionInfo &session_info,
    ObSPIResultSet &spi_result,
    ObPLSqlAuditRecord &record,
    int &ret,
    ObString ps_sql,
    observer::ObQueryRetryCtrl &retry_ctrl,
    ObPLSPITraceIdGuard &traceid_guard,
    stmt::StmtType stmt_type,
    bool is_ps_cursor_open,
    ObPLCursorInfo *cursor)
  : exec_ctx_(exec_ctx),
    session_info_(session_info),
    spi_result_(spi_result),
    record_(record),
    ret_(ret),
    ps_sql_(ps_sql),
    retry_ctrl_(retry_ctrl),
    traceid_guard_(traceid_guard),
    stmt_type_(stmt_type),
    sql_used_memory_size_(0),
    pmcb_(0, sql_used_memory_size_),
    memory_guard_(pmcb_),
    is_ps_cursor_open_(is_ps_cursor_open),
    cursor_(cursor)

{
  enable_perf_event_ = lib::is_diagnose_info_enabled();
  enable_sql_audit_ = GCONF.enable_sql_audit && session_info_.get_local_ob_enable_sql_audit();
  enable_sql_stat_ = session_info.is_sqlstat_enabled();
  max_wait_guard_ = new (memory1) ObMaxWaitGuard(enable_perf_event_ ? &max_wait_desc_ : NULL);
  total_wait_guard_ = new (memory2) ObTotalWaitGuard(enable_perf_event_ ? &total_wait_desc_ : NULL);
  if (enable_perf_event_) {
    record_.exec_record_.record_start();
  }
  if (enable_sql_stat_ && OB_NOT_NULL(exec_ctx_.get_sql_ctx())) {
   sqlstat_record_.record_sqlstat_start_value();
   sqlstat_record_.set_is_in_retry(session_info_.get_is_in_retry());
   session_info_.sql_sess_record_sql_stat_start_value(sqlstat_record_);
  }
  // 监控项统计开始
  record_.time_record_.set_send_timestamp(ObTimeUtility::current_time());
  session_info_.get_raw_audit_record().sql_memory_used_ = &sql_used_memory_size_;
  plsql_compile_time_ = session_info_.get_plsql_compile_time();
  session_info_.reset_plsql_compile_time();
}

ObPLSqlAuditGuard::~ObPLSqlAuditGuard()
{
  int &ret = ret_;
  record_.time_record_.set_exec_end_timestamp(ObTimeUtility::current_time());
  if (enable_perf_event_) {
    record_.exec_record_.record_end();
  }
  if (enable_sql_stat_ && OB_NOT_NULL(exec_ctx_.get_sql_ctx()) && OB_NOT_NULL(spi_result_.get_result_set())) {
    sqlstat_record_.record_sqlstat_end_value();
    sqlstat_record_.set_is_plan_cache_hit(exec_ctx_.get_sql_ctx()->plan_cache_hit_);
    sqlstat_record_.set_is_muti_query(session_info_.get_capability().cap_flags_.OB_CLIENT_MULTI_STATEMENTS);
    if (OB_NOT_NULL(exec_ctx_.get_sql_ctx()) && OB_NOT_NULL(exec_ctx_.get_sql_ctx())) {
      sqlstat_record_.set_is_muti_query_batch(exec_ctx_.get_sql_ctx()->multi_stmt_item_.is_batched_multi_stmt());
    }
    if (OB_NOT_NULL(spi_result_.get_result_set()) && OB_NOT_NULL(spi_result_.get_result_set()->get_physical_plan())) {
      sqlstat_record_.set_is_full_table_scan(spi_result_.get_result_set()->get_physical_plan()->contain_table_scan());
    }
    sqlstat_record_.set_is_failed(0 != ret && OB_ITER_END != ret);
    sqlstat_record_.move_to_sqlstat_cache(
      session_info_, exec_ctx_.get_sql_ctx()->cur_sql_, spi_result_.get_result_set()->get_physical_plan());
  }
  max_wait_guard_->~ObMaxWaitGuard();
  total_wait_guard_->~ObTotalWaitGuard();

  LOG_TRACE("Start PL/Sql Audit Record"/*, KPC(this)*/ );

  if (OB_NOT_NULL(spi_result_.get_result_set())) {
    if (spi_result_.get_result_set()->is_inited()) {
      if (ObStmt::is_execute_stmt(stmt_type_)) {
        ps_sql_ = session_info_.get_current_query_string();
      }
      int64_t try_cnt = session_info_.get_raw_audit_record().try_cnt_;
      ObExecRecord record_bak = session_info_.get_raw_audit_record().exec_record_;
      session_info_.get_raw_audit_record().try_cnt_ = retry_ctrl_.get_retry_times();
      session_info_.get_raw_audit_record().pl_trace_id_.set(traceid_guard_.origin_trace_id_);
      if (ret_ == OB_SUCCESS && is_ps_cursor_open_) {
        // if ps cursor open succeed, record audit after fill cursor
      } else {
        observer::ObInnerSQLConnection::process_record(*(spi_result_.get_result_set()),
                                                      spi_result_.get_sql_ctx(),
                                                      session_info_,
                                                      record_.time_record_,
                                                      ret_,
                                                      session_info_.get_current_execution_id(),
                                                      OB_INVALID_ID, //FIXME@hr351303
                                                      max_wait_desc_,
                                                      total_wait_desc_,
                                                      record_.exec_record_,
                                                      record_.exec_timestamp_,
                                                      true,
                                                      ps_sql_,
                                                      true,
                                                      spi_result_.get_exec_params_str_ptr(),
                                                      false,
                                                      cursor_);
        session_info_.get_raw_audit_record().exec_record_ = record_bak;
        session_info_.get_raw_audit_record().try_cnt_ = try_cnt;
        session_info_.get_raw_audit_record().pl_trace_id_.reset();
      }
    } else {
      LOG_DEBUG("result set is not inited, do not process record", K(ret_), K(ps_sql_));
    }
  } else {
    if (OB_SUCCESS == ret_) {
      ret_ = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, result_set is null", K(ret_), K(ps_sql_));
    } else {
      LOG_WARN("result_set is null", K(ret_), K(ps_sql_));
    }
  }
  if(nullptr != session_info_.get_raw_audit_record().sql_memory_used_) {
    session_info_.get_raw_audit_record().sql_memory_used_ = nullptr;
  }
  session_info_.add_plsql_compile_time(plsql_compile_time_);
}

} // namespace pl
} // namespace oceanbase
