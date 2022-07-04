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

#define USING_LOG_PREFIX SERVER

#include "observer/mysql/ob_sync_plan_driver.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/packet/ompk_resheader.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/profile/ob_perf_event.h"
#include "obsm_row.h"
#include "observer/mysql/obmp_query.h"
#include "sql/engine/px/ob_px_admission.h"

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace obmysql;
namespace observer {

ObSyncPlanDriver::ObSyncPlanDriver(const ObGlobalContext &gctx, const ObSqlCtx &ctx, sql::ObSQLSessionInfo &session,
    ObQueryRetryCtrl &retry_ctrl, ObIMPPacketSender &sender)
    : ObQueryDriver(gctx, ctx, session, retry_ctrl, sender)
{}

ObSyncPlanDriver::~ObSyncPlanDriver()
{}

int ObSyncPlanDriver::response_result(ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  bool process_ok = false;
  // for select SQL
  bool ac = true;
  bool admission_fail_and_need_retry = false;
  const ObNewRow *not_used_row = NULL;
  if (OB_ISNULL(result.get_physical_plan())) {
    ret = OB_NOT_INIT;
    LOG_WARN("should have set plan to result set", K(ret));
  } else if (OB_FAIL(session_.get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else if (OB_FAIL(result.sync_open())) {
    int cret = OB_SUCCESS;
    int cli_ret = OB_SUCCESS;
    // move result.close() below, after test_and_save_retry_state().
    // open fail,check whether need retry
    retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, result, ret, cli_ret);
    if (OB_TRANSACTION_SET_VIOLATION != ret && OB_REPLICA_NOT_READABLE != ret) {
      if (OB_TRY_LOCK_ROW_CONFLICT == ret && retry_ctrl_.need_retry()) {
        // retry the lock conflict without printing the log to avoid refreshing the screen
      } else {
        LOG_WARN("result set open failed, check if need retry", K(ret), K(cli_ret), K(retry_ctrl_.need_retry()));
      }
    }
    cret = result.close(retry_ctrl_.need_retry());
    if (cret != OB_SUCCESS && cret != OB_TRANSACTION_SET_VIOLATION && OB_TRY_LOCK_ROW_CONFLICT != cret) {
      LOG_WARN("close result set fail", K(cret));
    }
    ret = cli_ret;
  } else if (result.is_with_rows()) {
    // Is the result set, do not try again after starting to send data
    bool can_retry = false;
    if (OB_FAIL(response_query_result(result, result.has_more_result(), can_retry))) {
      LOG_WARN("response query result fail", K(ret));
      // move result.close() below, after test_and_save_retry_state().
      if (can_retry) {
        // I can try again, here to determine if you want to try again
        int cli_ret = OB_SUCCESS;
        // response query result fail,check whether need retry
        retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, result, ret, cli_ret);
        LOG_WARN("result response failed, check if need retry", K(ret), K(cli_ret), K(retry_ctrl_.need_retry()));
        ret = cli_ret;
      } else {
        ObResultSet::refresh_location_cache(result.get_exec_context().get_task_exec_ctx(), true, ret);
      }
      // After judging whether you need to retry, we won't judge whether to retry later
      THIS_WORKER.disable_retry();
      int cret = result.close(retry_ctrl_.need_retry());
      if (cret != OB_SUCCESS) {
        LOG_WARN("close result set fail", K(cret));
      }
    } else if (FALSE_IT(THIS_WORKER.disable_retry())) {  // response succeed,not need retry
    } else if (OB_FAIL(result.close())) {
      LOG_WARN("close result set fail", K(ret));
    } else {
      process_ok = true;

      OMPKEOF eofp;
      const ObWarningBuffer *warnings_buf = common::ob_get_tsi_warning_buffer();
      uint16_t warning_count = 0;
      if (OB_ISNULL(warnings_buf)) {
        LOG_WARN("can not get thread warnings buffer");
      } else {
        warning_count = static_cast<uint16_t>(warnings_buf->get_readable_warning_count());
      }
      eofp.set_warning_count(warning_count);
      ObServerStatusFlags flags = eofp.get_server_status();
      flags.status_flags_.OB_SERVER_STATUS_IN_TRANS = (session_.is_server_status_in_transaction() ? 1 : 0);
      flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (ac ? 1 : 0);
      flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = result.has_more_result();
      if (!session_.is_obproxy_mode()) {
        // in java client or others, use slow query bit to indicate partition hit or not
        flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !session_.partition_hit().get_bool();
      }

      eofp.set_server_status(flags);

      // for proxy
      // in multi-stmt, send extra ok packet in the last stmt(has no more result)
      if (!result.has_more_result()) {
        sender_.update_last_pkt_pos();
      }
      if (OB_SUCC(ret) && !result.get_is_com_filed_list() && OB_FAIL(sender_.response_packet(eofp))) {
        LOG_WARN("response packet fail", K(ret));
      }
      // for obproxy
      if (OB_SUCC(ret)) {

        // in multi-stmt, send extra ok packet in the last stmt(has no more result)
        if (sender_.need_send_extra_ok_packet() && !result.has_more_result()) {
          ObOKPParam ok_param;
          ok_param.affected_rows_ = 0;
          ok_param.is_partition_hit_ = session_.partition_hit().get_bool();
          ok_param.has_more_result_ = result.has_more_result();
          if (OB_FAIL(sender_.send_ok_packet(session_, ok_param))) {
            LOG_WARN("fail to send ok packt", K(ok_param), K(ret));
          }
        }
      }
    }
  } else {
    // is not result_set,open succeed, and not need retry
    THIS_WORKER.disable_retry();
    if (OB_FAIL(result.close())) {
      LOG_WARN("close result set fail", K(ret));
    } else {
      if (!result.has_implicit_cursor()) {
        // no implicit cursor, send one ok packet to client
        ObOKPParam ok_param;
        ok_param.message_ = const_cast<char *>(result.get_message());
        ok_param.affected_rows_ = result.get_affected_rows();
        ok_param.lii_ = result.get_last_insert_id_to_client();
        const ObWarningBuffer *warnings_buf = common::ob_get_tsi_warning_buffer();
        if (OB_ISNULL(warnings_buf)) {
          LOG_WARN("can not get thread warnings buffer");
        } else {
          ok_param.warnings_count_ = static_cast<uint16_t>(warnings_buf->get_readable_warning_count());
        }
        ok_param.is_partition_hit_ = session_.partition_hit().get_bool();
        ok_param.has_more_result_ = result.has_more_result();
        process_ok = true;
        if (OB_FAIL(sender_.send_ok_packet(session_, ok_param))) {
          LOG_WARN("send ok packet fail", K(ok_param), K(ret));
        }
      } else {
        // has implicit cursor, send ok packet to client by implicit cursor
        result.reset_implicit_cursor_idx();
        while (OB_SUCC(ret) && OB_SUCC(result.switch_implicit_cursor())) {
          ObOKPParam ok_param;
          ok_param.message_ = const_cast<char *>(result.get_message());
          ok_param.affected_rows_ = result.get_affected_rows();
          ok_param.is_partition_hit_ = session_.partition_hit().get_bool();
          ok_param.has_more_result_ = !result.is_cursor_end();
          process_ok = true;
          if (OB_FAIL(sender_.send_ok_packet(session_, ok_param))) {
            LOG_WARN("send ok packet failed", K(ret), K(ok_param));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("send implicit cursor info to client failed", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret) && !process_ok && !retry_ctrl_.need_retry() && !admission_fail_and_need_retry &&
      OB_BATCHED_MULTI_STMT_ROLLBACK != ret) {
    // if OB_BATCHED_MULTI_STMT_ROLLBACK is err ret of batch stmt rollback,not return to client, retry
    int sret = OB_SUCCESS;
    bool is_partition_hit = session_.get_err_final_partition_hit(ret);
    if (OB_SUCCESS != (sret = sender_.send_error_packet(ret, NULL, is_partition_hit))) {
      LOG_WARN("send error packet fail", K(sret), K(ret));
    }
  }

  return ret;
}

int ObSyncPlanDriver::response_query_result(
    ObResultSet &result, bool has_more_result, bool &can_retry, int64_t fetch_limit)
{
  int ret = OB_SUCCESS;
  can_retry = true;
  bool is_first_row = true;
  const ObNewRow *result_row = NULL;
  bool has_top_limit = result.get_has_top_limit();
  bool is_cac_found_rows = result.is_calc_found_rows();
  int64_t limit_count = OB_INVALID_COUNT == fetch_limit ? INT64_MAX : fetch_limit;
  int64_t row_num = 0;
  if (!has_top_limit && OB_INVALID_COUNT == fetch_limit) {
    limit_count = INT64_MAX;
    if (OB_FAIL(session_.get_sql_select_limit(limit_count))) {
      LOG_WARN("fail tp get sql select limit", K(ret));
    }
  }
  session_.get_trans_desc().consistency_wait();
  MYSQL_PROTOCOL_TYPE protocol_type = result.is_ps_protocol() ? BINARY : TEXT;
  const common::ColumnsFieldIArray *fields = NULL;
  ObArenaAllocator *convert_allocator = NULL; //just for convert charset
  if (OB_SUCC(ret)) {
    fields = result.get_field_columns();
    if (OB_ISNULL(fields)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fields is null", K(ret), KP(fields));
    }
  }
  while (OB_SUCC(ret) && row_num < limit_count && !OB_FAIL(result.get_next_row(result_row))) {
    ObNewRow *row = const_cast<ObNewRow *>(result_row);
    // If it is the first line, first reply to the client with field and other information
    if (is_first_row) {
      is_first_row = false;
      can_retry = false;  // The first row of data has been obtained, so not need try again
      if (OB_FAIL(response_query_header(result, has_more_result))) {
        LOG_WARN("fail to response query header", K(ret), K(row_num), K(can_retry));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(convert_allocator))) {
        LOG_WARN("fail to get lob fake allocator", K(ret));
      } else if (OB_ISNULL(convert_allocator)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocator is unexpected", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row->get_count(); i++) {
      ObObj &value = row->get_cell(i);
      if (result.is_ps_protocol()) {
        if (value.get_type() != fields->at(i).type_.get_type()) {
          ObCastCtx cast_ctx(convert_allocator, NULL, CM_WARN_ON_FAIL, CS_TYPE_INVALID);
          if (OB_FAIL(common::ObObjCaster::to_type(fields->at(i).type_.get_type(), cast_ctx, value, value))) {
            LOG_WARN("failed to cast object", K(ret), K(value), K(value.get_type()), K(fields->at(i).type_.get_type()));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (ob_is_string_type(value.get_type()) && CS_TYPE_INVALID != value.get_collation_type()) {
        OZ(convert_string_value_charset(value, result));
      } else if (value.is_clob_locator() && OB_FAIL(convert_lob_value_charset(value, result))) {
        LOG_WARN("convert lob value charset failed", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(convert_lob_locator_to_longtext(value, result))) {
        LOG_WARN("convert lob locator to longtext failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(&session_);
      ObSMRow sm_row(protocol_type,
          *row,
          dtc_params,
          result.get_field_columns(),
          ctx_.schema_guard_,
          session_.get_effective_tenant_id());
      OMPKRow rp(sm_row);
      if (OB_FAIL(sender_.response_packet(rp))) {
        LOG_WARN("response packet fail", K(ret), KP(row), K(row_num), K(can_retry));
        // break;
      } else {
        convert_allocator->reset();
      }
      if (OB_SUCC(ret)) {
        ++row_num;
      }
    }
  }
  if (is_cac_found_rows) {
    while (OB_SUCC(ret) && !OB_FAIL(result.get_next_row(result_row))) {
      // nothing
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("fail to iterate and response", K(ret), K(row_num), K(can_retry));
  }
  if (OB_SUCC(ret) && 0 == row_num) {
    // If there is no data, still have to reply to the client with fields and other information,
    // and do not try again
    can_retry = false;
    if (OB_FAIL(response_query_header(result, has_more_result))) {
      LOG_WARN("fail to response query header", K(ret), K(row_num), K(can_retry));
    }
  }
  return ret;
}

ObRemotePlanDriver::ObRemotePlanDriver(const ObGlobalContext &gctx, const ObSqlCtx &ctx, sql::ObSQLSessionInfo &session,
    ObQueryRetryCtrl &retry_ctrl, ObIMPPacketSender &sender)
    : ObSyncPlanDriver(gctx, ctx, session, retry_ctrl, sender)
{}

int ObRemotePlanDriver::response_result(ObMySQLResultSet &result)
{
  int ret = result.get_errcode();
  bool process_ok = false;
  // for select SQL
  bool ac = true;
  if (OB_FAIL(ret)) {
    int cli_ret = OB_SUCCESS;
    // response query result fail,check consider whether need retry
    retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, result, ret, cli_ret);
    LOG_WARN("result response failed, check if need retry",
        K(ret),
        K(cli_ret),
        K(retry_ctrl_.need_retry()),
        K(session_.get_retry_info()),
        K(session_.get_last_query_trace_id()));
    ret = cli_ret;
    THIS_WORKER.disable_retry();
    int cret = result.close(retry_ctrl_.need_retry());
    if (cret != OB_SUCCESS) {
      LOG_WARN("close result set fail", K(cret));
    }
  } else if (OB_FAIL(result.open())) {
    LOG_WARN("open result set failed", K(ret));
  } else if (result.is_with_rows()) {
    bool can_retry = false;
    if (OB_FAIL(response_query_result(result, result.has_more_result(), can_retry))) {
      LOG_WARN("response query result fail", K(ret));
      // move result.close() below, after test_and_save_retry_state().
      if (can_retry) {
        int cli_ret = OB_SUCCESS;
        retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, result, ret, cli_ret);
        LOG_WARN("result response failed, check if need retry", K(ret), K(cli_ret), K(retry_ctrl_.need_retry()));
        ret = cli_ret;
      }
    }
    THIS_WORKER.disable_retry();
    int cret = result.close(retry_ctrl_.need_retry());
    if (cret != OB_SUCCESS) {
      LOG_WARN("close result set fail", K(cret));
    }
    ret = (OB_SUCCESS == ret) ? cret : ret;
    if (OB_SUCC(ret)) {
      process_ok = true;
      OMPKEOF eofp;
      const ObWarningBuffer *warnings_buf = common::ob_get_tsi_warning_buffer();
      uint16_t warning_count = 0;
      if (OB_ISNULL(warnings_buf)) {
        LOG_WARN("can not get thread warnings buffer");
      } else {
        warning_count = static_cast<uint16_t>(warnings_buf->get_readable_warning_count());
      }
      eofp.set_warning_count(warning_count);
      ObServerStatusFlags flags = eofp.get_server_status();
      flags.status_flags_.OB_SERVER_STATUS_IN_TRANS = (session_.is_server_status_in_transaction() ? 1 : 0);
      flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (ac ? 1 : 0);
      flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = result.has_more_result();
      if (!session_.is_obproxy_mode()) {
        // in java client or others, use slow query bit to indicate partition hit or not
        flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !session_.partition_hit().get_bool();
      }

      eofp.set_server_status(flags);
      // for proxy
      // in multi-stmt, send extra ok packet in the last stmt(has no more result)
      sender_.update_last_pkt_pos();
      if (OB_SUCC(ret) && OB_FAIL(sender_.response_packet(eofp))) {
        LOG_WARN("response packet fail", K(ret));
      }
      // for obproxy
      if (OB_SUCC(ret)) {
        // in multi-stmt, send extra ok packet in the last stmt(has no more result)
        if (sender_.need_send_extra_ok_packet() && !result.has_more_result()) {
          ObOKPParam ok_param;
          ok_param.affected_rows_ = 0;
          ok_param.is_partition_hit_ = session_.partition_hit().get_bool();
          ok_param.has_more_result_ = result.has_more_result();
          if (OB_FAIL(sender_.send_ok_packet(session_, ok_param))) {
            LOG_WARN("fail to send ok packt", K(ok_param), K(ret));
          }
        }
      }
    }
  } else {
    THIS_WORKER.disable_retry();
    if (OB_FAIL(result.close())) {
      LOG_WARN("close result set fail", K(ret));
    } else if (!result.has_implicit_cursor()) {
      // no implicit cursor, send one ok packet to client
      ObOKPParam ok_param;
      ok_param.message_ = const_cast<char *>(result.get_message());
      ok_param.affected_rows_ = result.get_affected_rows();
      ok_param.lii_ = result.get_last_insert_id_to_client();
      const ObWarningBuffer *warnings_buf = common::ob_get_tsi_warning_buffer();
      if (OB_ISNULL(warnings_buf)) {
        LOG_WARN("can not get thread warnings buffer");
      } else {
        ok_param.warnings_count_ = static_cast<uint16_t>(warnings_buf->get_readable_warning_count());
      }
      ok_param.is_partition_hit_ = session_.partition_hit().get_bool();
      ok_param.has_more_result_ = result.has_more_result();
      process_ok = true;
      if (OB_FAIL(sender_.send_ok_packet(session_, ok_param))) {
        LOG_WARN("send ok packet fail", K(ok_param), K(ret));
      }
    } else {
      // has implicit cursor, send ok packet to client by implicit cursor
      result.reset_implicit_cursor_idx();
      while (OB_SUCC(ret) && OB_SUCC(result.switch_implicit_cursor())) {
        ObOKPParam ok_param;
        ok_param.message_ = const_cast<char *>(result.get_message());
        ok_param.affected_rows_ = result.get_affected_rows();
        ok_param.is_partition_hit_ = session_.partition_hit().get_bool();
        ok_param.has_more_result_ = !result.is_cursor_end();
        process_ok = true;
        if (OB_FAIL(sender_.send_ok_packet(session_, ok_param))) {
          LOG_WARN("send ok packet failed", K(ret), K(ok_param));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("send implicit cursor info to client failed", K(ret));
      }
    }
  }

  if (!retry_ctrl_.need_retry()) {
    if (OB_FAIL(ret) && !process_ok) {
      int sret = OB_SUCCESS;
      bool is_partition_hit = session_.get_err_final_partition_hit(ret);
      if (OB_SUCCESS != (sret = sender_.send_error_packet(ret, NULL, is_partition_hit))) {
        LOG_WARN("send error packet fail", K(sret), K(ret));
      }
    }
  }

  return ret;
}
}  // namespace observer
}  // namespace oceanbase
