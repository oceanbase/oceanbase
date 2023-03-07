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


#include "observer/mysql/obmp_stmt_prexecute.h"
#include "lib/oblog/ob_log.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/encode/ob_base64_encode.h"
#include "observer/mysql/obsm_utils.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/packet/ompk_prepare.h"
#include "rpc/obmysql/packet/ompk_prexecute.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/packet/ompk_resheader.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "rpc/obmysql/obsm_struct.h"
#include "observer/mysql/obsm_row.h"
#include "observer/mysql/obmp_stmt_prepare.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_time_utility2.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "observer/omt/ob_tenant.h"
#include "observer/mysql/ob_sync_plan_driver.h"
#include "observer/mysql/ob_sync_cmd_driver.h"
#include "observer/mysql/ob_async_cmd_driver.h"
#include "observer/mysql/ob_async_plan_driver.h"
#include "observer/mysql/obmp_stmt_send_piece_data.h"
#include "observer/ob_req_time_service.h"
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_exception_handling.h"
#include "rpc/obmysql/ob_mysql_field.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace obmysql;
using namespace rpc;
using namespace sql;
using namespace pl;
namespace observer
{
ObMPStmtPrexecute::ObMPStmtPrexecute(const ObGlobalContext &gctx)
    : ObMPStmtExecute(gctx),
    sql_(),
    sql_len_(0),
    iteration_count_(0),
    exec_mode_(0),
    close_stmt_count_(0),
    extend_flag_(0),
    first_time_(false),
    allocator_(NULL)
{}

int ObMPStmtPrexecute::before_process()
{
  int ret = OB_SUCCESS;
  bool use_sess_trace = false;

  if (OB_FAIL(ObMPBase::before_process())) {
    LOG_WARN("fail to call before process", K(ret));
  } else if ((OB_ISNULL(req_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("request should not be null", K(ret));
  } else if (req_->get_type() != ObRequest::OB_MYSQL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid request", K(ret), K_(*req));
  } else {
    // its life cycle is very important
    allocator_ = &CURRENT_CONTEXT->get_arena_allocator();
    ParamStore *params = NULL;
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query allocator is null.");
    } else if (OB_ISNULL(params = static_cast<ParamStore *>(allocator_->alloc(sizeof(ParamStore))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      params = new(params)ParamStore( (ObWrapperAllocator(*allocator_)) );
      set_param(params);
    }
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    const char* pos = pkt.get_cdata();
    // stmt_id
    int32_t stmt_id = -1;
    ObMySQLUtil::get_int4(pos, stmt_id);
    stmt_id_ = stmt_id;

    // flags
    int8_t flag = 0;
    ObMySQLUtil::get_int1(pos, flag);

    // iteration_count
    ObMySQLUtil::get_int4(pos, iteration_count_);

    // sql
    if (OB_SUCC(ret) && OB_FAIL(ObMySQLUtil::get_length(pos, sql_len_))) {
      LOG_WARN("failed to get length", K(ret));
    } else {
      sql_.assign_ptr(pos, static_cast<ObString::obstr_size_t>(sql_len_));
      pos += sql_len_;
      LOG_DEBUG("get sql in prexecute protocol.", K(stmt_id_), K(sql_));
    }

    // params_num
    int32_t num = 0;
    ObMySQLUtil::get_int4(pos, num);
    set_param_num(num);

    ObSQLSessionInfo *session = NULL;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(get_session(session))) {
      LOG_WARN("get session failed");
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL or invalid", K(ret), K(session));
    } else {
      uint32_t ps_stmt_checksum = DEFAULT_ITERATION_COUNT;
      ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
      session->set_current_trace_id(ObCurTraceId::get_trace_id());
      session->set_proxy_version(get_proxy_version());
      int64_t packet_len = (reinterpret_cast<const ObMySQLRawPacket &>
                                (req_->get_packet())).get_clen();
      ObReqTimeGuard req_timeinfo_guard;
      SMART_VAR(ObMySQLResultSet, result, *session, THIS_WORKER.get_allocator()) {
        result.set_has_more_result(false);
        if (sql_len_ > 0 && 0 == stmt_id_) {
          int64_t query_timeout = 0;
          int64_t tenant_version = 0;
          int64_t sys_version = 0;
          bool force_sync_resp = false;
          bool need_response_error = false;
          if (OB_UNLIKELY(!session->is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("invalid session", K_(sql), K(ret));
          } else if (OB_UNLIKELY(session->is_zombie())) {
            ret = OB_ERR_SESSION_INTERRUPTED;
            LOG_WARN("session has been killed", K(session->get_session_state()), K_(sql),
                    K(session->get_sessid()), "proxy_sessid", session->get_proxy_sessid(), K(ret));
          } else if (OB_UNLIKELY(packet_len > session->get_max_packet_size())) {
            ret = OB_ERR_NET_PACKET_TOO_LARGE;
            LOG_WARN("packet too large than allowd for the session", K_(sql), K(ret));
          } else if (OB_FAIL(session->get_query_timeout(query_timeout))) {
            LOG_WARN("fail to get query timeout", K_(sql), K(ret));
          } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                      session->get_effective_tenant_id(), tenant_version))) {
            LOG_WARN("fail get tenant broadcast version", K(ret));
          } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                      OB_SYS_TENANT_ID, sys_version))) {
            LOG_WARN("fail get tenant broadcast version", K(ret));
          } else if (OB_FAIL(ObMPStmtPrepare::multiple_query_check(*session,
                                                                    sql_,
                                                                    force_sync_resp,
                                                                    need_response_error))) {
            LOG_WARN("not support multiple_query", K(ret));
          } else {
            THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout);
            retry_ctrl_.set_tenant_global_schema_version(tenant_version);
            retry_ctrl_.set_sys_global_schema_version(sys_version);
            if (OB_FAIL(init_process_var(get_ctx(), ObMultiStmtItem(false, 0, ObString()), *session,
                                         use_sess_trace))) {
              LOG_WARN("init process var faield.", K(ret));
            } else if (OB_FAIL(check_and_refresh_schema(session->get_login_tenant_id(),
                                                        session->get_effective_tenant_id()))) {
              LOG_WARN("failed to check_and_refresh_schema", K(ret));
            } else {
              do {
                share::schema::ObSchemaGetterGuard schema_guard;
                const uint64_t tenant_id = session->get_effective_tenant_id();
                ObVirtualTableIteratorFactory vt_iter_factory(*gctx_.vt_iter_creator_);
                retry_ctrl_.clear_state_before_each_retry(session->get_retry_info_for_update());
                if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
                  LOG_WARN("get schema guard failed", K(ret));
                } else if (OB_FAIL(schema_guard.get_schema_version(
                                    session->get_effective_tenant_id(), tenant_version))) {
                  LOG_WARN("fail get schema version", K(ret));
                } else if (OB_FAIL(schema_guard.get_schema_version(
                                    OB_SYS_TENANT_ID, sys_version))) {
                  LOG_WARN("fail get sys schema version", K(ret));
                } else if (OB_ISNULL(result.get_exec_context().get_task_executor_ctx())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_ERROR("task executor ctx can not be NULL", K(ret));
                } else {
                  retry_ctrl_.set_tenant_local_schema_version(tenant_version);
                  retry_ctrl_.set_sys_local_schema_version(sys_version);
                  ObTaskExecutorCtx *task_ctx = result.get_exec_context().get_task_executor_ctx();
                  task_ctx->set_query_tenant_begin_schema_version(
                                retry_ctrl_.get_tenant_global_schema_version());
                  task_ctx->set_query_sys_begin_schema_version(
                                retry_ctrl_.get_sys_global_schema_version());
                  task_ctx->set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
                  get_ctx().schema_guard_ = &schema_guard;
                  get_ctx().session_info_ = session;
                  get_ctx().retry_times_ = get_retry_ctrl().get_retry_times();
                  get_ctx().self_add_plan_ = false;
                  get_ctx().is_prepare_protocol_ = true; //set to prepare protocol
                  get_ctx().is_pre_execute_ = true;
                  get_ctx().is_prepare_stage_ = true;
                  if (OB_FAIL(result.init())) {
                    LOG_WARN("result set init failed", K(ret));
                  } else if (OB_FAIL(ObMPBase::set_session_active(sql_, *session, ObTimeUtil::current_time(),
                                  obmysql::ObMySQLCmd::COM_STMT_PREPARE))) {
                    LOG_WARN("fail to set session active", K(ret));
                  }
                  if (OB_SUCC(ret)) {
                    if (OB_FAIL(gctx_.sql_engine_->stmt_prepare(sql_,
                                                                get_ctx(),
                                                                result,
                                                                false/*is_inner_sql*/))) {
                      set_exec_start_timestamp(ObTimeUtility::current_time());
                      int cli_ret = OB_SUCCESS;
                      get_retry_ctrl().test_and_save_retry_state(gctx_,
                                                                 get_ctx(),
                                                                 result,
                                                                 ret,
                                                                 cli_ret);
                      LOG_WARN("run stmt_query failed, check if need retry",
                               K(ret), K(cli_ret), K(get_retry_ctrl().need_retry()), K(sql_));
                      ret = cli_ret;
                    } else {
                      if (session->get_in_transaction()) {
                        if (ObStmt::is_write_stmt(result.get_stmt_type(),
                                                  result.has_global_variable())) {
                          session->set_has_exec_write_stmt(true);
                        }
                      } else {
                        session->set_has_exec_write_stmt(false);
                      }
                    }
                    session->set_session_in_retry(retry_ctrl_.need_retry());
                  }
                }
              } while (RETRY_TYPE_LOCAL == retry_ctrl_.get_retry_type());
              if (OB_SUCC(ret) && retry_ctrl_.get_retry_times() > 0) {
                LOG_TRACE("sql retry succeed", K(ret),
                          "retry_times", retry_ctrl_.get_retry_times());
              }
            }
            if (OB_SUCC(ret)) {
              stmt_id_ = result.get_statement_id();
              first_time_ = true; // need prepare at first time
              LOG_DEBUG("execute prexecute success.", K(stmt_id_), K(iteration_count_),
                K(get_param_num()), K(result.get_stmt_type()));
            }
            get_ctx().is_prepare_protocol_ = false; //set to prepare protocol
            get_ctx().is_prepare_stage_ = false;
          }
        } else {
          get_ctx().is_pre_execute_ = true;
        }
        if (OB_SUCC(ret)) {
          ObPsSessionInfo *ps_session_info = NULL;
          if (OB_FAIL(session->get_ps_session_info(stmt_id_, ps_session_info))) {
            LOG_WARN("get_ps_session_info failed", K(ret), K_(stmt_id));
          } else {
            stmt_type_ = ps_session_info->get_stmt_type();
            if (is_arraybinding_has_result_type(stmt_type_) && iteration_count_ > 1) {
              set_arraybounding(true);
              if (get_ctx().can_reroute_sql_) {
                get_ctx().can_reroute_sql_ = false;
                LOG_INFO("arraybinding not support reroute sql.");
              }
              // only init param_store
              // array_binding_row_ and array_binding_columns_ will init later
              OZ (init_arraybinding_paramstore(*allocator_));
            } else {
              set_arraybounding(false);
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(request_params(session, pos, ps_stmt_checksum, *allocator_, params_num_))) {
              LOG_WARN("prepare-execute protocol get params request failed", K(ret));
            } else {
              ObMySQLUtil::get_uint4(pos, exec_mode_);
              // https://yuque.antfin.com/docs/share/a5705d97-1d74-4b90-8be2-6e500249345f?#
              // is_commit_on_success_ is not use yet
              // other exec_mode set use ==
              is_commit_on_success_ = exec_mode_ & OB_OCI_COMMIT_ON_SUCCESS;
              exec_mode_ = exec_mode_ & (0xffffffff - OB_OCI_COMMIT_ON_SUCCESS);
              if (OB_OCI_BATCH_ERRORS == exec_mode_ && !is_pl_stmt(stmt_type_)) {
                set_save_exception(true);
              }
              if (OB_SUCC(ret)) {
                ObMySQLUtil::get_uint4(pos, close_stmt_count_);
                int tmp_ret = OB_SUCCESS;
                if (0 != close_stmt_count_) {
                  LOG_INFO("close stmt count:", K(close_stmt_count_), K(stmt_id_));
                  // OCI not support close_stmt_count_ is not 0 yet.
                  // for (int64_t i = 0; i < close_stmt_count_; i++) {
                  //   int32_t close_stmt_id = -1;
                  //   ObMySQLUtil::get_int4(pos, close_stmt_id);
                  //   if (OB_NOT_NULL(session->get_cursor(close_stmt_id))) {
                  //     if (OB_FAIL(session->close_cursor(close_stmt_id))) {
                  //       tmp_ret = ret;
                  //       LOG_WARN("fail to close cursor", K(ret), K(stmt_id_), K(close_stmt_id), K(session->get_sessid()));
                  //     }
                  //   }
                  //   if (OB_FAIL(session->close_ps_stmt(close_stmt_id))) {
                  //     LOG_WARN("close ps stmt fail in prepare-execute.", K(stmt_id_), K(close_stmt_id));
                  //   }
                  //   if (OB_SUCCESS != tmp_ret) {
                  //     ret = tmp_ret;
                  //   }
                  // }
                }
              }
              if (OB_SUCC(ret)) {
                ObMySQLUtil::get_uint4(pos, ps_stmt_checksum);
                if (DEFAULT_ITERATION_COUNT == ps_stmt_checksum
                      || (OB_NOT_NULL(ps_session_info)
                          && ps_stmt_checksum != ps_session_info->get_ps_stmt_checksum())) {
                    ret = OB_ERR_PREPARE_STMT_CHECKSUM;
                    LOG_ERROR("ps stmt checksum fail", K(ret), "session_id", session->get_sessid(),
                                                    K(ps_stmt_checksum), K(*ps_session_info));
                } else {
                    ObMySQLUtil::get_uint4(pos, extend_flag_);
                }
              }
              if (OB_FAIL(ret)) {
                // do nothing
              } else if (is_send_long_data()) {
                // response prepare packet
                if (!first_time_) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("send long data need stmt_id is 0.", K(ret));
                } else {
                  OMPKPrepare prepare_packet;
                  prepare_packet.set_statement_id(static_cast<uint32_t>(result.get_statement_id()));
                  prepare_packet.set_column_num(static_cast<uint16_t>(result.get_field_cnt()));
                  prepare_packet.set_warning_count(static_cast<uint16_t>(result.get_warning_count()));
                  if (OB_ISNULL(result.get_param_fields())) {
                    ret = OB_INVALID_ARGUMENT;
                    LOG_WARN("invalid argument", K(ret), K(result.get_param_fields()));
                  } else {
                    prepare_packet.set_param_num(
                      static_cast<uint16_t>(result.get_param_fields()->count()));
                  }
                  if (OB_SUCC(ret) && OB_FAIL(response_packet(prepare_packet, &result.get_session()))) {
                    LOG_WARN("response packet failed", K(ret));
                  }
                }
              }
              LOG_DEBUG("after get packet in prexecute protocol.", K(close_stmt_count_),
                      K(exec_mode_), K(extend_flag_));
            }
          }
        }
      }
      session->set_last_trace_id(ObCurTraceId::get_trace_id());
    }
    if (session != NULL) {
      revert_session(session);
    }
  }

  if (OB_FAIL(ret)) {
    send_error_packet(ret, NULL);
    if (OB_ERR_PREPARE_STMT_CHECKSUM == ret) {
      force_disconnect();
      LOG_WARN("prepare stmt checksum error, disconnect connection", K(ret));
    }
    OZ (flush_buffer(true));
  }

  return ret;
}

int ObMPStmtPrexecute::execute_response(ObSQLSessionInfo &session,
                                        ParamStore &params,
                                        sql::ObSqlCtx &ctx,
                                        ObMySQLResultSet &result,
                                        ObQueryRetryCtrl &retry_ctrl,
                                        const bool enable_perf_event,
                                        bool &need_response_error,
                                        bool &is_diagnostics_stmt,
                                        int64_t &execution_id,
                                        const bool force_sync_resp,
                                        bool &async_resp_used)
{
  int ret = OB_SUCCESS;
  if (OB_OCI_EXACT_FETCH != exec_mode_ && stmt::T_SELECT == stmt_type_) {
    LOG_DEBUG("begin server cursor.");
    set_ps_cursor_type(ObPrexecutePsCursorType);
    ObDbmsCursorInfo *cursor = NULL;
    bool use_stream = false;
    // 1.创建cursor
    ObPsStmtId inner_stmt_id = OB_INVALID_ID;
    if (OB_NOT_NULL(session.get_cursor(stmt_id_))) {
      if (OB_FAIL(session.close_cursor(stmt_id_))) {
        LOG_WARN("fail to close result set", K(ret), K(stmt_id_), K(session.get_sessid()));
      }
    }
    OZ (session.make_dbms_cursor(cursor, stmt_id_));
    CK (OB_NOT_NULL(cursor));
    OX (cursor->set_stmt_type(stmt::T_SELECT));
    OZ (session.get_inner_ps_stmt_id(stmt_id_, inner_stmt_id));
    OX (cursor->set_stmt_id(inner_stmt_id));
    OZ (session.ps_use_stream_result_set(use_stream));
    if (use_stream) {
      OX (cursor->set_streaming());
    }
    if (OB_OCI_STMT_SCROLLABLE_READONLY == exec_mode_) {
      OX (cursor->set_scrollable());
    }
    OZ (cursor->prepare_entity(session));
    CK (OB_NOT_NULL(cursor->get_allocator()));
    OZ (cursor->init_params(params.count()));
    OZ (cursor->get_exec_params().assign(params));
    OZ (gctx_.sql_engine_->init_result_set(ctx, result));
    if (OB_SUCCESS != ret || enable_perf_event) {
      set_exec_start_timestamp(ObTimeUtility::current_time());
    }
    if (OB_SUCC(ret)) {
      ObPLExecCtx pl_ctx(cursor->get_allocator(), &result.get_exec_context(), NULL/*params*/,
                        NULL/*result*/, &ret, NULL/*func*/, true);
      get_ctx().cur_sql_ = sql_;
      if (OB_FAIL(ObSPIService::dbms_dynamic_open(&pl_ctx, *cursor))) {
        LOG_WARN("cursor open faild.", K(cursor->get_id()));
      } else {
        int64_t row_num = 0;
        bool is_fetched = false;
        int64_t cur = 0;
        bool last_row = false;
        bool ac = true;
        int8_t has_result = 0;
        if (0 != iteration_count_ && cursor->get_field_columns().count() > 0) {
          has_result = 1;
          LOG_DEBUG("has result set.", K(stmt_id_));
        }
        if (OB_FAIL(session.get_autocommit(ac))) {
          LOG_WARN("fail to get autocommit", K(ret));
        } else if (OB_FAIL(response_param_query_header(session, &cursor->get_field_columns(),
                                          get_params(), stmt_id_, has_result, 0))) {
          LOG_WARN("send header packet faild.", K(ret));
        }
        if (-1 == cursor->get_current_position() && iteration_count_ > 0) {
          // execute 如果返回数据，需要更新一下cursor的指针位置
          OX (cursor->set_current_position(0));
        }
        while (OB_SUCC(ret) && row_num < iteration_count_
                && OB_SUCC(sql::ObSPIService::dbms_cursor_fetch(&pl_ctx, *cursor))) {
          common::ObNewRow &row = cursor->get_current_row();
#ifndef NDEBUG
          LOG_DEBUG("cursor fetch: ", K(cursor->get_id()), K(cursor->is_streaming()),
                                          K(cursor->get_current_row().cells_[0]),
                                          K(cursor->get_current_position()),
                                          K(row_num), K(iteration_count_));
#endif
          cur = cursor->get_current_position();
          ++cur;
          cursor->set_current_position(cur);
          is_fetched = true;
          if (OB_FAIL(response_row(session, row, &cursor->get_field_columns()))) {
            LOG_WARN("response row fail at line: ", K(ret), K(row_num));
          } else {
            ++row_num;
          }
        }
        if (is_fetched) {
          // always be set wether there is a error or not
          cur = cursor->get_current_position();
          cur = cur - 1;
          cursor->set_current_position(cur);
        }
        if (OB_ITER_END == ret || OB_READ_NOTHING == ret) {
          ret = OB_SUCCESS;
          last_row = true;
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("response query result fail", K(ret));
        } else {
          const ObWarningBuffer *warnings_buf = common::ob_get_tsi_warning_buffer();
          uint16_t warning_count = 0;
          if (OB_ISNULL(warnings_buf)) {
            LOG_WARN("can not get thread warnings buffer");
          } else {
            warning_count = static_cast<uint16_t>(warnings_buf->get_readable_warning_count());
          }
          if (has_result) {
            OZ (send_eof_packet(session, warning_count, false, !last_row, last_row));
          }
          if (OB_SUCC(ret) && (last_row && !cursor->is_scrollable())
                           && lib::is_oracle_mode()
                           && !cursor->is_streaming()
                           && OB_NOT_NULL(cursor->get_spi_cursor())
                           && cursor->get_spi_cursor()->row_store_.get_row_cnt() > 0) {
            if (OB_FAIL(session.close_cursor(cursor->get_id()))) {
              LOG_WARN("no scrollable cursor close cursor failed at last row.", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            bool last_row_ok_param = last_row;
            if (OB_NOT_NULL(cursor->get_spi_cursor())
                && cursor->get_spi_cursor()->row_store_.get_row_cnt() == 0) {
              last_row_ok_param = true;
            }
            if (OB_FAIL(send_ok_packet(session,
                                       iteration_count_,
                                       session.partition_hit().get_bool(),
                                       false,
                                       true,
                                       last_row_ok_param))) {
              LOG_WARN("fail to send ok packt", K(iteration_count_),
                       "is_partition_hit" ,session.partition_hit().get_bool(),
                       K(last_row_ok_param), K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCCESS != ret) {
      int tmp_ret = ret;
      if (OB_NOT_NULL(cursor) && OB_FAIL(session.close_cursor(cursor->get_id()))) {
        LOG_WARN("close cursor failed.", K(ret), K(stmt_id_));
      }
      // first time the stmt_id do not response to the client
      // Whether the cursor close is successful or not, it is required
      if (first_time_ && OB_FAIL(session.close_ps_stmt(stmt_id_))) {
        LOG_WARN("close cursor failed.", K(ret), K(stmt_id_));
      }
      ret = tmp_ret;
      LOG_WARN("execute server cursor failed.", K(ret));
    }
  } else if (OB_FAIL(gctx_.sql_engine_->stmt_execute(stmt_id_,
                                                    stmt_type_,
                                                    params,
                                                    ctx,
                                                    result,
                                                    false /* is_inner_sql */))) {
    set_exec_start_timestamp(ObTimeUtility::current_time());
    if (!THIS_WORKER.need_retry()) {
      int cli_ret = OB_SUCCESS;
      retry_ctrl.test_and_save_retry_state(
        gctx_, ctx, result, ret, cli_ret, get_arraybounding() /*ararybinding only local retry*/);
      if (OB_ERR_PROXY_REROUTE == ret) {
        LOG_DEBUG("run stmt_query failed, check if need retry",
                  K(ret), K(cli_ret), K(retry_ctrl.need_retry()), K_(stmt_id));
      } else {
        LOG_WARN("run stmt_query failed, check if need retry",
                K(ret), K(cli_ret), K(retry_ctrl.need_retry()), K_(stmt_id));
      }
      ret = cli_ret;
    }
    if (OB_ERR_PROXY_REROUTE == ret && !get_arraybounding()) {
      need_response_error = true;
    }
    int response_ret = OB_SUCCESS;
    if (need_response_pkg_when_error_occur()) {
      // not need to retry and response pkt
      bool ps_out = result.is_pl_stmt(stmt_type_) && result.is_with_rows() ? true : false;
      if (OB_SUCCESS != (response_ret = response_header_for_arraybinding(session, result))) {
        LOG_WARN("fail to response header", K(response_ret));
      } else if (arraybinding_row_->get_count() >= 3) {
        if (OB_SUCCESS != (response_fail_result(session, ret))) {
          LOG_WARN("fail to response fail row", K(response_ret));
        }
        if (is_save_exception_) {
          // batch errors mode must cover the code
          ret = response_ret;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arraybinding_row_ count must larger than 3", K(ret));
      }
      // array binding without returning into result.get_field_columns().count() == 0
      if (OB_FAIL(ret)) {
        // array_binding must with eof pkt
        if (OB_SUCCESS != (response_ret = send_eof_packet(session, 0, false, true, ps_out))) {
          LOG_WARN("send eof field failed", K(response_ret), K(ret));
        }
      } else if (curr_sql_idx_ == (arraybinding_size_ - 1)) {
        if (OB_SUCCESS != (response_ret = send_eof_packet(session, 0, false, true, ps_out))) {
          ret = response_ret;
          LOG_WARN("send eof field failed", K(response_ret), K(ret));
        }
      }
    }
    int close_ret = result.close();
    if (OB_SUCCESS != close_ret) {
      LOG_WARN("fail to close result", K(close_ret));
    }
  } else {
    if (first_time_
        && stmt::T_ANONYMOUS_BLOCK == stmt_type_
        && !is_send_long_data()
        && !get_arraybounding()) {
      int8_t has_result = 0;
      if (OB_NOT_NULL(result.get_field_columns())
          && result.get_field_columns()->count() > 0) {
        has_result = 1;
      }
      if (OB_FAIL(response_query_header(session,
                                        result.get_field_columns(),
                                        result.get_param_fields(),
                                        NULL,
                                        has_result,
                                        result.get_warning_count(),
                                        has_result /*ps_out*/))) {
        LOG_WARN("send header packet faild.", K(ret));
      }
    }
    int8_t has_result = 0;
    if (OB_SUCC(ret)) {
      if (enable_perf_event) {
        //监控项统计开始
        set_exec_start_timestamp(ObTimeUtility::current_time());
      }
      // 本分支内如果出错, 全部会在response_result内部处理妥当, 无需再额外处理回复错误包
      need_response_error = false;
      is_diagnostics_stmt = ObStmt::is_diagnostic_stmt(result.get_literal_stmt_type());
      ctx.is_show_trace_stmt_ = ObStmt::is_show_trace_stmt(result.get_literal_stmt_type());
      session.set_current_execution_id(execution_id);
      result.set_statement_id(stmt_id_);
      check_has_result(result, has_result);
    }
    if (OB_FAIL(ret)) {
    } else if (get_arraybounding()) {
      if (OB_FAIL(after_do_process_for_arraybinding(session, result))) {
        LOG_WARN("failed to process arraybinding sql", K(ret));
      }
    } else {
      if (OB_FAIL(response_query_header(session,
                                        result.get_field_columns(),
                                        result.get_param_fields(),
                                        result.get_returning_param_fields(),
                                        has_result,
                                        result.get_warning_count(),
                                        1 == has_result ? true : false))) {
        LOG_WARN("send header packet faild.", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(response_result(result, session, force_sync_resp, async_resp_used))) {
          ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
          if (OB_ISNULL(plan_ctx)) {
            LOG_ERROR("execute query fail, and plan_ctx is NULL", K(ret));
          } else {
            LOG_WARN("execute query fail",
                    K(ret), "timeout_timestamp", plan_ctx->get_timeout_timestamp());
          }
        } else if (OB_OCI_EXACT_FETCH == exec_mode_ && stmt::T_SELECT == stmt_type_) {
          int exact_fetch_ret = OB_SUCCESS;
          if (0 == iteration_count_) {
            // do nothing
          } else if (iteration_count_ > result.get_return_rows()) {
            exact_fetch_ret = OB_READ_NOTHING;
            LOG_WARN("OCI_EXACT_FETCH has not enoughrows.", K(ret));
          } else if (iteration_count_ < result.get_return_rows()) {
            exact_fetch_ret = OB_ERR_TOO_MANY_ROWS;
            LOG_WARN("OCI_EXACT_FETCH has too many rows.", K(ret));
          }
          if (OB_SUCCESS == exact_fetch_ret) {
            uint64_t affected_rows_of_ok_param = result.get_return_rows() < iteration_count_
                ? result.get_return_rows()
                : iteration_count_;
            if (OB_FAIL(send_ok_packet(session,
                                       affected_rows_of_ok_param,
                                       session.partition_hit().get_bool(),
                                       false,
                                       true,
                                       true))) {
              LOG_WARN("fail to send ok packt", K(affected_rows_of_ok_param),
                                     "is_partition_hit" ,session.partition_hit().get_bool(), K(ret));
            }
          } else {
            bool is_partition_hit = session.get_err_final_partition_hit(exact_fetch_ret);
            if (OB_FAIL(send_error_packet(exact_fetch_ret, NULL, is_partition_hit))) {
              LOG_WARN("exact fetch : iterator_count dose not match result set",
                        K(exact_fetch_ret), K(ret));
            }
          }
        } else if (sql::ObDMLStmt::is_dml_write_stmt(stmt_type_) && result.is_with_rows()) {
          if (OB_FAIL(send_ok_packet(session,
                                     result.get_return_rows(),
                                     session.partition_hit().get_bool(),
                                     false,
                                     false,
                                     true))) {
            LOG_WARN("fail to send ok packt", "affect_rows", result.get_return_rows(),
                     "is_partition_hit" ,session.partition_hit().get_bool(), K(ret));
          }
        }
      }
    }
    if ((OB_SUCC(ret) && is_diagnostics_stmt) || async_resp_used) {
      // if diagnostic stmt succeed, no need to clear warning buf.
      // or async resp is used, it will be cleared in callback thread.
      session.update_show_warnings_buf();
    } else {
      session.set_show_warnings_buf(ret);
    }
  }
  return ret;
}

void ObMPStmtPrexecute::check_has_result(ObMySQLResultSet &result, int8_t &has_result)
{
  if ((0 != iteration_count_
      && OB_NOT_NULL(result.get_field_columns())
      && result.get_field_columns()->count() > 0)
      || (OB_OCI_EXACT_FETCH == exec_mode_ && stmt::T_SELECT == stmt_type_)) {
    has_result = 1;
  }
}

int ObMPStmtPrexecute::response_query_header(ObSQLSessionInfo &session,
                                            const ColumnsFieldIArray *fields,
                                            const ParamsFieldIArray *inout_params,
                                            const ParamsFieldIArray *returning_params_field,
                                            int8_t has_result,
                                            int64_t warning_count,
                                            bool ps_out)
{
  // TODO: 增加com类型的处理
  int ret = OB_SUCCESS;
  bool need_send_eof = false;
  LOG_DEBUG("before response_query_header",KPC(fields), KPC(inout_params), KPC(returning_params_field));
  if (!prepare_packet_sent_) {
    uint64_t params_cnt = 0;
    uint64_t returning_params_cnt = 0;
    int64_t fields_count = 0;
    bool has_arraybinding_result = false;
    bool has_ps_out = false;
    if (OB_NOT_NULL(inout_params)) {
      params_cnt = inout_params->count();
    }
    if (OB_NOT_NULL(fields)) {
      fields_count = fields->count();
    }

    if (OB_NOT_NULL(returning_params_field) && is_arraybinding_has_result_type(stmt_type_)) {
      returning_params_cnt = returning_params_field->count();
      params_cnt = params_cnt + returning_params_cnt;
    }

    if (returning_params_cnt > 0) {
      has_arraybinding_result = true;
    }

    if ((stmt::T_ANONYMOUS_BLOCK == stmt_type_ || stmt::T_CALL_PROCEDURE == stmt_type_)
          && ps_out && get_arraybounding()) {
      has_ps_out = true;
    }
    if (OB_FAIL(send_prepare_packet(stmt_id_,
                                    fields_count,
                                    params_cnt,
                                    warning_count,
                                    has_result,
                                    has_arraybinding_result,
                                    has_ps_out))) {
      LOG_WARN("packet send prepare infomation fail", K(ret), K(stmt_id_));
    }
    if (OB_SUCC(ret) && params_cnt > 0) {
      need_send_eof = true;
      if (OB_FAIL(send_param_field_packet(session, inout_params))) {
        LOG_WARN("response param packet fail", K(ret));
      }
    }
    if (OB_SUCC(ret) && returning_params_cnt > 0 && is_arraybinding_has_result_type(stmt_type_)) {
      need_send_eof = true;
      if (OB_FAIL(send_param_field_packet(session, returning_params_field))) {
        LOG_WARN("response param packet fail", K(ret));
      }
    }
    if (OB_SUCC(ret) && need_send_eof) {
      if (OB_FAIL(send_eof_packet(session, 0, false, true, false))) {
        LOG_WARN("send eof field failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && fields_count > 0) {
      if (stmt::T_ANONYMOUS_BLOCK == stmt_type_
              || (OB_OCI_EXACT_FETCH == exec_mode_ && stmt::T_SELECT == stmt_type_)) {
        // send column at sync_cmd_driver or sync_plan_driver
        // do nothing here.
      } else if (is_arraybinding_has_result_type(stmt_type_) && returning_params_field->count() > 0) {
        // insert, update, delete returning stmt will send column at sync_plan_driver
        // do nothing here.
      } else if (OB_FAIL(send_column_packet(session, fields, ps_out))) {
        LOG_WARN("response column packet fail", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      prepare_packet_sent_ = true;
    }
  }
  return ret;
}

int ObMPStmtPrexecute::response_param_query_header(ObSQLSessionInfo &session,
                                                  const ColumnsFieldIArray *fields,
                                                  ParamStore *params,
                                                  int64_t stmt_id,
                                                  int8_t has_result,
                                                  int64_t warning_count,
                                                  bool ps_out)
{
  // TODO: 增加com类型的处理
  int ret = OB_SUCCESS;
  if (!prepare_packet_sent_) {
    uint64_t params_cnt = 0;
    int64_t fields_count = 0;
    if (OB_NOT_NULL(params)) {
      params_cnt = params->count();
    }
    if (OB_NOT_NULL(fields)) {
      fields_count = fields->count();
    }
    if (OB_FAIL(send_prepare_packet(stmt_id,
                                    fields_count,
                                    params_cnt,
                                    warning_count,
                                    has_result,
                                    false,
                                    false))) {
      LOG_WARN("packet send prepare infomation fail", K(ret), K(stmt_id));
    }
    if (OB_SUCC(ret) && params_cnt > 0) {
      if (OB_FAIL(send_param_packet(session, params))) {
        LOG_WARN("response param packet fail", K(ret));
      }
    }
    if (OB_SUCC(ret) && fields_count > 0) {
      if (stmt::T_ANONYMOUS_BLOCK == stmt_type_
              || (OB_OCI_EXACT_FETCH == exec_mode_ && stmt::T_SELECT == stmt_type_)) {
        // do nothing
      } else if (OB_FAIL(send_column_packet(session, fields, ps_out))) {
        LOG_WARN("response column packet fail", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      prepare_packet_sent_ = true;
    }
  }
  return ret;
}

int ObMPStmtPrexecute::after_do_process_for_arraybinding(ObSQLSessionInfo &session,
                                                         ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  int response_ret = OB_SUCCESS;
  int close_ret = OB_SUCCESS;
  if (ObStmt::is_dml_write_stmt(stmt_type_) && OB_ISNULL(result.get_physical_plan())) {
    ret = OB_NOT_INIT;
    LOG_WARN("should have set plan to result set", K(ret));
  } else if (OB_FAIL(result.open())) {
    int cli_ret = OB_SUCCESS;
    retry_ctrl_.test_and_save_retry_state(gctx_,
                                          ctx_,
                                          result,
                                          ret,
                                          cli_ret,
                                          true /*arraybinding only local retry*/);
    if (OB_TRANSACTION_SET_VIOLATION != ret && OB_REPLICA_NOT_READABLE != ret) {
      if (OB_TRY_LOCK_ROW_CONFLICT == ret && retry_ctrl_.need_retry()) {
        // lock conflict not print log
      } else {
        LOG_WARN("result set open failed, check if need retry",
                 K(ret), K(cli_ret), K(retry_ctrl_.need_retry()));
      }
    }
    ret = cli_ret;
    if (need_response_pkg_when_error_occur()) {
      // not need retry, should response packet
      if (OB_SUCCESS != (response_ret = response_header_for_arraybinding(session, result))) {
        LOG_WARN("fail to response header", K(response_ret));
      } else if (arraybinding_row_->get_count() >= 3) {
        if (OB_SUCCESS != (response_fail_result(session, ret))) {
          LOG_WARN("fail to response fail row", K(response_ret));
        }
        if (is_save_exception_) {
          // batch errors mode must cover the err_code
          ret = response_ret;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arraybinding_row_ count must larger than 3", K(ret));
      }
    }
  } else if (OB_FAIL(response_arraybinding_result(session, result))) {
    LOG_WARN("fail to send arraybinding result", K(ret));
  }
  // whether succeed or fail, must close result
  close_ret = result.close();
  if (close_ret != OB_SUCCESS && close_ret != OB_TRANSACTION_SET_VIOLATION
      && OB_TRY_LOCK_ROW_CONFLICT != close_ret) {
    LOG_WARN("close result set fail", K(close_ret));
  }
  OX (arraybinding_rowcnt_ += result.get_affected_rows());

  bool ps_out = result.is_pl_stmt(stmt_type_) && result.is_with_rows() ? true : false;
  if (ret != OB_SUCCESS) {
    // 当前执行报错的情况下，如果不再retry，并且是非batch优化才需要回一个eof包
    if (need_response_pkg_when_error_occur()) {
      if (OB_SUCCESS != (response_ret = send_eof_packet(session, 0, false, true, ps_out))) {
        LOG_WARN("send eof field failed", K(response_ret), K(ret));
      }
    }
  } else {
    if (ctx_.multi_stmt_item_.is_ab_batch_opt()
        && ctx_.multi_stmt_item_.get_batched_stmt_cnt() == arraybinding_size_) {
      // 执行成功，全部数据都完成batch优化，直接回eof包
      if (OB_SUCCESS != (response_ret = send_eof_packet(session, 0, false, true, false))) {
        ret = response_ret;
        LOG_WARN("send eof field failed", K(response_ret), K(ret));
      }
    } else if (curr_sql_idx_ == (arraybinding_size_ - 1)) {
      // 最后一行数据完成执行
      if (OB_SUCCESS != (response_ret = send_eof_packet(session, 0, false, true, ps_out))) {
        ret = response_ret;
        LOG_WARN("send eof field failed", K(response_ret), K(ret));
      }
    }
  }
  return ret;
}

bool ObMPStmtPrexecute::need_response_pkg_when_error_occur()
{
  bool bret = false;
  if (sql::SESS_NOT_IN_RETRY == retry_ctrl_.need_retry()
      && !THIS_WORKER.need_retry()
      && get_arraybounding()) {
    if (ctx_.multi_stmt_item_.is_batched_multi_stmt()) {
      // 如果是batch优化，所有失败的场景下都不回包，因为还会做强制重试
      bret = false;
    } else {
      bret = true;
    }
  }
  return bret;
}

int ObMPStmtPrexecute::response_arraybinding_rows(sql::ObSQLSessionInfo &session, int64_t affect_rows)
{
  int ret = OB_SUCCESS;
  // arraybinding_row_.count() >= 3
  bool is_pl = stmt::T_ANONYMOUS_BLOCK == stmt_type_ || stmt::T_CALL_PROCEDURE == stmt_type_ 
                ? true : false;
  if (3 != arraybinding_row_->count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arraybind count is wrong.", K(ret));
   } else {
    arraybinding_row_->get_cell(0).set_int(is_pl ? curr_sql_idx_ : affect_rows);
    arraybinding_row_->get_cell(1).set_int(0);
    arraybinding_row_->get_cell(2).set_null();
    if (OB_FAIL(response_row(session, *arraybinding_row_, arraybinding_columns_))) {
      LOG_WARN("fail to response fail row to client", K(ret));
    }
  }
  return ret;
}

int ObMPStmtPrexecute::response_fail_result(sql::ObSQLSessionInfo &session, int err_ret)
{
  int ret = OB_SUCCESS;
  // arraybinding_row_.count() >= 3
  arraybinding_row_->get_cell(0).set_int(curr_sql_idx_);
  arraybinding_row_->get_cell(1).set_int(static_cast<uint16_t>(ob_errpkt_errno(err_ret, lib::is_oracle_mode())));
  for (int64_t i = 2; i < (arraybinding_row_->get_count() - 1); i++) {
    arraybinding_row_->get_cell(i).set_null();
  }
  arraybinding_row_->get_cell(arraybinding_row_->get_count() - 1).set_varchar(ob_oracle_strerror(err_ret));
  if (OB_FAIL(response_row(session, *arraybinding_row_, arraybinding_columns_))) {
    LOG_WARN("fail to response fail row to client", K(ret));
  }
  return ret;
}
int ObMPStmtPrexecute::response_returning_rows(ObSQLSessionInfo &session,
                                               ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  int response_ret = OB_SUCCESS;
//  session.get_trans_desc().consistency_wait(); // sync timestamp
  MYSQL_PROTOCOL_TYPE protocol_type = BINARY;
  const ObNewRow *result_row = NULL;
  const common::ColumnsFieldIArray *fields = result.get_field_columns();
  if (OB_ISNULL(fields) || NULL == arraybinding_row_ || arraybinding_row_->get_count() < 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fields is null", K(ret), KP(fields));
  } else {
    while (OB_SUCC(ret) && OB_SUCCESS == response_ret
           && !OB_FAIL(result.get_next_row(result_row))) {
      arraybinding_row_->get_cell(0).set_int(get_curr_sql_idx());
      arraybinding_row_->get_cell(1).set_int(ret);
      LOG_DEBUG("before send result_row", KPC(result_row));
      CK((arraybinding_row_->get_count() == (result_row->get_count() + 3)), arraybinding_row_->get_count(), result_row->get_count());
      ObNewRow *row = const_cast<ObNewRow*>(result_row);
      for (int i = 0; OB_SUCC(ret) && i < row->get_count(); i++) {
        ObObj &value = row->get_cell(i);
        arraybinding_row_->get_cell(i+2) = value;
      }
      arraybinding_row_->get_cell(arraybinding_row_->get_count() - 1).set_null();
      if (OB_SUCCESS != (response_ret = response_row(session, *arraybinding_row_, arraybinding_columns_))) {
        LOG_WARN("fail to response row to client", K(response_ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else if (OB_SUCCESS != response_ret) {
      LOG_WARN("cli_ret != OB_SUCCESS", K(response_ret));
    } else {
      //ret != OB_SUCCESS && OB_SUCCESS == response_ret
      // response row with err_ret and err_msg
      // if is batch_errors mode，cover the err_ret
      arraybinding_row_->get_cell(0).set_int(get_curr_sql_idx());
      arraybinding_row_->get_cell(1).set_int(ret);
      for (int i = 0; i < result_row->get_count(); i++) {
        arraybinding_row_->get_cell(i+2).set_null();
      }
      arraybinding_row_->get_cell(arraybinding_row_->get_count() - 1).set_varchar(ob_oracle_strerror(ret));
      LOG_DEBUG("error occured before send arraybinding_row_", KPC(arraybinding_row_));
      if (OB_SUCCESS != (response_ret = response_row(session, *arraybinding_row_, arraybinding_columns_))) {
        LOG_WARN("fail to response row to client", K(response_ret));
      }
      if (is_save_exception_) {
        ret = OB_SUCCESS;
      }
    }
  }
  ret = (ret != OB_SUCCESS ? ret : response_ret);
  return ret;
}
int ObMPStmtPrexecute::response_arraybinding_result(ObSQLSessionInfo &session,
                                                    ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(response_header_for_arraybinding(session, result))) {
    LOG_WARN("fail to response header", K(ret));
  } else if (result.is_with_rows()) {
    if (OB_FAIL(response_returning_rows(session, result))) {
      LOG_WARN("response_returning_rows failed");
    }
  } else {
    if (OB_FAIL(response_arraybinding_rows(session, result.get_affected_rows()))) {
      LOG_WARN("response_returning_rows failed");
    }
  }
  return ret;
}
int ObMPStmtPrexecute::response_header_for_arraybinding(ObSQLSessionInfo &session,
                                                        ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  uint64_t column_num = 0;
  uint64_t input_param_num = 0;
  uint64_t returning_param_num = 0;
  uint16_t warning_count = 0;
  int8_t has_result_set = 1;
  bool need_send_eof = false;
  bool ps_out = false;
  ObIAllocator &alloc = CURRENT_CONTEXT->get_arena_allocator();
  const ColumnsFieldIArray *column_fields = result.get_field_columns();
  const ParamsFieldIArray *input_params_field = result.get_param_fields();
  const ParamsFieldIArray *returning_params_field = result.get_returning_param_fields();
  if (OB_ISNULL(column_fields) || OB_ISNULL(input_params_field) || OB_ISNULL(returning_params_field)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fields columns or inout_params or returning_params is null", K(ret),
             "column_fields", NULL == column_fields,
             "inout_params", NULL == input_params_field,
             "returning_params_field", NULL == returning_params_field);
  } else {
    column_num = column_fields->count();
    input_param_num = input_params_field->count();
    returning_param_num = returning_params_field->count();
  }

  if ((column_num > 0 && (stmt::T_ANONYMOUS_BLOCK == stmt_type_ 
                            || stmt::T_CALL_PROCEDURE == stmt_type_))) {
    ps_out = true;
  }

  // 1. send prepare_packet
  // 2. send param_field with eof
  // 3. send result field
  // only send once, use sent_arr_binding_header_ to check it
  if (!prepare_packet_sent_) {
    if (OB_SUCC(ret) && OB_FAIL(init_arraybinding_fields_and_row(result))) {
      LOG_WARN("fail to init arraybinding columns and row", K(ret));
    } else {
      prepare_packet_sent_ = true;
    }
    if (OB_SUCC(ret) && OB_FAIL(send_prepare_packet(stmt_id_,
                                                    arraybinding_columns_->count(),
                                                    input_param_num + returning_param_num,
                                                    warning_count,
                                                    true,
                                                    returning_param_num > 0,
                                                    ps_out))) {
      LOG_WARN("fail to send_prepare_packet", K(stmt_id_));
    }


    if (OB_SUCC(ret) && input_param_num > 0) {
      need_send_eof = true;
      if (OB_FAIL(send_param_field_packet(session, input_params_field))) {
        LOG_WARN("response param packet fail", K(ret));
      }
    }
    if (OB_SUCC(ret) && returning_param_num > 0) {
      need_send_eof = true;
      if (OB_FAIL(send_param_field_packet(session, returning_params_field))) {
        LOG_WARN("response param packet fail", K(ret));
      }
    }
    if (OB_SUCC(ret) && need_send_eof) {
      if (OB_FAIL(send_eof_packet(session, 0, false, true, ps_out))) {
        LOG_WARN("send eof field failed", K(ret));
      }
    }

    LOG_DEBUG("response arraybinding header", K(ret), K(returning_param_num), K(input_param_num),K(need_send_eof),
            KPC(input_params_field), KPC(returning_params_field), KPC(arraybinding_columns_));
    // send_column_packet will send eof pkt
    if (OB_SUCC(ret) && OB_FAIL(send_column_packet(session, arraybinding_columns_, ps_out))) {
      LOG_WARN("fail to send column packet", K(ret));
    }
  }
  return ret;
}
int ObMPStmtPrexecute::send_ok_packet(sql::ObSQLSessionInfo &session,
                                      uint64_t affected_rows,
                                      bool is_partition_hit,
                                      bool has_more_result,
                                      bool cursor_exist,
                                      bool send_last_row)
{
  int ret = OB_SUCCESS;
  ObOKPParam ok_param;
  ok_param.affected_rows_ = affected_rows;
  ok_param.is_partition_hit_ = is_partition_hit;
  ok_param.has_more_result_ = has_more_result;
  ok_param.cursor_exist_ = cursor_exist;
  ok_param.send_last_row_ = send_last_row;
  if (OB_FAIL(ObMPBase::send_ok_packet(session, ok_param))) {
    LOG_WARN("fail to send ok packt", K(ok_param), K(ret));
  }
  return ret;
}

int ObMPStmtPrexecute::send_prepare_packet(uint32_t statement_id,
                                           uint16_t column_num,
                                           uint16_t param_num,
                                           uint16_t warning_count,
                                           int8_t has_result_set,
                                           bool is_returning_into,
                                           bool is_ps_out)
{
  int ret = OB_SUCCESS;
  obmysql::OMPKPrexecute prexecute_packet;
  prexecute_packet.set_statement_id(statement_id);
  prexecute_packet.set_column_num(column_num);
  prexecute_packet.set_param_num(param_num);
  prexecute_packet.set_warning_count(warning_count);
  prexecute_packet.set_has_result_set(has_result_set);
  ObServerExtendFlag extend_flag = prexecute_packet.get_extend_flag();
  extend_flag.IS_RETURNING_INTO_STMT = is_returning_into ? 1 : 0;
  extend_flag.IS_ARRAY_BINDING = is_arraybinding_ ? 1 : 0;
  extend_flag.IS_PS_OUT = is_ps_out ? 1 : 0;
  prexecute_packet.set_extend_flag(extend_flag);

  if (OB_SUCC(ret) && OB_FAIL(response_packet(prexecute_packet, NULL))) {
    LOG_WARN("response packet failed", K(ret));
  }
  LOG_DEBUG("send prepare packet in prepare-execute protocol.", K(statement_id),
                                                                K(column_num),
                                                                K(param_num),
                                                                K(warning_count),
                                                                K(has_result_set),
                                                                K(sql_));
  return ret;
}

int ObMPStmtPrexecute::send_param_field_packet(ObSQLSessionInfo &session,
                                               const ParamsFieldIArray *inout_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inout_params)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(inout_params));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < inout_params->count(); i++) {
      ObMySQLField field;
      const ObField &ob_field = inout_params->at(i);
      if (OB_FAIL(ObMySQLResultSet::to_mysql_field(ob_field, field))) {
        LOG_WARN("fail to copy param field", K(ob_field));
      } else {
        OMPKField fp(field);
        ObMySQLResultSet::replace_lob_type(session, ob_field, field);
        if (OB_FAIL(response_packet(fp, &session))) {
          LOG_DEBUG("response packet fail", K(ret));
        } else {
          LOG_DEBUG("send param packet in prepare-execute protocol.", K(field));
        }
      }
    }
  }
  return ret;
}

int ObMPStmtPrexecute::send_param_packet(ObSQLSessionInfo &session,
                                         ParamStore *params)
{
  int ret = OB_SUCCESS;
  uint64_t params_cnt = 0;
  if (OB_NOT_NULL(params)) {
    params_cnt = params->count();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params_cnt; ++i) {
    ObField param_field;
    ObMySQLField field;
    param_field.type_.set_type(params->at(i).get_type()); // @bug
    param_field.cname_ = ObString::make_string("?");
    OZ (ObMySQLResultSet::to_mysql_field(param_field, field));
    OMPKField fp(field);
    OZ (response_packet(fp, &session));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(send_eof_packet(session, 0, false, false, false))) {
      LOG_WARN("send eof field failed", K(ret));
    }
  }
  return ret;
}

int ObMPStmtPrexecute::send_column_packet(ObSQLSessionInfo &session,
                                          const ColumnsFieldIArray *fields,
                                          bool ps_out)
{
  int ret = OB_SUCCESS;
  int64_t fields_count = 0;
  if (OB_ISNULL(fields)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("column fields is null", K(ret));
  } else {
    fields_count = fields->count();
    for (int64_t i = 0; OB_SUCC(ret) && i < fields_count; ++i) {
      ObMySQLField field;
      const ObField &ob_field = fields->at(i);
      if (OB_FAIL(ObMySQLResultSet::to_mysql_field(ob_field, field))) {
        // do nothing
      } else {
        ObMySQLResultSet::replace_lob_type(session, ob_field, field);
      }
      if (OB_SUCC(ret)) {
        OMPKField fp(field);
        if (OB_FAIL(response_packet(fp, &session))) {
          LOG_WARN("response packet fail", K(ret));
        } else {
          LOG_DEBUG("send column packet in prepare-execute protocol.", K(field));
        }
      }
    }
    if (OB_SUCC(ret) && fields_count > 0) {
      if (OB_FAIL(send_eof_packet(session, 0, false, true, false, ps_out))) {
        LOG_WARN("column send eof fail", K(ret));
      }
    }
  }
  return ret;
}

int ObMPStmtPrexecute::send_eof_packet(ObSQLSessionInfo &session,
                                            uint16_t warning_count,
                                            bool has_result,
                                            bool cursor_exist,
                                            bool last_row,
                                            bool ps_out) {
  int ret = OB_SUCCESS;
  OMPKEOF eofp;
  bool ac = true;
  if (OB_FAIL(session.get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else {
    eofp.set_warning_count(warning_count);
    ObServerStatusFlags flags = eofp.get_server_status();
    flags.status_flags_.OB_SERVER_STATUS_IN_TRANS
      = (session.is_server_status_in_transaction() ? 1 : 0);
    flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (ac ? 1 : 0);
    flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = has_result;
    flags.status_flags_.OB_SERVER_STATUS_CURSOR_EXISTS = cursor_exist ? 1 : 0;
    flags.status_flags_.OB_SERVER_STATUS_LAST_ROW_SENT = last_row ? 1 : 0;
    flags.status_flags_.OB_SERVER_PS_OUT_PARAMS = ps_out ? 1 : 0;
    if (!session.is_obproxy_mode()) {
      flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !session.partition_hit().get_bool();
    }
    eofp.set_server_status(flags);
    if (OB_SUCC(ret)) {
      if(OB_FAIL(response_packet(eofp, &session))) {
        LOG_WARN("response packet fail", K(ret));
      } else {
        LOG_DEBUG("send eof packet in prepare-execute protocol.");
      }
    }
  }
  return ret;
}

} //end of namespace observer
} //end of namespace oceanbase
