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
#include "observer/mysql/obmp_stmt_fetch.h"
#include "lib/oblog/ob_log.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/timezone/ob_time_convert.h"
#include "observer/mysql/obsm_utils.h"
#include "observer/mysql/obmp_utils.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/obsm_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_time_utility2.h"
#include "sql/ob_sql.h"
#include "sql/ob_spi.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "observer/omt/ob_tenant.h"
#include "observer/mysql/ob_sync_plan_driver.h"
#include "observer/mysql/ob_sync_cmd_driver.h"
#include "observer/mysql/ob_async_plan_driver.h"
#include "pl/ob_pl_user_type.h"
#include "pl/sys_package/ob_dbms_sql.h"
#include "pl/ob_pl_exception_handling.h"
#include "sql/engine/ob_exec_context.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "observer/mysql/obsm_row.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/packet/ompk_resheader.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "observer/mysql/obmp_stmt_send_piece_data.h"
#include "share/ob_lob_access_utils.h"
#include "sql/plan_cache/ob_ps_cache.h"

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
ObMPStmtFetch::ObMPStmtFetch(const ObGlobalContext &gctx)
    : ObMPBase(gctx),
      cursor_id_(OB_INVALID_ID),
      fetch_rows_(OB_INVALID_COUNT),
      single_process_timestamp_(0),
      exec_start_timestamp_(0),
      exec_end_timestamp_(0),
      offset_type_(OB_OCI_DEFAULT),
      offset_(0),
      extend_flag_(0),
      column_flag_(NULL)
{
}
int ObMPStmtFetch::before_process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMPBase::before_process())) {
    LOG_WARN("fail to call before process", K(ret));
  } else if ((OB_ISNULL(req_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("request should not be null", K(ret));
  } else if (req_->get_type() != ObRequest::OB_MYSQL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid request", K(ret), K_(*req));
  } else {
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    const char* pos = pkt.get_cdata();
    uint32_t cursor_id = -1; //OB_INVALID_ID;
    ObMySQLUtil::get_uint4(pos, cursor_id);
    cursor_id_ = cursor_id;
    int32_t fetch_rows = 0;
    ObMySQLUtil::get_int4(pos, fetch_rows);
    fetch_rows_ = fetch_rows;
    if (pkt.get_clen() > FETCH_PACKET_SIZE_WITHOUT_OFFSET) {
      if (lib::is_mysql_mode()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support offset type in mysql mode.", K(ret), K(cursor_id));
      } else {
        ObMySQLUtil::get_int2(pos, offset_type_);
        ObMySQLUtil::get_int4(pos, offset_);
        if (pkt.get_clen() > FETCH_PACKET_SIZE_WITH_OFFSET) {
          ObMySQLUtil::get_int4(pos, extend_flag_);
          if (has_long_data()) {
            ObSQLSessionInfo *session = NULL;
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(get_session(session))) {
              LOG_WARN("get session failed");
            } else if (OB_ISNULL(session)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("session is NULL or invalid", K(ret), K(session));
            } else if (OB_NOT_NULL(session->get_dbms_cursor(cursor_id_))) {
              int64_t column_count = session->get_dbms_cursor(cursor_id_)
                                            ->get_field_columns().count();
              int64_t len = (column_count + 7) / 8;
              column_flag_ = static_cast<char*>(THIS_WORKER.get_sql_arena_allocator()
                                                            .alloc(len + 1));
              MEMSET(column_flag_, 0, len+1);
              MEMCPY(column_flag_, pos, len);
              pos += len;
            } else {
              ret = OB_ERR_FETCH_OUT_SEQUENCE;
              LOG_ERROR("cursor not found", K(cursor_id_), K(ret));
            }
            if (session != NULL) {
              revert_session(session);
            }
          }
        } else {
          extend_flag_ = 0;
        }
      }
    } else {
      offset_type_ = OB_OCI_DEFAULT;
      offset_ = 0;
    }
  }
  if (OB_FAIL(ret)) {
    send_error_packet(ret, NULL);
    if (OB_ERR_PREPARE_STMT_CHECKSUM == ret) {
      force_disconnect();
      LOG_WARN("prepare stmt checksum error, disconnect connection", K(ret));
    }
    flush_buffer(true);
  }
  return ret;
}
int ObMPStmtFetch::set_session_active(ObSQLSessionInfo &session) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session.set_session_state(QUERY_ACTIVE))) {
    LOG_WARN("fail to set session state", K(ret));
  } else {
    session.set_query_start_time(get_receive_timestamp());
    session.set_mysql_cmd(obmysql::COM_STMT_FETCH);
    session.update_last_active_time();
  }
  return ret;
}
int ObMPStmtFetch::do_process(ObSQLSessionInfo &session,
                              bool &need_response_error)
{
  int ret = OB_SUCCESS;
  ObAuditRecordData &audit_record = session.get_raw_audit_record();
  audit_record.try_cnt_++;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  const bool enable_sql_audit = GCONF.enable_sql_audit
                                && session.get_local_ob_enable_sql_audit();
  single_process_timestamp_ = ObTimeUtility::current_time();
  ObPLCursorInfo *cursor = session.get_cursor(cursor_id_);
  if (OB_ISNULL(cursor)) {
    ret = OB_ERR_FETCH_OUT_SEQUENCE;
    LOG_ERROR("cursor not found", K(cursor_id_), K(ret));
    //如果发生fetch过程中找不到cursor的情况，无论什么原因立刻断连接，由应用程序进行容错
    //disconnect();
  } else {
    ObWaitEventStat total_wait_desc;
    int64_t execution_id = 0;
    ObDiagnoseSessionInfo *di = ObDiagnoseSessionInfo::get_local_diagnose_info();
    ObMaxWaitGuard max_wait_guard(enable_perf_event
        ? &audit_record.exec_record_.max_wait_event_ : NULL, di);
    ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
    int64_t fetch_limit = OB_INVALID_COUNT == fetch_rows_ ? INT64_MAX : fetch_rows_;
    int64_t true_row_num = 0;
    if (enable_perf_event) {
      audit_record.exec_record_.record_start(di);
    }
    if (OB_ISNULL(gctx_.sql_engine_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid sql engine", K(ret), K(gctx_));
    } else if (FALSE_IT(execution_id = gctx_.sql_engine_->get_execution_id())) {
      //nothing to do
    } else if (OB_FAIL(set_session_active(session))) {
      LOG_WARN("fail to set session active", K(ret));
    } else {
      exec_start_timestamp_ = ObTimeUtility::current_time();
    }
    if (OB_SUCC(ret)) {
      //监控项统计开始
      exec_start_timestamp_ = ObTimeUtility::current_time();
      // 本分支内如果出错，全部会在response_result内部处理妥当
      // 无需再额外处理回复错误包
      session.set_current_execution_id(execution_id);
      OX (need_response_error = false);
      if (0 == fetch_limit && !cursor->is_streaming()
                           && cursor->is_ps_cursor()
                           && lib::is_oracle_mode()
                           && OB_NOT_NULL(cursor->get_spi_cursor())
                           && cursor->get_spi_cursor()->row_store_.get_row_cnt() > 0) {
        set_close_cursor();
      } else {
        OZ (response_result(*cursor, session, fetch_limit, true_row_num));
      }
      if (OB_READ_NOTHING == ret) {
        LOG_WARN("nothing to read", K(ret));
        // oracle return success when read nothing
        ret = OB_SUCCESS;
      }
      OX (need_response_error = true);
    }
    //监控项统计结束
    exec_end_timestamp_ = ObTimeUtility::current_time();

    // some statistics must be recorded for plan stat, even though sql audit disabled
    bool first_record = (1 == audit_record.try_cnt_);
    ObExecStatUtils::record_exec_timestamp(*this, first_record, audit_record.exec_timestamp_);
    audit_record.exec_timestamp_.update_stage_time();

    if (enable_perf_event) {
      audit_record.exec_record_.record_end(di);
      record_stat(stmt::T_EXECUTE, exec_end_timestamp_);
      audit_record.exec_record_.wait_time_end_ = total_wait_desc.time_waited_;
      audit_record.exec_record_.wait_count_end_ = total_wait_desc.total_waits_;
      audit_record.update_event_stage_state();
    }

    if (enable_sql_audit) {
      audit_record.affected_rows_ = fetch_limit;
      audit_record.return_rows_ = true_row_num;
      audit_record.ps_stmt_id_ = cursor_id_;
      audit_record.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();
      if (OB_NOT_NULL(cursor)
          && cursor->is_ps_cursor()) {
        ObPsStmtInfoGuard guard;
        ObPsStmtInfo *ps_info = NULL;
        ObPsStmtId inner_stmt_id = OB_INVALID_ID;
        if (OB_SUCC(session.get_inner_ps_stmt_id(cursor_id_, inner_stmt_id))
              && OB_SUCC(session.get_ps_cache()->get_stmt_info_guard(inner_stmt_id, guard))
              && OB_NOT_NULL(ps_info = guard.get_stmt_info())) {
          audit_record.ps_inner_stmt_id_ = inner_stmt_id;
          audit_record.sql_ = const_cast<char *>(ps_info->get_ps_sql().ptr());
          audit_record.sql_len_ = min(ps_info->get_ps_sql().length(), OB_MAX_SQL_LENGTH);
        } else {
          LOG_WARN("get sql fail in fetch", K(ret), K(cursor_id_), K(cursor->get_id()));
        }
      }
    }
    session.partition_hit().freeze();
    session.set_show_warnings_buf(ret); // TODO: 挪个地方性能会更好，减少部分wb拷贝

    clear_wb_content(session);
    // 流式审计信息交给dbms_cursor来做
    ObSQLUtils::handle_audit_record(false/*no need retry*/, EXECUTE_PS_FETCH, session);
  }
  return ret;
}

int ObMPStmtFetch::response_query_header(ObSQLSessionInfo &session, 
                                         const ColumnsFieldArray *fields)
{
  // TODO: 增加com类型的处理
  int ret = OB_SUCCESS;
  bool ac = true;
  ObSqlCtx ctx;
  ObQueryRetryCtrl retry_ctrl;
  ObSyncPlanDriver drv(gctx_,
                           ctx,
                           session,
                           retry_ctrl,
                           *this,
                           false,
                           OB_INVALID_COUNT);
  if (NULL == fields) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(drv.response_query_header(*fields,
                                               has_ok_packet(),
                                               false))) {
    LOG_WARN("fail to get autocommit", K(ret));
  }
  return ret;
}

/* fetch协议返向客户端发送结果集
 * 协议上需要注意的： oracle协议每次返回结果集之前需要发送head packet, mysql不需要特殊处理
 * 内存使用上需要注意的： 获取fetch需要返回的row时注意要切换到 cursor 所在的 allocator
 * cursor的fetch通过dbms_cursor里的方法完成
 * offset功能实现：通过操作当前偏移量实现
 */
int ObMPStmtFetch::response_result(pl::ObPLCursorInfo &cursor,
                                   ObSQLSessionInfo &session,
                                   int64_t fetch_limit,
                                   int64_t &row_num)
{
  int ret = OB_SUCCESS;
  bool process_ok = false;
  // for select SQL
  bool ac = true;
  bool admission_fail_and_need_retry = false;
  bool last_row = false;
  int32_t offset = offset_;
  int64_t max_count = 0;
  row_num = 0;

  if (OB_FAIL(session.get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else {
    CK (OB_NOT_NULL(cursor.get_cursor_entity()));
    if (OB_SUCC(ret)) {
      WITH_CONTEXT(cursor.get_cursor_entity()) {
        lib::ContextTLOptGuard guard(false);
        ParamStore params;
        ObExecContext *exec_ctx = NULL;
        const common::ObNewRow *row = NULL;
        bool need_fetch = true;
        int64_t cur = 0;
        const ColumnsFieldArray *fields = NULL;
        ObArenaAllocator allocator(ObModIds::OB_SQL_EXECUTOR);
        SMART_VAR(ObExecContext, tmp_exec_ctx, allocator) {
          if (cursor.is_streaming()) {
            CK (OB_NOT_NULL(cursor.get_cursor_handler()));
            CK (OB_NOT_NULL(cursor.get_cursor_handler()->get_result_set()));
            OX (fields = dynamic_cast<const common::ColumnsFieldArray *>(
              cursor.get_cursor_handler()->get_result_set()->get_field_columns()));
          } else if (!cursor.is_ps_cursor()) {
            CK (OB_NOT_NULL(cursor.get_spi_cursor()));
            CK (cursor.get_spi_cursor()->fields_.count() > 0);
            OX (fields = &(cursor.get_spi_cursor()->fields_));
          } else {
            fields = &static_cast<pl::ObDbmsCursorInfo&>(cursor).get_field_columns();
          }
          if (OB_SUCC(ret) && lib::is_oracle_mode()) {
            // oracle 模式每次都需要返回head packet
            // mysql模式 兼容 mysql 协议，不返回 headpacket
            OZ (response_query_header(session, fields));
          }
          if (OB_SUCC(ret)) {
            // offset类型
            // TODO: 流式只能前滚，所以只能兼容next，default，relative(偏移量大于0)，absolute(其实位置大于当前位置)
            // 非流式用OB_RA_ROW_STORE实现，可以根据index访问任意行，兼容所有的offset类型
            if (cursor.is_streaming()) {
              // 流式结果集需要用到exec_ctx，不能用临时结果代替
              if (OB_NOT_NULL(cursor.get_cursor_handler()) &&
                  OB_NOT_NULL(cursor.get_cursor_handler()->get_result_set())){
                exec_ctx = &cursor.get_cursor_handler()->get_result_set()->get_exec_context();
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpect streaming result set.", K(ret), K(cursor.get_id()));
              }
              if (OB_OCI_DEFAULT != offset_type_ || OB_OCI_FETCH_NEXT != offset_type_) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("streaming result set not support this offset type.", K(ret), 
                                                                               K(cursor.get_id()), 
                                                                               K(offset_type_));
              }
            } else {
              tmp_exec_ctx.set_my_session(&session);
              tmp_exec_ctx.set_mem_attr(ObMemAttr(session.get_effective_tenant_id(),
                                                  ObModIds::OB_SQL_EXEC_CONTEXT,
                                                  ObCtxIds::EXECUTE_CTX_ID));
              exec_ctx = &tmp_exec_ctx;
              if (OB_ISNULL(cursor.get_spi_cursor())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("cursor result set is null.", K(ret), K(cursor.get_id()));
              } else {
                ObSPICursor *spi_cursor = cursor.get_spi_cursor();
                // ps cursor position is be record in current_position
                // ref cursor position is be record in get_spi_cursor()->cur_
                if (!cursor.is_ps_cursor() && cursor.get_spi_cursor()->cur_ > 0
                      && cursor.get_current_position() != cursor.get_spi_cursor()->cur_) {
                  cursor.set_current_position(cursor.get_spi_cursor()->cur_ - 1);
                }
                cur = cursor.get_current_position();
                max_count = spi_cursor->row_store_.get_row_cnt();
                if (max_count > 0) {
                  switch (offset_type_) {
                    case OB_OCI_DEFAULT: {
                      cur++;
                      break;
                    }
                    case OB_OCI_FETCH_CURRENT: {
                      break;
                    }
                    case OB_OCI_FETCH_NEXT: {
                      cur++;
                      break;
                    }
                    case OB_OCI_FETCH_FIRST: {
                      cur = 0;
                      if (max_count < fetch_limit) {
                        cursor.set_current_position(fetch_limit);
                        need_fetch = false;	
                        ret = OB_ITER_END;
                      }
                      break;
                    }
                    case OB_OCI_FETCH_LAST: {
                      const int64_t row_id = max_count - 1;
                      cur = max_count - 1;
                      if (cursor.get_last_row().is_invalid()) {
                        if (OB_FAIL(spi_cursor->row_store_.get_row(row_id, row))){
                          LOG_WARN("get first row fail.", K(ret), K(cursor.get_id()));
                        } else {
                          if (OB_FAIL(ob_write_row(cursor.get_cursor_entity()->get_arena_allocator(),
                                                    *row,
                                                    cursor.get_last_row()))){
                            LOG_WARN("Fail to get last row", K(ret), K(cursor.get_id()));
                          }
                        }
                      } else {
                        row = &cursor.get_last_row();
                      }
                      if (OB_SUCC(ret)) {
                        need_fetch = false;
                      }
                      break;
                    }
                    case OB_OCI_FETCH_PRIOR: {
                      // prior协议，无论offset设置为多少，每次都向前滚1行，所以cur需要减1
                      cur = cur - 1;
                      if (cur + fetch_limit > max_count) {
                        need_fetch = false;
                        cursor.set_current_position(max_count - 1);
                        ret = OB_ITER_END;
                      }
                      break;
                    }
                    case OB_OCI_FETCH_ABSOLUTE: {
                      cur = offset - 1;
                      break;
                    }
                    case OB_OCI_FETCH_RELATIVE: {
                      cur = cur + offset;
                      break;
                    }
                    default: {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_WARN("unexpect offset type.", K(ret));
                    }
                  }
                }
                if (OB_SUCC(ret) && (cur >= max_count || cur < 0 || max_count <= 0)) {
                  // fetch过程中，除了OB_ITER_END扫描到结果集末尾的报错被吞掉，其余报错会断开连接
                  // oracle中如果扫描超出了范围，不会断开连接，会报OCI_NO_DATA的错误
                  // 为了兼容这种表现，超出范围时设置OB_ITER_END错误码，不报错不返回数据，由驱动产生OCI_NO_DATA错误码
                  need_fetch = false;
                  ret = OB_ITER_END;
                }
                if (OB_SUCC(ret)) {
                  // 只要设置了正确的偏移量，cursor里的指针就要跟着移动
                  OZ (cursor.set_current_position(cur));
                }
              }
            }
          }
          if (OB_SUCC(ret) && !need_fetch && NULL != row) {
            if (has_long_data()) {
              OZ (response_row(session, *(const_cast<common::ObNewRow*>(row)), 
                               fields, column_flag_, cursor_id_, true));
            } else {
              OZ (response_row(session, *(const_cast<common::ObNewRow*>(row)), fields));
            }
            if (OB_FAIL(ret)) {
              LOG_WARN("response row fail.", K(ret));
            }
          }
          ObPLExecCtx pl_ctx(cursor.get_allocator(), exec_ctx, &params,
                            NULL/*result*/, &ret, NULL/*func*/, true);
          while (OB_SUCC(ret) && need_fetch && row_num < fetch_limit
                  && OB_SUCC(sql::ObSPIService::dbms_cursor_fetch(&pl_ctx,
                                                  static_cast<pl::ObDbmsCursorInfo&>(cursor)))) {
            common::ObNewRow &row = cursor.get_current_row();
#ifndef NDEBUG
            LOG_INFO("cursor fetch: ", K(cursor.get_id()),
                                       K(cursor.is_streaming()),
                                       K(cursor.is_ps_cursor()),
                                       K(cursor.get_current_row().cells_[0]),
                                       K(cursor.get_current_position()),
                                       K(offset), K(row_num), K(fetch_limit));
#endif
            cur = cursor.get_current_position();
            ++cur;
            cursor.set_current_position(cur);
            if (has_long_data()) {
              OZ (response_row(session, row, fields, column_flag_, cursor_id_,
                                0 == row_num ? true : false));
            } else {
              OZ (response_row(session, row, fields));
            }
            if (OB_SUCC(ret)) {
              ++row_num;
            } else {
              LOG_WARN("response row fail at line: ", K(ret), K(row_num));
            }
          }
          if (need_fetch) {
            cur = cursor.get_current_position();
            cur = cur - 1;
            cursor.set_current_position(cur);
          }
          if (OB_ITER_END == ret || OB_READ_NOTHING == ret) {
            ret = OB_SUCCESS;
            // need_fetch 为 true 并且 得到了 OB_ITER_END 错误码，说明正常找到了最后一行，要设置last_row
            if (need_fetch) {
              last_row = true;
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("response query result fail", K(ret));
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
        flags.status_flags_.OB_SERVER_STATUS_IN_TRANS
          = (session.is_server_status_in_transaction() ? 1 : 0);
        flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (ac ? 1 : 0);
        flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = has_ok_packet() ? true : false; /*no more result*/
        flags.status_flags_.OB_SERVER_STATUS_CURSOR_EXISTS = !last_row ? 1 : 0;
        if ((!cursor.is_streaming()
             && max_count == cursor.get_current_position() + 1)
              || last_row) {
          flags.status_flags_.OB_SERVER_STATUS_LAST_ROW_SENT = 1;
        } else {
          flags.status_flags_.OB_SERVER_STATUS_LAST_ROW_SENT = 0;
        }
        if (!session.is_obproxy_mode()) {
          // in java client or others, use slow query bit to indicate partition hit or not
          flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !session.partition_hit().get_bool();
        }
        eofp.set_server_status(flags);
        // for proxy
        // in multi-stmt, send extra ok packet in the last stmt(has no more result)
        if (OB_SUCC(ret)) {
          if (OB_FAIL(packet_sender_.alloc_ezbuf())) {
            LOG_WARN("failed to alloc easy buf", K(ret));
          } else if (!has_ok_packet() && OB_FAIL(update_last_pkt_pos())) {
            LOG_WARN("failed to update last packet pos", K(ret));
          } else if (last_row && !cursor.is_scrollable() 
                              && !cursor.is_streaming()
                              && cursor.is_ps_cursor()
                              && lib::is_oracle_mode() 
                              && OB_NOT_NULL(cursor.get_spi_cursor())
                              && cursor.get_spi_cursor()->row_store_.get_row_cnt() > 0) {
            set_close_cursor();
          }
        }
        // for obproxy
        if (OB_SUCC(ret)) {
          // in multi-stmt, send extra ok packet in the last stmt(has no more result)
          if (need_send_extra_ok_packet() || has_ok_packet()) {
            ObOKPParam ok_param;
            if (has_ok_packet()) {
              ok_param.affected_rows_ = last_row ? max_count : cursor.get_current_position() + 1;
            } else {
              ok_param.affected_rows_ = 0;
            }
            if ((!cursor.is_streaming()
                 && max_count == cursor.get_current_position() + 1)
                || last_row) {
              ok_param.send_last_row_ = true;
            } else {
              ok_param.send_last_row_ = false;
            }
            ok_param.is_partition_hit_ = session.partition_hit().get_bool();
            ok_param.has_more_result_ = false;
            if (OB_FAIL(send_ok_packet(session, ok_param, &eofp))) {
              LOG_WARN("fail to send ok packt", K(ok_param), K(ret));
            }
          } else {
            if (OB_FAIL(response_packet(eofp, &session))) {
              LOG_WARN("response packet fail", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) &&
      !process_ok &&
      !admission_fail_and_need_retry) {
    int sret = OB_SUCCESS;
    bool is_partition_hit = session.partition_hit().get_bool();
    if (OB_SUCCESS != (sret = send_error_packet(ret, NULL, is_partition_hit))) {
      LOG_WARN("send error packet fail", K(sret), K(ret));
    }
  }
  return ret;
}

int ObMPStmtFetch::process_fetch_stmt(ObSQLSessionInfo &session,
                                      bool &need_response_error)
{
  int ret = OB_SUCCESS;
  // 执行setup_wb后，所有WARNING都会写入到当前session的WARNING BUFFER中
  setup_wb(session);
  ObSessionStatEstGuard stat_est_guard(get_conn()->tenant_->id(), session.get_sessid());
  const bool enable_trace_log = lib::is_trace_log_enabled();
  if (enable_trace_log) {
    //set session log_level.Must use ObThreadLogLevelUtils::clear() in pair
    ObThreadLogLevelUtils::init(session.get_log_id_level_map());
  }
  // obproxy may use 'SET @@last_schema_version = xxxx' to set newest schema,
  // observer will force refresh schema if local_schema_version < last_schema_version;
  if (OB_FAIL(check_and_refresh_schema(session.get_login_tenant_id(),
                                       session.get_effective_tenant_id()))) {
    LOG_WARN("failed to check_and_refresh_schema", K(ret));
  } else {
    //每次执行不同sql都需要更新
    if (OB_FAIL(update_transmission_checksum_flag(session))) {
      LOG_WARN("update transmisson checksum flag failed", K(ret));
    } else {
      // do the real work
      ret = do_process(session, need_response_error);
    }
  }
  if (enable_trace_log) {
    ObThreadLogLevelUtils::clear();
  }
  const int64_t debug_sync_timeout = GCONF.debug_sync_timeout;
  if (debug_sync_timeout > 0) {
    // ignore thread local debug sync actions to session actions failed
    int tmp_ret = OB_SUCCESS;
    tmp_ret = GDS.collect_result_actions(session.get_debug_sync_actions());
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      LOG_WARN("set thread local debug sync actions to session actions failed", K(tmp_ret));
    }
  }
  //对于tracelog的处理，不影响正常逻辑，错误码无须赋值给ret
  {
    int tmp_ret = OB_SUCCESS;
    //清空WARNING BUFFER
    ObSqlCtx sql_ctx; // sql_ctx do nothing in do_after_process
    tmp_ret = do_after_process(session, sql_ctx, false/*no asyn response*/);
    UNUSED(tmp_ret);
  }
  return ret;
}
int ObMPStmtFetch::process()
{
  int ret = OB_SUCCESS;
  int flush_ret = OB_SUCCESS;
  bool need_disconnect = true;
  bool need_response_error = true;
  ObSQLSessionInfo *sess = NULL;
  int64_t query_timeout = 0;
  ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
  ObSMConnection *conn = get_conn();
  const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
  bool cursor_fetched = false;
  reset_close_cursor();
  if (OB_ISNULL(req_) || OB_ISNULL(conn) || OB_ISNULL(cur_trace_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null conn ptr", K_(cursor_id), K_(req), K(cur_trace_id), K(ret));
  } else if (OB_UNLIKELY(!conn->is_in_authed_phase())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("receive sql without session", K_(cursor_id), K(ret));
  } else if (OB_ISNULL(conn->tenant_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant", K_(cursor_id), K(conn->tenant_), K(ret));
  } else if (OB_FAIL(get_session(sess))) {
    LOG_WARN("get session fail", K_(cursor_id), K(ret));
  } else if (OB_ISNULL(sess)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL or invalid", K_(cursor_id), K(sess), K(ret));
  } else if (OB_FAIL(update_transmission_checksum_flag(*sess))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret));
  } else {
    ObSQLSessionInfo &session = *sess;
    int64_t tenant_version = 0;
    int64_t sys_version = 0;
    THIS_WORKER.set_session(sess);
    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    session.set_current_trace_id(ObCurTraceId::get_trace_id());
    session.get_raw_audit_record().request_memory_used_ = 0;
    observer::ObProcessMallocCallback pmcb(0,
          session.get_raw_audit_record().request_memory_used_);
    lib::ObMallocCallbackGuard guard(pmcb);
    int64_t packet_len = (reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet())).get_clen();
    if (OB_UNLIKELY(!session.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid session", K_(cursor_id), K(ret));
    } else if (OB_UNLIKELY(session.is_zombie())) {
      //session has been killed some moment ago
      ret = OB_ERR_SESSION_INTERRUPTED;
      LOG_WARN("session has been killed", K(session.get_session_state()), K_(cursor_id),
               K(session.get_sessid()), "proxy_sessid", session.get_proxy_sessid(), K(ret));
    } else if (OB_UNLIKELY(packet_len > session.get_max_packet_size())) {
      //packet size check with session variable max_allowd_packet or net_buffer_length
      ret = OB_ERR_NET_PACKET_TOO_LARGE;
      LOG_WARN("packet too large than allowed for the session", K_(cursor_id), K(ret));
    } else if (OB_FAIL(session.get_query_timeout(query_timeout))) {
      LOG_WARN("fail to get query timeout", K(ret));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                session.get_effective_tenant_id(), tenant_version))) {
      LOG_WARN("fail get tenant broadcast version", K(ret));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                OB_SYS_TENANT_ID, sys_version))) {
      LOG_WARN("fail get tenant broadcast version", K(ret));
    } else if (FALSE_IT(session.set_txn_free_route(pkt.txn_free_route()))) {
    } else if (OB_FAIL(process_extra_info(session, pkt, need_response_error))) {
      LOG_WARN("fail get process extra info", K(ret));
    } else if (FALSE_IT(session.post_sync_session_info())) {
    } else {
      need_disconnect = false;
      ObPLCursorInfo *cursor = NULL;
      THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout);
      session.partition_hit().reset();
      ret = process_fetch_stmt(session, need_response_error);
      // set cursor fetched info. if cursor has be fetched, we need to disconnect
      cursor = session.get_cursor(cursor_id_);
      if (OB_NOT_NULL(cursor) && cursor->get_fetched()) {
        cursor_fetched = true;
      }
      if (need_close_cursor()) {
        // close at here because after do_process, need read some cursor info for log in process_fetch_stmt
        int tmp_ret = session.close_cursor(cursor_id_);
        ret = ret == OB_SUCCESS ? tmp_ret : ret;
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("no scrollable cursor close cursor failed at last row.", K(tmp_ret));
        }
      }
    }
    session.check_and_reset_retry_info(*cur_trace_id, THIS_WORKER.need_retry());
    session.set_last_trace_id(ObCurTraceId::get_trace_id());
  }

  if (OB_FAIL(ret) && is_conn_valid()) {
    if (need_response_error) {
      send_error_packet(ret, NULL);
    }
    if (cursor_fetched || need_disconnect) {
      force_disconnect();
      LOG_WARN("disconnect connection when process query", K(ret));
    }
  }

  if (!THIS_WORKER.need_retry()) {
    flush_ret = flush_buffer(true);
  }
  THIS_WORKER.set_session(NULL);
  if (sess != NULL) {
    revert_session(sess); //current ignore revert session ret
  }
  return (OB_SUCCESS != ret) ? ret : flush_ret;
}

void ObMPStmtFetch::record_stat(const stmt::StmtType type, const int64_t end_time) const
{
  UNUSED(type);
  if (lib::is_diagnose_info_enabled()) {
    const int64_t time_cost = end_time - get_receive_timestamp();
    EVENT_INC(SQL_OTHER_COUNT);
    EVENT_ADD(SQL_OTHER_TIME, time_cost);
  }
}


int ObMPStmtFetch::response_row(ObSQLSessionInfo &session,
                                common::ObNewRow &src_row,
                                const ColumnsFieldArray *fields,
                                char *column_map,
                                int32_t stmt_id,
                                bool first_time)
{
  int ret = OB_SUCCESS;
  common::ObNewRow row;
  ObPieceCache *piece_cache = static_cast<ObPieceCache*>(session.get_piece_cache(true));
  if (OB_ISNULL(piece_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece cache is null.", K(ret), K(stmt_id));
  } else if (NULL != column_map) {
    if (OB_FAIL(ob_create_row(THIS_WORKER.get_sql_arena_allocator(), 
                              src_row.get_count(), 
                              row))) {
      LOG_WARN("create row fail.", K(ret), K(stmt_id));
    } else if (OB_FAIL(ob_write_row_by_projector(THIS_WORKER.get_sql_arena_allocator(), 
                                                 src_row, 
                                                 row))) {
      LOG_WARN("wirte tmp row fail.", K(stmt_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_map is null.", K(stmt_id), K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < row.get_count(); ++i) {
    ObObj &value = row.get_cell(i);
    if (OB_SUCC(ret) && NULL != column_map && ObSMUtils::update_from_bitmap(column_map, i)) {
      bool is_long_data = false;
      ObString str;
      ObPiece *piece = NULL;
      if (OB_FAIL(piece_cache->get_piece(stmt_id, i, piece))) {
        LOG_WARN("get piece fail", K(stmt_id), K(i), K(ret) );
      } else if (first_time) {
        if (NULL != piece) {
          if (OB_FAIL(piece_cache->remove_piece(piece_cache->get_piece_key(stmt_id, i),
                                                session))) {
            LOG_WARN("remove old piece fail.", K(ret), K(stmt_id), K(i));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(piece_cache->make_piece(stmt_id, i, piece, session))) {
          LOG_WARN("make piece fail.", K(ret), K(stmt_id), K(i));
        } 
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (NULL == piece) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("piece is null before use.", K(ret), K(stmt_id), K(i));
      } else if (is_lob_storage(value.get_type())) {
        ObTextStringIter iter(value);
        if (OB_FAIL(iter.init(0, &session, &(THIS_WORKER.get_sql_arena_allocator())))) {
          LOG_WARN("Lob: init lob str iter failed ", K(ret), K(value));
        } else if (OB_FAIL(iter.get_full_data(str))) {
          LOG_WARN("Lob: get full data failed ", K(ret), K(value));
        }
      } else if (ob_is_string_type(value.get_type()) || ob_is_raw(value.get_type())) {
        str = value.get_string();
      } else if (ob_is_rowid_tc(value.get_type())) {
        str = value.get_string_ptr();
        str.set_length(value.get_string_len());
      } else if (ob_is_lob_locator(value.get_type())) {
        str.assign(const_cast<char*>(value.get_lob_locator()->get_payload_ptr()),
                    static_cast<int64_t>(value.get_lob_locator()->get_payload_length()));
      } else if (ob_is_null(value.get_type())) {
        str.assign(NULL, 0);
      } else if (ob_is_extend(value.get_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("long data protocol not support array type yet.",  
                    K(stmt_id), K(i), K(value.get_type()));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("long piece protocol not support type: ", K(value.get_type()));
      }
      /* 
        * Piece's memory management strategy:
        * 1. All pieces allocate their own memory through session
        * 2. Each piece has a separate memory, which is maintained by itself, 
        *    and needs to be released when each piece dies
        * 3. The memory of piecebuffer itself comes from piece, 
        *    and the buf memory on piecebuffer also comes from this
        * 4. The piecebuffer should be cleaned up immediately after use, 
        *    and two parts should be cleaned up, 
        *    the memory occupied by the buf and the memory of the piecebuffer itself
        * 5. Clean up all pieces when the connection is closed, 
        *    and clean up the memory of piecebuffer->buf, piecebuffer and piece step by step
        */ 
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(piece_cache->add_piece_buffer(piece, ObInvalidPiece, &str))) {
        LOG_WARN("add piece buffer fail.", K(ret), K(stmt_id));
      } else {
        value.set_null();
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(response_row(session, row, fields))) {
    LOG_WARN("response row fail.", K(ret), K(stmt_id));
  } else {
    LOG_DEBUG("response row success.", K(stmt_id));
  }
  return ret;
}


} //end of namespace observer
} //end of namespace oceanbase

