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

#include "observer/mysql/obmp_stmt_execute.h"

#include "lib/oblog/ob_log.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/encode/ob_base64_encode.h"
#include "observer/mysql/obsm_utils.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/packet/ompk_resheader.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "observer/mysql/obsm_struct.h"
#include "observer/mysql/obsm_row.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_time_utility2.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "observer/omt/ob_tenant.h"
#include "observer/mysql/ob_sync_plan_driver.h"
#include "observer/mysql/ob_sync_cmd_driver.h"
#include "observer/mysql/ob_async_cmd_driver.h"
#include "observer/mysql/ob_async_plan_driver.h"
#include "observer/mysql/obmp_stmt_send_long_data.h"
#include "observer/ob_req_time_service.h"

namespace oceanbase {

using namespace common;
using namespace share;
using namespace obmysql;
using namespace rpc;
using namespace sql;
namespace observer {
ObMPStmtExecute::ObMPStmtExecute(const ObGlobalContext& gctx)
    : ObMPBase(gctx),
      retry_ctrl_(/*ctx_.retry_info_*/),
      ctx_(),
      stmt_id_(),
      stmt_type_(stmt::T_NONE),
      params_(NULL),
      arraybinding_params_(NULL),
      arraybinding_columns_(NULL),
      arraybinding_row_(NULL),
      is_arraybinding_(false),
      is_save_exception_(false),
      arraybinding_size_(0),
      arraybinding_rowcnt_(0),
      is_cursor_readonly_(false),
      single_process_timestamp_(0),
      exec_start_timestamp_(0),
      exec_end_timestamp_(0),
      params_num_(0)
{
  ctx_.exec_type_ = MpQuery;
}

int ObMPStmtExecute::init_field_for_arraybinding()
{
  int ret = OB_SUCCESS;

  ObField sql_no_field, err_no_field, err_msg_field;

  OX(sql_no_field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI);
  OX(sql_no_field.type_.set_type(ObIntType));
  OZ(common::ObField::get_field_mb_length(
      sql_no_field.type_.get_type(), sql_no_field.accuracy_, common::CS_TYPE_INVALID, sql_no_field.length_));
  OX(sql_no_field.cname_ = ObString("sql_no"));

  OX(err_no_field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI);
  OX(err_no_field.type_.set_type(ObIntType));
  OZ(common::ObField::get_field_mb_length(
      err_no_field.type_.get_type(), err_no_field.accuracy_, common::CS_TYPE_INVALID, err_no_field.length_));
  OX(err_no_field.cname_ = ObString("error_code"));

  OX(err_msg_field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI);
  OX(err_msg_field.type_.set_type(ObVarcharType));
  OZ(common::ObField::get_field_mb_length(
      err_msg_field.type_.get_type(), err_msg_field.accuracy_, common::CS_TYPE_INVALID, err_msg_field.length_));
  OX(err_msg_field.cname_ = ObString("error_message"));

  OZ(arraybinding_columns_->push_back(sql_no_field));
  OZ(arraybinding_columns_->push_back(err_no_field));
  OZ(arraybinding_columns_->push_back(err_msg_field));

  return ret;
}

int ObMPStmtExecute::init_row_for_arraybinding(ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  ObObj* obj = static_cast<ObObj*>(alloc.alloc(sizeof(ObObj) * 3));
  if (OB_ISNULL(obj)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for row", K(ret));
  } else {
    ObObj* ptr = obj;
    for (int64_t i = 0; i < 3; ++i) {
      ptr = new (ptr) ObObj();
      ptr++;
    }
    arraybinding_row_->assign(obj, 3);
  }
  return ret;
}

int ObMPStmtExecute::init_for_arraybinding(ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arraybinding_params_ = static_cast<ParamStore*>(alloc.alloc(sizeof(ParamStore))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (is_save_exception_) {
    if (OB_ISNULL(arraybinding_columns_ = static_cast<ColumnsFieldArray*>(alloc.alloc(sizeof(ColumnsFieldArray))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_ISNULL(arraybinding_row_ = static_cast<ObNewRow*>(alloc.alloc(sizeof(ObNewRow))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      arraybinding_columns_ = new (arraybinding_columns_) ColumnsFieldArray(alloc, 3);
      arraybinding_row_ = new (arraybinding_row_) ObNewRow();
    }
    OZ(init_field_for_arraybinding());
    OZ(init_row_for_arraybinding(alloc));
  }
  OX(arraybinding_params_ = new (arraybinding_params_) ParamStore((ObWrapperAllocator(alloc))));
  return ret;
}

int ObMPStmtExecute::check_param_type_for_arraybinding(
    ObSQLSessionInfo* session_info, ParamTypeInfoArray& param_type_infos)
{
  int ret = OB_SUCCESS;
  if (!ObStmt::is_dml_write_stmt(stmt_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("oci arraybinding only support write dml", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "oci arraybinding got no write dml");
  } else if (session_info->get_local_autocommit()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("oci arraybinding must in autocommit off", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "oci arraybinding has autocommit = on");
  } else if (OB_UNLIKELY(param_type_infos.count() <= 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("oci arraybinding must has parameters", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "oci arraybinding has no parameter");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_type_infos.count(); ++i) {
      TypeInfo& type_info = param_type_infos.at(i);
      if (type_info.is_basic_type_ || !type_info.is_elem_type_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("oci arraybinding parameter must be anonymous array", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "oci arraybinding parameter is not anonymous array");
      }
    }
  }
  return ret;
}

int ObMPStmtExecute::check_param_value_for_arraybinding(ObObjParam& param)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support array binging", K(param));
  return ret;
}

int ObMPStmtExecute::construct_execute_param_for_arraybinding(int64_t pos)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support array binging", K(pos));
  return ret;
}

void ObMPStmtExecute::reset_collection_param_for_arraybinding()
{
  return;
}

int ObMPStmtExecute::send_eof_packet_for_arraybinding(ObSQLSessionInfo& session_info)
{
  int ret = OB_SUCCESS;

  OMPKEOF eofp;
  const ObWarningBuffer* warnings_buf = common::ob_get_tsi_warning_buffer();
  uint16_t warning_count = 0;
  if (OB_ISNULL(warnings_buf)) {
    LOG_WARN("can not get thread warnings buffer");
  } else {
    warning_count = static_cast<uint16_t>(warnings_buf->get_readable_warning_count());
  }
  eofp.set_warning_count(warning_count);
  ObServerStatusFlags flags = eofp.get_server_status();
  flags.status_flags_.OB_SERVER_STATUS_IN_TRANS = (session_info.is_server_status_in_transaction() ? 1 : 0);
  flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (session_info.get_local_autocommit() ? 1 : 0);
  flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = true;
  if (!session_info.is_obproxy_mode()) {
    flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !session_info.partition_hit().get_bool();
  }
  eofp.set_server_status(flags);
  OZ(response_packet(eofp));

  return ret;
}

int ObMPStmtExecute::response_result_for_arraybinding(
    ObSQLSessionInfo& session_info, ObIArray<ObSavedException>& exception_array)
{
  int ret = OB_SUCCESS;
  if (exception_array.count() > 0) {

    OMPKResheader rhp;
    rhp.set_field_count(3);
    OZ(response_packet(rhp));

    for (int64_t i = 0; OB_SUCC(ret) && i < arraybinding_columns_->count(); ++i) {
      ObMySQLField field;
      OZ(ObMySQLResultSet::to_mysql_field(arraybinding_columns_->at(i), field));
      OMPKField fp(field);
      OZ(response_packet(fp));
    }

    OZ(send_eof_packet_for_arraybinding(session_info));

    for (int64_t i = 0; OB_SUCC(ret) && i < exception_array.count(); ++i) {
      arraybinding_row_->get_cell(0).set_int(exception_array.at(i).pos_);
      arraybinding_row_->get_cell(1).set_int(exception_array.at(i).error_code_);
      arraybinding_row_->get_cell(2).set_varchar(exception_array.at(i).error_msg_);

      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(&session_info);
      OMPKRow rp(ObSMRow(BINARY,
          *arraybinding_row_,
          dtc_params,
          arraybinding_columns_,
          ctx_.schema_guard_,
          session_info.get_effective_tenant_id()));
      OZ(response_packet(rp));
    }
    OZ(send_eof_packet_for_arraybinding(session_info));
  }
  if (OB_SUCC(ret)) {
    ObOKPParam ok_param;
    ok_param.affected_rows_ = arraybinding_rowcnt_;
    ok_param.is_partition_hit_ = session_info.partition_hit().get_bool();
    OZ(send_ok_packet(session_info, ok_param));
  }
  return ret;
}

int ObMPStmtExecute::save_exception_for_arraybinding(
    int64_t pos, int error_code, ObIArray<ObSavedException>& exception_array)
{
  int ret = OB_SUCCESS;
  ObSavedException exception;

  const char* errm_result = NULL;
  int64_t errm_length = 0;

  exception.pos_ = pos;
  exception.error_code_ = static_cast<uint16_t>(ob_errpkt_errno(error_code, lib::is_oracle_mode()));

  ObIAllocator& alloc = CURRENT_CONTEXT->get_arena_allocator();

  const ObWarningBuffer* wb = common::ob_get_tsi_warning_buffer();
  if (OB_LIKELY(NULL != wb) && wb->get_err_code() == error_code) {
    errm_result = wb->get_err_msg();
    errm_length = strlen(errm_result);
  } else {
    errm_result = ob_errpkt_strerror(error_code, true);
    if (NULL == errm_result) {
      errm_result = "ORA%ld: Message error_code not found; product=RDBMS; facility=ORA";
    }
    errm_length = strlen(errm_result);
  }

  OZ(ob_write_string(alloc, ObString(errm_length, errm_result), exception.error_msg_));
  OZ(exception_array.push_back(exception));
  return ret;
}

int ObMPStmtExecute::after_do_process_for_arraybinding(ObMySQLResultSet& result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result.get_physical_plan())) {
    ret = OB_NOT_INIT;
    LOG_WARN("should have set plan to result set", K(ret));
  } else if (OB_FAIL(result.sync_open())) {
    int cret = OB_SUCCESS;
    int cli_ret = OB_SUCCESS;
    retry_ctrl_.test_and_save_retry_state(gctx_, ctx_, result, ret, cli_ret, true /*arraybinding only local retry*/);
    if (OB_TRANSACTION_SET_VIOLATION != ret && OB_REPLICA_NOT_READABLE != ret) {
      if (OB_TRY_LOCK_ROW_CONFLICT == ret && retry_ctrl_.need_retry()) {
        // lock conflict error is very common, need not LOG_WARN,
        // otherwise we may print lots of log.
      } else {
        LOG_WARN("result set open failed, check if need retry", K(ret), K(cli_ret), K(retry_ctrl_.need_retry()));
      }
    }
    ret = cli_ret;
    cret = result.close(retry_ctrl_.need_retry());
    if (cret != OB_SUCCESS && cret != OB_TRANSACTION_SET_VIOLATION && OB_TRY_LOCK_ROW_CONFLICT != cret) {
      LOG_WARN("close result set fail", K(cret));
    }
  } else if (result.is_with_rows()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("in oci arraybinding, dml with rows is not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "in oci arraybinding, dml with rows");
  } else {
    OZ(result.close());
    OX(arraybinding_rowcnt_ += result.get_affected_rows());
  }
  return ret;
}

int ObMPStmtExecute::before_process()
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
    ObIAllocator& alloc = CURRENT_CONTEXT->get_arena_allocator();
    if (OB_ISNULL(params_ = static_cast<ParamStore*>(alloc.alloc(sizeof(ParamStore))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      params_ = new (params_) ParamStore((ObWrapperAllocator(alloc)));
    }
    const ObMySQLRawPacket& pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    const char* pos = pkt.get_cdata();
    int32_t stmt_id = -1;  // INVALID_STMT_ID
    ObMySQLUtil::get_int4(pos, stmt_id);
    stmt_id_ = stmt_id;

    // pos += 1; //skip flags
    int8_t flag = 0;
    ObMySQLUtil::get_int1(pos, flag);
    const uint8_t ARRAYBINDING_MODE = 8;
    const uint8_t SAVE_EXCEPTION_MODE = 16;
    is_arraybinding_ = flag & ARRAYBINDING_MODE;
    is_save_exception_ = flag & SAVE_EXCEPTION_MODE;
    is_cursor_readonly_ = flag & CURSOR_TYPE_READ_ONLY;

    // 4 bytes, iteration-count, used for checksum
    uint32_t ps_stmt_checksum = 0;
    ObMySQLUtil::get_uint4(pos, ps_stmt_checksum);
    const uint32_t DEFAULT_ITERATION_COUNT = 1;

    ObSQLSessionInfo* session = NULL;
    if (is_arraybinding_) {
      OZ(init_for_arraybinding(alloc));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_session(session))) {
      LOG_WARN("get session failed");
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL or invalid", K(ret), K(session));
    } else {
      ObPsSessionInfo* ps_session_info = NULL;
      ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
      ObCharsetType charset = CHARSET_INVALID;
      ObCollationType cs_conn = CS_TYPE_INVALID;
      ObCollationType cs_server = CS_TYPE_INVALID;
      share::schema::ObSchemaGetterGuard schema_guard;
      const uint64_t tenant_id = session->get_effective_tenant_id();
      if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(session->get_character_set_connection(charset))) {
        LOG_WARN("get charset for client failed", K(ret));
      } else if (OB_FAIL(session->get_collation_connection(cs_conn))) {
        LOG_WARN("get charset for client failed", K(ret));
      } else if (OB_FAIL(session->get_collation_server(cs_server))) {
        LOG_WARN("get charset for client failed", K(ret));
      } else if (OB_FAIL(session->get_ps_session_info(stmt_id_, ps_session_info))) {
        LOG_WARN("get_ps_session_info failed", K(ret), K_(stmt_id));
      } else if (OB_ISNULL(ps_session_info)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ps_session_info is null", K(ret));
      } else if (DEFAULT_ITERATION_COUNT == ps_stmt_checksum) {
        // do nothing
      } else if (ps_stmt_checksum != ps_session_info->get_ps_stmt_checksum()) {
        ret = OB_ERR_PREPARE_STMT_CHECKSUM;
        LOG_ERROR("ps stmt checksum fail",
            K(ret),
            "session_id",
            session->get_sessid(),
            K(ps_stmt_checksum),
            K(*ps_session_info));
      }
      if (OB_SUCC(ret)) {
        LOG_TRACE("ps session info", K(ret), "session_id", session->get_sessid(), K(*ps_session_info));
        share::schema::ObSchemaGetterGuard* old_guard = ctx_.schema_guard_;
        ObSQLSessionInfo* old_sess_info = ctx_.session_info_;
        ctx_.schema_guard_ = &schema_guard;
        ctx_.session_info_ = session;
        const int64_t params_num_ = ps_session_info->get_param_count();
        stmt_type_ = ps_session_info->get_stmt_type();
        int8_t new_param_bound_flag = 0;
        if (params_num_ > 0) {
          // Step1: handle bitmap
          int64_t bitmap_types = (params_num_ + 7) / 8;
          const char* bitmap = pos;
          pos += bitmap_types;
          // Step2: get new_param_bound_flag
          ObMySQLUtil::get_int1(pos, new_param_bound_flag);
          ParamTypeArray& param_types = ps_session_info->get_param_types();
          ParamTypeInfoArray param_type_infos;
          ParamCastArray param_cast_infos;
          if (new_param_bound_flag == 1) {
            param_types.reuse();
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(param_type_infos.prepare_allocate(params_num_))) {
            LOG_WARN("array prepare allocate failed", K(ret));
          } else if (OB_FAIL(params_->prepare_allocate(params_num_))) {
            LOG_WARN("array prepare allocate failed", K(ret));
          } else if (OB_FAIL(param_cast_infos.prepare_allocate(params_num_))) {
            LOG_WARN("array prepare allocate failed", K(ret));
          } else if (is_arraybinding_) {
            CK(OB_NOT_NULL(arraybinding_params_));
            OZ(arraybinding_params_->prepare_allocate(params_num_));
          }
          // Step3: get type
          for (int i = 0; OB_SUCC(ret) && i < params_num_; ++i) {
            uint8_t type = 0;
            int8_t flag = 0;
            if (1 == new_param_bound_flag) {
              ObMySQLUtil::get_uint1(pos, type);
              ObMySQLUtil::get_int1(pos, flag);
              if (OB_FAIL(param_types.push_back(static_cast<EMySQLFieldType>(type)))) {
                LOG_WARN("push back field failed", K(ret));
              }
            } else {
              if (params_num_ != param_types.count()) {
                ret = OB_ERR_WRONG_DYNAMIC_PARAM;
                LOG_USER_ERROR(OB_ERR_WRONG_DYNAMIC_PARAM, param_types.count(), params_num_);
              } else {
                type = static_cast<uint8_t>(param_types.at(i));
              }
            }
            if (OB_SUCC(ret)) {
              TypeInfo& type_name_info = param_type_infos.at(i);
              uint8_t elem_type = 0;
              param_cast_infos.at(i) = false;
              if (EMySQLFieldType::MYSQL_TYPE_COMPLEX == type) {
                type_name_info.is_basic_type_ = false;
                if (OB_FAIL(decode_type_info(pos, type_name_info))) {
                  LOG_WARN("failed to decode type info", K(ret));
                } else if (type_name_info.type_name_.empty()) {
                  type_name_info.is_elem_type_ = true;
                  ObMySQLUtil::get_uint1(pos, elem_type);
                  ObObjType ob_elem_type;
                  OZ(ObSMUtils::get_ob_type(ob_elem_type, static_cast<EMySQLFieldType>(elem_type)), elem_type);
                  OX(type_name_info.elem_type_.set_obj_type(ob_elem_type));
                  if (OB_SUCC(ret) && EMySQLFieldType::MYSQL_TYPE_COMPLEX == elem_type) {
                    OZ(decode_type_info(pos, type_name_info));
                  }
                }
              }
            }
          }

          if (OB_SUCC(ret) && is_arraybinding_) {
            OZ(check_param_type_for_arraybinding(session, param_type_infos));
          }

          // Step4: check input param for CallProcedure.
          ParamTypeInfoArray& dst_type_infos = ps_session_info->get_param_type_infos();
          if (OB_SUCC(ret) && stmt::T_CALL_PROCEDURE == ps_session_info->get_stmt_type() &&
              ps_session_info->get_param_type_infos().count() > 0) {
            ret = OB_NOT_SUPPORTED;
          }
          // Step5: decode value
          const char* params = pos;
          for (int64_t i = 0; OB_SUCC(ret) && i < params_num_; ++i) {
            ObObjParam& param = is_arraybinding_ ? arraybinding_params_->at(i) : params_->at(i);
            ObObjType ob_type;
            if (OB_FAIL(ObSMUtils::get_ob_type(ob_type, static_cast<EMySQLFieldType>(param_types.at(i))))) {
              LOG_WARN("cast ob type from mysql type failed", K(ob_type), K(param_types.at(i)), K(ret));
            } else {
              param.set_type(ob_type);
              param.set_param_meta();
              bool is_null = ObSMUtils::update_from_bitmap(param, bitmap, i);
              if (is_null) {
                LOG_DEBUG("param is null", K(i), K(param));
              } else if (OB_FAIL(parse_param_value(alloc,
                             param_types.at(i),
                             charset,
                             lib::is_oracle_mode() ? cs_server : cs_conn,
                             session->get_nls_collation_nation(),
                             params,
                             session->get_timezone_info(),
                             &(param_type_infos.at(i)),
                             param_cast_infos.at(i) ? &(dst_type_infos.at(i)) : NULL,
                             param,
                             i))) {
                LOG_WARN("get param value failed", K(param), K(i));
              } else {
                LOG_TRACE("execute with param", K(param), K(i));
              }
            }
            if (OB_SUCC(ret) && is_arraybinding_) {
              // OZ (check_param_value_for_arraybinding(param));
            }
          }  // for end
        }
        ctx_.schema_guard_ = old_guard;
        ctx_.session_info_ = old_sess_info;
      }
    }
    if (session != NULL) {
      revert_session(session);
    }
  }
  if (OB_FAIL(ret)) {
    send_error_packet(ret, NULL);
    if (OB_ERR_PREPARE_STMT_CHECKSUM == ret) {
      disconnect();
      LOG_WARN("prepare stmt checksum error, disconnect connection", K(ret));
    }
    flush_buffer(true);
  }

  return ret;
}

int ObMPStmtExecute::decode_type_info(const char*& buf, TypeInfo& type_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    uint64_t length = 0;
    if (OB_FAIL(ObMySQLUtil::get_length(buf, length))) {
      LOG_WARN("failed to get length", K(ret));
    } else {
      type_info.relation_name_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
      buf += length;
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t length = 0;
    if (OB_FAIL(ObMySQLUtil::get_length(buf, length))) {
      LOG_WARN("failed to get length", K(ret));
    } else {
      type_info.type_name_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
      buf += length;
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t version = 0;
    if (OB_FAIL(ObMySQLUtil::get_length(buf, version))) {
      LOG_WARN("failed to get version", K(ret));
    }
  }
  return ret;
}

int ObMPStmtExecute::set_session_active(ObSQLSessionInfo& session) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session.set_session_state(QUERY_ACTIVE))) {
    LOG_WARN("fail to set session state", K(ret));
  } else {
    session.set_query_start_time(get_receive_timestamp());
    session.set_mysql_cmd(obmysql::OB_MYSQL_COM_STMT_EXECUTE);
    session.update_last_active_time();
  }
  return ret;
}

int ObMPStmtExecute::do_process(
    ObSQLSessionInfo& session, const bool has_more_result, const bool force_sync_resp, bool& async_resp_used)
{
  int ret = OB_SUCCESS;
  ObAuditRecordData& audit_record = session.get_audit_record();
  bool is_diagnostics_stmt = false;
  bool need_response_error = is_arraybinding_ ? false : true;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  const bool enable_sql_audit = GCONF.enable_sql_audit && session.get_local_ob_enable_sql_audit();

  single_process_timestamp_ = ObTimeUtility::current_time();

  // req_timeinfo_guard should be defined before result.
  ObReqTimeGuard req_timeinfo_guard;
  SMART_VAR(ObMySQLResultSet, result, session, THIS_WORKER.get_sql_arena_allocator())
  {
    ObWaitEventStat total_wait_desc;
    int64_t execution_id = 0;
    ObDiagnoseSessionInfo* di = ObDiagnoseSessionInfo::get_local_diagnose_info();
    {
      ObMaxWaitGuard max_wait_guard(enable_perf_event ? &audit_record.exec_record_.max_wait_event_ : NULL, di);
      ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
      ObSQLMockSchemaGuard mock_schema_guard;
      if (enable_sql_audit) {
        audit_record.exec_record_.record_start(di);
      }

      result.init_partition_location_cache(gctx_.location_cache_, gctx_.self_addr_, ctx_.schema_guard_);
      result.set_has_more_result(has_more_result);
      result.set_ps_protocol();
      ObTaskExecutorCtx* task_ctx = result.get_exec_context().get_task_executor_ctx();
      if (OB_ISNULL(task_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("task executor ctx can not be NULL", K(task_ctx), K(ret));
      } else {
        task_ctx->schema_service_ = gctx_.schema_service_;
        task_ctx->set_query_tenant_begin_schema_version(retry_ctrl_.get_tenant_local_schema_version());
        task_ctx->set_query_sys_begin_schema_version(retry_ctrl_.get_sys_local_schema_version());
        task_ctx->set_min_cluster_version(GET_MIN_CLUSTER_VERSION());

        ctx_.retry_times_ = retry_ctrl_.get_retry_times();
        ctx_.partition_location_cache_ = &(result.get_partition_location_cache());
        if (OB_ISNULL(ctx_.schema_guard_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("newest schema is NULL", K(ret));
        } else if (OB_FAIL(result.init())) {
          LOG_WARN("result set init failed", K(ret));
        } else if (OB_ISNULL(gctx_.sql_engine_) || OB_ISNULL(params_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid sql engine", K(ret), K(gctx_), K(params_));
        } else if (FALSE_IT(execution_id = gctx_.sql_engine_->get_execution_id())) {
          // do nothing ...
        } else if (OB_FAIL(set_session_active(session))) {
          LOG_WARN("fail to set session active", K(ret));
        } else if (is_cursor_readonly_) {
          ret = OB_NOT_SUPPORTED;
        } else if (OB_FAIL(gctx_.sql_engine_->stmt_execute(
                       stmt_id_, stmt_type_, *params_, ctx_, result, false /* is_inner_sql */))) {
          exec_start_timestamp_ = ObTimeUtility::current_time();
          if (!THIS_WORKER.need_retry()) {
            int cli_ret = OB_SUCCESS;
            retry_ctrl_.test_and_save_retry_state(
                gctx_, ctx_, result, ret, cli_ret, is_arraybinding_ /*ararybinding only local retry*/);
            if (OB_ERR_PROXY_REROUTE == ret) {
              LOG_DEBUG("run stmt_query failed, check if need retry",
                  K(ret),
                  K(cli_ret),
                  K(retry_ctrl_.need_retry()),
                  K_(stmt_id));
            } else {
              LOG_WARN("run stmt_query failed, check if need retry",
                  K(ret),
                  K(cli_ret),
                  K(retry_ctrl_.need_retry()),
                  K_(stmt_id));
            }
            ret = cli_ret;
          }
          if (OB_ERR_PROXY_REROUTE == ret && !is_arraybinding_) {
            need_response_error = true;
          }
        } else {
          if (enable_perf_event) {
            exec_start_timestamp_ = ObTimeUtility::current_time();
          }
          need_response_error = false;
          is_diagnostics_stmt = ObStmt::is_diagnostic_stmt(result.get_literal_stmt_type());
          ctx_.is_show_trace_stmt_ = ObStmt::is_show_trace_stmt(result.get_literal_stmt_type());
          session.set_current_execution_id(execution_id);
          session.set_last_trace_id(ObCurTraceId::get_trace_id());
          if (OB_FAIL(ret)) {
          } else if (is_arraybinding_) {
            if (OB_FAIL(after_do_process_for_arraybinding(result))) {
              LOG_WARN("failed to process arraybinding sql", K(ret));
            }
          } else if (OB_FAIL(response_result(result, session, force_sync_resp, async_resp_used))) {
            ObPhysicalPlanCtx* plan_ctx = result.get_exec_context().get_physical_plan_ctx();
            if (OB_ISNULL(plan_ctx)) {
              LOG_ERROR("execute query fail, and plan_ctx is NULL", K(ret));
            } else {
              LOG_WARN("execute query fail", K(ret), "timeout_timestamp", plan_ctx->get_timeout_timestamp());
            }
          }
          if ((OB_SUCC(ret) && is_diagnostics_stmt) || async_resp_used) {
            // if diagnostic stmt succeed, no need to clear warning buf.
            // or async resp is used, it will be cleared in callback thread.
          } else {
            session.set_show_warnings_buf(ret);
          }
        }
        if (enable_perf_event) {
          exec_end_timestamp_ = ObTimeUtility::current_time();
          record_stat(result.get_stmt_type(), exec_end_timestamp_);
        }
        if (enable_sql_audit) {
          audit_record.exec_record_.record_end(di);
          bool first_record = (0 == audit_record.try_cnt_);
          ObExecStatUtils::record_exec_timestamp(*this, first_record, audit_record.exec_timestamp_);
        }
        if (OB_FAIL(ret)
            && !async_resp_used
            && need_response_error
            && conn_valid_
            && !THIS_WORKER.need_retry()
            && !retry_ctrl_.need_retry()) {
          LOG_WARN("query failed", K(ret), K(retry_ctrl_.need_retry()), K_(stmt_id));
          bool is_partition_hit = session.get_err_final_partition_hit(ret);
          int err = send_error_packet(ret, NULL, is_partition_hit, (void *)(&ctx_.reroute_info_));
          if (OB_SUCCESS != err) {  // send error package
            LOG_WARN("send error packet failed", K(ret), K(err));
          }
        }
      }
    }
    if (OB_SUCC(ret) && !is_cursor_readonly_) {
      // cursor has been handled in inner open.
      ObPhysicalPlan* plan = NULL;
      if (session.get_in_transaction()) {
        if (ObStmt::is_write_stmt(result.get_stmt_type(), result.has_global_variable())) {
          session.set_has_exec_write_stmt(true);
        }
      } else {
        session.set_has_exec_write_stmt(false);
      }
      plan = result.get_physical_plan();
      audit_record.status_ = (0 == ret || OB_ITER_END == ret) ? REQUEST_SUCC : (ret);
      if (enable_sql_audit) {
        audit_record.seq_ = 0;  // don't use now
        audit_record.execution_id_ = execution_id;
        audit_record.client_addr_ = session.get_peer_addr();
        audit_record.user_client_addr_ = session.get_user_client_addr();
        audit_record.user_group_ = THIS_WORKER.get_group_id();
        MEMCPY(audit_record.sql_id_, ctx_.sql_id_, (int32_t)sizeof(audit_record.sql_id_));
        if (NULL != plan) {
          audit_record.plan_type_ = plan->get_plan_type();
          audit_record.table_scan_ = plan->contain_table_scan();
          audit_record.plan_id_ = plan->get_plan_id();
          audit_record.plan_hash_ = plan->get_plan_hash_value();
        }
        audit_record.affected_rows_ = result.get_affected_rows();
        audit_record.return_rows_ = result.get_return_rows();
        audit_record.partition_cnt_ = result.get_exec_context().get_task_exec_ctx().get_related_part_cnt();
        audit_record.expected_worker_cnt_ = result.get_exec_context().get_task_exec_ctx().get_expected_worker_cnt();
        audit_record.used_worker_cnt_ = result.get_exec_context().get_task_exec_ctx().get_allocated_worker_cnt();

        audit_record.exec_record_.wait_time_end_ = total_wait_desc.time_waited_;
        audit_record.exec_record_.wait_count_end_ = total_wait_desc.total_waits_;

        audit_record.is_executor_rpc_ = false;
        audit_record.is_inner_sql_ = false;
        audit_record.is_hit_plan_cache_ = result.get_is_from_plan_cache();
        audit_record.sql_ = const_cast<char*>(ctx_.cur_sql_.ptr());
        audit_record.sql_len_ = min(ctx_.cur_sql_.length(), OB_MAX_SQL_LENGTH);
        audit_record.ps_stmt_id_ = stmt_id_;

        ObPhysicalPlanCtx* plan_ctx = result.get_exec_context().get_physical_plan_ctx();
        if (OB_NOT_NULL(plan_ctx)) {
          audit_record.consistency_level_ = plan_ctx->get_consistency_level();
        }
        audit_record.tenant_id_=session.get_effective_tenant_id();
        audit_record.update_stage_stat();
      }
      // update v$sql statistics
      if ((OB_SUCC(ret) || audit_record.is_timeout()) && session.get_local_ob_enable_plan_cache() &&
          !retry_ctrl_.need_retry()) {
        ObIArray<ObTableRowCount>* table_row_count_list = NULL;
        ObPhysicalPlanCtx* plan_ctx = result.get_exec_context().get_physical_plan_ctx();
        if (OB_NOT_NULL(plan_ctx)) {
          table_row_count_list = &(plan_ctx->get_table_row_count_list());
          audit_record.table_scan_stat_ = plan_ctx->get_table_scan_stat();
        }
        if (NULL != plan) {
          if (!(ctx_.self_add_plan_) && ctx_.plan_cache_hit_) {
            plan->update_plan_stat(audit_record,
                false,  // false mean not first update plan stat
                result.get_exec_context().get_is_evolution(),
                table_row_count_list);
            plan->update_cache_access_stat(audit_record.table_scan_stat_);
          } else if (ctx_.self_add_plan_ && !ctx_.plan_cache_hit_) {
            plan->update_plan_stat(
                audit_record, true, result.get_exec_context().get_is_evolution(), table_row_count_list);
            plan->update_cache_access_stat(audit_record.table_scan_stat_);
          }
        }
      }
    }
  }

  // reset thread waring buffer in sync mode
  if (!async_resp_used) {
    clear_wb_content(session);
  }

  bool need_retry = (THIS_THWORKER.need_retry() || RETRY_TYPE_NONE != retry_ctrl_.get_retry_type());
  ObSQLUtils::handle_audit_record(need_retry, EXECUTE_PS_EXECUTE, session, result.get_exec_context());
  return ret;
}

// return false only if send packet fail.
int ObMPStmtExecute::response_result(
    ObMySQLResultSet& result, ObSQLSessionInfo& session, bool force_sync_resp, bool& async_resp_used)
{
  int ret = OB_SUCCESS;
  bool need_trans_cb = result.need_end_trans_callback() && (!force_sync_resp);

  // NG_TRACE_EXT(exec_begin, ID(arg1), force_sync_resp, ID(end_trans_cb), need_trans_cb);

  if (OB_LIKELY(NULL != result.get_physical_plan())) {
    if (need_trans_cb) {
      ObAsyncPlanDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      // NOTE: sql_end_cb must be inited before drv.response_result().
      ObSqlEndTransCb& sql_end_cb = session.get_mysql_end_trans_cb();
      if (OB_FAIL(sql_end_cb.init(const_cast<rpc::ObRequest*>(req_), &session, seq_, conn_valid_))) {
        LOG_WARN("failed to init sql end callback", K(ret), K(conn_valid_));
      } else if (OB_FAIL(drv.response_result(result))) {
        LOG_WARN("fail response async result", K(ret));
      }
      async_resp_used = result.is_async_end_trans_submitted();
    } else {
      ObSyncPlanDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      ret = drv.response_result(result);
    }
  } else {
    if (need_trans_cb) {
      ObSqlEndTransCb& sql_end_cb = session.get_mysql_end_trans_cb();
      ObAsyncCmdDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      if (OB_FAIL(sql_end_cb.init(const_cast<rpc::ObRequest*>(req_), &session, seq_, conn_valid_))) {
        LOG_WARN("failed to init sql end callback", K(ret), K(conn_valid_));
      } else if (OB_FAIL(drv.response_result(result))) {
        LOG_WARN("fail response async result", K(ret));
      } else {
        LOG_DEBUG("use async cmd driver success!", K(result.get_stmt_type()), K(session.get_local_autocommit()));
      }
      async_resp_used = result.is_async_end_trans_submitted();
    } else {
      ObSyncCmdDriver drv(gctx_, ctx_, session, retry_ctrl_, *this);
      if (OB_FAIL(drv.response_result(result))) {
        LOG_WARN("failed response sync result", K(ret));
      } else {
        LOG_DEBUG("use sync cmd driver success!", K(result.get_stmt_type()), K(session.get_local_autocommit()));
      }
    }
  }
  //  NG_TRACE(exec_end);
  return ret;
}

int ObMPStmtExecute::do_process_single(
    ObSQLSessionInfo& session, bool has_more_result, bool force_sync_resp, bool& async_resp_used)
{
  int ret = OB_SUCCESS;
  ctx_.self_add_plan_ = false;
  do {
    ret = OB_SUCCESS;
    share::schema::ObSchemaGetterGuard schema_guard;
    int64_t tenant_version = 0;
    int64_t sys_version = 0;
    OZ(gctx_.schema_service_->get_tenant_schema_guard(session.get_effective_tenant_id(), schema_guard));
    OZ(schema_guard.get_schema_version(session.get_effective_tenant_id(), tenant_version));
    OZ(schema_guard.get_schema_version(OB_SYS_TENANT_ID, sys_version));
    OX(ctx_.schema_guard_ = &schema_guard);
    OX(retry_ctrl_.set_tenant_local_schema_version(tenant_version));
    OX(retry_ctrl_.set_sys_local_schema_version(sys_version));

    if (OB_SUCC(ret)) {
      retry_ctrl_.clear_state_before_each_retry(session.get_retry_info_for_update());
      ret = do_process(session, has_more_result, force_sync_resp, async_resp_used);
      session.set_session_in_retry(retry_ctrl_.need_retry());
    }
    if (RETRY_TYPE_LOCAL == retry_ctrl_.get_retry_type()) {
      THIS_WORKER.reset_retry_flag();
    }
  } while (RETRY_TYPE_LOCAL == retry_ctrl_.get_retry_type());

  if (OB_SUCC(ret) && retry_ctrl_.get_retry_times() > 0) {
    LOG_TRACE("sql retry succeed", K(ret), "retry_times", retry_ctrl_.get_retry_times(), "sql", ctx_.cur_sql_);
  }
  return ret;
}

int ObMPStmtExecute::process_execute_stmt(const ObMultiStmtItem& multi_stmt_item, ObSQLSessionInfo& session,
    bool has_more_result, bool force_sync_resp, bool& async_resp_used)
{
  int ret = OB_SUCCESS;

  bool need_response_error = true;
  bool use_sess_trace = false;

  setup_wb(session);
  const bool enable_trace_log = lib::is_trace_log_enabled();

  //==================== Note the life cycle of these variables ========================
  ObVirtualTableIteratorFactory vt_iter_factory(*gctx_.vt_iter_creator_);
  //====================================================================================
  ObSessionStatEstGuard stat_est_guard(get_conn()->tenant_->id(), session.get_sessid());
  if (OB_FAIL(init_process_var(ctx_, multi_stmt_item, session, vt_iter_factory, use_sess_trace))) {
    LOG_WARN("init process var failed.", K(ret), K(multi_stmt_item));
  } else {
    if (enable_trace_log) {
      // set session log_level.Must use ObThreadLogLevelUtils::clear() in pair
      ObThreadLogLevelUtils::init(session.get_log_id_level_map());
    }
    // obproxy may use 'SET @@last_schema_version = xxxx' to set newest schema,
    // observer will force refresh schema if local_schema_version < last_schema_version;
    if (OB_FAIL(check_and_refresh_schema(session.get_login_tenant_id(), session.get_effective_tenant_id()))) {
      LOG_WARN("failed to check_and_refresh_schema", K(ret));
    } else {
      if (is_arraybinding_) {
        need_response_error = false;
        ObSEArray<ObSavedException, 4> exception_array;
        if (OB_UNLIKELY(arraybinding_size_ <= 0)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("oci arraybinding has no parameters", K(ret), K(arraybinding_size_));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "oci arraybinding has no parameters");
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < arraybinding_size_; ++i) {
          OZ(construct_execute_param_for_arraybinding(i));
          OZ(do_process_single(session, has_more_result, force_sync_resp, async_resp_used));
          if (OB_FAIL(ret)) {
            if (is_save_exception_) {
              ret = save_exception_for_arraybinding(i, ret, exception_array);
            }
            if (OB_FAIL(ret)) {
              need_response_error = true;
              break;
            }
          }
        }
        reset_collection_param_for_arraybinding();
        OZ(response_result_for_arraybinding(session, exception_array));
      } else {
        need_response_error = false;
        OZ(do_process_single(session, has_more_result, force_sync_resp, async_resp_used));
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
  }

  do_after_process(session, use_sess_trace, ctx_, async_resp_used);

  if (OB_FAIL(ret) && need_response_error && conn_valid_) {
    send_error_packet(ret, NULL);
  }

  return ret;
}

int ObMPStmtExecute::process()
{
  int ret = OB_SUCCESS;
  int flush_ret = OB_SUCCESS;
  ObSQLSessionInfo* sess = NULL;
  bool need_response_error = true;
  bool async_resp_used = false;
  int64_t query_timeout = 0;

  ObCurTraceId::TraceId* cur_trace_id = ObCurTraceId::get_trace_id();
  ObSMConnection* conn = get_conn();
  if (OB_ISNULL(req_) || OB_ISNULL(conn) || OB_ISNULL(cur_trace_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null conn ptr", K_(stmt_id), K_(req), K(cur_trace_id), K(ret));
  } else if (OB_UNLIKELY(!conn->is_in_authed_phase())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("receive sql without session", K_(stmt_id), K(ret));
  } else if (OB_ISNULL(conn->tenant_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant", K_(stmt_id), K(conn->tenant_), K(ret));
  } else if (OB_FAIL(get_session(sess))) {
    LOG_WARN("get session fail", K_(stmt_id), K(ret));
  } else if (OB_ISNULL(sess)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL or invalid", K_(stmt_id), K(sess), K(ret));
  } else if (OB_FAIL(update_transmission_checksum_flag(*sess))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret));
  } else {
    ObSQLSessionInfo& session = *sess;
    int64_t tenant_version = 0;
    int64_t sys_version = 0;
    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    session.set_use_static_typing_engine(false);
    session.set_current_trace_id(ObCurTraceId::get_trace_id());
    session.set_thread_id(GETTID());
    const ObMySQLRawPacket& pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    int64_t packet_len = pkt.get_clen();
    if (OB_UNLIKELY(!session.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid session", K_(stmt_id), K(ret));
    } else if (OB_UNLIKELY(session.is_zombie())) {
      // session has been killed some moment ago
      ret = OB_ERR_SESSION_INTERRUPTED;
      LOG_WARN("session has been killed",
          K(session.get_session_state()),
          K_(stmt_id),
          K(session.get_sessid()),
          "proxy_sessid",
          session.get_proxy_sessid(),
          K(ret));
    } else if (OB_UNLIKELY(packet_len > session.get_max_packet_size())) {
      // packet size check with session variable max_allowd_packet or net_buffer_length
      ret = OB_ERR_NET_PACKET_TOO_LARGE;
      LOG_WARN("packet too large than allowed for the session", K_(stmt_id), K(ret));
    } else if (OB_FAIL(session.check_and_init_retry_info(*cur_trace_id, ctx_.cur_sql_))) {
      LOG_WARN("fail to check and init retry info", K(ret), K(*cur_trace_id), K(ctx_.cur_sql_));
    } else if (OB_FAIL(session.get_query_timeout(query_timeout))) {
      LOG_WARN("fail to get query timeout", K(ret));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                   session.get_effective_tenant_id(), tenant_version))) {
      LOG_WARN("fail get tenant broadcast version", K(ret));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(OB_SYS_TENANT_ID, sys_version))) {
      LOG_WARN("fail get tenant broadcast version", K(ret));
    } else if (pkt.exist_trace_info() &&
               OB_FAIL(session.update_sys_variable(SYS_VAR_OB_TRACE_INFO, pkt.get_trace_info()))) {
      LOG_WARN("fail to update trace info", K(ret));
    } else {
      THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout);
      retry_ctrl_.set_tenant_global_schema_version(tenant_version);
      retry_ctrl_.set_sys_global_schema_version(sys_version);
      session.partition_hit().reset();
      session.set_pl_can_retry(true);
      ObLockWaitNode& lock_wait_node = req_->get_lock_wait_node();
      lock_wait_node.set_session_info(session.get_sessid(), session.get_version());

      need_response_error = false;

      ret = process_execute_stmt(ObMultiStmtItem(false, 0, ObString()),
          session,
          false,  // has_mode
          false,  // force_sync_resp
          async_resp_used);

      if (OB_FAIL(ret)) {
        if (OB_EAGAIN == ret) {
          // large query, do nothing
        } else if (conn_valid_) {  // The memory of sql string is invalid if conn_valid_ has been set false.
          LOG_WARN("fail execute sql", "sql_id", ctx_.sql_id_, K_(stmt_id), K(ret));
        } else {
          LOG_WARN("fail execute sql", K(ret));
        }
      }
    }
    session.check_and_reset_retry_info(*cur_trace_id, THIS_WORKER.need_retry());
  }
  // whether the previous error was reported, a cleanup is to be done here
  if (NULL != sess) {
    ObPieceCache *piece_cache = static_cast<ObPieceCache *>(sess->get_piece_cache());
    if (OB_ISNULL(piece_cache)) {
      // do nothing
      // piece_cache not be null in piece data protocol
    } else {
      for (uint64_t i = 0; OB_SUCC(ret) && i < params_num_; i++) {
        if (OB_FAIL(piece_cache->remove_piece(piece_cache->get_piece_key(stmt_id_, i), *sess))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("remove piece fail", K(stmt_id_), K(i), K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) && need_response_error && conn_valid_) {
    send_error_packet(ret, NULL);
    disconnect();
    LOG_WARN("disconnect connection when process query", K(ret));
  }

  if (!THIS_WORKER.need_retry()) {
    if (async_resp_used) {
      req_has_wokenup_ = true;
    } else {
      flush_ret = flush_buffer(true);
    }
  }

  if (sess != NULL) {
    revert_session(sess);  // current ignore revert session ret
  }

  return (OB_SUCCESS != ret) ? ret : flush_ret;
}

int ObMPStmtExecute::parse_basic_param_value(ObIAllocator& allocator, const uint32_t type, const ObCharsetType charset,
    const ObCollationType cs_type, const ObCollationType ncs_type, const char*& data,
    const common::ObTimeZoneInfo* tz_info, ObObj& param)
{
  int ret = OB_SUCCESS;
  UNUSED(charset);
  switch (type) {
    case MYSQL_TYPE_TINY: {
      int8_t value;
      ObMySQLUtil::get_int1(data, value);
      param.set_tinyint(value);
      break;
    }
    case MYSQL_TYPE_SHORT: {
      int16_t value = 0;
      ObMySQLUtil::get_int2(data, value);
      param.set_int(value);
      break;
    }
    case MYSQL_TYPE_LONG: {
      int32_t value = 0;
      ObMySQLUtil::get_int4(data, value);
      param.set_int(value);
      break;
    }
    case MYSQL_TYPE_LONGLONG: {
      int64_t value = 0;
      ObMySQLUtil::get_int8(data, value);
      param.set_int(value);
      break;
    }
    case MYSQL_TYPE_FLOAT: {
      float value = 0;
      MEMCPY(&value, data, sizeof(value));
      data += sizeof(value);
      param.set_float(value);
      break;
    }
    case MYSQL_TYPE_DOUBLE: {
      double value = 0;
      MEMCPY(&value, data, sizeof(value));
      data += sizeof(value);
      param.set_double(value);
      break;
    }
    case MYSQL_TYPE_YEAR: {
      int16_t value = 0;
      ObMySQLUtil::get_int2(data, value);
      param.set_year(static_cast<uint8_t>(value));
      break;
    }
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP: {
      if (OB_FAIL(parse_mysql_timestamp_value(static_cast<EMySQLFieldType>(type), data, param, tz_info))) {
        LOG_WARN("parse timestamp value from client failed", K(ret));
      }
      break;
    }
    case MYSQL_TYPE_TIME: {
      if (OB_FAIL(parse_mysql_time_value(data, param))) {
        LOG_WARN("parse timestamp value from client failed", K(ret));
      }
      break;
    }
    case MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
    case MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case MYSQL_TYPE_OB_TIMESTAMP_NANO: {
      ObTimeConvertCtx cvrt_ctx(tz_info, true);
      if (OB_FAIL(parse_oracle_timestamp_value(static_cast<EMySQLFieldType>(type), data, cvrt_ctx, param))) {
        LOG_WARN("parse timestamp value from client failed", K(ret));
      }
      break;
    }
    case MYSQL_TYPE_OB_NVARCHAR2:
    case MYSQL_TYPE_OB_NCHAR:
    case MYSQL_TYPE_OB_RAW:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_OB_NUMBER_FLOAT:
    case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_OB_UROWID:
    case MYSQL_TYPE_ORA_BLOB:
    case MYSQL_TYPE_ORA_CLOB:
    case MYSQL_TYPE_JSON: {
      ObString str;
      ObString dst;
      uint64_t length = 0;
      ObCollationType cur_cs_type = ObCharset::get_default_collation(charset);
      if (OB_FAIL(ObMySQLUtil::get_length(data, length))) {
        LOG_ERROR("decode varchar param value failed", K(ret));
      } else {
        str.assign_ptr(data, static_cast<ObString::obstr_size_t>(length));
      }

      if (OB_FAIL(ret)) {
      } else if (MYSQL_TYPE_OB_NVARCHAR2 == type || MYSQL_TYPE_OB_NCHAR == type) {
        OZ(copy_or_convert_str(allocator, cur_cs_type, ncs_type, str, dst));
        if (OB_SUCC(ret)) {
          MYSQL_TYPE_OB_NVARCHAR2 == type ? param.set_nvarchar2(dst) : param.set_nchar(dst);
          param.set_collation_type(ncs_type);
        }
        LOG_DEBUG("recieve Nchar param", K(ret), K(str), K(dst));
      } else if (ObURowIDType == type) {
        // decode bae64 str and get urowid content
        ObURowIDData urowid_data;
        if (OB_FAIL(ObURowIDData::decode2urowid(str.ptr(), str.length(), allocator, urowid_data))) {
          LOG_WARN("failed to decode to urowid", K(ret));
          if (OB_INVALID_ROWID == ret) {
            LOG_USER_ERROR(OB_INVALID_ROWID);
          }
        } else {
          param.set_urowid(urowid_data);
        }
      } else {
        if (MYSQL_TYPE_STRING == type 
            || MYSQL_TYPE_VARCHAR == type 
            || MYSQL_TYPE_VAR_STRING == type 
            || MYSQL_TYPE_ORA_CLOB == type 
            || MYSQL_TYPE_JSON == type ) {
          const int64_t extra_len = MYSQL_TYPE_ORA_CLOB == type
                                        ? str.length() - reinterpret_cast<const ObLobLocator*>(str.ptr())->payload_size_
                                        : 0;
          OZ(copy_or_convert_str(allocator,
              cur_cs_type,
              cs_type,
              ObString(str.length() - extra_len, str.ptr() + extra_len),
              dst,
              extra_len));
          if (OB_SUCC(ret) && MYSQL_TYPE_ORA_CLOB == type) {
            // copy lob header
            dst.assign_ptr(dst.ptr() - extra_len, dst.length() + extra_len);
            MEMCPY(dst.ptr(), str.ptr(), extra_len);
            reinterpret_cast<ObLobLocator*>(dst.ptr())->payload_size_ = dst.length() - extra_len;
          }
        } else if (OB_FAIL(ob_write_string(allocator, str, dst))) {
          LOG_WARN("Failed to write str", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (MYSQL_TYPE_NEWDECIMAL == type) {
            number::ObNumber nb;
            if (OB_FAIL(nb.from(str.ptr(), length, allocator))) {
              LOG_WARN("decode varchar param to number failed", K(ret), K(str));
            } else {
              param.set_number(nb);
            }
          } else if (MYSQL_TYPE_OB_NUMBER_FLOAT == type) {
            number::ObNumber nb;
            if (OB_FAIL(nb.from(str.ptr(), length, allocator))) {
              LOG_WARN("decode varchar param to number failed", K(ret), K(str));
            } else {
              param.set_number_float(nb);
            }
          } else if (MYSQL_TYPE_OB_RAW == type) {
            param.set_raw(dst);
          } else if (MYSQL_TYPE_ORA_BLOB == type || MYSQL_TYPE_ORA_CLOB == type) {
            if (MYSQL_TYPE_ORA_BLOB == type) {
              param.set_collation_type(CS_TYPE_BINARY);
            } else {
              param.set_collation_type(cs_type);
            }
            const ObLobLocator& lob = *(reinterpret_cast<const ObLobLocator*>(dst.ptr()));
            if (OB_UNLIKELY(!lob.is_valid() || lob.get_total_size() != length)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("got invalid ps lob param",
                  K(length),
                  K(lob.magic_code_),
                  K(lob.table_id_),
                  K(lob.column_id_),
                  K(lob.payload_offset_),
                  K(lob.payload_size_),
                  K(type),
                  K(cs_type));
            } else {
              LOG_TRACE("get lob locator", K(lob), K(cs_type), K(type));
              param.set_lob_locator(lob);
            }
          } else if (MYSQL_TYPE_TINY_BLOB == type 
                || MYSQL_TYPE_MEDIUM_BLOB == type 
                || MYSQL_TYPE_BLOB == type 
                || MYSQL_TYPE_LONG_BLOB == type
                || MYSQL_TYPE_JSON == type) {
            // in ps protocol:
            //    Oracle mode: client driver will call hextoraw()
            //    MySQL mode: no need to call hextoraw
            // in text protocol:
            //    Oracle mode: server will call hextoraw()
            //    MySQL mode: no need to call hextoraw
            param.set_collation_type(cs_type);
            if (MYSQL_TYPE_TINY_BLOB == type) {
              param.set_lob_value(ObTinyTextType, dst.ptr(), dst.length());
            } else if (MYSQL_TYPE_MEDIUM_BLOB == type) {
              param.set_lob_value(ObMediumTextType, dst.ptr(), dst.length());
            } else if (MYSQL_TYPE_BLOB == type) {
              param.set_lob_value(ObTextType, dst.ptr(), dst.length());
            } else if (MYSQL_TYPE_LONG_BLOB == type) {
              param.set_lob_value(ObLongTextType, dst.ptr(), dst.length());
            } else if (MYSQL_TYPE_JSON == type) {
              param.set_json_value(ObJsonType, dst.ptr(), dst.length());
            }
          } else {
            param.set_collation_type(cs_type);
            if (lib::is_oracle_mode()) {
              param.set_char(dst);
            } else {
              param.set_varchar(dst);
            }
          }
        }
      }
      data += length;
      break;
    }
    case MYSQL_TYPE_OB_INTERVAL_YM: {
      if (OB_FAIL(parse_oracle_interval_ym_value(data, param))) {
        LOG_WARN("failed to parse oracle interval year to month value", K(ret));
      }
      break;
    }
    case MYSQL_TYPE_OB_INTERVAL_DS: {
      if (OB_FAIL(parse_oracle_interval_ds_value(data, param))) {
        LOG_WARN("failed to parse oracle interval year to month value", K(ret));
      }
      break;
    }
    default: {
      LOG_USER_ERROR(OB_ERR_ILLEGAL_TYPE, type);
      ret = OB_ERR_ILLEGAL_TYPE;
      break;
    }
  }
  if (OB_SUCC(ret) && share::is_mysql_mode()) {
    param.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObMPStmtExecute::parse_param_value(ObIAllocator& allocator, const uint32_t type, const ObCharsetType charset,
    const ObCollationType cs_type, const ObCollationType ncs_type, const char*& data,
    const common::ObTimeZoneInfo* tz_info, TypeInfo* type_info, TypeInfo* dst_type_info, ObObjParam& param, int16_t param_id)
{
  int ret = OB_SUCCESS;
  uint64_t length = 0;
  common::ObFixedArray<ObSqlString, ObIAllocator>
                str_buf(THIS_WORKER.get_sql_arena_allocator());
  ObPieceCache *piece_cache = NULL == ctx_.session_info_
                                ? NULL
                                : static_cast<ObPieceCache*>(ctx_.session_info_->get_piece_cache());
  ObPiece *piece = NULL;
  if (OB_UNLIKELY(MYSQL_TYPE_COMPLEX == type)) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_UNLIKELY(MYSQL_TYPE_CURSOR == type)) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_NOT_NULL(piece_cache) && OB_FAIL(piece_cache->get_piece(stmt_id_, param_id, piece))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get piece fail.", K(ret));
  } else if (OB_ISNULL(piece_cache) || OB_ISNULL(piece)) {
    // not (send long data) column
    if (OB_FAIL(parse_basic_param_value(allocator, type, charset, cs_type, ncs_type, data, tz_info, param))) {
      LOG_WARN("failed to parse basic param value", K(ret), K(type_info), K(dst_type_info));
    } else {
      param.set_param_meta();
    }
  } else if (!support_send_long_data(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this type is not support send long data.", K(type), K(ret));
  } else if (NULL == piece->get_allocator()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece allocator is null.", K(stmt_id_), K(param_id), K(ret));
  } else {
    ObSqlString str_buf;
    if (OB_FAIL(piece_cache->get_buffer(stmt_id_,
                                        param_id,
                                        length,
                                        str_buf))) {
      LOG_WARN("piece get buffer fail.", K(ret), K(stmt_id_), K(param_id));
    } else {
      char *tmp = static_cast<char*>(piece->get_allocator()->alloc(length));
      int64_t pos = 0;
      MEMSET(tmp, 0, length);
      if (OB_FAIL(ObMySQLUtil::store_obstr(tmp, length, str_buf.string(), pos))) {
        LOG_WARN("store string fail.", K(ret), K(stmt_id_), K(param_id));
      } else {
        const char* src = tmp;
        if (OB_FAIL(parse_basic_param_value(allocator, type, charset, cs_type, ncs_type,
                                            src, tz_info, param))) {
          LOG_WARN("failed to parse basic param value", K(ret));
        } else {
          param.set_param_meta();
        }
      }
      piece->get_allocator()->free(tmp);
    }
  }
  return ret;
}

int ObMPStmtExecute::copy_or_convert_str(common::ObIAllocator& allocator, const ObCollationType src_type,
    const ObCollationType dst_type, const ObString& src, ObString& out, int64_t extra_buf_len /* = 0 */)
{
  int ret = OB_SUCCESS;
  if (!ObCharset::is_valid_collation(src_type) || !ObCharset::is_valid_collation(dst_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid collation", K(ret), K(dst_type));
  } else if (0 == src.length() ||
             ObCharset::charset_type_by_coll(src_type) == ObCharset::charset_type_by_coll(dst_type)) {
    int64_t len = src.length() + extra_buf_len;
    if (len > 0) {
      char* buf = static_cast<char*>(allocator.alloc(len));
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate failed", K(ret), K(len));
      } else {
        if (src.length() > 0) {
          MEMCPY(buf + extra_buf_len, src.ptr(), src.length());
        }
        out.assign_ptr(buf + extra_buf_len, src.length());
      }
    } else {
      out.reset();
    }
  } else {
    int64_t maxmb_len = 0;
    OZ(ObCharset::get_mbmaxlen_by_coll(dst_type, maxmb_len));
    const int64_t len = maxmb_len * src.length() + 1 + extra_buf_len;
    uint32_t res_len = 0;
    if (OB_SUCC(ret)) {
      char* buf = static_cast<char*>(allocator.alloc(len));
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate failed", K(ret), K(len));
      } else if (OB_FAIL(ObCharset::charset_convert(
                     src_type, src.ptr(), src.length(), dst_type, buf + extra_buf_len, len - extra_buf_len, res_len))) {
        LOG_WARN("charset convert failed", K(ret), K(src_type), K(dst_type), K(src));
      } else {
        out.assign_ptr(buf + extra_buf_len, res_len);
      }
    }
  }
  return ret;
}

int ObMPStmtExecute::parse_mysql_timestamp_value(
    const EMySQLFieldType field_type, const char *&data, ObObj &param, const common::ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  int8_t length = 0;
  int16_t year = 0;
  int8_t month = 0;
  int8_t day = 0;
  int8_t hour = 0;
  int8_t min = 0;
  int8_t second = 0;
  int32_t microsecond = 0;
  ObMySQLUtil::get_int1(data, length);
  ObPreciseDateTime value;
  if (0 == length) {
    value = 0;
  } else if (4 == length) {
    ObMySQLUtil::get_int2(data, year);
    ObMySQLUtil::get_int1(data, month);
    ObMySQLUtil::get_int1(data, day);
  } else if (7 == length) {
    ObMySQLUtil::get_int2(data, year);
    ObMySQLUtil::get_int1(data, month);
    ObMySQLUtil::get_int1(data, day);
    ObMySQLUtil::get_int1(data, hour);
    ObMySQLUtil::get_int1(data, min);
    ObMySQLUtil::get_int1(data, second);
  } else if (11 == length) {
    ObMySQLUtil::get_int2(data, year);
    ObMySQLUtil::get_int1(data, month);
    ObMySQLUtil::get_int1(data, day);
    ObMySQLUtil::get_int1(data, hour);
    ObMySQLUtil::get_int1(data, min);
    ObMySQLUtil::get_int1(data, second);
    ObMySQLUtil::get_int4(data, microsecond);
  } else {
    ret = OB_ERROR;
    LOG_WARN("invalid mysql timestamp value length", K(length));
  }

  if (OB_SUCC(ret)) {
    ObTime ob_time;
    if (0 != length) {
      if (share::is_oracle_mode()) {
        // oracle mode datetime should not has microsecond
        microsecond = 0;
      }
      ob_time.parts_[DT_YEAR] = year;
      ob_time.parts_[DT_MON] = month;
      ob_time.parts_[DT_MDAY] = day;
      ob_time.parts_[DT_HOUR] = hour;
      ob_time.parts_[DT_MIN] = min;
      ob_time.parts_[DT_SEC] = second;
      ob_time.parts_[DT_USEC] = microsecond;
      if (!ObTimeUtility2::is_valid_date(year, month, day) ||
          !ObTimeUtility2::is_valid_time(hour, min, second, microsecond)) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("invalid date format", K(ret));
      } else {
        ObTimeConvertCtx cvrt_ctx(NULL, false);
        ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
        if (field_type == MYSQL_TYPE_DATE) {
          value = ob_time.parts_[DT_DATE];
        } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, value))) {
          LOG_WARN(
              "convert obtime to datetime failed", K(value), K(year), K(month), K(day), K(hour), K(min), K(second));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (field_type == MYSQL_TYPE_TIMESTAMP) {
      int64_t ts_value = 0;
      if (OB_FAIL(ObTimeConverter::datetime_to_timestamp(value, tz_info, ts_value))) {
        LOG_WARN("datetime to timestamp failed", K(ret));
      } else {
        param.set_timestamp(ts_value);
      }
    } else if (field_type == MYSQL_TYPE_DATETIME) {
      param.set_datetime(value);
    } else if (field_type == MYSQL_TYPE_DATE) {
      param.set_date(static_cast<int32_t>(value));
    }
  }
  LOG_DEBUG("get datetime", K(length), K(year), K(month), K(day), K(hour), K(min), K(second), K(microsecond), K(value));
  return ret;
}

int ObMPStmtExecute::parse_oracle_timestamp_value(
    const obmysql::EMySQLFieldType field_type, const char*& data, const ObTimeConvertCtx& cvrt_ctx, ObObj& param)
{
  int ret = OB_SUCCESS;
  int8_t total_len = 0;
  ObObjType obj_type;
  ObOTimestampData ot_data;
  int8_t scale = -1;
  ObMySQLUtil::get_int1(data, total_len);
  if (OB_FAIL(ObSMUtils::get_ob_type(obj_type, field_type))) {
    LOG_WARN("failed to get_ob_type", K(ret));
  } else if (OB_FAIL(ObTimeConverter::decode_otimestamp(obj_type, data, total_len, cvrt_ctx, ot_data, scale))) {
    LOG_WARN("failed to decode_otimestamp", K(ret));
  } else {
    data += total_len;
    param.set_otimestamp_value(obj_type, ot_data);
    param.set_scale(scale);
  }
  return ret;
}

int ObMPStmtExecute::parse_mysql_time_value(const char*& data, ObObj& param)
{
  int ret = OB_SUCCESS;
  int8_t length = 0;
  int8_t is_negative = 0;
  int16_t year = 0;
  int8_t month = 0;
  int32_t day = 0;
  int8_t hour = 0;
  int8_t min = 0;
  int8_t second = 0;
  int32_t microsecond = 0;
  struct tm tmval;
  MEMSET(&tmval, 0, sizeof(tmval));
  ObMySQLUtil::get_int1(data, length);
  int64_t value;
  if (0 == length) {
    value = 0;
  } else if (8 == length) {
    ObMySQLUtil::get_int1(data, is_negative);
    ObMySQLUtil::get_int4(data, day);
    ObMySQLUtil::get_int1(data, hour);
    ObMySQLUtil::get_int1(data, min);
    ObMySQLUtil::get_int1(data, second);
  } else if (12 == length) {
    ObMySQLUtil::get_int1(data, is_negative);
    ObMySQLUtil::get_int4(data, day);
    ObMySQLUtil::get_int1(data, hour);
    ObMySQLUtil::get_int1(data, min);
    ObMySQLUtil::get_int1(data, second);
    ObMySQLUtil::get_int4(data, microsecond);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected time length", K(length), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObTime ob_time;
    if (0 != length) {
      ob_time.parts_[DT_YEAR] = year;
      ob_time.parts_[DT_MON] = month;
      ob_time.parts_[DT_MDAY] = day;
      ob_time.parts_[DT_HOUR] = hour;
      ob_time.parts_[DT_MIN] = min;
      ob_time.parts_[DT_SEC] = second;
      ob_time.parts_[DT_USEC] = microsecond;
      if (!ObTimeUtility2::is_valid_time(hour, min, second, microsecond)) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("invalid date format", K(ret));
      } else {
        ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
        value = ObTimeConverter::ob_time_to_time(ob_time);
      }
    }
  }
  if (OB_SUCC(ret)) {
    param.set_time(value);
  }
  LOG_INFO("get time", K(length), K(year), K(month), K(day), K(hour), K(min), K(second), K(microsecond), K(value));
  return ret;
}

int ObMPStmtExecute::parse_oracle_interval_ds_value(const char*& data, ObObj& param)
{
  int ret = OB_SUCCESS;
  int8_t length = 0;
  ObScale scale = 0;
  ObIntervalDSValue value;

  ObMySQLUtil::get_int1(data, length);

  if (OB_FAIL(ObTimeConverter::decode_interval_ds(data, length, value, scale))) {
    LOG_WARN("fail to decode interval day to second", K(ret), K(length));
  } else {
    param.set_interval_ds(value);
    param.set_scale(scale);
  }

  return ret;
}

int ObMPStmtExecute::parse_oracle_interval_ym_value(const char*& data, ObObj& param)
{
  int ret = OB_SUCCESS;
  int8_t length = 0;
  ObScale scale = 0;
  ObIntervalYMValue value;

  ObMySQLUtil::get_int1(data, length);
  if (OB_FAIL(ObTimeConverter::decode_interval_ym(data, length, value, scale))) {
    LOG_WARN("fail to decode interval year to month", K(ret), K(length));
  } else {
    param.set_interval_ym(value);
    param.set_scale(scale);
  }

  return ret;
}

void ObMPStmtExecute::record_stat(const stmt::StmtType type, const int64_t end_time) const
{
#define ADD_STMT_STAT(type)                  \
  case stmt::T_##type:                       \
    EVENT_INC(SQL_##type##_COUNT);           \
    EVENT_ADD(SQL_##type##_TIME, time_cost); \
    break
  if (lib::is_diagnose_info_enabled()) {
    const int64_t time_cost = end_time - get_receive_timestamp();
    if (!THIS_THWORKER.need_retry()) {
      EVENT_INC(SQL_PS_EXECUTE_COUNT);
      switch (type) {
        ADD_STMT_STAT(SELECT);
        ADD_STMT_STAT(INSERT);
        ADD_STMT_STAT(REPLACE);
        ADD_STMT_STAT(UPDATE);
        ADD_STMT_STAT(DELETE);
        default: {
          EVENT_INC(SQL_OTHER_COUNT);
          EVENT_ADD(SQL_OTHER_TIME, time_cost);
        }
      }
    }
  }
#undef ADD_STMT_STAT
}

}  // end of namespace observer
}  // end of namespace oceanbase
