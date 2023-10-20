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
#include "rpc/obmysql/obsm_struct.h"
#include "observer/mysql/obsm_row.h"
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
#include "observer/ob_req_time_service.h"
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_exception_handling.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "observer/mysql/obmp_stmt_prexecute.h"
#include "observer/mysql/obmp_stmt_send_piece_data.h"
#include "observer/mysql/obmp_utils.h"
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
inline int ObPSAnalysisChecker::detection(const int64_t len)
{
  int ret = OB_SUCCESS;
  if (!need_check_) {
  } else if (*pos_ + len > end_pos_) {
    ret = OB_ERR_MALFORMED_PS_PACKET;
    LOG_USER_ERROR(OB_ERR_MALFORMED_PS_PACKET);
    LOG_ERROR("malformed ps data packet, please check the number and content of data packet parameters", K(ret), KP(pos_), KP(begin_pos_),
    K(end_pos_ - begin_pos_), K(len), K(data_len_), K(remain_len()));
  }
  return ret;
}

ObMPStmtExecute::ObMPStmtExecute(const ObGlobalContext &gctx)
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
      ps_cursor_type_(ObNormalType),
      single_process_timestamp_(0),
      exec_start_timestamp_(0),
      exec_end_timestamp_(0),
      prepare_packet_sent_(false),
      params_num_(0),
      params_value_len_(0),
      params_value_(NULL),
      curr_sql_idx_(0)
{
  ctx_.exec_type_ = MpQuery;
}

int ObMPStmtExecute::init_arraybinding_field(int64_t column_field_cnt,
                                             const ColumnsFieldIArray *column_fields)
{
  int ret = OB_SUCCESS;

  ObField sql_no_field, err_no_field, err_msg_field;

  OX (sql_no_field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI);
  OX (sql_no_field.type_.set_type(ObIntType));
  OZ (common::ObField::get_field_mb_length(sql_no_field.type_.get_type(),
                                           sql_no_field.accuracy_,
                                           common::CS_TYPE_INVALID,
                                           sql_no_field.length_));
  OX (sql_no_field.cname_ = ObString("sql_no"));

  OX (err_no_field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI);
  OX (err_no_field.type_.set_type(ObIntType));
  OZ (common::ObField::get_field_mb_length(err_no_field.type_.get_type(),
                                           err_no_field.accuracy_,
                                           common::CS_TYPE_INVALID, err_no_field.length_));
  OX (err_no_field.cname_ = ObString("error_code"));

  OX (err_msg_field.charsetnr_ = CS_TYPE_UTF8MB4_GENERAL_CI);
  OX (err_msg_field.type_.set_type(ObVarcharType));
  OZ (common::ObField::get_field_mb_length(err_msg_field.type_.get_type(),
                                           err_msg_field.accuracy_,
                                           common::CS_TYPE_INVALID,
                                           err_msg_field.length_));
  OX (err_msg_field.cname_ = ObString("error_message"));

  OZ (arraybinding_columns_->push_back(sql_no_field));
  OZ (arraybinding_columns_->push_back(err_no_field));
  if (is_prexecute() && column_field_cnt > 3 && OB_NOT_NULL(column_fields)) {
    // only for pre_execute
    for (int64_t i = 0; OB_SUCC(ret) && i < column_fields->count(); i++) {
      if (OB_FAIL(arraybinding_columns_->push_back(column_fields->at(i)))) {
        LOG_WARN("fail to push arraybinding_columns_", "field", column_fields->at(i), K(i));
      }
    }
  }
  OZ (arraybinding_columns_->push_back(err_msg_field));

  return ret;
}

int ObMPStmtExecute::init_row_for_arraybinding(ObIAllocator &alloc, int64_t array_binding_row_num)
{
  int ret = OB_SUCCESS;
  ObObj* obj = static_cast<ObObj*>(alloc.alloc(sizeof(ObObj) * array_binding_row_num));
  if (OB_ISNULL(obj)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for row", K(ret));
  } else {
    ObObj *ptr = obj;
    for (int64_t i = 0; i < array_binding_row_num; ++i) {
      ptr = new(ptr)ObObj();
      ptr++;
    }
    arraybinding_row_->assign(obj, array_binding_row_num);
  }
  return ret;
}

int ObMPStmtExecute::init_arraybinding_paramstore(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arraybinding_params_
      = static_cast<ParamStore*>(alloc.alloc(sizeof(ParamStore))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  }
  OX (arraybinding_params_ = new(arraybinding_params_)ParamStore((ObWrapperAllocator(alloc))));
  return ret;
}


// only used for pre_execute
int ObMPStmtExecute::init_arraybinding_fields_and_row(ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  int64_t returning_field_num = 0;

  if (!is_prexecute()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support execute protocol", K(ret));
  } else if (OB_ISNULL(result.get_field_columns())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support execute protocol", K(ret));
  } else {
    ObIAllocator *alloc = static_cast<ObMPStmtPrexecute*>(this)->get_alloc();
    if (OB_ISNULL(alloc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else if (OB_ISNULL(result.get_field_columns())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("returning param field is null", K(ret));
    } else {
      returning_field_num = result.get_field_columns()->count();
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(arraybinding_columns_
        = static_cast<ColumnsFieldArray*>(alloc->alloc(sizeof(ColumnsFieldArray))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_ISNULL(arraybinding_row_
        = static_cast<ObNewRow*>(alloc->alloc(sizeof(ObNewRow))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      arraybinding_columns_
        = new(arraybinding_columns_)ColumnsFieldArray(*alloc, 3 + returning_field_num);
      arraybinding_row_ = new(arraybinding_row_)ObNewRow();
    }
    OZ (init_arraybinding_field(returning_field_num + 3, result.get_field_columns()));
    OZ (init_row_for_arraybinding(*alloc, returning_field_num + 3));
  }

  return ret;
}

int ObMPStmtExecute::init_for_arraybinding(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arraybinding_params_
      = static_cast<ParamStore*>(alloc.alloc(sizeof(ParamStore))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (is_save_exception_) {
    if (OB_ISNULL(arraybinding_columns_
        = static_cast<ColumnsFieldArray*>(alloc.alloc(sizeof(ColumnsFieldArray))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_ISNULL(arraybinding_row_
        = static_cast<ObNewRow*>(alloc.alloc(sizeof(ObNewRow))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      arraybinding_columns_
        = new(arraybinding_columns_)ColumnsFieldArray(alloc, 3);
      arraybinding_row_ = new(arraybinding_row_)ObNewRow();
    }
    OZ (init_arraybinding_field(3, NULL));
    OZ (init_row_for_arraybinding(alloc, 3));
  }
  OX (arraybinding_params_ = new(arraybinding_params_)ParamStore((ObWrapperAllocator(alloc))));
  return ret;
}

int ObMPStmtExecute::check_param_type_for_arraybinding(
    ObSQLSessionInfo *session_info,
    ParamTypeInfoArray &param_type_infos)
{
  int ret = OB_SUCCESS;
  if (!ObStmt::is_dml_write_stmt(stmt_type_)
      && stmt::T_ANONYMOUS_BLOCK != stmt_type_
      && stmt::T_CALL_PROCEDURE != stmt_type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("arraybinding only support write dml", K(ret), K(stmt_type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "arraybinding got no write dml");
  } else if (session_info->get_local_autocommit()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("arraybinding must in autocommit off", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "arraybinding has autocommit = on");
  } else if (OB_UNLIKELY(param_type_infos.count() <= 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("arraybinding must has parameters", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "arraybinding has no parameter");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_type_infos.count(); ++i) {
      TypeInfo &type_info = param_type_infos.at(i);
      if (type_info.is_basic_type_ || !type_info.is_elem_type_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("arraybinding parameter must be anonymous array", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                       "arraybinding parameter is not anonymous array");
      }
    }
  }
  return ret;
}

int ObMPStmtExecute::check_param_value_for_arraybinding(ObObjParam &param)
{
  int ret = OB_SUCCESS;
  ObPLCollection *coll = NULL;
  CK (param.is_ext());
  CK (OB_NOT_NULL(coll = reinterpret_cast<ObPLCollection*>(param.get_ext())));
  if (OB_FAIL(ret)) {
  } else if (0 == arraybinding_size_) {
    arraybinding_size_ = coll->get_count();
  } else {
    CK (arraybinding_size_ == coll->get_count());
  }
  return ret;
}

int ObMPStmtExecute::construct_execute_param_for_arraybinding(int64_t pos)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(arraybinding_params_));
  CK (OB_NOT_NULL(params_));
  CK (arraybinding_params_->count() == params_->count());
  for (int64_t i = 0; OB_SUCC(ret) && i < arraybinding_params_->count(); ++i) {
    ObObjParam &obj = arraybinding_params_->at(i);
    ObPLCollection *coll = NULL;
    ObObj *data = NULL;
    CK (obj.is_ext());
    CK (OB_NOT_NULL(coll = reinterpret_cast<ObPLCollection*>(obj.get_ext())));
    CK (coll->get_count() > pos);
    CK (1 == coll->get_column_count());
    CK (OB_NOT_NULL(data = reinterpret_cast<ObObj*>(coll->get_data())));
    OX (params_->at(i) = *(data + pos));
  }
  return ret;
}

void ObMPStmtExecute::reset_complex_param_memory(ParamStore *params, ObSQLSessionInfo &session_info)
{
  if (OB_NOT_NULL(params)) {
    for (int64_t i = 0; i < params->count(); ++i) {
      ObObjParam &obj = params->at(i);
      if (obj.is_pl_extend()) {
        int ret = ObUserDefinedType::destruct_obj(obj, &session_info);
        if (OB_SUCCESS != ret) {
          LOG_WARN("fail to destruct obj", K(ret), K(i));
        }
      }
    }
  }
}

int ObMPStmtExecute::send_eof_packet_for_arraybinding(ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;

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
    = (session_info.is_server_status_in_transaction() ? 1 : 0);
  flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (session_info.get_local_autocommit() ? 1 : 0);
  // MORE_RESULTS need false in prexecute protocol.
  // only is_save_exception_ will use this func in prexecute protocol.
  flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = is_prexecute() && is_save_exception_
                                                        ? false : true;
  if (!session_info.is_obproxy_mode()) {
    flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !session_info.partition_hit().get_bool();
  }
  eofp.set_server_status(flags);
  OZ (response_packet(eofp, &session_info));

  return ret;
}

int ObMPStmtExecute::response_result_for_arraybinding(
    ObSQLSessionInfo &session_info,
    ObIArray<ObSavedException> &exception_array)
{
  int ret = OB_SUCCESS;
  if (exception_array.count() > 0) {
    if (is_prexecute()) {
      // do nothing
    } else {
      OMPKResheader rhp;
      rhp.set_field_count(3);
      OZ (response_packet(rhp, &session_info));

      for (int64_t i = 0; OB_SUCC(ret) && i < arraybinding_columns_->count(); ++i) {
        ObMySQLField field;
        OZ (ObMySQLResultSet::to_mysql_field(arraybinding_columns_->at(i), field));
        ObMySQLResultSet::replace_lob_type(session_info, arraybinding_columns_->at(i), field);
        OMPKField fp(field);
        OZ (response_packet(fp, &session_info));
      }

      OZ (send_eof_packet_for_arraybinding(session_info));

      for (int64_t i = 0; OB_SUCC(ret) && i < exception_array.count(); ++i) {
        arraybinding_row_->get_cell(0).set_int(exception_array.at(i).pos_);
        arraybinding_row_->get_cell(1).set_int(exception_array.at(i).error_code_);
        arraybinding_row_->get_cell(2).set_varchar(exception_array.at(i).error_msg_);

        const ObDataTypeCastParams dtc_params
          = ObBasicSessionInfo::create_dtc_params(&session_info);
        ObSMRow sm_row(BINARY,
                *arraybinding_row_,
                dtc_params,
                arraybinding_columns_,
                ctx_.schema_guard_,
                session_info.get_effective_tenant_id());
        OMPKRow rp(sm_row);
        OZ (response_packet(rp, &session_info));
      }
      OZ (send_eof_packet_for_arraybinding(session_info));
    }
  }

  if (OB_SUCC(ret)) {
    bool ps_out = ((stmt::T_ANONYMOUS_BLOCK == stmt_type_ || stmt::T_CALL_PROCEDURE == stmt_type_)
                    && arraybinding_columns_->count() > 3) ? true : false;
    ObOKPParam ok_param;
    ok_param.affected_rows_ = arraybinding_rowcnt_;
    ok_param.is_partition_hit_ = session_info.partition_hit().get_bool();
    ok_param.has_pl_out_ = ps_out;
    OZ (send_ok_packet(session_info, ok_param));
  }
  return ret;
}

int ObMPStmtExecute::save_exception_for_arraybinding(
  int64_t pos, int error_code, ObIArray<ObSavedException> &exception_array)
{
  int ret = OB_SUCCESS;
  ObSavedException exception;

  const char *errm_result = NULL;
  int64_t errm_length = 0;

  exception.pos_ = pos;
  exception.error_code_ = static_cast<uint16_t>(ob_errpkt_errno(error_code, lib::is_oracle_mode()));

  ObIAllocator &alloc = CURRENT_CONTEXT->get_arena_allocator();

  const ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
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

  OZ (ob_write_string(alloc, ObString(errm_length, errm_result), exception.error_msg_));
  OZ (exception_array.push_back(exception));
  return ret;
}

int ObMPStmtExecute::after_do_process_for_arraybinding(ObMySQLResultSet &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result.get_physical_plan())) {
    ret = OB_NOT_INIT;
    LOG_WARN("should have set plan to result set", K(ret));
  } else if (OB_FAIL(result.open())) {
    int cret = OB_SUCCESS;
    int cli_ret = OB_SUCCESS;
    retry_ctrl_.test_and_save_retry_state(gctx_,
                                          ctx_,
                                          result,
                                          ret,
                                          cli_ret,
                                          true/*arraybinding only local retry*/);
    if (OB_TRANSACTION_SET_VIOLATION != ret && OB_REPLICA_NOT_READABLE != ret) {
      if (OB_TRY_LOCK_ROW_CONFLICT == ret && retry_ctrl_.need_retry()) {
        //锁冲突重试不打印日志，避免刷屏
      } else {
        LOG_WARN("result set open failed, check if need retry",
                 K(ret), K(cli_ret), K(retry_ctrl_.need_retry()));
      }
    }
    ret = cli_ret;
    cret = result.close();
    if (cret != OB_SUCCESS &&
        cret != OB_TRANSACTION_SET_VIOLATION &&
        OB_TRY_LOCK_ROW_CONFLICT != cret) {
      LOG_WARN("close result set fail", K(cret));
    }
  } else if (result.is_with_rows()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("in arraybinding, dml with rows is not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "in arraybinding, dml with rows");
  } else {
    OZ (result.close());
    OX (arraybinding_rowcnt_ += result.get_affected_rows());
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
    ObIAllocator &alloc = CURRENT_CONTEXT->get_arena_allocator();
    if (OB_ISNULL(params_ = static_cast<ParamStore *>(alloc.alloc(sizeof(ParamStore))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      params_ = new(params_)ParamStore( (ObWrapperAllocator(alloc)) );
    }
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    const char* pos = pkt.get_cdata();
    analysis_checker_.init(pos, pkt.get_clen());
    int32_t stmt_id = -1; //INVALID_STMT_ID
    uint32_t ps_stmt_checksum = 0;
    ObSQLSessionInfo *session = NULL;
    PS_DEFENSE_CHECK(9) // stmt_id(4) + flag(1) + checksum(4)
    {
      ObMySQLUtil::get_int4(pos, stmt_id);
      stmt_id_ = stmt_id;

      // pos += 1; //skip flags
      int8_t flag = 0;
      ObMySQLUtil::get_int1(pos, flag);
      const uint8_t ARRAYBINDING_MODE = 8;
      const uint8_t SAVE_EXCEPTION_MODE = 16;
      is_arraybinding_ = flag & ARRAYBINDING_MODE;
      is_save_exception_ = flag & SAVE_EXCEPTION_MODE;
      ps_cursor_type_ = 0 != (flag & CURSOR_TYPE_READ_ONLY)
                          ? ObExecutePsCursorType
                          : ObNormalType;

      // 4 bytes, iteration-count, used for checksum
      ObMySQLUtil::get_uint4(pos, ps_stmt_checksum);

      if (is_arraybinding_) {
        OZ (init_for_arraybinding(alloc));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_session(session))) {
      LOG_WARN("get session failed");
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL or invalid", K(ret), K(session));
    } else {
      const bool enable_sql_audit =
      GCONF.enable_sql_audit && session->get_local_ob_enable_sql_audit();
      OZ (request_params(session, pos, ps_stmt_checksum, alloc, -1));
      if (!is_pl_stmt(stmt_type_) && enable_sql_audit) {
        OZ (store_params_value_to_str(alloc, *session));
      }
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
    flush_buffer(true);
  }

  return ret;
}

int ObMPStmtExecute::store_params_value_to_str(ObIAllocator &alloc, sql::ObSQLSessionInfo &session)
{
  return store_params_value_to_str(alloc, session, params_, params_value_, params_value_len_);
}

int ObMPStmtExecute::store_params_value_to_str(ObIAllocator &alloc,
                                               sql::ObSQLSessionInfo &session,
                                               ParamStore *params,
                                               char *&params_value,
                                               int64_t &params_value_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t length = OB_MAX_SQL_LENGTH;
  CK (OB_NOT_NULL(params));
  CK (OB_ISNULL(params_value));
  CK (OB_NOT_NULL(params_value = static_cast<char *>(alloc.alloc(OB_MAX_SQL_LENGTH))));
  for (int i = 0; OB_SUCC(ret) && i < params->count(); ++i) {
    const common::ObObjParam &param = params->at(i);
    if (param.is_ext()) {
      pos = 0;
      params_value = NULL;
      params_value_len = 0;
      break;
    } else {
      OZ (param.print_sql_literal(params_value, length, pos, alloc, TZ_INFO(&session)));
      if (i != params->count() - 1) {
        OZ (databuff_printf(params_value, length, pos, alloc, ","));
      }
    }
  }
  if (OB_FAIL(ret)) {
    params_value = NULL;
    params_value_len = 0;
    // The failure of store_params_value_to_str does not affect the execution of SQL,
    // so the error code is ignored here
    ret = OB_SUCCESS;
  } else {
    params_value_len = pos;
  }
  return ret;
}

int ObMPStmtExecute::parse_request_type(const char* &pos,
                                       int64_t num_of_params,
                                       int8_t new_param_bound_flag,
                                       ObCollationType cs_type,
                                       ObCollationType ncs_type,
                                       ParamTypeArray &param_types,
                                       ParamTypeInfoArray &param_type_infos
                                       /*ParamCastArray param_cast_infos*/)
{
  int ret = OB_SUCCESS;
  // Step3: get type info
  if (param_type_infos.count() < num_of_params) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type array length is not normal", K(ret), K(param_types.count()), K(param_type_infos.count()));
  }
  for (int i = 0; OB_SUCC(ret) && i < num_of_params; ++i) {
    uint8_t type = 0;
    int8_t flag = 0;
    TypeInfo &type_name_info = param_type_infos.at(i);
    if (1 == new_param_bound_flag) {
      PS_DEFENSE_CHECK(2) // type(1) + flag(1)
      {
        ObMySQLUtil::get_uint1(pos, type);
        ObMySQLUtil::get_int1(pos, flag);
        if (OB_FAIL(param_types.push_back(static_cast<EMySQLFieldType>(type)))) {
          LOG_WARN("fail to push back", K(type), K(i));
        } else if (EMySQLFieldType::MYSQL_TYPE_COMPLEX != type) {
          int16_t unsigned_flag = 128;
          ObObjType ob_elem_type;
          if (OB_FAIL(ObSMUtils::get_ob_type(ob_elem_type,
                                    static_cast<EMySQLFieldType>(type),
                                    flag & unsigned_flag ? true : false))) {
            LOG_WARN("get ob type fail. ", K(type));
          } else {
            type_name_info.elem_type_.set_obj_type(ob_elem_type);
          }
        }
      }
    } else {
      if (num_of_params != param_types.count()) {
        ret = OB_ERR_WRONG_DYNAMIC_PARAM;
        LOG_USER_ERROR(OB_ERR_WRONG_DYNAMIC_PARAM,
            param_types.count(), num_of_params);
      } else {
        type = static_cast<uint8_t>(param_types.at(i));
      }
    }

    if (OB_SUCC(ret)) {

      uint8_t elem_type = 0;
      if (EMySQLFieldType::MYSQL_TYPE_COMPLEX == type) {
        type_name_info.is_basic_type_ = false;
        if (OB_FAIL(decode_type_info(pos, type_name_info))) {
          LOG_WARN("failed to decode type info", K(ret));
        } else if (type_name_info.type_name_.empty()) {
          ObObjType ob_elem_type;
          type_name_info.is_elem_type_ = true;
          PS_DEFENSE_CHECK(1) // elem_type(1)
          {
            ObMySQLUtil::get_uint1(pos, elem_type);
          }
          OZ (ObSMUtils::get_ob_type(
            ob_elem_type, static_cast<EMySQLFieldType>(elem_type)), elem_type);
          OX (type_name_info.elem_type_.set_obj_type(ob_elem_type));
          if (OB_SUCC(ret)) {
            switch (elem_type) {
              case MYSQL_TYPE_OB_NVARCHAR2:
              case MYSQL_TYPE_OB_NCHAR: {
                type_name_info.elem_type_.set_collation_type(ncs_type);
              } break;
              case MYSQL_TYPE_ORA_BLOB: {
                type_name_info.elem_type_.set_collation_type(CS_TYPE_BINARY);
              } break;
              default: {
                type_name_info.elem_type_.set_collation_type(cs_type);
              } break;
            }
          }
          if (OB_SUCC(ret) && EMySQLFieldType::MYSQL_TYPE_COMPLEX == elem_type) {
            OZ (decode_type_info(pos, type_name_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObMPStmtExecute::parse_request_param_value(ObIAllocator &alloc,
                                             sql::ObSQLSessionInfo *session,
                                             const char* &pos,
                                             int64_t idx,
                                             EMySQLFieldType &param_type,
                                             TypeInfo &param_type_info,
                                             ObObjParam &param,
                                             const char *bitmap)
{
  int ret = OB_SUCCESS;
  ObCharsetType charset = CHARSET_INVALID;
  ObCharsetType ncharset = CHARSET_INVALID;
  ObCollationType cs_conn = CS_TYPE_INVALID;
  ObCollationType cs_server = CS_TYPE_INVALID;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(session->get_character_set_connection(charset))) {
    LOG_WARN("get charset for client failed", K(ret));
  } else if (OB_FAIL(session->get_collation_connection(cs_conn))) {
    LOG_WARN("get charset for client failed", K(ret));
  } else if (OB_FAIL(session->get_collation_server(cs_server))) {
    LOG_WARN("get charset for client failed", K(ret));
  } else if (OB_FAIL(session->get_ncharacter_set_connection(ncharset))) {
    LOG_WARN("get charset for client failed", K(ret));
  }
  // Step5: decode value
  ObObjType ob_type;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSMUtils::get_ob_type(
        ob_type, static_cast<EMySQLFieldType>(param_type)))) {
    LOG_WARN("cast ob type from mysql type failed",
              K(ob_type), K(param_type), K(ret));
  } else {
    param.set_type(ob_type);
    param.set_param_meta();
    if (OB_FAIL(parse_param_value(alloc,
                                         param_type,
                                         charset,
                                         ncharset,
                                         is_oracle_mode() ? cs_server : cs_conn,
                                         session->get_nls_collation_nation(),
                                         pos,
                                         session->get_timezone_info(),
                                         &param_type_info,
                                         param,
                                         bitmap,
                                         idx))) {
      LOG_WARN("get param value failed", K(param));
    } else {
      LOG_DEBUG("resolve execute with param", K(param));
    }
  }
  return ret;
}

bool ObMPStmtExecute::is_contain_complex_element(const sql::ParamTypeArray &param_types) const
{
  bool b_ret = false;
  for (int64_t i = 0; i < param_types.count(); i++) {
    const obmysql::EMySQLFieldType field_type = param_types.at(i);
    if (MYSQL_TYPE_COMPLEX == field_type) {
      b_ret = true;
      break;
    }
  }
  return b_ret;
}

int ObMPStmtExecute::request_params(ObSQLSessionInfo *session,
                                    const char* &pos,
                                    uint32_t ps_stmt_checksum,
                                    ObIAllocator &alloc,
                                    int32_t all_param_num)
{
  int ret = OB_SUCCESS;
  ObPsSessionInfo *ps_session_info = NULL;
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
    // 新协议不在这里做
  } else if (ps_stmt_checksum != ps_session_info->get_ps_stmt_checksum()) {
    ret = OB_ERR_PREPARE_STMT_CHECKSUM;
    LOG_ERROR("ps stmt checksum fail", K(ret), "session_id", session->get_sessid(),
                                        K(ps_stmt_checksum), K(*ps_session_info));
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("ps session info",
              K(ret), "session_id", session->get_sessid(), K(*ps_session_info));
    share::schema::ObSchemaGetterGuard *old_guard = ctx_.schema_guard_;
    ObSQLSessionInfo *old_sess_info = ctx_.session_info_;
    ctx_.schema_guard_ = &schema_guard;
    ctx_.session_info_ = session;
    const int64_t input_param_num = ps_session_info->get_param_count();
    stmt_type_ = ps_session_info->get_stmt_type();
    int8_t new_param_bound_flag = 0;
    if (is_pl_stmt(stmt_type_)) {
      // pl not support save exception
      is_save_exception_ = 0;
    }
    // for returning into,
    // all_param_num  = input_param_num + returning_param_num
    params_num_ = (all_param_num > input_param_num) ? all_param_num : input_param_num;
    int64_t returning_params_num = all_param_num - input_param_num;
    if (is_prexecute() && 0 != params_num_) {
      if (ps_session_info->get_num_of_returning_into() > 0) {
        // check param_cnt for returning into
        if (returning_params_num != ps_session_info->get_num_of_returning_into()
            || input_param_num != ps_session_info->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param num is not match ps stmt prama count.", K(is_prexecute()), K(params_num_),
                    K(ps_session_info->get_param_count()));
        }
      } else if (params_num_ != ps_session_info->get_param_count()) {
        // check param_cnt
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param num is not match ps stmt prama count.", K(is_prexecute()), K(params_num_),
                 K(ps_session_info->get_param_count()));
      }
    }
    if (OB_SUCC(ret) && params_num_ > 0) {
      // Step1: 处理空值位图
      int64_t bitmap_types = (params_num_ + 7) / 8;
      const char *bitmap = pos;
      pos += bitmap_types;
      ParamTypeArray &param_types = ps_session_info->get_param_types();
      ParamTypeInfoArray param_type_infos;
      ParamCastArray param_cast_infos;

      ParamTypeArray returning_param_types;
      ParamTypeInfoArray returning_param_type_infos;
      int64_t len = bitmap_types + 1/*new_param_bound_flag*/;
      PS_DEFENSE_CHECK(len) // bitmap_types
      {
        // Step2: 获取new_param_bound_flag字段
        ObMySQLUtil::get_int1(pos, new_param_bound_flag);
        if (new_param_bound_flag == 1) {
          param_types.reuse();
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(param_type_infos.prepare_allocate(input_param_num))) {
        LOG_WARN("array prepare allocate failed", K(ret), K(input_param_num));
      } else if (OB_FAIL(params_->prepare_allocate(input_param_num))) {
        LOG_WARN("array prepare allocate failed", K(ret));
      } else if (OB_FAIL(param_cast_infos.prepare_allocate(input_param_num))) {
        LOG_WARN("array prepare allocate failed", K(ret));
      } else if (is_arraybinding_) {
        CK (OB_NOT_NULL(arraybinding_params_));
        OZ (arraybinding_params_->prepare_allocate(input_param_num));
      }

      for (int i = 0; OB_SUCC(ret) && i < input_param_num; ++i) {
        param_cast_infos.at(i) = false;
      }

      if (OB_FAIL(ret)) {

      } else if (params_num_ <= input_param_num) {
        // not need init returning_param_types and returning_param_type_infos
      } else if (OB_FAIL(returning_param_type_infos.prepare_allocate(params_num_ - input_param_num))) {
        LOG_WARN("array prepare allocate failed", K(ret));
      }

      // Step3: 获取type信息
      if (OB_SUCC(ret) && OB_FAIL(parse_request_type(pos,
                                                     input_param_num,
                                                     new_param_bound_flag,
                                                     cs_conn,
                                                     cs_server,
                                                     param_types,
                                                     param_type_infos))) {
        LOG_WARN("fail to parse input params type", K(ret));
      } else if (is_contain_complex_element(param_types)) {
        analysis_checker_.need_check_ = false;
      }

      // Step3-2: 获取returning into params type信息
      if (OB_SUCC(ret) && returning_params_num > 0) {
        if (new_param_bound_flag != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("returning into parm must define type", K(ret));
        } else if (OB_FAIL(parse_request_type(pos,
                                              returning_params_num,
                                              new_param_bound_flag,
                                              cs_conn,
                                              cs_server,
                                              returning_param_types,
                                              returning_param_type_infos))) {
          LOG_WARN("fail to parse returning into params type", K(ret));
        }
      }

      if (OB_SUCC(ret) && is_arraybinding_) {
        OZ (check_param_type_for_arraybinding(session, param_type_infos));
      }
      if (OB_SUCC(ret) && stmt::T_CALL_PROCEDURE == ps_session_info->get_stmt_type()) {
        ctx_.is_execute_call_stmt_ = true;
      }

      // Step5: decode value
      for (int64_t i = 0; OB_SUCC(ret) && i < input_param_num; ++i) {
        ObObjParam &param = is_arraybinding_ ? arraybinding_params_->at(i) : params_->at(i);
        if (OB_SUCC(ret) && OB_FAIL(parse_request_param_value(alloc,
                                                              session,
                                                              pos,
                                                              i,
                                                              param_types.at(i),
                                                              param_type_infos.at(i),
                                                              param,
                                                              bitmap))) {
          LOG_WARN("fail to parse request param values", K(ret), K(i));
        } else {
          LOG_DEBUG("after parser param", K(param), K(i));
        }
        if (OB_SUCC(ret) && is_arraybinding_) {
          OZ (check_param_value_for_arraybinding(param));
        }
      }

      // Step5-2: decode returning into value
      // need parse returning into params
      if (OB_SUCC(ret) && returning_params_num > 0) {
        CK(returning_param_types.count() == returning_params_num);
        CK(returning_param_type_infos.count() == returning_params_num);
        for (int64_t i = 0; OB_SUCC(ret) && i < returning_params_num; ++i) {
          ObObjParam param;
          if (OB_FAIL(parse_request_param_value(alloc,
                                                session,
                                                pos,
                                                i + input_param_num,
                                                returning_param_types.at(i),
                                                returning_param_type_infos.at(i),
                                                param,
                                                bitmap))) {
            LOG_WARN("fail to parse request returning into param values", K(ret), K(i));
          } else {
            LOG_DEBUG("after parser resolve returning into", K(param), K(i));
          }
        }
      }
    }
    ctx_.schema_guard_ = old_guard;
    ctx_.session_info_ = old_sess_info;
  }
  return ret;
}

int ObMPStmtExecute::decode_type_info(const char*& buf, TypeInfo &type_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    uint64_t length = 0;
    if (OB_FAIL(ObMySQLUtil::get_length(buf, length))) {
      LOG_WARN("failed to get length", K(ret));
    } else {
      PS_DEFENSE_CHECK(length)
      {
        type_info.relation_name_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
        buf += length;
      }
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t length = 0;
    if (OB_FAIL(ObMySQLUtil::get_length(buf, length))) {
      LOG_WARN("failed to get length", K(ret));
    } else {
      PS_DEFENSE_CHECK(length)
      {
        type_info.type_name_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
        buf += length;
      }
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

int ObMPStmtExecute::set_session_active(ObSQLSessionInfo &session) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session.set_session_state(QUERY_ACTIVE))) {
    LOG_WARN("fail to set session state", K(ret));
  } else {
    session.set_query_start_time(get_receive_timestamp());
    session.set_mysql_cmd(obmysql::COM_STMT_EXECUTE);
    session.update_last_active_time();
  }
  return ret;
}

int ObMPStmtExecute::execute_response(ObSQLSessionInfo &session,
                                      ObMySQLResultSet &result,
                                      const bool enable_perf_event,
                                      bool &need_response_error,
                                      bool &is_diagnostics_stmt,
                                      int64_t &execution_id,
                                      const bool force_sync_resp,
                                      bool &async_resp_used,
                                      ObPsStmtId &inner_stmt_id)
{
  int ret = OB_SUCCESS;
  inner_stmt_id = OB_INVALID_ID;
  ObIAllocator &alloc = CURRENT_CONTEXT->get_arena_allocator();
  if (OB_ISNULL(session.get_ps_cache())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ps : ps cache is null.", K(ret), K(stmt_id_));
  } else if (OB_FAIL(session.get_inner_ps_stmt_id(stmt_id_, inner_stmt_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ps : get inner stmt id fail.", K(ret), K(stmt_id_));
  } else {
    ObPsStmtInfoGuard guard;
    ObPsStmtInfo *ps_info = NULL;
    if (OB_FAIL(session.get_ps_cache()->get_stmt_info_guard(inner_stmt_id, guard))) {
      LOG_WARN("get stmt info guard failed", K(ret), K(stmt_id_), K(inner_stmt_id));
    } else if (OB_ISNULL(ps_info = guard.get_stmt_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get stmt info is null", K(ret));
    } else {
      if (is_execute_ps_cursor() && stmt::T_SELECT != ps_info->get_stmt_type()) {
        set_ps_cursor_type(ObNormalType);
      }
      ctx_.cur_sql_ = ps_info->get_ps_sql();
    }
  }
  if OB_FAIL(ret) {
    // do nothing
  } else if (is_execute_ps_cursor()) {
    ObDbmsCursorInfo *cursor = NULL;
    bool use_stream = false;
    // 1.创建cursor
    if (OB_NOT_NULL(session.get_cursor(stmt_id_))) {
      if (OB_FAIL(session.close_cursor(stmt_id_))) {
        LOG_WARN("fail to close result set", K(ret), K(stmt_id_), K(session.get_sessid()));
      }
    }
    OZ (session.make_dbms_cursor(cursor, stmt_id_));
    CK (OB_NOT_NULL(cursor));
    OX (cursor->set_stmt_type(stmt::T_SELECT));
    OX (cursor->set_ps_sql(ctx_.cur_sql_));
    OZ (session.ps_use_stream_result_set(use_stream));
    if (use_stream) {
      OX (cursor->set_streaming());
    }
    OZ (cursor->prepare_entity(session));
    CK (OB_NOT_NULL(cursor->get_allocator()));
    OZ (cursor->init_params(params_->count()));
    OZ (cursor->get_exec_params().assign(*params_));
    OZ (gctx_.sql_engine_->init_result_set(ctx_, result));
    if (OB_SUCCESS != ret || enable_perf_event) {
      exec_start_timestamp_ = ObTimeUtility::current_time();
    }
    if (OB_SUCC(ret)) {
      ObPLExecCtx pl_ctx(cursor->get_allocator(), &result.get_exec_context(), NULL/*params*/,
                        NULL/*result*/, &ret, NULL/*func*/, true);
      if (OB_FAIL(ObSPIService::dbms_dynamic_open(&pl_ctx, *cursor))) {
        LOG_WARN("open cursor fail. ", K(ret), K(stmt_id_));
        if (!THIS_WORKER.need_retry()) {
          int cli_ret = OB_SUCCESS;
          retry_ctrl_.test_and_save_retry_state(
            gctx_, ctx_, result, ret, cli_ret, is_arraybinding_ /*ararybinding only local retry*/);
          if (OB_ERR_PROXY_REROUTE == ret) {
            LOG_DEBUG("run stmt_query failed, check if need retry",
                      K(ret), K(cli_ret), K(retry_ctrl_.need_retry()), K_(stmt_id));
          } else {
            LOG_WARN("run stmt_query failed, check if need retry",
                      K(ret), K(cli_ret), K(retry_ctrl_.need_retry()), K_(stmt_id));
          }
          ret = cli_ret;
        }
        if (OB_ERR_PROXY_REROUTE == ret && !is_arraybinding_) {
          need_response_error = true;
        }
      }
    }
    /*
    * PS模式exec-cursor协议中，
    * 不返回 result_set 结果集，只返回包头信息
    * 并在EOF包中设置 OB_SERVER_STATUS_CURSOR_EXISTS 状态
    * 提示驱动发送fetch协议
    */
    OZ (response_query_header(session, *cursor));
    if (OB_SUCCESS != ret && OB_NOT_NULL(cursor)) {
      int tmp_ret = ret;
      if (OB_FAIL(session.close_cursor(cursor->get_id()))) {
        LOG_WARN("close cursor failed.", K(ret), K(stmt_id_));
      }
      ret = tmp_ret;
    }
  } else if (FALSE_IT(ctx_.enable_sql_resource_manage_ = true)) {
  } else if (OB_FAIL(gctx_.sql_engine_->stmt_execute(stmt_id_,
                                                      stmt_type_,
                                                      *params_,
                                                      ctx_, result,
                                                      false /* is_inner_sql */))) {
    exec_start_timestamp_ = ObTimeUtility::current_time();
    if (!THIS_WORKER.need_retry()) {
      int cli_ret = OB_SUCCESS;
      retry_ctrl_.test_and_save_retry_state(
        gctx_, ctx_, result, ret, cli_ret, is_arraybinding_ /*ararybinding only local retry*/);
      if (OB_ERR_PROXY_REROUTE == ret) {
        LOG_DEBUG("run stmt_query failed, check if need retry",
                  K(ret), K(cli_ret), K(retry_ctrl_.need_retry()), K_(stmt_id));
      } else {
        LOG_WARN("run stmt_query failed, check if need retry",
                  K(ret), K(cli_ret), K(retry_ctrl_.need_retry()), K_(stmt_id));
      }
      ret = cli_ret;
    }
    if (OB_ERR_PROXY_REROUTE == ret && !is_arraybinding_) {
      need_response_error = true;
    }
  } else {
    //监控项统计开始
    exec_start_timestamp_ = ObTimeUtility::current_time();
    result.get_exec_context().set_plan_start_time(exec_start_timestamp_);
    // 本分支内如果出错，全部会在response_result内部处理妥当
    // 无需再额外处理回复错误包

    need_response_error = false;
    is_diagnostics_stmt = ObStmt::is_diagnostic_stmt(result.get_literal_stmt_type());
    ctx_.is_show_trace_stmt_ = ObStmt::is_show_trace_stmt(result.get_literal_stmt_type());
    session.set_current_execution_id(execution_id);

    if (OB_FAIL(ret)) {
    } else if (is_arraybinding_) {
      if (OB_FAIL(after_do_process_for_arraybinding(result))) {
        LOG_WARN("failed to process arraybinding sql", K(ret));
      }
    } else if (OB_FAIL(response_result(result,
                                        session,
                                        force_sync_resp,
                                        async_resp_used))) {
      ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
      if (OB_ISNULL(plan_ctx)) {
        LOG_ERROR("execute query fail, and plan_ctx is NULL", K(ret));
      } else {
        LOG_WARN("execute query fail", K(ret), "timeout_timestamp",
                  plan_ctx->get_timeout_timestamp());
      }
    }
  }
  return ret;
}
int ObMPStmtExecute::do_process(ObSQLSessionInfo &session,
                                 ParamStore *param_store,
                                 const bool has_more_result,
                                 const bool force_sync_resp,
                                 bool &async_resp_used)
{
  int ret = OB_SUCCESS;
  ObAuditRecordData &audit_record = session.get_raw_audit_record();
  audit_record.try_cnt_++;
  bool is_diagnostics_stmt = false;
  ObPsStmtId inner_stmt_id = OB_INVALID_ID;
  bool need_response_error = is_arraybinding_ ? false : true;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  const bool enable_sql_audit =
    GCONF.enable_sql_audit && session.get_local_ob_enable_sql_audit();

  single_process_timestamp_ = ObTimeUtility::current_time();

  /* !!!
   * 注意req_timeinfo_guard一定要放在result前面
   * !!!
   */
  ObReqTimeGuard req_timeinfo_guard;
  SMART_VAR(ObMySQLResultSet, result, session, THIS_WORKER.get_sql_arena_allocator()) {

    ObWaitEventStat total_wait_desc;
    int64_t execution_id = 0;
    ObDiagnoseSessionInfo *di = ObDiagnoseSessionInfo::get_local_diagnose_info();
    {
      ObMaxWaitGuard max_wait_guard(enable_perf_event ? &audit_record.exec_record_.max_wait_event_ : NULL, di);
      ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL, di);
      if (enable_perf_event) {
        audit_record.exec_record_.record_start(di);
      }

      result.set_has_more_result(has_more_result);
      result.set_ps_protocol();
      ObTaskExecutorCtx *task_ctx = result.get_exec_context().get_task_executor_ctx();
      if (OB_ISNULL(task_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("task executor ctx can not be NULL", K(task_ctx), K(ret));
      } else {
        task_ctx->schema_service_ = gctx_.schema_service_;
        task_ctx->set_query_tenant_begin_schema_version(retry_ctrl_.get_tenant_local_schema_version());
        task_ctx->set_query_sys_begin_schema_version(retry_ctrl_.get_sys_local_schema_version());
        task_ctx->set_min_cluster_version(GET_MIN_CLUSTER_VERSION());

        ctx_.retry_times_ = retry_ctrl_.get_retry_times();
        if (OB_ISNULL(ctx_.schema_guard_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("newest schema is NULL", K(ret));
        } else if (OB_FAIL(result.init())) {
          LOG_WARN("result set init failed", K(ret));
        } else if (OB_ISNULL(gctx_.sql_engine_) || OB_ISNULL(param_store)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid sql engine", K(ret), K(gctx_), K(param_store));
        } else if (FALSE_IT(execution_id = gctx_.sql_engine_->get_execution_id())) {
          // do nothing ...
        } else if (OB_FAIL(set_session_active(session))) {
          LOG_WARN("fail to set session active", K(ret));
        } else {
          if (is_prexecute()) {
            ret = static_cast<ObMPStmtPrexecute*>(this)->
                      execute_response(session,
                                        *param_store,
                                        ctx_,
                                        result,
                                        retry_ctrl_,
                                        enable_perf_event,
                                        need_response_error,
                                        is_diagnostics_stmt,
                                        execution_id,
                                        force_sync_resp,
                                        async_resp_used,
                                        inner_stmt_id);
          } else {
            ret = execute_response(session,
                                    result,
                                    enable_perf_event,
                                    need_response_error,
                                    is_diagnostics_stmt,
                                    execution_id,
                                    force_sync_resp,
                                    async_resp_used,
                                    inner_stmt_id);
          }
          if ((OB_SUCC(ret) && is_diagnostics_stmt) || async_resp_used) {
            // if diagnostic stmt succeed, no need to clear warning buf.
            // or async resp is used, it will be cleared in callback thread.
            session.update_show_warnings_buf();
          } else {
            session.set_show_warnings_buf(ret);
          }
        }
      }
      //监控项统计结束
      exec_end_timestamp_ = ObTimeUtility::current_time();

      // some statistics must be recorded for plan stat, even though sql audit disabled
      bool first_record = (1 == audit_record.try_cnt_);
      ObExecStatUtils::record_exec_timestamp(*this, first_record, audit_record.exec_timestamp_);
      audit_record.exec_timestamp_.update_stage_time();

      if (enable_perf_event) {
        audit_record.exec_record_.record_end(di);
        record_stat(result.get_stmt_type(), exec_end_timestamp_);
        audit_record.exec_record_.wait_time_end_ = total_wait_desc.time_waited_;
        audit_record.exec_record_.wait_count_end_ = total_wait_desc.total_waits_;
        audit_record.update_event_stage_state();
      }

      if (enable_perf_event && !THIS_THWORKER.need_retry()
        && OB_NOT_NULL(result.get_physical_plan())) {
        const int64_t time_cost = exec_end_timestamp_ - get_receive_timestamp();
        ObSQLUtils::record_execute_time(result.get_physical_plan()->get_plan_type(), time_cost);
      }

      if (OB_FAIL(ret)
          && !async_resp_used
          && need_response_error
          && is_conn_valid()
          && !THIS_WORKER.need_retry()
          && !retry_ctrl_.need_retry()) {
        LOG_WARN("query failed", K(ret), K(retry_ctrl_.need_retry()), K_(stmt_id));
        // 当need_retry=false时，可能给客户端回过包了，可能还没有回过任何包。
        // 不过，可以确定：这个请求出错了，还没处理完。如果不是已经交给异步EndTrans收尾，
        // 则需要在下面回复一个error_packet作为收尾。否则后面没人帮忙发错误包给客户端了，
        // 可能会导致客户端挂起等回包。
        bool is_partition_hit = session.get_err_final_partition_hit(ret);
        int err = send_error_packet(ret, NULL, is_partition_hit, (void *)ctx_.get_reroute_info());
        if (OB_SUCCESS != err) {  // 发送error包
          LOG_WARN("send error packet failed", K(ret), K(err));
        }
      }
    }

    audit_record.status_ =
      (0 == ret || OB_ITER_END == ret) ? REQUEST_SUCC : (ret);
    if (enable_sql_audit && !is_ps_cursor()) {
      ObPhysicalPlan *plan = result.get_physical_plan();
      audit_record.seq_ = 0;  //don't use now
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
        audit_record.rule_name_ = const_cast<char *>(plan->get_rule_name().ptr());
        audit_record.rule_name_len_ = plan->get_rule_name().length();
        audit_record.partition_hit_ = session.partition_hit().get_bool();
      }
      audit_record.affected_rows_ = result.get_affected_rows();
      audit_record.return_rows_ = result.get_return_rows();
      audit_record.partition_cnt_ =
        result.get_exec_context().get_das_ctx().get_related_tablet_cnt();
      audit_record.expected_worker_cnt_ =
        result.get_exec_context().get_task_exec_ctx().get_expected_worker_cnt();
      audit_record.used_worker_cnt_ =
        result.get_exec_context().get_task_exec_ctx().get_admited_worker_cnt();

      audit_record.is_executor_rpc_ = false;
      audit_record.is_inner_sql_ = false;
      audit_record.is_hit_plan_cache_ = result.get_is_from_plan_cache();
      audit_record.sql_ = const_cast<char *>(ctx_.cur_sql_.ptr());
      audit_record.sql_len_ = min(ctx_.cur_sql_.length(), OB_MAX_SQL_LENGTH);
      audit_record.sql_cs_type_ = session.get_local_collation_connection();
      audit_record.ps_stmt_id_ = stmt_id_;
      audit_record.ps_inner_stmt_id_ = inner_stmt_id;
      audit_record.params_value_ = params_value_;
      audit_record.params_value_len_ = params_value_len_;
      audit_record.is_perf_event_closed_ = !lib::is_diagnose_info_enabled();

      ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
      if (OB_NOT_NULL(plan_ctx)) {
        audit_record.consistency_level_ = plan_ctx->get_consistency_level();
      }
    }

    //update v$sql statistics
    if ((OB_SUCC(ret) || audit_record.is_timeout())
        && session.get_local_ob_enable_plan_cache()
        && !retry_ctrl_.need_retry()
        && !is_ps_cursor()) {
      // ps cursor do this in inner open
      ObIArray<ObTableRowCount> *table_row_count_list = NULL;
      ObPhysicalPlan *plan = result.get_physical_plan();
      ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
      if (OB_NOT_NULL(plan_ctx)) {
        table_row_count_list = &(plan_ctx->get_table_row_count_list());
        audit_record.table_scan_stat_ = plan_ctx->get_table_scan_stat();
      }
      if (NULL != plan) {
        if (!(ctx_.self_add_plan_) && ctx_.plan_cache_hit_) {
          plan->update_plan_stat(audit_record,
              false, // false mean not first update plan stat
              result.get_exec_context().get_is_evolution(),
              table_row_count_list);
          plan->update_cache_access_stat(audit_record.table_scan_stat_);
        } else if (ctx_.self_add_plan_ && !ctx_.plan_cache_hit_) {
          plan->update_plan_stat(audit_record,
              true,
              result.get_exec_context().get_is_evolution(),
              table_row_count_list);
          plan->update_cache_access_stat(audit_record.table_scan_stat_);
        } else if (ctx_.self_add_plan_ && ctx_.plan_cache_hit_) {
          // spm evolution plan first execute
          plan->update_plan_stat(audit_record,
              true,
              result.get_exec_context().get_is_evolution(),
              table_row_count_list);
          plan->update_cache_access_stat(audit_record.table_scan_stat_);
        }
      }
    }

    // reset thread waring buffer in sync mode
    if (!async_resp_used) {
      clear_wb_content(session);
    }

    bool need_retry = (THIS_THWORKER.need_retry()
                       || RETRY_TYPE_NONE != retry_ctrl_.get_retry_type());
    if (!is_ps_cursor()) {
#ifdef OB_BUILD_SPM
      if (!need_retry) {
        (void)ObSQLUtils::handle_plan_baseline(audit_record, result.get_physical_plan(), ret, ctx_);
      }
#endif
      // ps cursor has already record after inner_open in spi
      ObSQLUtils::handle_audit_record(need_retry, EXECUTE_PS_EXECUTE, session, ctx_.is_sensitive_);
    }
  }
  return ret;
}

// return false only if send packet fail.
int ObMPStmtExecute::response_result(
    ObMySQLResultSet &result,
    ObSQLSessionInfo &session,
    bool force_sync_resp,
    bool &async_resp_used)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_SPM
  bool need_trans_cb  = result.need_end_trans_callback() && (!force_sync_resp);
#else
  bool need_trans_cb  = result.need_end_trans_callback() &&
                        (!force_sync_resp) &&
                        (!ctx_.spm_ctx_.check_execute_status_);
#endif

  // NG_TRACE_EXT(exec_begin, ID(arg1), force_sync_resp, ID(end_trans_cb), need_trans_cb);

  if (OB_LIKELY(NULL != result.get_physical_plan())) {
    if (need_trans_cb) {
      ObAsyncPlanDriver drv(gctx_, ctx_, session, retry_ctrl_, *this, is_prexecute());
      // NOTE: sql_end_cb必须在drv.response_result()之前初始化好
      ObSqlEndTransCb &sql_end_cb = session.get_mysql_end_trans_cb();
      if (OB_FAIL(sql_end_cb.init(packet_sender_, &session,
                                    stmt_id_, params_num_))) {
        LOG_WARN("failed to init sql end callback", K(ret));
      } else if (OB_FAIL(drv.response_result(result))) {
        LOG_WARN("fail response async result", K(ret));
      }
      async_resp_used = result.is_async_end_trans_submitted();
    } else {
      // 试点ObQuerySyncDriver
      int32_t iteration_count = OB_INVALID_COUNT;
      if (is_prexecute()) {
        iteration_count = static_cast<ObMPStmtPrexecute*>(this)->get_iteration_count();
      }
      ObSyncPlanDriver drv(gctx_,
                           ctx_,
                           session,
                           retry_ctrl_,
                           *this,
                           is_prexecute(),
                           iteration_count);
      ret = drv.response_result(result);
    }
  } else {
    if (need_trans_cb) {
      ObSqlEndTransCb &sql_end_cb = session.get_mysql_end_trans_cb();
      ObAsyncCmdDriver drv(gctx_, ctx_, session, retry_ctrl_, *this, is_prexecute());
      if (OB_FAIL(sql_end_cb.init(packet_sender_, &session,
                                    stmt_id_, params_num_))) {
        LOG_WARN("failed to init sql end callback", K(ret));
      } else if (OB_FAIL(drv.response_result(result))) {
        LOG_WARN("fail response async result", K(ret));
      } else {
        LOG_DEBUG("use async cmd driver success!",
                  K(result.get_stmt_type()), K(session.get_local_autocommit()));
      }
      async_resp_used = result.is_async_end_trans_submitted();
    } else {
      ObSyncCmdDriver drv(gctx_, ctx_, session, retry_ctrl_, *this, is_prexecute());
      session.set_pl_query_sender(&drv);
      session.set_ps_protocol(result.is_ps_protocol());
      if (OB_FAIL(drv.response_result(result))) {
        LOG_WARN("failed response sync result", K(ret));
      } else {
        LOG_DEBUG("use sync cmd driver success!",
                  K(result.get_stmt_type()), K(session.get_local_autocommit()));
      }
      session.set_pl_query_sender(NULL);
    }
  }
//  NG_TRACE(exec_end);
  return ret;
}

OB_NOINLINE int ObMPStmtExecute::process_retry(ObSQLSessionInfo &session,
                                               ParamStore *param_store,
                                               bool has_more_result,
                                               bool force_sync_resp,
                                               bool &async_resp_used)
{
  int ret = OB_SUCCESS;
  //create a temporary memory context to process retry, avoid memory bloat caused by retries
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(),
      ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
    .set_properties(lib::USE_TL_PAGE_OPTIONAL)
    .set_page_size(!lib::is_mini_mode() ? OB_MALLOC_BIG_BLOCK_SIZE
        : OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  CREATE_WITH_TEMP_CONTEXT(param) {
    ret = do_process(session,
                     param_store,
                     has_more_result,
                     force_sync_resp,
                     async_resp_used);
    ctx_.clear();
  }
  return ret;
}

int ObMPStmtExecute::do_process_single(ObSQLSessionInfo &session,
                                       ParamStore *param_store,
                                       bool has_more_result,
                                       bool force_sync_resp,
                                       bool &async_resp_used)
{
  int ret = OB_SUCCESS;
  // 每次执行不同sql都需要更新
  ctx_.self_add_plan_ = false;
  oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_LOCAL_RETRY);
  do {
    // 每次都必须设置为OB_SCCESS, 否则可能会因为没有调用do_process()造成死循环
    ret = OB_SUCCESS;
    share::schema::ObSchemaGetterGuard schema_guard;
    int64_t tenant_version = 0;
    int64_t sys_version = 0;
    retry_ctrl_.clear_state_before_each_retry(session.get_retry_info_for_update());
    OZ (gctx_.schema_service_->get_tenant_schema_guard(session.get_effective_tenant_id(),
                                                       schema_guard));
    OZ (schema_guard.get_schema_version(session.get_effective_tenant_id(), tenant_version));
    OZ (schema_guard.get_schema_version(OB_SYS_TENANT_ID, sys_version));
    OX (ctx_.schema_guard_ = &schema_guard);
    OX (retry_ctrl_.set_tenant_local_schema_version(tenant_version));
    OX (retry_ctrl_.set_sys_local_schema_version(sys_version));

    if (OB_SUCC(ret) && !is_send_long_data()) {
      if (OB_LIKELY(session.get_is_in_retry()) 
            || (is_arraybinding_ && (prepare_packet_sent_ || !is_prexecute()))) {
        ret = process_retry(session,
				                    param_store,
                            has_more_result,
                            force_sync_resp,
                            async_resp_used);
      } else {
        ret = do_process(session,
						             param_store,
                         has_more_result,
                         force_sync_resp,
                         async_resp_used);
        ctx_.clear();
      }
      session.set_session_in_retry(retry_ctrl_.need_retry());
    }
  } while (RETRY_TYPE_LOCAL == retry_ctrl_.get_retry_type());

  if (OB_SUCC(ret) && retry_ctrl_.get_retry_times() > 0) {
    // 经过重试之后才成功的，把sql打印出来。这里只能覆盖到本地重试的情况，没法覆盖到扔回队列重试的情况。
    // 如果需要重试则ret不可能为OB_SUCCESS，因此这里不用判断retry_type。
    LOG_TRACE("sql retry",
              K(ret), "retry_times", retry_ctrl_.get_retry_times(), "sql", ctx_.cur_sql_);
  }
  return ret;
}

int ObMPStmtExecute::is_arraybinding_returning(sql::ObSQLSessionInfo &session, bool &is_ab_return)
{
  int ret = OB_SUCCESS;
  ObPsCache *ps_cache = NULL;
  ObPsStmtId inner_stmt_id = OB_INVALID_ID;
  ObPsStmtInfoGuard guard;
  ObPsStmtInfo *ps_info = NULL;
  is_ab_return = false;
  if (OB_ISNULL(ps_cache = session.get_ps_cache())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("physical plan context or ps plan cache is NULL or schema_guard is null",
              K(ret), K(ps_cache));
  } else if (OB_FAIL(session.get_inner_ps_stmt_id(stmt_id_, inner_stmt_id))) {
    LOG_WARN("fail to get inner ps stmt_id", K(ret), K(stmt_id_), K(inner_stmt_id));
  } else if (OB_FAIL(session.get_ps_cache()->get_stmt_info_guard(inner_stmt_id, guard))) {
    LOG_WARN("get stmt info guard failed", K(ret), K(stmt_id_), K(inner_stmt_id));
  } else if (OB_ISNULL(ps_info = guard.get_stmt_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get stmt info is null", K(ret));
  } else if (ps_info->get_num_of_returning_into() > 0) {
    is_ab_return = true;
    LOG_TRACE("is arraybinding returning", K(ret), KPC(ps_info));
  }
  return ret;
}

int ObMPStmtExecute::try_batch_multi_stmt_optimization(ObSQLSessionInfo &session,
                                                       bool has_more_result,
                                                       bool force_sync_resp,
                                                       bool &async_resp_used,
                                                       bool &optimization_done)
{
  // 1. save_exception 不能batch
  // 2. returning 不能batch
  int ret = OB_SUCCESS;
  optimization_done = false;
  ctx_.multi_stmt_item_.set_ps_mode(true);
  ctx_.multi_stmt_item_.set_ab_cnt(arraybinding_size_);
  bool is_ab_returning = false;
  ParamStore *array_binding_params = NULL;
  bool enable_batch_opt = session.is_enable_batched_multi_statement();
  bool use_plan_cache = session.get_local_ob_enable_plan_cache();
  ObIAllocator &alloc = CURRENT_CONTEXT->get_arena_allocator();

  if (!enable_batch_opt) {
    // 不支持做batch执行
    LOG_TRACE("not open the batch optimization");
  } else if (!use_plan_cache) {
    LOG_TRACE("not enable the plan_cache", K(use_plan_cache));
    // plan_cache开关没打开
  } else if (!is_prexecute()) {
    // 只对二合一协议开启batch优化
  } else if (is_pl_stmt(stmt_type_)) {
    LOG_TRACE("is pl execution, can't do the batch optimization");
  } else if (1 == arraybinding_size_) {
    LOG_TRACE("arraybinding size is 1, not need d batch");
  } else if (get_save_exception()) {
    LOG_TRACE("is save exception mode, not supported batch optimization");
  } else if (OB_FAIL(is_arraybinding_returning(session, is_ab_returning))) {
    LOG_WARN("failed to check is arraybinding returning", K(ret));
  } else if (is_ab_returning) {
    LOG_TRACE("returning not support the batch optimization");
  } else if (OB_FAIL(ObSQLUtils::transform_pl_ext_type(*arraybinding_params_,
                                                       arraybinding_size_,
                                                       alloc,
                                                       array_binding_params))) {
    LOG_WARN("fail to trans_form extend type params_store", K(ret), K(arraybinding_size_));
  } else if (OB_FAIL(do_process_single(session, array_binding_params, has_more_result, force_sync_resp, async_resp_used))) {
    // 调用do_single接口
    if (THIS_WORKER.need_retry()) {
      // just go back to large query queue and retry
    } else if (OB_BATCHED_MULTI_STMT_ROLLBACK == ret) {
      LOG_TRACE("batched multi_stmt needs rollback", K(ret));
      ret = OB_SUCCESS;
    } else {
      // 无论什么报错，都走单行执行一次，用于容错
      int ret_tmp = ret;
      ret = OB_SUCCESS;
      LOG_WARN("failed to process batch stmt, cover the error code, reset retry flag, then execute with single row",
          K(ret_tmp), K(ret), K(THIS_WORKER.need_retry()));
    }
  } else {
    optimization_done = true;
  }
  LOG_TRACE("after try batched multi-stmt optimization", K(ret), K(stmt_type_), K(use_plan_cache),
      K(optimization_done), K(enable_batch_opt), K(is_ab_returning), K(THIS_WORKER.need_retry()), K(arraybinding_size_));
  return ret;
}

int ObMPStmtExecute::process_execute_stmt(const ObMultiStmtItem &multi_stmt_item,
                                          ObSQLSessionInfo &session,
                                          bool has_more_result,
                                          bool force_sync_resp,
                                          bool &async_resp_used)
{
  int ret = OB_SUCCESS;
  bool need_response_error = true;

  // 执行setup_wb后，所有WARNING都会写入到当前session的WARNING BUFFER中
  setup_wb(session);
  const bool enable_trace_log = lib::is_trace_log_enabled();
  //============================ 注意这些变量的生命周期 ================================
  ObSMConnection *conn = get_conn();
  ObSessionStatEstGuard stat_est_guard(conn->tenant_->id(), session.get_sessid());
  if (OB_FAIL(init_process_var(ctx_, multi_stmt_item, session))) {
    LOG_WARN("init process var failed.", K(ret), K(multi_stmt_item));
  } else {
    if (enable_trace_log) {
      //set session log_level.Must use ObThreadLogLevelUtils::clear() in pair
      ObThreadLogLevelUtils::init(session.get_log_id_level_map());
    }
    // obproxy may use 'SET @@last_schema_version = xxxx' to set newest schema,
    // observer will force refresh schema if local_schema_version < last_schema_version;
    if (OB_FAIL(check_and_refresh_schema(session.get_login_tenant_id(),
                                         session.get_effective_tenant_id()))) {
      LOG_WARN("failed to check_and_refresh_schema", K(ret));
    } else if (OB_FAIL(session.update_timezone_info())) {
      LOG_WARN("fail to update time zone info", K(ret));
    } else if (is_arraybinding_) {
      need_response_error = false;
      bool optimization_done = false;
      if (ctx_.can_reroute_sql_) {
        ctx_.can_reroute_sql_ = false;
        LOG_INFO("arraybinding not support reroute sql.");
      }
      ObSEArray<ObSavedException, 4> exception_array;
      if (OB_UNLIKELY(arraybinding_size_ <= 0)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("arraybinding has no parameters", K(ret), K(arraybinding_size_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "oci arraybinding has no parameters");
      } else if (OB_FAIL(try_batch_multi_stmt_optimization(session,
                                                           has_more_result,
                                                           force_sync_resp,
                                                           async_resp_used, optimization_done))) {
        LOG_WARN("fail to try_batch_multi_stmt_optimization", K(ret));
      } else if (!optimization_done) {
        ctx_.multi_stmt_item_.set_ps_mode(true);
        ctx_.multi_stmt_item_.set_ab_cnt(0);
        for (int64_t i = 0; OB_SUCC(ret) && i < arraybinding_size_; ++i) {
          set_curr_sql_idx(i);
          OZ (construct_execute_param_for_arraybinding(i));
          OZ (do_process_single(session, params_, has_more_result, force_sync_resp, async_resp_used));
          if (OB_FAIL(ret)) {
            if (is_save_exception_ && !is_prexecute()) {
              // The old ps protocol will only collect error information here,
              // and the new one has already done fault tolerance in the front
              ret = save_exception_for_arraybinding(i, ret, exception_array);
              ret = OB_SUCCESS;
            }
            if (OB_FAIL(ret)) {
              // If there is still an error in the new ps protocol,
              // then send an err package,
              // indicating that the server has an error that is not expected by the customer
              need_response_error = true;
              break;
            }
          }
        }
      }
      // 释放数组内存避免内存泄漏
      reset_complex_param_memory(arraybinding_params_, session);
      OZ (response_result_for_arraybinding(session, exception_array));
    } else {
      need_response_error = false;
      if (OB_FAIL(do_process_single(session, params_, has_more_result, force_sync_resp, async_resp_used))) {
        LOG_WARN("fail to do process", K(ret), K(ctx_.cur_sql_));
      }
      if (OB_UNLIKELY(NULL != GCTX.cgroup_ctrl_) && GCTX.cgroup_ctrl_->is_valid() && is_conn_valid()) {
        int bak_ret = ret;
        ObSQLSessionInfo *sess = NULL;
        if (OB_FAIL(get_session(sess))) {
          LOG_WARN("get session fail", K(ret));
        } else if (OB_ISNULL(sess)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session is NULL or invalid", K(ret));
        } else {
          // Call setup_user_resource_group no matter OB_SUCC or OB_FAIL
          if (OB_FAIL(setup_user_resource_group(*conn, sess->get_effective_tenant_id(), sess))) {
            LOG_WARN("fail setup user resource group", K(ret));
          }
        }
        if (sess != NULL) {
          revert_session(sess);
        }
        ret = OB_SUCC(bak_ret) ? ret : bak_ret;
      }
      reset_complex_param_memory(params_, session);
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

  //对于tracelog的处理, 不影响正常逻辑, 错误码无须赋值给ret, 清空WARNING BUFFER
  do_after_process(session, ctx_, async_resp_used);

  if (OB_FAIL(ret) && need_response_error && is_conn_valid()) {
    send_error_packet(ret, NULL);
  }

  return ret;
}


int ObMPStmtExecute::process()
{
  int ret = OB_SUCCESS;
  int flush_ret = OB_SUCCESS;
  trace::UUID ps_execute_span_id;
  ObSQLSessionInfo *sess = NULL;
  bool need_response_error = true;
  bool need_disconnect = true;
  bool async_resp_used = false; // 由事务提交线程异步回复客户端
  int64_t query_timeout = 0;

  ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
  ObSMConnection *conn = get_conn();
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
    ObSQLSessionInfo &session = *sess;
    int64_t tenant_version = 0;
    int64_t sys_version = 0;
    THIS_WORKER.set_session(sess);
    lib::CompatModeGuard g(sess->get_compatibility_mode() == ORACLE_MODE ?
                             lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
    ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
    session.set_current_trace_id(ObCurTraceId::get_trace_id());
    session.get_raw_audit_record().request_memory_used_ = 0;
    observer::ObProcessMallocCallback pmcb(0,
          session.get_raw_audit_record().request_memory_used_);
    lib::ObMallocCallbackGuard guard(pmcb);
    session.set_thread_id(GETTID());
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    int64_t packet_len = pkt.get_clen();
    if (OB_UNLIKELY(!session.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid session", K_(stmt_id), K(ret));
    } else if (OB_UNLIKELY(session.is_zombie())) {
      //session has been killed some moment ago
      ret = OB_ERR_SESSION_INTERRUPTED;
      LOG_WARN("session has been killed", K(session.get_session_state()), K_(stmt_id),
               K(session.get_sessid()), "proxy_sessid", session.get_proxy_sessid(), K(ret));
    } else if (OB_FAIL(session.check_and_init_retry_info(*cur_trace_id, ctx_.cur_sql_))) {
      LOG_WARN("fail to check and init retry info", K(ret), K(*cur_trace_id), K(ctx_.cur_sql_));
    } else if (OB_FAIL(session.get_query_timeout(query_timeout))) {
      LOG_WARN("fail to get query timeout", K(ret));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                session.get_effective_tenant_id(), tenant_version))) {
      LOG_WARN("fail get tenant broadcast version", K(ret));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(
                OB_SYS_TENANT_ID, sys_version))) {
      LOG_WARN("fail get tenant broadcast version", K(ret));
    } else if (pkt.exist_trace_info()
               && OB_FAIL(session.update_sys_variable(SYS_VAR_OB_TRACE_INFO,
                                                      pkt.get_trace_info()))) {
      LOG_WARN("fail to update trace info", K(ret));
    } else if (FALSE_IT(session.set_txn_free_route(pkt.txn_free_route()))) {
    } else if (OB_FAIL(process_extra_info(session, pkt, need_response_error))) {
      LOG_WARN("fail get process extra info", K(ret));
    } else if (FALSE_IT(session.post_sync_session_info())) {
    } else if (OB_UNLIKELY(packet_len > session.get_max_packet_size())) {
      //packet size check with session variable max_allowd_packet or net_buffer_length
      ret = OB_ERR_NET_PACKET_TOO_LARGE;
      LOG_WARN("packet too large than allowed for the session", K_(stmt_id), K(ret));
    } else if (OB_FAIL(sql::ObFLTUtils::init_flt_info(pkt.get_extra_info(), session,
                            conn->proxy_cap_flags_.is_full_link_trace_support()))) {
      LOG_WARN("failed to init flt extra info", K(ret));
    } else if (OB_FAIL(session.gen_configs_in_pc_str())) {
      LOG_WARN("fail to generate configuration string that can influence execution plan", K(ret));
    } else {
      FLTSpanGuard(ps_execute);
      FLT_SET_TAG(log_trace_id, ObCurTraceId::get_trace_id_str(),
                    receive_ts, get_receive_timestamp(),
                    client_info, session.get_client_info(),
                    module_name, session.get_module_name(),
                    action_name, session.get_action_name(),
                    sess_id, session.get_sessid());
      THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout);
      retry_ctrl_.set_tenant_global_schema_version(tenant_version);
      retry_ctrl_.set_sys_global_schema_version(sys_version);
      session.partition_hit().reset();
      session.set_pl_can_retry(true);
      ObLockWaitNode &lock_wait_node  = req_->get_lock_wait_node();
      lock_wait_node.set_session_info(session.get_sessid());

      need_response_error = false;
      need_disconnect = false;
      ret = process_execute_stmt(ObMultiStmtItem(false, 0, ObString()),
                                 session,
                                 false, // has_mode
                                 false, // force_sync_resp
                                 async_resp_used);

      // 退出前打印出SQL语句，便于定位各种问题
      if (OB_FAIL(ret)) {
        if (OB_EAGAIN == ret) {
          //large query, do nothing
        } else if (is_conn_valid()) {// The memory of sql string is invalid if conn_valid_ has been set false.
          LOG_WARN("fail execute sql", "sql_id", ctx_.sql_id_, K_(stmt_id), K(ret));
        } else {
          LOG_WARN("fail execute sql", K(ret));
        }
      }
    }
    session.check_and_reset_retry_info(*cur_trace_id, THIS_WORKER.need_retry());
    session.set_last_trace_id(ObCurTraceId::get_trace_id());
    // whether the previous error was reported, a cleanup is to be done here
    if (!async_resp_used) {
      // async remove in ObSqlEndTransCb
      ObPieceCache *piece_cache = static_cast<ObPieceCache*>(session.get_piece_cache());
      if (OB_ISNULL(piece_cache)) {
        // do nothing
        // piece_cache not be null in piece data protocol
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < params_num_; i++) {
          if (OB_FAIL(piece_cache->remove_piece(
                              piece_cache->get_piece_key(stmt_id_, i),
                              session))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("remove piece fail", K(stmt_id_), K(i), K(ret));
            }
          }
        }
      }
    }
    record_flt_trace(session);
  }

  if (OB_NOT_NULL(sess) && !sess->get_in_transaction()) {
    // transcation ends, end trace
    FLT_END_TRACE();
  }

  if (OB_FAIL(ret) && is_conn_valid()) {
    if (need_response_error) {
      send_error_packet(ret, NULL);
    }
    if (need_disconnect) {
      force_disconnect();
      LOG_WARN("disconnect connection when process query", K(ret));
    }
  }

  // 如果已经异步回包，则这部分逻辑在cb中执行，这里跳过flush_buffer()
  if (!THIS_WORKER.need_retry()) {
    if (async_resp_used) {
      async_resp_used_ = true;
      packet_sender_.disable_response();
    } else {
      flush_ret = flush_buffer(true);
    }
  } else {
    need_retry_ = true;
  }

  THIS_WORKER.set_session(NULL);
  if (sess != NULL) {
    revert_session(sess); //current ignore revert session ret
  }

  return (OB_SUCCESS != ret) ? ret : flush_ret;
}

int ObMPStmtExecute::get_udt_by_name(ObString relation_name,
                                     ObString type_name,
                                     const share::schema::ObUDTTypeInfo *&udt_info)
{
  int ret = OB_SUCCESS;
  ObString new_relation_name;
  CK (OB_NOT_NULL(ctx_.schema_guard_));
  CK (OB_NOT_NULL(ctx_.session_info_));
  if (OB_SUCC(ret)) {
    if (relation_name.empty()) {
      if (ctx_.session_info_->get_database_name().empty()) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("no select no database", K(ret));
      } else {
        new_relation_name = ctx_.session_info_->get_database_name();
      }
    } else {
      new_relation_name = relation_name;
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t database_id = OB_INVALID_ID;
    OZ (ctx_.schema_guard_->get_database_id(ctx_.session_info_->get_effective_tenant_id(),
                                            new_relation_name,
                                            database_id));
    CK (OB_LIKELY(OB_INVALID_ID != database_id));
    OZ (ctx_.schema_guard_->get_udt_info(ctx_.session_info_->get_effective_tenant_id(),
                                         database_id,
                                         OB_INVALID_ID,
                                         type_name,
                                         udt_info));
    if (OB_ISNULL(udt_info)) {
      // 尝试下是不是系统type
      if (relation_name.empty()
          || relation_name.case_compare("oceanbase")
          || relation_name.case_compare("sys")) {
        OZ (ctx_.schema_guard_->get_udt_info(OB_SYS_TENANT_ID,
                                             OB_SYS_DATABASE_ID,
                                             OB_INVALID_ID,
                                             type_name,
                                             udt_info));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(udt_info)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("udt not exist", K(ret), K(relation_name), K(type_name));
    }
  }
  return ret;
}

int ObMPStmtExecute::get_package_type_by_name(ObIAllocator &allocator,
                                              const TypeInfo *type_info,
                                              const pl::ObUserDefinedType *&pl_type)
{
  int ret = OB_SUCCESS;
  const share::schema::ObPackageInfo *package_info = NULL;
  int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                  : COMPATIBLE_MYSQL_MODE;
  ObSchemaChecker schema_checker;
  CK (OB_NOT_NULL(type_info));
  CK (OB_NOT_NULL(ctx_.schema_guard_));
  CK (OB_NOT_NULL(ctx_.session_info_));
  CK (OB_NOT_NULL(ctx_.session_info_->get_pl_engine()));
  if (OB_SUCC(ret) && OB_ISNULL(pl_type ))
  OZ (schema_checker.init(*ctx_.schema_guard_, ctx_.session_info_->get_sessid()));
  OZ (schema_checker.get_package_info(ctx_.session_info_->get_effective_tenant_id(),
                                      type_info->relation_name_,
                                      type_info->package_name_,
                                      share::schema::PACKAGE_TYPE,
                                      compatible_mode,
                                      package_info));
  CK (OB_NOT_NULL(package_info));
  if (OB_SUCC(ret)) {
    pl::ObPLPackageManager &package_manager
      = ctx_.session_info_->get_pl_engine()->get_package_manager();
    pl::ObPLPackageGuard package_guard(ctx_.session_info_->get_effective_tenant_id());
    pl::ObPLResolveCtx resolve_ctx(allocator,
                                   *(ctx_.session_info_),
                                   *(ctx_.schema_guard_),
                                   package_guard,
                                   *(GCTX.sql_proxy_),
                                   false);
    OZ (package_manager.get_package_type(
      resolve_ctx, package_info->get_package_id(), type_info->type_name_, pl_type));
    CK (OB_NOT_NULL(pl_type));
  }
  return ret;
}

int ObMPStmtExecute::get_pl_type_by_type_info(ObIAllocator &allocator,
                                              const TypeInfo *type_info,
                                              const pl::ObUserDefinedType *&pl_type)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(allocator, type_info, pl_type);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not support", K(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Get PL type by type info is not supported in CE version");
#else
  const share::schema::ObUDTTypeInfo *udt_info = NULL;
  if (OB_ISNULL(type_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type info is null", K(ret), K(type_info));
  } else if (!type_info->is_elem_type_) {
    if (type_info->package_name_.empty()) {
      OZ (get_udt_by_name(type_info->relation_name_, type_info->type_name_, udt_info));
      OZ (udt_info->transform_to_pl_type(allocator, pl_type));
    } else {
      OZ (get_package_type_by_name(allocator, type_info, pl_type));
    }
  } else {
    void *ptr = NULL;
    pl::ObNestedTableType *table_type = NULL;
    pl::ObPLDataType elem_type;
    const pl::ObUserDefinedType *elem_type_ptr = NULL;
    if (type_info->elem_type_.get_obj_type() != ObExtendType) {
      elem_type.set_data_type(type_info->elem_type_);
    } else if (OB_FAIL(get_udt_by_name(type_info->relation_name_, type_info->type_name_, udt_info))) {
      LOG_WARN("failed to get udt info", K(ret), K(type_info->relation_name_), K(type_info->type_name_));
    } else if (OB_FAIL(udt_info->transform_to_pl_type(allocator, elem_type_ptr))) {
      LOG_WARN("failed to transform udt to pl type", K(ret));
    } else if (OB_ISNULL(elem_type_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get elem type ptr", K(ret));
    } else {
      elem_type = *(static_cast<const pl::ObPLDataType*>(elem_type_ptr));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(pl::ObNestedTableType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObNestedTableType", K(ret));
    } else {
      table_type = new(ptr)pl::ObNestedTableType();
      table_type->set_type_from(pl::ObPLTypeFrom::PL_TYPE_LOCAL);
      table_type->set_element_type(elem_type);
      pl_type = table_type;
    }
  }
  CK (OB_NOT_NULL(pl_type));
#endif
  return ret;
}

int ObMPStmtExecute::parse_complex_param_value(ObIAllocator &allocator,
                                               const ObCharsetType charset,
                                               const ObCollationType cs_type,
                                               const ObCollationType ncs_type,
                                               const char *&data,
                                               const common::ObTimeZoneInfo *tz_info,
                                               TypeInfo *type_info,
                                               ObObjParam &param)
{
  int ret = OB_SUCCESS;
  const pl::ObUserDefinedType *pl_type = NULL;
  int64_t param_size = 0, param_pos = 0;
  CK (OB_NOT_NULL(type_info));
  OZ (get_pl_type_by_type_info(allocator, type_info, pl_type));
  CK (OB_NOT_NULL(pl_type));
  OZ (pl_type->init_obj(*(ctx_.schema_guard_), allocator, param, param_size));
  OX (param.set_udt_id(pl_type->get_user_type_id()));
  OZ (pl_type->deserialize(*(ctx_.schema_guard_), allocator, charset, cs_type, ncs_type,
        tz_info, data, reinterpret_cast<char *>(param.get_ext()), param_size, param_pos));
  OX (param.set_need_to_check_extend_type(true));
  return ret;
}

int ObMPStmtExecute::parse_basic_param_value(ObIAllocator &allocator,
                                             const uint32_t type,
                                             const ObCharsetType charset,
                                             const ObCharsetType ncharset,
                                             const ObCollationType cs_type,
                                             const ObCollationType ncs_type,
                                             const char *& data,
                                             const common::ObTimeZoneInfo *tz_info,
                                             ObObj &param,
                                             bool is_complex_element,
                                             ObPSAnalysisChecker *checker,
                                             bool is_unsigned)
{
  int ret = OB_SUCCESS;
  UNUSED(charset);
  switch(type) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG: {
      if (OB_FAIL(parse_integer_value(type, data, param, allocator, is_complex_element, checker, is_unsigned))) {
        LOG_WARN("parse integer value from client failed", K(ret));
      }
      break;
    }
    case MYSQL_TYPE_FLOAT: {
      float value = 0;
      PS_STATIC_DEFENSE_CHECK(checker, sizeof(value))
      {
        MEMCPY(&value, data, sizeof(value));
        data += sizeof(value);
        param.set_float(value);
      }
      break;
    }
    case MYSQL_TYPE_ORA_BINARY_FLOAT: {
      float value = 0;
      PS_STATIC_DEFENSE_CHECK(checker, sizeof(value))
      {
        MEMCPY(&value, data, sizeof(value));
        data += sizeof(value);
        param.set_float(value);
      }
      break;
    }
    case MYSQL_TYPE_DOUBLE: {
      double value = 0;
      PS_STATIC_DEFENSE_CHECK(checker, sizeof(value))
      {
        MEMCPY(&value, data, sizeof(value));
        data += sizeof(value);
        if (lib::is_mysql_mode()) {
          param.set_double(value);
        } else {
          char *buf = NULL;
          int64_t buf_len = 0;
          number::ObNumber nb;
          const int64_t alloc_size = OB_MAX_DOUBLE_FLOAT_DISPLAY_WIDTH;
          if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(alloc_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          } else if (FALSE_IT(buf_len = ob_gcvt_strict(value, OB_GCVT_ARG_DOUBLE, alloc_size,
                                                      buf, NULL, TRUE/*is_oracle_mode*/,
                                                      FALSE/*is_binary_double*/, FALSE))) {
          } else if (OB_FAIL(nb.from_sci_opt(buf, buf_len, allocator))) {
            LOG_WARN("decode double param to number failed", K(ret));
          } else {
            param.set_number(nb);
          }
        }
      }
      break;
    }
    case MYSQL_TYPE_ORA_BINARY_DOUBLE: {
      double value = 0;
      PS_STATIC_DEFENSE_CHECK(checker, sizeof(value))
      {
        MEMCPY(&value, data, sizeof(value));
        data += sizeof(value);
        param.set_double(value);
      }
      break;
    }
    case MYSQL_TYPE_YEAR: {
      int16_t value = 0;
      PS_STATIC_DEFENSE_CHECK(checker, 2)
      {
        ObMySQLUtil::get_int2(data, value);
        param.set_year(static_cast<uint8_t>(value));
      }
      break;
    }
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP: {
      if (OB_FAIL(parse_mysql_timestamp_value(static_cast<EMySQLFieldType>(type), data,
                                              param, tz_info, checker))) {
        LOG_WARN("parse timestamp value from client failed", K(ret));
      }
      break;
    }
    case MYSQL_TYPE_TIME:{
      if (OB_FAIL(parse_mysql_time_value(data, param, checker))) {
        LOG_WARN("parse timestamp value from client failed", K(ret));
      }
      break;
    }
    case MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
    case MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case MYSQL_TYPE_OB_TIMESTAMP_NANO: {
      ObTimeConvertCtx cvrt_ctx(tz_info, true);
      if (OB_FAIL(parse_oracle_timestamp_value(
                            static_cast<EMySQLFieldType>(type), data, cvrt_ctx, param, checker))) {
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
    case MYSQL_TYPE_JSON:
    case MYSQL_TYPE_GEOMETRY: {
      ObString str;
      ObString dst;
      uint64_t length = 0;
      ObCollationType cur_cs_type = ObCharset::get_default_collation(charset);
      ObCollationType cur_ncs_type = ObCollationType::CS_TYPE_INVALID;
      if (ncharset == ObCharsetType::CHARSET_INVALID || ncharset == ObCharsetType::CHARSET_BINARY) {
        cur_ncs_type = ObCharset::get_default_collation(charset);
      } else {
        cur_ncs_type = ObCharset::get_default_collation(ncharset);
      }
      if (OB_FAIL(ObMySQLUtil::get_length(data, length))) {
        LOG_ERROR("decode varchar param value failed", K(ret));
      } else {
        PS_STATIC_DEFENSE_CHECK(checker, length)
        {
          str.assign_ptr(data, static_cast<ObString::obstr_size_t>(length));
        }
        if (OB_FAIL(ret)) {
        } else if (length > OB_MAX_LONGTEXT_LENGTH) {
          ret = OB_ERR_INVALID_INPUT_ARGUMENT;
          LOG_WARN("input param len is over size", K(ret), K(length));
        } else if (MYSQL_TYPE_OB_NVARCHAR2 == type
                  || MYSQL_TYPE_OB_NCHAR == type) {
          OZ(copy_or_convert_str(allocator, cur_ncs_type, ncs_type, str, dst));
          if (OB_SUCC(ret)) {
            MYSQL_TYPE_OB_NVARCHAR2 == type ? param.set_nvarchar2(dst)
                                            : param.set_nchar(dst);
            param.set_collation_type(ncs_type);
          }
          LOG_DEBUG("recieve Nchar param", K(ret), K(str), K(dst));
        } else if (ObURowIDType == type) {
          // decode bae64 str and get urowid content
          ObURowIDData urowid_data;
          if (OB_FAIL(ObURowIDData::decode2urowid(str.ptr(), str.length(),
                                                  allocator, urowid_data))) {
            LOG_WARN("failed to decode to urowid", K(ret));
            if (OB_INVALID_ROWID == ret) {
              LOG_USER_ERROR(OB_INVALID_ROWID);
            }
          } else {
            param.set_urowid(urowid_data);
          }
        } else {
          bool is_lob_v1 = false;
          if (MYSQL_TYPE_STRING == type
              || MYSQL_TYPE_VARCHAR == type
              || MYSQL_TYPE_VAR_STRING == type
              || MYSQL_TYPE_ORA_CLOB == type
              || MYSQL_TYPE_JSON == type
              || MYSQL_TYPE_GEOMETRY == type) {
            int64_t extra_len = 0;
            if (MYSQL_TYPE_ORA_CLOB == type) {
              ObLobLocatorV2 lob(str);
              if (lob.is_lob_locator_v1()) {
                is_lob_v1 = true;
                const ObLobLocator &lobv1 = *(reinterpret_cast<const ObLobLocator *>(str.ptr()));
                if (OB_UNLIKELY(! lobv1.is_valid() || lobv1.get_total_size() != length)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("got invalid ps lob param", K(length), K(lobv1.magic_code_), K(lobv1.table_id_),
                            K(lobv1.column_id_), K(lobv1.payload_offset_), K(lobv1.payload_size_),
                            K(type), K(cs_type), K(lobv1.get_total_size()), K(lobv1.get_data_length()));
                } else {
                  extra_len = str.length() - reinterpret_cast<const ObLobLocator *>(str.ptr())->payload_size_;
                }
              } else {
                if (!lob.is_valid()) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("got invalid ps lob param", K(length), K(lob), K(type), K(cs_type));
                } // if INROW, does it need to do copy_or_convert_str?
              }
            }
            if (is_lob_v1 || MYSQL_TYPE_ORA_CLOB != type) {
              OZ(copy_or_convert_str(allocator,
                                  cur_cs_type,
                                  cs_type,
                                  ObString(str.length() - extra_len, str.ptr() + extra_len),
                                  dst,
                                  extra_len));
            }
            if (OB_SUCC(ret) && MYSQL_TYPE_ORA_CLOB == type) {
              if (is_lob_v1) {
                // copy lob header
                dst.assign_ptr(dst.ptr() - extra_len, dst.length() + extra_len);
                MEMCPY(dst.ptr(), str.ptr(), extra_len);
                reinterpret_cast<ObLobLocator *>(dst.ptr())->payload_size_ = dst.length() - extra_len;
              } else {
                if (OB_FAIL(ob_write_string(allocator, str, dst))) {
                  LOG_WARN("Failed to write str", K(ret));
                }
              }
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
            } else if (MYSQL_TYPE_ORA_BLOB == type
                      || MYSQL_TYPE_ORA_CLOB == type) {
              if (MYSQL_TYPE_ORA_BLOB == type) {
                param.set_collation_type(CS_TYPE_BINARY);
              } else {
                param.set_collation_type(cs_type);
              }
              ObLobLocatorV2 lobv2(str);
              if (lobv2.is_lob_locator_v1()) {
                const ObLobLocator &lob = *(reinterpret_cast<const ObLobLocator *>(dst.ptr()));
                if (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0 && lob.get_payload_length() == 0) {
                  // do convert empty lob v1 to v2
                  ObString payload;
                  if (OB_FAIL(lob.get_payload(payload))) {
                    LOG_WARN("fail to get payload", K(ret), K(lob));
                  } else {
                    param.set_lob_value(ObLongTextType, payload.ptr(), payload.length());
                    if (OB_FAIL(ObTextStringResult::ob_convert_obj_temporay_lob(param, allocator))) {
                      LOG_WARN("Fail to convert plain lob data to templob",K(ret), K(payload));
                    } else {
                      LOG_TRACE("convert empty lob v1 to v2", K(lob), K(cs_type), K(type));
                    }
                  }
                } else {
                  param.set_lob_locator(lob);
                  param.set_has_lob_header();
                  LOG_TRACE("get lob locator", K(lob), K(cs_type), K(type));
                }
              } else {
                param.set_lob_value(ObLongTextType, dst.ptr(), dst.length());
                param.set_has_lob_header();
                LOG_TRACE("get lob locator v2", K(lobv2), K(cs_type), K(type));
              }
            } else if (MYSQL_TYPE_TINY_BLOB == type
                      || MYSQL_TYPE_MEDIUM_BLOB == type
                      || MYSQL_TYPE_BLOB == type
                      || MYSQL_TYPE_LONG_BLOB == type
                      || MYSQL_TYPE_JSON == type
                      || MYSQL_TYPE_GEOMETRY == type) {
              // in ps protocol:
              //    Oracle mode: client driver will call hextoraw()
              //    MySQL mode: no need to call hextoraw
              // in text protocol:
              //    Oracle mode: server will call hextoraw()
              //    MySQL mode: no need to call hextoraw
              // Notice: text tc without lob header here, should not set has_lob_header flag here
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
              } else if (MYSQL_TYPE_GEOMETRY == type) {
                param.set_geometry_value(ObGeometryType, dst.ptr(), dst.length());
              }
              if (OB_SUCC(ret) && param.is_lob_storage() && dst.length() > 0) {
                if (OB_FAIL(ObTextStringResult::ob_convert_obj_temporay_lob(param, allocator))) {
                  LOG_WARN("Fail to convert plain lob data to templob",K(ret));
                }
              }
            } else {
              param.set_collation_type(cs_type);
              if (is_oracle_mode() && !is_complex_element) {
                param.set_char(dst);
              } else {
                if (is_complex_element && dst.length()== 0) {
                  param.set_null();
                } else {
                  param.set_varchar(dst);
                }
              }
            }
          }
        }
      }
      data += length;
      break;
    }
    case MYSQL_TYPE_OB_INTERVAL_YM: {
      if (OB_FAIL(parse_oracle_interval_ym_value(data, param, checker))) {
        LOG_WARN("failed to parse oracle interval year to month value", K(ret));
      }
      break;
    }
    case MYSQL_TYPE_OB_INTERVAL_DS:{
      if (OB_FAIL(parse_oracle_interval_ds_value(data, param, checker))) {
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
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    param.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObMPStmtExecute::parse_param_value(ObIAllocator &allocator,
                                       const uint32_t type,
                                       const ObCharsetType charset,
                                       const ObCharsetType ncharset,
                                       const ObCollationType cs_type,
                                       const ObCollationType ncs_type,
                                       const char *&data,
                                       const common::ObTimeZoneInfo *tz_info,
                                       TypeInfo *type_info,
                                       ObObjParam &param,
                                       const char *bitmap,
                                       int64_t param_id)
{
  int ret = OB_SUCCESS;
  uint64_t length = 0;
  uint64_t count = 1;
  common::ObFixedArray<ObSqlString, ObIAllocator>
                str_buf(THIS_WORKER.get_sql_arena_allocator());
  ObPieceCache *piece_cache = NULL == ctx_.session_info_
                                ? NULL
                                : static_cast<ObPieceCache*>(ctx_.session_info_->get_piece_cache());
  ObPiece *piece = NULL;
  if (OB_NOT_NULL(piece_cache) && OB_FAIL(piece_cache->get_piece(stmt_id_, param_id, piece))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get piece fail.", K(ret));
  } else if (OB_ISNULL(piece_cache) || OB_ISNULL(piece)) {
    // send piece data will init piece cache
    // if piece cache is null, it must not be send piece protocol
    bool is_null = ObSMUtils::update_from_bitmap(param, bitmap, param_id);
    if (is_null) {
      LOG_DEBUG("param is null", K(param_id), K(param), K(type));
    } else if (OB_UNLIKELY(MYSQL_TYPE_COMPLEX == type)) {
      if (OB_FAIL(parse_complex_param_value(allocator, charset, cs_type, ncs_type,
                                            data, tz_info, type_info,
                                            param))) {
        LOG_WARN("failed to parse complex value", K(ret));
      }
    } else if (OB_UNLIKELY(MYSQL_TYPE_CURSOR == type)) {
      CK (OB_NOT_NULL(ctx_.session_info_));
      if (OB_SUCC(ret)) {
        ObPLCursorInfo *cursor = NULL;
        // OZ (ctx_.session_info_->make_cursor(cursor));
        OX (param.set_extend(reinterpret_cast<int64_t>(cursor), PL_CURSOR_TYPE));
        OX (param.set_param_meta());
      }
    } else {
      bool is_unsigned = NULL == type_info || !type_info->elem_type_.get_meta_type().is_unsigned_integer() ? false : true;
      if (OB_FAIL(parse_basic_param_value(allocator, type, charset, ncharset, cs_type, ncs_type,
                                          data, tz_info, param, false, &analysis_checker_, is_unsigned))) {
        LOG_WARN("failed to parse basic param value", K(ret));
      } else {
        param.set_param_meta();
      }
    }
  } else if (!support_send_long_data(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this type is not support send long data.", K(type), K(ret));
  } else if (NULL == piece->get_allocator()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece allocator is null.", K(stmt_id_), K(param_id), K(ret));
  } else if (OB_SUCCESS != piece->get_error_ret()) {
    ret = piece->get_error_ret();
    LOG_WARN("send long data has error. ", K(stmt_id_), K(param_id), K(ret));
  } else {
    if (OB_UNLIKELY(MYSQL_TYPE_COMPLEX == type)) {
      // this must be array bounding.
      bool is_null = ObSMUtils::update_from_bitmap(param, bitmap, param_id);
      if (is_null) {
        LOG_DEBUG("param is null", K(param_id), K(param), K(type));
      } else {
        // 1. read count
        ObMySQLUtil::get_length(data, count);
        // 2. make null map
        int64_t bitmap_bytes = ((count + 7) / 8);
        char is_null_map[bitmap_bytes];
        MEMSET(is_null_map, 0, bitmap_bytes);
        length = piece_cache->get_length_length(count) + bitmap_bytes;
        // 3. get string buffer (include lenght + value)
        if (OB_FAIL(str_buf.prepare_allocate(count))) {
          LOG_WARN("prepare fail.");
        } else if (OB_FAIL(piece_cache->get_buffer(stmt_id_,
                                                  param_id,
                                                  count,
                                                  length,
                                                  str_buf,
                                                  is_null_map))) {
          LOG_WARN("piece get buffer fail.", K(ret), K(stmt_id_), K(param_id));
        } else {
          // 4. merge all this info
          char *tmp = static_cast<char*>(piece->get_allocator()->alloc(length));
          int64_t pos = 0;
          if (OB_ISNULL(tmp)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (FALSE_IT(MEMSET(tmp, 0, length))) {
          } else if (OB_FAIL(ObMySQLUtil::store_length(tmp, length, count, pos))) {
            LOG_WARN("store length fail.", K(ret), K(stmt_id_), K(param_id));
          } else {
            MEMCPY(tmp+pos, is_null_map, bitmap_bytes);
            pos += bitmap_bytes;
            for (int64_t i=0; OB_SUCC(ret) && i<count; i++) {
              if (OB_FAIL(ObMySQLUtil::store_obstr(tmp, length, str_buf.at(i).string(), pos))) {
                LOG_WARN("store string fail.", K(ret), K(stmt_id_), K(param_id),
                        K(length), K(pos), K(i), K(str_buf.at(i).string()), K(str_buf.at(i).string().length()),
                        K(str_buf.at(i).length()));
              }
            }
          }
          if (OB_FAIL(ret)) {
            // do nothing.
          } else {
            const char* src = tmp;
            if (OB_FAIL(parse_complex_param_value(allocator, charset, cs_type, ncs_type,
                                                  src, tz_info, type_info,
                                                  param))) {
              LOG_WARN("failed to parse complex value", K(ret));
            }
          }
          piece->get_allocator()->free(tmp);
        }
      }
    } else {
      if (OB_FAIL(str_buf.prepare_allocate(count))) {
        LOG_WARN("prepare fail.");
      } else if (OB_FAIL(piece_cache->get_buffer(stmt_id_,
                                                  param_id,
                                                  count,
                                                  length,
                                                  str_buf,
                                                  NULL))) {
        LOG_WARN("piece get buffer fail.", K(ret), K(stmt_id_), K(param_id));
      } else {
        char *tmp = static_cast<char*>(piece->get_allocator()->alloc(length));
        int64_t pos = 0;
        if (OB_ISNULL(tmp)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else if (FALSE_IT(MEMSET(tmp, 0, length))) {
        } else if (OB_FAIL(ObMySQLUtil::store_obstr(tmp, length, str_buf.at(0).string(), pos))) {
          LOG_WARN("store string fail.", K(ret), K(stmt_id_), K(param_id));
        } else {
          const char* src = tmp;
          bool is_unsigned = NULL == type_info || !type_info->elem_type_.get_meta_type().is_unsigned_integer() ? false : true;
          if (OB_FAIL(parse_basic_param_value(allocator, type, charset, ncharset, cs_type, ncs_type,
                                              src, tz_info, param, false, NULL ,is_unsigned))) {
            LOG_WARN("failed to parse basic param value", K(ret));
          } else {
            param.set_param_meta();
          }
        }
        piece->get_allocator()->free(tmp);
      }
    }
  }
  return ret;
}



int ObMPStmtExecute::copy_or_convert_str(common::ObIAllocator &allocator,
                                         const ObCollationType src_type,
                                         const ObCollationType dst_type,
                                         const ObString &src,
                                         ObString &out,
                                         int64_t extra_buf_len /* = 0 */)
{
  int ret = OB_SUCCESS;
  if (!ObCharset::is_valid_collation(src_type) || !ObCharset::is_valid_collation(dst_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid collation", K(ret), K(dst_type));
  } else if (0 == src.length()
             || ObCharset::charset_type_by_coll(src_type)
                == ObCharset::charset_type_by_coll(dst_type)) {
    int64_t len = src.length() + extra_buf_len;
    if (len > 0) {
      char *buf = static_cast<char *>(allocator.alloc(len));
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
      char *buf = static_cast<char *>(allocator.alloc(len));
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate failed", K(ret), K(len));
      } else {
        ObDataBuffer buf_alloc(buf + extra_buf_len, len - extra_buf_len);
        if (OB_FAIL(ObCharset::charset_convert(buf_alloc,
                                               src,
                                               src_type,
                                               dst_type,
                                               out,
                                               ObCharset::REPLACE_UNKNOWN_CHARACTER))) {
          LOG_WARN("fail to charset convert", K(ret), K(src_type), K(dst_type),
          K(src), K(len), K(extra_buf_len));
        }
      }
    }
  }
  return ret;
}

int ObMPStmtExecute::parse_integer_value(const uint32_t type,
                                         const char *&data,
                                         ObObj &param,
                                         ObIAllocator &allocator,
                                         bool is_complex_element,
                                         ObPSAnalysisChecker *checker,
                                         bool is_unsigned) // oracle unsigned need
{
  int ret = OB_SUCCESS;
  bool cast_to_number = !(lib::is_mysql_mode() || is_complex_element || MYSQL_TYPE_TINY == type);
  int64_t res_val = 0;
  switch(type) {
    case MYSQL_TYPE_TINY: {
      PS_STATIC_DEFENSE_CHECK(checker, 1)
      {
        int8_t value;
        ObMySQLUtil::get_int1(data, value);
        is_unsigned ? param.set_utinyint(value) : param.set_tinyint(value);
      }
      break;
    }
    case MYSQL_TYPE_SHORT: {
      PS_STATIC_DEFENSE_CHECK(checker, 2)
      {
        int16_t value = 0;
        ObMySQLUtil::get_int2(data, value);
        if (!cast_to_number) {
          is_unsigned ? param.set_usmallint(value) : param.set_smallint(value);
        } else {
          res_val = static_cast<int64_t>(value);
          if (is_unsigned) {
            if (((1LL << 16) + res_val) < 1 || res_val > 0xFFFF) {
              ret = OB_DECIMAL_OVERFLOW_WARN;
              LOG_WARN("param is over flower.", K(res_val), K(type), K(ret));
            } else {
              res_val = res_val < 0 ? ((1LL << 16) + res_val) : res_val;
            }
          }
        }
      }
      break;
    }
    case MYSQL_TYPE_LONG: {
      PS_STATIC_DEFENSE_CHECK(checker, 4)
      {
        int32_t value = 0;
        ObMySQLUtil::get_int4(data, value);
        if (!cast_to_number) {
          is_unsigned ? param.set_uint32(value) : param.set_int32(value);
        } else {
          res_val = static_cast<int64_t>(value);
          if (is_unsigned) {
            if (((1LL << 32) + res_val) < 1 || res_val > 0xFFFFFFFF) {
              ret = OB_DECIMAL_OVERFLOW_WARN;
              LOG_WARN("param is over flower.", K(res_val), K(type), K(ret));
            } else {
              res_val = res_val < 0 ? ((1LL << 32) + res_val) : res_val;
            }
          }
        }
      }
      break;
    }
    case MYSQL_TYPE_LONGLONG: {
      PS_STATIC_DEFENSE_CHECK(checker, 8)
      {
        int64_t value = 0;
        ObMySQLUtil::get_int8(data, value);
        if (!cast_to_number) {
          is_unsigned ? param.set_uint(ObUInt64Type, value) : param.set_int(value);
        } else {
          res_val = value;
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected integer type", K(type), K(ret));
      break;
    }
  }
  if (OB_SUCC(ret) && cast_to_number) {
    number::ObNumber nb;
    if (is_unsigned && OB_FAIL(nb.from(static_cast<uint64_t>(res_val), allocator))) {
      LOG_WARN("decode param to number failed", K(ret), K(res_val));
    } else if (!is_unsigned && OB_FAIL(nb.from(static_cast<int64_t>(res_val), allocator))) {
      LOG_WARN("decode param to number failed", K(ret), K(res_val));
    } else {
      param.set_number(nb);
    }
  }
  return ret;
}

int ObMPStmtExecute::parse_mysql_timestamp_value(const EMySQLFieldType field_type,
                                                 const char *&data,
                                                 ObObj &param,
                                                 const common::ObTimeZoneInfo *tz_info,
                                                 ObPSAnalysisChecker *checker)
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
  ObPreciseDateTime value;
  PS_STATIC_DEFENSE_CHECK(checker, 1)
  {
    ObMySQLUtil::get_int1(data, length);
    if (0 == length) {
      value = 0;
    } else if (4 == length) {
      PS_STATIC_DEFENSE_CHECK(checker, 4)
      {
        ObMySQLUtil::get_int2(data, year);
        ObMySQLUtil::get_int1(data, month);
        ObMySQLUtil::get_int1(data, day);
      }
    } else if (7 == length) {
      PS_STATIC_DEFENSE_CHECK(checker, 7)
      {
        ObMySQLUtil::get_int2(data, year);
        ObMySQLUtil::get_int1(data, month);
        ObMySQLUtil::get_int1(data, day);
        ObMySQLUtil::get_int1(data, hour);
        ObMySQLUtil::get_int1(data, min);
        ObMySQLUtil::get_int1(data, second);
      }
    } else if (11 == length) {
      PS_STATIC_DEFENSE_CHECK(checker, 11)
      {
        ObMySQLUtil::get_int2(data, year);
        ObMySQLUtil::get_int1(data, month);
        ObMySQLUtil::get_int1(data, day);
        ObMySQLUtil::get_int1(data, hour);
        ObMySQLUtil::get_int1(data, min);
        ObMySQLUtil::get_int1(data, second);
        ObMySQLUtil::get_int4(data, microsecond);
      }
    } else {
      ret = OB_ERROR;
      LOG_WARN("invalid mysql timestamp value length", K(length));
    }
  }

  if (OB_SUCC(ret)) {
    ObTime ob_time;
    if (0 != length) {
      if (lib::is_oracle_mode()) {
        //oracle mode datetime should not has microsecond
        microsecond = 0;
      }
      ob_time.parts_[DT_YEAR] = year;
      ob_time.parts_[DT_MON] = month;
      ob_time.parts_[DT_MDAY] = day;
      ob_time.parts_[DT_HOUR] = hour;
      ob_time.parts_[DT_MIN] = min;
      ob_time.parts_[DT_SEC] = second;
      ob_time.parts_[DT_USEC] = microsecond;
      if (!ObTimeUtility2::is_valid_date(year, month, day)
          || !ObTimeUtility2::is_valid_time(hour, min, second, microsecond)) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("invalid date format", K(ret));
      } else {
        ObTimeConvertCtx cvrt_ctx(NULL, false);
        ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
        if (field_type == MYSQL_TYPE_DATE) {
          value = ob_time.parts_[DT_DATE];
        } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, value))){
          LOG_WARN("convert obtime to datetime failed", K(value), K(year), K(month),
                   K(day), K(hour), K(min), K(second));
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
  LOG_DEBUG("get datetime", K(length), K(year), K(month), K(day), K(hour), K(min),K(second),  K(microsecond), K(value));
  return ret;
}

int ObMPStmtExecute::parse_oracle_timestamp_value(const obmysql::EMySQLFieldType field_type,
    const char *&data, const ObTimeConvertCtx &cvrt_ctx, ObObj &param, ObPSAnalysisChecker *checker)
{
  int ret = OB_SUCCESS;
  int8_t total_len = 0;
  ObObjType obj_type;
  ObOTimestampData ot_data;
  int8_t scale = -1;
  PS_STATIC_DEFENSE_CHECK(checker, 1)
  {
    ObMySQLUtil::get_int1(data, total_len);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSMUtils::get_ob_type(obj_type, field_type))) {
    LOG_WARN("failed to get_ob_type", K(ret));
  } else if (OB_FAIL(ObTimeConverter::decode_otimestamp(obj_type, data, total_len, cvrt_ctx, ot_data, scale))) {
    LOG_WARN("failed to decode_otimestamp", K(ret));
  } else {
    PS_STATIC_DEFENSE_CHECK(checker, total_len)
    {
      data += total_len;
      param.set_otimestamp_value(obj_type, ot_data);
      param.set_scale(scale);
    }
  }
  return ret;
}

int ObMPStmtExecute::parse_mysql_time_value(const char *&data, ObObj &param, ObPSAnalysisChecker *checker)
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
  int64_t value;
  PS_STATIC_DEFENSE_CHECK(checker, 1)
  {
    ObMySQLUtil::get_int1(data, length);
    if (0 == length) {
      value = 0;
    } else if (8 == length) {
      PS_STATIC_DEFENSE_CHECK(checker, 8)
      {
        ObMySQLUtil::get_int1(data, is_negative);
        ObMySQLUtil::get_int4(data, day);
        ObMySQLUtil::get_int1(data, hour);
        ObMySQLUtil::get_int1(data, min);
        ObMySQLUtil::get_int1(data, second);
      }
    } else if (12 == length) {
      PS_STATIC_DEFENSE_CHECK(checker, 12)
      {
        ObMySQLUtil::get_int1(data, is_negative);
        ObMySQLUtil::get_int4(data, day);
        ObMySQLUtil::get_int1(data, hour);
        ObMySQLUtil::get_int1(data, min);
        ObMySQLUtil::get_int1(data, second);
        ObMySQLUtil::get_int4(data, microsecond);
      }
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
  }
  if (OB_SUCC(ret)) {
    param.set_time(value);
  }
  LOG_INFO("get time", K(length), K(year), K(month), K(day), K(hour), K(min),K(second),  K(microsecond), K(value));
  return ret;
}

int ObMPStmtExecute::parse_oracle_interval_ds_value(const char *&data, ObObj &param, ObPSAnalysisChecker *checker)
{
  int ret = OB_SUCCESS;
  int8_t length = 0;
  ObScale scale = 0;
  ObIntervalDSValue value;

  ObMySQLUtil::get_int1(data, length);
  PS_STATIC_DEFENSE_CHECK(checker, length)
  {
    if (OB_FAIL(ObTimeConverter::decode_interval_ds(data, length, value, scale))) {
      LOG_WARN("fail to decode interval day to second", K(ret), K(length));
    } else {
      param.set_interval_ds(value);
      param.set_scale(scale);
    }
  }

  return ret;
}

int ObMPStmtExecute::parse_oracle_interval_ym_value(const char *&data, ObObj &param, ObPSAnalysisChecker *checker)
{
  int ret = OB_SUCCESS;
  int8_t length = 0;
  ObScale scale = 0;
  ObIntervalYMValue value;

  ObMySQLUtil::get_int1(data, length);
  PS_STATIC_DEFENSE_CHECK(checker, length)
  {
    if (OB_FAIL(ObTimeConverter::decode_interval_ym(data, length, value, scale))) {
      LOG_WARN("fail to decode interval year to month", K(ret), K(length));
    } else {
      param.set_interval_ym(value);
      param.set_scale(scale);
    }
  }

  return ret;
}

void ObMPStmtExecute::record_stat(const stmt::StmtType type, const int64_t end_time) const
{
#define ADD_STMT_STAT(type)                     \
  case stmt::T_##type:                          \
    EVENT_INC(SQL_##type##_COUNT);              \
    EVENT_ADD(SQL_##type##_TIME, time_cost);    \
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

int ObMPStmtExecute::response_query_header(ObSQLSessionInfo &session, pl::ObDbmsCursorInfo &cursor)
{
  int ret = OB_SUCCESS;
  ObSyncPlanDriver drv(gctx_,
                           ctx_,
                           session,
                           retry_ctrl_,
                           *this,
                           false,
                           OB_INVALID_COUNT);
  if (0 == cursor.get_field_columns().count()) {
    // SELECT * INTO OUTFILE return null field, and only response ok packet
    ObOKPParam ok_param;
    ok_param.affected_rows_ = 0;
    ok_param.is_partition_hit_ = session.partition_hit().get_bool();
    ok_param.has_more_result_ = false;
    if (OB_FAIL(send_ok_packet(session, ok_param))) {
      LOG_WARN("fail to send ok packt", K(ok_param), K(ret));
    }
  } else {
    if (OB_FAIL(drv.response_query_header(cursor.get_field_columns(),
                                          false,
                                          false,
                                          true))) {
      LOG_WARN("fail to get autocommit", K(ret));
    }
  }
  return ret;
}

} //end of namespace observer
} //end of namespace oceanbase
