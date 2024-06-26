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

#include "ob_query_driver.h"
#include "ob_mysql_result_set.h"
#include "obmp_base.h"
#include "obsm_row.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "rpc/obmysql/packet/ompk_resheader.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include <string.h>
#include "share/ob_lob_access_utils.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/expr/ob_expr_sql_udt_utils.h"
#include "observer/mysql/obmp_stmt_prexecute.h"
#include "lib/xml/ob_multi_mode_interface.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace obmysql;
namespace observer
{

int ObQueryDriver::response_query_header(ObResultSet &result,
                                         bool has_more_result,
                                         bool need_set_ps_out_flag,
                                         bool need_flush_buffer)
{
  int ret = OB_SUCCESS;
  if (is_prexecute_) {
    // 二合一协议发送 header 包, 不单独发送 column
    if (OB_FAIL(static_cast<ObMPStmtPrexecute&>(sender_).response_query_header(session_, result, need_flush_buffer))) {
      LOG_WARN("prexecute response query head fail. ", K(ret));
    }
  } else {
    if (NULL == result.get_field_columns()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("response field is null. ", K(ret));
    } else if (OB_FAIL(response_query_header(*result.get_field_columns(),
                                             has_more_result,
                                             need_set_ps_out_flag,
                                             false,
                                             &result))) {
      LOG_WARN("response query head fail. ", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    result.set_errcode(ret);
  }
  return ret;
}

int ObQueryDriver::response_query_header(const ColumnsFieldIArray &fields,
                                         bool has_more_result,
                                         bool need_set_ps_out_flag,
                                         bool ps_cursor_execute,
                                         ObResultSet *result)
{
  int ret = OB_SUCCESS;
  bool ac = true;
  // result == null means ps cursor in execute or fetch .
  if (NULL != result && (&fields != result->get_field_columns())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filed is not from result in non ps cursor mode. ", K(ret));
  } else if (fields.count() <= 0) {
    LOG_WARN("column cnt is null ", K(fields.count()));
    ret = OB_ERR_BAD_FIELD_ERROR;
  } else if (OB_FAIL(session_.get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else if (!(NULL != result && result->get_is_com_filed_list())) {
    // 普通协议发送 cnt 值
    OMPKResheader rhp;
    rhp.set_field_count(fields.count());
    if (OB_FAIL(sender_.response_packet(rhp, &session_))) {
      LOG_WARN("response packet fail", K(ret));
    }
  } else {
    // com field 协议在这里什么都不发，直接发送 field 信息
  }

  // 发送 field 信息
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
      bool is_not_match = false;
      ObMySQLField field;
      const ObField &ob_field = fields.at(i);
      if (NULL != result && result->get_is_com_filed_list()
                         && OB_FAIL(is_com_filed_list_match_wildcard_str(
                                                  *result,
                                                  static_cast<ObCollationType>(ob_field.charsetnr_),
                                                  ob_field.org_cname_,
                                                  is_not_match))) {
        LOG_WARN("failed to is com filed list match wildcard str", K(ret));
      } else if (is_not_match) {
        /*do nothing*/
      } else {
        ret = ObMySQLResultSet::to_mysql_field(ob_field, field);
        if (OB_SUCC(ret)) {
          ObMySQLResultSet::replace_lob_type(session_, ob_field, field);
          if (NULL != result && result->get_is_com_filed_list()) {
            field.default_value_ = static_cast<EMySQLFieldType>(ob_field.default_value_.get_ext());
          }
          OMPKField fp(field);
          if (OB_FAIL(sender_.response_packet(fp, &session_))) {
            LOG_WARN("response packet fail", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    OMPKEOF eofp;
    eofp.set_warning_count(0);
    ObServerStatusFlags flags = eofp.get_server_status();
    flags.status_flags_.OB_SERVER_STATUS_IN_TRANS
      = (session_.is_server_status_in_transaction() ? 1 : 0);
    flags.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = (ac ? 1 : 0);
    flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = has_more_result;
    flags.status_flags_.OB_SERVER_PS_OUT_PARAMS = need_set_ps_out_flag ? 1 : 0;
    // NULL == result 说明是老协议 ps cursor execute 回包，或者fetch 协议回包， cursor_exit = true
    flags.status_flags_.OB_SERVER_STATUS_CURSOR_EXISTS = NULL == result ? 1 : 0;
    if (!session_.is_obproxy_mode()) {
      // in java client or others, use slow query bit to indicate partition hit or not
      flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !session_.partition_hit().get_bool();
    }
    eofp.set_server_status(flags);

    if (ps_cursor_execute && sender_.need_send_extra_ok_packet()) {
      // 老协议 ps cursor execute 回包， 只回 field 信息， 所以对于 proxy ， 需要额外回一个 OK包
      // 但是由于 2.0 协议需要在回 OK 包的同时了解 EOF 包的情况，所以这个 OK 包没办法抽到 execute 协议层处理
      if (OB_FAIL(sender_.update_last_pkt_pos())) {
        LOG_WARN("failed to update last packet pos", K(ret));
      } else {
        // in multi-stmt, send extra ok packet in the last stmt(has no more result)
        ObOKPParam ok_param;
        ok_param.affected_rows_ = 0;
        ok_param.is_partition_hit_ = session_.partition_hit().get_bool();
        ok_param.has_more_result_ = false;
        if (OB_FAIL(sender_.send_ok_packet(session_, ok_param, &eofp))) {
          LOG_WARN("fail to send ok packt", K(ok_param), K(ret));
        }
      }
    } else {
      if (OB_FAIL(sender_.response_packet(eofp, &session_))) {
        LOG_WARN("response packet fail", K(ret));
      }
    }
  }
  return ret;
}

int ObQueryDriver::response_query_result(ObResultSet &result,
                                         bool is_ps_protocol,
                                         bool has_more_result,
                                         bool &can_retry,
                                         int64_t fetch_limit)
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(response_result);
  can_retry = true;
  bool is_first_row = true;
  const ObNewRow *result_row = NULL;
  bool has_top_limit = result.get_has_top_limit();
  bool is_cac_found_rows =  result.is_calc_found_rows();
  int64_t limit_count = OB_INVALID_COUNT == fetch_limit ? INT64_MAX : fetch_limit;
  int64_t row_num = 0;
  ObSqlCtx *sql_ctx = result.get_exec_context().get_sql_ctx();
  if (!has_top_limit && OB_INVALID_COUNT == fetch_limit) {
    limit_count = INT64_MAX;
    if (!lib::is_oracle_mode()) {
      if (OB_FAIL(session_.get_sql_select_limit(limit_count))) {
        LOG_WARN("fail tp get sql select limit", K(ret));
      }
    }
  }
  bool is_packed = result.get_physical_plan() ? result.get_physical_plan()->is_packed() : false;
  MYSQL_PROTOCOL_TYPE protocol_type = is_ps_protocol ? MYSQL_PROTOCOL_TYPE::BINARY : MYSQL_PROTOCOL_TYPE::TEXT;
  const common::ColumnsFieldIArray *fields = NULL;
  if (OB_SUCC(ret)) {
    fields = result.get_field_columns();
    if (OB_ISNULL(fields)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fields is null", K(ret), KP(fields));
    }
  }
  while (OB_SUCC(ret) && row_num < limit_count && !OB_FAIL(result.get_next_row(result_row)) ) {
    ObNewRow *row = const_cast<ObNewRow*>(result_row);
    if (is_prexecute_ && row_num == limit_count - 1) {
      LOG_DEBUG("is_prexecute_ and row_num is equal with limit_count", K(limit_count));
      break;
    }
    // 如果是第一行，则先给客户端回复field等信息
    if (is_first_row) {
      is_first_row = false;
      can_retry = false; // 已经获取到第一行数据，不再重试了
#ifdef OB_BUILD_SPM
      if (OB_NOT_NULL(result.get_exec_context().get_physical_plan_ctx()) &&
          OB_NOT_NULL(sql_ctx) && sql_ctx->spm_ctx_.need_spm_timeout_) {
        LOG_TRACE("reset to origin timeout because result is returning to user");
        result.get_exec_context().get_physical_plan_ctx()->set_spm_timeout_timestamp(0);
      }
#endif
      if (OB_FAIL(response_query_header(result, has_more_result, false))) {
        LOG_WARN("fail to response query header", K(ret), K(row_num), K(can_retry));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row->get_count(); i++) {
      ObObj& value = row->get_cell(i);
      if (result.is_ps_protocol() && !is_packed
          && !(value.is_geometry() && lib::is_oracle_mode())) { // oracle gis will do cast in process_sql_udt_results
        if (value.get_type() != fields->at(i).type_.get_type()) {
          ObCastCtx cast_ctx(&result.get_mem_pool(), NULL, CM_WARN_ON_FAIL,
            fields->at(i).type_.get_collation_type());
          if (OB_FAIL(common::ObObjCaster::to_type(fields->at(i).type_.get_type(),
                                           cast_ctx,
                                           value,
                                           value))) {
            LOG_WARN("failed to cast object", K(ret), K(value),
                     K(value.get_type()), K(fields->at(i).type_.get_type()));
          }
        }
      }
      if (OB_SUCC(ret) && !is_packed) {
        // cluster version < 4.1
        //    use only locator and response routine
        // >= 4.1 for oracle modle
        //    1. user full lob locator v2 with extern header if client supports locator
        //    2. remove locator if client does not support locator
        // >= 4.1 for mysql modle
        //    remove locator
        if (ob_is_string_tc(value.get_type())
            && CS_TYPE_INVALID != value.get_collation_type()) {
          OZ(convert_string_value_charset(value, result));
        } else if (value.is_clob_locator()
                    && OB_FAIL(convert_lob_value_charset(value, result))) {
          LOG_WARN("convert lob value charset failed", K(ret));
        } else if (ob_is_text_tc(value.get_type())
                    && OB_FAIL(convert_text_value_charset(value, result))) {
          LOG_WARN("convert text value charset failed", K(ret));
        }
        if (OB_FAIL(ret)){
        } else if ((value.is_lob() || value.is_lob_locator() || value.is_json() || value.is_geometry() || value.is_roaringbitmap())
                  && OB_FAIL(process_lob_locator_results(value, result))) {
          LOG_WARN("convert lob locator to longtext failed", K(ret));
        } else if ((value.is_user_defined_sql_type() || value.is_collection_sql_type() || value.is_geometry()) &&
                   OB_FAIL(ObXMLExprHelper::process_sql_udt_results(value, result))) {
          LOG_WARN("convert udt to client format failed", K(ret), K(value.get_udt_subschema_id()));
        }
      }
    }
    if (OB_SUCC(ret)) {
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(&session_);
      ObSMRow sm(protocol_type, *row, dtc_params,
                         result.get_field_columns(),
                         ctx_.schema_guard_,
                         session_.get_effective_tenant_id());
      sm.set_packed(is_packed);
      OMPKRow rp(sm);
      rp.set_is_packed(is_packed);
      if (OB_FAIL(sender_.response_packet(rp, &result.get_session()))) {
        LOG_WARN("response packet fail", K(ret), KP(row), K(row_num),
            K(can_retry));
        // break;
      } else {
        LOG_DEBUG("response row succ", K(*row));
      }
      if (OB_SUCC(ret)) {
        ++row_num;
        if (0 == row_num % RESET_CONVERT_CHARSET_ALLOCATOR_EVERY_X_ROWS) {
          (void) result.get_exec_context().try_reset_convert_charset_allocator();
        }
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
    // 如果是一行数据也没有，则还是要给客户端回复field等信息，并且不再重试了
    can_retry = false;
    if (OB_FAIL(response_query_header(result, has_more_result, false))) {
      LOG_WARN("fail to response query header", K(ret), K(row_num), K(can_retry));
    }
  }
  if (OB_FAIL(ret) && !can_retry) {
    FLOG_INFO("The query has already returned partial results to the client and cannot be retried", KR(ret));
  }

  return ret;
}

int ObQueryDriver::convert_field_charset(ObIAllocator& allocator,
                                         const ObCollationType& from_collation,
                                         const ObCollationType& dest_collation,
                                         const ObString &from_string,
                                         ObString &dest_string)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int32_t buf_len = from_string.length() * ObCharset::CharConvertFactorNum;
  uint32_t result_len = 0;
  if (0 == buf_len) {
  } else if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(
            allocator.alloc(buf_len))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret));
  } else if (OB_FAIL(ObCharset::charset_convert(
          static_cast<ObCollationType>(from_collation),
          from_string.ptr(),
          from_string.length(),
          dest_collation,
          buf,
          buf_len,
          result_len))) {
    LOG_WARN("charset convert failed", K(ret), K(from_collation), K(dest_collation));
  } else {
    dest_string.assign(buf, static_cast<int32_t>(result_len));
  }
  return ret;
}

int ObQueryDriver::convert_string_value_charset(ObObj& value, ObResultSet &result)
{
  int ret = OB_SUCCESS;
  ObCharsetType charset_type = CHARSET_INVALID;
  const ObSQLSessionInfo &my_session = result.get_session();
  ObArenaAllocator *allocator = NULL;
  ObCollationType from_collation_type = value.get_collation_type();
  ObCollationType to_collation_type = CS_TYPE_INVALID;
  ObCharsetType nchar = CHARSET_INVALID;
  if (OB_FAIL(my_session.get_ncharacter_set_connection(nchar))) {
    LOG_WARN("get ncharacter set connection failed", K(ret));
  } else if (lib::is_oracle_mode()
            && (value.is_nchar() || value.is_nvarchar2())
            && nchar != CHARSET_INVALID
            && nchar != CHARSET_BINARY) {
    to_collation_type = ObCharset::get_default_collation(nchar);
    charset_type = nchar;
  } else if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
  } else {
    to_collation_type = ObCharset::get_default_collation(charset_type);
  }
  if (OB_FAIL(ret)) {
  } else if (from_collation_type == to_collation_type) {
    const ObCharsetInfo *charset_info = ObCharset::get_charset(from_collation_type);
    if (OB_ISNULL(charset_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("charsetinfo is null", K(ret), K(from_collation_type), K(to_collation_type), K(value));
    } else if (CS_TYPE_INVALID == from_collation_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid collation", K(ret), K(from_collation_type), K(to_collation_type), K(value));
    }
  } else if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
    LOG_WARN("fail to get lob fake allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob fake allocator is null.", K(ret), K(value));
  } else {
    OZ (value.convert_string_value_charset(charset_type, *allocator));
  }
  return ret;
}

int ObQueryDriver::convert_string_value_charset(ObObj& value, 
                                                ObCharsetType charset_type, 
                                                ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString str;
  value.get_string(str);
  if (ObCharset::is_valid_charset(charset_type) && CHARSET_BINARY != charset_type) {
    ObCollationType collation_type = ObCharset::get_default_collation(charset_type);
    const ObCharsetInfo *from_charset_info = ObCharset::get_charset(value.get_collation_type());
    const ObCharsetInfo *to_charset_info = ObCharset::get_charset(collation_type);
    if (OB_ISNULL(from_charset_info) || OB_ISNULL(to_charset_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("charsetinfo is null", K(ret), K(value.get_collation_type()), K(collation_type));
    } else if (CS_TYPE_INVALID == value.get_collation_type() || CS_TYPE_INVALID == collation_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid collation", K(value.get_collation_type()), K(collation_type), K(ret));
    } else if (CS_TYPE_BINARY != value.get_collation_type() && CS_TYPE_BINARY != collation_type
        && strcmp(from_charset_info->csname, to_charset_info->csname) != 0) {
      char *buf = NULL;
      int32_t buf_len = str.length() * ObCharset::CharConvertFactorNum;
      uint32_t result_len = 0;
      if (0 == buf_len) {
        //do noting
      } else if (OB_UNLIKELY(NULL == (buf = static_cast<char *>(
                allocator.alloc(buf_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K(buf_len));
      } else if (OB_FAIL(convert_string_charset(str, value.get_collation_type(),
                                                collation_type, buf, buf_len, result_len))) {
        LOG_WARN("convert string charset failed", K(ret));
      } else {
        value.set_string(value.get_type(), buf, static_cast<int32_t>(result_len));
        value.set_collation_type(collation_type);
      }
    }
  }
  return ret;
}

int ObQueryDriver::convert_lob_value_charset(common::ObObj& value, sql::ObResultSet &result)
{
  int ret = OB_SUCCESS;

  ObCharsetType charset_type = CHARSET_INVALID;
  ObCharsetType nchar = CHARSET_INVALID;
  const ObSQLSessionInfo &my_session = result.get_session();
  ObArenaAllocator *allocator = NULL;
  if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
    LOG_WARN("fail to get lob fake allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob fake allocator is null.", K(ret), K(value));
  } else if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
  } else if (OB_FAIL(my_session.get_ncharacter_set_connection(nchar))) {
    LOG_WARN("get ncharacter set connection failed", K(ret));
  } else if (lib::is_oracle_mode()
            && (value.is_nchar() || value.is_nvarchar2())
            && nchar != CHARSET_INVALID
            && nchar != CHARSET_BINARY) {
    charset_type = nchar;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(convert_lob_value_charset(value, charset_type, *allocator))) {
    LOG_WARN("convert lob value fail.", K(ret), K(value));
  }
  return ret;
}

int ObQueryDriver::convert_text_value_charset(common::ObObj& value, sql::ObResultSet &result)
{
  int ret = OB_SUCCESS;
  ObCharsetType charset_type = CHARSET_INVALID;
  ObCharsetType nchar = CHARSET_INVALID;
  const ObSQLSessionInfo &my_session = result.get_session();
  ObArenaAllocator *allocator = NULL;
  if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
    LOG_WARN("fail to get lob fake allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("text fake allocator is null.", K(ret), K(value));
  } else if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
    } else if (OB_FAIL(my_session.get_ncharacter_set_connection(nchar))) {
    LOG_WARN("get ncharacter set connection failed", K(ret));
  } else if (lib::is_oracle_mode()
            && (value.is_nchar() || value.is_nvarchar2())
            && nchar != CHARSET_INVALID
            && nchar != CHARSET_BINARY) {
    charset_type = nchar;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(convert_text_value_charset(value, charset_type, *allocator, &my_session, &result.get_exec_context()))) {
    LOG_WARN("convert lob value fail.", K(ret), K(value));
  }
  return ret;
}

int ObQueryDriver::like_match(const char* str, int64_t length_str, int64_t i,
                              const char* pattern, int64_t length_pat, int64_t j,
                              bool &is_match)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(str) || OB_ISNULL(pattern) ||
      OB_UNLIKELY(length_str < 0 || i < 0 || length_pat < 0 || j < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(str), K(length_str), K(i),
                                     K(pattern), K(length_pat), K(j));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (i == length_str && j == length_pat) {
    is_match = true;
  } else if (i != length_str && j >= length_pat) {
    is_match = false;
  } else if (j < length_pat && pattern[j] == '%') {
    ++j;
    if (OB_FAIL(like_match(str, length_str, i,
                           pattern, length_pat, j,
                           is_match))) {
      LOG_WARN("failed to match", K(ret));
    } else if (!is_match && i < length_str) {
      ++i;
      --j;
      if (OB_FAIL(like_match(str, length_str, i,
                             pattern, length_pat, j,
                             is_match))) {
        LOG_WARN("failed to match", K(ret));
      }
    }
  } else if (i < length_str && j < length_pat && pattern[j] == '_') {
    ++i;
    ++j;
    if (OB_FAIL(like_match(str, length_str, i,
                           pattern, length_pat, j,
                           is_match))) {
      LOG_WARN("failed to match", K(ret));
    }
  } else if (i < length_str && j < length_pat && tolower(str[i]) == tolower(pattern[j])) {
    ++i;
    ++j;
    if (OB_FAIL(like_match(str, length_str, i,
                           pattern, length_pat, j,
                           is_match))) {
      LOG_WARN("failed to match", K(ret));
    }
  } else {
    is_match = false;
  }
  return ret;
}

int ObQueryDriver::convert_lob_locator_to_longtext(ObObj& value, sql::ObResultSet &result)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator *allocator = NULL;
  if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
    LOG_WARN("fail to get lob fake allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob fake allocator is null.", K(ret), K(value));
  } else if (OB_FAIL(convert_lob_locator_to_longtext(value, 
                                                     session_.is_client_use_lob_locator(), 
                                                     allocator))) {
    LOG_WARN("convert lob to longtext fail.", K(ret), K(value));
  }
  return ret;
}

int ObQueryDriver::convert_lob_locator_to_longtext(ObObj& value,
                                                   bool is_use_lob_locator,
                                                   ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  // 如果客户端使用新的lob locator, 则返回lob locator数据
  // 如果客户端使用老的lob(没有locator头, 仅数据), 则返回老的lob
  if (lib::is_oracle_mode()) {
    if (is_use_lob_locator && value.is_lob()) {
      ObString str;
      char *buf = nullptr;
      ObLobLocator *lob_locator = nullptr;
      if (OB_FAIL(value.get_string(str))) {
        STORAGE_LOG(WARN, "Failed to get string from obj", K(ret), K(value));
      } else if (OB_ISNULL(buf = static_cast<char*>(
                            allocator->alloc(str.length() + sizeof(ObLobLocator))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(str));
      } else if (FALSE_IT(lob_locator = reinterpret_cast<ObLobLocator *> (buf))) {
      } else if (OB_FAIL(lob_locator->init(str))) {
        STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(str), KPC(lob_locator));
      } else {
        value.set_lob_locator(*lob_locator);
        LOG_TRACE("return lob locator", K(*lob_locator), K(str));
      }
    } else if (!is_use_lob_locator && value.is_lob_locator()) {
      ObString payload;
      if (OB_ISNULL(value.get_lob_locator())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(value));
      } else if (OB_FAIL(value.get_lob_locator()->get_payload(payload))) {
        LOG_WARN("fail to get payload", K(ret));
      } else {
        value.set_lob_value(ObLongTextType,
                            payload.ptr(),
                            static_cast<int32_t>(payload.length()));
      }
    }
    LOG_TRACE("return data", K(is_use_lob_locator), K(value), K(lbt()));
  }
  return ret;
}

int ObQueryDriver::process_lob_locator_results(ObObj& value, sql::ObResultSet &result)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator *allocator = NULL;
  if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
    LOG_WARN("fail to get lob fake allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob fake allocator is null.", K(ret), K(value));
  } else if (OB_FAIL(process_lob_locator_results(value,
                                                 session_.is_client_use_lob_locator(),
                                                 session_.is_client_support_lob_locatorv2(),
                                                 allocator,
                                                 &result.get_session(),
                                                 &result.get_exec_context()))) {
    LOG_WARN("convert lob to longtext fail.", K(ret), K(value));
  }
  return ret;
}

int ObQueryDriver::process_lob_locator_results(ObObj& value,
                                               bool is_use_lob_locator,
                                               bool is_support_outrow_locator_v2,
                                               ObIAllocator *allocator,
                                               const sql::ObSQLSessionInfo *session_info,
                                               sql::ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  // 1. if client is_use_lob_locator, return lob locator
  // 2. if client is_use_lob_locator, but not support outrow lob, return lob locator with inrow data
  //    refer to sz/aibo1m
  // 3. if client does not support use_lob_locator ,,return full lob data without locator header
  bool is_lob_type = value.is_lob() || value.is_lob_locator()
                     || value.is_json() || value.is_geometry() || value.is_roaringbitmap() ;
  bool is_actual_return_lob_locator = is_use_lob_locator && !value.is_json()
                                      && !value.is_geometry() && !value.is_roaringbitmap();
  if (!is_lob_type) {
    // not lob types, do nothing
  } else if (value.is_null() || value.is_nop_value()) {
    // do nothing
  } else if (is_actual_return_lob_locator && is_lob_type && lib::is_oracle_mode()) {
    // Should be ObLobType (cluster version < 4.1), or Text/Json/Gis with lob header
    ObLobLocatorV2 loc(value.get_string(), value.has_lob_header());
    if (loc.is_lob_locator_v1()) {// do nothing, lob locator version 1
    } else if (loc.is_valid()) { // lob locator v2
      if (!loc.has_lob_header()) {
        ObString data = value.get_string();
        ObTextStringResult new_tmp_lob(value.get_type(), true, allocator);
        if (OB_FAIL(new_tmp_lob.init(data.length()))) {
          LOG_WARN("fail to init text string result", K(ret), K(new_tmp_lob), K(data.length()));
        } else if (OB_FAIL(new_tmp_lob.append(data))) {
          LOG_WARN("fail to append data", K(ret), K(new_tmp_lob), K(data.length()));
        } else {
          ObString res;
          new_tmp_lob.get_result_buffer(res);
          value.set_lob_value(value.get_type(), res.ptr(), res.length());
          value.set_has_lob_header(); // must has lob header
        }
      } else if (!loc.has_extern()) {
        if (!loc.has_inrow_data()) {
          // currently all temp lobs have extern field in oracle mode
          // or the lob locator header cannot compatable with clients for 4.0
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Lob: oracle lob locator v2 with out extern segment", K(value), K(GET_MIN_CLUSTER_VERSION()));
        } else if (!is_support_outrow_locator_v2) {
          // convert to full extern temp lob locator
          ObString inrow_data;
          if (OB_FAIL(loc.get_inrow_data(inrow_data))) {
            LOG_WARN("Lob: get inrow data failed", K(ret), K(loc));
          } else {
            value.set_string(value.get_type(), inrow_data.ptr(), inrow_data.length());
            value.set_inrow();
            if (OB_FAIL(ObTextStringResult::ob_convert_obj_temporay_lob(value, *allocator))) {
              LOG_WARN("fail to convert temp lob locator", K(ret), K(value));
            }
          }
        }
      } else {
        if (!is_support_outrow_locator_v2 && !loc.has_inrow_data()) {
          if (OB_FAIL(ObTextStringIter::append_outrow_lob_fulldata(value, session_info, *allocator))) {
            LOG_WARN("Lob: convert lob to outrow failed", K(value), K(GET_MIN_CLUSTER_VERSION()));
          }
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Lob: invalid lob locator", K(value), K(GET_MIN_CLUSTER_VERSION()));
    }
  } else if ((!is_actual_return_lob_locator && lib::is_oracle_mode())
             || lib::is_mysql_mode()) {
    // Should remove locator header and read full lob data
    ObString data;
    ObLobLocatorV2 loc(value.get_string(), value.has_lob_header());
    if (loc.is_null()) { // maybe v1 empty lob
    } else if (loc.is_lob_locator_v1()) {
      if (OB_FAIL(convert_lob_locator_to_longtext(value, is_use_lob_locator, allocator))) {
        LOG_WARN("Lob: handle lob locator v1 failed", K(value), K(GET_MIN_CLUSTER_VERSION()));
      }
    } else { // lob locator v2
      ObArenaAllocator tmp_alloc("ObLobRead", OB_MALLOC_NORMAL_BLOCK_SIZE, session_info->get_effective_tenant_id());
      ObTextStringIter instr_iter(value);
      if (OB_FAIL(ObTextStringHelper::build_text_iter(instr_iter, exec_ctx, session_info, allocator, &tmp_alloc))) {
        LOG_WARN("init lob str inter failed", K(ret), K(value));
      } else if (OB_FAIL(instr_iter.get_full_data(data))) {
        LOG_WARN("Lob: init lob str iter failed ", K(value));
      } else {
        ObObjType dst_type = ObLongTextType;
        if (value.is_json()) {
          dst_type = ObJsonType;
        } else if (value.is_geometry()) {
          dst_type = ObGeometryType;
        } else if (value.is_roaringbitmap()) {
          dst_type = ObRoaringBitmapType;
        }
        // remove has lob header flag
        value.set_lob_value(dst_type, data.ptr(), static_cast<int32_t>(data.length()));
      }
    }
  } else { /* do nothing */ }
  return ret;
}

int ObQueryDriver::convert_string_charset(const ObString &in_str, const ObCollationType in_cs_type, 
                                          const ObCollationType out_cs_type, 
                                          char *buf, int32_t buf_len, uint32_t &result_len)
{
  int ret = OB_SUCCESS;
  ret = ObCharset::charset_convert(in_cs_type, in_str.ptr(),
        in_str.length(),out_cs_type, buf, buf_len, result_len);
  if (OB_SUCCESS != ret) {
    int32_t str_offset = 0;
    int64_t buf_offset = 0;
    ObString question_mark = ObCharsetUtils::get_const_str(out_cs_type, '?');
    while (str_offset < in_str.length() && buf_offset + question_mark.length() <= buf_len) {
      int64_t offset = ObCharset::charpos(in_cs_type, in_str.ptr() + str_offset,
                                          in_str.length() - str_offset, 1);
      ret = ObCharset::charset_convert(in_cs_type, in_str.ptr() + str_offset, offset, out_cs_type,
                                       buf + buf_offset, buf_len - buf_offset, result_len);
      str_offset += offset;
      if (OB_SUCCESS == ret) {
        buf_offset += result_len;
      } else {
        MEMCPY(buf + buf_offset, question_mark.ptr(), question_mark.length());
        buf_offset += question_mark.length();
      }
    }
    if (str_offset < in_str.length()) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("sizeoverflow", K(ret), K(in_str), KPHEX(in_str.ptr(), in_str.length()));
    } else {
      result_len = buf_offset;
      ret = OB_SUCCESS;
      LOG_WARN("charset convert failed", K(ret), K(in_cs_type), K(out_cs_type));
    }
  }
  return ret;
}

int ObQueryDriver::convert_lob_value_charset(ObObj& value,
                                             ObCharsetType charset_type, 
                                             ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  ObString str;
  ObLobLocator *lob_locator = NULL;
  if (OB_FAIL(value.get_lob_locator(lob_locator))) {
    LOG_WARN("get lob locator failed", K(ret));
  } else if (OB_ISNULL(lob_locator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null lob locator", K(ret));
  } else if (OB_FAIL(lob_locator->get_payload(str))) {
    LOG_WARN("get lob locator payload failed", K(ret));
  } else if (ObCharset::is_valid_charset(charset_type) && CHARSET_BINARY != charset_type) {
    ObCollationType collation_type = ObCharset::get_default_collation(charset_type);
    const ObCharsetInfo *from_charset_info = ObCharset::get_charset(value.get_collation_type());
    const ObCharsetInfo *to_charset_info = ObCharset::get_charset(collation_type);
    if (OB_ISNULL(from_charset_info) || OB_ISNULL(to_charset_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("charsetinfo is null", K(ret), K(value.get_collation_type()), K(collation_type));
    } else if (CS_TYPE_INVALID == value.get_collation_type() || CS_TYPE_INVALID == collation_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid collation", K(value.get_collation_type()), K(collation_type), K(ret));
    } else if (CS_TYPE_BINARY != value.get_collation_type() && CS_TYPE_BINARY != collation_type
        && strcmp(from_charset_info->csname, to_charset_info->csname) != 0) {
      char *buf = NULL;
      int32_t header_len = value.get_val_len() - lob_locator->payload_size_;
      int32_t str_len = lob_locator->payload_size_ * 4;
      uint32_t result_len = 0;
      if (OB_UNLIKELY(NULL == (buf = static_cast<char *>(
                allocator.alloc(header_len + str_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K(header_len), K(str_len));
      } else {
        MEMCPY(buf, lob_locator, header_len);
        if (OB_FAIL(convert_string_charset(str, value.get_collation_type(), collation_type,
                                           buf + header_len, str_len, result_len))) {
          LOG_WARN("convert string charset failed", K(ret));
        } else {
          ObLobLocator *result_lob = reinterpret_cast<ObLobLocator *>(buf);
          result_lob->payload_size_ = result_len;
          value.set_lob_locator(*result_lob);
          value.set_collation_type(collation_type);
        }
      }
    }
  }
  return ret;
}

int ObQueryDriver::convert_text_value_charset(ObObj& value,
                                              ObCharsetType charset_type,
                                              ObIAllocator &allocator,
                                              const sql::ObSQLSessionInfo *session,
                                              sql::ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  ObString raw_str = value.get_string();
  if (value.is_null() || value.is_nop_value()) {
  } else if (OB_ISNULL(raw_str.ptr()) || raw_str.length() == 0) {
    if (!value.has_lob_header() || !value.is_lob_storage()) {
      LOG_DEBUG("Lob: get empty or null obj without header or not lob", K(value));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Lob: get  empty or null lob obj with header", K(ret), K(value));
    }
  } else if (ObCharset::is_valid_charset(charset_type) && CHARSET_BINARY != charset_type) {
    ObCollationType to_collation_type = ObCharset::get_default_collation(charset_type);
    ObCollationType from_collation_type = value.get_collation_type();
    const ObCharsetInfo *from_charset_info = ObCharset::get_charset(from_collation_type);
    const ObCharsetInfo *to_charset_info = ObCharset::get_charset(to_collation_type);
    const ObObjType type = value.get_type();

    if (OB_ISNULL(from_charset_info) || OB_ISNULL(to_charset_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Lob: charsetinfo is null", K(ret), K(from_collation_type), K(to_collation_type));
    } else if (CS_TYPE_INVALID == from_collation_type || CS_TYPE_INVALID == to_collation_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Lob: invalid collation", K(from_collation_type), K(to_collation_type), K(ret));
    } else if (CS_TYPE_BINARY != from_collation_type && CS_TYPE_BINARY != to_collation_type
        && strcmp(from_charset_info->csname, to_charset_info->csname) != 0) {
      bool is_actual_return_lob_locator = session->is_client_use_lob_locator() && !value.is_json();
      if (value.is_lob_storage() && value.has_lob_header() && OB_NOT_NULL(session) &&
          is_actual_return_lob_locator && lib::is_oracle_mode()) {
        ObLobLocatorV2 lob;
        ObString inrow_data;
        if (OB_FAIL(process_lob_locator_results(value,
                                                session->is_client_use_lob_locator(),
                                                session->is_client_support_lob_locatorv2(),
                                                &allocator,
                                                session,
                                                exec_ctx))) {
          LOG_WARN("fail to process lob locator", K(ret), K(value));
        } else if (OB_FAIL(value.get_lob_locatorv2(lob))) {
          LOG_WARN("fail to lob locator v2", K(ret), K(value));
        } else if (!lob.has_inrow_data()) {
          // do nothing
        } else if (OB_FAIL(lob.get_inrow_data(inrow_data))) {
          LOG_WARN("fail to get inrow data", K(ret), K(lob));
        } else if (inrow_data.length() == 0) {
          // do nothing
        } else {
          int64_t lob_data_byte_len = inrow_data.length();
          int64_t offset_len = reinterpret_cast<uint64_t>(inrow_data.ptr()) - reinterpret_cast<uint64_t>(lob.ptr_);
          int64_t res_len = offset_len + lob_data_byte_len * ObCharset::CharConvertFactorNum;
          char *buf = static_cast<char*>(allocator.alloc(res_len));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret), K(res_len));
          } else {
            MEMCPY(buf, lob.ptr_, offset_len);
            uint32_t result_len = 0;
            if (OB_FAIL(convert_string_charset(inrow_data, from_collation_type, to_collation_type,
                                               buf + offset_len, res_len - offset_len, result_len))) {
              LOG_WARN("Lob: convert string charset failed", K(ret));
            } else {
              lob.assign_buffer(buf, offset_len + result_len, lob.has_lob_header());
              // refresh payload size
              if (lob.has_extern()) {
                ObMemLobExternHeader *ex_header = nullptr;
                if (OB_FAIL(lob.get_extern_header(ex_header))) {
                  LOG_WARN("fail to get extern header", K(ret), K(lob));
                } else {
                  ex_header->payload_size_ = result_len;
                }
              }
              // refresh lob data size
              if (OB_SUCC(ret)) {
                ObLobCommon *lob_common = nullptr;
                if (OB_FAIL(lob.get_disk_locator(lob_common))) {
                  LOG_WARN("fail to get lob common", K(ret), K(lob));
                } else if (lob_common->is_init_) {
                  ObLobData *lob_data = reinterpret_cast<ObLobData*>(lob_common->buffer_);
                  lob_data->byte_size_ = result_len;
                }
              }
              if (OB_SUCC(ret)) {
                LOG_DEBUG("Lob: new temp convert_text_value_charset in convert_text_value",
                K(ret), K(raw_str), K(type), K(to_collation_type), K(from_collation_type),
                K(from_charset_info->csname), K(to_charset_info->csname));
                value.set_lob_value(type, buf, offset_len + result_len);
                value.set_collation_type(to_collation_type);
                value.set_has_lob_header();
              }
            }
          }
        }
      } else {
        // get full data, buffer size is full byte length * ObCharset::CharConvertFactorNum
        ObString data_str = value.get_string();
        int64_t lob_data_byte_len = data_str.length();
        if (!value.has_lob_header()) {
        } else {
          ObLobLocatorV2 loc(raw_str, value.has_lob_header());
          ObTextStringIter str_iter(value);
          if (OB_FAIL(ObTextStringHelper::build_text_iter(str_iter, exec_ctx, session, &allocator))) {
            LOG_WARN("Lob: init lob str iter failed ", K(ret), K(value));
          } else if (OB_FAIL(str_iter.get_full_data(data_str))) {
            LOG_WARN("Lob: get full data failed ", K(ret), K(value));
          } else if (OB_FAIL(loc.get_lob_data_byte_len(lob_data_byte_len))) {
            LOG_WARN("Lob: get lob data byte len failed", K(ret), K(loc));
          }
        }
        if (OB_SUCC(ret)) {
          // mock result buffer and reserve data length
          // could do streaming charset convert
          ObTextStringResult new_tmp_lob(type, value.has_lob_header(), &allocator);
          char *buf = NULL;
          int64_t buf_len = 0;
          uint32_t result_len = 0;
          int64_t converted_len = lob_data_byte_len * ObCharset::CharConvertFactorNum;
          if (OB_FAIL(new_tmp_lob.init(converted_len))) {
            LOG_WARN("Lob: init tmp lob failed", K(ret), K(converted_len));
          } else if (OB_FAIL(new_tmp_lob.get_reserved_buffer(buf, buf_len))) {
            LOG_WARN("Lob: get empty buffer failed", K(ret), K(converted_len));
          } else if (OB_FAIL(convert_string_charset(data_str, from_collation_type, to_collation_type,
                                                    buf, buf_len, result_len))) {
            LOG_WARN("Lob: convert string charset failed", K(ret));
          } else if (OB_FAIL(new_tmp_lob.lseek(result_len, 0))) {
            LOG_WARN("Lob: temp lob lseek failed", K(ret));
          } else {
            ObString lob_loc_str;
            new_tmp_lob.get_result_buffer(lob_loc_str);
            LOG_DEBUG("Lob: new temp convert_text_value_charset in convert_text_value",
                K(ret), K(raw_str), K(type), K(to_collation_type), K(from_collation_type),
                K(from_charset_info->csname), K(to_charset_info->csname));
            value.set_lob_value(type, lob_loc_str.ptr(), lob_loc_str.length());
            value.set_collation_type(to_collation_type);
            if (new_tmp_lob.has_lob_header()) {
              value.set_has_lob_header();
            }
          }
        }
      }
    }
  }
  return ret;
}

/*@brief:is_com_filed_list_match_wildcard_str 用于匹配client发过来的COM_FIELD_LIST中包含的参数中有匹配符
* 情形,eg:COM_FIELD_LIST(t1, c*) , t1 中有c1,c2,pk 三列 ==> 仅返回c1, c2，不返回 pk，因为他和 c* 不匹配;
* 其规则类似于like情形；详细参考链接：
*
*/
int ObQueryDriver::is_com_filed_list_match_wildcard_str(ObResultSet &result,
                                                        const ObCollationType &from_collation,
                                                        const ObString &from_string,
                                                        bool &is_not_match)
{
  int ret = OB_SUCCESS;
  is_not_match = false;
  if (!result.get_is_com_filed_list() || result.get_wildcard_string().empty()) {
    /*do nothing*/
  } else {
    /*需要考虑不同的字符集之间进行比较时需要转换为同一种字符集进行比较*/
    ObIAllocator &allocator = result.get_mem_pool();
    ObString wildcard_str;
    if (result.get_session().get_nls_collation() != from_collation) {
      if (OB_FAIL(convert_field_charset(allocator,
                                        result.get_session().get_nls_collation(),
                                        from_collation,
                                        result.get_wildcard_string(),
                                        wildcard_str))) {
        LOG_WARN("failed to convert field charset", K(ret));
      }
    } else {
      wildcard_str = result.get_wildcard_string();
    }
    if (OB_SUCC(ret)) {
      bool is_match = false;
      if (OB_FAIL(like_match(from_string.ptr(),
                             from_string.length(),
                             0,
                             wildcard_str.ptr(),
                             wildcard_str.length(),
                             0,
                             is_match))) {
        LOG_WARN("failed to like match", K(ret));
      } else if (!is_match) {
        is_not_match = true;
      }
    }
  }
  return ret;
}

}
}

