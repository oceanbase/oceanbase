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

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace obmysql;
namespace observer
{

int ObQueryDriver::response_query_header(ObResultSet &result,
                                         bool has_nore_result,
                                         bool need_set_ps_out_flag,
                                         bool is_prexecute)
{
  int ret = OB_SUCCESS;
  bool ac = true;
  if (result.get_field_cnt() <= 0) {
    LOG_WARN("result set column", K(result.get_field_cnt()));
    ret = OB_ERR_BAD_FIELD_ERROR;
  } else if (OB_FAIL(session_.get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  }
  if (OB_SUCC(ret) && !result.get_is_com_filed_list() && !is_prexecute) {
    OMPKResheader rhp;
    rhp.set_field_count(result.get_field_cnt());
    if (OB_FAIL(sender_.response_packet(rhp, &result.get_session()))) {
      LOG_WARN("response packet fail", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const ColumnsFieldIArray *fields = result.get_field_columns();
    if (OB_ISNULL(fields)) {
      ret = OB_INVALID_ARGUMENT;
      result.set_errcode(ret);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < result.get_field_cnt(); ++i) {
      bool is_not_match = false;
      ObMySQLField field;
      const ObField &ob_field = result.get_field_columns()->at(i);
      if (OB_FAIL(is_com_filed_list_match_wildcard_str(
                                                  result,
                                                  static_cast<ObCollationType>(ob_field.charsetnr_),
                                                  ob_field.org_cname_,
                                                  is_not_match))) {
        LOG_WARN("failed to is com filed list match wildcard str", K(ret));
      } else if (is_not_match) {
        /*do nothing*/
      } else {
        ret = ObMySQLResultSet::to_mysql_field(ob_field, field);
        result.replace_lob_type(result.get_session(), ob_field, field);
        if (result.get_is_com_filed_list()) {
          field.default_value_ = static_cast<EMySQLFieldType>(ob_field.default_value_.get_ext());
        }
        result.set_errcode(ret);
        if (OB_SUCC(ret)) {
          OMPKField fp(field);
          if (OB_FAIL(sender_.response_packet(fp, &result.get_session()))) {
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
    flags.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS = has_nore_result;
    flags.status_flags_.OB_SERVER_PS_OUT_PARAMS = need_set_ps_out_flag ? 1 : 0;
    if (!session_.is_obproxy_mode()) {
      // in java client or others, use slow query bit to indicate partition hit or not
      flags.status_flags_.OB_SERVER_QUERY_WAS_SLOW = !session_.partition_hit().get_bool();
    }
    eofp.set_server_status(flags);

    if (OB_FAIL(sender_.response_packet(eofp, &result.get_session()))) {
      LOG_WARN("response packet fail", K(ret));
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
  if (!has_top_limit && OB_INVALID_COUNT == fetch_limit) {
    limit_count = INT64_MAX;
    if (OB_FAIL(session_.get_sql_select_limit(limit_count))) {
      LOG_WARN("fail tp get sql select limit", K(ret));
    }
  }
  bool is_packed = result.get_physical_plan() ? result.get_physical_plan()->is_packed() : false;
  MYSQL_PROTOCOL_TYPE protocol_type = is_ps_protocol ? BINARY : TEXT;
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
      if (OB_FAIL(response_query_header(result, has_more_result, false, is_prexecute_))) {
        LOG_WARN("fail to response query header", K(ret), K(row_num), K(can_retry));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row->get_count(); i++) {
      ObObj& value = row->get_cell(i);
      if (result.is_ps_protocol() && !is_packed) {
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
        if (OB_SUCC(ret)
            && (value.is_lob() || value.is_lob_locator() || value.is_json() || value.is_geometry())
            && OB_FAIL(process_lob_locator_results(value, result))) {
          LOG_WARN("convert lob locator to longtext failed", K(ret));
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
        ObArenaAllocator *allocator = NULL;
        if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
          LOG_WARN("fail to get lob fake allocator", K(ret));
        } else if (OB_NOT_NULL(allocator)) {
          allocator->reset_remain_one_page();
        }
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
    // 如果是一行数据也没有，则还是要给客户端回复field等信息，并且不再重试了
    can_retry = false;
    if (OB_FAIL(response_query_header(result, has_more_result, false, is_prexecute_))) {
      LOG_WARN("fail to response query header", K(ret), K(row_num), K(can_retry));
    }
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
  if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
    LOG_WARN("fail to get lob fake allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob fake allocator is null.", K(ret), K(value));
  } else if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
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
  const ObSQLSessionInfo &my_session = result.get_session();
  ObArenaAllocator *allocator = NULL;
  if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
    LOG_WARN("fail to get lob fake allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob fake allocator is null.", K(ret), K(value));
  } else if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
  } else if (OB_FAIL(convert_lob_value_charset(value, charset_type, *allocator))) {
    LOG_WARN("convert lob value fail.", K(ret), K(value));
  }
  return ret;
}

int ObQueryDriver::convert_text_value_charset(common::ObObj& value, sql::ObResultSet &result)
{
  int ret = OB_SUCCESS;
  ObCharsetType charset_type = CHARSET_INVALID;
  const ObSQLSessionInfo &my_session = result.get_session();
  ObArenaAllocator *allocator = NULL;
  if (OB_FAIL(result.get_exec_context().get_convert_charset_allocator(allocator))) {
    LOG_WARN("fail to get lob fake allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("text fake allocator is null.", K(ret), K(value));
  } else if (OB_FAIL(my_session.get_character_set_results(charset_type))) {
    LOG_WARN("fail to get result charset", K(ret));
  } else if (OB_FAIL(convert_text_value_charset(value, charset_type, *allocator, &my_session))) {
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
                                                 &result.get_session()))) {
    LOG_WARN("convert lob to longtext fail.", K(ret), K(value));
  }
  return ret;
}

int ObQueryDriver::process_lob_locator_results(ObObj& value,
                                               bool is_use_lob_locator,
                                               bool is_support_outrow_locator_v2,
                                               ObIAllocator *allocator,
                                               sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  // 1. if client is_use_lob_locator, return lob locator
  // 2. if client is_use_lob_locator, but not support outrow lob, return lob locator with inrow data
  //    refer to https://yuque.antfin.com/ob/gtuwei/aibo1m
  // 3. if client does not support use_lob_locator ,,return full lob data without locator header
  bool is_lob_type = value.is_lob() || value.is_json() || value.is_geometry() || value.is_lob_locator();
  if (!is_lob_type) {
    // not lob types, do nothing
  } else if (value.is_null() || value.is_nop_value()) {
    // do nothing
  } else if (is_use_lob_locator && is_lob_type && lib::is_oracle_mode()) {
    // Should be ObLobType (cluster version < 4.1), or Text/Json/Gis with lob header
    ObLobLocatorV2 loc(value.get_string(), value.has_lob_header());
    if (loc.is_lob_locator_v1()) {// do nothing, lob locator version 1
    } else if (loc.is_valid()) { // lob locator v2
      if (!loc.has_extern()) {
        // currently all temp lobs have extern field in oracle mode
        // or the lob locator header cannot compatable with clients for 4.0
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Lob: oracle lob locator v2 with out extern segment", K(value), K(GET_MIN_CLUSTER_VERSION()));
      } else {
        if (!is_support_outrow_locator_v2 && !loc.is_inrow()) {
          if (OB_FAIL(ObTextStringIter::append_outrow_lob_fulldata(value, session_info, *allocator))) {
            LOG_WARN("Lob: convert lob to outrow failed", K(value), K(GET_MIN_CLUSTER_VERSION()));
          }
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Lob: invalid lob locator", K(value), K(GET_MIN_CLUSTER_VERSION()));
    }
  } else if ((!is_use_lob_locator && lib::is_oracle_mode())
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
      ObTextStringIter instr_iter(value);
      if (OB_FAIL(instr_iter.init(0, session_info, allocator))) {
        LOG_WARN("init lob str inter failed", K(ret), K(value));
      } else if (OB_FAIL(instr_iter.get_full_data(data))) {
        LOG_WARN("Lob: init lob str iter failed ", K(value));
      } else {
        ObObjType dst_type = ObLongTextType;
        if (value.is_json()) {
          dst_type = ObJsonType;
        } else if (value.is_geometry()) {
          dst_type = ObGeometryType;
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
                                              const sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ObString raw_str = value.get_string();
  if (value.is_null() || value.is_nop_value()) {
  } else if (OB_ISNULL(raw_str.ptr()) || raw_str.length() == 0) {
    LOG_WARN("Lob: get null lob locator v2", K(value));
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

      // get full data, buffer size is full byte length * 4
      ObString data_str = value.get_string();
      int64_t lob_data_byte_len = data_str.length();
      if (!value.has_lob_header()) {
      } else {
        ObLobLocatorV2 loc(raw_str, value.has_lob_header());
        ObTextStringIter str_iter(value);
        if (OB_FAIL(str_iter.init(0, session, &allocator))) {
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

        if (OB_FAIL(new_tmp_lob.init(lob_data_byte_len * 4))) {
          LOG_WARN("Lob: init tmp lob failed", K(ret), K(lob_data_byte_len * 4));
        } else if (OB_FAIL(new_tmp_lob.get_reserved_buffer(buf, buf_len))) {
          LOG_WARN("Lob: get empty buffer failed", K(ret), K(lob_data_byte_len * 4));
        } else if (OB_FAIL(convert_string_charset(data_str, from_collation_type, to_collation_type,
                                                  buf, buf_len, result_len))) {
          LOG_WARN("Lob: convert string charset failed", K(ret));
        } else if (OB_FAIL(new_tmp_lob.lseek(result_len, 0))) {
          LOG_WARN("Lob: temp lob lseek failed", K(ret));
        } else {
          ObString lob_loc_str;
          new_tmp_lob.get_result_buffer(lob_loc_str);
          LOG_INFO("Lob: new temp convert_text_value_charset in convert_text_value",
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
  return ret;
}

/*@brief:is_com_filed_list_match_wildcard_str 用于匹配client发过来的COM_FIELD_LIST中包含的参数中有匹配符
* 情形,eg:COM_FIELD_LIST(t1, c*) , t1 中有c1,c2,pk 三列 ==> 仅返回c1, c2，不返回 pk，因为他和 c* 不匹配;
* 其规则类似于like情形；详细参考链接：
* https://code.aone.alibaba-inc.com/oceanbase/oceanbase/codereview/3397651
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

