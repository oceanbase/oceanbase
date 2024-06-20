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
 * This file is for implement of func json expr helper
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/ob_errno.h"
#include "share/ob_json_access_utils.h"
#include "ob_expr_json_utils.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "ob_expr_json_func_helper.h"
#include "lib/encode/ob_base64_encode.h" // for ObBase64Encoder
#include "lib/utility/ob_fast_convert.h" // ObFastFormatInt::format_unsigned
#include "lib/charset/ob_dtoa.h" // ob_gcvt_opt
#include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE
#include "sql/ob_result_set.h"
#include "sql/ob_spi.h"
#include "storage/lob/ob_lob_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
static OB_INLINE int common_construct_otimestamp(const ObObjType type,
                                                 const ObDatum &in_datum,
                                                 ObOTimestampData &out_val)
{
  int ret = OB_SUCCESS;
  if (ObTimestampTZType == type) {
    out_val = in_datum.get_otimestamp_tz();
  } else if (ObTimestampLTZType == type || ObTimestampNanoType == type) {
    out_val = in_datum.get_otimestamp_tiny();
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid in type", K(ret), K(type));
  }
  return ret;
}

template <typename T>
int get_otimestamp_from_datum(const T &datum, ObOTimestampData &in_val, ObObjType type);
template <>
int get_otimestamp_from_datum<ObDatum>(const ObDatum &datum, ObOTimestampData &in_val, ObObjType type)
{
  INIT_SUCC(ret);
  if (ObTimestampTZType == type) {
    in_val = datum.get_otimestamp_tz();
  } else if (ObTimestampLTZType == type || ObTimestampNanoType == type) {
    in_val = datum.get_otimestamp_tiny();
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid in type", K(ret), K(type));
  }
  return ret;
}
template <>
int get_otimestamp_from_datum<ObObj>(const ObObj &datum, ObOTimestampData &in_val, ObObjType type)
{
  INIT_SUCC(ret);
  in_val = datum.get_otimestamp_value();
  return ret;
}

ObJsonInType ObJsonExprHelper::get_json_internal_type(ObObjType type)
{
  return type == ObJsonType ? ObJsonInType::JSON_BIN : ObJsonInType::JSON_TREE;
}

int ObJsonExprHelper::ensure_collation(ObObjType type, ObCollationType cs_type)
{
  INIT_SUCC(ret);
  if (ob_is_string_type(type) && (ObCharset::charset_type_by_coll(cs_type) != CHARSET_UTF8MB4)) {
    LOG_WARN("unsuport json string type with binary charset", K(type), K(cs_type));
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_CHARSET);
  }

  return ret;
}

int ObJsonExprHelper::get_json_or_str_data(ObExpr *expr, ObEvalCtx &ctx,
                                           common::ObIAllocator &allocator,
                                           ObString& str, bool& is_null)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObObjType val_type = expr->datum_meta_.type_;
  if (OB_UNLIKELY(OB_FAIL(expr->eval(ctx, json_datum)))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (json_datum->is_null() || val_type == ObNullType) {
    is_null = true;
  } else if (!ob_is_extend(val_type)
              && !ob_is_json(val_type)
              && !ob_is_raw(val_type)
              && !ob_is_string_type(val_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("input type error", K(val_type));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, *json_datum,
                expr->datum_meta_, expr->obj_meta_.has_lob_header(), str, &ctx.exec_ctx_))) {
    LOG_WARN("fail to get real data.", K(ret), K(str));
  }
  return ret;
}

int ObJsonExprHelper::get_json_schema(const ObExpr &expr, ObEvalCtx &ctx,
                                      common::ObArenaAllocator &allocator,
                                      uint16_t index, ObIJsonBase*& j_schema,
                                      bool &is_null)
{
  INIT_SUCC(ret);

  ObDatum *schema_datum = nullptr;
  ObExpr *schema_arg = expr.args_[index];
  ObObjType type = schema_arg->datum_meta_.type_;
  ObCollationType cs_type = schema_arg->datum_meta_.cs_type_;
  if (OB_FAIL(schema_arg->eval(ctx, schema_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (type == ObNullType || schema_datum->is_null()) {
    is_null = true;
  } else if (!ob_is_string_type(type) && type != ObJsonType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input type error", K(type));
  } else if (lib::is_mysql_mode() && OB_FAIL(ObJsonExprHelper::ensure_collation(type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(type), K(cs_type));
  } else {
    ObString j_str;
    if (OB_FAIL(get_json_or_str_data(schema_arg, ctx, allocator, j_str, is_null))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_str));
    } else if (is_null) {
    } else {
      ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(type);
      ObJsonSchemaCache ctx_cache(&allocator);
      ObJsonSchemaCache* schema_cache = ObJsonExprHelper::get_schema_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
      schema_cache = ((schema_cache != nullptr) ? schema_cache : &ctx_cache);

      if (OB_SUCC(ret) && OB_FAIL(ObJsonExprHelper::find_and_add_schema_cache(schema_cache, j_schema, j_str, 1, j_in_type))) {
        LOG_WARN("invalid json schema", K(ret));
      }
    }
  }
  return ret;
}

int ObJsonExprHelper::get_json_doc(const ObExpr &expr, ObEvalCtx &ctx,
                                   common::ObArenaAllocator &allocator,
                                   uint16_t index, ObIJsonBase*& j_base,
                                   bool &is_null, bool need_to_tree,
                                   bool relax, bool preserve_dup)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[index];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObCollationType cs_type = json_arg->datum_meta_.cs_type_;

  bool is_oracle = lib::is_oracle_mode();
  bool allow_partial_update = false;

  if (OB_UNLIKELY(OB_FAIL(json_arg->eval(ctx, json_datum)))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (val_type == ObNullType || json_datum->is_null()) {
    is_null = true;
  } else if (val_type != ObJsonType && !ob_is_string_type(val_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("input type error", K(val_type));
  } else if (lib::is_mysql_mode() && OB_FAIL(ObJsonExprHelper::ensure_collation(val_type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(val_type), K(cs_type));
  } else if (ob_is_json(val_type)
      && OB_FAIL(ObJsonExprHelper::is_allow_partial_update(expr, ctx, json_datum->get_string(), allow_partial_update))) {
    LOG_WARN("get partial updaet setting fail", K(ret));
  } else if (allow_partial_update) {
    if (OB_FAIL(get_json_for_partial_update(expr, *json_arg, ctx, allocator, *json_datum, j_base))) {
      LOG_WARN("get_json_for_partial_update fail", K(ret), K(val_type));
    }
  } else {
    ObString j_str;
    if (OB_FAIL(get_json_or_str_data(json_arg, ctx, allocator, j_str, is_null))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_str));
    } else if (is_null) {
    } else {
      ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(val_type);
      ObJsonInType expect_type = need_to_tree ? ObJsonInType::JSON_TREE : j_in_type;
      bool relax_json = (lib::is_oracle_mode() && relax);
      uint32_t parse_flag = relax_json ? ObJsonParser::JSN_RELAXED_FLAG : 0;
      ADD_FLAG_IF_NEED(preserve_dup, parse_flag, ObJsonParser::JSN_PRESERVE_DUP_FLAG);
      if (is_oracle && j_str.length() == 0) {
        is_null = true;
      } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, j_str, j_in_type,
                                                  expect_type, j_base, parse_flag))) {
        LOG_WARN("fail to get json base", K(ret), K(j_in_type));
        if (is_oracle) {
          ret = OB_ERR_JSON_SYNTAX_ERROR;
        } else {
          ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
        }
      }
    }
  }
  return ret;
}

int ObJsonExprHelper::get_const_json_schema(const common::ObObj &data, const char* func_name,
                                            common::ObIAllocator *allocator, ObIJsonBase*& j_schema)
{
  INIT_SUCC(ret);
  ObObjType val_type = data.get_type();
  if (data.is_null()) {
    void *json_node_buf = allocator->alloc(sizeof(ObJsonNull));
    if (OB_ISNULL(json_node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    } else {
      ObJsonNull *null_node = static_cast<ObJsonNull*>(new(json_node_buf) ObJsonNull());
      if (OB_FAIL(ObJsonBaseFactory::transform(allocator, null_node, ObJsonInType::JSON_BIN, j_schema))) {
        LOG_WARN("failed: json tree to bin", K(ret));
      }
    }
  } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
    ObCollationType cs_type = data.get_collation_type();
    ObIJsonBase* j_base = nullptr;
    // whether it is Oracle or MySQL, only lowercase true/false is considered a Boolean value
    // so, use strict mode
    if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(data, val_type, allocator,
                                                                  cs_type, j_base, ObConv2JsonParam(false,
                                                                  data.has_lob_header(), false,
                                                                  lib::is_oracle_mode(), true, true)))) {
      if (ret == OB_ERR_NULL_VALUE || ret == OB_ERR_INVALID_JSON_TEXT) { // json_str is null
        if (lib::is_oracle_mode()) {
          ret = OB_ERR_JSON_SYNTAX_ERROR;
        } else {
          ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
        }
      }
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    } else if (j_base->json_type() != common::ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_TYPE_OF_JSON_SCHEMA;
      LOG_WARN("json schema must be object", K(ret), K(j_base->json_type()));
    } else {
      ObJsonSchemaTree json_schema(allocator);
      ObJsonArray* schema_map = nullptr;
      if (OB_FAIL(json_schema.build_schema_tree(j_base))) {
        LOG_WARN("invalid json schema", K(ret));
      } else if (OB_ISNULL(schema_map = json_schema.get_schema_map())) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("should not be null", K(ret));
      } else if (OB_FAIL(ObJsonBaseFactory::transform(allocator, schema_map, ObJsonInType::JSON_BIN, j_schema))) {
        LOG_WARN("failed: json schema to bin", K(ret));
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 1, func_name);
  }
  return ret;
}

int ObJsonExprHelper::get_partial_json_bin(
    ObIAllocator &allocator,
    ObILobCursor *cursor,
    ObJsonBinUpdateCtx *update_ctx,
    ObIJsonBase *&j_base)
{
  INIT_SUCC(ret);
  ObJsonBinCtx *bin_ctx = nullptr;
  ObJsonBin *j_bin = nullptr;
  if (OB_ISNULL(cursor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cursor is null", KR(ret));
  } else if (OB_ISNULL(update_ctx) && OB_ISNULL(update_ctx = OB_NEWx(ObJsonBinUpdateCtx, &allocator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc update ctx fail", K(ret));
  } else if (OB_FALSE_IT(update_ctx->set_lob_cursor(cursor))) {
  // build json bin
  } else if (OB_ISNULL(bin_ctx = OB_NEWx(ObJsonBinCtx, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc ctx fail", K(ret), K(sizeof(ObJsonBinCtx)));
  } else if (OB_FALSE_IT(bin_ctx->update_ctx_ = update_ctx)) {
  } else if (OB_FALSE_IT(bin_ctx->is_update_ctx_alloc_ = true)) {
  } else if (OB_ISNULL(j_bin = OB_NEWx(ObJsonBin, &allocator, &allocator, bin_ctx, true))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc update ctx fail", K(ret));
  } else if (OB_FAIL(j_bin->reset_iter())) {
    LOG_WARN("fail to reset iter", K(ret));
  } else if (OB_FALSE_IT(j_bin->set_seek_flag(false))) {
  } else {
    j_base = j_bin;
  }
  return ret;
}

int ObJsonExprHelper::get_json_for_partial_update(
    const ObExpr &expr,
    const ObExpr &json_expr,
    ObEvalCtx &ctx,
    ObIAllocator &allocator,
    ObDatum &json_datum,
    ObIJsonBase *&j_base)
{
  INIT_SUCC(ret);
  ObString lob_str = json_datum.get_string();
  ObLobLocatorV2 locator(lob_str);
  bool allow_partial_update = false;
  ObString metas;
  storage::ObLobCursor *cursor = nullptr;
  int64_t query_timeout_ts = ObTimeUtility::current_time() + 60 * USECS_PER_SEC;
  storage::ObLobManager *lob_mgr = MTL(storage::ObLobManager*);
  uint8_t root_type = 0;
  if (lob_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob locator is empty", KR(ret));
  } else if (OB_FAIL(get_session_query_timeout_ts(ctx, query_timeout_ts))) {
    LOG_WARN("get_session_query_timeout fail", K(ret), K(locator));
  } else if (locator.is_delta_temp_lob()) {
    ObJsonDeltaLob delta_lob;
    if (OB_FAIL(delta_lob.init(&allocator, locator, query_timeout_ts))) {
      LOG_WARN("init json delta lob fail", K(ret), K(locator));
    } else {
      j_base = delta_lob.get_json_bin();
    }
    if (OB_FAIL(ret)) {
      delta_lob.reset();
    }
  } else if (! locator.is_persist_lob() || locator.is_inrow()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("persis lob or no-delta inrow lob locator not support", KR(ret), K(locator));
  } else if (OB_FAIL(lob_mgr->query(&allocator, locator, query_timeout_ts, false, nullptr, cursor))) {
    LOG_WARN("build lob cursor fail", K(ret), K(locator));
  } else if (OB_FAIL(cursor->read_i8(0, reinterpret_cast<int8_t*>(&root_type)))) {
    LOG_WARN("read root type fail", KR(ret), KPC(cursor));
  } else if (! ObJsonBin::is_doc_header(root_type)) {
    // if root type not doc header, means that old json data.
    // old json is not binary charset in lob, so can not use partial lob
    ObString j_str;
    if (cursor->has_one_chunk_with_all_data()) {
      LOG_DEBUG("one chunk will all data", K(lob_str), K(root_type), KPC(cursor), K(json_datum), K(json_expr));
      if (OB_FAIL(cursor->get_one_chunk_with_all_data(j_str))) {
        LOG_WARN("get real data fail", KR(ret), K(json_datum), K(json_expr));
      }
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, json_datum,
                json_expr.datum_meta_, json_expr.obj_meta_.has_lob_header(), j_str, &ctx.exec_ctx_))) {
      LOG_WARN("get real data fail", KR(ret), K(json_datum), K(json_expr));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(
          &allocator, j_str, ObJsonInType::JSON_BIN, ObJsonInType::JSON_TREE, j_base, 0))) {
      LOG_WARN("get json base fail", K(ret), K(j_str));
    }
    cursor->~ObLobCursor();
    cursor = nullptr;
  } else if (OB_FAIL(get_partial_json_bin(allocator, cursor, nullptr, j_base))) {
    LOG_WARN("fail to reset iter", K(ret));
  } else if (OB_ISNULL(j_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get j_base is null", KR(ret), K(locator));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(j_base)) {
      j_base->reset();
      j_base = nullptr;
    }
    if (OB_NOT_NULL(cursor)) {
      cursor->~ObLobCursor();
      cursor = nullptr;
    }
  }
  return ret;
}

int ObJsonExprHelper::refine_range_json_value_const(
  const common::ObObj &data, ObExecContext *ctx,
  bool is_bool, common::ObIAllocator *allocator,
  ObIJsonBase*& j_base, bool to_bin)
{
  int ret = OB_SUCCESS;
  ObObjType val_type = data.get_type();
  ObCollationType cs_type = data.get_collation_type();

  if (!ObJsonExprHelper::is_convertible_to_json(val_type)) {
    ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
  } else if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(data, val_type, allocator,
                                                                cs_type, j_base, ObConv2JsonParam(to_bin,
                                                                data.has_lob_header(), false, false, true, true)))) {
    LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
  }

  return ret;
}

int ObJsonExprHelper::get_json_val(const common::ObObj &data, ObExecContext *ctx,
                                   bool is_bool, common::ObIAllocator *allocator,
                                   ObIJsonBase*& j_base, bool to_bin)
{
  INIT_SUCC(ret);
  ObObjType val_type = data.get_type();
  if (data.is_null()) {
    void *json_node_buf = allocator->alloc(sizeof(ObJsonNull));
    if (OB_ISNULL(json_node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    } else {
      ObJsonNull *null_node = static_cast<ObJsonNull*>(new(json_node_buf) ObJsonNull());
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, null_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = null_node;
      }
    }
  } else if (is_bool) {
    void *json_node_buf = allocator->alloc(sizeof(ObJsonBoolean));
    if (OB_ISNULL(json_node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    } else {
      ObJsonBoolean *bool_node = (ObJsonBoolean*)new(json_node_buf)ObJsonBoolean(data.get_bool());
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, bool_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = bool_node;
      }
    }
  } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
    ObCollationType cs_type = data.get_collation_type();
    if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(data, val_type, allocator,
                                                                  cs_type, j_base, ObConv2JsonParam(to_bin,
                                                                  data.has_lob_header(), false, false, true, true)))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  } else {
    ObBasicSessionInfo *session = ctx->get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(data, val_type,
                                                                    allocator, data.get_scale(),
                                                                    session->get_timezone_info(),
                                                                    session,
                                                                    j_base, to_bin))) {
        LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  }

  return ret;
}

int ObJsonExprHelper::get_json_val(const common::ObDatum &data,
                                   ObExecContext &ctx,
                                   ObExpr* expr,
                                   common::ObIAllocator *allocator,
                                   ObObjType val_type,
                                   ObCollationType &cs_type,
                                   ObIJsonBase*& j_base, bool to_bin)
{
  INIT_SUCC(ret);
  if (data.is_null()) {
    void *json_node_buf = allocator->alloc(sizeof(ObJsonNull));
    if (OB_ISNULL(json_node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    } else {
      ObJsonNull *null_node = static_cast<ObJsonNull*>(new(json_node_buf) ObJsonNull());
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, null_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = null_node;
      }
    }
  } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
    if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(data, val_type, allocator,
                                                                  cs_type, j_base, ObConv2JsonParam(to_bin,
                                                                  expr->obj_meta_.has_lob_header(),
                                                                  false, lib::is_oracle_mode(), true)))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  } else {
    ObBasicSessionInfo *session = ctx.get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(data, val_type,
                                                                    allocator, expr->datum_meta_.scale_,
                                                                    session->get_timezone_info(),
                                                                    session,
                                                                    j_base, to_bin,
                                                                    expr->is_boolean_))) {
        LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  }

  return ret;
}

int ObJsonExprHelper::cast_to_json_tree(ObString &text, common::ObIAllocator *allocator, uint32_t parse_flag)
{
  INIT_SUCC(ret);
  ObJsonNode *j_tree = NULL;
  if (OB_FAIL(ObJsonParser::get_tree(allocator, text, j_tree, parse_flag))) {
    LOG_WARN("get json tree fail", K(ret));
  } else {
    ObJsonBuffer jbuf(allocator);
    if (OB_FAIL(j_tree->print(jbuf, true, false, 0))) {
      LOG_WARN("json binary to string failed", K(ret));
    } else if (jbuf.empty()) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for result failed", K(ret));
    } else {
      text.assign_ptr(jbuf.ptr(), jbuf.length());
    }
  }
  return ret;
}

int ObJsonExprHelper::cast_to_res(ObIAllocator &allocator,
                                  ObDatum &src_datum,
                                  const ObExpr &expr,
                                  const ObExpr &default_expr,
                                  ObEvalCtx &ctx,
                                  ObDatum &res,
                                  bool xt_need_acc_check)
{
  int ret = OB_SUCCESS;
  ObCastMode def_cm = CM_NONE;
  ObSQLSessionInfo *session = NULL;
  ObObj dst_obj, buf_obj, src_obj;
  const ObObj *res_obj = NULL;
  ObAccuracy out_acc;
  if (src_datum.is_null()) {
    res.set_null();
  } else if (OB_FAIL(src_datum.to_obj(src_obj, default_expr.obj_meta_, default_expr.obj_datum_map_))) {
    LOG_WARN("fail cast datum to obj", K(ret));
  } else {
    // to type
    if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sessioninfo is NULL");
    } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                                  session, def_cm))) {
      LOG_WARN("get_default_cast_mode failed", K(ret));
    } else {
      ObObjType obj_type = expr.datum_meta_.type_;
      ObCollationType cs_type = expr.datum_meta_.cs_type_;
      ObPhysicalPlanCtx *phy_plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
      ObCastCtx cast_ctx(&allocator, &dtc_params, get_cur_time(phy_plan_ctx), def_cm,
                         cs_type, NULL, NULL);
      if (OB_FAIL(ObObjCaster::to_type(obj_type, cs_type, cast_ctx, src_obj, dst_obj))) {
        LOG_WARN("failed to cast object to ", K(ret), K(src_obj), K(obj_type));
      } else if (FALSE_IT(get_accuracy_from_expr(expr, out_acc))) {
      } else if (FALSE_IT(res_obj = &dst_obj)) {
      } else if (OB_FAIL(obj_accuracy_check(cast_ctx, out_acc, cs_type, dst_obj, buf_obj, res_obj))) {
        if (!xt_need_acc_check && (ob_is_varchar_or_char(obj_type, cs_type) || ob_is_nchar(obj_type)) && ret == OB_ERR_DATA_TOO_LONG) {
          ObLengthSemantics ls = lib::is_oracle_mode() ?
                                 expr.datum_meta_.length_semantics_ : LS_CHAR;
          const char* str = dst_obj.get_string_ptr();
          int32_t str_len_byte = dst_obj.get_string_len();
          int64_t char_len = 0;
          int32_t trunc_len_byte = 0;
          trunc_len_byte = (ls == LS_BYTE ?
                ObCharset::max_bytes_charpos(cs_type, str, str_len_byte,
                                             expr.max_length_, char_len):
                ObCharset::charpos(cs_type, str, str_len_byte, expr.max_length_));
          if (trunc_len_byte == 0) {
            (const_cast<ObObj*>(res_obj))->set_null();
          } else {
            (const_cast<ObObj*>(res_obj))->set_common_value(ObString(trunc_len_byte, str));
          }
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("accuracy check failed", K(ret), K(out_acc), K(res_obj));
        }
      } else if (OB_FAIL(ObSPIService::spi_pad_char_or_varchar(session, obj_type, out_acc, &allocator, const_cast<ObObj *>(res_obj)))) {
        LOG_WARN("fail to pad char", K(ret), K(*res_obj));
      }

      if (OB_SUCC(ret)) {
        if (OB_NOT_NULL(res_obj)) {
          res.from_obj(*res_obj);
          ObExprStrResAlloc res_alloc(expr, ctx);
          if (OB_FAIL(res.deep_copy(res, res_alloc))) {
            LOG_WARN("fail to deep copy for res datum", K(ret), KPC(res_obj), K(res));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("res obj is NULL", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObJsonExprHelper::get_accuracy_from_expr(const ObExpr &expr, ObAccuracy &accuracy)
{
  accuracy.set_length(expr.max_length_);
  accuracy.set_scale(expr.datum_meta_.scale_);
  const ObObjTypeClass &dst_tc = ob_obj_type_class(expr.datum_meta_.type_);
  if (ObStringTC == dst_tc || ObTextTC == dst_tc) {
    accuracy.set_length_semantics(expr.datum_meta_.length_semantics_);
  } else {
    accuracy.set_precision(expr.datum_meta_.precision_);
  }
}

int ObJsonExprHelper::get_json_val(const ObExpr &expr, ObEvalCtx &ctx,
                                   common::ObIAllocator *allocator, uint16_t index,
                                   ObIJsonBase*& j_base, bool to_bin, bool format_json,
                                   uint32_t parse_flag)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[index];
  ObObjType val_type = json_arg->datum_meta_.type_;
  if (OB_UNLIKELY(OB_FAIL(json_arg->eval(ctx, json_datum)))) {
    LOG_WARN("eval json arg failed", K(ret), K(val_type));
  } else if (json_datum->is_null()) {
    void *json_node_buf = allocator->alloc(sizeof(ObJsonNull));
    if (OB_ISNULL(json_node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    } else {
      ObJsonNull *null_node = static_cast<ObJsonNull*>(new(json_node_buf) ObJsonNull());
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, null_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = null_node;
      }
    }  
  } else if (json_arg->is_boolean_ == 1) {
    void *json_node_buf = allocator->alloc(sizeof(ObJsonBoolean));
    if (OB_ISNULL(json_node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    } else {
      ObJsonBoolean *bool_node = (ObJsonBoolean*)new(json_node_buf)ObJsonBoolean(json_datum->get_bool());
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, bool_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = bool_node;
      }
    }
  } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
    ObCollationType cs_type = json_arg->datum_meta_.cs_type_;
    if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(*json_datum, val_type,
                                                                  allocator, cs_type,
                                                                  j_base, ObConv2JsonParam(to_bin,
                                                                  json_arg->obj_meta_.has_lob_header(),
                                                                  false,
                                                                  HAS_FLAG(parse_flag, ObJsonParser::JSN_RELAXED_FLAG),
                                                                  format_json)))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  } else {
    ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
    ObScale scale = json_arg->datum_meta_.scale_;
    scale = (val_type == ObBitType) ? json_arg->datum_meta_.length_semantics_ : scale;
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(*json_datum, val_type,
                                                                    allocator, scale,
                                                                    session->get_timezone_info(),
                                                                    session,
                                                                    j_base, to_bin))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(j_base)) {
    j_base->set_allocator(allocator);
  }
  return ret;
}

int ObJsonExprHelper::eval_oracle_json_val(ObExpr *expr,
                                           ObEvalCtx &ctx,
                                           common::ObIAllocator *allocator,
                                           ObIJsonBase*& j_base,
                                           bool is_format_json,
                                           bool is_strict,
                                           bool is_bin,
                                           bool is_absent_null)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = nullptr;
  ObExpr *json_arg = expr;
  bool is_bool_data_type = (json_arg->is_boolean_ || json_arg->datum_meta_.type_ == ObTinyIntType);

  if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret), K(json_arg->datum_meta_));
  } else if ((json_datum->is_null() || ob_is_null(json_arg->obj_meta_.get_type()))
             && is_absent_null) {
  } else if (OB_FAIL(oracle_datum2_json_val(json_datum,
                                            json_arg->obj_meta_,
                                            allocator,
                                            ctx.exec_ctx_.get_my_session(),
                                            j_base,
                                            is_bool_data_type,
                                            is_format_json,
                                            is_strict, is_bin))) {
    LOG_WARN("failed to wrapper json base", K(ret), K(json_arg->datum_meta_),
             K(is_format_json), K(is_strict), K(is_bin));
  }

  return ret;
}

int ObJsonExprHelper::oracle_datum2_json_val(const ObDatum *json_datum,
                                             ObObjMeta& data_meta,
                                             common::ObIAllocator *allocator,
                                             ObBasicSessionInfo *session,
                                             ObIJsonBase*& j_base,
                                             bool is_bool_data_type,
                                             bool is_format_json,
                                             bool is_strict,
                                             bool is_bin)
{
  INIT_SUCC(ret);
  ObObjType val_type = data_meta.get_type();
  ObCollationType cs_type = data_meta.get_collation_type();
  bool is_nchar = (val_type == ObNCharType || val_type == ObNVarchar2Type);
  bool is_raw_type = (val_type == ObRawType);

  if (json_datum->is_null() || ob_is_null(val_type)) {
    ObJsonNull *null_node = nullptr;
    if (OB_ISNULL(null_node = OB_NEWx(ObJsonNull, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    }
    j_base = null_node;
  } else if (is_bool_data_type) {
    ObJsonBoolean *bool_node = nullptr;
    if (OB_ISNULL(bool_node = OB_NEWx(ObJsonBoolean, allocator, (json_datum->get_bool())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    }
    j_base = bool_node;
  } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
    if (val_type == ObVarcharType
        || val_type == ObCharType
        || val_type == ObTinyTextType
        || val_type == ObTextType
        || val_type == ObMediumTextType
        || val_type == ObLongTextType
        || is_raw_type
        || is_nchar) {

      uint32_t parse_flag = ObJsonParser::JSN_RELAXED_FLAG;
      ObString j_str = json_datum->get_string();
      ObString out_str;
      bool need_convert = (cs_type != CS_TYPE_INVALID && cs_type != CS_TYPE_BINARY);
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                    allocator, val_type, cs_type, data_meta.has_lob_header(), j_str))) {
        LOG_WARN("fail to get real data.", K(ret), K(j_str));
      } else if (need_convert && OB_FAIL(ObExprUtil::convert_string_collation(j_str, cs_type, out_str, CS_TYPE_UTF8MB4_BIN, *allocator))) {
        LOG_WARN("fail to convert charset.", K(ret), K(j_str), K(cs_type));
      } else if ((!need_convert || out_str.ptr() == j_str.ptr()) && OB_FAIL(deep_copy_ob_string(*allocator, j_str, out_str))) {
        LOG_WARN("fail to deep copy string.", K(ret), K(j_str));
      } else if (FALSE_IT(j_str.assign_ptr(out_str.ptr(), out_str.length()))) {
      } else if (!is_format_json || is_raw_type) {
        if (is_raw_type) {
          ObObj tmp_result;
          ObObj obj;

          ObDatum tmp_datum = *json_datum;
          tmp_datum.set_string(j_str);
          ObCastCtx cast_ctx(allocator, nullptr, CM_NONE, CS_TYPE_INVALID);

          if (OB_FAIL(tmp_datum.to_obj(obj, data_meta))) {
            LOG_WARN("datum to obj fail", K(ret));
          } else if (OB_FAIL(ObHexUtils::rawtohex(obj, cast_ctx, tmp_result))) {
            LOG_WARN("fail to check json syntax", K(ret), K(data_meta));
          } else {
            j_str = tmp_result.get_string();
          }
        }
        ObJsonString* string_node = nullptr;
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(string_node = OB_NEWx(ObJsonString, allocator, j_str.ptr(), j_str.length()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed: alloscate json string node", K(ret));
        } else {
          j_base = string_node;
        }
      } else {
        if (OB_FAIL(ObJsonParser::check_json_syntax(j_str, allocator, parse_flag))) {
          if (!is_strict) {
            ret = OB_SUCCESS;
            ObJsonString* string_node = nullptr;
            if (OB_ISNULL(string_node = OB_NEWx(ObJsonString, allocator, j_str.ptr(), j_str.length()))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed: alloscate json string node", K(ret));
            }
            j_base = string_node;
          } else {
            ret = OB_ERR_JSON_SYNTAX_ERROR;
            LOG_WARN("fail to check json syntax", K(ret), K(j_str));
          }
        } else if (OB_FAIL(ObJsonBaseFactory::get_json_base( allocator, j_str,
                                   ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base, parse_flag))) {
          LOG_WARN("failed: parse json string node", K(ret), K(j_str));
        }
      }
    } else if (val_type == ObJsonType) {
      ObJsonInType to_type = is_bin ? ObJsonInType::JSON_BIN : ObJsonInType::JSON_TREE;
      ObString j_str = json_datum->get_string();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                    allocator, val_type, cs_type, data_meta.has_lob_header(), j_str))) {
        LOG_WARN("fail to get real data.", K(ret), K(j_str));
      } else if (OB_FAIL(deep_copy_ob_string(*allocator, j_str, j_str))) {
        LOG_WARN("fail to deep copy string.", K(ret), K(j_str));
      } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, ObJsonInType::JSON_BIN, to_type, j_base))) {
        ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
        LOG_WARN("fail to get json base", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument", K(ret), K(val_type));
    }

    if (OB_SUCC(ret)) {

      if (OB_FAIL(ObJsonBaseFactory::transform(allocator, j_base,
                      is_bin ? ObJsonInType::JSON_BIN : ObJsonInType::JSON_TREE , j_base))) {
        LOG_WARN("failed: json tree to bin", K(ret));
      } else {
        j_base->set_allocator(allocator);
      }
    }
  } else {
    ObScale scale = data_meta.get_scale();
    if (is_format_json) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("input type error", K(val_type));
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (val_type == ObIntervalDSType || val_type == ObIntervalYMType) {
      // internal json type not support interval type using string type instead
      int64_t len = 0;
      char* string_buf = nullptr;
      ObJsonString* string_node = nullptr;

      if (OB_ISNULL(string_buf = static_cast<char*>(allocator->alloc(OB_CAST_TO_VARCHAR_MAX_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (ob_is_interval_ym(val_type)) {
        ObIntervalYMValue in_val(json_datum->get_interval_nmonth());
        if (OB_FAIL(ObTimeConverter::interval_ym_to_str(
                      in_val, scale, string_buf, OB_CAST_TO_VARCHAR_MAX_LENGTH, len, true))) {
          LOG_WARN("interval_ym_to_str failed", K(ret));
        }
      } else {
        ObIntervalDSValue in_val(json_datum->get_interval_ds());
        if (OB_FAIL(ObTimeConverter::interval_ds_to_str(
                      in_val, scale, string_buf, OB_CAST_TO_VARCHAR_MAX_LENGTH, len, true))) {
          LOG_WARN("interval_ym_to_str failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(string_node = OB_NEWx(ObJsonString, allocator, (const char*)string_buf, len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create json string node", K(ret));
      } else {
        j_base = string_node;
      }
    } else if (val_type == ObTimestampTZType || val_type == ObTimestampLTZType) {
      // internal json type not support time zone or local time zone type using string type instead
      char* string_buf = nullptr;
      int64_t len = 0;
      ObJsonString* string_node = nullptr;

      if (OB_ISNULL(string_buf = static_cast<char*>(allocator->alloc(OB_CAST_TO_VARCHAR_MAX_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        ObOTimestampData in_val;
        ObScale scale = data_meta.get_scale();
        const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
        if (OB_FAIL(common_construct_otimestamp(val_type, *json_datum, in_val))) {
          LOG_WARN("common_construct_otimestamp failed", K(ret));
        } else if (OB_FAIL(ObTimeConverter::otimestamp_to_str(in_val, dtc_params, scale,
                                                              val_type, string_buf, OB_CAST_TO_VARCHAR_MAX_LENGTH, len))) {
          LOG_WARN("failed to convert otimestamp to string", K(ret));
        } else if (OB_ISNULL(string_node = OB_NEWx(ObJsonString, allocator, (const char*)string_buf, len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create json string node", K(ret));
        } else {
          j_base = string_node;
        }
      }
    } else if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(*json_datum, val_type,
                                                                    allocator, scale,
                                                                    session->get_timezone_info(),
                                                                    session,
                                                                    j_base,
                                                                    is_bin))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  }
  return ret;
}

int ObJsonExprHelper::convert_string_collation_type(ObCollationType in_cs_type,
                                                    ObCollationType dst_cs_type,
                                                    ObIAllocator* allocator,
                                                    ObString &in_str,
                                                    ObString &out_str)
{
  INIT_SUCC(ret);

  bool is_need_convert = ((CS_TYPE_BINARY == dst_cs_type)
          || (ObCharset::charset_type_by_coll(in_cs_type) != ObCharset::charset_type_by_coll(dst_cs_type)));

  if (is_need_convert) {
    if (CS_TYPE_BINARY != in_cs_type && CS_TYPE_BINARY != dst_cs_type
        && (ObCharset::charset_type_by_coll(in_cs_type) != ObCharset::charset_type_by_coll(dst_cs_type))) {
      char *buf = nullptr;
      int64_t buf_len = (in_str.length() == 0 ? 1 : in_str.length()) * ObCharset::CharConvertFactorNum;
      uint32_t result_len = 0;

      if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator->alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(buf_len));
      } else if (OB_FAIL(ObCharset::charset_convert(in_cs_type,
                                                    in_str.ptr(),
                                                    in_str.length(),
                                                    dst_cs_type,
                                                    buf,
                                                    buf_len,
                                                    result_len))) {
        LOG_WARN("charset convert failed", K(ret));
      } else {
        out_str.assign_ptr(buf, result_len);
      }
    } else {
      if (CS_TYPE_BINARY == in_cs_type || CS_TYPE_BINARY == dst_cs_type) {
        // just copy string when in_cs_type or out_cs_type is binary
        const ObCharsetInfo *cs = NULL;
        int64_t align_offset = 0;
        if (CS_TYPE_BINARY == in_cs_type && (NULL != (cs = ObCharset::get_charset(dst_cs_type)))) {
          if (cs->mbminlen > 0 && in_str.length() % cs->mbminlen != 0) {
            align_offset = cs->mbminlen - in_str.length() % cs->mbminlen;
          }
        }
        int64_t len = align_offset + in_str.length();
        char *buf = nullptr;

        if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(len));
        } else {
          MEMMOVE(buf + align_offset, in_str.ptr(), len - align_offset);
          MEMSET(buf, 0, align_offset);
          out_str.assign_ptr(buf, len);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("same charset should not be here, just use cast_eval_arg", K(ret),
                  K(in_cs_type), K(dst_cs_type), K(in_cs_type), K(dst_cs_type));
      }
    }
  } else {
    out_str = in_str;
  }

  return ret;
}

int ObJsonExprHelper::json_base_replace(ObIJsonBase *json_old, ObIJsonBase *json_new,
                                        ObIJsonBase *&json_doc)
{
  INIT_SUCC(ret);
  ObIAllocator *allocator = nullptr;
  ObIJsonBase *parent = nullptr;
  ObIJsonBase *new_node = json_new;

  if (OB_ISNULL(json_old) || OB_ISNULL(json_new) || OB_ISNULL(json_doc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json_new or json_old or json_doc is null", K(ret), KP(json_old), KP(json_new), KP(json_doc));
  } else if (OB_ISNULL(allocator = json_doc->get_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret), KPC(json_doc));
  } else if (OB_FAIL(json_old->get_parent(parent))) {
    LOG_WARN("get old parent fail", K(ret));
  } else if (json_old == json_doc || OB_ISNULL(parent)) {
    // for json binary, json_old and json_doc may be at same position,
    if (OB_FAIL(json_doc->reset())) {
      LOG_WARN("reset fail", K(ret), KPC(json_doc));
    } else {
      json_doc = json_new;
      // json_new may no allocator, so update
      if (OB_ISNULL(json_doc->get_allocator())) {
        json_doc->set_allocator(allocator);
      }
    }
  } else {
    if (json_doc->is_bin()) {
      if (OB_NOT_NULL(json_new) && ! json_new->is_bin() && OB_FAIL(ObJsonBaseFactory::transform(allocator, json_new, ObJsonInType::JSON_BIN, new_node))) {
        LOG_WARN("fail to transform to tree", K(ret), K(json_new));
      }
    } else {
      if (OB_NOT_NULL(json_new) && ! json_new->is_tree() && ObJsonBaseFactory::transform(allocator, json_new, ObJsonInType::JSON_TREE, new_node)) {
        LOG_WARN("fail to transform to tree", K(ret), K(*json_old));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parent->replace(json_old, new_node))) {
      LOG_WARN("json base replace failed", K(ret));
    } else if (OB_FAIL(refresh_root_when_bin_rebuild_all(json_doc))) {
      LOG_WARN("refresh_root_when_bin_rebuild_all fail", K(ret));
    }
  }
  return ret;
}

// get json expr param cache context, if not exists cache context do nothing
ObJsonParamCacheCtx* ObJsonExprHelper::get_param_cache_ctx(const uint64_t& id, ObExecContext *exec_ctx)
{
  INIT_SUCC(ret);
  ObJsonParamCacheCtx* cache_ctx = NULL;
  if (ObExpr::INVALID_EXP_CTX_ID != id) {
    cache_ctx = static_cast<ObJsonParamCacheCtx*>(exec_ctx->get_expr_op_ctx(id));
    if (OB_ISNULL(cache_ctx)) {
      // if pathcache not exist, create one
      void *cache_ctx_buf = NULL;
      ret = exec_ctx->create_expr_op_ctx(id, sizeof(ObJsonParamCacheCtx), cache_ctx_buf);
      if (OB_SUCC(ret) && OB_NOT_NULL(cache_ctx_buf)) {
        cache_ctx = new (cache_ctx_buf) ObJsonParamCacheCtx(&exec_ctx->get_allocator());
      }
    }
  }
  return (cache_ctx == NULL) ? NULL : cache_ctx;
}

// get json expr path cache context, if not exists cache context do nothing
ObJsonPathCache* ObJsonExprHelper::get_path_cache_ctx(const uint64_t& id, ObExecContext *exec_ctx)
{
  INIT_SUCC(ret);
  ObJsonPathCacheCtx* cache_ctx = NULL;
  if (ObExpr::INVALID_EXP_CTX_ID != id) {
    cache_ctx = static_cast<ObJsonPathCacheCtx*>(exec_ctx->get_expr_op_ctx(id));
    if (OB_ISNULL(cache_ctx)) {
      // if pathcache not exist, create one
      void *cache_ctx_buf = NULL;
      ret = exec_ctx->create_expr_op_ctx(id, sizeof(ObJsonPathCacheCtx), cache_ctx_buf);
      if (OB_SUCC(ret) && OB_NOT_NULL(cache_ctx_buf)) {
        cache_ctx = new (cache_ctx_buf) ObJsonPathCacheCtx(&exec_ctx->get_allocator());
      }
    }
  }
  return (cache_ctx == NULL) ? NULL : cache_ctx->get_path_cache();
}

// get json expr schema cache context, if not exists cache context do nothing
ObJsonSchemaCache* ObJsonExprHelper::get_schema_cache_ctx(const uint64_t& id, ObExecContext *exec_ctx)
{
  INIT_SUCC(ret);
  ObJsonSchemaCacheCtx* cache_ctx = NULL;
  if (ObExpr::INVALID_EXP_CTX_ID != id) {
    cache_ctx = static_cast< ObJsonSchemaCacheCtx*>(exec_ctx->get_expr_op_ctx(id));
    if (OB_ISNULL(cache_ctx)) {
      // if pathcache not exist, create one
      void *cache_ctx_buf = NULL;
      ret = exec_ctx->create_expr_op_ctx(id, sizeof( ObJsonSchemaCacheCtx), cache_ctx_buf);
      if (OB_SUCC(ret) && OB_NOT_NULL(cache_ctx_buf)) {
        cache_ctx = new (cache_ctx_buf)  ObJsonSchemaCacheCtx(&exec_ctx->get_allocator());
      }
    }
  }
  return (cache_ctx == NULL) ? NULL : cache_ctx->get_schema_cache();
}

int ObJsonExprHelper::find_and_add_schema_cache(ObJsonSchemaCache* schema_cache, ObIJsonBase*& res_schema,
                                      ObString& schema_str, int arg_idx, const ObJsonInType& in_type)
{
  INIT_SUCC(ret);
  if (OB_FAIL(schema_cache->find_and_add_cache(res_schema, schema_str, arg_idx, in_type))) {
    if (ret == OB_ERR_INVALID_JSON_TEXT) {
      if (lib::is_oracle_mode()) {
        ret = OB_ERR_JSON_SYNTAX_ERROR;
      } else {
        ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
      }
    }
  }
  return ret;
}

int ObJsonExprHelper::find_and_add_cache(ObJsonPathCache* path_cache, ObJsonPath*& res_path,
                                         ObString& path_str, int arg_idx, bool enable_wildcard,
                                         bool is_const)
{
  INIT_SUCC(ret);
  if (OB_FAIL(path_cache->find_and_add_cache(res_path, path_str, arg_idx, is_const))) {
    ret = OB_ERR_INVALID_JSON_PATH;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
  } else if (!enable_wildcard && res_path->can_match_many()) {
    ret = OB_ERR_INVALID_JSON_PATH_WILDCARD;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH_WILDCARD);
  }
  return ret;
}

bool ObJsonExprHelper::is_convertible_to_json(ObObjType &type)
{
  bool val = false;
  switch (type) {
    case ObNullType:
    case ObJsonType:
    case ObVarcharType:
    case ObCharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLobType:
    case ObRawType:
    case ObLongTextType: {
      val = true;
      break;
    }
    default:
      break;
  }
  if (lib::is_oracle_mode()) {
    switch (type) {
      case ObNVarchar2Type:
      case ObNCharType: {
        val = true;
        break;
      }
      default:
        break;
    }
  }
  return val;
}

int ObJsonExprHelper::is_valid_for_json(ObExprResType* types_stack,
                                        uint32_t index,
                                        const char* func_name)
{
  INIT_SUCC(ret);
  ObObjType in_type = types_stack[index].get_type();

  if (!is_convertible_to_json(in_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, index + 1, func_name);
  } else if (ob_is_string_type(in_type)) {
    if (types_stack[index].get_collation_type() == CS_TYPE_BINARY) {
      types_stack[index].set_calc_collation_type(CS_TYPE_BINARY);
    } else {
      if (types_stack[index].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
  }

  return ret;
}

int ObJsonExprHelper::is_valid_for_json(ObExprResType& type,
                                        uint32_t index,
                                        const char* func_name)
{
  INIT_SUCC(ret);
  ObObjType in_type = type.get_type();
  
  if (!is_convertible_to_json(in_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, index, func_name);
  } else if (ob_is_string_type(in_type) && type.get_collation_type() != CS_TYPE_BINARY) {
    if (type.get_charset_type() != CHARSET_UTF8MB4) {
      type.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  }

  return ret;
}

int ObJsonExprHelper::is_valid_for_path(ObExprResType* types_stack, uint32_t index)
{
  INIT_SUCC(ret);
  ObObjType in_type = types_stack[index].get_type();
  if (in_type == ObNullType || in_type == ObJsonType) {
  } else if (ob_is_string_type(in_type)) {
    if (types_stack[index].get_charset_type() != CHARSET_UTF8MB4) {
      types_stack[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  } else {
    ret = OB_ERR_INVALID_JSON_PATH;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
  }
  return ret;
}

void ObJsonExprHelper::set_type_for_value(ObExprResType* types_stack, uint32_t index)
{
  ObObjType in_type = types_stack[index].get_type();
  if (in_type == ObNullType) {
  } else if (ob_is_string_type(in_type)) {
    if (in_type == ObVarcharType && types_stack[index].get_collation_type() == CS_TYPE_BINARY) {
      types_stack[index].set_calc_type(ObHexStringType);
      types_stack[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else if (types_stack[index].get_charset_type() != CHARSET_UTF8MB4) {
      types_stack[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  } else if  (in_type == ObJsonType) {
    types_stack[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
  }
}

int ObJsonExprHelper::is_json_zero(const ObString& data, int& result)
{
  INIT_SUCC(ret);
  int tmp_result = 0;
  ObJsonBinCtx ctx;
  ObJsonBin j_bin(data.ptr(), data.length(), &ctx);
  if (data.length() == 0) {
    result = 1; 
  } else if (OB_FAIL(j_bin.reset_iter())) {
    LOG_WARN("failed: reset iter", K(ret));
  } else if (OB_FAIL(ObJsonBaseUtil::compare_int_json(0, &j_bin, tmp_result))) {
    LOG_WARN("failed: cmp json", K(ret));
  } else {
    result = (tmp_result == 0) ? 0 : 1;
  }
  return ret;
}

int ObJsonExprHelper::is_json_true(const ObString& data, int& result)
{
  INIT_SUCC(ret);
  int tmp_result = 0;
  ObJsonBinCtx ctx;
  ObJsonBin j_bin(data.ptr(), data.length(), &ctx);
  if (data.length() == 0) {
    result = 0;
  } else if (OB_FAIL(j_bin.reset_iter())) {
    LOG_WARN("failed: reset iter", K(ret));
  } else if ((j_bin.json_type() == ObJsonNodeType::J_OBJECT ||
             j_bin.json_type() == ObJsonNodeType::J_ARRAY ||
             j_bin.json_type() == ObJsonNodeType::J_STRING ||
             j_bin.json_type() == ObJsonNodeType::J_NULL ||
             j_bin.json_type() == ObJsonNodeType::J_BOOLEAN)) {
    result = 1;
  } else if ((j_bin.json_type() == ObJsonNodeType::J_INT || j_bin.json_type() == ObJsonNodeType::J_UINT)
             && OB_FAIL(ObJsonBaseUtil::compare_int_json(0, &j_bin, tmp_result))) {
    LOG_WARN("failed: cmp json", K(ret));
  } else if ((j_bin.json_type() == ObJsonNodeType::J_DECIMAL)
             && OB_FAIL(ObJsonBaseUtil::compare_decimal_uint(j_bin.get_decimal_data(), 0, tmp_result))) {
    LOG_WARN("failed: cmp json", K(ret));
  } else if ((j_bin.json_type() == ObJsonNodeType::J_DOUBLE)
             && OB_FAIL(ObJsonBaseUtil::compare_double_int(j_bin.get_double(), 0, tmp_result))) {
    LOG_WARN("failed: cmp json", K(ret));
  } else {
    result = (tmp_result == 0) ? 0 : 1;
  }
  return ret;
}

template <typename T>
int ObJsonExprHelper::transform_scalar_2jsonBase(const T &datum,
                                                 ObObjType type,
                                                 common::ObIAllocator *allocator,
                                                 ObScale scale,
                                                 const ObTimeZoneInfo *tz_info,
                                                 ObBasicSessionInfo *session,
                                                 ObIJsonBase*& j_base,
                                                 bool to_bin,
                                                 bool is_bool)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObIJsonBase* json_node = NULL;

  switch(type) {
    case ObTinyIntType: {
      // mysql boolean type 
      if (is_bool) {
        buf = allocator->alloc(sizeof(ObJsonBoolean));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("buf allocate failed", K(ret), K(type));
        } else {
          json_node = (ObJsonBoolean*)new(buf)ObJsonBoolean(datum.get_bool());
        }
      } else {
        buf = allocator->alloc(sizeof(ObJsonInt));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("buf allocate failed", K(ret), K(type));
        } else {
          json_node = (ObJsonInt*)new(buf)ObJsonInt(datum.get_int());
        }
      }
      break;
    }
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      buf = allocator->alloc(sizeof(ObJsonInt));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("buf allocate failed", K(ret), K(type));
      } else {
        json_node = (ObJsonInt*)new(buf)ObJsonInt(datum.get_int());
      }
      break;
    }
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type: {
      buf = allocator->alloc(sizeof(ObJsonUint));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("buf allocate failed", K(ret), K(type));
      } else {
        json_node = (ObJsonInt*)new(buf)ObJsonUint(datum.get_uint64());
      }
      break;
    }
    case ObYearType: {
      buf = allocator->alloc(sizeof(ObJsonInt));
      int64_t value = 0;
      if (OB_FAIL(ObTimeConverter::year_to_int(datum.get_year(), value))) {
        LOG_WARN("fail to get year data", K(ret));
      } else if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("buf allocate failed", K(ret), K(type));
      } else {
        json_node = (ObJsonInt*)new(buf)ObJsonInt(value);
      }
      break;
    }
    case ObDateTimeType:
    case ObTimestampType:
    case ObTimestampNanoType:
    case ObDateType:
    case ObTimeType: {
      ObTime ob_time;
      int64_t value = 0;
      ObJsonNodeType node_type;
      if (type == ObDateType) {
        node_type = ObJsonNodeType::J_DATE;
        value = datum.get_date();
        ob_time.mode_ = DT_TYPE_DATE;
        if (OB_FAIL(ObTimeConverter::date_to_ob_time(value, ob_time))) {
          LOG_WARN("date transform to ob time failed", K(ret), K(value));
        }
      } else if (type == ObTimeType) {
        node_type = ObJsonNodeType::J_TIME;
        value = datum.get_time();
        ob_time.mode_ = DT_TYPE_TIME;
        if (OB_FAIL(ObTimeConverter::time_to_ob_time(value, ob_time))) {
          LOG_WARN("time transform to ob time failed", K(ret), K(value));
        }
      } else if (type == ObDateTimeType) {
        node_type = ObJsonNodeType::J_DATETIME;
        value = datum.get_datetime();
        ob_time.mode_ = DT_TYPE_DATETIME;
        if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(value, nullptr, ob_time))) {
          LOG_WARN("datetime transform to ob time failed", K(ret), K(value));
        }
      } else {
        node_type = ObJsonNodeType::J_TIMESTAMP;
        if (lib::is_oracle_mode()) {
          ObOTimestampData in_val;
          char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
          int64_t len = 0;
          if (OB_ISNULL(session)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("session is NULL", K(ret));
          } else {
            const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
            if (OB_FAIL(get_otimestamp_from_datum(datum, in_val, type))) {
              LOG_WARN("get otimestamp fail", K(ret));
            } else if (OB_SUCC(ret) && OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(type, in_val, NULL, ob_time))) {
              LOG_WARN("fail to convert otimestamp to ob_time", K(ret), K(in_val));
            }
          }
        } else {
          value = datum.get_timestamp();
          ob_time.mode_ = DT_TYPE_DATETIME;
          if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(value, tz_info, ob_time))) {
            LOG_WARN("default transform : datetime to ob time failed", K(ret), K(value));
          }
        }
      }

      if (OB_SUCC(ret)) {
        buf = allocator->alloc(sizeof(ObJsonDatetime));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("buf allocate failed", K(ret));
        } else {
          json_node = (ObJsonDatetime *)new(buf)ObJsonDatetime(node_type, ob_time);
        }
      }
      break;
    }

    case ObFloatType:
    case ObDoubleType:
    case ObUFloatType:
    case ObUDoubleType: {
      double val;
      if (type == ObFloatType || type ==ObUFloatType ) {
        val = datum.get_float();
      } else {
        val = datum.get_double();
      }
      if (isnan(val)) {
        buf = allocator->alloc(sizeof(ObJsonString));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("buf allocate failed", K(ret), K(type));
        } else {
          json_node = (ObJsonString *)new(buf)ObJsonString("Nan", 3);
        }
      } else if (isinf(val) != 0) {
        buf = allocator->alloc(sizeof(ObJsonString));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("buf allocate failed", K(ret), K(type));
        } else {
          if (isinf(val) == 1) {
            json_node = (ObJsonString *)new(buf)ObJsonString("Inf", 3);
          } else {
            json_node = (ObJsonString *)new(buf)ObJsonString("-Inf", 4);
          }
        }
      } else {
        buf = allocator->alloc(sizeof(ObJsonDouble));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("buf allocate failed", K(ret), K(type));
        } else {
          json_node = (ObJsonDouble *)new(buf)ObJsonDouble(val);
        }
      }
      break;
    }
    case ObDecimalIntType:
    case ObUNumberType:
    case ObNumberFloatType:
    case ObNumberType: {
      // won't waster much memory, do deep copy num
      number::ObNumber num;
      buf = allocator->alloc(sizeof(ObJsonDecimal));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("buf allocate failed", K(ret), K(type));
      }
      if (OB_FAIL(ret)) {
      } else if (ob_is_decimal_int(type)) {
        if (OB_FAIL(wide::to_number(datum.get_decimal_int(), datum.get_int_bytes(), scale, *allocator, num))) {
          LOG_WARN("cast decimal int to number failed", K(ret));
        }
      } else if (OB_FAIL(num.deep_copy_v3(datum.get_number(), *allocator))) {
        LOG_WARN("num deep copy failed", K(ret), K(type));
      }
      if (OB_FAIL(ret)) {
      } else {
        // shadow copy
        json_node = (ObJsonDecimal *)new(buf)ObJsonDecimal(num, -1, scale);
      }
      break;
    }
    case ObHexStringType: {
      buf = allocator->alloc(sizeof(ObJsonOpaque));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("buf allocate failed", K(ret), K(type));
      } else {
        json_node = (ObJsonOpaque *)new(buf)ObJsonOpaque(datum.get_string(), type);
      }
      break;
    }
    case ObBitType: {
      // using bit as char array to do cast.
      uint64_t in_val = datum.get_uint64();
      char *bit_buf = nullptr;
      const int32_t bit_buf_len = (OB_MAX_BIT_LENGTH + 7) / 8;
      int64_t bit_buf_pos = 0;
      if (OB_ISNULL(bit_buf = static_cast<char*>(allocator->alloc(bit_buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate bit buf fail", K(ret), K(type), K(bit_buf_len));
      } else if (OB_FAIL(bit_to_char_array(in_val, scale, bit_buf, bit_buf_len, bit_buf_pos))) {
        LOG_WARN("bit_to_char_array fail", K(ret), K(in_val), K(scale), KP(bit_buf), K(bit_buf_len), K(bit_buf_pos));
      } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObJsonOpaque)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate ObJsonOpaque fail", K(ret), K(type), "size", sizeof(ObJsonOpaque));
      } else {
        json_node = (ObJsonOpaque *)new(buf)ObJsonOpaque(ObString(bit_buf_pos, bit_buf), type);
      }
      break;
    }
    case ObGeometryType: {
      ret = OB_ERR_INVALID_JSON_TEXT;
      LOG_WARN("Internal JSON error", K(ret), K(type));
      break;
    }
    case ObUserDefinedSQLType: {
      ret = OB_ERR_INVALID_CAST_TO_JSON;
      LOG_WARN("UDT transform to json is not supported currently", K(ret), K(type));
      break;
    }
    default:
    {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument", K(ret), K(type));
    }
  }

  if (OB_SUCC(ret)) {
    if (to_bin) {
      if (OB_FAIL(ObJsonBaseFactory::transform(allocator, json_node, ObJsonInType::JSON_BIN, j_base))) {
        LOG_WARN("failed: json tree to bin", K(ret));
      }
    } else {
      j_base = json_node;
    }
  }

  return ret;
}

#define PRINT_OB_DATETIME(ob_time, value, j_buf) \
  const int64_t tmp_buf_len = DATETIME_MAX_LENGTH + 1; \
  char tmp_buf[tmp_buf_len] = {0}; \
  int64_t pos = 0; \
  const int16_t print_scale = 6; \
  if (OB_FAIL(j_buf.append("\""))) { \
    LOG_WARN("fail to append \"", K(ret)); \
  } else if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(value, tz_info, ob_time))) { \
  } else if (OB_FAIL(ObTimeConverter::ob_time_to_str(ob_time, ob_time.mode_, print_scale,  \
                                                      tmp_buf, tmp_buf_len, pos, true))) { \
    LOG_WARN("fail to change time to string", K(ret), K(ob_time), K(pos)); \
  } else if (OB_FAIL(j_buf.append(tmp_buf))) { \
    LOG_WARN("fail to append date_buf to j_buf", K(ret), KCSTRING(tmp_buf)); \
  } else if (OB_FAIL(j_buf.append("\""))) { \
    LOG_WARN("fail to append \"", K(ret)); \
  }

#define PRINT_OB_TIME(ob_time, value, TO_OB_TIME_METHOD, j_buf) \
  const int64_t tmp_buf_len = DATETIME_MAX_LENGTH + 1; \
  char tmp_buf[tmp_buf_len] = {0}; \
  int64_t pos = 0; \
  const int16_t print_scale = 6; \
  if (OB_FAIL(j_buf.append("\""))) { \
    LOG_WARN("fail to append \"", K(ret)); \
  } else if (OB_FAIL(ObTimeConverter::TO_OB_TIME_METHOD(value, ob_time))) { \
  } else if (OB_FAIL(ObTimeConverter::ob_time_to_str(ob_time, ob_time.mode_, print_scale,  \
                                                      tmp_buf, tmp_buf_len, pos, true))) { \
    LOG_WARN("fail to change time to string", K(ret), K(ob_time), K(pos)); \
  } else if (OB_FAIL(j_buf.append(tmp_buf))) { \
    LOG_WARN("fail to append date_buf to j_buf", K(ret), KCSTRING(tmp_buf)); \
  } else if (OB_FAIL(j_buf.append("\""))) { \
    LOG_WARN("fail to append \"", K(ret)); \
  }

struct ObFindDoubleEscapeFunc {
  ObFindDoubleEscapeFunc() {}

  bool operator()(const char c)
  {
    return c == '.' || c == 'e';
  }
};

int ObJsonExprHelper::get_timestamp_str_in_oracle_mode(ObEvalCtx &ctx,
                                                        const ObDatum &datum,
                                                        ObObjType type,
                                                        ObScale scale,
                                                        const ObTimeZoneInfo *tz_info,
                                                        ObJsonBuffer &j_buf)
{
  INIT_SUCC(ret);
  ObOTimestampData in_val;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
  int64_t len = 0;
  ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
    if (OB_FAIL(common_construct_otimestamp(type, datum, in_val))) {
      LOG_WARN("common_construct_otimestamp failed", K(ret));
    } else if (OB_FAIL(ObTimeConverter::otimestamp_to_str(in_val,
                                                  dtc_params,
                                                  scale, type, buf,
                                                  OB_CAST_TO_VARCHAR_MAX_LENGTH,
                                                  len))) {
      LOG_WARN("failed to convert otimestamp to string", K(ret));
    } else {
      ObString in_str(sizeof(buf), static_cast<int32_t>(len), buf);
      if (OB_FAIL(j_buf.append("\""))) {
        LOG_WARN("fail to append \"", K(ret));
      } else if (OB_FAIL(j_buf.append(in_str))) {
        LOG_WARN("fail to append date_buf to j_buf", K(ret), K(in_str));
      } else if (OB_FAIL(j_buf.append("\""))) {
        LOG_WARN("fail to append \"", K(ret));
      }
    }
  }
  return ret;
}

bool ObJsonExprHelper::is_cs_type_bin(ObCollationType &cs_type)
{
  bool res = false;
  switch(cs_type){
    case CS_TYPE_BINARY : {
      res = true;
      break;
    }
    default : {
      break;
    }
  }
  return res;
}

template <typename T>
int ObJsonExprHelper::transform_convertible_2jsonBase(const T &datum,
                                                      ObObjType type,
                                                      common::ObIAllocator *allocator,
                                                      ObCollationType cs_type,
                                                      ObIJsonBase*& j_base,
                                                      ObConv2JsonParam flags)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObIJsonBase* json_node = NULL;

  switch(type) {
    case ObNullType: {
      buf = allocator->alloc(sizeof(ObJsonNull));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        json_node = (ObJsonNull*)new(buf)ObJsonNull();
      }
      break;
    }
    case ObVarcharType:
    case ObCharType:
    case ObTinyTextType:
    case ObTextType :
    case ObMediumTextType:
    case ObLongTextType: {
      ObString j_str;
      if (is_mysql_mode()
          && OB_FAIL(ObJsonExprHelper::ensure_collation(type, cs_type))) {
        // should check collation first
        LOG_WARN("Invalid collation type for input string.", K(ret));
      } else {
        j_str = datum.get_string();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, type, cs_type, flags.has_lob_header_, j_str))) {
          LOG_WARN("fail to get real data.", K(ret), K(j_str));
        } else if (flags.deep_copy_) {
          ret = deep_copy_ob_string(*allocator, j_str, j_str);
        }
      }

      if (OB_SUCC(ret)) {
        if (flags.format_json_) {
          uint32_t parse_flag = flags.relax_type_ ? ObJsonParser::JSN_RELAXED_FLAG : ObJsonParser::JSN_STRICT_FLAG;
          ADD_FLAG_IF_NEED(flags.is_schema_, parse_flag, ObJsonParser::JSN_SCHEMA_FLAG);
          if(OB_FAIL(ObJsonExprHelper::cast_to_json_tree(j_str, allocator, parse_flag))) {
            if (flags.wrap_on_fail_) {
              if (OB_ISNULL(buf = allocator->alloc(sizeof(ObJsonString)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to allocate json string", K(ret));
              } else {
                ret = OB_SUCCESS;
                json_node = (ObJsonString*)new(buf)ObJsonString(j_str.ptr(), j_str.length());
              }
            } else {
              LOG_WARN("cast to json tree fail", K(ret));
            }
          } else {
            ObJsonInType to_type = flags.to_bin_ ? ObJsonInType::JSON_BIN : ObJsonInType::JSON_TREE;
            if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, ObJsonInType::JSON_TREE,
                                                        to_type, json_node, parse_flag))) {
              if (flags.is_schema_ && ret == OB_ERR_UNSUPPROTED_REF_IN_JSON_SCHEMA) {
              } else {
                ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
              }
              LOG_WARN("fail to get json base", K(ret), K(j_str));
            }
          }
        } else {
          if (OB_ISNULL(buf = allocator->alloc(sizeof(ObJsonString)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate json string", K(ret));
          } else {
            json_node = (ObJsonString*)new(buf)ObJsonString(j_str.ptr(), j_str.length());
          }
        }
      }

      break;
    }
    case ObJsonType: {
      ObString j_str = datum.get_string();
      if (OB_SUCC(ret)) {
        ObString tmp_str = j_str;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, type, cs_type, flags.has_lob_header_, tmp_str))) {
          LOG_WARN("fail to get real data.", K(ret), K(j_str));
        } else if (flags.deep_copy_) {
          if (OB_FAIL(deep_copy_ob_string(*allocator, tmp_str, j_str))) {
            LOG_WARN("do deep copy failed", K(ret));
          }
        } else {
          j_str = tmp_str;
        }
      }

      if (OB_SUCC(ret)) {
        ObJsonInType to_type = flags.to_bin_ ? ObJsonInType::JSON_BIN : ObJsonInType::JSON_TREE;
        uint32_t parse_flag = flags.relax_type_ ? ObJsonParser::JSN_RELAXED_FLAG : ObJsonParser::JSN_STRICT_FLAG;
        ADD_FLAG_IF_NEED(flags.is_schema_, parse_flag, ObJsonParser::JSN_SCHEMA_FLAG);
        if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, ObJsonInType::JSON_BIN,
                                                     to_type, json_node, parse_flag))) {
          if (flags.is_schema_ && ret == OB_ERR_UNSUPPROTED_REF_IN_JSON_SCHEMA) {
          } else {
            ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
          }
          LOG_WARN("fail to get json base", K(ret));
        }
      }
      break;
    }
    default:
    {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument", K(ret), K(type));
    }
  }

  if (OB_SUCC(ret)) {
    if (flags.to_bin_) {
      if (OB_FAIL(ObJsonBaseFactory::transform(allocator, json_node, ObJsonInType::JSON_BIN, j_base))) {
        LOG_WARN("failed: json tree to bin", K(ret));
      }
    } else {
      json_node->set_allocator(allocator);
      j_base = json_node;
    }
  }

  return ret;
}

int ObJsonExprHelper::get_sql_scalar_type(
  ObEvalCtx& ctx,
  int64_t origin,
  ObObjType& scalar_type,
  int32_t& scalar_len,
  int32_t& precision,
  int32_t& scale,
  ObAccuracy& accuracy,
  ObLengthSemantics& length_semantics)
{
  int ret = OB_SUCCESS;
  ParseNode parse_node;
  parse_node.value_ = origin;

  scalar_type = static_cast<ObObjType>(parse_node.int16_values_[0]);
  scalar_len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];

  if (ob_is_string_tc(scalar_type) && OB_FAIL(ObJsonUtil::get_accuracy_internal(
    accuracy, ctx, scalar_type, origin, length_semantics))) {
    LOG_WARN("failed to get accuracy", K(ret));
  } else if (ob_is_number_or_decimal_int_tc(scalar_type)) {
    precision = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX];
    scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];

    if (ObNumberType == scalar_type && is_decimal_int_accuracy_valid(precision, scale)) {
      bool enable_decimalint = false;
      if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type_ctx.get_session() is null", K(ret));
      } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(
        ctx.exec_ctx_.get_my_session(), enable_decimalint))) {
        LOG_WARN("fail to check_enable_decimalint_type",
            K(ret), K(ctx.exec_ctx_.get_my_session()->get_effective_tenant_id()));
      } else if (enable_decimalint) {
        scalar_type = ObDecimalIntType;
      }
    }
  }

  return ret;
}

int ObJsonExprHelper::get_cast_type(const ObExprResType param_type2,
                                    ObExprResType &dst_type,
                                    ObExprTypeCtx &type_ctx)
{
  INIT_SUCC(ret);
    if (!param_type2.is_int() && !param_type2.get_param().is_int()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast param type is unexpected", K(param_type2));
  } else {
    const ObObj &param = param_type2.get_param();
    ParseNode parse_node;
    parse_node.value_ = param.get_int();
    ObObjType obj_type = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
    dst_type.set_collation_type(static_cast<ObCollationType>(parse_node.int16_values_[OB_NODE_CAST_COLL_IDX]));
    dst_type.set_type(obj_type);
    if (ob_is_string_type(obj_type) || ob_is_lob_locator(obj_type)) {
      // cast(x as char(10)) or cast(x as binary(10))
      dst_type.set_full_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX],
                               param_type2.get_accuracy().get_length_semantics());
    } else if (ob_is_raw(obj_type)) {
      dst_type.set_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX]);
    } else if (ObFloatType == dst_type.get_type()) {
      // Compatible with mysql. If the precision p is not specified, produces a result of type FLOAT.
      // If p is provided and 0 <= < p <= 24, the result is of type FLOAT. If 25 <= p <= 53,
      // the result is of type DOUBLE. If p < 0 or p > 53, an error is returned
      // however, ob use -1 as default precision, so it is a valid value
      ObPrecision float_precision = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX];
      if (float_precision < -1 || float_precision > OB_MAX_DOUBLE_FLOAT_PRECISION) {
        ret = OB_ERR_TOO_BIG_PRECISION;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, float_precision, "CAST", OB_MAX_DOUBLE_FLOAT_PRECISION);
      } else if (float_precision <= OB_MAX_FLOAT_PRECISION) {
        dst_type.set_type(ObFloatType);
      } else {
        dst_type.set_type(ObDoubleType);
      }
      dst_type.set_precision(-1);
      dst_type.set_scale(parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]);
    } else if (lib::is_mysql_mode() && ObJsonType == dst_type.get_type()) {
      dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else {
      dst_type.set_precision(parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX]);
      dst_type.set_scale(parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]);
      if (ObNumberType == dst_type.get_type()
          && is_decimal_int_accuracy_valid(dst_type.get_precision(), dst_type.get_scale())) {
        bool enable_decimalint = false;
        if (OB_ISNULL(type_ctx.get_session())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("type_ctx.get_session() is null", K(ret));
        } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(type_ctx.get_session(),
                                                               enable_decimalint))) {
          LOG_WARN("fail to check_enable_decimalint_type",
              K(ret), K(type_ctx.get_session()->get_effective_tenant_id()));
        } else if (enable_decimalint) {
          dst_type.set_type(ObDecimalIntType);
        }
      }
    }
    LOG_DEBUG("get_cast_type", K(dst_type), K(param_type2));
  }
  return ret;
}

int ObJsonExprHelper::set_dest_type(ObExprResType &type1,
                                  ObExprResType &type,
                                  ObExprResType &dst_type,
                                  ObExprTypeCtx &type_ctx)
{
  INIT_SUCC(ret);
  const sql::ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is NULL", K(ret), KP(session));
  } else {
    // always cast to user requested type
    if (!lib::is_oracle_mode() &&
        ObCharType == dst_type.get_type()) {
      // cast(x as binary(10)), in parser,binary->T_CHAR+bianry, but, result type should be varchar, so set it.
      type.set_type(ObVarcharType);
    } else {
      type.set_type(dst_type.get_type());
      type.set_collation_type(dst_type.get_collation_type());
    }
    int16_t scale = dst_type.get_scale();
    if (!lib::is_oracle_mode()
        && (ObTimeType == dst_type.get_type() || ObDateTimeType == dst_type.get_type())
        && scale > MAX_SCALE_FOR_TEMPORAL) {
      ret = OB_ERR_TOO_BIG_PRECISION;
      LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST", OB_MAX_DATETIME_PRECISION);
    }
    if (OB_SUCC(ret)) {
      ObCompatibilityMode compatibility_mode = get_compatibility_mode();
      ObCollationType collation_connection = type_ctx.get_coll_type();
      ObCollationType collation_nation = session->get_nls_collation_nation();
      int32_t length = 0;
      if (ob_is_string_type(dst_type.get_type()) || ob_is_json(dst_type.get_type())) {
        type.set_collation_level(CS_LEVEL_IMPLICIT);
        int32_t len = dst_type.get_length();
        int16_t length_semantics = ((dst_type.is_string_type())
            ? dst_type.get_length_semantics()
            : (OB_NOT_NULL(type_ctx.get_session())
                ? type_ctx.get_session()->get_actual_nls_length_semantics()
                : LS_BYTE));
        if (len > 0) { // cast(1 as char(10))
          type.set_full_length(len, length_semantics);
        } else if (OB_FAIL(get_cast_string_len(type1, dst_type, type_ctx, len, length_semantics,
                                               collation_connection))) { // cast (1 as char)
          LOG_WARN("fail to get cast string length", K(ret));
        } else {
          type.set_full_length(len, length_semantics);
        }
        if (CS_TYPE_INVALID != dst_type.get_collation_type()) {
          // cast as binary
          type.set_collation_type(dst_type.get_collation_type());
        } else {
          // use collation of current session
          type.set_collation_type(ob_is_nstring_type(dst_type.get_type()) ?
                                  collation_nation : collation_connection);
        }
      } else {
        type.set_length(length);
        if (ObNumberTC == dst_type.get_type_class() && 0 == dst_type.get_precision()) {
          // MySql:cast (1 as decimal(0)) = cast(1 as decimal)
          // Oracle: cast(1.4 as number) = cast(1.4 as number(-1, -1))
          type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObNumberType].get_precision());
        } else if (ObDecimalIntTC == dst_type.get_type_class() && 0 == dst_type.get_precision()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dst type is decimal int, but precision is zero", K(ret), K(dst_type));
        } else if (ObIntTC == dst_type.get_type_class() || ObUIntTC == dst_type.get_type_class()) {
          // for int or uint , the precision = len
          int32_t len = 0;
          int16_t length_semantics = LS_BYTE;//unused
          if (OB_FAIL(get_cast_inttc_len(type1, dst_type, type_ctx, len, length_semantics, collation_connection))) {
            LOG_WARN("fail to get cast inttc length", K(ret));
          } else {
            len = len > OB_LITERAL_MAX_INT_LEN ? OB_LITERAL_MAX_INT_LEN : len;
            type.set_precision(static_cast<int16_t>(len));
          }
        } else if (ORACLE_MODE == compatibility_mode && ObDoubleType == dst_type.get_type()) {
          ObAccuracy acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][dst_type.get_type()];
          type.set_accuracy(acc);
        } else {
          type.set_precision(dst_type.get_precision());
        }
        type.set_scale(dst_type.get_scale());
      }
    }
  }

  return ret;
}

int ObJsonExprHelper::get_cast_inttc_len(ObExprResType &type1,
                                        ObExprResType &type2,
                                        ObExprTypeCtx &type_ctx,
                                        int32_t &res_len,
                                        int16_t &length_semantics,
                                        ObCollationType conn)
{
  INIT_SUCC(ret);

  if (type1.is_literal()) { // literal
    if (ObStringTC == type1.get_type_class()) {
      res_len = type1.get_accuracy().get_length();
      length_semantics = type1.get_length_semantics();
    } else if (OB_FAIL(ObField::get_field_mb_length(type1.get_type(),
        type1.get_accuracy(), type1.get_collation_type(), res_len))) {
      LOG_WARN("failed to get filed mb length");
    }
  } else {
    res_len = CAST_STRING_DEFUALT_LENGTH[type1.get_type()];
    ObObjTypeClass tc1 = type1.get_type_class();
    int16_t scale = type1.get_accuracy().get_scale();
    if (ObDoubleTC == tc1) {
      res_len -= 1;
    } else if (ObDateTimeTC == tc1 && scale > 0) {
      res_len += scale - 1;
    } else if (OB_FAIL(get_cast_string_len(type1, type2, type_ctx, res_len, length_semantics, conn))) {
      LOG_WARN("fail to get cast string length", K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObJsonExprHelper::get_dest_type(const ObExpr &expr, int64_t pos,
                                    ObEvalCtx& ctx,
                                    ObObjType &dest_type, int64_t &dst_len)
{
  INIT_SUCC(ret);
  ParseNode node;
  ObDatum *dst_type_dat = NULL;
  if (OB_ISNULL(expr.args_) || OB_ISNULL(expr.args_[pos])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[pos]->eval(ctx, dst_type_dat))) {
    LOG_WARN("eval dst type datum failed", K(ret));
  } else {
    node.value_ = dst_type_dat->get_int();
    dest_type = static_cast<ObObjType>(node.int16_values_[0]);
    dst_len = node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
  }
  return ret;
}

int ObJsonExprHelper::get_clause_opt(ObExpr *expr,
                                    ObEvalCtx &ctx,
                                    int8_t &type)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  if (OB_FAIL(expr->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else {
    int64_t option_type = json_datum->get_int();
    type = static_cast<int8_t>(option_type);
  }
  return ret;
}

int ObJsonExprHelper::get_cast_string_len(ObExprResType &type1,
                        ObExprResType &type2,
                        common::ObExprTypeCtx &type_ctx,
                        int32_t &res_len,
                        int16_t &length_semantics,
                        common::ObCollationType conn)
{
  INIT_SUCC(ret);
  const ObObj &val = type1.get_param();

  if (!type1.is_literal()) { // column
    res_len = CAST_STRING_DEFUALT_LENGTH[type1.get_type()];
    int16_t prec = type1.get_accuracy().get_precision();
    int16_t scale = type1.get_accuracy().get_scale();
    switch(type1.get_type()) {
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType:
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type: {
        int32_t prec = static_cast<int32_t>(type1.get_accuracy().get_precision());
        res_len = prec > res_len ? prec : res_len;
        break;
      }
      case ObNumberType:
      case ObDecimalIntType:
      case ObUNumberType: {
        if (lib::is_oracle_mode()) {
          if (0 < prec) {
            if (0 < scale) {
              res_len =  prec + 2;
            } else if (0 == scale) {
              res_len = prec + 1;
            } else {
              res_len = prec - scale;
            }
          }
        } else {
          if (0 < prec) {
            if (0 < scale) {
              res_len =  prec + 2;
            } else {
              res_len = prec + 1;
            }
          }
        }
        break;
      }
      case ObTimestampTZType:
      case ObTimestampLTZType:
      case ObTimestampNanoType:
      case ObDateTimeType:
      case ObTimestampType: {
        if (scale > 0) {
          res_len += scale + 1;
        }
        break;
      }
      case ObTimeType: {
        if (scale > 0) {
          res_len += scale + 1;
        }
        break;
      }
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType:
      case ObVarcharType:
      case ObCharType:
      case ObHexStringType:
      case ObRawType:
      case ObNVarchar2Type:
      case ObNCharType: {
        res_len = type1.get_length();
        length_semantics = type1.get_length_semantics();
        break;
      }
      default: {
        break;
      }
    }
  } else if (type1.is_null()) {
    res_len = 0;//compatible with mysql;
  } else if (OB_ISNULL(type_ctx.get_session())) {
    // calc type don't set ret, just print the log. by design.
    LOG_WARN("my_session is null");
  } else { // literal
    ObArenaAllocator oballocator(ObModIds::BLOCK_ALLOC);
    ObCastMode cast_mode = CM_NONE;
    ObCollationType cast_coll_type = (CS_TYPE_INVALID != type2.get_collation_type())
        ? type2.get_collation_type()
        : conn;
    const ObDataTypeCastParams dtc_params =
          ObBasicSessionInfo::create_dtc_params(type_ctx.get_session());
    ObCastCtx cast_ctx(&oballocator,
                       &dtc_params,
                       0,
                       cast_mode,
                       cast_coll_type);
    ObString val_str;
    EXPR_GET_VARCHAR_V2(val, val_str);
    if (OB_SUCC(ret) && NULL != val_str.ptr()) {
      int32_t len_byte = val_str.length();
      res_len = len_byte;
      length_semantics = LS_CHAR;
      if (NULL != val_str.ptr()) {
        int32_t trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cast_coll_type,
            val_str.ptr(), len_byte));
        res_len = static_cast<int32_t>(ObCharset::strlen_char(cast_coll_type,
            val_str.ptr(), trunc_len_byte));
      }
      if (type1.is_numeric_type() && !type1.is_integer_type()) {
        res_len += 1;
      }
    }
  }
  return ret;
}

int ObJsonExprHelper::parse_res_type(ObExprResType& type1,
                                     ObExprResType& res_type,
                                     ObExprResType& result_type,
                                     ObExprTypeCtx& type_ctx)
{
  INIT_SUCC(ret);
  const int32_t VARCHAR2_DEFAULT_LEN = 4000;
  ObExprResType dst_type;
  const ObObj &param = res_type.get_param();

  if (param.get_int() == 0) {
    //     result_type.set_type(type1.get_type());
    //     result_type.set_collation_type(type1.get_collation_type());
    //     result_type.set_accuracy(type1.get_accuracy());
    //     ObObjType obj_type = type1.get_type();

    ObObjType obj_type = ObJsonType;
    result_type.set_type(ObJsonType);
    result_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    int16_t length_semantics = (OB_NOT_NULL(type_ctx.get_session())
                ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
    result_type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());

    result_type.set_collation_level(CS_LEVEL_IMPLICIT);
  } else if (OB_FAIL(ObJsonExprHelper::get_cast_type(res_type, dst_type, type_ctx))) {
    LOG_WARN("get cast dest type failed", K(ret));
  } else if (OB_FAIL(ObJsonExprHelper::set_dest_type(type1, result_type, dst_type, type_ctx))) {
    LOG_WARN("set dest type failed", K(ret));
  } else {
    result_type.set_calc_collation_type(result_type.get_collation_type());
  }

  return ret;
}

int ObJsonExprHelper::eval_and_check_res_type(int64_t value, ObObjType& type, int32_t& dst_len)
{
  INIT_SUCC(ret);
  ParseNode node;
  node.value_ = value;
  ObObjType dst_type = static_cast<ObObjType>(node.int16_values_[0]);
  dst_len = node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
  if (dst_type != ObVarcharType
      && dst_type != ObLongTextType
      && dst_type != ObJsonType) {
    ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
    LOG_USER_ERROR(OB_ERR_INVALID_DATA_TYPE_RETURNING);
  } else {
    type = dst_type;
  }
  return ret;
}
// JSON_EXPR_FLAG 0 json value , 1 json query
int ObJsonExprHelper::check_item_func_with_return(ObJsonPathNodeType path_type, ObObjType dst_type, common::ObCollationType dst_coll_type, int8_t JSON_EXPR_FLAG)
{
  INIT_SUCC(ret);
  switch (path_type) {
    case JPN_ABS :{
      break;
    }
    case JPN_BOOLEAN :
    case JPN_BOOL_ONLY :{
      if (dst_type == ObVarcharType) {
      } else {
        ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
        LOG_WARN("item func is double, but return type is ", K(dst_type), K(ret));
      }
      break;
    }
    case JPN_DATE :{
      if (dst_type == ObDateTimeType) {
      } else {
        ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
        LOG_WARN("item func is double, but return type is ", K(dst_type), K(ret));
      }
      break;
    }
    case JPN_DOUBLE :{
      if (dst_type == ObDoubleType || dst_type == ObUDoubleType) {
      } else {
        ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
        LOG_WARN("item func is double, but return type is ", K(dst_type), K(ret));
      }
      break;
    }
    case JPN_FLOOR :
    case JPN_CEILING :
    case JPN_LENGTH :
    case JPN_NUMBER :
    case JPN_NUM_ONLY :
    case JPN_SIZE :{
      if (JSON_EXPR_FLAG == 1 || (JSON_EXPR_FLAG == 0 && ob_is_numeric_type(dst_type))) {
      } else {
        ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
        LOG_WARN("item func is lower/upper, but return type is ", K(dst_type), K(ret));
      }
      break;
    }

    case JPN_TIMESTAMP :{
      if (dst_type != ObTimestampNanoType && dst_type != ObTimestampType) {
        ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
        LOG_WARN("item func is type, but return type is ", K(dst_type), K(ret));
      }
      break;
    }
    case JPN_TYPE :{
      if (dst_type == ObJsonType && JSON_EXPR_FLAG != 1) {
        ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
        LOG_WARN("item func is type, but return type is ", K(dst_type), K(ret));
      }
    }
    case JPN_STRING :
    case JPN_STR_ONLY :
    case JPN_LOWER :
    case JPN_UPPER :{
      if (dst_type == ObVarcharType || (dst_type == ObLongTextType && dst_coll_type != CS_TYPE_BINARY)) {
      } else {
        ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
        LOG_WARN("item func is lower/upper, but return type is ", K(dst_type), K(ret));
      }
      break;
    }
    default :{
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can't find right path type", K(ret));
    }
  }
  return ret;
}

int ObJsonExprHelper::get_expr_option_value(const ObExprResType param_type2, int8_t &dst_type)
{
  INIT_SUCC(ret);
  if (!param_type2.is_int() && !param_type2.get_param().is_int()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast param type is unexpected", K(param_type2));
  } else {
    dst_type = param_type2.get_param().get_int();
  }
  return ret;
}

int ObJsonExprHelper::calc_asciistr_in_expr(const ObString &src,
                                            const ObCollationType src_cs_type,
                                            const ObCollationType dst_cs_type,
                                            char* buf, const int64_t buf_len, int32_t &pos)
{
  int ret = OB_SUCCESS;
  ObStringScanner scanner(src, src_cs_type);
  ObString encoding;
  int32_t wchar = 0;

  while (OB_SUCC(ret)
         && scanner.next_character(encoding, wchar, ret)) {

    if (ob_isascii(wchar) && '\\' != wchar) {
      int32_t written_bytes = 0;

      if (OB_FAIL(ObCharset::wc_mb(dst_cs_type, wchar,
                                   buf + pos, buf_len - pos, written_bytes))) {
        LOG_WARN("fail to convert unicode to multi-byte", K(ret), K(wchar));
      } else {
        pos += written_bytes;
      }
    } else {
      const int64_t temp_buf_len = 4;
      char temp_buf[temp_buf_len];
      int32_t temp_written_bytes = 0;

      if (OB_FAIL(ObCharset::wc_mb(CS_TYPE_UTF16_BIN, wchar,
                                   temp_buf, temp_buf_len, temp_written_bytes))) {
        LOG_WARN("fail to convert unicode to multi-byte", K(ret), K(wchar));
      } else {
        const int utf16_minmb_len = 2;

        if (OB_UNLIKELY(ObCharset::is_cs_nonascii(dst_cs_type))) {
          // not support non-ascii database charset for now
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "charset except ascii");
          LOG_WARN("not support charset", K(ret), K(dst_cs_type));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < temp_written_bytes/utf16_minmb_len; ++i) {
            if (OB_UNLIKELY(pos >= buf_len)) {
              ret = OB_SIZE_OVERFLOW;
              LOG_WARN("size overflow", K(ret), K(pos), K(buf_len));
            } else {
              buf[pos++] = '\\';
            }
            if (OB_SUCC(ret) && '\\' != wchar) {
              if (OB_UNLIKELY(pos >= buf_len)) {
                ret = OB_SIZE_OVERFLOW;
                LOG_WARN("size overflow", K(ret), K(pos), K(buf_len));
              } else {
                buf[pos++] = 'u';
              }
              int64_t hex_writtern_bytes = 0;
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(hex_print(temp_buf + i*utf16_minmb_len, utf16_minmb_len,
                                    buf + pos, buf_len - pos, hex_writtern_bytes))) {
                LOG_WARN("fail to convert to hex", K(ret), K(temp_written_bytes), K(pos), K(buf_len));
              } else {
                pos += hex_writtern_bytes;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonExprHelper::parse_asc_option(ObExprResType& asc_type,
                                       ObExprResType& type1,
                                       ObExprResType& res_type,
                                       ObExprTypeCtx& type_ctx)
{
  INIT_SUCC(ret);
  ObExprResType temp_type;
  ObObjType doc_type = type1.get_type();
  int8_t asc_option = 0;

  if (asc_type.get_type() != ObIntType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("<ASCII type> param type is unexpected", K(asc_type.get_type()));
  } else if (OB_FAIL(ObJsonExprHelper::get_expr_option_value(asc_type, asc_option))) {
    LOG_WARN("get ascii type fail", K(ret));
  } else if (asc_option == 1
             && ob_is_string_type(doc_type)
             && ((res_type.is_character_type() && (res_type.get_length_semantics() == LS_CHAR || res_type.get_length_semantics() == LS_BYTE))
                  || res_type.is_lob())) {
    type1.set_calc_length_semantics(res_type.get_length_semantics());
    ObLength length = 0;
    ObExprResType temp_type;
    temp_type.set_meta(type1.get_calc_meta());
    temp_type.set_length_semantics(res_type.get_length_semantics());

    if (doc_type == ObNCharType) {
      length = type1.get_param().get_string_len() * ObCharset::MAX_MB_LEN * 2;
      type1.set_calc_length(length);
      res_type.set_length(length);
    } else if (!temp_type.is_blob() && OB_FAIL(ObExprResultTypeUtil::deduce_max_string_length_oracle(
      type_ctx.get_session()->get_dtc_params(), type1, temp_type, length))) {
      LOG_WARN("fail to deduce max string length.", K(ret), K(temp_type), K(type1));
    } else {
      type1.set_calc_length(length);
      res_type.set_length(length * 10);
      if (res_type.is_lob()) {
        res_type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]).get_length());
      }
    }
  }

  return ret;
}

int ObJsonExprHelper::character2_ascii_string(common::ObIAllocator *allocator,
                                              const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              ObString& result,
                                              int32_t reserve_len)
{
  INIT_SUCC(ret);
  char *buf = NULL;
  int64_t buf_len = result.length() * ObCharset::MAX_MB_LEN * 2;
  int32_t length = 0;

  if ((OB_NOT_NULL(allocator) && OB_ISNULL(buf = static_cast<char*>(allocator->alloc(buf_len + reserve_len + 1))))
       || (OB_ISNULL(allocator) && OB_ISNULL(buf = static_cast<char*>(expr.get_str_res_mem(ctx, buf_len + reserve_len + 1))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(buf_len), K(result.length()));
  } else if (OB_FAIL(ObJsonExprHelper::calc_asciistr_in_expr(result,
                                                             expr.datum_meta_.cs_type_,
                                                             expr.datum_meta_.cs_type_,
                                                             buf, buf_len, length))) {
    LOG_WARN("fail to calc unistr", K(ret));
  } else {
    buf[length] = 0;
    result.assign_ptr(buf, length);
  }
  return ret;
}

int ObJsonExprHelper::pre_default_value_check(ObObjType dst_type, ObString val_str, ObObjType val_type, size_t length) {
  INIT_SUCC(ret);
  size_t len;
  switch (dst_type) {
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObVarcharType:
    case ObCharType:
    case ObHexStringType:
    case ObRawType:
    case ObNVarchar2Type:
    case ObNCharType: {
      if (lib::is_mysql_mode()) {
      } else  if (val_type != ObVarcharType && val_type != ObCharType) {
        ret = OB_ERR_DEFAULT_VALUE_NOT_MATCH;
        LOG_WARN("default value not match",K(ret));
      } else if (dst_type == ObVarcharType) {
         if (val_str.length() > length) {
          ret = OB_ERR_VALUE_EXCEEDED_MAX;
          LOG_USER_ERROR(OB_ERR_VALUE_EXCEEDED_MAX, static_cast<int>(val_str.length()), static_cast<int>(length));
         }
      }
      break;
    }
    case ObNumberType:
    case ObDecimalIntType: {
      if (val_type == ObNumberType || val_type == ObDecimalIntType) {
      } else {
        len = val_str.length();
        for(size_t i = 0; i < len; i++) {
          if (val_str[i] == '-' || val_str[i] == '+') {
          } else if (val_str[i] > '9' || val_str[i] < '0') {
            ret = OB_ERR_INVALID_DEFAULT_VALUE_PROVIDED;
            LOG_WARN("number check fail");
          }
        }
      }
      break;
    }
    case ObDateType:
    case ObTimestampNanoType:
    case ObTimestampTZType:
    case ObDateTimeType:
    case ObTimestampLTZType: {
      if (dst_type == val_type) {
      } else if (val_type != ObCharType) {
        ret = OB_ERR_INVALID_DEFAULT_VALUE_PROVIDED;
      } else {
        len = val_str.length();
        if(len >= 5 && val_str[4] != '-') {
          ret = OB_ERR_INVALID_DEFAULT_VALUE_PROVIDED;
        } else if(len >= 8 && val_str[7] != '-') {
          ret = OB_ERR_INVALID_DEFAULT_VALUE_PROVIDED;
        }
      }
      break;
    }
    case ObJsonType: {
      if (ob_is_number_tc(val_type)) {
        ret = OB_ERR_DEFAULT_VALUE_NOT_MATCH;
      }
      break;
    }
    default:
      break;
  }
  return ret;
}

/********** ObJsonExprHelper for json partial update  ****************/
int ObJsonExprHelper::pack_json_diff_res(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    ObIAllocator &temp_allocator,
    ObIJsonBase *json_doc,
    ObDatum &res)
{
  INIT_SUCC(ret);
  ObJsonBin *bin = nullptr;
  ObJsonDeltaLob json_delta_lob;
  char *res_buf = nullptr;
  int64_t res_buf_len = 0;
  int64_t pos = 0;
  if (OB_ISNULL(json_doc)) {
    res.set_null();
  } else if (! json_doc->is_bin()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not json binary", K(ret), K(json_doc));
  } else if (OB_FALSE_IT(bin = static_cast<ObJsonBin*>(json_doc))) {
  } else if (OB_FAIL(json_delta_lob.init(bin))) {
    LOG_WARN("init fail", K(ret), K(bin));
  } else if (OB_FAIL(json_delta_lob.check_binary_diff())) {
    LOG_WARN("init fail", K(ret), K(bin));
  } else if (OB_FALSE_IT(res_buf_len = json_delta_lob.get_serialize_size())) {
  } else if (OB_ISNULL(res_buf = expr.get_str_res_mem(ctx, res_buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for delta lob locator fail", K(ret), K(res_buf_len));
  } else if (OB_FAIL(json_delta_lob.serialize(res_buf, res_buf_len, pos))) {
    LOG_WARN("serialize fail", KR(ret), K(res_buf_len));
  } else {
    res.set_string(res_buf, res_buf_len);
  }
  return ret;
}

int ObJsonExprHelper::pack_json_res(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    ObIAllocator &temp_allocator,
    ObIJsonBase *json_doc,
    ObDatum &res)
{
  INIT_SUCC(ret);
  bool shoudl_pack_diff = false;
  ObJsonBin *json_bin = nullptr;
  if (OB_ISNULL(json_doc)) {
  } else if (! json_doc->is_bin()) {
  } else if (OB_FALSE_IT(json_bin = static_cast<ObJsonBin*>(json_doc))) {
  } else if (OB_FAIL(json_bin->should_pack_diff(shoudl_pack_diff))) {
    LOG_WARN("get should_pack_diff fail", K(ret));
  }

  if (OB_ISNULL(json_doc)) {
    res.set_null();
  } else if (shoudl_pack_diff) {
    if (OB_FAIL(ObJsonExprHelper::pack_json_diff_res(expr, ctx, temp_allocator, json_doc, res))) {
      LOG_WARN("pack diff fail", K(ret));
    }
  } else {
    ObString str;
    if (OB_FAIL(ObJsonWrapper::get_raw_binary(json_doc, str, &temp_allocator))) {
      LOG_WARN("json_set result to binary failed", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, str))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  }
  return ret;
}


int ObJsonExprHelper::is_allow_partial_update(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObString &locator_str,
    bool &allow_partial_update)
{
  INIT_SUCC(ret);
  ObString option;
  ObLobLocatorV2 locator(locator_str);
  sql::ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  // 1. is delta lob
  // 2. perisit outrow lob
  } else if (! locator.is_delta_temp_lob() && ! (locator.is_persist_lob() && ! locator.is_inrow())) {
  } else if (is_json_partial_update_mode(expr)) {
    option = session->get_log_row_value_option();
    allow_partial_update = option.case_compare(OB_LOG_ROW_VALUE_PARTIAL_JSON) == 0
        || option.case_compare(OB_LOG_ROW_VALUE_PARTIAL_ALL) == 0;
  }
  return ret;
}

bool ObJsonExprHelper::is_json_partial_update_mode(const ObExpr &expr)
{
  return (expr.extra_ & OB_JSON_PARTIAL_UPDATE_ALLOW) != 0;
}

int ObJsonExprHelper::refresh_root_when_bin_rebuild_all(ObIJsonBase *j_base)
{
  INIT_SUCC(ret);
  ObJsonBin *j_bin = nullptr;
  ObJsonBinUpdateCtx *update_ctx = nullptr;
  if (OB_ISNULL(j_base)) {
  } else if (! j_base->is_bin()) {
  } else if (OB_FALSE_IT(j_bin = static_cast<ObJsonBin*>(j_base))) {
  } else if (OB_ISNULL(update_ctx = j_bin->get_update_ctx())) {
  } else if (! update_ctx->is_rebuild_all()) {
  } else if (OB_FAIL(j_bin->reset(0))) {
    LOG_WARN("reset fail", K(ret), K(*j_bin));
  }
  return ret;
}

int ObJsonExprHelper::init_json_expr_extra_info(
    ObIAllocator *allocator,
    const ObRawExpr &raw_expr,
    const ObExprOperatorType type,
    ObExpr &rt_expr)
{
  int ret = OB_SUCCESS;
  const uint64_t extra = raw_expr.get_extra();
  if (! is_json_partial_update_mode(extra)) { // only used for json partial update now
  } else {
    rt_expr.extra_ = extra;
  }
  return ret;
}

int ObJsonExprHelper::get_session_query_timeout_ts(ObEvalCtx &ctx, int64_t &timeout_ts)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    timeout_ts = session->get_query_timeout_ts();
  }
  return ret;
}

/********** ObJsonExprHelper for json partial update  ****************/


/********** ObJsonDeltaLob ****************/

int ObJsonDeltaLob::init(ObJsonBin *j_bin)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(update_ctx_ = j_bin->get_update_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update ctx is null", K(ret), KPC(j_bin));
  } else {
    cursor_ = static_cast<storage::ObLobCursor*>(update_ctx_->cursor_);
    partial_data_ = cursor_->partial_data_;
  }
  return ret;
}

int ObJsonDeltaLob::init(ObIAllocator *allocator, ObLobLocatorV2 locator, int64_t query_timeout_ts)
{
  int ret = OB_SUCCESS;
  allocator_ = allocator;
  query_timeout_ts_ = query_timeout_ts;
  if (OB_FAIL(deserialize(locator))) {
    LOG_WARN("deserialize json delta lob fail", K(ret), K(locator));
  }
  return ret;
}

void ObJsonDeltaLob::reset()
{
  if (OB_NOT_NULL(j_base_)) {
    j_base_->reset();
    j_base_ = nullptr;
  }
  if (OB_NOT_NULL(update_ctx_)) {
    update_ctx_->~ObJsonBinUpdateCtx();
    update_ctx_ = nullptr;
  }
  if (OB_NOT_NULL(cursor_)) {
    cursor_->~ObLobCursor();
    cursor_ = nullptr;
  }
  if (OB_NOT_NULL(partial_data_)) {
    partial_data_->~ObLobPartialData();
    partial_data_ = nullptr;
  }
}

int64_t ObJsonDeltaLob::get_lob_diff_serialize_size() const
{
  int64_t len = 0;
  // binary diff (lob diff)
  const ObJsonBinaryDiffArray &binary_diffs = update_ctx_->binary_diffs_;
  len += sizeof(storage::ObLobDiff) * binary_diffs.count();

  // json diff
  const ObJsonDiffArray &json_diffs = update_ctx_->json_diffs_;
  ObJsonDiffHeader json_diff_header;
  json_diff_header.cnt_ = json_diffs.count();
  len += json_diff_header.get_serialize_size();
  for (int i = 0; i < json_diffs.count(); ++i) {
    const ObJsonDiff& diff = json_diffs[i];
    len += json_diffs[i].get_serialize_size();
  }
  return len;
}

uint32_t ObJsonDeltaLob::get_lob_diff_cnt() const
{
  return  update_ctx_->binary_diffs_.count();
}

int64_t ObJsonDeltaLob::get_partial_data_serialize_size() const
{
  return nullptr == partial_data_ ? 0 : partial_data_->get_serialize_size();
}

int ObJsonDeltaLob::check_binary_diff() const
{
  INIT_SUCC(ret);
  for (int i = 0; OB_SUCC(ret) && i < partial_data_->index_.count(); ++i) {
    ObLobChunkIndex &chunk_index = partial_data_->index_[i];
    uint64_t chunk_start_offset = chunk_index.offset_;
    uint64_t chunk_end_offset = chunk_index.offset_ + chunk_index.byte_len_;
    uint64_t max_end_offset = chunk_start_offset + partial_data_->chunk_size_;
    bool is_chunk_updated = false;
    for (int j = 0; ! chunk_index.is_add_ && j < update_ctx_->binary_diffs_.count(); ++j) {
      const ObJsonBinaryDiff &diff = update_ctx_->binary_diffs_[j];
      uint64_t diff_start_offset = diff.dst_offset_;
      uint64_t diff_end_offset = diff.dst_offset_ + diff.dst_len_;
      if (diff_start_offset >= chunk_start_offset && diff_start_offset < chunk_end_offset) {
        is_chunk_updated = true;
      } else if (diff_end_offset > chunk_start_offset && diff_end_offset <= chunk_end_offset) {
        is_chunk_updated = true;
      } else if (diff_start_offset <= chunk_start_offset &&  chunk_end_offset <= diff_end_offset) {
        is_chunk_updated = true;
      }
    }
    // if it should be updated and it's exist chunk, but no modified flag and old data, this is unexpected.
    if (is_chunk_updated
        && ! chunk_index.is_add_ // exist chunk
        && ! (chunk_index.is_modified_ && chunk_index.old_data_idx_ >= 0))  { // no modified flag and old data
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("old data not set", KR(ret), K(i), K(chunk_index));
    }
  }
  return ret;
}

int ObJsonDeltaLob::serialize_partial_data(char* buf, const int64_t buf_len, int64_t& pos) const
{
  INIT_SUCC(ret);
  if (OB_ISNULL(partial_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partial_data_ is null", KR(ret));
  } else if (OB_FAIL(partial_data_->serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize fail", K(ret), K(buf_len), K(pos));
  }
  return ret;
}

int ObJsonDeltaLob::deserialize_partial_data(storage::ObLobDiffHeader *diff_header)
{
  INIT_SUCC(ret);
  storage::ObLobAccessParam *param = nullptr;
  char *buf = diff_header->data_;
  int64_t data_len = diff_header->persist_loc_size_;
  int64_t pos = 0;
  ObLobLocatorV2 locator;
  ObLobManager* lob_mgr = MTL(ObLobManager*);
  if (OB_ISNULL(partial_data_ = OB_NEWx(storage::ObLobPartialData, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc lob param fail", K(ret), "size", sizeof(ObLobPartialData));
  } else if (OB_FAIL(partial_data_->init())) {
    LOG_WARN("map create fail", K(ret));
  } else if (OB_FAIL(partial_data_->deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize fail", K(ret), K(data_len), K(pos), KPC(diff_header));
  } else if (OB_FALSE_IT(locator.assign_buffer(partial_data_->locator_.ptr(), partial_data_->locator_.length()))) {
  } else if (OB_FAIL(lob_mgr->query(allocator_, locator,
                    query_timeout_ts_, false, partial_data_, cursor_))) {
    LOG_WARN("build_lob_param fail", K(ret));
  }
  return ret;
}

int ObJsonDeltaLob::serialize_lob_diffs(char* buf, const int64_t buf_len, storage::ObLobDiffHeader *diff_header) const
{
  int ret = OB_SUCCESS;
  char *diff_data_ptr = diff_header->get_inline_data_ptr();
  storage::ObLobDiff *lob_diffs = diff_header->get_diff_ptr();
  int64_t data_len = buf_len - (diff_data_ptr - buf);
  int64_t data_pos = 0;

  for (int i = 0; OB_SUCC(ret) && i < diff_header->diff_cnt_; ++i) {
    const ObJsonBinaryDiff &diff = update_ctx_->binary_diffs_[i];
    storage::ObLobDiff *lob_diff = new (lob_diffs + i) storage::ObLobDiff();
    lob_diff->type_ = get_diff_type();
    lob_diff->dst_offset_ = diff.dst_offset_;
    lob_diff->dst_len_ = diff.dst_len_;
  }

  const ObJsonDiffArray &json_diffs = update_ctx_->json_diffs_;
  ObJsonDiffHeader json_diff_header;
  json_diff_header.cnt_ = json_diffs.count();
  if (OB_FAIL(json_diff_header.serialize(diff_data_ptr, data_len, data_pos))) {
    LOG_WARN("serialize json diff header fail", KR(ret), K(buf_len), K(data_pos));
  }
  for (int i = 0; OB_SUCC(ret) && i < json_diffs.count(); ++i) {
    const ObJsonDiff& diff = json_diffs[i];
    if (OB_FAIL(diff.serialize(diff_data_ptr, data_len, data_pos))) {
      LOG_WARN("serialize json diff fail", KR(ret), K(i), K(buf_len), K(data_pos), K(json_diffs));
    }
  }
  return ret;
}

int ObJsonDeltaLob::deserialize_lob_diffs(char* buf, const int64_t buf_len, storage::ObLobDiffHeader *diff_header)
{
  INIT_SUCC(ret);
  ObJsonBinUpdateCtx *update_ctx = nullptr;
  storage::ObLobDiff *lob_diffs = nullptr;
  char *data_ptr = nullptr;
  if (OB_ISNULL(update_ctx_ = OB_NEWx(ObJsonBinUpdateCtx, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc update ctx fail", K(ret), "size", sizeof(ObJsonBinUpdateCtx));
  } else if (OB_ISNULL(data_ptr = diff_header->get_inline_data_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_ptr is null", K(ret), KPC(diff_header));
  } else if (OB_ISNULL(lob_diffs = diff_header->get_diff_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob_diffs is null", K(ret), KPC(diff_header));
  } else {
    int64_t data_len = buf_len - (data_ptr - buf);
    int64_t data_pos = 0;
    for (int64_t i = 0 ; OB_SUCC(ret) && i < diff_header->diff_cnt_; ++i) {
      ObLobDiff &lob_diff = lob_diffs[i];
      ObJsonBinaryDiff binary_diff;
      binary_diff.dst_offset_ = lob_diff.dst_offset_;
      binary_diff.dst_len_ = lob_diff.dst_len_;
      if (OB_FAIL(update_ctx_->binary_diffs_.push_back(binary_diff))) {
        LOG_WARN("push diff fail", KR(ret), K(lob_diff), K(binary_diff), K(i), KPC(diff_header));
      }
    }

    ObJsonDiffHeader json_diff_header;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(json_diff_header.deserialize(data_ptr, data_len, data_pos))) {
      LOG_WARN("deserialize json diff header fail", K(ret), K(data_len), K(data_pos), K(json_diff_header));
    }
    for (int64_t i = 0 ; OB_SUCC(ret) && i < json_diff_header.cnt_; ++i) {
      ObJsonDiff json_diff;
      if (OB_FAIL(json_diff.deserialize(data_ptr, data_len, data_pos))) {
        LOG_WARN("deserialize fail", K(ret), K(i), K(json_diff_header), K(data_len), K(data_pos));
      } else if (OB_FAIL(update_ctx_->json_diffs_.push_back(json_diff))) {
        LOG_WARN("push diff fail", KR(ret), K(i), K(json_diff), K(i), K(json_diff_header), KPC(diff_header));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonExprHelper::get_partial_json_bin(*allocator_, cursor_, update_ctx_, j_base_))) {
      LOG_WARN("get_partial_json_bin fail", K(ret));
    }
  }
  return ret;
}

/********** ObJsonDeltaLob ****************/

}
}
