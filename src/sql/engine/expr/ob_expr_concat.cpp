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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_concat.h"
#include <string.h>
#include "lib/oblog/ob_log.h"
//#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprConcat::ObExprConcat(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_OP_CNN, N_CONCAT, MORE_THAN_ZERO)
{
  need_charset_convert_ = false;
}

ObExprConcat::~ObExprConcat()
{}

int ObExprConcat::calc(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObIAllocator* allocator,
    const ObObjType result_type, bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode) {
    if (obj1.is_null() && obj2.is_null()) {
      result.set_null();
    } else {
      ObString str1;
      ObString str2;
      if (!obj1.is_null()) {
        str1 = obj1.get_string();
      }
      if (!obj2.is_null()) {
        str2 = obj2.get_string();
      }
      if (ob_is_text_tc(result_type)) {
        OZ(calc(result, str1.ptr(), str1.length(), str2.ptr(), str2.length(), allocator));
      } else {
        OZ(calc(result, str1, str2, allocator, is_oracle_mode));
      }
    }
  } else {
    if (OB_UNLIKELY(obj1.is_null()) || OB_UNLIKELY(obj2.is_null())) {
      result.set_null();
    } else if (OB_UNLIKELY(obj1.is_text() || obj2.is_text())) {
      ret = calc(result,
          obj1.get_string_ptr(),
          obj1.get_string_len(),
          obj2.get_string_ptr(),
          obj2.get_string_len(),
          allocator);
    } else {
      TYPE_CHECK(obj1, ObVarcharType);
      TYPE_CHECK(obj2, ObVarcharType);
      ObString str1 = obj1.get_string();
      ObString str2 = obj2.get_string();
      ret = calc(result, str1, str2, allocator);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("concat error", K(ret), K(is_oracle_mode), K(obj1), K(obj2));
    }
  }
  return ret;
}

int ObExprConcat::calc_resultN(
    common::ObObj& result, const common::ObObj* objs_array, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(param_num <= 0) || OB_ISNULL(objs_array) ||
      (param_num == 1 && share::is_oracle_mode())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(expr_ctx.calc_buf_), K(objs_array));
  } else if (param_num == 1) {
    ObString null_string;
    ObObj null_obj;
    null_obj.set_varchar(null_string);
    if (OB_FAIL(calc(
            result, null_obj, objs_array[0], expr_ctx.calc_buf_, result_type_.get_type(), lib::is_oracle_mode()))) {
      LOG_WARN("fail to calc function concat", K(ret), K(objs_array[0]), K(lib::is_oracle_mode()));
    }
  } else {
    bool is_oracle_mode = share::is_oracle_mode();
    if (is_oracle_mode && param_num > 2) {
      ret = OB_INVALID_ARGUMENT_NUM;
      LOG_WARN("CONCAT expect 2 arguments", "actual argument count", param_num, K(ret));
    } else if (OB_FAIL(calc(result,
                   objs_array[0],
                   objs_array[1],
                   expr_ctx.calc_buf_,
                   result_type_.get_type(),
                   is_oracle_mode))) {
      LOG_WARN("fail to calc function concat", K(ret), K(objs_array[0]), K(objs_array[1]), K(is_oracle_mode));
    } else if (!result.is_null()) {
      for (int64_t i = 2; OB_SUCC(ret) && i < param_num; ++i) {
        if (OB_FAIL(calc(result, result, objs_array[i], expr_ctx.calc_buf_, result_type_.get_type(), is_oracle_mode))) {
          LOG_WARN("fail to calc function concat", K(ret), K(result), K(objs_array[i]), K(is_oracle_mode));
        } else if (result.is_null()) {
          break;
        }
      }
    } else {
      // result is null. do nothing.
    }
  }
  if (OB_SUCC(ret) && !result.is_null()) {
    result.set_meta_type(result_type_);
  }
  return ret;
}

int ObExprConcat::calc(common::ObObj& result, const char* obj1_ptr, const int32_t this_len, const char* obj2_ptr,
    const int32_t other_len, ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  ObString varchar;
  int32_t max_length = OB_MAX_PACKET_LENGTH;
  if (OB_UNLIKELY(this_len + other_len > max_length)) {
    result.set_null();
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("length overflow", K(ret), K(this_len), K(other_len), K(max_length));
  } else if (OB_UNLIKELY(this_len <= 0)) {
    result.set_lob_value(ObLongTextType, obj2_ptr, other_len);
  } else if (OB_UNLIKELY(other_len <= 0)) {
    result.set_lob_value(ObLongTextType, obj1_ptr, this_len);
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null allocator", K(ret), K(allocator));
  } else {
    char* buf = NULL;
    if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(this_len + other_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret), K(this_len + other_len));
    } else {
      MEMCPY(buf, obj1_ptr, this_len);
      MEMCPY(buf + this_len, obj2_ptr, other_len);
      result.set_lob_value(ObLongTextType, buf, this_len + other_len);
    }
  }
  return ret;
}

int ObExprConcat::calc(common::ObObj& result, const common::ObString obj1, const common::ObString obj2,
    ObIAllocator* allocator, bool is_oracle_mode, const int64_t max_result_len /* =0 */)
{
  int ret = OB_SUCCESS;
  int32_t this_len = obj1.length();
  int32_t other_len = obj2.length();
  ObString varchar;
  int64_t max_length = max_result_len;
  if (max_result_len <= 0) {
    max_length = is_oracle_mode ? OB_MAX_ORACLE_VARCHAR_LENGTH : OB_MAX_VARCHAR_LENGTH;
  }
  if (OB_UNLIKELY(this_len + other_len > max_length)) {
    result.set_null();
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_UNLIKELY(this_len <= 0)) {
    result.set_varchar(obj2);
  } else if (OB_UNLIKELY(other_len <= 0)) {
    result.set_varchar(obj1);
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null allocator", K(ret), K(allocator));
  } else {
    char* buf = NULL;
    if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(this_len + other_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret), K(this_len + other_len));
    } else {
      MEMCPY(buf, obj1.ptr(), this_len);
      MEMCPY(buf + this_len, obj2.ptr(), other_len);
      varchar.assign(buf, this_len + other_len);
      result.set_varchar(varchar);
    }
  }
  return ret;
}

int ObExprConcat::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num <= 0 || (share::is_oracle_mode() && param_num != 2))) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  }

  CK(OB_NOT_NULL(type_ctx.get_session()));

  if (share::is_oracle_mode()) {
    ObSEArray<ObExprResType*, 2, ObNullAllocator> params;
    for (int64_t i = 0; i < param_num; ++i) {
      OZ(params.push_back(&types[i]));
    }
    OZ(aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), params, type));
    OZ(deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, params));
  } else {
    type.set_varchar();
    OZ(aggregate_charsets_for_string_result(type, types, param_num, type_ctx.get_coll_type()));
    for (int64_t i = 0; i < param_num; ++i) {
      types[i].set_calc_type(ObVarcharType);
      types[i].set_calc_collation_type(type.get_collation_type());
    }
  }

  if (OB_SUCC(ret)) {
    ObLength max_len = 0;
    for (int64_t i = 0; i < param_num; ++i) {
      if (ObRawType == types[i].get_type()) {
        max_len += types[i].get_length() * 2;
      } else {
        max_len += types[i].get_length();
      }
    }
    if (ObLongTextType == type.get_type()) {
      max_len = OB_MAX_LONGTEXT_LENGTH / 4;
    } else {
      if (share::is_oracle_mode()) {
        max_len = MIN(OB_MAX_ORACLE_VARCHAR_LENGTH, max_len);
      } else {
        max_len = MIN(OB_MAX_VARCHAR_LENGTH, max_len);
      }
    }
    type.set_length(max_len);
  }

  /*
    if (OB_SUCC(ret)) {
      ObLength max_len = 0;
      bool has_clob = false;
      bool has_varchar = false;
      bool has_nstring = false;
      bool is_oracle_mode = lib::is_oracle_mode();
      for (int64_t i = 0; i < param_num; ++i) {
        if (is_oracle_mode && types[i].is_text()) {
          has_clob = true;
        } else if (is_oracle_mode && (types[i].is_fixed_len_char_type() || types[i].is_null())) {
          types[i].set_calc_type(ObVarcharType);
        } else {
          has_varchar = true;
          types[i].set_calc_type(ObVarcharType);
        }
        has_nstring |= ob_is_nstring_type(types[i].get_type());
        if (ObRawType == types[i].get_type()) {
          max_len += types[i].get_length() * 2;
        } else {
          max_len += types[i].get_length();
        }
      }

      if (OB_UNLIKELY(has_clob)) {
        if (has_nstring) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("nclob not supported", K(ret));
        }
        type.set_type(ObLongTextType);
      } else if (has_varchar) {
        has_nstring ? type.set_nvarchar2() : type.set_varchar();
      } else {
        has_nstring ? type.set_nchar() : type.set_char();
      }
      if (OB_SUCC(ret) && is_oracle_mode) {
        if (param_num != 2) {
          ret = OB_INVALID_ARGUMENT_NUM;
          LOG_WARN("invalid argument number, param should not less than 1", K(ret), K(param_num));
        } else if (has_nstring) {
          type.set_length_semantics(LS_CHAR);
        } else {
          if (types[0].get_length_semantics() == types[1].get_length_semantics()) {
            type.set_length_semantics(types[0].get_length_semantics());
          } else {
            const ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session()) ?
    type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
            type.set_length_semantics(default_length_semantics);
          }
        }
      }
      if (ObLongTextType == type.get_type()) {
        max_len = OB_MAX_LONGTEXT_LENGTH / 4;
      } else {
        if (is_oracle_mode) {
          max_len = MIN(OB_MAX_ORACLE_VARCHAR_LENGTH, max_len);
        } else {
          max_len = MIN(OB_MAX_VARCHAR_LENGTH, max_len);
        }
      }
      type.set_length(max_len);
      //TODO::
      if (OB_FAIL(ret)) {}
      else if (OB_FAIL(aggregate_charsets_for_string_result(type,
                                                            types,
                                                            param_num,
                                                            type_ctx.get_coll_type()))) {
        LOG_WARN("aggregate_charsets_for_string_result failed", K(ret));
      } else {
        for (int64_t i = 0; i < param_num; i++) {
          types[i].set_calc_collation_type(type.get_collation_type());
          if (share::is_oracle_mode() && has_nstring && types[i].get_calc_type() == ObVarcharType) {
            types[i].set_calc_type(ObNVarchar2Type);
          }
        }
      }
    }
  */

  return ret;
}

int ObExprConcat::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& expr) const
{
  int ret = OB_SUCCESS;
  CK(expr.arg_cnt_ > 0);
  if (lib::is_oracle_mode()) {
    CK(2 == expr.arg_cnt_);
  }
  if (OB_SUCC(ret)) {
    expr.eval_func_ = &eval_concat;
  }
  return ret;
}

int ObExprConcat::eval_concat(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters values failed", K(ret));
  } else {
    ObDatum* first_not_null = NULL;
    int64_t null_cnt = 0;
    int64_t res_len = 0;

    for (int64_t i = 0; i < expr.arg_cnt_; i++) {
      ObDatum& v = expr.locate_param_datum(ctx, i);
      if (v.is_null()) {
        null_cnt += 1;
      } else {
        res_len += v.len_;
        if (NULL == first_not_null) {
          first_not_null = &v;
        }
      }
    }

    int64_t max_len = lib::is_oracle_mode() ? OB_MAX_ORACLE_VARCHAR_LENGTH : OB_MAX_VARCHAR_LENGTH;
    if (ob_is_text_tc(expr.datum_meta_.type_)) {
      // FIXME : mysql mode can not reach here, since result type is always varchar.
      max_len = OB_MAX_PACKET_LENGTH;
    }

    if (res_len > max_len) {
      expr_datum.set_null();
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("size overflow", K(ret), K(res_len), K(max_len));
    } else if (expr.arg_cnt_ == null_cnt || (!lib::is_oracle_mode() && null_cnt > 0)) {
      // input are all null or has null in mysql mode
      expr_datum.set_null();
    } else if (expr.arg_cnt_ - null_cnt == 1) {
      // only one valid input, shadow copy
      expr_datum.set_datum(*first_not_null);
    } else {
      char* buf = expr.get_str_res_mem(ctx, res_len);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(res_len));
      } else {
        int64_t off = 0;
        for (int64_t i = 0; i < expr.arg_cnt_; i++) {
          ObDatum& v = expr.locate_param_datum(ctx, i);
          if (!v.is_null()) {
            MEMCPY(buf + off, v.ptr_, v.len_);
            off += v.len_;
          }
        }
        OB_ASSERT(off == res_len);
      }
      expr_datum.set_string(buf, res_len);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
