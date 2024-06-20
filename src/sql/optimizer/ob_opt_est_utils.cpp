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

#define USING_LOG_PREFIX SQL_OPT
#include "lib/number/ob_number_v2.h"

#include "sql/optimizer/ob_opt_est_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"
#include <cmath>

namespace oceanbase
{
using namespace common;
using namespace number;
using namespace share::schema;
namespace sql
{
bool ObOptEstUtils::is_monotonic_op(const ObItemType type)
{
  return  (T_OP_ADD == type || T_OP_MINUS == type || T_OP_MUL == type || T_FUN_SYS_CAST == type);
}

int ObOptEstUtils::extract_column_exprs_with_op_check(
    const ObRawExpr *raw_expr,
    ObIArray<const ObColumnRefRawExpr*> &column_exprs,
    bool &only_monotonic_op,
    const int64_t level /* = 0 */)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (0 == level) {
    only_monotonic_op = true;
  }
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Raw expr is NULL", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (raw_expr->is_column_ref_expr()) {
    ret = column_exprs.push_back(static_cast<const ObColumnRefRawExpr *>(raw_expr));
  } else if (raw_expr->is_const_expr()) {
    //do nothing
  } else {
    if (!is_monotonic_op(raw_expr->get_expr_type())) {
      only_monotonic_op = false;
    }
    int64_t N = raw_expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(extract_column_exprs_with_op_check(raw_expr->get_param_expr(i),
                                                                column_exprs,
                                                                only_monotonic_op,
                                                                level + 1)))) {
        LOG_WARN("Failed to extract column exprs", K(ret));
      }
    }
  }
  return ret;
}


int ObOptEstUtils::is_range_expr(const ObRawExpr *qual, bool &is_simple_filter, const int64_t level)
{
  int ret = OB_SUCCESS;
  if (0 == level) {
    is_simple_filter = true;
  }
  if (OB_ISNULL(qual)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("qual is null", K(ret));
  } else if (IS_RANGE_CMP_OP(qual->get_expr_type()) && qual->has_flag(IS_RANGE_COND)) {
    // c1 > 1 , 1 < c1 do nothing
  } else if (T_OP_AND == qual->get_expr_type() || T_OP_OR == qual->get_expr_type()) {
    const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(qual);
    for (int idx = 0 ; idx < op_expr->get_param_count() && is_simple_filter && OB_SUCC(ret); ++idx) {
      if (OB_FAIL(is_range_expr(op_expr->get_param_expr(idx), is_simple_filter, level + 1))) {
        LOG_WARN("failed to judge if expr is range", K(ret));
      }
    }
  } else {
    is_simple_filter = false;
  }
  return ret;
}

int ObOptEstUtils::extract_simple_cond_filters(ObRawExpr &qual,
                                               bool &can_be_extracted,
                                               ObIArray<RangeExprs> &column_exprs_array)
{
  int ret = OB_SUCCESS;
  can_be_extracted = true;
  ObArray<ObRawExpr*> column_exprs;
  if (OB_FAIL(is_range_expr(&qual, can_be_extracted))) {
    LOG_WARN("judge range expr failed", K(ret));
  } else if (!can_be_extracted) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(&qual, column_exprs))) {
    LOG_WARN("extract_column_exprs error in clause_selectivity", K(ret));
  } else if (column_exprs.count() != 1) {
    can_be_extracted = false;
  } else {
    ObColumnRefRawExpr *column_expr = static_cast<ObColumnRefRawExpr *>(column_exprs.at(0));
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < column_exprs_array.count(); ++i) {
      if (column_exprs_array.at(i).column_expr_ == column_expr) {
        if (OB_FAIL(column_exprs_array.at(i).range_exprs_.push_back(&qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else {
          find = true;
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      RangeExprs *range_exprs = column_exprs_array.alloc_place_holder();
      if (OB_ISNULL(range_exprs)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc place holder", K(ret));
      } else if (OB_FAIL(range_exprs->range_exprs_.push_back(&qual))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        range_exprs->column_expr_ = column_expr;
      }
    }
  }
  return ret;
}

bool ObOptEstUtils::is_calculable_expr(const ObRawExpr &expr, const int64_t param_count)
{
  UNUSED(param_count);
  return expr.is_static_const_expr();
}

int ObOptEstUtils::get_expr_value(const ParamStore *params,
                                  const ObRawExpr &expr,
                                  ObExecContext *exec_ctx,
                                  ObIAllocator &allocator,
                                  bool &get_value,
                                  ObObj &value)
{
  int ret = OB_SUCCESS;
  get_value = false;
  if (OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null input", K(params), K(ret));
  } else if (is_calculable_expr(expr, params->count())) {
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                          &expr,
                                                          value,
                                                          get_value,
                                                          allocator))) {
      LOG_WARN("Failed to get const or calculable expr value", K(ret));
    }
  } else { }//do nothing
  return ret;
}

int ObOptEstUtils::if_expr_value_null(const ParamStore *params,
                                      const ObRawExpr &expr,
                                      ObExecContext *exec_ctx,
                                      ObIAllocator &allocator,
                                      bool &is_null)
{
  int ret = OB_SUCCESS;
  is_null = false;
  bool get_value = false;
  ObObj value;
  if (OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null input", K(params), K(ret));
  } else if (OB_FAIL(get_expr_value(params, expr, exec_ctx,
                                    allocator, get_value, value))) {
    LOG_WARN("Failed to get expr value", K(ret));
  } else if (get_value) {
    is_null = value.is_null();
  } else { }//do nothing
  return ret;
}

int ObOptEstUtils::if_expr_start_with_patten_sign(const ParamStore *params,
                                                  const ObRawExpr *expr,
                                                  const ObRawExpr *esp_expr,
                                                  ObExecContext *exec_ctx,
                                                  ObIAllocator &allocator,
                                                  bool &is_start_with,
                                                  bool &all_is_percent_sign)
{
  int ret = OB_SUCCESS;
  is_start_with = false;
  all_is_percent_sign = false;
  bool get_value = false;
  bool empty_escape = false;
  char escape;
  ObObj value;
  ObObj esp_value;
  if (OB_ISNULL(params) || OB_ISNULL(expr) || OB_ISNULL(esp_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null input", K(ret), K(params), K(expr), K(esp_expr));
  } else if (OB_FAIL(get_expr_value(params, *esp_expr, exec_ctx,
                                    allocator, get_value, esp_value))) {
    LOG_WARN("Failed to get expr value", K(ret));
  } else if (!get_value || !esp_value.is_string_type()) {
    // do nothing
  } else {
    if (esp_value.get_char().length() > 0) {
      escape = esp_value.get_char()[0];
    } else {
      empty_escape = true;
    }
    if (OB_FAIL(get_expr_value(params, *expr, exec_ctx, allocator, get_value, value))) {
      LOG_WARN("Failed to get expr value", K(ret));
    } else if (get_value && value.is_string_type() && value.get_string().length() > 0) {
      // 1. patten not start with `escape sign`
      // 2. patten start with `%` or `_` && `%` or `_` is not `escape sign`
      char start_c = value.get_string()[0];
      if (empty_escape) {
        is_start_with = ('%' == start_c || '_' == start_c);
      } else {
        is_start_with = (escape != start_c && ('%' == start_c || '_' == start_c));
      }
    } else { /* do nothing */ }
  }
  if (OB_SUCC(ret) && is_start_with) {
    all_is_percent_sign = true;
    const ObString &expr_str = value.get_string();
    for (int64_t i = 0; all_is_percent_sign && i < expr_str.length(); i++) {
      if (expr_str[i] != '%') {
        all_is_percent_sign = false;
      }
    }
  }
  return ret;
}

int ObOptEstUtils::if_expr_value_equal(ObOptimizerContext &opt_ctx,
                                       const ObDMLStmt *stmt,
                                       const ObRawExpr &first_expr,
                                       const ObRawExpr &second_expr,
                                       const bool null_safe,
                                       bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  bool get_first = false;
  bool get_second = false;
  ObObj first_value;
  ObObj second_value;
  const ParamStore *params = opt_ctx.get_params();
  ObSQLSessionInfo *session = opt_ctx.get_session_info();
  ObIAllocator &allocator = opt_ctx.get_allocator();
  ObExecContext *exec_ctx = opt_ctx.get_exec_ctx();
  CK( OB_NOT_NULL(exec_ctx),
      OB_NOT_NULL(params),
      OB_NOT_NULL(stmt));
  if (OB_FAIL(get_expr_value(
                params,
                first_expr,
                exec_ctx,
                allocator,
                get_first, first_value))) {
    LOG_WARN("Failed to get first value", K(ret));
  } else if (!get_first) {
    equal = false;
  } else if (OB_FAIL(get_expr_value(params, second_expr,
                                    exec_ctx, allocator,
                                    get_second, second_value))) {
    LOG_WARN("Failed to get second value", K(ret));
  } else if (!get_second) {
    equal = false;
  } else if (first_value.is_null() && second_value.is_null() && null_safe) {
    equal = true;
  } else if (first_value.is_null() || second_value.is_null()) {
    equal = false;
  } else if (first_value.is_min_value() || second_value.is_min_value()) {
    equal = (first_value.is_min_value() && second_value.is_min_value());
  } else if (first_value.is_max_value() || second_value.is_max_value()) {
    equal = (first_value.is_max_value() && second_value.is_max_value());
  } else if (first_value.can_compare(second_value)
             && first_value.get_collation_type() == second_value.get_collation_type()) {
    equal = (first_value == second_value);
  } else {
    //When type of value or collation_type is different.We need to use
    //ObExprEqual to check whether values equal.
    //(When realize histogram, we need to considering this in more cases.)
    //'a' = 'A' with general_ci collation, true
    //'a' = 'A' without general_ci collation, false
    //'1' = 1, true
    //'a' = 0, true
    //'a' = 1, false
    ObExprCtx expr_ctx;
    //As this function may called in places that we don't have op expr, but want to check
    //whether the value of cacluable expr is equal. So we calc result type here.
    ObExprTypeCtx type_ctx;
//    type_ctx.my_session_ = exec_ctx->get_my_session();
    ObSQLUtils::init_type_ctx(session, type_ctx);

    ObExprEqual equal_op(allocator);
    ObExprResType result_type;
    ObExprResType first_type = first_expr.get_result_type();
    ObExprResType second_type = second_expr.get_result_type();
    ObOpRawExpr equal_expr(const_cast<ObRawExpr *>(&first_expr), const_cast<ObRawExpr *>(&second_expr), T_OP_EQ);
    type_ctx.set_raw_expr(&equal_expr);
    equal_op.set_raw_expr(&equal_expr);
    if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt->get_stmt_type(), *exec_ctx, allocator, expr_ctx))) {
      LOG_WARN("Failed to wrap expr ctx", K(ret));
    } else if (OB_FAIL(equal_op.calc_result_type2(result_type, first_type, second_type, type_ctx))) {
      LOG_WARN("Failed to calc result type", K(ret));
    } else {
      ObCompareCtx cmp_ctx(result_type.get_type(),
                           result_type.get_collation_type(),
                           null_safe,
                           expr_ctx.tz_offset_,
                           default_null_pos());
      //cast_mode is CM_WARN_ON_FAIL in select_stmt||explain_stmt||not_strict_sql_mode
      //CM_WARN_ON_FAIL would cast 'a' to 0 without report error.
      //CM_NONE, the cast 'a' to int would return error.
      //Here we just use CM_WARN_ON_FAIL, as if CM_NONE, exectution would report the error.
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);
      ObObj result;
      if (OB_FAIL(ObExprEqual::calc(result, first_value, second_value, cmp_ctx, cast_ctx))) {
        LOG_WARN("Compare expression failed", K(ret));
        ret = OB_SUCCESS;
      } else {
        equal = result.is_true();
      }
    }

  }
  return ret;
}

int ObOptEstUtils::columns_has_unique_subset(const ObIArray<uint64_t> &full,
                                             const ObRowkeyInfo &sub,
                                             bool &is_subset)
{
  int ret = OB_SUCCESS;
  is_subset = true;
  LOG_TRACE("show row key info", K(sub.get_size()), K(sub));
  // 使用时注意 sub 为空集的情况.
  // 注意提前验证 sub 的所在的 schema 有 full 中的列.
  for (int64_t i = 0; OB_SUCC(ret) && is_subset && i < sub.get_size(); ++i) {
    uint64_t sub_column_id = OB_INVALID_ID;
    if (OB_FAIL(sub.get_column_id(i, sub_column_id))) {
      LOG_WARN("failed to get column id", K(ret));
    } else if (OB_INVALID_ID != sub_column_id) {
      bool is_find = false;
      for (int64_t j = 0; !is_find && j < full.count(); ++j) {
        if (full.at(j) == sub_column_id) {
          is_find = true;
        }
      }
      is_subset = is_find;
    }
  }
  return ret;
}

int ObOptEstObjToScalar::convert_obj_to_scalar(const ObObj *obj, double &scalar)
{
  int ret = OB_SUCCESS;
  scalar = 0.0;

  if (NULL == obj) {
    //NULL obj means a double 0.0 as scalar to return
  } else {
    switch (obj->get_type()) {
    case ObNullType:
        scalar = 0;
        break;
    case ObTinyIntType:              // int8, aka mysql boolean type
        scalar = static_cast<double>(obj->get_tinyint());
        break;
    case ObSmallIntType:               // int16
        scalar = static_cast<double>(obj->get_smallint());
        break;
    case ObMediumIntType:              // int24
        scalar = static_cast<double>(obj->get_mediumint());
        break;
    case ObInt32Type:                 // int32
        scalar = static_cast<double>(obj->get_int32());
        break;
    case ObIntType:                    // int64, aka bigint
        scalar = static_cast<double>(obj->get_int());
        break;
    case ObUTinyIntType:                // uint8
        scalar = static_cast<double>(obj->get_utinyint());
        break;
    case ObUSmallIntType:               // uint16
        scalar = static_cast<double>(obj->get_usmallint());
        break;
    case ObUMediumIntType:              // uint24
        scalar = static_cast<double>(obj->get_umediumint());
        break;
    case ObUInt32Type:                    // uint32
        scalar = static_cast<double>(obj->get_uint32());
        break;
    case ObUInt64Type:                 // uint64
        scalar = static_cast<double>(obj->get_uint64());
        break;
    case ObFloatType:                  // single-precision floating point
        scalar = static_cast<double>(obj->get_float());
        break;
    case ObDoubleType:                 // double-precision floating point
        scalar = obj->get_double();
        break;
    case ObUFloatType:            // unsigned single-precision floating point
        scalar = static_cast<double>(obj->get_ufloat());
        break;
    case ObUDoubleType:           // unsigned double-precision floating point
        scalar = static_cast<double>(obj->get_udouble());
        break;
    case ObNumberType:
    case ObUNumberType:
    case ObNumberFloatType:
    case ObDecimalIntType:
        // aka decimal/numeric, already converted to double in `convert_obj_to_scalar_obj`
        scalar = static_cast<double>(obj->get_double());
        break;
    case ObDateTimeType:
    case ObTimestampType:
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType:
        scalar = static_cast<double>(obj->get_datetime());
        break;
    case ObDateType:
        scalar = static_cast<double>(obj->get_date());
        break;
    case ObTimeType:
        scalar = static_cast<double>(obj->get_time());
        break;
    case ObYearType:
        scalar = static_cast<double>(obj->get_year());
        break;
    // TODO@hanhui text share with varchar temporarily
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObVarcharType: {  // charset: utf-8, collation: utf8_general_ci
        const ObString &str = obj->get_varchar();
        ret = convert_string_to_scalar(obj->get_collation_type(), str, scalar);
        break;
    }
    case ObCharType:
    case ObNCharType:
    case ObNVarchar2Type: {    // charset: utf-8, collation: utf8_general_ci
        const ObString &str = obj->get_string();
        ret = convert_string_to_scalar(obj->get_collation_type(), str, scalar);
        break;
    }
    case ObHexStringType: {
        const ObString &str = obj->get_varbinary();
        ret = convert_string_to_scalar(obj->get_collation_type(), str, scalar);
        break;
    }
    case ObRawType: {
        const ObString &str = obj->get_raw();
        ret = convert_string_to_scalar(obj->get_collation_type(), str, scalar);
        break;
    }
    case ObIntervalYMType: {
        scalar = static_cast<double>(obj->get_interval_ym().get_nmonth());
        break;
    }
    case ObIntervalDSType: {
        scalar = static_cast<double>(
          obj->get_interval_ds().get_nsecond() * ObIntervalDSValue::MAX_FS_VALUE
          + obj->get_interval_ds().get_fs());
        break;
    }
    case ObExtendType:                 // Min, Max, NOP etc.
    case ObUnknownType:                // For question mark(?) in prepared statement, no need to serialize
      //TODO:
        break;
    default:
        break;
    }
  }

  return ret;
}

int ObOptEstObjToScalar::convert_obj_to_double(const ObObj *obj, double &num)
{
  int ret = OB_SUCCESS;
  num = 0.0;
  if (OB_ISNULL(obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("obj is null", K(ret));
  } else if (ObNumberType == obj->get_type() || ObUNumberType == obj->get_type()
             || ObDecimalIntType == obj->get_type()) {
    ObObj calc_obj;
    ObArenaAllocator calc_buffer(ObModIds::OB_BUFFER);
    // tz_info is UNUSED in converting number to double
    ObCastCtx cast_ctx(&calc_buffer, NULL, CM_NONE, obj->get_collation_type());
    const ObObj *ref_out = NULL;
    if (OB_SUCCESS == (ret = ObObjCaster::to_type(ObDoubleType, cast_ctx, *obj, calc_obj, ref_out))) {
      if (OB_ISNULL(ref_out)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get NULL ObObj after cast", K(ret));
      } else {
        num = ref_out->get_double();
      }
    } else {
      if (OB_LIKELY(OB_DATA_OUT_OF_RANGE == ret)) {
        num = 0.0;
      } else {
        LOG_WARN("failed to get double from number", K(ret));
      }
    }
  } else if (OB_FAIL(convert_obj_to_scalar(obj, num))) {
    LOG_WARN("failed to convert obj to scalar", K(ret));
  }
  return ret;
}

int ObOptEstObjToScalar::convert_obj_to_scalar_obj(const common::ObObj* obj, common::ObObj* out)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj) || OB_ISNULL(out)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input or output is null", KP(obj), KP(out), K(ret));
  } else {
    switch (obj->get_type()) {
    case ObDecimalIntType:
    case ObNumberFloatType:
    case ObNumberType: // aka decimal/numeric
        // same as under
    case ObUNumberType: {
      ObObj calc_obj;
      ObArenaAllocator calc_buffer(ObModIds::OB_BUFFER);
      // tz_info is UNUSED in converting number to double
      ObCastCtx cast_ctx(&calc_buffer, NULL, CM_NONE, obj->get_collation_type());
      const ObObj *ref_out = NULL;
      if (OB_SUCCESS == (ret = ObObjCaster::to_type(ObDoubleType, cast_ctx, *obj, calc_obj, ref_out))) {
        if (OB_ISNULL(ref_out)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get NULL ObObj after cast", K(ret));
        } else {
          out->set_double(ref_out->get_double());
        }
      } else {
        if (OB_LIKELY(OB_DATA_OUT_OF_RANGE == ret)) {
          if (obj->is_decimal_int()) {
            if (wide::is_negative(obj->get_decimal_int(), obj->get_int_bytes())) {
              out->set_min_value();
            } else {
              out->set_max_value();
            }
          } else if (obj->get_number().is_negative()) {
            out->set_min_value();
          } else {
            out->set_max_value();
          }
        } else {
          LOG_WARN("failed to get double from number", K(ret));
        }
      }
      break;
    }
    case ObExtendType:
    case ObUnknownType: {
      //pass through min, max, etc
      *out = *obj;
      break;
    }
    default: {
      double num = 0.0;
      if (OB_FAIL(convert_obj_to_scalar(obj, num))) {
        LOG_WARN("failed to convert obj to scalar", K(ret));
      } else {
        out->set_double(num);
      }
      break;
    }
    }
  }
  return ret;
}


int ObOptEstObjToScalar::convert_objs_to_scalars(
    const ObObj *min,
    const ObObj *max,
    const ObObj *start,
    const ObObj *end,
    ObObj *min_out,
    ObObj *max_out,
    ObObj *start_out,
    ObObj *end_out,
    bool convert2sortkey)
{
  int ret = OB_SUCCESS;
  const static int64_t START_POS = 0;
  const static int64_t END_POS = 1;
  const static int64_t MIN_POS = 2;
  const static int64_t MAX_POS = 3;
  const static int64_t OBJ_COUNT = 4;
  int64_t skip_count = 0;
  const ObObj *input_ptrs[OBJ_COUNT] = {start, end, min, max};
  ObObj *output_ptrs[OBJ_COUNT] = {start_out, end_out, min_out, max_out};
  ObSEArray<double, 4> string_scalars;
  //this map is for recording which obj is converted using new method
  uint64_t str_conv_map = 0;
  if (OB_ISNULL(start) || OB_ISNULL(end) || OB_ISNULL(start_out) || OB_ISNULL(end_out)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start or end obj not specified", K(ret), K(start), K(end), K(start_out), K(end_out));
  } else if ((NULL == min) != (NULL == max)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("min and max obj not specified together", KP(min), KP(max), K(ret));
  } else if (((NULL == min) != (NULL == min_out))
      || ((NULL == max) != (NULL == max_out))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input and output pair not specified together",
        KP(min), KP(min_out), KP(max), KP(max_out),
        KP(start), KP(start_out), KP(end), KP(end_out), K(ret));
  } else {
    bool with_min_max = (NULL != min);
    //check whether to use string conversion method : all string except for min / max
    bool null_first_check = !lib::is_oracle_mode() && start->is_null(); // for mysql
    bool null_last_check = lib::is_oracle_mode() && end->is_null(); // for oracle
    bool use_dynamic_base = (start->is_string_type() || start->is_min_value() || start->is_max_value() || null_first_check)
        && (end->is_string_type() || end->is_min_value() || end->is_max_value() || null_last_check);
    if (use_dynamic_base && with_min_max) {
      use_dynamic_base &= ((min->is_string_type() || min->is_min_value() || min->is_max_value()));
      use_dynamic_base &= ((max->is_string_type() || max->is_min_value() || max->is_max_value()));
    }
    if (use_dynamic_base) {
      //Special case for All String : truncate common header and use dynamic base
      ObString str;
      ObSEArray<ObString, 4> strs;
      ObSEArray<ObCollationType, 4> cs_type;
      if (start->is_string_type()
          && OB_FAIL(add_to_string_conversion_array(*start, cs_type, strs, str_conv_map, START_POS))) {
        LOG_WARN("Failed to add start to convert array", K(ret));
      } else if (end->is_string_type()
          && OB_FAIL(add_to_string_conversion_array(*end, cs_type, strs, str_conv_map, END_POS))) {
        LOG_WARN("Failed to add end to convert array", K(ret));
      } else if (with_min_max) {
        if (min->is_string_type()
            && OB_FAIL(add_to_string_conversion_array(*min, cs_type, strs, str_conv_map, MIN_POS))) {
          LOG_WARN("Failed to add min to convert array", K(ret));
        } else if (max->is_string_type()
            && OB_FAIL(add_to_string_conversion_array(*max, cs_type, strs, str_conv_map, MAX_POS))) {
          LOG_WARN("Failed to add min to convert array", K(ret));
        } else {
          //do nothing
        }
      }
      if (OB_SUCC(ret)) {
        if (strs.count() > 0) {
          if (!convert2sortkey) {
            for (int64_t i = 0; i < cs_type.count(); i ++) {
              cs_type.at(i) = CS_TYPE_BINARY;
            }
          }
          if (OB_FAIL(convert_strings_to_scalar(cs_type, strs, string_scalars))) {
            LOG_WARN("Failed to convert string scalar", K(ret));
          } else if (string_scalars.count() != strs.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Scalars and strings not match",
                K(string_scalars.count()), K(strs.count()), K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < OBJ_COUNT; ++i) {
      ObObj *out_ptr = output_ptrs[i];
      const ObObj *in_ptr = input_ptrs[i];
      if ((START_POS == i || END_POS == i) && (OB_ISNULL(in_ptr) || OB_ISNULL(out_ptr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("start or end is null", K(i), K(in_ptr), K(out_ptr), K(ret));
      } else if ((MIN_POS == i || MAX_POS == i) && ((NULL == in_ptr) != (NULL == out_ptr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input or output for min max not given together",
            K(i), K(in_ptr), K(out_ptr), K(ret));
      } else {
        if (str_conv_map & (0x1 << i)) {
          //this obj is already converted using string special method:
          out_ptr->set_double(string_scalars.at(i - skip_count));
        } else {
          //this obj is to be converted using normal method:
          ++skip_count;
          if (NULL != out_ptr && NULL != in_ptr) {
            if (OB_FAIL(convert_obj_to_scalar_obj(in_ptr, out_ptr))) {
              LOG_WARN("Failed to convert obj using old method", K(ret));
            }
          }
        }
      }
    }

    if (lib::is_oracle_mode()) {
      if (!start->is_null() && end->is_null()) {
        end_out->set_max_value();//TODO 暂且把这个设置为max value但是这样并不是太好,
        //后面需要更强的区分能力，可以区分是否包含NULL,计算NULL sel不同
      }
    } else {
      if (start->is_null() && !end->is_null()) {
        start_out->set_min_value();//TODO 暂且把这个设置为min value但是这样并不是太好,
        //后面需要更强的区分能力，可以区分是否包含NULL,计算NULL sel不同
      }
    }
  }
  return ret;
}

int ObOptEstObjToScalar::add_to_string_conversion_array(
    const ObObj &strobj,
    common::ObIArray<ObCollationType> &cs_type,
    ObIArray<common::ObString> &arr,
    uint64_t &convertable_map,
    int64_t pos)
{
  int ret = OB_SUCCESS;
  ObString str;
  if (!strobj.is_string_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("obj is not string", K(ret));
  } else if (convertable_map & (0x1 << pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Already in array", K(pos), K(ret));
  } else if (OB_FAIL(strobj.get_string(str))) {
    LOG_WARN("Failed to get string", K(ret));
  } else if (OB_FAIL(arr.push_back(str))) {
    LOG_WARN("Failed to push back", K(ret));
  } else if (OB_FAIL(cs_type.push_back(strobj.get_collation_type()))) {
    LOG_WARN("failed to push back", K(ret));
  } else {
    convertable_map |= (0x1 << pos);
  }
  return ret;
}

int ObOptEstObjToScalar::convert_strings_to_scalar(
    const common::ObIArray<ObCollationType> &cs_type,
    const common::ObIArray<common::ObString> &origin_strs,
    common::ObIArray<double> &scalars)
{
  int ret = OB_SUCCESS;
  ObString str;
  double base = 256.0;
  uint8_t offset = 0;
  int64_t common_prefix_length = 0;
  ObArenaAllocator tmp_alloc("ObOptEstUtils");
  common::ObSEArray<common::ObString, 4> sort_keys;
  if (OB_UNLIKELY(origin_strs.count() != cs_type.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cs type", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < origin_strs.count(); i ++)
  {
    ObString *sort_key = sort_keys.alloc_place_holder();
    if (OB_ISNULL(sort_key)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate", K(ret));
    } else if (OB_FAIL(get_string_sort_key(tmp_alloc, cs_type.at(i), origin_strs.at(i), *sort_key))) {
      LOG_WARN("failed to get sort key", K(ret));
    }
  }
  if (FAILEDx(find_common_prefix_len(sort_keys, common_prefix_length))) {
    LOG_WARN("Failed to find common prefix length", K(ret));
  } else if (OB_FAIL(find_string_scalar_offset_base(sort_keys, common_prefix_length, offset, base))) {
    LOG_WARN("Failed to find offset and base", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_strs.count(); ++i) {
      double scalar = convert_string_to_scalar(sort_keys.at(i),
                                               common_prefix_length,
                                               offset,
                                               base);
      if (OB_FAIL(scalars.push_back(scalar))) {
        LOG_WARN("Failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObOptEstObjToScalar::find_common_prefix_len(
    const ObIArray<ObString> &strs,
    int64_t &length)
{
  int ret = OB_SUCCESS;
  length = 0;
  if (strs.count() == 0) {
    length = 0;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < strs.count(); ++i) {
      const ObString &str = strs.at(i);
      if (str.length() < 0
          || (str.length() > 0 && str.ptr() == NULL)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid str", K(str), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t i = 0;
      bool found = false;
      while (OB_SUCC(ret) && !found) {
        char target_char = '\0';
        for (int64_t stri = 0; !found && stri < strs.count(); ++stri) {
          const ObString &str = strs.at(stri);
          if (str.length() == i) {
            found = true;
            length = i;
            //end of one string, common prefix = str[0 : i - 1], len = i
          } else {
            if (0 == stri) {
              target_char = str[i];
            } else {
              if (str[i] == target_char) {
                //same char on this posision, check next str
              } else {
                found = true;
                length = i;
                //different char found, common prefix = str[0 : i - 1], len = i
              }
            }
          }
        }
        ++i;
      }
    }
  }
  return ret;
}

int ObOptEstObjToScalar::find_string_scalar_offset_base(
    const ObIArray<ObString> &strs,
    int64_t prefix_len,
    uint8_t &offset,
    double &base)
{
  int ret = OB_SUCCESS;
  if (prefix_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Prefix len should not less than 0", K(ret), K(prefix_len));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < strs.count(); ++i) {
    const ObString &str = strs.at(i);
    if (str.length() < 0
        || (str.length() > 0 && str.ptr() == NULL)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid str", K(str), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    uint8_t min = UINT8_MAX;
    uint8_t max = 0;
    for (int64_t stri = 0; OB_SUCC(ret) && stri < strs.count(); ++stri) {
      const ObString &str = strs.at(stri);
      //start from the char after common prefix, find min max of all bytes of all strs
      for (int64_t i = prefix_len; i < str.length(); ++i) {
        if (isdigit(str[i])) {
          expand_range(min, max, '0', '9');
        } else if (islower(str[i])) {
          expand_range(min, max, 'a', 'z');
        } else if (isupper(str[i])) {
          expand_range(min, max, 'A', 'Z');
        } else {
          expand_range(min, max, str[i], str[i]);
        }
      }
    }
    if (max == min || (UINT8_MAX == min && 0 == max)) {
      //if no char processed, or only one non-digit non-upper-or-lower char processed,
      //fallback to old method
      offset = 0;
      base = 256;
    } else {
      offset = min;
      base = static_cast<double>(max - min + 1);
    }
  }
  return ret;
}

int ObOptEstObjToScalar::get_string_sort_key(ObIAllocator &alloc, ObCollationType cs_type,
                                             const common::ObString &str, common::ObString &sort_key)
{
  int ret = OB_SUCCESS;
  const ObCharsetInfo *cs = ObCharset::get_charset(cs_type);
  if (ObCharset::is_bin_sort(cs_type) || str.empty() ||
      NULL == cs || NULL == cs->coll) {
    sort_key = str;
  } else {
    size_t buf_len = cs->coll->strnxfrmlen(cs, str.length()) * cs->mbmaxlen;
    ObArrayWrap<char> buffer;
    bool is_valid_character = false;
    if (OB_FAIL(buffer.allocate_array(alloc, buf_len))) {
      LOG_WARN("failed to allocate", K(ret));
    } else {
      size_t sort_key_len = ObCharset::sortkey(cs_type, str.ptr(), str.length(),
                                               buffer.get_data(), buf_len, is_valid_character);
      sort_key.assign_ptr(buffer.get_data(), sort_key_len);
    }
  }
  return ret;
}

int ObOptEstObjToScalar::convert_string_to_scalar(ObCollationType cs_type, const common::ObString &str, double &scalar)
{
  int ret = OB_SUCCESS;
  ObString sort_key;
  ObArenaAllocator tmp_alloc("ObOptEstUtils");
  if (OB_FAIL(get_string_sort_key(tmp_alloc, cs_type, str, sort_key))) {
    LOG_WARN("failed to get sort key", K(ret));
  } else {
    scalar = convert_string_to_scalar(sort_key);
  }
  return ret;
}

double ObOptEstObjToScalar::convert_string_to_scalar(
    const common::ObString &str,
    int64_t prefix_len,
    uint8_t offset,
    double base)
{
  if (prefix_len < 0) {
    prefix_len = 0;
  }
  if (fabs(base - 0.0) < OB_DOUBLE_EPSINON) {
    //base is 0, fallback to base 256
    base = 256.0;
    offset = 0;
  }
  double scalar = 0;
  double weight = base;
  const char* ptr = str.ptr();
  for (int64_t i = prefix_len; i < str.length(); ++i) {
    scalar += ((uint8_t)ptr[i] - offset) / weight;
    weight *= base;
  }
  return scalar;
}

int ObOptEstObjToScalar::convert_string_to_scalar_for_number(
    const common::ObString &str, double &scalar)
{
  int ret = OB_SUCCESS;
  scalar = 0;
  if (NULL != str.ptr()) {
    if (1 != sscanf(str.ptr(), "%lf", &scalar)) {
    	ret = OB_INVALID_DATA;
    	LOG_WARN("failed to get back info", K(ret));
    } else { /* do nothing*/ }
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
