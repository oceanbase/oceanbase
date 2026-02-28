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
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprConcat::ObExprConcat(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_OP_CNN, N_CONCAT, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}

ObExprConcat::~ObExprConcat()
{
}

int ObExprConcat::calc(ObObj &result, const ObObj &obj1, const ObObj &obj2,
                       ObIAllocator *allocator, const ObObjType result_type, bool is_oracle_mode,
                       const int64_t max_result_len)
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
        OZ (calc_text(result, obj1, obj2, allocator));
      } else {
        OZ (calc(result, str1, str2, allocator, is_oracle_mode, max_result_len));
      }
    }
  } else {
    if (OB_UNLIKELY(obj1.is_null()) ||
         OB_UNLIKELY(obj2.is_null())) {
      result.set_null();
    } else if (OB_UNLIKELY(obj1.is_text() || obj2.is_text())) {
      ret = calc_text(result, obj1, obj2, allocator);
    } else {
      TYPE_CHECK(obj1, ObVarcharType);
      TYPE_CHECK(obj2, ObVarcharType);
      ObString str1 = obj1.get_string();
      ObString str2 = obj2.get_string();
      ret = calc(result, str1, str2, allocator, false, 0);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("concat error", K(ret), K(is_oracle_mode), K(obj1), K(obj2));
    }
  }
  return ret;
}

// text tc
int ObExprConcat::calc_text(common::ObObj &result,
                            const common::ObObj obj1,
                            const common::ObObj obj2,
                            ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  int32_t max_length = OB_MAX_PACKET_LENGTH;
  // use a temp allocator to read lob data, the input allocator may not alloc more then once
  common::ObArenaAllocator temp_allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTextStringIter str_iter1(obj1);
  ObTextStringIter str_iter2(obj2);
  int64_t str1_byte_len = 0;
  int64_t str2_byte_len = 0;
  if (OB_FAIL(str_iter1.init(0, NULL, &temp_allocator))) {
    LOG_WARN("init str_iter1 failed ", K(ret), K(str_iter1));
  } else if (OB_FAIL(str_iter2.init(0, NULL, &temp_allocator))) {
    LOG_WARN("init str_iter2 failed ", K(ret), K(str_iter2));
  } else if (OB_FAIL(str_iter1.get_byte_len(str1_byte_len))) {
    LOG_WARN("init str_iter1 failed ", K(ret), K(str_iter1));
  } else if (OB_FAIL(str_iter2.get_byte_len(str2_byte_len))) {
    LOG_WARN("init str_iter1 failed ", K(ret), K(str_iter2));
  } else {
    bool has_lob_header = obj1.has_lob_header() || obj2.has_lob_header();
    ObTextStringResult tmp_lob(ObLongTextType, has_lob_header, allocator);
    int64_t res_data_byte_len = str1_byte_len + str2_byte_len;
    if (OB_UNLIKELY(res_data_byte_len > max_length)) {
      result.set_null();
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("length overflow", K(ret), K(str1_byte_len), K(str2_byte_len), K(max_length));
    } else if (OB_FAIL(tmp_lob.init(res_data_byte_len))) {
      LOG_WARN("init tmp lob failed", K(ret), K(res_data_byte_len));
    } else {
      ObTextStringIterState state;
      ObString src_block_data;
      while (OB_SUCC(ret)
             && (state = str_iter1.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
        if (OB_FAIL(tmp_lob.append(src_block_data))) {
          LOG_WARN("output_result append failed", K(ret), K(src_block_data));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
        ret = (str_iter1.get_inner_ret() != OB_SUCCESS) ?
              str_iter1.get_inner_ret() : OB_INVALID_DATA;
        LOG_WARN("iter state invalid", K(ret), K(state), K(str_iter1));
      }
      while (OB_SUCC(ret)
             && (state = str_iter2.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
        if (OB_FAIL(tmp_lob.append(src_block_data))) {
          LOG_WARN("output_result append failed", K(ret), K(src_block_data));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
        ret = (str_iter2.get_inner_ret() != OB_SUCCESS) ?
              str_iter2.get_inner_ret() : OB_INVALID_DATA;
        LOG_WARN("iter state invalid", K(ret), K(state), K(str_iter2));
      }
    }
    if (OB_SUCC(ret)) {
      ObString lob_loc_str;
      tmp_lob.get_result_buffer(lob_loc_str);
      result.set_lob_value(ObLongTextType, lob_loc_str.ptr(), lob_loc_str.length());
      if (tmp_lob.has_lob_header()) {
        result.set_has_lob_header();
      }
    }
  }

  return ret;
}

// non-text tc
int ObExprConcat::calc(common::ObObj &result,
                       const common::ObString obj1,
                       const common::ObString obj2,
                       ObIAllocator *allocator,
                       bool is_oracle_mode,
                       const int64_t max_result_len)
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
    //FIXME: 合并后的字符串长度超过了最大限制，结果设置为NULL
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
    char *buf = NULL;
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

int ObExprConcat::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types,
                                    int64_t param_num,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num <= 0)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  }

  CK (OB_NOT_NULL(type_ctx.get_session()));

  //类型 + 字符集推导
  if (lib::is_oracle_mode()) {
    ObSEArray<ObExprResType*, 2> params;
    for (int64_t i = 0; i < param_num; ++i) {
      OZ (params.push_back(&types[i]));
    }
    OZ (aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), params, type));
    OZ (deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, params));
  } else {
    bool has_text = false;
    for (int64_t i = 0; !has_text && i < param_num; ++i) {
      if (ObTinyTextType != types[i].get_type() && types[i].is_lob()) {
        has_text = true;
      }
    }
    if (has_text && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0) {
      type.set_type(ObLongTextType);
    } else {
      type.set_varchar();
    }
    OZ (aggregate_charsets_for_string_result(type,
                                             types,
                                             param_num,
                                             type_ctx));
    for (int64_t i = 0; i < param_num; ++i) {
      types[i].set_calc_type(type.get_type());
      types[i].set_calc_collation_type(type.get_collation_type());
      types[i].set_calc_collation_level(type.get_collation_level());
    }
  }

  //结果的长度推导
  if (OB_SUCC(ret)) {
    if (lib::is_oracle_mode()) {
      ObLength max_len = 0;
      for (int64_t i = 0; i < param_num; ++i) {
        max_len += types[i].get_calc_length();
      }
      type.set_length(max_len);
    } else {
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
        max_len = MIN(OB_MAX_VARCHAR_LENGTH, max_len);
        if (max_len <= 0) {
          max_len = OB_MAX_VARCHAR_LENGTH;
        }
      }
      type.set_length(max_len);
    }
  }


/*
  if (OB_SUCC(ret)) {
    // 计算函数参数的目标类型
    ObLength max_len = 0;
    bool has_clob = false;
    bool has_varchar = false;
    bool has_nstring = false;
    bool is_oracle_mode = lib::is_oracle_mode();
    for (int64_t i = 0; i < param_num; ++i) {
      if (is_oracle_mode && types[i].is_text()) {
        has_clob = true;
      } else if (is_oracle_mode && (types[i].is_fixed_len_char_type() || types[i].is_null())) {
        // 对于都是 char 的 concat, 都用 varchar 计算, 最后再转换为 char.
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

    // 计算函数结果的类型
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
        // oracle 只会有两个参数
        if (types[0].get_length_semantics() == types[1].get_length_semantics()) {
          type.set_length_semantics(types[0].get_length_semantics());
        } else {
          const ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
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
    //TODO::@yanhua
    if (OB_FAIL(ret)) {}
    else if (OB_FAIL(aggregate_charsets_for_string_result(type,
                                                          types,
                                                          param_num,
                                                          type_ctx.get_coll_type()))) {
      LOG_WARN("aggregate_charsets_for_string_result failed", K(ret));
    } else {
      for (int64_t i = 0; i < param_num; i++) {
        types[i].set_calc_collation_type(type.get_collation_type());
        if (lib::is_oracle_mode() && has_nstring && types[i].get_calc_type() == ObVarcharType) {
          types[i].set_calc_type(ObNVarchar2Type);
        }
      }
    }
  }
*/

  return ret;
}

int ObExprConcat::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  CK(expr.arg_cnt_ > 0);
  if (OB_SUCC(ret)) {
    expr.eval_func_ = &eval_concat;
    expr.eval_vector_func_ = &eval_concat_vector;
  }
  return ret;
}

int ObExprConcat::eval_concat_text(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum, const int64_t &res_len)
{
  int ret = OB_SUCCESS;
  ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
  if (OB_FAIL(output_result.init(res_len))) {
    LOG_WARN("init lob result failed");
  } else {
    int64_t off = 0;
    ObString v_str;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
      ObDatum &v = expr.locate_param_datum(ctx, i);
      if (v.is_null()) {
      } else {
        ObDatumMeta input_meta = expr.args_[i]->datum_meta_;
        bool has_lob_header = expr.args_[i]->obj_meta_.has_lob_header();
        ObTextStringIter input_iter(input_meta.type_, input_meta.cs_type_, v.get_string(), has_lob_header);
        ObTextStringIterState state;
        ObString src_block_data;
        if (OB_FAIL(input_iter.init(0, NULL, &calc_alloc))) {
          LOG_WARN("init input_iter failed ", K(ret), K(input_iter));
        }
        while (OB_SUCC(ret)
                && (state = input_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
          if (OB_FAIL(output_result.append(src_block_data))) {
            LOG_WARN("output_result append failed", K(ret), K(src_block_data));
          } else {
            off += src_block_data.length();
          }
        }
        if (OB_FAIL(ret)) {
        } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
          ret = (input_iter.get_inner_ret() != OB_SUCCESS) ?
                input_iter.get_inner_ret() : OB_INVALID_DATA;
          LOG_WARN("iter state invalid", K(ret), K(state), K(input_iter));
        }
      }
    }
    if (OB_SUCC(ret)) {
      output_result.set_result();
      OB_ASSERT(off == res_len);
    }
  }
  return ret;
}

int ObExprConcat::eval_concat(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters values failed", K(ret));
  } else {
    ObDatum *first_not_null = NULL;
    int64_t null_cnt = 0;
    int64_t res_len = 0;
    int64_t lob_data_byte_len = 0;
    ObObjType res_type = expr.datum_meta_.type_;
    // get result length
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
      ObDatum &v = expr.locate_param_datum(ctx, i);
      if (v.is_null()) {
        null_cnt += 1;
      } else {
        if (!ob_is_text_tc(expr.args_[i]->datum_meta_.type_)) {
          res_len += v.len_;
        } else {
          ObLobLocatorV2 locator(v.get_string(), expr.args_[i]->obj_meta_.has_lob_header());
          if (OB_FAIL(locator.get_lob_data_byte_len(lob_data_byte_len))) {
            LOG_WARN("get lob data byte length failed", K(ret), K(locator));
          } else {
            res_len += lob_data_byte_len;
          }
        }
        if (OB_SUCC(ret) && NULL == first_not_null) {
          first_not_null = &v;
        }
      }
    }
    int64_t max_len = 0;
    if (is_mysql_mode()) {
      max_len = OB_MAX_VARCHAR_LENGTH;
    } else if (expr.is_called_in_sql_) { // SQL in oracle mode
      max_len = OB_MAX_ORACLE_VARCHAR_LENGTH;
    } else { // PL in oracle mode
      const int64_t concat_res_max_len_in_pl = 65535;
      max_len = concat_res_max_len_in_pl;
    }
    if (ob_is_text_tc(res_type)) {
      // FIXME bin.lb: mysql mode can not reach here, since result type is always varchar.
      // Seem to be a bug:
      max_len = OB_MAX_PACKET_LENGTH;
    }
    // mysql mode: all param calc types are varchar;
    // oracle mode: if result type is longtext, param calc types must be longtext
    if (OB_FAIL(ret)) {
    } else if (res_len > max_len) {
      expr_datum.set_null();
      // BUGFIX: issue id 49051626
      if (lib::is_oracle_mode()) ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
      else ret = OB_SIZE_OVERFLOW;
      LOG_WARN("size overflow", K(ret), K(res_len), K(max_len));
    } else if (expr.arg_cnt_ == null_cnt
               || (!lib::is_oracle_mode() && null_cnt > 0)) {
      // input are all null or has null in mysql mode
      expr_datum.set_null();
    } else if ((expr.arg_cnt_ - null_cnt == 1) && !ob_is_text_tc(res_type)) {
      // only one valid input, shadow copy
      expr_datum.set_datum(*first_not_null);
    } else if (!ob_is_text_tc(res_type)) {
      char *buf = expr.get_str_res_mem(ctx, res_len);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(res_len));
      } else {
        int64_t off = 0;
        for (int64_t i = 0; i < expr.arg_cnt_; i++) {
          ObDatum &v = expr.locate_param_datum(ctx, i);
          if (!v.is_null()) {
            MEMCPY(buf + off, v.ptr_, v.len_);
            off += v.len_;
          }
        }
        OB_ASSERT(off == res_len);
      }
      expr_datum.set_string(buf, res_len);
    } else { // text tc
      ret = eval_concat_text(expr, ctx, expr_datum, res_len);
    }

  }
  return ret;
}

int ObExprConcat::eval_concat_text_vector(const ObExpr &expr, ObEvalCtx &ctx, const int64_t &res_len, int64_t idx)
{
  int ret = OB_SUCCESS;
  ObIVector *res_vec = static_cast<ObIVector *>(expr.get_vector(ctx));
  ObTextStringVectorResult<ObIVector> output_result(expr.datum_meta_.type_, &expr, &ctx, res_vec, idx);
  if (OB_FAIL(output_result.init_with_batch_idx(res_len, idx))) {
    LOG_WARN("init lob result failed");
  } else {
    int64_t off = 0;
    ObString v_str;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
      ObIVector *arg_vec = static_cast<ObIVector *>(expr.args_[i]->get_vector(ctx));
      if (arg_vec->is_null(idx)) {
        // do nothing
      } else {
        ObDatumMeta input_meta = expr.args_[i]->datum_meta_;
        bool has_lob_header = expr.args_[i]->obj_meta_.has_lob_header();
        ObTextStringIter input_iter(input_meta.type_, input_meta.cs_type_, arg_vec->get_string(idx), has_lob_header);
        ObTextStringIterState state;
        ObString src_block_data;
        if (OB_FAIL(input_iter.init(0, NULL, &calc_alloc))) {
          LOG_WARN("init input_iter failed ", K(ret), K(input_iter));
        }
        while (OB_SUCC(ret)
                && (state = input_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
          if (OB_FAIL(output_result.append(src_block_data))) {
            LOG_WARN("output_result append failed", K(ret), K(src_block_data));
          } else {
            off += src_block_data.length();
          }
        }
        if (OB_FAIL(ret)) {
        } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
          ret = (input_iter.get_inner_ret() != OB_SUCCESS) ?
                input_iter.get_inner_ret() : OB_INVALID_DATA;
          LOG_WARN("iter state invalid", K(ret), K(state), K(input_iter));
        }
      }
    }
    if (OB_SUCC(ret)) {
      output_result.set_result();
      OB_ASSERT(off == res_len);
    }
  }
  return ret;
}

int ObExprConcat::eval_concat_vector_for_text(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              const ObBitVector &skip,
                                              const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObIVector *res_vec = reinterpret_cast<ObIVector *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  int64_t max_len = 0;
  if (ob_is_text_tc(expr.datum_meta_.type_)) {
    max_len = OB_MAX_PACKET_LENGTH;
  } else if (lib::is_mysql_mode()) {
    max_len = OB_MAX_VARCHAR_LENGTH;
  } else if (expr.is_called_in_sql_) { // SQL in oracle mode
    max_len = OB_MAX_ORACLE_VARCHAR_LENGTH;
  } else { // PL in oracle mode
    max_len = 65535;
  }


  if (OB_FAIL(expr.eval_vector_param_value(ctx, skip, bound))) {
    LOG_WARN("fail to eval vector param value", K(ret));
  }

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else {
      int64_t null_cnt = 0;
      int64_t res_len = 0;
      ObObjType res_type = expr.datum_meta_.type_;

      // get result length
      for (int64_t arg_idx = 0; OB_SUCC(ret) && arg_idx < expr.arg_cnt_; ++arg_idx) {
        ObIVector *arg_vec = expr.args_[arg_idx]->get_vector(ctx);
        if (arg_vec->is_null(idx)) {
          null_cnt += 1;
        } else {
          if (!ob_is_text_tc(expr.args_[arg_idx]->datum_meta_.type_)) {
            res_len += arg_vec->get_length(idx);
          } else {
            int64_t lob_data_byte_len = 0;
            ObString lob_str = arg_vec->get_string(idx);
            ObLobLocatorV2 locator(lob_str, expr.args_[arg_idx]->obj_meta_.has_lob_header());
            if (OB_FAIL(locator.get_lob_data_byte_len(lob_data_byte_len))) {
              LOG_WARN("get lob data byte length failed", K(ret), K(locator));
            } else {
              res_len += lob_data_byte_len;
            }
          }
        }
      }
      // mysql mode: all param calc types are varchar;
      // oracle mode: if result type is longtext, param calc types must be longtext
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(res_len > max_len)) {
        res_vec->set_null(idx);
        // BUGFIX: issue id 49051626
        if (!lib::is_mysql_mode()) {
          ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
        } else {
          ret = OB_SIZE_OVERFLOW;
        }
        LOG_WARN("size overflow", K(ret), K(res_len), K(max_len));
      } else if ((!lib::is_mysql_mode() && expr.arg_cnt_ == null_cnt) || (lib::is_mysql_mode() && null_cnt > 0)) {
        // input are all null or has null in mysql mode
        res_vec->set_null(idx);
      } else {
        ret = eval_concat_text_vector(expr, ctx, res_len, idx);
      }
    }
  } // for end
  return ret;
}

template <typename ArgVec,bool ExistsNull, bool all_rows_active, bool is_mysql_mode>
int ObExprConcat::do_concat_memcpy_inner(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObBitVector &skip,
                                          const EvalBound &bound,
                                          char *buf_array[],
                                          ObLength res_lengths[],
                                          int64_t arg_idx)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ArgVec *arg_vec = reinterpret_cast<ArgVec *>(expr.args_[arg_idx]->get_vector(ctx));
  for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
    if (!all_rows_active && (skip.at(i) || eval_flags.at(i))) {
      continue;
    }
    if (!ExistsNull) {
      if (is_mysql_mode && res_lengths[i] < 0) {
        // do nothing
      } else {
        memcpy(buf_array[i], arg_vec->get_payload(i), arg_vec->get_length(i));
        buf_array[i] += arg_vec->get_length(i);
      }
    } else {
      if (is_mysql_mode) {
        if (res_lengths[i] >= 0) {
          memcpy(buf_array[i], arg_vec->get_payload(i), arg_vec->get_length(i));
          buf_array[i] += arg_vec->get_length(i);
        }
      } else {
        if (!arg_vec->is_null(i)) {
          memcpy(buf_array[i], arg_vec->get_payload(i), arg_vec->get_length(i));
          buf_array[i] += arg_vec->get_length(i);
        }
      }
    }
  }
  return ret;
}

template <typename ArgVec>
int ObExprConcat::do_concat_memcpy_dispatch(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            const ObBitVector &skip,
                                            const EvalBound &bound,
                                            char *buf_array[],
                                            ObLength res_lengths[],
                                            int64_t arg_idx)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = reinterpret_cast<ArgVec *>(expr.args_[arg_idx]->get_vector(ctx));
  bool exist_null = arg_vec->has_null();
  bool is_mysql_mode = lib::is_mysql_mode();
  bool all_rows_active = bound.get_all_rows_active();
  if (exist_null) {
    if (all_rows_active) {
      if (is_mysql_mode) {
        ret = do_concat_memcpy_inner<ArgVec, true, true, true>(expr, ctx, skip, bound, buf_array, res_lengths, arg_idx);
      } else {
        ret = do_concat_memcpy_inner<ArgVec, true, true, false>(expr, ctx, skip, bound, buf_array, res_lengths, arg_idx);
      }
    } else {
      if (is_mysql_mode) {
        ret = do_concat_memcpy_inner<ArgVec, true, false, true>(expr, ctx, skip, bound, buf_array, res_lengths, arg_idx);
      } else {
        ret = do_concat_memcpy_inner<ArgVec, true, false, false>(expr, ctx, skip, bound, buf_array, res_lengths, arg_idx);
      }
    }
  } else {
    if (all_rows_active) {
      if (is_mysql_mode) {
        ret = do_concat_memcpy_inner<ArgVec, false, true, true>(expr, ctx, skip, bound, buf_array, res_lengths, arg_idx);
      } else {
        ret = do_concat_memcpy_inner<ArgVec, false, true, false>(expr, ctx, skip, bound, buf_array, res_lengths, arg_idx);
      }
    } else {
      if (is_mysql_mode) {
        ret = do_concat_memcpy_inner<ArgVec, false, false, true>(expr, ctx, skip, bound, buf_array, res_lengths, arg_idx);
      } else {
        ret = do_concat_memcpy_inner<ArgVec, false, false, false>(expr, ctx, skip, bound, buf_array, res_lengths, arg_idx);
      }
    }
  }
  return ret;
}

template <typename ArgVec, bool ExistsNull, bool is_mysql_mode, bool all_rows_active>
int ObExprConcat::calc_concat_length_inner(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObBitVector &skip,
                                    const EvalBound &bound,
                                    int64_t arg_idx,
                                    ObLength res_lengths[])
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ArgVec *arg_vec = reinterpret_cast<ArgVec *>(expr.args_[arg_idx]->get_vector(ctx));
  for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
    if (!all_rows_active && (skip.at(i) || eval_flags.at(i))) {
      continue;
    }
    if (is_mysql_mode && res_lengths[i] < 0) {
      // mysql mode: if any param is null, then the result is null.
      // In this case, the result length would be set to -1.
      continue;
    }
    if (ExistsNull && arg_vec->is_null(i)) {
      if (is_mysql_mode) {
        res_lengths[i] = -1;
      }
      continue;
    }
    res_lengths[i] += arg_vec->get_length(i);
  }
  return ret;
}

template <typename ArgVec>
int ObExprConcat::calc_concat_length_dispatch(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const ObBitVector &skip,
                                       const EvalBound &bound,
                                       int64_t arg_idx,
                                       ObLength res_lengths[])
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = reinterpret_cast<ArgVec *>(expr.args_[arg_idx]->get_vector(ctx));
  bool all_rows_active = bound.get_all_rows_active();
  bool is_mysql_mode = lib::is_mysql_mode();
  bool exist_null = arg_vec->has_null();
  if (exist_null) {
    if (is_mysql_mode) {
      if (all_rows_active) {
        ret = calc_concat_length_inner<ArgVec, true, true, true>(expr, ctx, skip, bound, arg_idx, res_lengths);
      } else {
        ret = calc_concat_length_inner<ArgVec, true, true, false>(expr, ctx, skip, bound, arg_idx, res_lengths);
      }
    } else {
      if (all_rows_active) {
        ret = calc_concat_length_inner<ArgVec, true, false, true>(expr, ctx, skip, bound, arg_idx, res_lengths);
      } else {
        ret = calc_concat_length_inner<ArgVec, true, false, false>(expr, ctx, skip, bound, arg_idx, res_lengths);
      }
    }
  } else {
    if (is_mysql_mode) {
      if (all_rows_active) {
        ret = calc_concat_length_inner<ArgVec, false, true, true>(expr, ctx, skip, bound, arg_idx, res_lengths);
      } else {
        ret = calc_concat_length_inner<ArgVec, false, true, false>(expr, ctx, skip, bound, arg_idx, res_lengths);
      }
    } else {
      if (all_rows_active) {
        ret = calc_concat_length_inner<ArgVec, false, false, true>(expr, ctx, skip, bound, arg_idx, res_lengths);
      } else {
        ret = calc_concat_length_inner<ArgVec, false, false, false>(expr, ctx, skip, bound, arg_idx, res_lengths);
      }
    }
  }
  return ret;
}

template <typename ResVec, bool is_mysql_mode, bool all_rows_active>
int ObExprConcat::get_concat_result_buffer_inner(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObBitVector &skip,
                                          const EvalBound &bound,
                                          char *buf_array[],
                                          ObLength res_lengths[])
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ResVec *res_vec = reinterpret_cast<ResVec *>(expr.get_vector(ctx));
  int64_t max_len = 0;

  if (is_mysql_mode) {
    max_len = OB_MAX_VARCHAR_LENGTH;
  } else if (expr.is_called_in_sql_) { // SQL in oracle mode
    max_len = OB_MAX_ORACLE_VARCHAR_LENGTH;
  } else { // PL in oracle mode
    max_len = 65535;
  }

  for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
    if (!all_rows_active && (skip.at(i) || eval_flags.at(i))) {
      continue;
    }
    if (OB_UNLIKELY(res_lengths[i] > max_len)) {
      res_vec->set_null(i);
      // BUGFIX: issue id 49051626
      if (!is_mysql_mode) {
        ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
      } else {
        ret = OB_SIZE_OVERFLOW;
      }
      LOG_WARN("size overflow", K(ret), K(res_lengths[i]), K(max_len));
    } else if ((is_mysql_mode && res_lengths[i] < 0) || (!is_mysql_mode && res_lengths[i] == 0)) {
      // mysql mode: if any param is null, then the result is null.
      // In this case, the result length would be set to -1.
      // Oracle mode: if all params are null, then the result is null.
      // In this case, the result length would be 0.
      res_vec->set_null(i);
    } else {
      buf_array[i] = expr.get_str_res_mem(ctx, res_lengths[i], i);
      if (OB_ISNULL(buf_array[i])) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(res_lengths[i]));
      } else {
        res_vec->set_string(i, buf_array[i], res_lengths[i]);
      }
    }
  }
  return ret;
}

template <typename ResVec>
int ObExprConcat::get_concat_result_buffer_dispatch(const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             const ObBitVector &skip,
                                             const EvalBound &bound,
                                             char *buf_array[],
                                             ObLength res_lengths[])
{
  int ret = OB_SUCCESS;
  bool is_mysql_mode = lib::is_mysql_mode();
  bool all_rows_active = bound.get_all_rows_active();
  if (is_mysql_mode) {
    if (all_rows_active) {
      ret = get_concat_result_buffer_inner<ResVec, true, true>(expr, ctx, skip, bound, buf_array, res_lengths);
    } else {
      ret = get_concat_result_buffer_inner<ResVec, true, false>(expr, ctx, skip, bound, buf_array, res_lengths);
    }
  } else {
    if (all_rows_active) {
      ret = get_concat_result_buffer_inner<ResVec, false, true>(expr, ctx, skip, bound, buf_array, res_lengths);
    } else {
      ret = get_concat_result_buffer_inner<ResVec, false, false>(expr, ctx, skip, bound, buf_array, res_lengths);
    }
  }
  return ret;
}

int ObExprConcat::get_or_create_concat_ctx(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            int64_t required_capacity,
                                            ObExprConcatContext *&concat_ctx,
                                            char **&buf_array,
                                            ObLength *&res_lengths)
{
  int ret = OB_SUCCESS;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  uint64_t concat_id = static_cast<uint64_t>(expr.expr_ctx_id_);

  if (NULL == (concat_ctx = static_cast<ObExprConcatContext *>(exec_ctx.get_expr_op_ctx(concat_id)))) {
    // Create new context, use required capacity directly
    int64_t alloc_capacity = std::max<int64_t>(common::next_pow2(required_capacity), 256);
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(concat_id, concat_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(concat_id));
    } else {
      // Allocate buf_array and res_lengths
      char **buf_array_new = static_cast<char **>(exec_ctx.get_allocator().alloc(sizeof(char *) * alloc_capacity));
      ObLength *res_lengths_new = static_cast<ObLength *>(exec_ctx.get_allocator().alloc(sizeof(ObLength) * alloc_capacity));
      if (OB_ISNULL(buf_array_new) || OB_ISNULL(res_lengths_new)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc buf_array or res_lengths", K(ret), K(alloc_capacity));
      } else {
        memset(res_lengths_new, 0, sizeof(ObLength) * alloc_capacity);
        concat_ctx->reset();
        concat_ctx->set_allocator(&exec_ctx.get_allocator());
        concat_ctx->set_buf_array(buf_array_new);
        concat_ctx->set_res_lengths(res_lengths_new);
        concat_ctx->set_capacity(alloc_capacity);
        buf_array = buf_array_new;
        res_lengths = res_lengths_new;
      }
    }
  } else if (concat_ctx->get_capacity() < required_capacity) {
    // Reallocate if capacity is not enough, use next power of 2
    int64_t alloc_capacity = std::max<int64_t>(common::next_pow2(required_capacity), 256);
    char **buf_array_new = static_cast<char **>(exec_ctx.get_allocator().alloc(sizeof(char *) * alloc_capacity));
    ObLength *res_lengths_new = static_cast<ObLength *>(exec_ctx.get_allocator().alloc(sizeof(ObLength) * alloc_capacity));
    if (OB_ISNULL(buf_array_new) || OB_ISNULL(res_lengths_new)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to realloc buf_array or res_lengths", K(ret), K(alloc_capacity));
    } else {
      memset(res_lengths_new, 0, sizeof(ObLength) * alloc_capacity);
      concat_ctx->reset();
      concat_ctx->set_allocator(&exec_ctx.get_allocator());
      concat_ctx->set_buf_array(buf_array_new);
      concat_ctx->set_res_lengths(res_lengths_new);
      concat_ctx->set_capacity(alloc_capacity);
      buf_array = buf_array_new;
      res_lengths = res_lengths_new;
    }
  } else {
    // Capacity is sufficient, just get pointers
    buf_array = concat_ctx->get_buf_array();
    res_lengths = concat_ctx->get_res_lengths();
  }

  return ret;
}

int ObExprConcat::eval_concat_vector_by_column(const ObExpr &expr,
                                               ObEvalCtx &ctx,
                                               const ObBitVector &skip,
                                               const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObVectorBase *res_vec = reinterpret_cast<ObVectorBase *>(expr.get_vector(ctx));
  ObExprConcatContext *concat_ctx = NULL;
  char **buf_array = NULL;
  ObLength *res_lengths = NULL;

  // Get or create context and ensure capacity
  int64_t required_capacity = bound.end();
  if (OB_FAIL(get_or_create_concat_ctx(expr, ctx, required_capacity, concat_ctx, buf_array, res_lengths))) {
    LOG_WARN("failed to get or create concat ctx", K(ret));
  } else {
    // Reset res_lengths for current bound
    memset(res_lengths + bound.start(), 0, sizeof(ObLength) * (bound.end() - bound.start()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    if (OB_FAIL(expr.args_[i]->eval_vector(ctx, skip, bound))) {
      LOG_WARN("fail to eval sin param", K(ret));
    } else {
      VectorFormat arg_format = expr.args_[i]->get_format(ctx);
      if (arg_format == VEC_UNIFORM) {
        ret = calc_concat_length_dispatch<UniformFormat>(expr, ctx, skip, bound, i, res_lengths);
      } else if (arg_format == VEC_UNIFORM_CONST) {
        ret = calc_concat_length_dispatch<ConstUniformFormat>(expr, ctx, skip, bound, i, res_lengths);
      } else if (arg_format == VEC_DISCRETE) {
        ret = calc_concat_length_dispatch<ObDiscreteFormat>(expr, ctx, skip, bound, i, res_lengths);
      } else if (arg_format == VEC_CONTINUOUS) {
        ret = calc_concat_length_dispatch<ObContinuousFormat>(expr, ctx, skip, bound, i, res_lengths);
      } else {
        ret = calc_concat_length_dispatch<ObVectorBase>(expr, ctx, skip, bound, i, res_lengths);
      }
    }
  }

  if (OB_SUCC(ret)) {
    VectorFormat res_format = expr.get_format(ctx);
    if (res_format == VEC_UNIFORM) {
      ret = get_concat_result_buffer_dispatch<UniformFormat>(expr, ctx, skip, bound, buf_array, res_lengths);
    } else if (res_format == VEC_DISCRETE) {
      ret = get_concat_result_buffer_dispatch<ObDiscreteFormat>(expr, ctx, skip, bound, buf_array, res_lengths);
    } else if (res_format == VEC_CONTINUOUS) {
      ret = get_concat_result_buffer_dispatch<ObContinuousFormat>(expr, ctx, skip, bound, buf_array, res_lengths);
    } else {
      ret = get_concat_result_buffer_dispatch<ObVectorBase>(expr, ctx, skip, bound, buf_array, res_lengths);
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    VectorFormat arg_format = expr.args_[i]->get_format(ctx);
    if (arg_format == VEC_UNIFORM) {
      ret = do_concat_memcpy_dispatch<UniformFormat>(expr, ctx, skip, bound, buf_array, res_lengths, i);
    } else if (arg_format == VEC_UNIFORM_CONST) {
      ret = do_concat_memcpy_dispatch<ConstUniformFormat>(expr, ctx, skip, bound, buf_array, res_lengths, i);
    } else if (arg_format == VEC_DISCRETE) {
      ret = do_concat_memcpy_dispatch<ObDiscreteFormat>(expr, ctx, skip, bound, buf_array, res_lengths, i);
    } else if (arg_format == VEC_CONTINUOUS) {
      ret = do_concat_memcpy_dispatch<ObContinuousFormat>(expr, ctx, skip, bound, buf_array, res_lengths, i);
    } else {
      ret = do_concat_memcpy_dispatch<ObVectorBase>(expr, ctx, skip, bound, buf_array, res_lengths, i);
    }
  }

  if(OB_FAIL(ret)) {
  } else if (bound.get_all_rows_active()) {
  } else {
    for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
    }
  }
  return ret;
}

int ObExprConcat::eval_concat_vector(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     const ObBitVector &skip,
                                     const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  // check if exist text type
  bool exist_text = ob_is_text_tc(expr.datum_meta_.type_) && (expr.datum_meta_.type_ != ObTinyTextType);
  for (int64_t i = 0; OB_SUCC(ret) && !exist_text && i < expr.arg_cnt_; ++i) {
    exist_text = ob_is_text_tc(expr.args_[i]->datum_meta_.type_) && (expr.args_[i]->datum_meta_.type_ != ObTinyTextType);
  }
  if (exist_text) {
    ret = eval_concat_vector_for_text(expr, ctx, skip, bound);
  } else {
    ret = eval_concat_vector_by_column(expr, ctx, skip, bound);
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprConcat, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}
}
