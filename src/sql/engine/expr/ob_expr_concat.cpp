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
#include "objit/common/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
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
  if (OB_UNLIKELY(param_num <= 0
                  || (lib::is_oracle_mode() && param_num != 2))) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  }

  CK (OB_NOT_NULL(type_ctx.get_session()));

  //类型 + 字符集推导
  if (lib::is_oracle_mode()) {
    ObSEArray<ObExprResType*, 2, ObNullAllocator> params;
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
                                             type_ctx.get_coll_type()));
    for (int64_t i = 0; i < param_num; ++i) {
      types[i].set_calc_type(type.get_type());
      types[i].set_calc_collation_type(type.get_collation_type());
    }
  }

  //结果的长度推导
  if (OB_SUCC(ret)) {
    if (lib::is_oracle_mode()) {
      type.set_length(types[0].get_calc_length() + types[1].get_calc_length());
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
  if (lib::is_oracle_mode()) {
    CK(2 == expr.arg_cnt_);
  }
  if (OB_SUCC(ret)) {
    expr.eval_func_ = &eval_concat;
  }
  return ret;
}

static int eval_concat_text(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum, const int64_t &res_len)
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

}
}
