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
#include "sql/engine/expr/ob_expr_insert.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

ObExprInsert::ObExprInsert(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_INSERT, N_INSERT, MORE_THAN_TWO, VALID_FOR_GENERATED_COL)
{
}

ObExprInsert::~ObExprInsert()
{
}

int ObExprInsert::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types_array,
                                    int64_t param_num,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObjMeta, 16> coll_types;
  ObObjMeta coll0, coll3; // insert 的第一个、第四个参数才需要参与计算 charset
  if (4 != param_num) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert should have four arguments",K(ret));
  } else {
    type.set_varchar();
    type.set_length(types_array[0].get_length() + types_array[3].get_length());
    coll0.set_collation_type(types_array[0].get_collation_type());
    coll0.set_collation_level(types_array[0].get_collation_level());
    coll3.set_collation_type(types_array[3].get_collation_type());
    coll3.set_collation_level(types_array[3].get_collation_level());
    if (OB_FAIL(coll_types.push_back(coll0))) {
      LOG_WARN("fail push col", K(coll0), K(ret));
    } else if (OB_FAIL(coll_types.push_back(coll3))) {
      LOG_WARN("fail push col", K(coll3), K(ret));
    } else if (OB_FAIL(aggregate_charsets_for_string_result(
                type, &coll_types.at(0), 2, type_ctx.get_coll_type()))) {
      LOG_WARN("aggregate cahrset for string result failed", K(ret));
    } else {
      types_array[0].set_calc_type(ObVarcharType);
      types_array[1].set_calc_type(ObIntType);
      types_array[2].set_calc_type(ObIntType);
      types_array[3].set_calc_type(ObVarcharType);
      types_array[0].set_calc_collation_type(type.get_collation_type());
      types_array[3].set_calc_collation_type(type.get_collation_type());
    }
  }
  return ret;
}

int ObExprInsert::calc_result(ObObj &result,
                               const ObObj &text,
                               const ObObj &start_pos,
                               const ObObj &length,
                               const ObObj &replace_text,
                               ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc(result,
                   text,
                   start_pos,
                   length,
                   replace_text,
                   expr_ctx,
                   result_type_.get_collation_type()))) {
    OB_LOG(WARN, "fail to calc function concat", K(ret));
  }
  if (OB_SUCC(ret) && OB_LIKELY(!result.is_null())) {
    result.set_collation(result_type_);
  }
  return ret;
}

int ObExprInsert::calc(ObObj &result,
                       const ObObj &text,
                       const ObObj &start_pos,
                       const ObObj &length,
                       const ObObj &replace_text,
                       common::ObExprCtx &expr_ctx,
                       ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (text.is_null() || start_pos.is_null() || length.is_null() || replace_text.is_null()) {
    result.set_null();
  } else  if (!is_type_valid(text.get_type()) || !is_type_valid(start_pos.get_type())
              || !is_type_valid(length.get_type()) || !is_type_valid(replace_text.get_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the param is not castable", K(text), K(start_pos), K(length), K(ret));
  } else {
    TYPE_CHECK(start_pos, ObIntType);
    TYPE_CHECK(replace_text, ObVarcharType);
    TYPE_CHECK(text, ObVarcharType);
    TYPE_CHECK(length, ObIntType);
    ObString str_val = text.get_string();
    int64_t start_pos_val = start_pos.get_int();
    int64_t length_val = length.get_int();
    ObString str_rep_val = replace_text.get_string();
    ObString str_res;
    if (OB_FAIL(calc(str_res, str_val, start_pos_val, length_val,
               str_rep_val, *expr_ctx.calc_buf_, cs_type))) {
      LOG_WARN("calc insert expr failed", K(ret));
    } else {
      result.set_varchar(str_res);
    }
  }
  return ret;
}

/*
 *select insert('Quadratic',6,-1,'What');   ==>QuadrWhat
 *select insert('Quadratic',6,3,'What');    ==>QuadrWhatc
 *select insert('Quadratic',-1,-1,'');      ==>Quadratic
 *select insert('',4,0,'What');             ==>''
 *select insert('  ',0,-1,'What');          ==>'   '
 *select insert('',1,0,'What');             ==>What
 *select insert('',1,0,'What');             ==>What
 */

// support multi-bytes 10 Aug 2015
int ObExprInsert::calc(ObString &result, const ObString &text, const int64_t start_pos,
                       const int64_t expect_length_of_str, const ObString &replace_text,
                       ObIAllocator &allocator, ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  ObString origin_text = text;
  ObString rep_text = replace_text;
  if (OB_ISNULL(origin_text.ptr())) {
    origin_text.assign(NULL, 0);
  } else if (OB_UNLIKELY(origin_text.length() < 0 || replace_text.length() < 0)) {
    // empty result string
    origin_text.assign(NULL, 0);
  } else {
    int64_t start = start_pos - 1;
    int64_t expect_length = expect_length_of_str;
    int64_t text_len = ObCharset::strlen_char(cs_type, origin_text.ptr(), origin_text.length());
    int64_t rep_text_len = ObCharset::strlen_char(cs_type, rep_text.ptr(), rep_text.length());
    if (OB_UNLIKELY(start < 0 || start >= text_len)) {
      result.assign(origin_text.ptr(), origin_text.length());   //返回整个串
    } else {
      if (expect_length < 0 || start + expect_length > text_len || start > INT64_MAX - expect_length) {
        expect_length = text_len - start;
      }
      if ((text_len - expect_length + rep_text_len) > 0) {
        int64_t start_char = ObCharset::charpos(cs_type, origin_text.ptr(), origin_text.length(), start);  //原始串第一段字节结束位置
        int64_t res_length = ObCharset::charpos(cs_type,
                                                origin_text.ptr(),
                                                origin_text.length(),
                                                expect_length + start);  //原始串第二个字节开始位置
        int64_t text_len_char = ObCharset::charpos(cs_type,
                                                   origin_text.ptr(),
                                                   origin_text.length(),
                                                   text_len);  //原始串字节数
        int64_t rep_text_len_char = ObCharset::charpos(cs_type,
                                                       rep_text.ptr(),
                                                       rep_text.length(),
                                                       rep_text_len);  //替换串字节数
        char *buf = reinterpret_cast<char *>(allocator.alloc(text_len_char
                                                             - res_length
                                                             + rep_text_len_char + start_char));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          _OB_LOG(ERROR, "alloc memory failed. size=%d", static_cast<int>(text_len_char
                                                                         - res_length + rep_text_len_char + start_char));
        } else {
          if (0 == rep_text_len) {
            MEMCPY(buf, origin_text.ptr(), start_char);
            MEMCPY(buf + start_char + rep_text_len_char,
                   origin_text.ptr() + res_length,
                   text_len_char - res_length);
            result.assign(buf, static_cast<int32_t>(text_len_char - res_length
                                                  + rep_text_len_char + start_char));
          } else {
            MEMCPY(buf, origin_text.ptr(), start_char);
            MEMCPY(buf + start_char , rep_text.ptr(), rep_text_len_char);
            MEMCPY(buf + start_char + rep_text_len_char,
                   origin_text.ptr() + res_length,
                   text_len_char - res_length);
            result.assign(buf, static_cast<int32_t>(text_len_char - res_length
                                                  + rep_text_len_char + start_char));
          }
        }
      } else {
        result.assign(origin_text.ptr(), 0);
      }
    }
  }
  return ret;
}

int ObExprInsert::cg_expr(ObExprCGCtx &op_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 4) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert expr should have 4 params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_)
             || OB_ISNULL(rt_expr.args_[0])
             || OB_ISNULL(rt_expr.args_[1])
             || OB_ISNULL(rt_expr.args_[2])
             || OB_ISNULL(rt_expr.args_[3])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of insert expr is null", K(ret), K(rt_expr.args_));
  } else {
    CK(ObVarcharType == rt_expr.args_[0]->datum_meta_.type_);
    CK(ObIntType == rt_expr.args_[1]->datum_meta_.type_);
    CK(ObIntType == rt_expr.args_[2]->datum_meta_.type_);
    CK(ObVarcharType == rt_expr.args_[3]->datum_meta_.type_);
    rt_expr.eval_func_ = ObExprInsert::calc_expr_insert;
  }
  return ret;
}

int ObExprInsert::calc_expr_insert(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("int2ip expr eval param value failed", K(ret));
  } else {
    ObDatum &text = expr.locate_param_datum(ctx, 0);
    ObDatum &start_pos = expr.locate_param_datum(ctx, 1);
    ObDatum &length = expr.locate_param_datum(ctx, 2);
    ObDatum &replace_text =  expr.locate_param_datum(ctx, 3);
    if (text.is_null() || start_pos.is_null() || length.is_null() || replace_text.is_null()) {
      expr_datum.set_null();
    } else {
      ObString str_val = text.get_string();
      int64_t start_pos_val = start_pos.get_int();
      int64_t length_val = length.get_int();
      ObString str_rep_val = replace_text.get_string();
      ObString result;
      ObExprStrResAlloc res_alloc(expr, ctx);
      if (OB_FAIL(calc(result, str_val, start_pos_val, length_val,
                       str_rep_val, res_alloc, expr.datum_meta_.cs_type_))) {
        LOG_WARN("calc insert expr failed", K(ret));
      } else {
        expr_datum.set_string(result);
      }
    }
  }
  return ret;
}

}
}


    
