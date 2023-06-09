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

#define USING_LOG_PREFIX  SQL_ENG
#include "sql/engine/expr/ob_expr_nlssort.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/charset/ob_charset.h"
#include "common/object/ob_obj_type.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_extra_info_factory.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprNLSSort::ObExprNLSSort(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_NLSSORT, N_NLSSORT, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprNLSSort::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types,
                                     int64_t param_num,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  auto params = make_const_carray(&types[0]);
  ObExprResType temp_res_type;
  int32_t length = 0;

  if (OB_ISNULL(type_ctx.get_session()) || OB_UNLIKELY(param_num > 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret));
  }
  OZ (aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), params, temp_res_type));
  OZ (deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), temp_res_type, params));

  if (OB_SUCC(ret)) {
    int32_t num_bytes = types[0].get_calc_length() * 
              (LS_BYTE == types[0].get_calc_accuracy().get_length_semantics() ? 1 : 4);
    length = num_bytes;
    if (param_num > 1 && types[1].is_literal() && types[1].get_param().is_string_type()) {
      ObString param = types[1].get_param().get_string();
      param.split_on('=');
      ObString collation_name = param.trim();
      if (collation_name.prefix_match("UCA0900_")) {
         const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_ZH_0900_AS_CS);
         length = cs->coll->strnxfrmlen(cs, num_bytes);
      } else if (collation_name.prefix_match("SCHINESE_")) {
         const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_GB18030_CHINESE_CI);
         length = cs->coll->strnxfrmlen(cs, num_bytes);
      }
    }
    LOG_DEBUG("nlssort deduce length", K(num_bytes));
  }

  type.set_raw();
  type.set_length(min(length, OB_MAX_ORACLE_VARCHAR_LENGTH));

  if (OB_SUCC(ret) && param_num > 1) {
    types[1].set_calc_collation_ascii_compatible();
  }

  LOG_DEBUG("nlssort deduce length", K(type.get_length()), K(types[0]));
  return ret;
}

int ObExprNLSSort::convert_to_coll_code(ObEvalCtx &ctx,
                                        const ObCollationType &from_type,
                                        const ObString &from_str,
                                        const ObCollationType &to_type,
                                        ObString &to_str)
{
  int ret = OB_SUCCESS;
  if (to_type == CS_TYPE_GB18030_CHINESE_CS ||
      to_type == CS_TYPE_GB18030_2022_PINYIN_CS ||
      to_type == CS_TYPE_GB18030_2022_RADICAL_CS ||
      to_type == CS_TYPE_GB18030_2022_STROKE_CS) {
    char *conv_buf = NULL;
    const int32_t MostBytes = 4; //most 4 bytes
    size_t conv_buf_len = from_str.length() * MostBytes;
    uint32_t result_len = 0;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_ISNULL(conv_buf = (char *)alloc_guard.get_allocator().alloc(conv_buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(from_type,
                                                  from_str.ptr(),
                                                  from_str.length(),
                                                  to_type,
                                                  conv_buf,
                                                  conv_buf_len,
                                                  result_len,
                                                  false,
                                                  false,
                                                  0xFFFD /*�*/))) {
      LOG_WARN("charset convert failed", K(ret));
    } else {
      to_str.assign_ptr(conv_buf, result_len);
    }
    LOG_DEBUG("charset convert", KPHEX(from_str.ptr(), from_str.length()), K(from_type), K(to_type), KPHEX(to_str.ptr(), to_str.length()), K(result_len));
  } else { 
    to_str.assign_ptr(from_str.ptr(), from_str.length());
  }
  return ret;
}

int ObExprNLSSort::eval_nlssort_inner(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      ObDatum &expr_datum,
                                      const ObCollationType &coll_type,
                                      const ObCollationType &arg0_coll_type,
                                      const ObObjType &arg0_obj_type,
                                      ObString input_str)
{
  int ret = OB_SUCCESS;
  ObString out;
  const ObCharsetInfo *cs = ObCharset::get_charset(coll_type);
  if (OB_ISNULL(cs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cs", K(ret), K(coll_type));
  } else if (((ob_is_nchar(arg0_obj_type)) || (ob_is_char(arg0_obj_type, arg0_coll_type)))
            && (OB_FAIL(ObCharsetUtils::remove_char_endspace(input_str,
                                        ObCharset::charset_type_by_coll(arg0_coll_type))))) {
    LOG_WARN("remove char endspace failed", K(ret));
  } else if (OB_FAIL(convert_to_coll_code(ctx, arg0_coll_type, input_str, coll_type, out))) {
    LOG_WARN("convert to coll code failed", K(ret));
  } else {
    LOG_DEBUG("check coll type", K(coll_type), K(arg0_coll_type), K(expr),
        K(arg0_obj_type), K(out.length()));
    size_t buf_len = cs->coll->strnxfrmlen(cs, out.length());
    char *buf = NULL;
    size_t result_len = 0;
    if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(ret), K(buf_len), K(input_str));
    } else {
      bool is_valid_unicode_tmp = 1;
      result_len = cs->coll->strnxfrm(cs,
                                      reinterpret_cast<uchar *>(buf),
                                      buf_len,
                                      buf_len,
                                      reinterpret_cast<const uchar *>(out.ptr()),
                                      out.length(),
                                      0,
                                      &is_valid_unicode_tmp);
      expr_datum.set_string(buf, result_len);
    }
  }
  return ret;
}

int ObExprNLSSort::eval_nlssort(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;

  ObDatum *input = NULL;
  ObDatum *nls_param = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, input))) {
    LOG_WARN("fail to eval", K(ret), KPC(expr.args_[0]));
  } else if (expr.arg_cnt_ > 1 && OB_FAIL(expr.args_[1]->eval(ctx, nls_param))) {
    LOG_WARN("fail to eval", K(ret), KPC(expr.args_[1]));
  } else if (input->is_null()
             || (OB_NOT_NULL(nls_param) && nls_param->is_null())) {
    expr_datum.set_null();
  } else {
    ObCollationType coll_type = CS_TYPE_INVALID;
    ObCollationType arg0_coll_type = expr.args_[0]->datum_meta_.cs_type_;
    ObObjType arg0_obj_type = expr.args_[0]->datum_meta_.type_;
    uint64_t rt_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
    ObExprNLSSORTParseCtx *nlssort_ctx = static_cast<ObExprNLSSORTParseCtx *>
        (ctx.exec_ctx_.get_expr_op_ctx(rt_ctx_id));

    if (OB_NOT_NULL(nlssort_ctx)) {
      coll_type = nlssort_ctx->coll_type_;
    } else {
      coll_type = expr.args_[0]->datum_meta_.cs_type_;
      if (OB_NOT_NULL(nls_param)) {
        ObString nls_param_str = nls_param->get_string();
        ObString param_k = nls_param_str.split_on('=').trim();
        ObString param_v = nls_param_str.trim();
        coll_type = ObCharset::get_coll_type_by_nlssort_param(
              ObCharset::charset_type_by_coll(arg0_coll_type), param_v);
        if (OB_UNLIKELY(!ObCharset::is_valid_collation(coll_type))
            || OB_UNLIKELY(0 != param_k.case_compare("NLS_SORT"))) {
          ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
          LOG_WARN("invalid collation", K(ret), K(param_k), K(param_v), K(coll_type));
        } else {
          if (!!(expr.args_[1]->is_static_const_)) {
            //cache coll type into op ctx
            if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(rt_ctx_id, nlssort_ctx))) {
              LOG_WARN("failed to create operator ctx", K(ret));
            } else {
              nlssort_ctx->coll_type_ = coll_type;
              LOG_DEBUG("cache coll type to op ctx", K(coll_type));
            }
          }
        }
      } else {
        coll_type = arg0_coll_type;
      }
    }
    if (OB_SUCC(ret)) {
      if (!ob_is_text_tc(arg0_obj_type)) {
        ObString input_str = input->get_string();
        ret = eval_nlssort_inner(expr, ctx, expr_datum, coll_type, arg0_coll_type, arg0_obj_type, input_str);
      } else { // text tc
        ObString input_str = input->get_string();
        // result type is raw, not a lob and length is set to OB_MAX_ORACLE_VARCHAR_LENGTH at most
        // so just use prefix for calc
        ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
        common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
        if (OB_FAIL(ObTextStringHelper::read_prefix_string_data(ctx,
                                                                *input,
                                                                expr.args_[0]->datum_meta_,
                                                                expr.args_[0]->obj_meta_.has_lob_header(),
                                                                &temp_allocator,
                                                                input_str,
                                                                OB_MAX_ORACLE_VARCHAR_LENGTH))) {
          LOG_WARN("failed to get string data", K(ret), K(expr.args_[0]->datum_meta_));
        } else {
          ret = eval_nlssort_inner(expr, ctx, expr_datum, coll_type, arg0_coll_type, arg0_obj_type, input_str);
        }
      }
    }
  }
  return ret;
}

int ObExprNLSSort::cg_expr(ObExprCGCtx &expr_cg_ctx,
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_nlssort;
  return ret;
}



}
}
