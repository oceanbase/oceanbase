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
#include <string.h>
#include "sql/engine/expr/ob_expr_to_blob.h"
#include "sql/session/ob_sql_session_info.h"
#include "objit/common/ob_item_type.h"
#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprToBlob::ObExprToBlob(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_TO_BLOB, N_TO_BLOB, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprToBlob::~ObExprToBlob()
{
}

int ObExprToBlob::calc_result_type1(ObExprResType &type,
                                    ObExprResType &text,
                                    common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  if (ob_is_null(text.get_type())
      || ob_is_blob(text.get_type(), text.get_collation_type())
      || ob_is_raw(text.get_type())
      || ob_is_string_tc(text.get_type())) {
    type.set_blob();
    type.set_collation_type(CS_TYPE_BINARY);
    if (ob_is_string_tc(text.get_type())) {
      ObLength length = text.get_length();
      if (LS_CHAR == text.get_length_semantics()) {
        length *= ObCharset::get_charset(text.get_collation_type())->mbmaxlen;
      }
      length = (length / 2) + (length % 2);
      type.set_length(length);
    } else { // input is blob or raw type
      type.set_length(text.get_length());
    }
    if (ob_is_string_tc(text.get_type())) {
      text.set_calc_type(common::ObRawType);
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("wrong type of argument in function to_blob", K(ret), K(text));
  }

  return ret;
}

int ObExprToBlob::eval_to_blob(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt or arg res type", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (arg->is_null()) {
    res.set_null();
  } else if (ob_is_blob(expr.args_[0]->datum_meta_.get_type(), expr.args_[0]->datum_meta_.cs_type_)) {
    res = *arg; // blob to blob
  } else {
    ObString raw_string = arg->get_string();
    int64_t res_len = raw_string.length();
    if (!ob_is_text_tc(expr.args_[0]->datum_meta_.get_type())) { // non-lob to blob
      ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res);
      if (OB_FAIL(str_result.init(res_len))) {
        LOG_WARN("init lob result failed");
      } else if (OB_FAIL(str_result.append(raw_string.ptr(), raw_string.length()))) {
        LOG_WARN("append lob result failed");
      } else {
        str_result.set_result();
      }
    } else { // clob to blob
      ObLobLocatorV2 lob(raw_string, expr.args_[0]->obj_meta_.has_lob_header());
      if (OB_FAIL(lob.get_lob_data_byte_len(res_len))) {
        LOG_WARN("get lob data byte length failed", K(ret), K(lob));
      } else {
        ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res);
        if (OB_FAIL(str_result.init(res_len))) {
          LOG_WARN("init lob result failed");
        } else {
          int64_t off = 0;
          ObString v_str;
          ObEvalCtx::TempAllocGuard alloc_guard(ctx);
          ObIAllocator &calc_alloc = alloc_guard.get_allocator();
          ObDatumMeta input_meta = expr.args_[0]->datum_meta_;
          bool has_lob_header = expr.args_[0]->obj_meta_.has_lob_header();
          ObTextStringIter input_iter(input_meta.type_, input_meta.cs_type_, arg->get_string(), has_lob_header);
          ObTextStringIterState state;
          ObString src_block_data;
          if (OB_FAIL(input_iter.init(0, NULL, &calc_alloc))) {
            LOG_WARN("init input_iter failed ", K(ret), K(input_iter));
          }
          while (OB_SUCC(ret)
                  && (state = input_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
            if (OB_FAIL(str_result.append(src_block_data))) {
              LOG_WARN("str_result append failed", K(ret), K(src_block_data));
            } else {
              off += src_block_data.length();
            }
          }
          if (OB_FAIL(ret)) {
          } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
            ret = (input_iter.get_inner_ret() != OB_SUCCESS) ?
                  input_iter.get_inner_ret() : OB_INVALID_DATA;
            LOG_WARN("iter state invalid", K(ret), K(state), K(input_iter));
          } else {
            str_result.set_result();
            OB_ASSERT(off == res_len);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprToBlob::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_to_blob;
  return ret;
}

} // end of sql
} // end of oceanbase
