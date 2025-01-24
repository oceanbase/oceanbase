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

#include "sql/engine/expr/ob_expr_split_part.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprSplitPart::ObExprSplitPart(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SPLIT_PART, N_SPLIT_PART, TWO_OR_THREE, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}
ObExprSplitPart::~ObExprSplitPart()
{
}
int ObExprSplitPart::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (param_num != 3) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the param number of split part should be 3", K(ret), K(param_num));
  } else if (lib::is_mysql_mode()) {
    types[0].set_calc_type(ObVarcharType);
    types[1].set_calc_type(ObVarcharType);
    types[2].set_calc_type(ObIntType);
    type.set_varchar();
    if (OB_FAIL(aggregate_charsets_for_string_result(type, types, 1, type_ctx))) {
      LOG_WARN("aggregate_charsets_for_string_result failed", K(ret));
    } else {
      for (int64_t i = 0; i < param_num; i++) {
        types[i].set_calc_collation_type(type.get_collation_type());
        types[i].set_calc_collation_level(type.get_collation_level());
      }
    }
  } else {
    ObSEArray<ObExprResType*, 2, ObNullAllocator> params;
    OZ (params.push_back(&types[0]));
    OZ (aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), params, type));
    OZ (params.push_back(&types[1]));
    OZ (deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), type, params));
    types[2].set_calc_type(ObIntType);
  }
  return ret;
}

int ObExprSplitPart::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_split_part_expr;
  return ret;
}

int ObExprSplitPart::calc_split_part_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                        ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *sub = NULL;
  ObDatum *ori = NULL;
  ObDatum *pos = NULL;
  if (OB_UNLIKELY(3 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx,ori)) ||
             OB_FAIL(expr.args_[1]->eval(ctx,sub)) ||
             OB_FAIL(expr.args_[2]->eval(ctx,pos)) ){
    LOG_WARN("eval arg failed", K(ret));
  } else if (ori->is_null() || sub->is_null() || pos->is_null()) {
    res.set_null();
  } else {
    int64_t pos_int = pos->get_int();
    const ObString &ori_str = ori->get_string();
    const ObString &sub_str = sub->get_string();
    ObString empty_str;
    ObCollationType calc_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (sub_str.empty()) {
      if (pos_int == 1 || pos_int == 0 || pos_int == -1) {
        res.set_string(ori->get_string());
      } else {
        res.set_string(empty_str);
      }
    } else if (ori_str.empty()) {
      res.set_string(empty_str);
    } else {
      int64_t total_splits = 0;
      int64_t cur_pos = 1;
      ObSEArray<int64_t, 32> splits_pos;
      // the array is [padding_start, ...split_positions, padding_end]
      int64_t idx = 0;
      OZ(splits_pos.push_back(1 - sub_str.length())); // padding_start
      while (OB_SUCC(ret) && cur_pos <= ori_str.length()){
        idx = ObCharset::instrb(calc_cs_type, ori_str.ptr() + cur_pos - 1, ori_str.length() - cur_pos + 1,
                                        sub_str.ptr(), sub_str.length()) + 1;
        if (idx == 0) {
          break;
        } else {
          total_splits++;
          cur_pos = cur_pos + idx;
          OZ(splits_pos.push_back(cur_pos - 1));
        }
      }
      OZ(splits_pos.push_back(ori_str.length() + 1)); // padding_end

      if (OB_SUCC(ret)) {
        if (total_splits == 0) {
          res.set_string(empty_str);
        } else {
          // handle pos_int
          if (0 == pos_int) {
            pos_int = 1;
          } else if (pos_int < 0){
            pos_int = pos_int + total_splits + 2;
          }
          if (total_splits + 1 < pos_int || 1 > pos_int){
            // out of range
            res.set_string(empty_str);
          } else {
            int64_t start_pos = 0;
            int64_t end_pos = 0;
            // the return string is [start_pos, end_pos)
            start_pos = splits_pos.at(pos_int - 1) - 1 + sub_str.length();
            end_pos = splits_pos.at(pos_int);
            if (start_pos >= end_pos) {
              res.set_string(empty_str);
            } else {
              res.set_string(ori_str.ptr() + start_pos, end_pos - start_pos - 1);
            }
          }
        }
      }
    }
  }
  return ret;
}
DEF_SET_LOCAL_SESSION_VARS(ObExprSplitPart, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}
}