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
 * This file contains implementation for JSON_EQUAL.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_equal.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonEqual::ObExprJsonEqual(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_EQUAL, N_JSON_EQUAL, TWO_OR_THREE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonEqual::~ObExprJsonEqual()
{
}

int ObExprJsonEqual::calc_result_typeN(ObExprResType& type,
                                       ObExprResType* types_stack,
                                       int64_t param_num,
                                       ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3) || OB_UNLIKELY(param_num < 2)) {
    ret = OB_ERR_PARAM_SIZE;
    ObString name(N_JSON_EQUAL);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else {
    // set the result type to bool
    type.set_int32();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
    // set type for json_doc and json_candidate
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_EQUAL))) {
      LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0]));
    } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 1, N_JSON_EQUAL))) {
      LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[1]));
    }

    if (OB_SUCC(ret) && param_num == 3) {
      if (types_stack[2].get_type() != ObIntType && types_stack[2].get_type() != ObNumberType) {
        ret = OB_ERR_INVALID_TYPE_FOR_JSON;
        LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 3, N_JSON_EQUAL);
      }
    }
  }

  return ret;
}

bool ObExprJsonEqual::is_json_scalar(ObIJsonBase *ptr)
{
  bool ret_bool = false;
  if (!OB_ISNULL(ptr)) {
    ObJsonNodeType json_type = ptr->json_type();
    if (json_type < ObJsonNodeType::J_MAX_TYPE
        && (json_type != ObJsonNodeType::J_OBJECT && json_type != ObJsonNodeType::J_ARRAY)) {
      ret_bool = true;
    }
  }
  return ret_bool;
}

int ObExprJsonEqual::eval_json_equal(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObIJsonBase *json_target = NULL;
  ObIJsonBase *json_candidate = NULL;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  int compare_res = -1;
  bool is_equal = false;
  uint8_t option_on_error = 0;
  // json数据解析出错或非标量，此时根据on error参数返回结果
  bool is_cover_by_error = false;
  bool both_json = false;

  // 检查是否均为 json 类型，如果均为json类型，标量相比不报错
  // 只要有一个参数不是json类型，标量相比就会报错
  if (expr.args_[0]->datum_meta_.type_ == ObJsonType
    || expr.args_[1]->datum_meta_.type_ == ObJsonType) {
      both_json = true;
  }

  ObExpr *json_arg_l = expr.args_[0];
  ObObjType val_type_l = json_arg_l->datum_meta_.type_;
  ObExpr *json_arg_r = expr.args_[1];
  ObObjType val_type_r = json_arg_l->datum_meta_.type_;
  if (val_type_l == ObRawType || val_type_r == ObRawType) {
    ret = OB_ERR_JSON_SYNTAX_ERROR;
    is_cover_by_error = true;
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0,
                                             json_target, is_null_result))) {
    if (ret == OB_ERR_JSON_SYNTAX_ERROR) is_cover_by_error = true;
    LOG_WARN("get_json_doc failed", K(ret));
  } else if (!is_null_result && OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 1,
                                                                       json_candidate, is_null_result))) {
    if (ret == OB_ERR_JSON_SYNTAX_ERROR) is_cover_by_error = true;
    LOG_WARN("get_json_doc failed", K(ret));
  } else if(!is_null_result && !both_json && (is_json_scalar(json_target)
                                              || is_json_scalar(json_candidate))) {
    ret = OB_ERR_JSON_SYNTAX_ERROR;
    is_cover_by_error = true;
    LOG_USER_ERROR(OB_ERR_JSON_SYNTAX_ERROR);
  } else if (!is_null_result && OB_FAIL(json_target->compare(*json_candidate, compare_res))){
    LOG_WARN("fail to compare between json doc", K(ret));
  }

  // 记录当前ret，防止取on error参数时覆盖之前的错误码
  INIT_SUCC(tmp_ret);
  if (!is_null_result && expr.arg_cnt_ == 3) {
    ObExpr *json_arg = expr.args_[2];
    ObObjType val_type = json_arg->datum_meta_.type_;
    ObDatum *json_datum = NULL;
    tmp_ret = ret;
    ret = OB_SUCCESS;

    val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObIntType) {
      int64_t option = json_datum->get_int();
      if (option < OB_JSON_FALSE_ON_ERROR ||
          option > OB_JSON_ERROR_ON_ERROR) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input option type error", K(option), K(ret));
      } else {
        option_on_error = static_cast<uint8_t>(option);
      }
    } else if (val_type == ObNumberType) {
      const uint32_t *option = json_datum->get_number_digits();
      if (*option < OB_JSON_FALSE_ON_ERROR ||
          *option > OB_JSON_ERROR_ON_ERROR) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input option type error", K(option), K(ret));
      } else {
        option_on_error = static_cast<uint8_t>(*option);
      }
    }
  } else if (expr.arg_cnt_ == 2) {
    tmp_ret = ret;
  }

  // set result
  if (OB_FAIL(ret) && !is_cover_by_error) {
  // 解析on error 参数出错，正常返回错误码
  } else if (OB_FAIL(tmp_ret) && is_cover_by_error) {
    // json数据出错
    if (option_on_error == OB_JSON_FALSE_ON_ERROR) {
      res.set_int(0);
      ret = OB_SUCCESS;
    } else if (option_on_error == OB_JSON_TRUE_ON_ERROR) {
      res.set_int(1);
      ret = OB_SUCCESS;
    } else if (tmp_ret == OB_ERR_INVALID_JSON_TEXT_IN_PARAM) {
      ret = OB_ERR_JSON_SYNTAX_ERROR;
    }
  } else if (is_null_result) {
    res.set_null();
  } else {
    is_equal = (compare_res == 0);
    res.set_int(static_cast<int64_t>(is_equal));
  }

  return ret;
}

int ObExprJsonEqual::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_equal;
  return OB_SUCCESS;
}

}
}