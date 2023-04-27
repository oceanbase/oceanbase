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
 * This file contains implementation for json_member_of.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_func_helper.h"
#include "ob_expr_json_member_of.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonMemberOf::ObExprJsonMemberOf(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_MEMBER_OF, N_JSON_MEMBER_OF, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonMemberOf::~ObExprJsonMemberOf()
{
}

int ObExprJsonMemberOf::calc_result_type2(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx); 
  UNUSED(type2);
  int ret = OB_SUCCESS;

  // set result to bool
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  
  // set json_val
  if (type1.get_type() == ObNullType) {
  } else if(ob_is_string_type(type1.get_type())) {
    if (type1.get_charset_type() != CHARSET_UTF8MB4) {
      type1.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  }
  return ret;
}

int ObExprJsonMemberOf::check_json_member_of_array(const ObIJsonBase *json_a,
                                                   const ObIJsonBase *json_b,
                                                   bool &is_member_of)
{
  int ret = OB_SUCCESS;
  is_member_of = false;

  if (OB_ISNULL(json_a) || OB_ISNULL(json_b)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret), KP(json_a), KP(json_b));
  } else {
    int cmp_res = 0;
    uint64_t b_len = json_b->element_count();
    for (uint64_t i = 0; i < b_len && OB_SUCC(ret) && !is_member_of; i++) {
      ObIJsonBase *tmp = NULL;
      if (OB_FAIL(json_b->get_array_element(i, tmp))) {
        LOG_WARN("fail to get array element", K(ret), K(i), K(*json_b));
      } else if (OB_FAIL(json_a->compare(*tmp, cmp_res))) {
        LOG_WARN("fail to compare json", K(ret), K(i), K(*json_a), K((*tmp)));
      } else if (cmp_res == 0) {
        is_member_of = true;
      }
    }
  }

  return ret;
}

int ObExprJsonMemberOf::eval_json_member_of(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObIJsonBase *json_a = NULL;
  ObIJsonBase *json_b = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  bool is_null_result = (expr.args_[0]->datum_meta_.type_ == ObNullType);
  if (!is_null_result) {
    ObDatum *json_datum = NULL;
    ObExpr *json_arg = expr.args_[0];
    ObObjType type2 = expr.args_[1]->datum_meta_.type_;
    if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (json_datum->is_null()) {
      is_null_result = true; 
    } else if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &temp_allocator, 0, json_a))) {
      LOG_WARN("get_json_value failed", K(ret));
    } else if (!ObJsonExprHelper::is_convertible_to_json(type2)) {
      ret = OB_ERR_INVALID_TYPE_FOR_JSON;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 2, N_JSON_MEMBER_OF);
    } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx,
                                                      temp_allocator, 1,
                                                      json_b, is_null_result))) {
      LOG_WARN("get_json_doc failed", K(ret));
    }
  }

  bool is_member_of = false;
  if (!is_null_result && OB_SUCC(ret)) {
    // make sura w_b is J_ARRAY type
    if (json_b->json_type() != ObJsonNodeType::J_ARRAY) {
      int result = -1;
      if (OB_FAIL(json_b->compare(*json_a, result))) {
        LOG_WARN("json compare failed", K(ret));
      } else {
        is_member_of = (result == 0);
      }
    } else if (OB_FAIL(check_json_member_of_array(json_a, json_b, is_member_of))) {
      LOG_WARN("check_json_member_of_array failed", K(ret));
    }
  }

  // set result
  if (OB_FAIL(ret)) {
    LOG_WARN("json_member_of failed", K(ret));
  } else if (is_null_result) {
    res.set_null();
  } else {
    res.set_int(static_cast<int64_t>(is_member_of));
  }

  return ret;
}

int ObExprJsonMemberOf::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_member_of;
  return OB_SUCCESS;
}

}
}