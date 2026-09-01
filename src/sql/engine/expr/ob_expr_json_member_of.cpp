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

int ObExprJsonMemberOf::check_json_member_of_array(const ObJsonWrapper &candidate,
                                                   const ObJsonWrapper &array,
                                                   bool &is_member_of)
{
  int ret = OB_SUCCESS;
  is_member_of = false;
  int cmp_res = 0;
  uint32_t cnt = array.element_count();
  for (uint32_t i = 0; i < cnt && OB_SUCC(ret) && !is_member_of; i++) {
    ObJsonWrapper elem;
    if (OB_FAIL(array.element(i, elem))) {
      LOG_WARN("fail to get array element", K(ret), K(i));
    } else if (OB_FAIL(ObJsonWrapper::compare(candidate, elem, cmp_res))) {
      LOG_WARN("fail to compare json", K(ret));
    } else if (cmp_res == 0) {
      is_member_of = true;
    }
  }

  return ret;
}

int ObExprJsonMemberOf::eval_json_member_of(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObJsonWrapper cand_wrapper;
  ObJsonWrapper target_wrapper;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, ret, ctx, "json_member_of");
  ObObjType cand_type = expr.args_[0]->datum_meta_.type_;
  ObObjType target_type = expr.args_[1]->datum_meta_.type_;
  bool is_null_result = (cand_type == ObNullType);
  if (is_null_result) {
    // skip
  } else if (OB_FAIL(ObJsonExprHelper::get_json_candidate_wrapper(expr, ctx, temp_allocator,
                                                                  0, NULL, true /*val semantics*/,
                                                                  cand_wrapper, is_null_result))) {
    LOG_WARN("candidate get_json_candidate_wrapper failed", K(ret));
  } else if (is_null_result) {
    // skip
  } else if (!ObJsonExprHelper::is_convertible_to_json(target_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 2, N_JSON_MEMBER_OF);
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc_wrapper(expr, ctx, temp_allocator, 1,
                                                            target_wrapper, is_null_result,
                                                            false, false))) {
    LOG_WARN("get target wrapper failed", K(ret));
  }

  bool is_member_of = false;
  if (!is_null_result && OB_SUCC(ret)) {
    // make sure target is J_ARRAY type
    if (target_wrapper.json_type() != ObJsonNodeType::J_ARRAY) {
      int result = -1;
      if (OB_FAIL(ObJsonWrapper::compare(target_wrapper, cand_wrapper, result))) {
        LOG_WARN("json compare failed", K(ret));
      } else {
        is_member_of = (result == 0);
      }
    } else if (OB_FAIL(check_json_member_of_array(cand_wrapper, target_wrapper, is_member_of))) {
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