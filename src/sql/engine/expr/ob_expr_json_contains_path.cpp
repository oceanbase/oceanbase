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
 * This file contains implementation for json_contains_path.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_contains_path.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonContainsPath::ObExprJsonContainsPath(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_CONTAINS_PATH, N_JSON_CONTAINS_PATH, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonContainsPath::~ObExprJsonContainsPath()
{
}

int ObExprJsonContainsPath::calc_result_typeN(ObExprResType& type,
                                              ObExprResType* types_stack,
                                              int64_t param_num,
                                              ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx); 
  int ret = OB_SUCCESS;
  // set result to bool
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  
  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_CONTAINS_PATH))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
  }

  // set type of one or all flag
  if (OB_SUCC(ret)) {
    if (types_stack[1].get_type() == ObNullType) {
    } else if (ob_is_string_type(types_stack[1].get_type())) {
      if (types_stack[1].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    } else {
      ret = OB_ERR_JSON_BAD_ONE_OR_ALL_ARG;
      LOG_USER_ERROR(OB_ERR_JSON_BAD_ONE_OR_ALL_ARG);
    }
  }

  for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i++) {
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, i))) {
      LOG_WARN("wrong type for json path.", K(ret), K(types_stack[i].get_type()));
    }
  }

  return ret;
}

int ObExprJsonContainsPath::eval_json_contains_path(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    ObDatum &res)
{
  // get json doc
  int ret = OB_SUCCESS;
  ObIJsonBase *json_target = NULL;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0, json_target, is_null_result, false))) {
    LOG_WARN("get_json_doc failed", K(ret));
  } else {
    // get one_or_all flag
    bool one_flag;
    ObDatum *json_datum = NULL;
    ObExpr *json_arg = expr.args_[1];
    ObObjType val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || json_datum->is_null()) {
      is_null_result = true;
    } else {
      ObString target_str = json_datum->get_string();
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, temp_allocator, target_str, is_null_result))) {
        LOG_WARN("fail to get real data.", K(ret), K(target_str));
      } else if (0 == target_str.case_compare("one")) {
        one_flag = true;
      } else if (0 == target_str.case_compare("all")) {
        one_flag = false;
      } else {
        ret = OB_ERR_JSON_BAD_ONE_OR_ALL_ARG;
        LOG_USER_ERROR(OB_ERR_JSON_BAD_ONE_OR_ALL_ARG);
      }
    }

    bool is_contains = false;
    if ((OB_SUCC(ret)) && !is_null_result) {
      ObJsonPathCache ctx_cache(&temp_allocator);
      ObJsonPathCache* path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
      path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);

      for (int64_t i = 2; OB_SUCC(ret) && i < expr.arg_cnt_ && !is_null_result; i++) {
        ObJsonSeekResult hit;
        ObDatum *path_data = NULL;
        if (OB_FAIL(expr.args_[i]->eval(ctx, path_data))) {
          LOG_WARN("eval json path datum failed", K(ret));
        } else if (expr.args_[i]->datum_meta_.type_ == ObNullType || path_data->is_null()) {
          is_null_result = true;
        } else {
          ObString path_val = path_data->get_string();
          ObJsonPath *json_path;
          if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[i], ctx, temp_allocator, path_val, is_null_result))) {
            LOG_WARN("fail to get real data.", K(ret), K(path_val));
          } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, json_path, path_val, i, true))) {
            LOG_WARN("failed: parse path", K(path_data->get_string()), K(ret));
          } else if (OB_FAIL(json_target->seek(*json_path, json_path->path_node_cnt(),
                                               true, false, hit))) {
            LOG_WARN("json seek failed", K(path_data->get_string()), K(ret));
          }
          
          if (hit.size() == 0) {
            if (!one_flag) {
              is_contains = false;
              break;
            }
          } else {
            is_contains = true;
            if (one_flag) {
              break; 
            }
          }
        }
      }
    }

    // set result
    if (OB_FAIL(ret)) {
      LOG_WARN("json_contains_path failed", K(ret));
    } else if (is_null_result) {
      res.set_null();
    } else {
      res.set_int(static_cast<int64_t>(is_contains));
    }

  }

  return ret;
}

int ObExprJsonContainsPath::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                    const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_contains_path;
  return OB_SUCCESS;
}

}
}
