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
 * This file contains implementation for json_contains.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_contains.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "lib/json_type/ob_json_bin_view.h"
#include "lib/utility/ob_sort.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonContains::ObExprJsonContains(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_CONTAINS, N_JSON_CONTAINS, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonContains::~ObExprJsonContains()
{
}

int ObExprJsonContains::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx); 
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3)) {
    ret = OB_ERR_PARAM_SIZE;
    ObString func_name_(N_JSON_CONTAINS);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    // set the result type to bool
    type.set_int32();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);

    for (int64_t i = 0; OB_SUCC(ret) && i < 2; i++) {
      ObObjType in_type = types_stack[i].get_type();
      if (!ob_is_string_type(in_type)) {
      } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, i, N_JSON_CONTAINS))) {
        LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[i].get_type()));
      }
    }

    // set type for json_path
    if (OB_SUCC(ret) && param_num == 3) {
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, 2))) {
        LOG_WARN("wrong type for json path.", K(ret), K(types_stack[2].get_type()));
      }
    }
  }
  return ret;
}

int ObExprJsonContains::eval_json_contains(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObJsonWrapper target_wrapper;
  ObJsonWrapper candidate_wrapper;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, ret, ctx, "json_contains");
  const bool cand_is_const = expr.args_[1]->is_static_const_expr();
  const bool need_param_ctx = cand_is_const || (expr.arg_cnt_ == 3);
  ObJsonParamCacheCtx* param_ctx = need_param_ctx ? ObJsonExprHelper::get_param_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_) : NULL;
  if (OB_FAIL(ObJsonExprHelper::get_json_doc_wrapper(expr, ctx, temp_allocator, 0,
                                                     target_wrapper, is_null_result,
                                                     false, false))) {
    LOG_WARN("target get_json_doc_wrapper failed", K(ret));
  } else if (!ObJsonExprHelper::is_convertible_to_json(expr.args_[1]->datum_meta_.type_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 2, N_JSON_CONTAINS);
  } else if (is_null_result) {
    // skip
  } else if (OB_FAIL(ObJsonExprHelper::get_json_candidate_wrapper(expr, ctx, temp_allocator,
                                                                  1, param_ctx,
                                                                  false /*doc semantics*/,
                                                                  candidate_wrapper, is_null_result))) {
    LOG_WARN("candidate get_json_candidate_wrapper failed", K(ret));
  }


  bool is_contains = false;
  if (!is_null_result && OB_SUCC(ret)) {
    if (expr.arg_cnt_ == 3) {
      ObJsonPathCache ctx_cache(&temp_allocator);
      ObJsonPathCache* path_cache = (param_ctx != NULL) ? param_ctx->get_path_cache() : &ctx_cache;

      ObDatum *path_data = NULL;
      if (OB_FAIL(temp_allocator.eval_arg(expr.args_[2], ctx, path_data))) {
        LOG_WARN("eval json path datum failed", K(ret));
      } else if (expr.args_[2]->datum_meta_.type_ == ObNullType || path_data->is_null()) {
        is_null_result = true;
      } else {
        bool is_const = expr.args_[2]->is_static_const_expr();
        ObSEArray<ObJsonWrapper, 1> hits;
        ObString path_val = path_data->get_string();
        ObJsonPath *json_path = NULL;
        if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[2], ctx, temp_allocator, path_val, is_null_result))) {
          LOG_WARN("fail to get real data.", K(ret), K(path_val));
        } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(temp_allocator, path_cache, json_path, path_val, 2, false, is_const))) {
          LOG_WARN("json path parse failed", K(path_data->get_string()), K(ret));
        } else if (OB_FAIL(target_wrapper.seek(*json_path, temp_allocator, hits, true, false))) {
          LOG_WARN("json seek failed", K(ret));
        } else if (hits.empty()) {
          is_null_result = true;
        } else if (OB_FAIL(json_contains(hits.at(0), candidate_wrapper, is_contains))) {
          LOG_WARN("json contains after seek failed", K(ret));
        }
      }
    } else {
      if (OB_FAIL(json_contains(target_wrapper, candidate_wrapper, is_contains))) {
        LOG_WARN("json contains failed", K(ret));
      }
    }
  }

  // set result
  if (OB_FAIL(ret)) {
    LOG_WARN("json_contains failed", K(ret));
  } else if (is_null_result) {
    res.set_null();
  } else {
    res.set_int(static_cast<int64_t>(is_contains));
  }

  return ret;
}

int ObExprJsonContains::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_contains;
  return OB_SUCCESS;
}

int ObExprJsonContains::json_contains_object(const ObJsonWrapper &target,
                                             const ObJsonWrapper &candidate,
                                             bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  if (candidate.json_type() != ObJsonNodeType::J_OBJECT) {
    result = false;
  } else if (candidate.element_count() == 0) {
    result = true;
  } else {
    uint32_t t_cnt = target.element_count();
    uint32_t c_cnt = candidate.element_count();
    uint32_t t_i = 0;
    uint32_t c_i = 0;
    while (t_i < t_cnt && c_i < c_cnt && OB_SUCC(ret)) {
      // find the same key
      ObString cand_key;
      ObJsonWrapper cand_val;
      ObString target_key;
      ObJsonWrapper target_val;
      if (OB_FAIL(candidate.get_key(c_i, cand_key))) {
        LOG_WARN("fail to get candidate key", K(ret), K(c_i));
      } else {
        while (t_i < t_cnt && OB_SUCC(ret)) {
          if (OB_FAIL(target.get_key(t_i, target_key))) {
            LOG_WARN("fail to get target key_value", K(ret), K(t_i));
          } else if (cand_key == target_key) {
            break;
          } else {
            t_i++;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (t_i >= t_cnt) {
          result = false;
          break;
        } else {
          // compare value
          if (OB_FAIL(target.get_value(t_i, target_val))) {
            LOG_WARN("fail to get target value", K(ret), K(t_i));
          } else if (OB_FAIL(candidate.get_value(c_i, cand_val))) {
            LOG_WARN("fail to get candidate value", K(ret), K(c_i));
          } else {
            if (OB_FAIL(json_contains(target_val, cand_val, result))) {
              LOG_WARN("recursive contains failed", K(ret));
            } else if (!result) {
              break;
            }
            c_i++;
          }
        }
      }
    }
  }

  return ret;
}

int ObExprJsonContains::json_contains_array(const ObJsonWrapper &target,
                                            const ObJsonWrapper &candidate,
                                            bool &result)
{
  int ret = OB_SUCCESS;
  bool ret_tmp = true;
  result = false;
  // materialize target and candidate into ObJsonWrapper arrays
  uint32_t target_cnt = target.element_count();
  ObSEArray<ObJsonWrapper, 32> t_arr;
  if (OB_FAIL(t_arr.reserve(target_cnt))) {
    LOG_WARN("reserve target array failed", K(ret), K(target_cnt));
  }
  for (uint32_t i = 0; OB_SUCC(ret) && i < target_cnt; ++i) {
    ObJsonWrapper elem;
    if (OB_FAIL(target.element(i, elem))) {
      LOG_WARN("get target element failed", K(ret), K(i));
    } else if (OB_FAIL(t_arr.push_back(elem))) {
      LOG_WARN("push_back target element failed", K(ret), K(i));
    }
  }

  ObSEArray<ObJsonWrapper, 32> c_arr;
  if (OB_SUCC(ret) && OB_FAIL(c_arr.reserve(candidate.element_count()))) {
    LOG_WARN("reserve candidate array failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (candidate.json_type() == ObJsonNodeType::J_ARRAY) {
      if (candidate.element_count() == 0) {
        result = true;
      } else {
        uint32_t cand_cnt = candidate.element_count();
        for (uint32_t i = 0; OB_SUCC(ret) && i < cand_cnt; ++i) {
          ObJsonWrapper elem;
          if (OB_FAIL(candidate.element(i, elem))) {
            LOG_WARN("get candidate element failed", K(ret), K(i));
          } else if (OB_FAIL(c_arr.push_back(elem))) {
            LOG_WARN("push_back candidate element failed", K(ret), K(i));
          }
        }
      }
    } else {
      if (OB_FAIL(c_arr.push_back(candidate))) {
        LOG_WARN("push_back candidate failed", K(ret));
      }
    }
  }
  // sort the array index
  if (OB_SUCC(ret) && !result) {
    ObJsonWrapperLess less(&ret);
    if (OB_FALSE_IT(lib::ob_sort(t_arr.begin(), t_arr.end(), less))) {
    } else if (OB_FAIL(ret)) {
      LOG_WARN("compare failed during sort", K(ret));
    } else if (OB_FALSE_IT(lib::ob_sort(c_arr.begin(), c_arr.end(), less))) {
    } else if (OB_FAIL(ret)) {
      LOG_WARN("compare failed during sort", K(ret));
    }
  }

  if (OB_SUCC(ret) && !result) {

    uint64_t t_i = 0;
    for (uint64_t c_i = 0; c_i < c_arr.count() && OB_SUCC(ret); c_i++) {
      ObJsonNodeType candt = c_arr.at(c_i).json_type();
      if (candt == ObJsonNodeType::J_ARRAY) {
        while (t_i < t_arr.count()) {
          if (t_arr.at(t_i).json_type() < candt) {
            t_i++;
          } else {
            break;
          }
        }

        bool found = false;
        uint64_t tmp = t_i;
        while (tmp < t_arr.count() && OB_SUCC(ret)) {
          if (t_arr.at(tmp).json_type() == ObJsonNodeType::J_ARRAY) {
            if (OB_FAIL(json_contains(t_arr.at(tmp), c_arr.at(c_i), found))) {
              LOG_WARN("recursive contains failed", K(ret));
            } else if (found) {
              break;
            } else {
              tmp++;
            }
          } else {
            break;
          }
        }

        if (!found) {
          ret_tmp = false;
          break;
        }
      } else {
        bool found = false;
        uint64_t tmp = t_i;

        while (tmp < t_arr.count() && OB_SUCC(ret)) {
          if (t_arr.at(tmp).json_type() == ObJsonNodeType::J_ARRAY ||
              t_arr.at(tmp).json_type() == ObJsonNodeType::J_OBJECT) {
            if (OB_FAIL(json_contains(t_arr.at(tmp), c_arr.at(c_i), found))) {
              LOG_WARN("recursive contains failed", K(ret));
            } else if (found) {
              break;
            }
          } else {
            int tmp_result = 0;
            if (OB_FAIL(ObJsonWrapper::compare(t_arr.at(tmp), c_arr.at(c_i), tmp_result))) {
              LOG_WARN("compare failed", K(ret));
            } else if (tmp_result == 0) {
              found = true;
              break;
            }
          }
          tmp++;
        }
        ret_tmp = (t_i == t_arr.count() || !found) ? false : true;
        if (!ret_tmp) {
          break;
        }
      }
    }
  }

  result = (OB_SUCCESS == ret) ? ret_tmp : false;
  return ret;
}
int ObExprJsonContains::json_contains(const ObJsonWrapper &target,
                                      const ObJsonWrapper &candidate,
                                      bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  switch (target.json_type()) {
    case ObJsonNodeType::J_ARRAY:
      if (OB_FAIL(json_contains_array(target, candidate, result))) {
        LOG_WARN("fail to json_contains with ARRAY type", K(ret));
      }
      break;
    case ObJsonNodeType::J_OBJECT:
      if (OB_FAIL(json_contains_object(target, candidate, result))) {
        LOG_WARN("fail to json_contains with OBJECT type", K(ret));
      }
      break;
    default: {
      int ret_tmp = 0;
      if (OB_FAIL(ObJsonWrapper::compare(target, candidate, ret_tmp))) {
        LOG_WARN("compare failed", K(ret));
      } else {
        result = (ret_tmp == 0);
      }
      break;
    }
  }
  return ret;
}

}
}