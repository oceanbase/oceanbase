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
  ObIJsonBase *json_target = NULL;
  ObIJsonBase *json_candidate = NULL;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0,
                                             json_target, is_null_result))) {
    LOG_WARN("get_json_doc failed", K(ret));
  } else if (!ObJsonExprHelper::is_convertible_to_json(expr.args_[1]->datum_meta_.type_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 2, N_JSON_CONTAINS);
  } else if (!is_null_result && OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 1,
                                                                       json_candidate, is_null_result))) {
    LOG_WARN("get_json_doc failed", K(ret));
  } else {}


  bool is_contains = false;
  if (!is_null_result && OB_SUCC(ret)) {
    if (expr.arg_cnt_ == 3) {
      ObJsonPathCache ctx_cache(&temp_allocator);
      ObJsonPathCache* path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
      path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);

      ObDatum *path_data = NULL;
      if (OB_FAIL(expr.args_[2]->eval(ctx, path_data))) {
        LOG_WARN("eval json path datum failed", K(ret));
      } else if (expr.args_[2]->datum_meta_.type_ == ObNullType || path_data->is_null()) {
        is_null_result = true;
      } else {
        ObJsonSeekResult sub_json_targets;
        ObString path_val = path_data->get_string();
        ObJsonPath *json_path;
        if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[2], ctx, temp_allocator, path_val, is_null_result))) {
          LOG_WARN("fail to get real data.", K(ret), K(path_val));
        } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, json_path, path_val, 2, false))) {
          LOG_WARN("json path parse failed", K(path_data->get_string()), K(ret));
        } else if (OB_FAIL(json_target->seek(*json_path, json_path->path_node_cnt(), true, false, sub_json_targets))) {
          LOG_WARN("json seek failed", K(path_data->get_string()), K(ret));
        } else {
          // use the first of results as candidate
          if (sub_json_targets.size() > 0) {
            if (OB_FAIL(json_contains(sub_json_targets[0], json_candidate, &is_contains))) {
              LOG_WARN("json contain in sub_json_targets failed", K(ret));
            }
          } else {
            is_null_result = true;
          }
        }
      }
    } else {
      if (OB_FAIL(json_contains(json_target, json_candidate, &is_contains))) {
        LOG_WARN("json contain in sub_json_targets failed", K(ret));
      } else {
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

int ObExprJsonContains::json_contains_object(ObIJsonBase* json_target, ObIJsonBase* json_candidate, bool *result)
{
  int ret = OB_SUCCESS;
  if (json_candidate->json_type() != ObJsonNodeType::J_OBJECT) {
    *result = false;
  } else if (json_target->element_count() == 0 && json_candidate->element_count() == 0) {
    *result = true;
  } else {
    JsonObjectIterator iter_t = json_target->object_iterator();
    JsonObjectIterator iter_c = json_candidate->object_iterator();
    while (!iter_t.end() && !iter_c.end() && OB_SUCC(ret)) {
      // find the same key
      ObString key1;
      if (OB_FAIL(iter_c.get_key(key1))) {
        LOG_WARN("fail to get key from iterator", K(ret));
      } else {
        while (!iter_t.end() && OB_SUCC(ret)) {
          ObString key2;
          if (OB_FAIL(iter_t.get_key(key2))) {
            LOG_WARN("fail to get key from iterator", K(ret));
          } else if (key1 == key2){
            break;
          } else {
            iter_t.next();
          }
        }
        if (iter_t.end()) {
          *result = false;
          break;
        }
        // compare value
        ObIJsonBase* j_t = NULL;
        ObIJsonBase* j_c = NULL;
        if (OB_FAIL(iter_t.get_value(j_t))) {
          LOG_WARN("fail to get value form iterator", K(ret));
        } else if (OB_FAIL(iter_c.get_value(j_c))) {
          LOG_WARN("fail to get value form iterator", K(ret));
        } else {
          if (OB_FAIL(json_contains(j_t, j_c, result))) {
            LOG_WARN("fail to compare j_t to j_c", K(ret));
          } else if (!*result) {
            break;
          }
          iter_c.next();
        }
      }
    }
  }

  return ret;
}

int ObExprJsonContains::json_contains_array(ObIJsonBase* json_target,
                                            ObIJsonBase* json_candidate,
                                            bool *result)
{
  int ret = OB_SUCCESS;
  bool ret_tmp = true;
  
  ObIAllocator *allocator = json_target->get_allocator();
  ObJsonArray tmp_arr(allocator);
  if (json_candidate->json_type() != ObJsonNodeType::J_ARRAY) {
    // convert to array
    ObIJsonBase *jb_node = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(allocator, json_candidate,
        ObJsonInType::JSON_TREE, jb_node))) {
      LOG_WARN("fail to transform to tree", K(ret), K(*json_candidate));
    } else {
      ObJsonNode *j_node = static_cast<ObJsonNode *>(jb_node);
      if (OB_FAIL(tmp_arr.array_append(j_node->clone(allocator)))) {
        LOG_WARN("result array append failed", K(ret), K(*j_node));
      } else {
        json_candidate = &tmp_arr;
      }
    }
  }

  // sort the array index
  ObSortedVector<ObIJsonBase *> t;
  ObSortedVector<ObIJsonBase *> c;

  if (OB_FAIL(ret) ||
      OB_FAIL(ObJsonBaseUtil::sort_array_pointer(json_target, t)) ||
      OB_FAIL(ObJsonBaseUtil::sort_array_pointer(json_candidate, c))) {
    LOG_WARN("sort_array_pointer failed.", K(ret));
  } else {
    uint64_t t_i = 0;
    for (uint64_t c_i = 0; c_i < c.size() && OB_SUCC(ret); c_i++) {
      ObJsonNodeType candt = c[c_i]->json_type();
      if (candt == ObJsonNodeType::J_ARRAY) {
        while (t_i < t.size()) {
          if (t[t_i]->json_type() < candt) {
            t_i++;
          } else {
            break;
          }
        }

        bool found = false;
        uint64_t tmp = t_i;
        while (tmp < t.size() && OB_SUCC(ret)) {
          if (t[tmp]->json_type() == ObJsonNodeType::J_ARRAY) {
            if (OB_FAIL(json_contains(t[tmp], c[c_i], &ret_tmp))) {
              LOG_WARN("json contain in sub_json_targets failed", K(ret));
            } else {
              if (ret_tmp) {
                found = true;
                break;
              }
              tmp++;
            }
          }  else {
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

        while (tmp < t.size() && OB_SUCC(ret)) {
          if (t[tmp]->json_type() == ObJsonNodeType::J_ARRAY ||
              t[tmp]->json_type() == ObJsonNodeType::J_OBJECT) {
            if (OB_FAIL(json_contains(t[tmp], c[c_i], &ret_tmp))) {
              LOG_WARN("json contain in sub_json_targets failed", K(ret));
            } else {
              if (ret_tmp) {
                found = true;
                break;
              }
            }
          } else {
            int tmp_result = -1;
            if (OB_FAIL(t[tmp]->compare(*c[c_i], tmp_result))) {
              LOG_WARN("json compare in sub_json_targets failed", K(ret));
            } else {
              if (tmp_result == 0) {
                found = true;
                break;
              }
            }
          }
          tmp++;
        }

        ret_tmp = (t_i == t.size() || !found) ? false : true;
        if (!ret_tmp) {
          break;
        }
      }
    }
  }

  *result = (OB_SUCCESS == ret) ? ret_tmp : false;
  return ret;
}

int ObExprJsonContains::json_contains(ObIJsonBase* json_target, ObIJsonBase* json_candidate, bool *result)
{
  int ret = OB_SUCCESS;
  switch (json_target->json_type()) {
    case ObJsonNodeType::J_ARRAY:
      if (OB_FAIL( json_contains_array(json_target, json_candidate, result))) {
        LOG_WARN("fail to json_contains with ARRAY type", K(ret));
      }
      break;

    case ObJsonNodeType::J_OBJECT:
      if (OB_FAIL( json_contains_object(json_target, json_candidate, result))) {
        LOG_WARN("fail to json_contains with OBJECT type", K(ret));
      }
      break;
    
    default:
      int res_int = -1;
      if (OB_FAIL( json_target->compare(*json_candidate, res_int))) {
        LOG_WARN("fail to json_contains with other type", K(ret));
      }
      *result = (res_int == 0);
  }

  return ret;
}

}
}