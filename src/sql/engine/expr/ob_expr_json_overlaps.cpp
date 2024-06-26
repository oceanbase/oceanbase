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
 * This file contains implementation for json_overlaps.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_overlaps.h"
#include "ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonOverlaps::ObExprJsonOverlaps(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_OVERLAPS, N_JSON_OVERLAPS, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonOverlaps::~ObExprJsonOverlaps()
{
}

int ObExprJsonOverlaps::calc_result_type2(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx); 
  int ret = OB_SUCCESS;
  bool is_strict = false;

  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);

  if (!ob_is_string_type(type1.get_type())) {
  } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type1, 1, N_JSON_OVERLAPS))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type1.get_type()));
  }

  if (OB_FAIL(ret))  {
  } else if (!ob_is_string_type(type2.get_type())) {
  } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type2, 2, N_JSON_OVERLAPS))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type2.get_type()));
  }
  
  return ret;
}

int ObExprJsonOverlaps::eval_json_overlaps(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *json_a = NULL;
  ObIJsonBase *json_b = NULL;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();

  if (!ObJsonExprHelper::is_convertible_to_json(expr.args_[0]->datum_meta_.type_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 1, N_JSON_OVERLAPS);
  } else if (!ObJsonExprHelper::is_convertible_to_json(expr.args_[1]->datum_meta_.type_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 2, N_JSON_OVERLAPS);
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0, json_a, is_null_result, false))) {
    LOG_WARN("get_json_doc failed", K(ret));
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 1, json_b, is_null_result, false))) {
    LOG_WARN("get_json_doc failed", K(ret));
  } else {
    bool is_overlaps = false;
    if (!is_null_result) {
      if (OB_FAIL(json_overlaps(json_a, json_b, &is_overlaps))) {
        LOG_WARN("json_overlaps in sub_json_targets failed", K(ret));
      }

    }

    // set result
    if (OB_FAIL(ret)) {
      LOG_WARN("json_overlaps failed", K(ret));
    } else if (is_null_result) {
      res.set_null();
    } else {
      res.set_int(static_cast<int64_t>(is_overlaps));
    }
  }

  return ret;
}

int ObExprJsonOverlaps::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_overlaps;
  return OB_SUCCESS;
}

int ObExprJsonOverlaps::json_overlaps_object(ObIJsonBase *json_a,
                                             ObIJsonBase *json_b,
                                             bool *result)
{
  int ret = OB_SUCCESS;
  if (json_b->json_type() != ObJsonNodeType::J_OBJECT) {
    *result = false;
  } else if (json_a->element_count() == 0 && json_b->element_count() == 0) {
    *result = true;
  } else {
    JsonObjectIterator iter_a = json_a->object_iterator();
    JsonObjectIterator iter_b = json_b->object_iterator();
    while (!iter_b.end() && OB_SUCC(ret)) {
      ObString key_b;
      ObIJsonBase *a_tmp = NULL;
      ObIJsonBase *b_tmp = NULL;
      if (OB_FAIL(iter_b.get_key(key_b))) {
        LOG_WARN("fail to get key from iterator", K(ret));
      } else if(OB_FAIL(iter_a.get_value(key_b, a_tmp))) {
        if (ret == OB_SEARCH_NOT_FOUND) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get object_value from wrapper", K(ret));
        }
      } else if (OB_FAIL(iter_b.get_value(b_tmp))) {
        LOG_WARN("fail to get value from iterator", K(ret));
      } else {
        int tmp_result;
        if (OB_FAIL(a_tmp->compare(*b_tmp, tmp_result))) {
          LOG_WARN("json_overlaps_object fail to compare with object type", K(ret));
        }
        if (tmp_result == 0) {
          *result = true;
          break;
        }
      }
      iter_b.next();
    }
  }

  return ret;
}

int ObExprJsonOverlaps::json_overlaps_array(ObIJsonBase *json_a,
                                            ObIJsonBase *json_b,
                                            bool *result)
{
  int ret = OB_SUCCESS;
  bool ret_tmp = true;
  
  ObIAllocator *allocator = json_a->get_allocator();
  ObJsonArray tmp_arr(allocator);
  if (json_b->json_type() != ObJsonNodeType::J_ARRAY) {
    // convert to array if needed
    ObIJsonBase *jb_node = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(allocator, json_b,
        ObJsonInType::JSON_TREE, jb_node))) {
      LOG_WARN("fail to transform to tree", K(ret), K(*json_b));
    } else {
      ObJsonNode *j_node = static_cast<ObJsonNode *>(jb_node);
      if (OB_FAIL(tmp_arr.array_append(j_node->clone(allocator)))) {
        LOG_WARN("result array append failed", K(ret), K(*j_node));
      } else {
        json_b = &tmp_arr;
      }
    }
  }

  ObSortedVector<ObIJsonBase *> vec_a;
  if (OB_SUCC(ret) && OB_FAIL(ObJsonBaseUtil::sort_array_pointer(json_a, vec_a))) {
    LOG_WARN("sort_array_pointer failed.", K(ret));
  } else {
    uint64_t b_len = json_b->element_count();
    for (uint64_t i = 0; i < b_len; i++) {
      ObIJsonBase *b_tmp = NULL;
      if (OB_FAIL(json_b->get_array_element(i, b_tmp))) {
        LOG_WARN("fail to get_array_element",K(ret), K(i));
      } else if (ObJsonBaseUtil::binary_search(vec_a, b_tmp)) {
        *result = true;
        break;
      }
    }
  }
  return ret;
}

int ObExprJsonOverlaps::json_overlaps(ObIJsonBase *json_a,
                                      ObIJsonBase *json_b,
                                      bool *result)
{
  int ret = OB_SUCCESS;

  // make sure json_a is array.
  if (json_a->json_type() != ObJsonNodeType::J_ARRAY && json_b->json_type() == ObJsonNodeType::J_ARRAY) {
    std::swap(json_a, json_b);
  }

  // make sure json_a has bigger size.
  if (json_a->json_type() == ObJsonNodeType::J_ARRAY
  && json_b->json_type() == ObJsonNodeType::J_ARRAY
  && json_a->element_count() < json_b->element_count()) {
    std::swap(json_a, json_b);
  }
  switch (json_a->json_type()) {
    case ObJsonNodeType::J_ARRAY:
      if (OB_FAIL( json_overlaps_array(json_a, json_b, result))) {
        LOG_WARN("fail to json_overlaps with ARRAY type", K(ret));
      }
      break;

    case ObJsonNodeType::J_OBJECT:
      if (OB_FAIL( json_overlaps_object(json_a, json_b, result))) {
        LOG_WARN("fail to json_overlaps with OBJECT type", K(ret));
      }
      break;
    
    default:
      int res_int = -1;
      if (OB_FAIL( json_a->compare(*json_b, res_int))) {
        LOG_WARN("fail to json_overlaps with other type", K(ret));
      }
      *result = (res_int == 0);
  }

  return ret;
}

}
}