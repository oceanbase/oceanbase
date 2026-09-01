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
#include "lib/utility/ob_sort.h"

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
  INIT_SUCC(ret);
  ObJsonWrapper wrapper_a;
  ObJsonWrapper wrapper_b;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, ret, ctx, "json_overlaps");
  const ObObjType args_a_type = expr.args_[0]->datum_meta_.type_;
  const ObObjType args_b_type = expr.args_[1]->datum_meta_.type_;
  if (!ObJsonExprHelper::is_convertible_to_json(args_a_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 1, N_JSON_OVERLAPS);
  } else if (!ObJsonExprHelper::is_convertible_to_json(args_b_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 2, N_JSON_OVERLAPS);
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc_wrapper(expr, ctx, temp_allocator, 0,
                                                            wrapper_a, is_null_result,
                                                            false, false))) {
    LOG_WARN("get wrapper a failed", K(ret));
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc_wrapper(expr, ctx, temp_allocator, 1,
                                                            wrapper_b, is_null_result,
                                                            false, false))) {
    LOG_WARN("get wrapper b failed", K(ret));
  } else {
    bool is_overlaps = false;
    if (!is_null_result) {
      if (OB_FAIL(json_overlaps(wrapper_a, wrapper_b, is_overlaps))) {
        LOG_WARN("json_overlaps failed", K(ret));
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

int ObExprJsonOverlaps::json_overlaps_object(const ObJsonWrapper &wrapper_a,
                                             const ObJsonWrapper &wrapper_b,
                                             bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  if (wrapper_a.json_type() != ObJsonNodeType::J_OBJECT
      || wrapper_b.json_type() != ObJsonNodeType::J_OBJECT) {
    result = false;
  } else if (wrapper_a.element_count() == 0 && wrapper_b.element_count() == 0) {
    result = true;
  } else {
    uint32_t cnt_b = wrapper_b.element_count();
    for (uint32_t i = 0; OB_SUCC(ret) && !result && i < cnt_b; ++i) {
      ObString key_b;
      ObJsonWrapper val_b;
      ObJsonWrapper val_a;
      if (OB_FAIL(wrapper_b.get_key(i, key_b))) {
        LOG_WARN("get_key_value b failed", K(ret), K(i));
      } else if (OB_FAIL(wrapper_a.lookup(key_b, val_a))) {
        if (ret == OB_SEARCH_NOT_FOUND) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get object_value from wrapper", K(ret), K(key_b));
        }
      } else if (OB_FAIL(wrapper_b.get_value(i, val_b))) {
        LOG_WARN("fail to get value", K(ret));
      } else {
        int cmp = 0;
        if (OB_FAIL(ObJsonWrapper::compare(val_a, val_b, cmp))) {
          LOG_WARN("compare failed", K(ret));
        } else if (cmp == 0) {
          result = true;
        }
      }
    }
  }

  return ret;
}

int ObExprJsonOverlaps::json_overlaps_array(const ObJsonWrapper &wrapper_a,
                                            const ObJsonWrapper &wrapper_b,
                                            bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  uint32_t cnt_a = wrapper_a.element_count();
  ObSEArray<ObJsonWrapper, 32> a_arr;
  if (OB_FAIL(a_arr.reserve(cnt_a))) {
    LOG_WARN("reserve array failed", K(ret), K(cnt_a));
  }
  for (uint32_t i = 0; OB_SUCC(ret) && i < cnt_a; ++i) {
    ObJsonWrapper elem;
    if (OB_FAIL(wrapper_a.element(i, elem))) {
      LOG_WARN("get element a failed", K(ret), K(i));
    } else if (OB_FAIL(a_arr.push_back(elem))) {
      LOG_WARN("push_back element a failed", K(ret), K(i));
    }
  }

  if (OB_SUCC(ret)) {
    ObJsonWrapperLess less(&ret);
    if (OB_FALSE_IT(lib::ob_sort(a_arr.begin(), a_arr.end(), less))) {
    } else if (OB_FAIL(ret)) {
      LOG_WARN("compare failed during sort", K(ret));
    } else if (wrapper_b.json_type() == ObJsonNodeType::J_ARRAY) {
      uint32_t cnt_b = wrapper_b.element_count();
      for (uint32_t i = 0; OB_SUCC(ret) && !result && i < cnt_b; ++i) {
        ObJsonWrapper elem_b;
        if (OB_FAIL(wrapper_b.element(i, elem_b))) {
          LOG_WARN("get element b failed", K(ret), K(i));
        } else if (OB_FAIL(ObJsonWrapper::binary_search(a_arr, elem_b, result))) {
          LOG_WARN("binary search failed", K(ret), K(i));
        }
      }
    } else {
      if (OB_FAIL(ObJsonWrapper::binary_search(a_arr, wrapper_b, result))) {
        LOG_WARN("binary search failed", K(ret));
      }
    }
  }
  return ret;
}

int ObExprJsonOverlaps::json_overlaps(const ObJsonWrapper &wrapper_a,
                                      const ObJsonWrapper &wrapper_b,
                                      bool &result)
{
  int ret = OB_SUCCESS;
  result = false;

  const ObJsonWrapper *pa = &wrapper_a;
  const ObJsonWrapper *pb = &wrapper_b;

  // make sure pa is array.
  if (pa->json_type() != ObJsonNodeType::J_ARRAY && pb->json_type() == ObJsonNodeType::J_ARRAY) {
    std::swap(pa, pb);
  }
  // make sure pa has bigger size
  if (pa->json_type() == ObJsonNodeType::J_ARRAY
      && pb->json_type() == ObJsonNodeType::J_ARRAY
      && pa->element_count() < pb->element_count()) {
    std::swap(pa, pb);
  }
  if (pa->json_type() == ObJsonNodeType::J_ARRAY) {
    if (OB_FAIL(json_overlaps_array(*pa, *pb, result))) {
      LOG_WARN("fail to json_overlaps with ARRAY type", K(ret));
    }
  } else if (pa->json_type() == ObJsonNodeType::J_OBJECT
             || pb->json_type() == ObJsonNodeType::J_OBJECT) {
    if (OB_FAIL(json_overlaps_object(*pa, *pb, result))) {
      LOG_WARN("fail to json_overlaps with OBJECT type", K(ret));
    }
  } else {
    int cmp = 0;
    if (OB_FAIL(ObJsonWrapper::compare(*pa, *pb, cmp))) {
      LOG_WARN("compare failed", K(ret));
    } else {
      result = (cmp == 0);
    }
  }

  return ret;
}

}
}