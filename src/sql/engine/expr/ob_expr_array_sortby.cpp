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
 * This file contains implementation for array_sortby.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_sortby.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
#include "lib/udt/ob_array_utils.h"
#include "sql/engine/expr/ob_array_cast.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/utility/ob_sort.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprArraySortby::ObExprArraySortby(ObIAllocator &alloc)
    : ObExprArrayMapCommon(alloc, T_FUNC_SYS_ARRAY_SORTBY, N_ARRAY_SORTBY, MORE_THAN_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArraySortby::ObExprArraySortby(ObIAllocator &alloc,
                                 ObExprOperatorType type,
                                 const char *name,
                                 int32_t param_num,
                                 int32_t dimension)
    : ObExprArrayMapCommon(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprArraySortby::~ObExprArraySortby()
{
}

int ObExprArraySortby::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObObjType lambda_type = types_stack[0].get_type();
  bool is_null_res = false;

  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (!ob_is_array_supported_type(lambda_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid data type", K(ret), K(lambda_type));
  } else if (ob_is_collection_sql_type(lambda_type)) {
    // will support when array compare is available
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support nested array", K(ret), K(lambda_type));
  } else if (lambda_type== ObDecimalIntType || lambda_type == ObNumberType) {
    // decimalint isn't supported in array, so cast to supported type
    if (types_stack[0].get_scale() != 0) {
      types_stack[0].set_calc_type(ObDoubleType);
    } else {
      types_stack[0].set_calc_type(ObDoubleType);
    }
  }
  // check the array params
  for (int64_t i = 1; i < param_num && OB_SUCC(ret) && !is_null_res; i++) {
    if (types_stack[i].is_null()) {
      is_null_res = true;
    } else if (!ob_is_collection_sql_type(types_stack[i].get_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid data type", K(ret), K(types_stack[i].get_type()));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    type.set_null();
  } else {
    // result type same as the first array
    type.set_collection(types_stack[1].get_subschema_id());
    type.set_accuracy(types_stack[1].get_accuracy());
  }

  return ret;
}

int ObExprArraySortby::eval_array_sortby(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprArrayMapInfo *info = static_cast<ObExprArrayMapInfo *>(expr.extra_info_);
  ObIArrayType *src_arrs[expr.arg_cnt_ - 1];
  uint32_t arr_dim = 0;
  uint32_t *sort_idx = NULL;
  const uint16_t res_subschema_id = expr.obj_meta_.get_subschema_id();
  ObIArrayType *lambda_arr = NULL;
  ObIArrayType *res_arr = NULL;
  bool is_null_res = false;

  if (ob_is_null(expr.obj_meta_.get_type())) {
    is_null_res = true;
  } else if (res_subschema_id != expr.args_[1]->obj_meta_.get_subschema_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subschema id is not equal", K(ret), K(res_subschema_id), K(expr.args_[1]->obj_meta_.get_subschema_id()));
  } else if (OB_UNLIKELY(OB_ISNULL(info))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr extra info is null", K(ret));
  } else if (OB_FAIL(eval_src_arrays(expr, ctx, tmp_allocator, src_arrs, arr_dim, is_null_res))) {
    LOG_WARN("failed to eval src arrays", K(ret));
  } else if (is_null_res) {
    // do nothing
  } else if (OB_FAIL(eval_lambda_array(ctx, tmp_allocator, info, src_arrs, arr_dim, expr.args_[0], lambda_arr))) {
    LOG_WARN("failed to eval lambda array", K(ret));
  } else if (OB_FAIL(lambda_arr->init())) {
    LOG_WARN("array init failed", K(ret));
  } else if (OB_FAIL(index_sort(tmp_allocator, lambda_arr, sort_idx))) {
    LOG_WARN("failed to sort array index", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, res_subschema_id, res_arr, false))) {
    LOG_WARN("construct child array obj failed", K(ret));
  } else if (OB_FAIL(fill_array_by_index(src_arrs[0], sort_idx, res_arr))) {
    LOG_WARN("failed to fill array by index", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
  } else {
    ObString res_str;
    if (OB_FAIL(ObArrayExprUtils::set_array_res(
            res_arr, res_arr->get_raw_binary_len(), expr, ctx, res_str))) {
      LOG_WARN("get array binary string failed", K(ret));
    } else {
      res.set_string(res_str);
    }
  }
  return ret;
}

int ObExprArraySortby::index_sort(common::ObArenaAllocator &allocator, ObIArrayType *src_arr, uint32_t *&sort_idx)
{
  int ret = OB_SUCCESS;
  struct arrSortCmp{
    arrSortCmp(ObIArrayType *arr) :
    arr_(arr) {}
    bool operator()(int32_t idx_l, int32_t idx_r) {
      return arr_->sort_cmp(idx_l, idx_r);
    }
    ObIArrayType *arr_;
  };
  arrSortCmp cmp(src_arr);
  uint32_t arr_size = src_arr->size();

  if (OB_ISNULL(sort_idx = static_cast<uint32_t *>(allocator.alloc(sizeof(uint32_t) * arr_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else {
    for (uint32_t i = 0; i < arr_size; ++i) {
      sort_idx[i] = i;
    }
    lib::ob_sort(sort_idx, sort_idx + arr_size, cmp);
  }

  return ret;
}

int ObExprArraySortby::fill_array_by_index(ObIArrayType *src_arr, uint32_t *sort_idx, ObIArrayType *res_arr)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < src_arr->size() && OB_SUCC(ret); i++) {
    if (OB_FAIL(res_arr->insert_from(*src_arr, sort_idx[i]))) {
      LOG_WARN("failed to insert element to result array", K(ret), K(i));
    }
  } // end for
  return ret;
}

int ObExprArraySortby::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  uint16_t lambda_subschema_id;
  ObIExprExtraInfo *extra_info = nullptr;

  if (OB_FAIL(get_lambda_subschema_id(exec_ctx, raw_expr, lambda_subschema_id))) {
    LOG_WARN("failed to get lambda subschema id", K(ret));
  } else if (OB_FAIL(construct_extra_info(expr_cg_ctx, raw_expr, rt_expr, extra_info))) {
    LOG_WARN("failed to construct extra info", K(ret));
  } else {
    static_cast<ObExprArrayMapInfo *>(extra_info)->lambda_subschema_id_ = lambda_subschema_id;
    rt_expr.extra_info_ = extra_info;
    rt_expr.eval_func_ = eval_array_sortby;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
