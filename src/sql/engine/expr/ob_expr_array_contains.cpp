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
 * This file contains implementation for array.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_contains.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprArrayContains::ObExprArrayContains(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_CONTAINS, N_ARRAY_CONTAINS, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayContains::ObExprArrayContains(ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name,
                         int32_t param_num,
                         int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprArrayContains::~ObExprArrayContains()
{
}

int ObExprArrayContains::calc_result_type2(ObExprResType &type,
                                           ObExprResType &type1,
                                           ObExprResType &type2,
                                           common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObSubSchemaValue arr_meta;
  const ObSqlCollectionInfo *coll_info = NULL;
  ObExprResType *type1_ptr = &type1;
  ObExprResType *type2_ptr = &type2;
  uint16_t subschema_id;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (OB_ISNULL(type_ctx.get_raw_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw expr is null", K(ret));
  } else if (type_ctx.get_raw_expr()->get_extra() != 0) {
    // It's any operator ,param order is reversed
    ObExprResType *type_tmp = type2_ptr;
    type2_ptr = type1_ptr;
    type1_ptr = type_tmp;
  }

  if (OB_FAIL(ret)) {
  } else if (type1_ptr->is_null()) {
    type.set_null();
  } else if (!ob_is_collection_sql_type(type1_ptr->get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(type1_ptr->get_type()), ob_obj_type_str(type2_ptr->get_type()));
  } else if (type2_ptr->is_null()) {
    // do nothing
  } else if (OB_FAIL(ObArrayExprUtils::deduce_array_type(exec_ctx, *type1_ptr, *type2_ptr, subschema_id))) {
    LOG_WARN("failed to get result array type subschema id", K(ret));
  }

  if (OB_SUCC(ret) && !type1_ptr->is_null()) {
    type.set_int32();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }

  return ret;
}

#define EVAL_FUNC_ARRAY_CONTAINS(TYPE, GET_FUNC)                                                                      \
  int ObExprArrayContains::eval_array_contains_##TYPE(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)               \
  {                                                                                                                   \
    int ret = OB_SUCCESS;                                                                                             \
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);                                                                       \
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();                                            \
    uint32_t p0 = expr.extra_ == 1 ? 1 : 0;                                                                           \
    uint32_t p1 = expr.extra_ == 1 ? 0 : 1;                                                                           \
    const uint16_t meta_id = expr.args_[p0]->obj_meta_.get_subschema_id();                                            \
    ObIArrayType *arr_obj = NULL;                                                                                     \
    ObDatum *datum = NULL;                                                                                            \
    ObDatum *datum_val = NULL;                                                                                        \
    TYPE val;                                                                                                         \
    bool bret = false;                                                                                                \
    if (OB_FAIL(expr.args_[p0]->eval(ctx, datum))) {                                                                  \
      LOG_WARN("failed to eval args", K(ret));                                                                        \
    } else if (OB_FAIL(expr.args_[p1]->eval(ctx, datum_val))) {                                                       \
      LOG_WARN("failed to eval args", K(ret));                                                                        \
    } else if (datum->is_null()) {                                                                                    \
      res.set_null();                                                                                                 \
    } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, meta_id, datum->get_string(), arr_obj))) { \
      LOG_WARN("construct array obj failed", K(ret));                                                                 \
    } else if (datum_val->is_null()) {                                                                                \
      bool contains_null = arr_obj->contain_null();                                                                   \
      res.set_bool(contains_null);                                                                                    \
    } else if (FALSE_IT(val = datum_val->GET_FUNC())) {                                                               \
    } else if (OB_FAIL(ObArrayUtil::contains(*arr_obj, val, bret))) {                                                 \
      LOG_WARN("array contains failed", K(ret));                                                                      \
    } else {                                                                                                          \
      res.set_bool(bret);                                                                                             \
    }                                                                                                                 \
    return ret;                                                                                                       \
  }

EVAL_FUNC_ARRAY_CONTAINS(int64_t, get_int)
EVAL_FUNC_ARRAY_CONTAINS(float, get_float)
EVAL_FUNC_ARRAY_CONTAINS(double, get_double)
EVAL_FUNC_ARRAY_CONTAINS(ObString, get_string)

int ObExprArrayContains::eval_array_contains_array(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  uint32_t p0 = expr.extra_ == 1 ? 1 : 0;
  uint32_t p1 = expr.extra_ == 1 ? 0 : 1;
  const uint16_t l_meta_id = expr.args_[p0]->obj_meta_.get_subschema_id();
  const uint16_t r_meta_id = expr.args_[p1]->obj_meta_.get_subschema_id();
  ObIArrayType *arr_obj = NULL;
  ObIArrayType *arr_val = NULL;
  ObDatum *datum = NULL;
  ObDatum *datum_val = NULL;
  bool bret = false;
  if (OB_FAIL(expr.args_[p0]->eval(ctx, datum))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (OB_FAIL(expr.args_[p1]->eval(ctx, datum_val))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, l_meta_id, datum->get_string(), arr_obj))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (datum_val->is_null()) {
    bool contains_null = arr_obj->contain_null();
    res.set_bool(contains_null);
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, r_meta_id, datum_val->get_string(), arr_val))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(ObArrayUtil::contains(*arr_obj, *arr_val, bret))) {
    LOG_WARN("array contains failed", K(ret));
  } else {
    res.set_bool(bret);
  }
  return ret;
}

#define EVAL_FUNC_ARRAY_CONTAINS_BATCH(TYPE, GET_FUNC)                                          \
  int ObExprArrayContains::eval_array_contains_batch_##TYPE(                                    \
      const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)    \
  {                                                                                             \
    int ret = OB_SUCCESS;                                                                       \
    ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);                                \
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);                                    \
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);                                                 \
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();                      \
    uint32_t p0 = expr.extra_ == 1 ? 1 : 0;                                                     \
    uint32_t p1 = expr.extra_ == 1 ? 0 : 1;                                                     \
    const uint16_t meta_id = expr.args_[p0]->obj_meta_.get_subschema_id();                      \
    ObIArrayType *arr_obj = NULL;                                                               \
    if (OB_FAIL(expr.args_[p0]->eval_batch(ctx, skip, batch_size))) {                           \
      LOG_WARN("eval date_unit_datum failed", K(ret));                                          \
    } else if (OB_FAIL(expr.args_[p1]->eval_batch(ctx, skip, batch_size))) {                    \
      LOG_WARN("failed to eval batch result args0", K(ret));                                    \
    } else {                                                                                    \
      ObDatumVector src_array = expr.args_[p0]->locate_expr_datumvector(ctx);                   \
      ObDatumVector val_array = expr.args_[p1]->locate_expr_datumvector(ctx);                   \
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {                                \
        if (skip.at(j) || eval_flags.at(j)) {                                                   \
          continue;                                                                             \
        }                                                                                       \
        eval_flags.set(j);                                                                      \
        bool bret = false;                                                                      \
        TYPE val;                                                                               \
        if (src_array.at(j)->is_null()) {                                                       \
          res_datum.at(j)->set_null();                                                          \
        } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(                                     \
                       tmp_allocator, ctx, meta_id, src_array.at(j)->get_string(), arr_obj))) { \
          LOG_WARN("construct array obj failed", K(ret));                                       \
        } else if (val_array.at(j)->is_null()) {                                                \
          bool contains_null = arr_obj->contain_null();                                         \
          res_datum.at(j)->set_bool(contains_null);                                             \
        } else if (FALSE_IT(val = val_array.at(j)->GET_FUNC())) {                               \
        } else if (OB_FAIL(ObArrayUtil::contains(*arr_obj, val, bret))) {                       \
          LOG_WARN("array contains failed", K(ret));                                            \
        } else {                                                                                \
          res_datum.at(j)->set_bool(bret);                                                      \
        }                                                                                       \
      }                                                                                         \
    }                                                                                           \
    return ret;                                                                                 \
  }

EVAL_FUNC_ARRAY_CONTAINS_BATCH(int64_t, get_int)
EVAL_FUNC_ARRAY_CONTAINS_BATCH(float, get_float)
EVAL_FUNC_ARRAY_CONTAINS_BATCH(double, get_double)
EVAL_FUNC_ARRAY_CONTAINS_BATCH(ObString, get_string)

int ObExprArrayContains::eval_array_contains_array_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  uint32_t p0 = expr.extra_ == 1 ? 1 : 0;
  uint32_t p1 = expr.extra_ == 1 ? 0 : 1;
  const uint16_t l_meta_id = expr.args_[p0]->obj_meta_.get_subschema_id();
  const uint16_t r_meta_id = expr.args_[p1]->obj_meta_.get_subschema_id();
  ObIArrayType *arr_obj = NULL;
  ObIArrayType *arr_val = NULL;
  if (OB_FAIL(expr.args_[p0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval date_unit_datum failed", K(ret));
  } else if (OB_FAIL(expr.args_[p1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("failed to eval batch result args0", K(ret));
  } else {
    ObDatumVector src_array = expr.args_[p0]->locate_expr_datumvector(ctx);
    ObDatumVector val_array = expr.args_[p1]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      bool bret = false;
      if (src_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL(
              ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, l_meta_id, src_array.at(j)->get_string(), arr_obj))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (val_array.at(j)->is_null()) {
        bool contains_null = arr_obj->contain_null();
        res_datum.at(j)->set_bool(contains_null);
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(
                     tmp_allocator, ctx, r_meta_id, val_array.at(j)->get_string(), arr_val))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (OB_FAIL(ObArrayUtil::contains(*arr_obj, *arr_val, bret))) {
        LOG_WARN("array contains failed", K(ret));
      } else {
        res_datum.at(j)->set_bool(bret);
      }
    }
  }
  return ret;
}

#define EVAL_FUNC_ARRAY_CONTAINS_VECTOR(TYPE, GET_FUNC)                                                   \
  int ObExprArrayContains::eval_array_contains_vector_##TYPE(                                             \
      const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)                \
  {                                                                                                       \
    int ret = OB_SUCCESS;                                                                                 \
    uint32_t p0 = expr.extra_ == 1 ? 1 : 0;                                                               \
    uint32_t p1 = expr.extra_ == 1 ? 0 : 1;                                                               \
    if (OB_FAIL(expr.args_[p0]->eval_vector(ctx, skip, bound)) ||                                         \
        OB_FAIL(expr.args_[p1]->eval_vector(ctx, skip, bound))) {                                         \
      LOG_WARN("fail to eval params", K(ret));                                                            \
    } else {                                                                                              \
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);                                                         \
      common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();                              \
      ObIVector *left_vec = expr.args_[p0]->get_vector(ctx);                                              \
      VectorFormat left_format = left_vec->get_format();                                                  \
      ObIVector *right_vec = expr.args_[p1]->get_vector(ctx);                                             \
      const uint16_t meta_id = expr.args_[p0]->obj_meta_.get_subschema_id();                              \
      ObIVector *res_vec = expr.get_vector(ctx);                                                          \
      ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);                                            \
      ObIArrayType *arr_obj = NULL;                                                                       \
      TYPE val;                                                                                           \
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {                       \
        bool is_null_res = false;                                                                         \
        if (skip.at(idx) || eval_flags.at(idx)) {                                                         \
          continue;                                                                                       \
        } else if (left_vec->is_null(idx)) {                                                              \
          is_null_res = true;                                                                             \
        } else if (left_format == VEC_UNIFORM || left_format == VEC_UNIFORM_CONST) {                      \
          ObString left = left_vec->get_string(idx);                                                      \
          if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, meta_id, left, arr_obj))) { \
            LOG_WARN("construct array obj failed", K(ret));                                               \
          }                                                                                               \
        } else if (OB_FAIL(ObNestedVectorFunc::construct_attr_param(                                      \
                       tmp_allocator, ctx, *expr.args_[p0], meta_id, idx, arr_obj))) {                    \
          LOG_WARN("construct array obj failed", K(ret));                                                 \
        }                                                                                                 \
        bool bret = false;                                                                                \
        if (OB_FAIL(ret)) {                                                                               \
        } else if (is_null_res) {                                                                         \
          res_vec->set_null(idx);                                                                         \
          eval_flags.set(idx);                                                                            \
        } else if (right_vec->is_null(idx)) {                                                             \
          bool contains_null = arr_obj->contain_null();                                                   \
          res_vec->set_bool(idx, contains_null);                                                          \
          eval_flags.set(idx);                                                                            \
        } else if (FALSE_IT(val = right_vec->GET_FUNC(idx))) {                                            \
        } else if (OB_FAIL(ObArrayUtil::contains(*arr_obj, val, bret))) {                                 \
          LOG_WARN("array contains failed", K(ret));                                                      \
        } else {                                                                                          \
          res_vec->set_bool(idx, bret);                                                                   \
          eval_flags.set(idx);                                                                            \
        }                                                                                                 \
      }                                                                                                   \
    }                                                                                                     \
    return ret;                                                                                           \
  }

EVAL_FUNC_ARRAY_CONTAINS_VECTOR(int64_t, get_int)
EVAL_FUNC_ARRAY_CONTAINS_VECTOR(float, get_float)
EVAL_FUNC_ARRAY_CONTAINS_VECTOR(double, get_double)
EVAL_FUNC_ARRAY_CONTAINS_VECTOR(ObString, get_string)

int ObExprArrayContains::eval_array_contains_array_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                          const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  uint32_t p0 = expr.extra_ == 1 ? 1 : 0;
  uint32_t p1 = expr.extra_ == 1 ? 0 : 1;
  if (OB_FAIL(expr.args_[p0]->eval_vector(ctx, skip, bound)) || OB_FAIL(expr.args_[p1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval params", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    ObIVector *left_vec = expr.args_[p0]->get_vector(ctx);
    VectorFormat left_format = left_vec->get_format();
    ObIVector *right_vec = expr.args_[p1]->get_vector(ctx);
    VectorFormat right_format = right_vec->get_format();
    const uint16_t left_meta_id = expr.args_[p0]->obj_meta_.get_subschema_id();
    const uint16_t right_meta_id = expr.args_[p1]->obj_meta_.get_subschema_id();
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObIArrayType *arr_obj = NULL;
    ObIArrayType *arr_val = NULL;
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_null_res = false;
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (left_vec->is_null(idx)) {
        is_null_res = true;
      } else if (left_format == VEC_UNIFORM || left_format == VEC_UNIFORM_CONST) {
        ObString left = left_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, left_meta_id, left, arr_obj))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      } else if (OB_FAIL(ObNestedVectorFunc::construct_attr_param(
                     tmp_allocator, ctx, *expr.args_[p0], left_meta_id, idx, arr_obj))) {
        LOG_WARN("construct array obj failed", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
      } else if (right_vec->is_null(idx)) {
        bool contains_null = arr_obj->contain_null();
        res_vec->set_bool(idx, contains_null);
        eval_flags.set(idx);
      } else {
        if (right_format == VEC_UNIFORM || right_format == VEC_UNIFORM_CONST) {
          ObString right = right_vec->get_string(idx);
          if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, right_meta_id, right, arr_val))) {
            LOG_WARN("construct array obj failed", K(ret));
          }
        } else if (OB_FAIL(ObNestedVectorFunc::construct_attr_param(
                      tmp_allocator, ctx, *expr.args_[p1], right_meta_id, idx, arr_val))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
        bool bret = false;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObArrayUtil::contains(*arr_obj, *arr_val, bret))) {
          LOG_WARN("array contains failed", K(ret));
        } else {
          res_vec->set_bool(idx, bret);
          eval_flags.set(idx);
        }
      }
    }
  }

  return ret;
}

int ObExprArrayContains::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  if (rt_expr.arg_cnt_ != 2 || OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of children is not 2 or children is null", K(ret), K(rt_expr.arg_cnt_),
                                                              K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]));
  } else {
    rt_expr.eval_func_ = NULL;
    rt_expr.may_not_need_raw_check_ = false;
    rt_expr.extra_ = raw_expr.get_extra();
    uint32_t p1 = rt_expr.extra_ == 1 ? 0 : 1;
    uint32_t p0 = rt_expr.extra_ == 1 ? 1 : 0;
    const ObObjType right_type = rt_expr.args_[p1]->datum_meta_.type_;
    ObObjTypeClass right_tc = ob_obj_type_class(right_type);
    if (right_tc == ObNullTC) {
      // use array element type
      ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
      const uint16_t sub_id = rt_expr.args_[p0]->obj_meta_.get_subschema_id();
      ObObjType elem_type;
      uint32_t unused;
      bool is_vec = false;
      if (OB_FAIL(ObArrayExprUtils::get_array_element_type(exec_ctx, sub_id, elem_type, unused, is_vec))) {
        LOG_WARN("failed to get collection elem type", K(ret), K(sub_id));
      } else {
        right_tc = ob_obj_type_class(elem_type);
      }
    }
    if OB_SUCC(ret) {
      switch (right_tc) {
        case ObUIntTC:
        case ObIntTC:
          rt_expr.eval_func_ = eval_array_contains_int64_t;
          rt_expr.eval_batch_func_ = eval_array_contains_batch_int64_t;
          rt_expr.eval_vector_func_ = eval_array_contains_vector_int64_t;
          break;
        case ObFloatTC:
          rt_expr.eval_func_ = eval_array_contains_float;
          rt_expr.eval_batch_func_ = eval_array_contains_batch_float;
          rt_expr.eval_vector_func_ = eval_array_contains_vector_float;
          break;
        case ObDoubleTC:
          rt_expr.eval_func_ = eval_array_contains_double;
          rt_expr.eval_batch_func_ = eval_array_contains_batch_double;
          rt_expr.eval_vector_func_ = eval_array_contains_vector_double;
          break;
        case ObStringTC:
          rt_expr.eval_func_ = eval_array_contains_ObString;
          rt_expr.eval_batch_func_ = eval_array_contains_batch_ObString;
          rt_expr.eval_vector_func_ = eval_array_contains_vector_ObString;
          break;
        case ObNullTC:
        case ObCollectionSQLTC:
          rt_expr.eval_func_ = eval_array_contains_array;
          rt_expr.eval_batch_func_ = eval_array_contains_array_batch;
          rt_expr.eval_vector_func_ = eval_array_contains_array_vector;
          break;
        default :
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("invalid type", K(ret), K(right_type), K(right_tc));
      }
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
