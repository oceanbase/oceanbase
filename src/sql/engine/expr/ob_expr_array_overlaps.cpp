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
 * This file contains implementation for array_overlaps.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_overlaps.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
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

ObExprArrayOverlaps::ObExprArrayOverlaps(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_OVERLAPS, N_ARRAY_OVERLAPS, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayOverlaps::ObExprArrayOverlaps(ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name,
                         int32_t param_num, 
                         int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension) 
{
}

ObExprArrayOverlaps::~ObExprArrayOverlaps()
{
}

int ObExprArrayOverlaps::calc_result_type2(ObExprResType &type,
                                           ObExprResType &type1,
                                           ObExprResType &type2,
                                           common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObCollectionTypeBase *coll_type1 = NULL;
  ObCollectionTypeBase *coll_type2 = NULL;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if ((!ob_is_collection_sql_type(type1.get_type()) && !type1.is_null())
             || (!ob_is_collection_sql_type(type2.get_type()) && !type2.is_null())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(type1.get_type()), ob_obj_type_str(type2.get_type()));
  } else if (type1.is_null() || type2.is_null()) {
    // do nothing
  } else if (OB_FAIL(ObArrayExprUtils::get_coll_type_by_subschema_id(exec_ctx, type1.get_subschema_id(), coll_type1))) {
    LOG_WARN("failed to get array type by subschema id", K(ret), K(type1.get_subschema_id()));
  } else if (coll_type1->type_id_ != ObNestedType::OB_ARRAY_TYPE && coll_type1->type_id_ != ObNestedType::OB_VECTOR_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid collection type", K(ret), K(coll_type1->type_id_));
  } else if (OB_FAIL(ObArrayExprUtils::get_coll_type_by_subschema_id(exec_ctx, type2.get_subschema_id(), coll_type2))) {
    LOG_WARN("failed to get array type by subschema id", K(ret), K(type2.get_subschema_id()));
  } else if (coll_type2->type_id_ != ObNestedType::OB_ARRAY_TYPE && coll_type2->type_id_ != ObNestedType::OB_VECTOR_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid collection type", K(ret), K(coll_type2->type_id_));
  } else if (type1.get_subschema_id() == type2.get_subschema_id()) {
    // do nothing
  } else {
    ObExprResType coll_calc_type;
    if (OB_FAIL(ObExprResultTypeUtil::get_array_calc_type(exec_ctx, type1, type2, coll_calc_type))) {
      LOG_WARN("failed to check array compatibilty", K(ret));
    } else {
      if (type1.get_subschema_id() != coll_calc_type.get_subschema_id()) {
        type1.set_calc_meta(coll_calc_type);
      }
      if (type2.get_subschema_id() != coll_calc_type.get_subschema_id()) {
        type2.set_calc_meta(coll_calc_type);
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_int32();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }
  
  return ret;
}

int ObExprArrayOverlaps::eval_array_relations(const ObExpr &expr, ObEvalCtx &ctx, Relation relation, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t l_meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
  const uint16_t r_meta_id = expr.args_[1]->obj_meta_.get_subschema_id();
  ObIArrayType *l_arr_obj = NULL;
  ObIArrayType *r_arr_obj = NULL;
  ObDatum *l_datum = NULL;
  ObDatum *r_datum = NULL;
  bool bret = false;
  if (OB_FAIL(expr.args_[0]->eval(ctx, l_datum))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, r_datum))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (l_datum->is_null() || r_datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, l_meta_id, l_datum->get_string(), l_arr_obj))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, r_meta_id, r_datum->get_string(), r_arr_obj))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (relation == OVERLAPS && OB_FAIL(l_arr_obj->overlaps(*r_arr_obj, bret))) {
    LOG_WARN("array overlaps failed", K(ret));
  } else if (relation == CONTAINS_ALL && OB_FAIL(l_arr_obj->contains_all(*r_arr_obj, bret))) {
    LOG_WARN("array contains failed", K(ret));
  } else {
    res.set_bool(bret);
  }
  return ret;
}

int ObExprArrayOverlaps::eval_array_relations_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                                    const int64_t batch_size, Relation relation)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t l_meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
  const uint16_t r_meta_id = expr.args_[1]->obj_meta_.get_subschema_id();
  ObIArrayType *l_arr_obj = NULL;
  ObIArrayType *r_arr_obj = NULL;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval date_unit_datum failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("failed to eval batch result args0", K(ret));
  } else {
    ObDatumVector l_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector r_array = expr.args_[1]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      bool bret = false;
      if (l_array.at(j)->is_null() || r_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, l_meta_id, l_array.at(j)->get_string(), l_arr_obj))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, r_meta_id, r_array.at(j)->get_string(), r_arr_obj))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (relation == OVERLAPS && OB_FAIL(l_arr_obj->overlaps(*r_arr_obj, bret))) {
        LOG_WARN("array overlaps failed", K(ret));
      } else if (relation == CONTAINS_ALL && OB_FAIL(l_arr_obj->contains_all(*r_arr_obj, bret))) {
        LOG_WARN("array contains all failed", K(ret));
      } else {
        res_datum.at(j)->set_bool(bret);
      }
    }
  }
  return ret;
}

int ObExprArrayOverlaps::eval_array_relation_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                    const ObBitVector &skip, const EvalBound &bound,
                                                    Relation relation)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound)) || OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval params", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    ObIVector *left_vec = expr.args_[0]->get_vector(ctx);
    VectorFormat left_format = left_vec->get_format();
    ObIVector *right_vec = expr.args_[1]->get_vector(ctx);
    VectorFormat right_format = right_vec->get_format();
    const uint16_t left_meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
    const uint16_t right_meta_id = expr.args_[1]->obj_meta_.get_subschema_id();
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObIArrayType *l_arr_obj = NULL;
    ObIArrayType *r_arr_obj = NULL;
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_null_res = false;
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (left_vec->is_null(idx) || right_vec->is_null(idx)) {
        is_null_res = true;
      } else {
        ObString left = left_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, left_meta_id, left, l_arr_obj))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        // do nothing, set result at last
      } else {
        ObString right = right_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, right_meta_id, right, r_arr_obj))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      }
      bool bret = false;
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
      } else if (relation == OVERLAPS && OB_FAIL(l_arr_obj->overlaps(*r_arr_obj, bret))) {
        LOG_WARN("array overlaps failed", K(ret));
      } else if (relation == CONTAINS_ALL && OB_FAIL(l_arr_obj->contains_all(*r_arr_obj, bret))) {
        LOG_WARN("array contains all failed", K(ret));
      } else {
        res_vec->set_bool(idx, bret);
        eval_flags.set(idx);
      }
    }
  }

  return ret;
}

int ObExprArrayOverlaps::eval_array_overlaps(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_array_relations(expr, ctx, OVERLAPS, res);
}

int ObExprArrayOverlaps::eval_array_overlaps_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  return eval_array_relations_batch(expr, ctx, skip, batch_size, OVERLAPS);
}

int ObExprArrayOverlaps::eval_array_overlaps_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                    const ObBitVector &skip, const EvalBound &bound)
{
  return eval_array_relation_vector(expr, ctx, skip, bound, OVERLAPS);
}

int ObExprArrayOverlaps::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_overlaps;
  rt_expr.eval_batch_func_ = eval_array_overlaps_batch;
  rt_expr.eval_vector_func_ = eval_array_overlaps_vector;

  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
