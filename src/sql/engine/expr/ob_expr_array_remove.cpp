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
 * This file contains implementation for array_remove.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_remove.h"
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

ObExprArrayRemove::ObExprArrayRemove(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_REMOVE, N_ARRAY_REMOVE, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayRemove::ObExprArrayRemove(ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name,
                         int32_t param_num, 
                         int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension) 
{
}

ObExprArrayRemove::~ObExprArrayRemove()
{
}

int ObExprArrayRemove::calc_result_type2(ObExprResType &type,
                                           ObExprResType &type1,
                                           ObExprResType &type2,
                                           common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  uint16_t subschema_id = type1.get_subschema_id();
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (type1.is_null()) {
    type.set_null();
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(type1.get_type()), ob_obj_type_str(type2.get_type()));
  } else if (OB_FAIL(ObArrayExprUtils::deduce_array_type(exec_ctx, type1, type2, subschema_id))) {
    LOG_WARN("failed to get result array type subschema id", K(ret));
  } 
  if (OB_SUCC(ret) && !type1.is_null()) {
    type.set_collection(subschema_id);
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObCollectionSQLType]).get_length());
  }
  
  return ret;
}

#define EVAL_FUNC_ARRAY_REMOVE(TYPE, GET_FUNC)                                                                        \
  int ObExprArrayRemove::eval_array_remove_##TYPE(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)                   \
  {                                                                                                                   \
    int ret = OB_SUCCESS;                                                                                             \
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);                                                                       \
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();                                            \
    const uint16_t meta_id = expr.args_[0]->obj_meta_.get_subschema_id();                                             \
    ObIArrayType *arr_obj = NULL;                                                                                     \
    ObIArrayType *res_arr_obj = NULL;                                                                                 \
    ObDatum *datum = NULL;                                                                                            \
    ObDatum *datum_val = NULL;                                                                                        \
    TYPE val;                                                                                                         \
    bool bret = true;                                                                                                 \
    bool changed = true;                                                                                              \
    if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {                                                                   \
      LOG_WARN("failed to eval args", K(ret));                                                                        \
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, datum_val))) {                                                        \
      LOG_WARN("failed to eval args", K(ret));                                                                        \
    } else if (datum->is_null()) {                                                                                    \
      res.set_null();                                                                                                 \
    } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, meta_id, datum->get_string(), arr_obj))) { \
      LOG_WARN("construct array obj failed", K(ret));                                                                 \
    } else if (datum_val->is_null() && !arr_obj->contain_null()) {                                                    \
      changed = false;                                                                                                \
      res_arr_obj = arr_obj;                                                                                          \
    } else if (!datum_val->is_null() && FALSE_IT(val = datum_val->GET_FUNC())) {                                      \
    } else if (!datum_val->is_null() && OB_FAIL(ObArrayUtil::contains(*arr_obj, val, bret))) {                        \
      LOG_WARN("array contains failed", K(ret));                                                                      \
    } else if (!bret) {                                                                                               \
      changed = false;                                                                                                \
      res_arr_obj = arr_obj;                                                                                          \
    }                                                                                                                 \
    if (OB_SUCC(ret) && !datum->is_null()) {                                                                          \
      if (changed) {                                                                                                  \
        if (OB_FAIL(ObArrayUtil::clone_except(tmp_allocator, *arr_obj, &val, datum_val->is_null(), res_arr_obj))) {   \
          LOG_WARN("array remove failed", K(ret));                                                                    \
        }                                                                                                             \
      }                                                                                                               \
      if (OB_SUCC(ret)) {                                                                                             \
        ObString res_str;                                                                                             \
        if (OB_FAIL(ObArrayExprUtils::set_array_res(                                                                  \
                res_arr_obj, res_arr_obj->get_raw_binary_len(), expr, ctx, res_str))) {                               \
          LOG_WARN("get array binary string failed", K(ret));                                                         \
        } else {                                                                                                      \
          res.set_string(res_str);                                                                                    \
        }                                                                                                             \
      }                                                                                                               \
    }                                                                                                                 \
    return ret;                                                                                                       \
  }

EVAL_FUNC_ARRAY_REMOVE(int64_t, get_int)
EVAL_FUNC_ARRAY_REMOVE(float, get_float)
EVAL_FUNC_ARRAY_REMOVE(double, get_double)
EVAL_FUNC_ARRAY_REMOVE(ObString, get_string)

int ObExprArrayRemove::eval_array_remove_array(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
  const uint16_t r_meta_id = expr.args_[1]->obj_meta_.get_subschema_id();
  ObIArrayType *arr_obj = NULL;
  ObIArrayType *remove_arr_obj = NULL;
  ObIArrayType *res_arr_obj = NULL;
  ObDatum *datum = NULL;
  ObDatum *datum_val = NULL;
  bool bret = true;
  bool changed = true;
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, datum_val))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, meta_id, datum->get_string(), arr_obj))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (datum_val->is_null() && !arr_obj->contain_null()) {
    changed = false;
    res_arr_obj = arr_obj;
  } else if (!datum_val->is_null()
             && OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, r_meta_id, datum_val->get_string(), remove_arr_obj))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (remove_arr_obj != NULL &&  OB_FAIL(ObArrayUtil::contains(*arr_obj, *remove_arr_obj, bret))) {
    LOG_WARN("array contains failed", K(ret));
  } else if (!bret) {
    changed = false;
    res_arr_obj = arr_obj;
  }
  if (OB_SUCC(ret) && !datum->is_null()) {
    if (changed) {
      if (OB_FAIL(ObArrayUtil::clone_except(tmp_allocator, *arr_obj, remove_arr_obj, datum_val->is_null(), res_arr_obj))) {
        LOG_WARN("array remove failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObString res_str;
      if (OB_FAIL(
              ObArrayExprUtils::set_array_res(res_arr_obj, res_arr_obj->get_raw_binary_len(), expr, ctx, res_str))) {
        LOG_WARN("get array binary string failed", K(ret));
      } else {
        res.set_string(res_str);
      }
    }
  }
  return ret;
}

#define EVAL_FUNC_ARRAY_REMOVE_BATCH(TYPE, GET_FUNC)                                                     \
  int ObExprArrayRemove::eval_array_remove_batch_##TYPE(                                                 \
      const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)             \
  {                                                                                                      \
    int ret = OB_SUCCESS;                                                                                \
    ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);                                         \
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);                                             \
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);                                                          \
    common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();                               \
    const uint16_t meta_id = expr.args_[0]->obj_meta_.get_subschema_id();                                \
    ObIArrayType *arr_obj = NULL;                                                                        \
    ObIArrayType *res_arr_obj = NULL;                                                                    \
    ObDatum *datum = NULL;                                                                               \
    ObDatum *datum_val = NULL;                                                                           \
    TYPE val;                                                                                            \
    if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {                                     \
      LOG_WARN("failed to eval args", K(ret));                                                           \
    } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {                              \
      LOG_WARN("failed to eval args", K(ret));                                                           \
    } else {                                                                                             \
      ObDatumVector src_array = expr.args_[0]->locate_expr_datumvector(ctx);                             \
      ObDatumVector val_array = expr.args_[1]->locate_expr_datumvector(ctx);                             \
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {                                         \
        if (skip.at(j) || eval_flags.at(j)) {                                                            \
          continue;                                                                                      \
        }                                                                                                \
        eval_flags.set(j);                                                                               \
        bool bret = true;                                                                                \
        bool changed = true;                                                                             \
        if (src_array.at(j)->is_null()) {                                                                \
          res_datum.at(j)->set_null();                                                                   \
        } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(                                              \
                       tmp_allocator, ctx, meta_id, src_array.at(j)->get_string(), arr_obj))) {          \
          LOG_WARN("construct array obj failed", K(ret));                                                \
        } else if (val_array.at(j)->is_null() && !arr_obj->contain_null()) {                             \
          changed = false;                                                                               \
          res_arr_obj = arr_obj;                                                                         \
        } else if (!val_array.at(j)->is_null() && FALSE_IT(val = val_array.at(j)->GET_FUNC())) {         \
        } else if (!val_array.at(j)->is_null() && OB_FAIL(ObArrayUtil::contains(*arr_obj, val, bret))) { \
          LOG_WARN("array contains failed", K(ret));                                                     \
        } else if (!bret) {                                                                              \
          changed = false;                                                                               \
          res_arr_obj = arr_obj;                                                                         \
        }                                                                                                \
        if (OB_SUCC(ret) && !src_array.at(j)->is_null()) {                                               \
          if (changed) {                                                                                 \
            if (OB_FAIL(ObArrayUtil::clone_except(                                                       \
                    tmp_allocator, *arr_obj, &val, val_array.at(j)->is_null(), res_arr_obj))) {          \
              LOG_WARN("array remove failed", K(ret));                                                   \
            }                                                                                            \
          }                                                                                              \
          if (OB_SUCC(ret)) {                                                                            \
            int32_t res_size = res_arr_obj->get_raw_binary_len();                                        \
            char *res_buf = nullptr;                                                                     \
            int64_t res_buf_len = 0;                                                                     \
            ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, res_datum.at(j)); \
            if (OB_FAIL(output_result.init_with_batch_idx(res_size, j))) {                               \
              LOG_WARN("fail to init result", K(ret), K(res_size));                                      \
            } else if (OB_FAIL(output_result.get_reserved_buffer(res_buf, res_buf_len))) {               \
              LOG_WARN("fail to get reserver buffer", K(ret));                                           \
            } else if (res_buf_len < res_size) {                                                         \
              ret = OB_ERR_UNEXPECTED;                                                                   \
              LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_size));                  \
            } else if (OB_FAIL(res_arr_obj->get_raw_binary(res_buf, res_buf_len))) {                     \
              LOG_WARN("get array raw binary failed", K(ret), K(res_buf_len), K(res_size));              \
            } else if (OB_FAIL(output_result.lseek(res_size, 0))) {                                      \
              LOG_WARN("failed to lseek res.", K(ret), K(output_result), K(res_size));                   \
            } else {                                                                                     \
              output_result.set_result();                                                                \
              res_arr_obj->clear();                                                                      \
            }                                                                                            \
          }                                                                                              \
        }                                                                                                \
      }                                                                                                  \
    }                                                                                                    \
    return ret;                                                                                          \
  }

EVAL_FUNC_ARRAY_REMOVE_BATCH(int64_t, get_int)
EVAL_FUNC_ARRAY_REMOVE_BATCH(float, get_float)
EVAL_FUNC_ARRAY_REMOVE_BATCH(double, get_double)
EVAL_FUNC_ARRAY_REMOVE_BATCH(ObString, get_string)

int ObExprArrayRemove::eval_array_remove_array_batch(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
  const uint16_t r_meta_id = expr.args_[1]->obj_meta_.get_subschema_id();
  ObIArrayType *arr_obj = NULL;
  ObIArrayType *remove_arr_obj = NULL;
  ObIArrayType *res_arr_obj = NULL;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("failed to eval args", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("failed to eval args", K(ret));
  } else {
    ObDatumVector src_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector val_array = expr.args_[1]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      bool bret = true;
      bool changed = true;
      if (src_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(
                     tmp_allocator, ctx, meta_id, src_array.at(j)->get_string(), arr_obj))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (val_array.at(j)->is_null() && !arr_obj->contain_null()) {
        changed = false;
        res_arr_obj = arr_obj;
      } else if (!val_array.at(j)->is_null() &&
                 OB_FAIL(ObArrayExprUtils::get_array_obj(
                     tmp_allocator, ctx, r_meta_id, val_array.at(j)->get_string(), remove_arr_obj))) {
        LOG_WARN("construct array obj failed", K(ret));
      } else if (!val_array.at(j)->is_null() && OB_FAIL(ObArrayUtil::contains(*arr_obj, *remove_arr_obj, bret))) {
        LOG_WARN("array contains failed", K(ret));
      } else if (!bret) {
        changed = false;
        res_arr_obj = arr_obj;
      }
      if (OB_SUCC(ret) && !src_array.at(j)->is_null()) {
        if (changed) {
          if (OB_FAIL(
                  ObArrayUtil::clone_except(tmp_allocator, *arr_obj, remove_arr_obj, val_array.at(j)->is_null(), res_arr_obj))) {
            LOG_WARN("array remove failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          int32_t res_size = res_arr_obj->get_raw_binary_len();
          char *res_buf = nullptr;
          int64_t res_buf_len = 0;
          ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, res_datum.at(j));
          if (OB_FAIL(output_result.init_with_batch_idx(res_size, j))) {
            LOG_WARN("fail to init result", K(ret), K(res_size));
          } else if (OB_FAIL(output_result.get_reserved_buffer(res_buf, res_buf_len))) {
            LOG_WARN("fail to get reserver buffer", K(ret));
          } else if (res_buf_len < res_size) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
          } else if (OB_FAIL(res_arr_obj->get_raw_binary(res_buf, res_buf_len))) {
            LOG_WARN("get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
          } else if (OB_FAIL(output_result.lseek(res_size, 0))) {
            LOG_WARN("failed to lseek res.", K(ret), K(output_result), K(res_size));
          } else {
            output_result.set_result();
            res_arr_obj->clear();
          }
        }
      }
    }
  }
  return ret;
}

#define EVAL_FUNC_ARRAY_REMOVE_VECTOR(TYPE, GET_FUNC)                                                                 \
  int ObExprArrayRemove::eval_array_remove_vector_##TYPE(                                                             \
      const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)                            \
  {                                                                                                                   \
    int ret = OB_SUCCESS;                                                                                             \
    if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound)) ||                                                      \
        OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {                                                      \
      LOG_WARN("fail to eval params", K(ret));                                                                        \
    } else {                                                                                                          \
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);                                                                     \
      common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();                                          \
      ObIVector *left_vec = expr.args_[0]->get_vector(ctx);                                                           \
      VectorFormat left_format = left_vec->get_format();                                                              \
      ObIVector *right_vec = expr.args_[1]->get_vector(ctx);                                                          \
      const uint16_t meta_id = expr.args_[0]->obj_meta_.get_subschema_id();                                           \
      ObIVector *res_vec = expr.get_vector(ctx);                                                                      \
      VectorFormat res_format = res_vec->get_format();                                                                \
      ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);                                                        \
      ObIArrayType *arr_obj = NULL;                                                                                   \
      TYPE val;                                                                                                       \
      ObIArrayType *res_arr_obj = NULL;                                                                               \
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {                                   \
        bool is_null_res = false;                                                                                     \
        bool bret = true;                                                                                             \
        bool changed = true;                                                                                          \
        if (skip.at(idx) || eval_flags.at(idx)) {                                                                     \
          continue;                                                                                                   \
        } else if (left_vec->is_null(idx)) {                                                                          \
          is_null_res = true;                                                                                         \
        } else {                                                                                                      \
          ObString left = left_vec->get_string(idx);                                                                  \
          if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, meta_id, left, arr_obj))) {             \
            LOG_WARN("construct array obj failed", K(ret));                                                           \
          }                                                                                                           \
        }                                                                                                             \
        if (OB_FAIL(ret)) {                                                                                           \
        } else if (is_null_res) {                                                                                     \
        } else if (right_vec->is_null(idx)) {                                                                         \
          if (!arr_obj->contain_null()) {                                                                             \
            changed = false;                                                                                          \
            res_arr_obj = arr_obj;                                                                                    \
          }                                                                                                           \
        } else if (FALSE_IT(val = right_vec->GET_FUNC(idx))) {                                                        \
        } else if (OB_FAIL(ObArrayUtil::contains(*arr_obj, val, bret))) {                                             \
          LOG_WARN("array contains failed", K(ret));                                                                  \
        } else if (!bret) {                                                                                           \
          changed = false;                                                                                            \
          res_arr_obj = arr_obj;                                                                                      \
        }                                                                                                             \
        if (OB_FAIL(ret)) {                                                                                           \
        } else if (is_null_res) {                                                                                     \
          res_vec->set_null(idx);                                                                                     \
          eval_flags.set(idx);                                                                                        \
        } else {                                                                                                      \
          if (changed) {                                                                                              \
            if (OB_FAIL(                                                                                              \
                    ObArrayUtil::clone_except(tmp_allocator, *arr_obj, &val, right_vec->is_null(idx), res_arr_obj))) { \
              LOG_WARN("array remove failed", K(ret));                                                                \
            }                                                                                                         \
          }                                                                                                           \
          if (OB_SUCC(ret)) {                                                                                         \
            if (res_format == VEC_DISCRETE) {                                                                         \
              if (OB_FAIL(ObArrayExprUtils::set_array_res<ObDiscreteFormat>(                                          \
                      res_arr_obj, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), idx))) {                      \
                LOG_WARN("set array res failed", K(ret));                                                             \
              }                                                                                                       \
            } else if (res_format == VEC_UNIFORM) {                                                                   \
              if (OB_FAIL(ObArrayExprUtils::set_array_res<ObUniformFormat<false>>(                                    \
                      res_arr_obj, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), idx))) {                \
                LOG_WARN("set array res failed", K(ret));                                                             \
              }                                                                                                       \
            } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ObVectorBase>(                                         \
                           res_arr_obj, expr, ctx, static_cast<ObVectorBase *>(res_vec), idx))) {                     \
              LOG_WARN("set array res failed", K(ret));                                                               \
            }                                                                                                         \
            if (OB_SUCC(ret)) {                                                                                       \
              eval_flags.set(idx);                                                                                    \
              res_arr_obj->clear();                                                                                   \
            }                                                                                                         \
          }                                                                                                           \
        }                                                                                                             \
      }                                                                                                               \
    }                                                                                                                 \
    return ret;                                                                                                       \
  }

EVAL_FUNC_ARRAY_REMOVE_VECTOR(int64_t, get_int)
EVAL_FUNC_ARRAY_REMOVE_VECTOR(float, get_float)
EVAL_FUNC_ARRAY_REMOVE_VECTOR(double, get_double)
EVAL_FUNC_ARRAY_REMOVE_VECTOR(ObString, get_string)

int ObExprArrayRemove::eval_array_remove_array_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                      const ObBitVector &skip, const EvalBound &bound)
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
    const uint16_t meta_id = expr.args_[0]->obj_meta_.get_subschema_id();
    const uint16_t r_meta_id = expr.args_[1]->obj_meta_.get_subschema_id();
    ObIVector *res_vec = expr.get_vector(ctx);
    VectorFormat res_format = res_vec->get_format();
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObIArrayType *arr_obj = NULL;
    ObIArrayType *arr_val = NULL;
    ObIArrayType *res_arr_obj = NULL;
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool is_null_res = false;
      bool bret = true;
      bool changed = true;
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (left_vec->is_null(idx)) {
        is_null_res = true;
      } else {
        ObString left = left_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, meta_id, left, arr_obj))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        // do noting
      } else if (right_vec->is_null(idx)) {
        if (!arr_obj->contain_null()) {
          changed = false;
          res_arr_obj = arr_obj;
        }
      } else {
        ObString right = right_vec->get_string(idx);
        if (OB_FAIL(ObNestedVectorFunc::construct_param(tmp_allocator, ctx, r_meta_id, right, arr_val))) {
          LOG_WARN("construct array obj failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
      } else if (!right_vec->is_null(idx) && OB_FAIL(ObArrayUtil::contains(*arr_obj, *arr_val, bret))) {
        LOG_WARN("array contains failed", K(ret));
      } else if (!bret) {
        changed = false;
        res_arr_obj = arr_obj;
      } 
      
      if (OB_SUCC(ret) && !is_null_res) {
        if (changed) {
          if (OB_FAIL(ObArrayUtil::clone_except(tmp_allocator, *arr_obj, arr_val, right_vec->is_null(idx), res_arr_obj))) {
            LOG_WARN("array remove failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (res_format == VEC_DISCRETE) {
            if (OB_FAIL(ObArrayExprUtils::set_array_res<ObDiscreteFormat>(res_arr_obj, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), idx))) {
              LOG_WARN("set array res failed", K(ret));
            }
          } else if (res_format == VEC_UNIFORM) {
            if (OB_FAIL(ObArrayExprUtils::set_array_res<ObUniformFormat<false>>(res_arr_obj, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), idx))) {
              LOG_WARN("set array res failed", K(ret));
            }
          } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ObVectorBase>(res_arr_obj, expr, ctx, static_cast<ObVectorBase *>(res_vec), idx))) {
            LOG_WARN("set array res failed", K(ret));
          } 
          if (OB_SUCC(ret)) {
            eval_flags.set(idx);
            res_arr_obj->clear();
          }
        }
      }
    }
  }

  return ret;
}


int ObExprArrayRemove::cg_expr(ObExprCGCtx &expr_cg_ctx,
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
    const ObObjType right_type = rt_expr.args_[1]->datum_meta_.type_;  
    ObObjTypeClass right_tc = ob_obj_type_class(right_type);
    if (right_tc == ObNullTC) {
      if (ob_is_null(rt_expr.args_[0]->datum_meta_.type_)) {
        right_tc = ObNullTC;
      } else {
        // use array element type
        ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
        const uint16_t sub_id = rt_expr.args_[0]->obj_meta_.get_subschema_id();
        ObObjType elem_type;
        uint32_t unused;
        bool is_vec = false;
        if (OB_FAIL(ObArrayExprUtils::get_array_element_type(exec_ctx, sub_id, elem_type, unused, is_vec))) {
          LOG_WARN("failed to get collection elem type", K(ret), K(sub_id));
        } else {
          right_tc = ob_obj_type_class(elem_type);
        }
      }
    }
    if OB_SUCC(ret) {
      switch (right_tc) {
        case ObIntTC:
        case ObUIntTC:
          rt_expr.eval_func_ = eval_array_remove_int64_t;
          rt_expr.eval_batch_func_ = eval_array_remove_batch_int64_t;
          rt_expr.eval_vector_func_ = eval_array_remove_vector_int64_t;
          break;
        case ObFloatTC:
          rt_expr.eval_func_ = eval_array_remove_float;
          rt_expr.eval_batch_func_ = eval_array_remove_batch_float;
          rt_expr.eval_vector_func_ = eval_array_remove_vector_float;
          break;
        case ObDoubleTC:
          rt_expr.eval_func_ = eval_array_remove_double;
          rt_expr.eval_batch_func_ = eval_array_remove_batch_double;
          rt_expr.eval_vector_func_ = eval_array_remove_vector_double;
          break;
        case ObStringTC:
          rt_expr.eval_func_ = eval_array_remove_ObString;
          rt_expr.eval_batch_func_ = eval_array_remove_batch_ObString;
          rt_expr.eval_vector_func_ = eval_array_remove_vector_ObString;
          break;
        case ObNullTC:
        case ObCollectionSQLTC:
          rt_expr.eval_func_ = eval_array_remove_array;
          rt_expr.eval_batch_func_ = eval_array_remove_array_batch;
          rt_expr.eval_vector_func_ = eval_array_remove_array_vector;
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
