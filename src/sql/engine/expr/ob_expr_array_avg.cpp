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
 * This file contains implementation for array_avg expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_avg.h"
#include "lib/udt/ob_array_type.h"
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
ObExprArrayAvg::ObExprArrayAvg(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_ARRAY_AVG, N_ARRAY_AVG, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayAvg::~ObExprArrayAvg()
{
}

int ObExprArrayAvg::calc_result_type1(ObExprResType &type,
                                      ObExprResType &type1,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  ObDataType src_elem_type;
  uint32_t depth = 0;
  bool is_vec = false;

  if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLSessionInfo is null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExecContext is null", K(ret));
  } else if (ob_is_null(type1.get_type())) {
    // do nothing
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ARRAY", ob_obj_type_str(type1.get_type()));
  } else if (OB_FAIL(ObArrayExprUtils::get_array_element_type(exec_ctx, type1.get_subschema_id(), src_elem_type, depth, is_vec))) {
    LOG_WARN("failed to get array element type", K(ret));
  } else if (depth != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "array_avg with multi-dimension array");
    LOG_WARN("not supported array dimension", K(ret), K(depth));
  } else if (!ob_is_numeric_type(src_elem_type.get_obj_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "array_avg with non-numeric type");
    LOG_WARN("not supported array data type", K(ret), K(src_elem_type.get_obj_type()));
  }
  if (OB_SUCC(ret)) {
    type.set_double();
  }
  return ret;
}

int ObExprArrayAvg::eval_array_avg(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObDatum *arr_datum = NULL;
  ObCollectionArrayType *arr_type = NULL;
  ObIArrayType *src_arr = NULL;
  double res_average = 0;
  uint32_t depth = 0;

  if (OB_FAIL(expr.args_[0]->eval(ctx, arr_datum))) {
    LOG_WARN("failed to eval source array arg", K(ret));
  } else if (arr_datum->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObArrayExprUtils::get_array_type_by_subschema_id(ctx, subschema_id, arr_type))) {
    LOG_WARN("failed to get array type by subschema id", K(ret), K(subschema_id));
  } else {
    ObString data_str = arr_datum->get_string();
    uint32_t len = 0, data_len = 0;
    uint8_t *null_bitmaps = nullptr;
    const char *data = nullptr;

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                        ObLongTextType,
                                        CS_TYPE_BINARY,
                                        true,
                                        data_str))) {
      LOG_WARN("fail to get real data", K(ret), K(data_str));
    } else if (OB_FAIL(ObArrayExprUtils::get_array_data(data_str,
                                            arr_type,
                                            len,
                                            null_bitmaps,
                                            data,
                                            data_len))) {
      LOG_WARN("failed to get array data", K(ret));
    } else if (ob_is_integer_type(arr_type->get_basic_meta(depth).get_obj_type())) {
      depth = 0;
      if (ob_is_unsigned_type(arr_type->get_basic_meta(depth).get_obj_type())) {
        uint64_t res_sum = 0;
        if (OB_FAIL(ObArrayExprUtils::calc_array_sum(len, null_bitmaps, data, data_len, arr_type, res_sum))) {
          LOG_WARN("failed to calc sum", K(ret));
        } else {
          res.set_double(static_cast<double>(res_sum) / len);
        }
      } else {
        int64_t res_sum = 0;
        if (OB_FAIL(ObArrayExprUtils::calc_array_sum(len, null_bitmaps, data, data_len, arr_type, res_sum))) {
          LOG_WARN("failed to calc sum", K(ret));
        } else {
          res.set_double(static_cast<double>(res_sum) / len);
        }
      }
    } else {
      double res_sum = 0;
      if (OB_FAIL(ObArrayExprUtils::calc_array_sum(len, null_bitmaps, data, data_len, arr_type, res_sum))) {
        LOG_WARN("failed to calc sum", K(ret));
      } else {
        res.set_double(res_sum / len);
      }
    }
  }

  return ret;
}

int ObExprArrayAvg::eval_array_avg_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                         const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObCollectionArrayType *arr_type = NULL;
  ObIArrayType *src_arr = NULL;
  uint32_t depth = 0;

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval source array failed", K(ret));
  } else {
    ObDatumVector arr_array = expr.args_[0]->locate_expr_datumvector(ctx);
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      bool is_null_res = false;
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_array.at(j)->is_null()) {
        res_datum.at(j)->set_null();
      } else if (OB_FAIL(ObArrayExprUtils::get_array_type_by_subschema_id(ctx, subschema_id, arr_type))) {
        LOG_WARN("failed to get array type by subschema id", K(ret), K(subschema_id));
      } else {
        ObString data_str = arr_array.at(j)->get_string();
        uint32_t len = 0, data_len = 0;
        uint8_t *null_bitmaps = nullptr;
        const char *data = nullptr;

        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                            ObLongTextType,
                                            CS_TYPE_BINARY,
                                            true,
                                            data_str))) {
          LOG_WARN("fail to get real data.", K(ret), K(data_str));
        } else if (OB_FAIL(ObArrayExprUtils::get_array_data(data_str,
                                                arr_type,
                                                len,
                                                null_bitmaps,
                                                data,
                                                data_len))) {
          LOG_WARN("failed to get array data", K(ret));
        } else if (ob_is_integer_type(arr_type->get_basic_meta(depth).get_obj_type())) {
          depth = 0;
          if (ob_is_unsigned_type(arr_type->get_basic_meta(depth).get_obj_type())) {
            uint64_t res_sum = 0;
            if (OB_FAIL(ObArrayExprUtils::calc_array_sum(len, null_bitmaps, data, data_len, arr_type, res_sum))) {
              LOG_WARN("failed to calc sum", K(ret));
            } else {
              res_datum.at(j)->set_double(static_cast<double>(res_sum) / len);
            }
          } else {
            int64_t res_sum = 0;
            if (OB_FAIL(ObArrayExprUtils::calc_array_sum(len, null_bitmaps, data, data_len, arr_type, res_sum))) {
              LOG_WARN("failed to calc sum", K(ret));
            } else {
              res_datum.at(j)->set_double(static_cast<double>(res_sum) / len);
            }
          }
        } else {
          double res_sum = 0;
          if (OB_FAIL(ObArrayExprUtils::calc_array_sum(len,
                                            null_bitmaps,
                                            data,
                                            data_len,
                                            arr_type,
                                            res_sum))) {
            LOG_WARN("failed to calc sum", K(ret));
          } else {
            res_datum.at(j)->set_double(res_sum / len);
          }
        }
      }
    } // end for
  }
  return ret;
}

int ObExprArrayAvg::eval_array_avg_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  ObCollectionArrayType *arr_type = NULL;
  ObIArrayType *src_arr = NULL;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else {
    uint32_t attr_count = expr.args_[0]->attrs_cnt_;
    ObIVector *arr_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *len_vec = nullptr;
    ObIVector *nullbitmap_vec = nullptr;
    ObIVector *data_vec = nullptr;
    if (attr_count == 3) {
      len_vec = expr.args_[0]->attrs_[0]->get_vector(ctx);
      nullbitmap_vec = expr.args_[0]->attrs_[1]->get_vector(ctx);
      data_vec = expr.args_[0]->attrs_[2]->get_vector(ctx);
    }
    ObIVector *res_vec = expr.get_vector(ctx);
    VectorFormat arr_format = arr_vec->get_format();
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    uint32_t depth = 0;

    for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
      bool is_null_res = false;
      uint32_t len = 0, data_len = 0;
      uint8_t *null_bitmaps = nullptr;
      const char *data = nullptr;

      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (arr_vec->is_null(j)) {
        is_null_res = true;
      } else if (OB_FAIL(ObArrayExprUtils::get_array_type_by_subschema_id(ctx, subschema_id, arr_type))) {
        LOG_WARN("failed to get array type by subschema id", K(ret), K(subschema_id));
      } else if (arr_format == VEC_UNIFORM || arr_format == VEC_UNIFORM_CONST) {
        ObString data_str = arr_vec->get_string(j);
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                            ObLongTextType,
                                            CS_TYPE_BINARY,
                                            true,
                                            data_str))) {
          LOG_WARN("fail to get real data.", K(ret), K(data_str));
        } else if (OB_FAIL(ObArrayExprUtils::get_array_data(data_str,
                                                arr_type,
                                                len,
                                                null_bitmaps,
                                                data,
                                                data_len))) {
          LOG_WARN("failed to get array data", K(ret));
        }
      } else if (OB_FAIL(ObArrayExprUtils::get_array_data(len_vec,
                                              nullbitmap_vec,
                                              data_vec,
                                              j,
                                              arr_type,
                                              len,
                                              null_bitmaps,
                                              data,
                                              data_len))) {
        LOG_WARN("failed to get array data", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (is_null_res) {
        res_vec->set_null(j);
      } else if (ob_is_integer_type(arr_type->get_basic_meta(depth).get_obj_type())) {
        depth = 0;
        if (ob_is_unsigned_type(arr_type->get_basic_meta(depth).get_obj_type())) {
          uint64_t res_sum = 0;
          if (OB_FAIL(ObArrayExprUtils::calc_array_sum(len,
                                            null_bitmaps,
                                            data,
                                            data_len,
                                            arr_type,
                                            res_sum))) {
            LOG_WARN("failed to calc sum", K(ret));
          } else {
            res_vec->set_double(j, static_cast<double>(res_sum) / len);
          }
        } else {
          int64_t res_sum = 0;
          if (OB_FAIL(ObArrayExprUtils::calc_array_sum(len,
                                            null_bitmaps,
                                            data,
                                            data_len,
                                            arr_type,
                                            res_sum))) {
            LOG_WARN("failed to calc sum", K(ret));
          } else {
            res_vec->set_double(j, static_cast<double>(res_sum) / len);
          }
        }
      } else {
        double res_sum = 0;
        if (OB_FAIL(ObArrayExprUtils::calc_array_sum(len,
                                          null_bitmaps,
                                          data,
                                          data_len,
                                          arr_type,
                                          res_sum))) {
          LOG_WARN("failed to calc sum", K(ret));
        } else {
          res_vec->set_double(j, res_sum / len);
        }
      }
    } // end for
  }

  return ret;
}

int ObExprArrayAvg::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_avg;
  rt_expr.eval_batch_func_ = eval_array_avg_batch;
  rt_expr.eval_vector_func_ = eval_array_avg_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
