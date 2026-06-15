/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_ifnull.h"

#include "share/vector/ob_bitmap_null_vector_base.h"
#include "share/vector/ob_uniform_base.h"
#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace oceanbase::common;
namespace sql
{

ObExprIfNull::ObExprIfNull(ObIAllocator &alloc) : ObFuncExprOperator(alloc, T_FUN_SYS_IFNULL, N_IFNULL, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIfNull::~ObExprIfNull()
{
}

int ObExprIfNull::calc_result_type2(ObExprResType &type,
                                    ObExprResType &type1,
                                    ObExprResType &type2,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(ObExprPromotionUtil::get_nvl_type(type, type1, type2))) {
    LOG_WARN("failed to get nvl type", K(ret));
  } else if (ob_is_string_type(type.get_type()) || ob_is_json_tc(type.get_type())) {
    ObExprResTypes res_types;
    if (OB_FAIL(res_types.push_back(type1))) {
      LOG_WARN("fail to push back res type", K(ret));
    } else if (OB_FAIL(res_types.push_back(type2))) {
      LOG_WARN("fail to push back res type", K(ret));
    } else if (OB_FAIL(aggregate_charsets_for_string_result(type, &res_types.at(0), 2, type_ctx))) {
      LOG_WARN("failed to aggregate_charsets_for_comparison", K(ret));
    }
  } else if (ob_is_roaringbitmap_tc(type.get_type())) {
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_collation_type(CS_TYPE_BINARY);
  }

  if (OB_FAIL(ret)) {
  } else if (ob_is_collection_sql_type(type.get_type())) {
    ObSQLSessionInfo *sess = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
    ObExecContext *exec_ctx = sess->get_cur_exec_ctx();
    ObExprResType coll_calc_type = type;
    if (OB_ISNULL(exec_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec ctx is null", K(ret));
    } else if (type1.get_subschema_id() == type2.get_subschema_id()) {
      type.set_collection(type1.get_subschema_id());
    } else if (OB_FAIL(ObExprResultTypeUtil::get_array_calc_type(exec_ctx, type1, type2, coll_calc_type))) {
      LOG_WARN("deduce calc type failed", K(ret));
    } else {
      type1.set_calc_meta(coll_calc_type);
      type2.set_calc_meta(coll_calc_type);
      type.set_collection(coll_calc_type.get_subschema_id());
    }
  } else {
    if (type.get_type() == type1.get_type()) {
      type.set_accuracy(type1.get_accuracy());
    } else {
      type.set_accuracy(type2.get_accuracy());
    }
    if (ob_is_integer_type(type1.get_type()) && ob_is_integer_type(type2.get_type())) {
      if (type1.get_type_class() == type2.get_type_class()) {
        type.set_type(MAX(type1.get_type(), type2.get_type()));
      } else { // unsigned and signed
        ObObjType signed_type = (type1.get_type_class() == ObIntTC) ? type1.get_type() : type2.get_type();
        ObObjType unsigned_type = (type1.get_type_class() == ObIntTC) ? type2.get_type() : type1.get_type();
        int signed_type_diff = static_cast<int>(signed_type) - static_cast<int>(ObTinyIntType);
        int unsigned_type_diff = static_cast<int>(unsigned_type) - static_cast<int>(ObUTinyIntType);
        int res_type_diff = (unsigned_type_diff >= signed_type_diff) ? (unsigned_type_diff + 1) : signed_type_diff;
        //对于 int 和uint64的混合类型，需要提升类型至decimal
        if (res_type_diff > (static_cast<int>(ObIntType) - static_cast<int>(ObTinyIntType))) {
          type.set_type(ObNumberType);
          type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].get_accuracy());
        } else {
          type.set_type(static_cast<ObObjType>(res_type_diff + ObTinyIntType));
        }
      }
    }

    //set scale
    ObScale scale1 = type1.is_null() ? 0 : type1.get_scale();
    ObScale scale2 = type2.is_null() ? 0 : type2.get_scale();
    if (-1 != scale1 && -1 != scale2) {
      type.set_scale(static_cast<ObScale>(max(scale1, scale2)));
    } else {
      type.set_scale(-1);
    }
    if (lib::is_mysql_mode() && SCALE_UNKNOWN_YET != type.get_scale()) {
      if (ob_is_real_type(type.get_type())) {
        type.set_precision(static_cast<ObPrecision>(ObMySQLUtil::float_length(type.get_scale())));
      } else if (ob_is_number_or_decimal_int_tc(type.get_type())) {
        const int16_t intd1 = type1.get_precision() - type1.get_scale();
        const int16_t intd2 = type2.get_precision() - type2.get_scale();
        const int16_t prec = MIN(OB_MAX_DECIMAL_POSSIBLE_PRECISION, MAX(type.get_precision(), MAX(intd1, intd2) + type.get_scale()));
        type.set_precision(static_cast<ObPrecision>(prec));
      }
    }
    type.set_length(MAX(type1.get_length(), type2.get_length()));
    if (OB_SUCC(ret)) {
      type1.set_calc_meta(type.get_obj_meta());
      type1.set_calc_accuracy(type.get_accuracy());
      type2.set_calc_meta(type.get_obj_meta());
      type2.set_calc_accuracy(type.get_accuracy());
    if (type1.has_result_flag(NOT_NULL_FLAG) || type2.has_result_flag(NOT_NULL_FLAG)) {
        type.set_result_flag(NOT_NULL_FLAG);
      }
    }
  }

  return ret;
}

int ObExprIfNull::calc_ifnull_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // ifnull(arg0, arg1);
  ObDatum *arg0 = NULL;
  ObDatum *arg1 = NULL;
  // MySQL ifnull是短路的
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg0))) {
    LOG_WARN("eval arg0 failed", K(ret));
  } else if (!arg0->is_null()) {
    res_datum.set_datum(*arg0);
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, arg1))) {
    LOG_WARN("eval arg1 failed", K(ret));
  } else {
    res_datum.set_datum(*arg1);
  }
  return ret;
}

int ObExprIfNull::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_ifnull_expr;
  rt_expr.eval_vector_func_ = eval_ifnull_vector;
  return ret;
}

OB_INLINE void ObExprIfNull::build_arg0_skip(const ObIVector *arg0_vec,
                                            VectorFormat fmt,
                                            const ObBitVector &skip,
                                            ObBitVector &skip_bmp,
                                            const EvalBound &bound)
{
  skip_bmp.deep_copy(skip, bound.start(), bound.end());
  if (fmt == VEC_UNIFORM || fmt == VEC_UNIFORM_CONST) {
    for (int64_t i = bound.start(); i < bound.end(); ++i) {
      if (!skip.at(i) && arg0_vec->is_null(i)) {
        skip_bmp.set(i);
      }
    }
  } else {
    const ObBitVector *nulls = nullptr;
    if (OB_NOT_NULL(nulls = static_cast<const ObBitmapNullVectorBase *>(arg0_vec)->get_nulls())) {
      skip_bmp.bit_or(*nulls, bound);
    }
  }
}

OB_INLINE void ObExprIfNull::build_arg1_skip(const ObBitVector &skip,
                                            ObBitVector &skip_bmp,
                                            const EvalBound &bound)
{
  // Transform skip_bmp: ~(skip_bmp ^ skip) for arg1 evaluation
  skip_bmp.bit_calculate(skip_bmp, skip, bound, BitXnorOp());
}

int ObExprIfNull::eval_ifnull_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval arg0 failed", K(ret));
  } else {
    ObIVector *res_vec = expr.get_vector(ctx);
    const ObIVector *arg0_vec = expr.args_[0]->get_vector(ctx);
    const VectorFormat arg0_format = arg0_vec->get_format();
    ObBitVector &skip_bmp = expr.get_pvt_skip(ctx);
    // Step 1: Build skip bitmap for arg0 (rows where arg0 is null)
    build_arg0_skip(arg0_vec, arg0_format, skip, skip_bmp, bound);
    if (OB_FAIL(res_vec->from_vector_shallow(arg0_vec, &skip_bmp, bound.start(), bound.end()))) {
      LOG_WARN("copy arg0 failed", K(ret));
    } else {
      // Step 2: Transform skip_bmp for arg1 evaluation (rows where arg0 is null)
      build_arg1_skip(skip, skip_bmp, bound);
      EvalBound arg1_bound = skip_bmp.accumulate_bit_cnt(bound) > 0
                           ? EvalBound(bound.batch_size(), bound.start(), bound.end(), false)
                           : bound;
      if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip_bmp, arg1_bound))) {
        LOG_WARN("eval arg1 failed", K(ret));
      } else {
        const ObIVector *arg1_vec = expr.args_[1]->get_vector(ctx);
        if (OB_FAIL(res_vec->from_vector_shallow(arg1_vec, &skip_bmp, bound.start(), bound.end()))) {
          LOG_WARN("copy arg1 failed", K(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
