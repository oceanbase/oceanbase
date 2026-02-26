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
 * This file contains implementation for _st_asmvtgeom.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_gtid.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprGTID::ObExprGTID(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name, int32_t param_num) :
  ObFuncExprOperator(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION){};

ObExprGTID::~ObExprGTID(){};

ObExprGTIDSubset::ObExprGTIDSubset(common::ObIAllocator &alloc) :
  ObExprGTID(alloc, T_FUN_SYS_GTID_SUBSET, N_GTID_SUBSET, 2){};

int ObExprGTIDSubset::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  CK (2 == rt_expr.arg_cnt_);
  const ObExpr *set1 = rt_expr.args_[0];
  const ObExpr *set2 = rt_expr.args_[1];
  CK (OB_NOT_NULL(set1), OB_NOT_NULL(set2));

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprGTIDSubset::eval_subset;
  }

  return ret;
}

int ObExprGTIDSubset::calc_result_type2(ObExprResType &type,
                                        ObExprResType &type1,
                                        ObExprResType &type2,
                                        common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  // result
  type.set_type(ObIntType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);

  // set1
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(ObCharset::get_system_collation());

  // set2
  type2.set_calc_type(ObVarcharType);
  type2.set_calc_collation_type(ObCharset::get_system_collation());
  return ret;
}

int ObExprGTIDSubset::eval_subset(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  // mock
  result.set_null();
  return OB_SUCCESS;
}

ObExprGTIDSubtract::ObExprGTIDSubtract(common::ObIAllocator &alloc) :
  ObExprGTID(alloc, T_FUN_SYS_GTID_SUBTRACT, N_GTID_SUBTRACT, 2){};

int ObExprGTIDSubtract::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  CK (2 == rt_expr.arg_cnt_);
  const ObExpr *set1 = rt_expr.args_[0];
  const ObExpr *set2 = rt_expr.args_[1];
  CK (OB_NOT_NULL(set1), OB_NOT_NULL(set2));

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprGTIDSubtract::eval_subtract;
  }

  return ret;
}

int ObExprGTIDSubtract::calc_result_type2(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  // result
  type.set_type(ObIntType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);

  // set1
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(ObCharset::get_system_collation());

  // set2
  type2.set_calc_type(ObVarcharType);
  type2.set_calc_collation_type(ObCharset::get_system_collation());
  return ret;
}

int ObExprGTIDSubtract::eval_subtract(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  // mock
  result.set_null();
  return OB_SUCCESS;
}

ObExprWaitForExecutedGTIDSet::ObExprWaitForExecutedGTIDSet(common::ObIAllocator &alloc) :
  ObExprGTID(alloc, T_FUN_SYS_WAIT_FOR_EXECUTED_GTID_SET, N_WAIT_FOR_EXECUTED_GTID_SET, ONE_OR_TWO){};

int ObExprWaitForExecutedGTIDSet::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  CK (1 == rt_expr.arg_cnt_ || 2 == rt_expr.arg_cnt_);
  const ObExpr *set = rt_expr.args_[0];
  CK (OB_NOT_NULL(set));
  if (2 == rt_expr.arg_cnt_) {
    const ObExpr *timeout = rt_expr.args_[1];
    CK (OB_NOT_NULL(timeout));
  }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprWaitForExecutedGTIDSet::eval_wait_for_executed_gtid_set;
  }

  return ret;
}

int ObExprWaitForExecutedGTIDSet::calc_result_typeN(ObExprResType &type,
                                                    ObExprResType *types,
                                                    int64_t param_num,
                                                    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  // result
  type.set_type(ObIntType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);

  // gtid_set
  types[0].set_calc_type(ObVarcharType);
  types[0].set_calc_collation_type(ObCharset::get_system_collation());

  // timeout
  if (2 == param_num) {
    type.set_type(ObIntType);
  }

  return ret;
}

int ObExprWaitForExecutedGTIDSet::eval_wait_for_executed_gtid_set(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  // mock
  result.set_null();
  return OB_SUCCESS;
}


ObExprWaitUntilSQLThreadAfterGTIDs::ObExprWaitUntilSQLThreadAfterGTIDs(common::ObIAllocator &alloc) :
  ObExprGTID(alloc, T_FUN_SYS_WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS, N_WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS, MORE_THAN_ZERO){};

int ObExprWaitUntilSQLThreadAfterGTIDs::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  CK (1 == rt_expr.arg_cnt_ || 2 == rt_expr.arg_cnt_ || 3 == rt_expr.arg_cnt_);
  const ObExpr *set = rt_expr.args_[0];
  CK (OB_NOT_NULL(set));
  if (2 <= rt_expr.arg_cnt_) {
    const ObExpr *timeout = rt_expr.args_[1];
    CK (OB_NOT_NULL(timeout));
    if (3 == rt_expr.arg_cnt_) {
      const ObExpr *channel = rt_expr.args_[2];
      CK (OB_NOT_NULL(channel));
    }
  }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprWaitUntilSQLThreadAfterGTIDs::eval_wait_until_sql_thread_after_gtids;
  }

  return ret;
}

int ObExprWaitUntilSQLThreadAfterGTIDs::calc_result_typeN(ObExprResType &type,
                                                          ObExprResType *types,
                                                          int64_t param_num,
                                                          common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  // result
  type.set_type(ObIntType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);

  // gtid_set
  types[0].set_calc_type(ObVarcharType);
  types[0].set_calc_collation_type(ObCharset::get_system_collation());

  if (2 <= param_num) {
    // timeout
    types[1].set_type(ObIntType);
    if (3 == param_num) {
      // channel
      types[2].set_type(ObIntType);
    }
  }
  return ret;
}

int ObExprWaitUntilSQLThreadAfterGTIDs::eval_wait_until_sql_thread_after_gtids(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  // mock
  result.set_null();
  return OB_SUCCESS;
}
}  // namespace sql
}  // namespace oceanbase
