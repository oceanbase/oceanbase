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
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_pl_associative_index.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_spi.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObExprPLAssocIndex, ObExprOperator),
                    info_.for_write_,
                    info_.out_of_range_set_err_,
                    info_.parent_expr_type_);

ObExprPLAssocIndex::ObExprPLAssocIndex(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_FUN_PL_ASSOCIATIVE_INDEX, N_PL_ASSOCIATIVE_INDEX, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                  false, INTERNAL_IN_ORACLE_MODE),
    info_()
{
}

ObExprPLAssocIndex::~ObExprPLAssocIndex()
{
}

void ObExprPLAssocIndex::reset()
{
  ObExprOperator::reset();
  info_ = Info();
}

int ObExprPLAssocIndex::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_FUN_PL_ASSOCIATIVE_INDEX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
    info_ = static_cast<const ObExprPLAssocIndex &>(other).info_;
  }
  return ret;
}

int ObExprPLAssocIndex::calc_result_type2(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(type1);
  UNUSED(type2);
  type.set_int();
  type.set_precision(DEFAULT_SCALE_FOR_INTEGER);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  return ret;
}


int ObExprPLAssocIndex::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  const auto &assoc_idx_expr = static_cast<const ObPLAssocIndexRawExpr &>(raw_expr);
  CK(2 == rt_expr.arg_cnt_);
  if (OB_SUCC(ret)) {
    Info info;
    info.for_write_ = assoc_idx_expr.get_write();
    info.out_of_range_set_err_ = assoc_idx_expr.get_out_of_range_set_err();
    info.parent_expr_type_ = assoc_idx_expr.get_parent_type();

    rt_expr.extra_ = info.v_;
    rt_expr.eval_func_ = &eval_assoc_idx;
  }
  return ret;
};

int ObExprPLAssocIndex::eval_assoc_idx(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSEDx(expr, ctx, expr_datum);
  return ret;
}

}  // namespace sql
}  // namespace oceanbase


