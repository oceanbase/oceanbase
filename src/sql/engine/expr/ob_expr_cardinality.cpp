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
#include "sql/engine/expr/ob_expr_cardinality.h"
#include "objit/common/ob_item_type.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_schema_struct.h"
#include "pl/ob_pl_allocator.h"
#include "sql/engine/expr/ob_expr_multiset.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
}
}

ObExprCardinality::ObExprCardinality(ObIAllocator &alloc)
  :ObFuncExprOperator(alloc, T_FUN_SYS_CARDINALITY, N_CARDINALITY, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprCardinality::~ObExprCardinality()
{
}

int ObExprCardinality::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_OP_SET) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  }
  return ret;
}

int ObExprCardinality::calc_result_type1(ObExprResType &type,
                                     ObExprResType &type1,
                                     ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (type1.is_ext()) {
      type.set_number();
      type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].scale_);
      type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].precision_);
      type.set_calc_type(common::ObNumberType);
    } else {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt",
              K(ret), K(type1));
    }
  } else {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt",
              K(ret), K(type1));
  }
  return ret;
}

int ObExprCardinality::cg_expr(ObExprCGCtx &expr_cg_ctx,
                               const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_card;
  return ret;
}

int ObExprCardinality::eval_card(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  ObObj obj;
  ObObj result;
  CK (1 == expr.arg_cnt_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr.eval_param_value(ctx, datum))) {
    LOG_WARN("failed to eval param value", K(ret));
  } else if (OB_FAIL(datum->to_obj(obj, expr.args_[0]->obj_meta_))) {
    LOG_WARN("failed to convert tp obj", K(ret));
  } else if (lib::is_oracle_mode() && obj.get_meta().is_ext()) {
    pl::ObPLCollection *c1 = reinterpret_cast<pl::ObPLCollection *>(obj.get_ext());
    int64_t elem_count = 0;
    if (OB_ISNULL(c1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("union udt failed due to null udt", K(ret), K(obj));
    } else if (pl::PL_NESTED_TABLE_TYPE != c1->get_type()) {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt",
                K(ret), K(c1->get_type()));
    } else {
      if (!c1->is_inited()) {
        result.set_null();
      } else {
        elem_count = c1->get_actual_count();
        number::ObNumber cardinality;
        if (OB_FAIL(cardinality.from(elem_count, ctx.exec_ctx_.get_allocator()))) {
          LOG_WARN("generator cardinality result failed.", K(ret));
        } else {
          result.set_number(cardinality);
        }
      }
      if (OB_SUCC(ret)) {
        OZ(res.from_obj(result, expr.obj_datum_map_));
        OZ(expr.deep_copy_datum(ctx, res));
      }
    }
  }
  return ret;
}
