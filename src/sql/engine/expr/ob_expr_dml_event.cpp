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

#define USING_LOG_PREFIX SQL_EXE
#include "sql/engine/expr/ob_expr_dml_event.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/dml/ob_table_modify_op.h"

namespace oceanbase
{
namespace sql
{
ObExprDmlEvent::ObExprDmlEvent(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_DML_EVENT,
                         N_DML_EVENT,
                         ONE_OR_TWO,
                         VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION,
                         false,
                         INTERNAL_IN_ORACLE_MODE)
{
}
ObExprDmlEvent::~ObExprDmlEvent(){}

int ObExprDmlEvent::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *dml,
                                      int64_t param_num,
                                      common::ObExprTypeCtx &type_ctx) const
{
  UNUSEDx(dml, type_ctx, param_num);
  int ret = OB_SUCCESS;
  type.set_tinyint();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  type.set_result_flag(NOT_NULL_FLAG);
  return ret;
}

int ObExprDmlEvent::cg_expr(ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSEDx(op_cg_ctx, raw_expr);
  rt_expr.eval_func_ = &calc_dml_event;
  return ret;
}

int ObExprDmlEvent::calc_dml_event(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *dml = NULL;
  ObDatum *update_col = NULL;
  OZ (expr.args_[0]->eval(ctx, dml));
  OV (OB_NOT_NULL(dml));
  if (2 == expr.arg_cnt_) {
    OZ (expr.args_[1]->eval(ctx, update_col));
    OV (OB_NOT_NULL(update_col) && !update_col->is_null(), OB_INVALID_ARGUMENT);
  }
  OX (expr_datum.set_bool(false));
  if (OB_SUCC(ret)) {
    if (dml->get_int() == static_cast<int64_t>(ctx.exec_ctx_.get_dml_event())) {
      if (static_cast<int64_t>(DE_UPDATING) == dml->get_int() && NULL != update_col) {
        bool find = false;
        const ObString &col = update_col->get_string();
        const ColContentFixedArray *update_cols = ctx.exec_ctx_.get_update_columns();
        for (int64_t i = 0; OB_SUCC(ret) && !find && i < update_cols->count(); i++) {
          find = (!update_cols->at(i).is_implicit_ && 0 == col.case_compare(update_cols->at(i).column_name_));
        }
        expr_datum.set_bool(find);
      } else {
        expr_datum.set_bool(true); // success
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
