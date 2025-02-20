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
#include "src/sql/engine/expr/ob_expr_lob_utils.h"
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
  int ret = OB_SUCCESS;
  type.set_tinyint();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  type.set_result_flag(NOT_NULL_FLAG);
  if (2 == param_num && lib::is_oracle_mode()) { // updating(expr) in oracle mode
    ObExprResType tmp_type;
    ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
    OZ (params.push_back(&dml[1]));
    OZ (aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), params, tmp_type));
    OZ (deduce_string_param_calc_type_and_charset(*type_ctx.get_session(), tmp_type, params));
  }
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
    OV (OB_NOT_NULL(update_col), OB_INVALID_ARGUMENT);
  }
  OX (expr_datum.set_bool(false));
  if (OB_SUCC(ret)) {
    if (dml->get_int() == static_cast<int64_t>(ctx.exec_ctx_.get_dml_event())) {
      if (static_cast<int64_t>(DE_UPDATING) == dml->get_int() && NULL != update_col) {
        ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
        common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
        ObString col;
        bool find = false;
        if (!ob_is_text_tc(expr.args_[1]->datum_meta_.type_)) {
          col = update_col->get_string();
        } else {
          col = update_col->get_string();
          OZ (sql::ObTextStringHelper::read_prefix_string_data(ctx,
                                                               *update_col,
                                                               expr.args_[1]->datum_meta_,
                                                               expr.args_[1]->obj_meta_.has_lob_header(),
                                                               &temp_allocator,
                                                               col));
        }
        const ColContentFixedArray *update_cols = ctx.exec_ctx_.get_update_columns();
        ObString converted_str;
        OZ (ObExprUtil::convert_string_collation(
          col, expr.args_[1]->obj_meta_.get_collation_type(), converted_str, CS_TYPE_UTF8MB4_BIN, temp_allocator));
        for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(update_cols) && !find && i < update_cols->count(); i++) {
          find = (!update_cols->at(i).is_implicit_ && 0 == converted_str.case_compare(update_cols->at(i).column_name_));
        }
        OX (expr_datum.set_bool(find));
      } else {
        expr_datum.set_bool(true); // success
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
