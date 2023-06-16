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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_err_log.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/optimizer/ob_del_upd_log_plan.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
ObLogErrLog::ObLogErrLog(ObLogPlan &plan)
  : ObLogicalOperator(plan),
    err_log_define_(),
    del_upd_stmt_(NULL)
{}

const char* ObLogErrLog::get_name() const
{
  return "ERROR LOGGING";
}

int ObLogErrLog::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *first_child = NULL;
  if (OB_ISNULL(first_child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first child is null", K(ret));
  } else {
    set_op_cost(0);
    set_cost(first_child->get_cost());
    set_card(first_child->get_card());
  }
  return ret;
}

int ObLogErrLog::get_plan_item_info(PlanText &plan_text,
                                    ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get base plan item info", K(ret));
  } else {
    ObString &name = get_err_log_define().err_log_table_name_;
    BUF_PRINT_OB_STR(name.ptr(),
                     name.length(),
                     plan_item.object_alias_,
                     plan_item.object_alias_len_);
  }
  return ret;
}

uint64_t ObLogErrLog::hash(uint64_t seed) const
{
  seed = do_hash(err_log_define_.err_log_database_name_, seed);
  seed = do_hash(err_log_define_.err_log_table_name_, seed);
  seed = ObLogicalOperator::hash(seed);
  return seed;
}

int ObLogErrLog::get_err_log_type(stmt::StmtType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(del_upd_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("del_upd_stmt_ is null", K(ret));
  } else {
    type = del_upd_stmt_->get_stmt_type();
  }
  return ret;
}

int ObLogErrLog::extract_err_log_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(del_upd_stmt_) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(del_upd_stmt_), K(get_stmt()), K(ret));
  } else if (OB_FAIL(ObLogDelUpd::generate_errlog_info(*del_upd_stmt_,
                                                       get_err_log_define()))) {
    LOG_WARN("failed to generate errlog info", K(ret));
  } else if (del_upd_stmt_->is_insert_stmt()) {
    ObSEArray<ObRawExpr *, 4> tmp;
    const ObInsertStmt *ins_stmt = static_cast<const ObInsertStmt *>(del_upd_stmt_);
    const TableItem *table = NULL;
    ObSEArray<ObRawExpr *, 4> select_exprs;
    ObSEArray<ObRawExpr *, 4> column_exprs;
    ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
    if (OB_UNLIKELY(!ins_stmt->value_from_select()) ||
        OB_ISNULL(table = ins_stmt->get_table_item(ins_stmt->get_from_item(0))) ||
        OB_UNLIKELY(!table->is_generated_table()) ||
        OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid params", K(ret), K(ins_stmt->value_from_select()), K(table));
    } else if (OB_FAIL(ins_stmt->get_column_exprs(table->table_id_,
                                                  column_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(
                         column_exprs,
                         static_cast<const ObSelectStmt&>(*get_stmt()),
                         select_exprs))) {
      LOG_WARN("failed to convert column expr to select", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(column_exprs, select_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(copier.copy_on_replace(get_err_log_define().err_log_value_exprs_,
                                              tmp))) {
      LOG_WARN("failed to copy on replace exprs", K(ret));
    } else if (OB_FAIL(get_err_log_define().err_log_value_exprs_.assign(tmp))) {
      LOG_WARN("failed to assign err log values", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_err_log_define().err_log_value_exprs_.count(); ++i) {
        ObRawExpr *&expr = get_err_log_define().err_log_value_exprs_.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(ret));
        } else if (expr->get_expr_type() == T_FUN_COLUMN_CONV) {
          expr = expr->get_param_expr(ObExprColumnConv::VALUE_EXPR);
        }
      }
    }
  }
  return ret;
}

int ObLogErrLog::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(all_exprs, get_err_log_define().err_log_value_exprs_))) {
    LOG_WARN("failed to add err log value expr into context", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObLogErrLog::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, get_err_log_define().err_log_value_exprs_))) {
    LOG_WARN("failed to replace err log value exprs", K(ret));
  }
  return ret;
}

int ObLogErrLog::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  is_fixed = ObOptimizerUtil::find_item(get_err_log_define().err_log_value_exprs_, expr);
  return OB_SUCCESS;
}
