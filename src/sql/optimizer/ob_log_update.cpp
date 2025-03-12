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
#include "ob_log_update.h"
#include "sql/optimizer/ob_join_order.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int ObLogUpdate::get_plan_item_info(PlanText &plan_text,
                                    ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogDelUpd::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(print_table_infos(ObString::make_string("table_columns"),
                                  buf,
                                  buf_len,
                                  pos,
                                  type))) {
      LOG_WARN("failed to print table infos", K(ret));
    } else if (need_barrier()) {
      ret = BUF_PRINTF(", ");
      ret = BUF_PRINTF("with_barrier");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(",\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("update("))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */ }
    bool has_assign = false;
    for (int64_t k = 0; OB_SUCC(ret) && k < get_index_dml_infos().count(); ++k) {
      const IndexDMLInfo *info = get_index_dml_infos().at(k);
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("info is null", K(ret));
      } else if (!info->is_primary_index_ && !is_pdml()) {
        continue;
      } else {
        const int64_t N = info->assignments_.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
          OZ(BUF_PRINTF("["));
          CK(OB_NOT_NULL(info->assignments_.at(i).column_expr_));
          OZ(info->assignments_.at(i).column_expr_->get_name(buf, buf_len, pos, type));
          OZ(BUF_PRINTF("="));
          CK(OB_NOT_NULL(info->assignments_.at(i).expr_));
          OZ(info->assignments_.at(i).expr_->get_name(buf, buf_len, pos, type));
          OZ(BUF_PRINTF("]"));
          OZ(BUF_PRINTF(", "));
          has_assign = true;
        }
      }
    }
    if (OB_SUCC(ret) && has_assign) {
      pos = pos - 2;
    }
    OZ(BUF_PRINTF(")"));
    if (OB_SUCC(ret) && get_das_dop() > 0) {
      ret = BUF_PRINTF(", das_dop=%ld", this->get_das_dop());
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item. special_predicates_len_);
  }
  return ret;
}

const char *ObLogUpdate::get_name() const
{
  const char *name = NULL;
  if (is_pdml()) {
    if (is_index_maintenance()) {
      name = "INDEX UPDATE";
    } else {
      name = ObLogDelUpd::get_name();
    }
  } else if (is_multi_part_dml()) {
    name = "DISTRIBUTED UPDATE";
  } else {
    name = ObLogDelUpd::get_name();
  }
  return name;
}

int ObLogUpdate::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_plan()), K(ret));
  } else if (OB_FAIL(ObLogDelUpd::inner_get_op_exprs(all_exprs, true))) {
    LOG_WARN("failed to add parent need expr", K(ret));
  }
  return ret;
}

int ObLogUpdate::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(is_dml_fixed_expr(expr, get_index_dml_infos(), is_fixed))) {
    LOG_WARN("failed to check is my fixed expr", K(ret));
  }
  return ret;
}

int ObLogUpdate::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else {
    double op_cost = 0.0;
    if (OB_FAIL(inner_est_cost(child->get_card(), op_cost))) {
      LOG_WARN("failed to get update cost", K(ret));
    } else {
      set_op_cost(op_cost);
      set_cost(child->get_cost() + get_op_cost());
      set_card(child->get_card());
    }
  }
  return ret;
}

int ObLogUpdate::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else {
    double child_card = child->get_card();
    double child_cost = child->get_cost();
    if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
      LOG_WARN("failed to re est exchange cost", K(ret));
    } else if (OB_FAIL(inner_est_cost(child_card, op_cost))) {
      LOG_WARN("failed to get update cost", K(ret));
    } else {
      cost = child_cost + op_cost;
      card = child_card;
    }
  }
  return ret;
}

int ObLogUpdate::inner_est_cost(double child_card, double &op_cost)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()));
  } else if (OB_FAIL(inner_est_cost(get_plan()->get_optimizer_context(),
                                    get_index_dml_infos(),
                                    child_card,
                                    op_cost))) {
    LOG_WARN("failed to get update cost", K(ret));
  }
  return ret;
}

int ObLogUpdate::inner_est_cost(const ObOptimizerContext &opt_ctx,
                                const ObIArray<IndexDMLInfo*> &index_infos,
                                const double child_card,
                                double &op_cost)
{
  int ret = OB_SUCCESS;
  ObDelUpCostInfo cost_info(0,0,0);
  cost_info.affect_rows_ = child_card;
  cost_info.index_count_ = index_infos.count();
  const IndexDMLInfo *update_dml_info = nullptr;
  if (OB_UNLIKELY(cost_info.index_count_ <= 0) ||
      OB_ISNULL(update_dml_info = index_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(update_dml_info));
  } else if (OB_FALSE_IT(cost_info.constraint_count_ = update_dml_info->ck_cst_exprs_.count())) {
  } else if (OB_FAIL(ObOptEstCost::cost_update(cost_info, op_cost, opt_ctx))) {
    LOG_WARN("failed to get update cost", K(ret));
  }
  return ret;
}

int ObLogUpdate::generate_rowid_expr_for_trigger()
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && !has_instead_of_trigger()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
      bool has_trg = false;
      IndexDMLInfo *dml_info = get_index_dml_infos().at(i);
      if (OB_ISNULL(dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dml info is null", K(ret), K(dml_info));
      } else if (!dml_info->is_primary_index_) {
        // do nothing
      } else if (OB_FAIL(check_has_trigger(dml_info->ref_table_id_, has_trg))) {
        LOG_WARN("failed to check has trigger", K(ret));
      } else if (!has_trg) {
        // do nothing
      } else if (OB_FAIL(generate_old_rowid_expr(*dml_info))) {
        LOG_WARN("failed to generate rowid expr", K(ret));
      } else if (OB_FAIL(generate_update_new_rowid_expr(*dml_info))) {
        LOG_WARN("failed to generate new rowid expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogUpdate::generate_part_id_expr_for_foreign_key(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
    IndexDMLInfo *dml_info = get_index_dml_infos().at(i);
    if (OB_ISNULL(dml_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dml info is null", K(ret), K(dml_info));
    } else if (!dml_info->is_primary_index_) {
      // do nothing
    } else if (OB_FAIL(generate_fk_lookup_part_id_expr(*dml_info))) {
      LOG_WARN("failed to generate lookup part expr for foreign key", K(ret));
    } else if (OB_FAIL(convert_update_new_fk_lookup_part_id_expr(all_exprs, *dml_info))) {
      LOG_WARN("failed to convert lookup part expr for foreign key", K(ret));
    }
  }
  return ret;
}

int ObLogUpdate::generate_multi_part_partition_id_expr()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
    if (OB_ISNULL(get_index_dml_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index dml info is null", K(ret));
    } else if (OB_FAIL(generate_old_calc_partid_expr(*get_index_dml_infos().at(i)))) {
      LOG_WARN("failed to generate calc partid expr", K(ret));
    } else if (OB_FAIL(generate_update_new_calc_partid_expr(*get_index_dml_infos().at(i)))) {
      LOG_WARN("failed to generate new calc partid expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogUpdate::op_is_update_pk_with_dop(bool &is_update)
{
  int ret = OB_SUCCESS;
  is_update = false;
  if (!index_dml_infos_.empty()) {
    IndexDMLInfo *index_dml_info = index_dml_infos_.at(0);
    if (OB_ISNULL(index_dml_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), K(index_dml_infos_));
    } else if (index_dml_info->is_update_primary_key_ && (is_pdml() || get_das_dop() > 1)) {
      is_update = true;
    }
  }
  return ret;
}
