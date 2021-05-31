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
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_optimizer_util.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

ObLogForUpdate::ObLogForUpdate(ObLogPlan& plan)
    : ObLogDelUpd(plan), skip_locked_(false), wait_ts_(-1), lock_tables_(), rowkeys_()
{}

const char* ObLogForUpdate::get_name() const
{
  if (is_multi_part_dml()) {
    return "MULTI FOR UPDATE";
  } else {
    return "FOR UPDATE";
  }
}

int ObLogForUpdate::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  ObLogForUpdate* for_update = NULL;
  out = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogSubplanScan", K(ret));
  } else if (OB_ISNULL(for_update = static_cast<ObLogForUpdate*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperaotr* to ObLogForUpdate*", K(ret));
  } else if (OB_FAIL(for_update->lock_tables_.assign(lock_tables_))) {
    LOG_WARN("failed to assign lock tables", K(ret));
  } else if (OB_FAIL(for_update->rowkeys_.assign(rowkeys_))) {
    LOG_WARN("failed to assign rowkey exprs", K(ret));
  } else {
    for_update->set_wait_ts(get_wait_ts());
    for_update->set_skip_locked(is_skip_locked());
    out = for_update;
  }
  return ret;
}

int ObLogForUpdate::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  UNUSED(type);
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(", lock tables"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < lock_tables_.count(); ++i) {
    uint64_t table_id = lock_tables_.at(i);
    TableItem* table = NULL;
    if (OB_ISNULL(table = stmt->get_table_item_by_id(table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is not found", K(ret), K(table_id));
    } else if (OB_FAIL(BUF_PRINTF("%c%.*s%c",
                   i == 0 ? '(' : ' ',
                   table->get_table_name().length(),
                   table->get_table_name().ptr(),
                   i == lock_tables_.count() - 1 ? ')' : ','))) {
      LOG_WARN("failed to print lock table name", K(ret));
    }
  }
  return ret;
}

int ObLogForUpdate::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> dep_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < lock_tables_.count(); ++i) {
    ObSEArray<ObColumnRefRawExpr*, 4> rowkeys;
    ObSEArray<ObRawExpr*, 4> part_keys;
    ObRawExpr* calc_part_expr = NULL;
    if (OB_FAIL(get_rowkey_exprs(lock_tables_.at(i), rowkeys))) {
      LOG_WARN("failed to get rowkey exprs", K(ret));
    } else if (OB_FAIL(append(dep_exprs, rowkeys))) {
      LOG_WARN("failed to get rowkey exprs", K(ret));
    } else if (!is_multi_part_dml()) {
      // do nothing
    } else if (OB_FAIL(get_part_columns(lock_tables_.at(i), part_keys))) {
      LOG_WARN("failed to get partition columns", K(ret));
    } else if (OB_FAIL(append(dep_exprs, part_keys))) {
      LOG_WARN("failed to get partition keys", K(ret));
    } else if (OB_FAIL(create_calc_part_expr(lock_tables_.at(i), calc_part_expr))) {
      LOG_WARN("failed to create calc part expr", K(ret));
    } else if (OB_FAIL(calc_part_id_exprs_.push_back(calc_part_expr))) {
      LOG_WARN("failed to append calc part id expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dep_exprs.count(); ++i) {
    if (OB_ISNULL(dep_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dependant expr is null", K(ret));
    } else {
      dep_exprs.at(i)->set_explicited_reference();
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_exprs_to_ctx(ctx, dep_exprs))) {
    LOG_WARN("failed to add exprs to context", K(ret));
  }
  return ret;
}

int ObLogForUpdate::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> dep_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < lock_tables_.count(); ++i) {
    ObSEArray<ObColumnRefRawExpr*, 4> rowkeys;
    ObSEArray<ObRawExpr*, 4> part_keys;
    if (OB_FAIL(get_rowkey_exprs(lock_tables_.at(i), rowkeys))) {
      LOG_WARN("failed to get rowkey exprs", K(ret));
    } else if (OB_FAIL(append(dep_exprs, rowkeys))) {
      LOG_WARN("failed to append input exprs", K(ret));
    } else if (!is_multi_part_dml()) {
      // do nothing
    } else if (OB_FAIL(get_part_columns(lock_tables_.at(i), part_keys))) {
      LOG_WARN("failed to get partition columns", K(ret));
    } else if (OB_FAIL(append(dep_exprs, part_keys))) {
      LOG_WARN("failed to append partition keys", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dep_exprs.count(); ++i) {
    if (OB_ISNULL(dep_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part expr is null", K(ret));
    } else if (OB_FAIL(checker.check(*dep_exprs.at(i)))) {
      LOG_WARN("failed to check part key expr", K(ret));
    }
  }
  return ret;
}

uint64_t ObLogForUpdate::hash(uint64_t seed) const
{
  seed = do_hash(skip_locked_, seed);
  seed = do_hash(wait_ts_, seed);
  HASH_ARRAY(lock_tables_, seed);
  seed = ObLogicalOperator::hash(seed);
  return seed;
}

int ObLogForUpdate::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* first_child = NULL;
  if (OB_ISNULL(first_child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first child is null", K(ret));
  } else {
    set_op_cost(0);
    set_cost(first_child->get_cost());
    set_card(first_child->get_card());
    set_width(first_child->get_width());
  }
  return ret;
}

int ObLogForUpdate::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Child is null", K(ret));
  } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
    LOG_WARN("Failed to set op ordering", K(ret));
  }
  return ret;
}

bool ObLogForUpdate::modify_multi_tables() const
{
  return lock_tables_.count() > 1;
}

uint64_t ObLogForUpdate::get_target_table_id() const
{
  uint64_t target_table_id = OB_INVALID_ID;
  if (lock_tables_.count() == 1) {
    target_table_id = lock_tables_.at(0);
  }
  return target_table_id;
}

int ObLogForUpdate::add_for_update_table(uint64_t table_id, const ObIArray<ObColumnRefRawExpr*>& keys)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_tables_.push_back(table_id))) {
    LOG_WARN("failed to push back table id", K(ret));
  } else if (OB_FAIL(append(rowkeys_, keys))) {
    LOG_WARN("failed to append rowkey exprs", K(ret));
  }
  return ret;
}

int ObLogForUpdate::get_table_columns(const uint64_t table_id, ObIArray<ObColumnRefRawExpr*>& table_cols) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(get_stmt()->get_column_exprs(table_id, table_cols))) {
    LOG_WARN("failed to get column exprs", K(ret));
  }
  for (int64_t i = table_cols.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_ISNULL(table_cols.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table column is null", K(ret), K(table_cols.at(i)));
    } else if (!IS_SHADOW_COLUMN(table_cols.at(i)->get_column_id())) {
      // do nothing
    } else if (OB_FAIL(table_cols.remove(i))) {
      LOG_WARN("failed to remove column expr", K(ret));
    }
  }
  return ret;
}

int ObLogForUpdate::get_rowkey_exprs(const uint64_t table_id, ObIArray<ObColumnRefRawExpr*>& rowkey) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys_.count(); ++i) {
    ObColumnRefRawExpr* col = NULL;
    if (OB_ISNULL(col = rowkeys_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey expr is null", K(ret));
    } else if (col->get_table_id() != table_id) {
      // do nothing
    } else if (OB_FAIL(rowkey.push_back(col))) {
      LOG_WARN("failed to push back col expr", K(ret));
    }
  }
  return ret;
}

int ObLogForUpdate::get_part_columns(const uint64_t table_id, ObIArray<ObRawExpr*>& part_cols) const
{
  int ret = OB_SUCCESS;
  const TableItem* table = NULL;
  ObRawExpr* part_expr = NULL;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(table = get_stmt()->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(table));
  } else if (NULL == (part_expr = get_stmt()->get_part_expr(table->table_id_, table->ref_id_))) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, part_cols))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (NULL == (part_expr = get_stmt()->get_subpart_expr(table->table_id_, table->ref_id_))) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, part_cols))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  }
  return ret;
}

int ObLogForUpdate::create_calc_part_expr(const uint64_t table_id, ObRawExpr*& calc_part_expr)
{
  int ret = OB_SUCCESS;
  const TableItem* table = NULL;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(table = get_stmt()->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret), K(get_stmt()), K(table));
  } else if (OB_FAIL(gen_calc_part_id_expr(table_id, table->ref_id_, calc_part_expr))) {
    LOG_WARN("failed to gen calc part id expr", K(ret));
  }
  return ret;
}

int ObLogForUpdate::get_calc_part_expr(const uint64_t table_id, ObRawExpr*& calc_part_expr) const
{
  int ret = OB_SUCCESS;
  calc_part_expr = NULL;
  if (OB_UNLIKELY(lock_tables_.count() != calc_part_id_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc part expr array is invalid", K(ret), K(lock_tables_.count()), K(calc_part_id_exprs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < lock_tables_.count(); ++i) {
    if (lock_tables_.at(i) == table_id) {
      calc_part_expr = calc_part_id_exprs_.at(i);
      break;
    }
  }
  return ret;
}

int ObLogForUpdate::is_rowkey_nullable(const uint64_t table_id, bool& is_nullable) const
{
  int ret = OB_SUCCESS;
  is_nullable = false;
  if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(get_stmt(), table_id, is_nullable))) {
    LOG_WARN("failed to check is table on null side", K(ret));
  }
  return ret;
}

int ObLogForUpdate::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else if (OB_FAIL(raw_exprs.append(child->get_output_exprs()))) {
    LOG_WARN("failed to append child output exprs", K(ret));
  } else if (OB_FAIL(raw_exprs.append(get_output_exprs()))) {
    LOG_WARN("failed to append output exprs", K(ret));
  } else if (OB_FAIL(raw_exprs.append(calc_part_id_exprs_))) {
    LOG_WARN("failed to append calc part id expr", K(ret));
  }
  return ret;
}
