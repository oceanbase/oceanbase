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
#include <stdint.h>
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_server_locality_cache.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_mv_table_scan.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_intersect_route_policy.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_sequence.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_select_into.h"
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_log_table_lookup.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_log_del_upd.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_function_table.h"
#include "sql/optimizer/ob_raw_expr_connectby_level_visitor.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "storage/ob_partition_service.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_log_temp_table_insert.h"
#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_log_temp_table_transformation.h"
#include "sql/optimizer/ob_px_resource_analyzer.h"
#include "common/ob_smart_call.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::sql::log_op_def;
using share::schema::ObColumnSchemaV2;
using share::schema::ObSchemaGetterGuard;
using share::schema::ObTableSchema;

#include "sql/optimizer/ob_join_property.map"
static const char* ExplainColumnName[] = {"ID", "OPERATOR", "NAME", "EST. ROWS", "COST"};

ObLogPlan::ObLogPlan(ObOptimizerContext& ctx, const ObDMLStmt* stmt)
    : optimizer_context_(ctx),
      allocator_(ctx.get_allocator()),
      stmt_(stmt),
      log_op_factory_(allocator_),
      candidates_(),
      phy_plan_type_(OB_PHY_PLAN_UNINITIALIZED),
      location_type_(OB_PHY_PLAN_UNINITIALIZED),
      group_replaced_exprs_(),
      query_ref_(NULL),
      root_(NULL),
      sql_text_(),
      hash_value_(0),
      subplan_infos_(),
      onetime_exprs_(),
      acs_index_infos_(),
      autoinc_params_(),
      join_order_(NULL),
      predicate_selectivities_(),
      affected_last_insert_id_(false),
      expected_worker_count_(0),
      multi_stmt_rowkey_pos_(),
      equal_sets_(),
      all_exprs_(allocator_),
      max_op_id_(OB_INVALID_ID),
      require_local_execution_(false),
      est_sel_info_(ctx, const_cast<ObDMLStmt*>(stmt)),
      is_subplan_scan_(false),
      const_exprs_()
{}

ObLogPlan::~ObLogPlan()
{
  if (NULL != join_order_) {
    join_order_->~ObJoinOrder();
    join_order_ = NULL;
  }

  for (int64_t i = 0; i < subplan_infos_.count(); ++i) {
    if (NULL != subplan_infos_.at(i)) {
      subplan_infos_.at(i)->~SubPlanInfo();
      subplan_infos_.at(i) = NULL;
    } else { /* Do nothing */
    }
  }
}

double ObLogPlan::get_optimization_cost()
{
  double opt_cost = 0.0;
  if (OB_NOT_NULL(root_)) {
    opt_cost = root_->get_cost();
  }
  return opt_cost;
}

int ObLogPlan::get_current_best_plan(ObLogicalOperator*& best_plan)
{
  int ret = OB_SUCCESS;
  best_plan = NULL;
  if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    LOG_TRACE("succeed to get best plan", K(best_plan->get_card()), K(best_plan->get_cost()), K(best_plan->get_type()));
  }
  return ret;
}

int ObLogPlan::set_final_plan_root(ObLogicalOperator* best_plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(best_plan), K(ret));
  } else {
    set_plan_root(best_plan);
    best_plan->mark_is_plan_root();
  }
  return ret;
}

int planText::init()
{
  int ret = OB_SUCCESS;
#undef KEY_DEF
#define KEY_DEF(identifier, name) name
  static const char* names[] = {KEYS_DEF};
#undef KEY_DEF
  for (int i = 0; i < Max_Plan_Column; i++) {
    formatter.column_width[i] = (int)strlen(names[i]);
    formatter.column_name[i] = names[i];
  }
  // formatter.num_of_columns = (EXPLAIN_SIMPLE == format) ? SIMPLE_COLUMN_NUM : Max_Plan_Column;
  if (EXPLAIN_BASIC == format) {
    formatter.num_of_columns = SIMPLE_COLUMN_NUM;
  } else if (EXPLAIN_PLANREGRESS == format) {
    formatter.num_of_columns = SIMPLE_COLUMN_NUM;
  } else {
    formatter.num_of_columns = Max_Plan_Column;
  }
  is_inited_ = true;
  return ret;
}

int ObLogPlan::plan_tree_copy(ObLogicalOperator* src, ObLogicalOperator*& dst)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(src));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    ObLogicalOperator* tmp = NULL;
    if (OB_FAIL(src->copy_without_child(tmp))) {
      LOG_WARN("failed to copy_without_child", K(src->get_type()), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < src->get_num_of_child(); i++) {
        ObLogicalOperator* sub_tree = NULL;
        if (OB_FAIL(SMART_CALL(plan_tree_copy(src->get_child(i), sub_tree)))) {
          LOG_WARN("failed to copy plan tree", K(ret));
        } else if (OB_ISNULL(tmp) || OB_ISNULL(sub_tree)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Get unexpected null", K(ret), K(tmp), K(sub_tree));
        } else {
          tmp->set_child(i, sub_tree);
        }
      }
    }
    if (OB_SUCC(ret)) {
      dst = tmp;
    } else { /* Do nothing */
    }
  }
  return ret;
}

#define NEW_LINE "\n"
#define SEPARATOR "|"
#define SPACE " "
#define PLAN_WRAPPER "="
#define LINE_SEPARATOR "-"

int64_t ObLogPlan::to_string(char* buf, const int64_t buf_len, ExplainType type) const
{
  int ret = OB_SUCCESS;
  planText plan(buf, buf_len, type);
  plan.is_oneline_ = false;
  int64_t& pos = plan.pos;
  if (OB_FAIL(plan.init())) {
    databuff_printf(buf, buf_len, pos, "WARN failed to plan.init(), ret=%d", ret);
  } else if (OB_FAIL(const_cast<ObLogPlan*>(this)->plan_tree_traverse(EXPLAIN_COLLECT_WIDTH, &plan))) {
    databuff_printf(buf, buf_len, pos, "WARN failed to traverse plan for collecting width, ret=%d", ret);
  } else {
    int64_t total_width = 0;
    for (int i = 0; OB_SUCC(ret) && i < plan.formatter.num_of_columns; i++) {
      total_width += plan.formatter.column_width[i];
    }
    // adding a separator between each column
    total_width += plan.formatter.num_of_columns + 1;

    // print plan text header line
    for (int i = 0; OB_SUCC(ret) && i < total_width; i++) {
      ret = BUF_PRINTF(PLAN_WRAPPER);
    }
    if (OB_FAIL(ret)) {
      databuff_printf(buf, buf_len, pos, "WARN failed to previous step, ret=%d", ret);
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) { /* Do nothing */
    } else {                                    // print column names
      ret = BUF_PRINTF(SEPARATOR);
    }
    for (int i = 0; OB_SUCC(ret) && i < plan.formatter.num_of_columns; i++) {
      if (OB_FAIL(BUF_PRINTF("%-*s", plan.formatter.column_width[i], ExplainColumnName[i]))) { /* Do nothing */
      } else {
        ret = BUF_PRINTF(SEPARATOR);
      }
    }

    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */
    }

    // print line separator
    for (int i = 0; OB_SUCC(ret) && i < total_width; i++) {
      ret = BUF_PRINTF(LINE_SEPARATOR);
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(NEW_LINE);
    } else { /* Do nothing */
    }

    plan.level = 0;
    if (OB_FAIL(ret)) {
      databuff_printf(buf, buf_len, pos, "WARN failed to previous step, ret=%d", ret);
    } else if (OB_FAIL(const_cast<ObLogPlan*>(this)->plan_tree_traverse(EXPLAIN_WRITE_BUFFER, &plan))) {
      databuff_printf(buf, buf_len, pos, "WARN failed to traverse plan for writing buffer, ret=%d", ret);
    } else {
      // print plan text footer line
      for (int i = 0; OB_SUCC(ret) && i < total_width; i++) {
        ret = BUF_PRINTF(PLAN_WRAPPER);
      }
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(NEW_LINE);
      } else { /* Do nothing */
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {                                       /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("Outputs & filters: "))) {                   /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {                                /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("-------------------------------------"))) { /* Do nothing */
      } else if (FALSE_IT(plan.level = 0)) {                                     /* Do nothing */
      } else if (OB_FAIL(const_cast<ObLogPlan*>(this)->plan_tree_traverse(EXPLAIN_WRITE_BUFFER_OUTPUT, &plan))) {
        databuff_printf(buf, buf_len, pos, "WARN failed to print output exprs, ret=%d", ret);
      } else {
        ret = BUF_PRINTF(NEW_LINE);
      }
    } else { /* Do nothing */
    }

    // print Used Hint
    if (OB_SUCC(ret) && (EXPLAIN_EXTENDED == type)) {
      plan.outline_type_ = USED_HINT;
      if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {                                         /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("Used Hint:"))) {                              /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {                                  /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("-------------------------------------\n"))) { /* Do nothing */
      } else if (OB_FAIL(print_outline(plan))) {
        databuff_printf(buf, buf_len, pos, "WARN failed to print outlien, ret=%d", ret);
      } else {
        ret = BUF_PRINTF(NEW_LINE);
      }
    } else { /* Do nothing */
    }

    // print Outline Data
    if (OB_SUCC(ret) && (EXPLAIN_EXTENDED == type || EXPLAIN_OUTLINE == type)) {
      plan.outline_type_ = OUTLINE_DATA;
      if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {                                         /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("Outline Data:"))) {                           /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {                                  /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("-------------------------------------\n"))) { /* Do nothing */
      } else if (OB_FAIL(print_outline(plan))) {
        databuff_printf(buf, buf_len, pos, "WARN failed to print outlien, ret=%d", ret);
      } else {
        ret = BUF_PRINTF(NEW_LINE);
      }
    } else { /* Do nothing */
    }

    // print log plan type
    if (OB_SUCC(ret) && EXPLAIN_EXTENDED == type) {
      if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {                                         /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("Plan Type:"))) {                              /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {                                  /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("-------------------------------------\n"))) { /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF(ob_plan_type_str(get_phy_plan_type())))) {     /* Do nothing */
      } else {
        ret = BUF_PRINTF(NEW_LINE);
      }
    } else { /* Do nothing */
    }

    // print optimization info
    if (OB_SUCC(ret) && EXPLAIN_EXTENDED == type) {
      if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {                                         /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("Optimization Info:"))) {                      /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {                                  /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("-------------------------------------\n"))) { /* Do nothing */
      } else if (OB_FAIL(const_cast<ObLogPlan*>(this)->plan_tree_traverse(EXPLAIN_INDEX_SELECTION_INFO, &plan))) {
        databuff_printf(buf, buf_len, pos, "WARN failed to print index selection info, ret=%d", ret);
      }
    } else { /* Do nothing */
    }

    // print params
    if (OB_SUCC(ret) && EXPLAIN_EXTENDED == type) {
      if (OB_FAIL(BUF_PRINTF("Parameters"))) {
      } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) {
      } else {
        ret = BUF_PRINTF("-------------------------------------\n");
      }
      const ParamStore* params = NULL;
      if (OB_ISNULL(params = get_optimizer_context().get_params())) {
        ret = OB_ERR_UNEXPECTED;
        databuff_printf(buf, buf_len, pos, "WARN Get unexpected null, ret=%d", ret);
      } else { /* Do nothing */
      }
      for (int64_t i = 0; OB_SUCC(ret) && NULL != params && i < params->count(); i++) {
        pos += params->at(i).to_string(buf + pos, buf_len - pos);
        if (i < params->count() - 1) {
          ret = BUF_PRINTF(", ");
        } else { /* Do nothing */
        }
      }
    } else { /* Do nothing */
    }
  }
  if (OB_FAIL(ret)) {
    databuff_printf(buf, buf_len, pos, "WARN Get unexpected error in to_string, ret=%d", ret);
  } else { /* Do nothing */
  }

  return pos;
}

int ObLogPlan::get_table_items(const ObIArray<FromItem>& from_items, ObIArray<TableItem*>& table_items)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(stmt), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); i++) {
      const FromItem& from_item = from_items.at(i);
      TableItem* temp_table_item = NULL;
      if (!from_item.is_joined_) {
        temp_table_item = stmt->get_table_item_by_id(from_item.table_id_);
      } else {
        temp_table_item = stmt->get_joined_table(from_item.table_id_);
      }
      if (OB_FAIL(table_items.push_back(temp_table_item))) {
        LOG_WARN("failed to push back table item", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogPlan::get_table_ids(TableItem* table_item, ObRelIds& table_ids)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(table_item) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item or stmt", K(ret));
  } else if (!table_item->is_joined_table()) {
    if (OB_FAIL(table_ids.add_member(stmt->get_table_bit_index(table_item->table_id_)))) {
      LOG_WARN("failed to add members", K(ret));
    }
  } else {
    JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
    if (OB_FAIL(ObTransformUtils::get_rel_ids_from_join_table(stmt, joined_table, table_ids))) {
      LOG_WARN("failed to get rel ids from joined table", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::get_table_ids(const ObIArray<uint64_t>& table_ids, ObRelIds& rel_ids)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  int32_t idx = OB_INVALID_INDEX;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    idx = stmt->get_table_bit_index(table_ids.at(i));
    if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item or stmt", K(ret));
    } else if (OB_FAIL(rel_ids.add_member(idx))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::get_table_ids(const ObIArray<TableItem*>& table_items, ObRelIds& table_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    TableItem* item = table_items.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (OB_FAIL(get_table_ids(item, table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::get_table_ids(const ObIArray<ObRawExpr*>& exprs, ObRelIds& table_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr* expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(table_ids.add_members(expr->get_relation_ids()))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::is_joined_table_filter(const ObIArray<TableItem*>& table_items, const ObRawExpr* expr, bool& is_filter)
{
  int ret = OB_SUCCESS;
  is_filter = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr or stmt", K(ret));
  } else if (expr->get_relation_ids().is_empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_filter && i < table_items.count(); ++i) {
      TableItem* item = table_items.at(i);
      ObRelIds joined_table_ids;
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null semi item", K(ret));
      } else if (!item->is_joined_table()) {
        // do nothing
      } else if (OB_FAIL(get_table_ids(item, joined_table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (joined_table_ids.is_superset(expr->get_relation_ids())) {
        is_filter = true;
      }
    }
  }
  return ret;
}

int ObLogPlan::find_inner_conflict_detector(
    const ObIArray<ConflictDetector*>& inner_conflict_detectors, const ObRelIds& rel_ids, ConflictDetector*& detector)
{
  int ret = OB_SUCCESS;
  detector = NULL;
  int64_t N = inner_conflict_detectors.count();
  for (int64_t i = 0; OB_SUCC(ret) && NULL == detector && i < N; ++i) {
    ConflictDetector* temp_detector = inner_conflict_detectors.at(i);
    if (OB_ISNULL(temp_detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null detector", K(ret));
    } else if (temp_detector->join_info_.join_type_ != INNER_JOIN) {
      // do nothing
    } else if (temp_detector->join_info_.table_set_.equal(rel_ids)) {
      detector = temp_detector;
    }
  }
  return ret;
}

int ObLogPlan::satisfy_associativity_rule(const ConflictDetector& left, const ConflictDetector& right, bool& is_satisfy)

{
  int ret = OB_SUCCESS;
  if (!ASSOC_PROPERTY[left.join_info_.join_type_][right.join_info_.join_type_]) {
    is_satisfy = false;
  } else if ((LEFT_OUTER_JOIN == left.join_info_.join_type_ || FULL_OUTER_JOIN == left.join_info_.join_type_) &&
             LEFT_OUTER_JOIN == right.join_info_.join_type_) {
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(right.join_info_.on_condition_,
            left.R_DS_.is_subset(right.L_DS_) ? left.R_DS_ : right.L_DS_,
            is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ && FULL_OUTER_JOIN == right.join_info_.join_type_) {
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
            left.join_info_.on_condition_, left.R_DS_.is_subset(right.L_DS_) ? left.R_DS_ : right.L_DS_, is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    } else if (!is_satisfy) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(right.join_info_.on_condition_,
                   left.R_DS_.is_subset(right.L_DS_) ? left.R_DS_ : right.L_DS_,
                   is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else {
    is_satisfy = true;
  }
  return ret;
}

int ObLogPlan::satisfy_left_asscom_rule(const ConflictDetector& left, const ConflictDetector& right, bool& is_satisfy)

{
  int ret = OB_SUCCESS;
  if (!L_ASSCOM_PROPERTY[left.join_info_.join_type_][right.join_info_.join_type_]) {
    is_satisfy = false;
  } else if (LEFT_OUTER_JOIN == left.join_info_.join_type_ && FULL_OUTER_JOIN == right.join_info_.join_type_) {
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(left.join_info_.on_condition_, left.L_DS_, is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ && LEFT_OUTER_JOIN == right.join_info_.join_type_) {
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(right.join_info_.on_condition_, left.L_DS_, is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ && FULL_OUTER_JOIN == right.join_info_.join_type_) {
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(left.join_info_.on_condition_, left.L_DS_, is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    } else if (!is_satisfy) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                   right.join_info_.on_condition_, left.L_DS_, is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else {
    is_satisfy = true;
  }
  LOG_TRACE("succeed to check l-asscom", K(left), K(right), K(is_satisfy));
  return ret;
}

int ObLogPlan::satisfy_right_asscom_rule(const ConflictDetector& left, const ConflictDetector& right, bool& is_satisfy)

{
  int ret = OB_SUCCESS;
  if (!R_ASSCOM_PROPERTY[left.join_info_.join_type_][right.join_info_.join_type_]) {
    is_satisfy = false;
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ && FULL_OUTER_JOIN == right.join_info_.join_type_) {
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(left.join_info_.on_condition_, right.R_DS_, is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    } else if (!is_satisfy) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                   right.join_info_.on_condition_, right.R_DS_, is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else {
    is_satisfy = true;
  }
  LOG_TRACE("succeed to check r-asscom", K(left), K(right), K(is_satisfy));
  return ret;
}

int ObLogPlan::add_conflict_rule(
    const ObRelIds& left, const ObRelIds& right, ObIArray<std::pair<ObRelIds, ObRelIds>>& rules)
{
  int ret = OB_SUCCESS;
  ObRelIds* left_handle_rule = NULL;
  ObRelIds* right_handle_rule = NULL;
  bool find = false;
  for (int64_t i = 0; !find && i < rules.count(); ++i) {
    if (rules.at(i).first.equal(left) && rules.at(i).second.equal(right)) {
      find = true;
    } else if (rules.at(i).first.equal(left)) {
      left_handle_rule = &rules.at(i).second;
    } else if (rules.at(i).second.equal(right)) {
      right_handle_rule = &rules.at(i).first;
    }
  }
  if (find) {
    // do nothing
  } else if (NULL != left_handle_rule) {
    if (OB_FAIL(left_handle_rule->add_members(right))) {
      LOG_WARN("failed to add members", K(ret));
    }
  } else if (NULL != right_handle_rule) {
    if (OB_FAIL(right_handle_rule->add_members(left))) {
      LOG_WARN("failed to add members", K(ret));
    }
  } else {
    std::pair<ObRelIds, ObRelIds> rule;
    if (OB_FAIL(rule.first.add_members(left))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(rule.second.add_members(right))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(rules.push_back(rule))) {
      LOG_WARN("failed to push back rule", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::generate_conflict_rule(ConflictDetector* parent, ConflictDetector* child, bool is_left_child,
    ObIArray<std::pair<ObRelIds, ObRelIds>>& rules)
{
  int ret = OB_SUCCESS;
  bool is_satisfy = false;
  ObRelIds ids;
  if (OB_ISNULL(parent) || OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null detector", K(ret));
  } else if (is_left_child) {
    LOG_TRACE("generate left child conflict rule for ", K(*parent), K(*child));
    // check assoc(o^a, o^b)
    if (OB_FAIL(satisfy_associativity_rule(*child, *parent, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy assoc");
    } else if (OB_FAIL(ids.intersect(child->L_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      // CR += T(right(o^a)) -> T(left(o^a))
      if (OB_FAIL(add_conflict_rule(child->R_DS_, child->L_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else if (OB_FAIL(add_conflict_rule(child->R_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict1", K(rules));
      }
    } else {
      // CR += T(right(o^a)) -> T(left(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->R_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else if (OB_FAIL(add_conflict_rule(child->R_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict2", K(rules));
      }
    }
    // check l-asscom(o^a, o^b)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(satisfy_left_asscom_rule(*child, *parent, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy l-asscom");
    } else if (OB_FAIL(ids.intersect(child->R_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      // CR += T(left(o^a)) -> T(right(o^a))
      if (OB_FAIL(add_conflict_rule(child->L_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict3", K(rules));
      }
    } else {
      // CR += T(left(o^a)) -> T(right(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->L_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict4", K(rules));
      }
    }
  } else {
    LOG_TRACE("generate right child conflict rule for ", K(*parent), K(*child));
    // check assoc(o^b, o^a)
    if (OB_FAIL(satisfy_associativity_rule(*parent, *child, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy assoc");
    } else if (OB_FAIL(ids.intersect(child->R_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      // CR += T(left(o^a)) -> T(right(o^a))
      if (OB_FAIL(add_conflict_rule(child->L_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict5", K(rules));
      }
    } else {
      // CR += T(left(o^a)) -> T(right(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->L_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict6", K(rules));
      }
    }
    // check r-asscom(o^b, o^a)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(satisfy_right_asscom_rule(*parent, *child, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy r-asscom");
    } else if (OB_FAIL(ids.intersect(child->L_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      // CR += T(right(o^a)) -> T(left(o^a))
      if (OB_FAIL(add_conflict_rule(child->R_DS_, child->L_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict7", K(rules));
      }
    } else {
      // CR += T(right(o^a)) -> T(left(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->R_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict8", K(rules));
      }
    }
  }
  return ret;
}

int ObLogPlan::get_base_table_items(ObDMLStmt& stmt, const ObIArray<TableItem*>& table_items,
    const ObIArray<SemiInfo*>& semi_infos, ObIArray<TableItem*>& base_tables)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    TableItem* item = table_items.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!item->is_joined_table()) {
      ret = base_tables.push_back(item);
    } else {
      JoinedTable* joined_table = static_cast<JoinedTable*>(item);
      for (int64_t j = 0; OB_SUCC(ret) && j < joined_table->single_table_ids_.count(); ++j) {
        TableItem* table = stmt.get_table_item_by_id(joined_table->single_table_ids_.at(j));
        ret = base_tables.push_back(table);
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
    if (OB_ISNULL(semi_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null semi info", K(ret));
    } else {
      TableItem* table = stmt.get_table_item_by_id(semi_infos.at(i)->right_table_id_);
      ret = base_tables.push_back(table);
    }
  }
  return ret;
}

int ObLogPlan::generate_join_orders()
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObSEArray<ObRawExpr*, 8> quals;
  ObSEArray<SemiInfo*, 4> semi_infos;
  ObSEArray<TableItem*, 4> table_items;
  JoinOrderArray base_level;
  int64_t join_level = 0;
  common::ObArray<JoinOrderArray> join_rels;
  ObSEArray<TableItem*, 8> base_tables;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(stmt), K(ret));
  } else if (OB_FAIL(semi_infos.assign(stmt->get_semi_infos()))) {
    LOG_WARN("failed to assign semi infos", K(ret));
  } else if (OB_FAIL(get_table_items(stmt->get_from_items(), table_items))) {
    LOG_WARN("failed to get table items", K(ret));
  } else if (stmt->is_insert_stmt()) {
    /*do nothing*/
  } else if (OB_FAIL(append(quals, get_stmt()->get_condition_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(quals, get_pushdown_filters()))) {
    LOG_WARN("failed to append exprs", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_base_table_items(*stmt, table_items, semi_infos, base_tables))) {
      LOG_WARN("failed to flatten table items", K(ret));
    } else if (OB_FAIL(generate_base_level_join_order(base_tables, base_level))) {
      LOG_WARN("fail to generate base level join order", K(ret));
    } else if (OB_FAIL(pre_process_quals(table_items, semi_infos, quals))) {
      LOG_WARN("failed to distribute special quals", K(ret));
    } else if (OB_FAIL(generate_conflict_detectors(table_items, semi_infos, quals, base_level))) {
      LOG_WARN("failed to generate conflict detectors", K(ret));
    } else {
      join_level = base_level.count();
      if (OB_UNLIKELY(join_level < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected join level", K(ret), K(join_level));
      } else if (OB_FAIL(join_rels.prepare_allocate(join_level))) {
        LOG_WARN("failed to prepare allocate join rels", K(ret));
      } else if (OB_FAIL(join_rels.at(0).assign(base_level))) {
        LOG_WARN("failed to assign base level join order", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < join_level; ++i) {
    if (OB_ISNULL(join_rels.at(0).at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join_rels_.at(0).at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(join_rels.at(0).at(i)->generate_base_paths())) {
      LOG_WARN("failed to generate base path", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_leading_info(table_items, semi_infos))) {
      LOG_WARN("failed to get leading hint info", K(ret));
    } else if (OB_FAIL(init_bushy_tree_info(table_items))) {
      LOG_WARN("failed to init bushy tree infos", K(ret));
    } else if (join_rels.at(0).count() <= DEFAULT_SEARCH_SPACE_RELS || !leading_tables_.is_empty()) {
      if (OB_FAIL(generate_join_levels_with_DP(join_rels, join_level))) {
        LOG_WARN("failed to generate join levels with dynamic program", K(ret));
      }
    } else {
      if (OB_FAIL(generate_join_levels_with_linear(join_rels, table_items, join_level))) {
        LOG_WARN("failed to generate join levels with linear", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (1 != join_rels.at(join_level - 1).count()) {
      ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
      LOG_WARN("No final JoinOrder generated", K(ret), K(join_rels.at(join_level - 1).count()));
    } else if (OB_ISNULL(join_order_ = join_rels.at(join_level - 1).at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null join order", K(ret), K(join_order_));
    } else if (OB_UNLIKELY(join_order_->get_interesting_paths().empty())) {
      ret = OB_ERR_NO_PATH_GENERATED;
      LOG_WARN("No final join path generated", K(ret), K(*join_order_));
    } else {
      LOG_TRACE("succeed to generate join order", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::generate_base_level_join_order(
    const ObIArray<TableItem*>& table_items, ObIArray<ObJoinOrder*>& base_level)
{
  int ret = OB_SUCCESS;
  ObJoinOrder* this_jo = NULL;
  int64_t N = table_items.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_ISNULL(table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table item", K(ret), K(i));
    } else if (OB_ISNULL(this_jo = create_join_order(ACCESS))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate an ObJoinOrder", K(ret));
    } else if (OB_FAIL(this_jo->init_base_join_order(table_items.at(i)))) {
      LOG_WARN("fail to generate the base rel", K(ret), K(*table_items.at(i)));
      this_jo->~ObJoinOrder();
      this_jo = NULL;
    } else if (OB_FAIL(base_level.push_back(this_jo))) {
      LOG_WARN("fail to push back an ObJoinOrder", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogPlan::split_or_quals(const ObIArray<TableItem*>& table_items, ObIArray<ObRawExpr*>& quals)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> new_quals;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    if (OB_FAIL(split_or_quals(table_items, quals.at(i), new_quals))) {
      LOG_WARN("failed to split or quals", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(quals, new_quals))) {
      LOG_WARN("failed to append quals", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::split_or_quals(const ObIArray<TableItem*>& table_items, ObRawExpr* qual, ObIArray<ObRawExpr*>& new_quals)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  bool is_filter = false;
  if (OB_ISNULL(qual)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null qual", K(ret));
  } else if (T_OP_OR != qual->get_expr_type()) {
    // do nothing
  } else if (!qual->has_flag(CNT_SUB_QUERY) && OB_FAIL(is_joined_table_filter(table_items, qual, is_filter))) {
    LOG_WARN("failed to check is joined table filter", K(ret));
  } else if (is_filter) {
  } else {
    ObOpRawExpr* or_qual = static_cast<ObOpRawExpr*>(qual);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_items.count(); ++j) {
      TableItem* table_item = table_items.at(j);
      table_ids.reuse();
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (OB_FAIL(get_table_ids(table_item, table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (!table_ids.overlap(or_qual->get_relation_ids())) {
        // do nothing
      } else if (qual->has_flag(CNT_SUB_QUERY) || (!table_ids.is_superset(or_qual->get_relation_ids()))) {
        ret = try_split_or_qual(table_ids, *or_qual, new_quals);
      }
    }
  }
  return ret;
}

int ObLogPlan::pre_process_quals(
    const ObIArray<TableItem*>& table_items, const ObIArray<SemiInfo*>& semi_infos, ObIArray<ObRawExpr*>& quals)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> normal_quals;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(stmt));
  }
  // 1. where conditions
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    ObRawExpr* qual = quals.at(i);
    bool can_distribute = false;
    if (OB_ISNULL(qual)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null qual", K(ret));
    } else if (qual->has_flag(CNT_ROWNUM)) {
      ret = add_rownum_expr(qual);
    } else if (qual->has_flag(CNT_RAND_FUNC)) {
      ret = add_random_expr(qual);
    } else if (qual->has_flag(CNT_USER_VARIABLE)) {
      ret = add_user_var_filter(qual);
    } else if (qual->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(distribute_onetime_subquery(qual, can_distribute))) {
        LOG_WARN("failed to check qual can pushdown", K(ret));
      } else if (!can_distribute) {
        if (OB_FAIL(split_or_quals(table_items, qual, quals))) {
          LOG_WARN("failed to split or quals", K(ret));
        } else {
          ret = add_subquery_filter(qual);
        }
      } else {
        ret = normal_quals.push_back(qual);
      }
    } else if (ObOptimizerUtil::has_hierarchical_expr(*qual)) {
      ret = normal_quals.push_back(qual);
    } else if (0 == qual->get_relation_ids().num_members() || is_upper_stmt_column_ref(*qual, *stmt)) {
      ret = add_startup_filter(qual);
    } else {
      ret = normal_quals.push_back(qual);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(quals.assign(normal_quals))) {
      LOG_WARN("failed to assign quals", K(ret));
    }
  }
  // 2. on conditions
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    ret = pre_process_quals(table_items.at(i));
  }
  // 3. semi conditions
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
    ret = pre_process_quals(semi_infos.at(i));
  }
  return ret;
}

int ObLogPlan::pre_process_quals(SemiInfo* semi_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null semi info", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_info->semi_conditions_.count(); ++i) {
    bool can_distribute = true;
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(expr = semi_info->semi_conditions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(expr));
    } else if (expr->has_flag(CNT_ROWNUM) || expr->has_flag(CNT_RAND_FUNC) || expr->has_flag(CNT_USER_VARIABLE)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr in semi condition", K(ret), K(*expr));
    } else if (expr->has_flag(CNT_SUB_QUERY) && OB_FAIL(distribute_subquery_qual(expr, can_distribute))) {
      LOG_WARN("failed to distribute subquery expr", K(ret));
    } else if (!can_distribute) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("correlated subquery in outer join condition is not supported", KPC(expr), K(ret));
    } else {
      semi_info->semi_conditions_.at(i) = expr;
    }
  }
  return ret;
}

int ObLogPlan::pre_process_quals(TableItem* table_item)
{
  int ret = OB_SUCCESS;
  JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table_item->is_joined_table()) {
    // do nothing
  } else if (OB_FAIL(SMART_CALL(pre_process_quals(joined_table->left_table_)))) {
    LOG_WARN("failed to distribute special quals", K(ret));
  } else if (OB_FAIL(SMART_CALL(pre_process_quals(joined_table->right_table_)))) {
    LOG_WARN("failed to distribute special quals", K(ret));
  } else {
    if (FULL_OUTER_JOIN == joined_table->joined_type_ &&
        !ObOptimizerUtil::has_equal_join_conditions(joined_table->join_conditions_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("full outer join without equal join conditions is not supported now", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->join_conditions_.count(); ++i) {
      bool can_distribute = true;
      ObRawExpr* expr = NULL;
      if (OB_ISNULL(expr = joined_table->join_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), K(expr));
      } else if (expr->has_flag(CNT_SUB_QUERY) && OB_FAIL(distribute_subquery_qual(expr, can_distribute))) {
        LOG_WARN("failed to distribute subquery expr", K(ret));
      } else if (!can_distribute) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("correlated subquery in outer join condition is not supported", KPC(expr), K(ret));
      } else {
        joined_table->join_conditions_.at(i) = expr;
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_conflict_detectors(const ObIArray<TableItem*>& table_items,
    const ObIArray<SemiInfo*>& semi_infos, ObIArray<ObRawExpr*>& quals, ObIArray<ObJoinOrder*>& baserels)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  ObSEArray<ConflictDetector*, 8> semi_join_detectors;
  ObSEArray<ConflictDetector*, 8> inner_join_detectors;
  LOG_TRACE("start to generate conflict detector", K(table_items), K(semi_infos), K(quals));
  conflict_detectors_.reuse();
  if (OB_FAIL(get_table_ids(table_items, table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(generate_inner_join_detectors(table_items, quals, baserels, inner_join_detectors))) {
    LOG_WARN("failed to generate inner join detectors", K(ret));
  } else if (OB_FAIL(generate_semi_join_detectors(semi_infos, table_ids, inner_join_detectors, semi_join_detectors))) {
    LOG_WARN("failed to generate semi join detectors", K(ret));
  } else if (OB_FAIL(append(conflict_detectors_, semi_join_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append(conflict_detectors_, inner_join_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else {
    LOG_TRACE("succeed to generate confilct detectors", K(semi_join_detectors), K(inner_join_detectors));
  }
  return ret;
}

int ObLogPlan::generate_semi_join_detectors(const ObIArray<SemiInfo*>& semi_infos, ObRelIds& left_rel_ids,
    const ObIArray<ConflictDetector*>& inner_join_detectors, ObIArray<ConflictDetector*>& semi_join_detectors)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> right_rel_ids;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
    right_rel_ids.reuse();
    SemiInfo* info = semi_infos.at(i);
    ConflictDetector* detector = NULL;
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null semi info", K(ret));
      // 1. create conflict detector
    } else if (OB_FAIL(ConflictDetector::build_confict(get_allocator(), detector))) {
      LOG_WARN("failed to build conflict detector", K(ret));
    } else if (OB_ISNULL(detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null conflict detector", K(ret));
    } else if (OB_FAIL(detector->L_DS_.add_members(left_rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(*stmt, info->right_table_id_, right_rel_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(detector->R_DS_.add_members(right_rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(semi_join_detectors.push_back(detector))) {
      LOG_WARN("failed to push back detector", K(ret));
    } else {
      detector->join_info_.join_type_ = info->join_type_;
      // 2. add equal join conditions
      ObRawExpr* expr = NULL;
      for (int64_t j = 0; OB_SUCC(ret) && j < info->semi_conditions_.count(); ++j) {
        if (OB_ISNULL(expr = info->semi_conditions_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(expr));
        } else if (OB_FAIL(detector->join_info_.table_set_.add_members(expr->get_relation_ids()))) {
          LOG_WARN("failed to add members", K(ret));
        } else if (OB_FAIL(detector->join_info_.where_condition_.push_back(expr))) {
          LOG_WARN("failed to push back exprs", K(ret));
        } else if (expr->has_flag(IS_JOIN_COND) &&
                   OB_FAIL(detector->join_info_.equal_join_condition_.push_back(expr))) {
          LOG_WARN("failed to push back qual", K(ret));
        }
      }
      // 3. add other infos to conflict detector
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(detector->L_TES_.intersect(detector->join_info_.table_set_, detector->L_DS_))) {
        LOG_WARN("failed to generate L-TES", K(ret));
      } else if (OB_FAIL(detector->R_TES_.intersect(detector->join_info_.table_set_, detector->R_DS_))) {
        LOG_WARN("failed to generate R-TES", K(ret));
      } else if (detector->join_info_.table_set_.overlap(detector->L_DS_) &&
                 detector->join_info_.table_set_.overlap(detector->R_DS_)) {
        detector->is_degenerate_pred_ = false;
        detector->is_commutative_ = COMM_PROPERTY[detector->join_info_.join_type_];
      } else {
        detector->is_degenerate_pred_ = true;
        detector->is_commutative_ = COMM_PROPERTY[detector->join_info_.join_type_];
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < inner_join_detectors.count(); ++j) {
      if (OB_FAIL(generate_conflict_rule(detector, inner_join_detectors.at(j), true, detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_inner_join_detectors(const ObIArray<TableItem*>& table_items, ObIArray<ObRawExpr*>& quals,
    ObIArray<ObJoinOrder*>& baserels, ObIArray<ConflictDetector*>& inner_join_detectors)
{
  int ret = OB_SUCCESS;
  ObSEArray<ConflictDetector*, 8> outer_join_detectors;
  ObSEArray<ObRawExpr*, 4> all_table_filters;
  ObSEArray<ObRawExpr*, 4> table_filters;
  ObRelIds table_ids;
  if (OB_FAIL(split_or_quals(table_items, quals))) {
    LOG_WARN("failed to split or quals", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    table_filters.reuse();
    table_ids.reuse();
    if (OB_FAIL(get_table_ids(table_items.at(i), table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < quals.count(); ++j) {
      ObRawExpr* expr = quals.at(j);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!expr->get_relation_ids().is_subset(table_ids)) {
        // do nothing
      } else if (OB_FAIL(table_filters.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(all_table_filters.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_outer_join_detectors(table_items.at(i), table_filters, baserels, outer_join_detectors))) {
        LOG_WARN("failed to generate outer join detectors", K(ret));
      }
    }
  }
  ObRawExpr* expr = NULL;
  ConflictDetector* detector = NULL;
  ObSEArray<ObRawExpr*, 4> join_conditions;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    if (OB_ISNULL(expr = quals.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (ObOptimizerUtil::find_item(all_table_filters, expr)) {
      // do nothing
    } else if (OB_FAIL(join_conditions.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(find_inner_conflict_detector(inner_join_detectors, expr->get_relation_ids(), detector))) {
      LOG_WARN("failed to find conflict detector", K(ret));
    } else if (NULL == detector) {
      if (OB_FAIL(ConflictDetector::build_confict(get_allocator(), detector))) {
        LOG_WARN("failed to build conflict detector", K(ret));
      } else if (OB_ISNULL(detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detector", K(ret));
      } else if (OB_FAIL(detector->join_info_.where_condition_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (expr->has_flag(IS_JOIN_COND) && OB_FAIL(detector->join_info_.equal_join_condition_.push_back(expr))) {
        LOG_WARN("failed to push back qual", K(ret));
      } else if (OB_FAIL(detector->join_info_.table_set_.add_members(expr->get_relation_ids()))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(inner_join_detectors.push_back(detector))) {
        LOG_WARN("failed to push back detector", K(ret));
      } else {
        detector->is_degenerate_pred_ = false;
        detector->is_commutative_ = COMM_PROPERTY[INNER_JOIN];
        detector->join_info_.join_type_ = INNER_JOIN;
      }
    } else if (OB_FAIL(detector->join_info_.where_condition_.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (expr->has_flag(IS_JOIN_COND) && OB_FAIL(detector->join_info_.equal_join_condition_.push_back(expr))) {
      LOG_WARN("failed to push back qual", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inner_join_detectors.count(); ++i) {
    ConflictDetector* inner_detector = inner_join_detectors.at(i);
    const ObRelIds& table_set = inner_detector->join_info_.table_set_;
    if (OB_ISNULL(inner_detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null detector", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < table_items.count(); ++j) {
      table_ids.reuse();
      if (OB_FAIL(get_table_ids(table_items.at(j), table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (!table_ids.overlap(table_set)) {
        // do nothing
      } else if (OB_FAIL(inner_detector->L_DS_.add_members(table_ids))) {
        LOG_WARN("failed to generate R-TES", K(ret));
      } else if (OB_FAIL(inner_detector->R_DS_.add_members(table_ids))) {
        LOG_WARN("failed to generate R-TES", K(ret));
      }
    }
    //对于inner join来说，满足交换律，所以不需要区分L_TES、R_TES
    //为了方便之后统一applicable算法，L_TES、R_TES都等于SES
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inner_detector->L_TES_.add_members(table_set))) {
      LOG_WARN("failed to generate L-TES", K(ret));
    } else if (OB_FAIL(inner_detector->R_TES_.add_members(table_set))) {
      LOG_WARN("failed to generate R-TES", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < outer_join_detectors.count(); ++j) {
      ConflictDetector* outer_detector = outer_join_detectors.at(j);
      if (OB_ISNULL(outer_detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detector", K(ret));
      } else if (!IS_OUTER_OR_CONNECT_BY_JOIN(outer_detector->join_info_.join_type_)) {
      } else if (OB_FAIL(generate_conflict_rule(inner_detector, outer_detector, true, inner_detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      } else if (OB_FAIL(generate_conflict_rule(inner_detector, outer_detector, false, inner_detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(inner_join_detectors, outer_join_detectors))) {
      LOG_WARN("failed to append detectors", K(ret));
    } else if (OB_FAIL(generate_cross_product_detector(table_items, join_conditions, inner_join_detectors))) {
      LOG_WARN("failed to generate cross product detector", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::generate_outer_join_detectors(TableItem* table_item, ObIArray<ObRawExpr*>& table_filter,
    ObIArray<ObJoinOrder*>& baserels, ObIArray<ConflictDetector*>& outer_join_detectors)
{
  int ret = OB_SUCCESS;
  JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table_item->is_joined_table()) {
    if (OB_FAIL(distribute_quals(table_item, table_filter, baserels))) {
      LOG_WARN("failed to distribute table filter", K(ret));
    }
  } else if (INNER_JOIN == joined_table->joined_type_) {
    ObSEArray<TableItem*, 4> table_items;
    ObSEArray<ConflictDetector*, 4> detectors;
    if (OB_FAIL(flatten_inner_join(table_item, table_filter, table_items))) {
      LOG_WARN("failed to flatten inner join", K(ret));
    } else if (OB_FAIL(generate_inner_join_detectors(table_items, table_filter, baserels, detectors))) {
      LOG_WARN("failed to generate inner join detectors", K(ret));
    } else if (OB_FAIL(append(outer_join_detectors, detectors))) {
      LOG_WARN("failed to append detectors", K(ret));
    }
  } else if (OB_FAIL(inner_generate_outer_join_detectors(joined_table, table_filter, baserels, outer_join_detectors))) {
    LOG_WARN("failed to generate outer join detectors", K(ret));
  }
  return ret;
}

int ObLogPlan::distribute_quals(
    TableItem* table_item, const ObIArray<ObRawExpr*>& table_filter, ObIArray<ObJoinOrder*>& baserels)
{
  int ret = OB_SUCCESS;
  ObJoinOrder* cur_rel = NULL;
  ObSEArray<int64_t, 8> relids;
  ObRelIds table_ids;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(table_item));
  } else if (OB_FAIL(get_table_ids(table_item, table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(table_ids.to_array(relids))) {
    LOG_WARN("to_array error", K(ret));
  } else if (1 != relids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect basic table item", K(ret));
  } else if (OB_FAIL(find_base_rel(baserels, relids.at(0), cur_rel))) {
    LOG_WARN("find_base_rel fails", K(ret));
  } else if (OB_ISNULL(cur_rel)) {
    ret = OB_SQL_OPT_ERROR;
    LOG_WARN("failed to distribute qual to rel", K(baserels), K(relids), K(ret));
  } else if (OB_FAIL(append(cur_rel->get_restrict_infos(), table_filter))) {
    LOG_WARN("failed to distribute qual to rel", K(ret));
  }
  return ret;
}

int ObLogPlan::flatten_table_items(
    ObIArray<TableItem*>& table_items, ObIArray<ObRawExpr*>& table_filter, ObIArray<TableItem*>& flatten_table_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    if (OB_FAIL(flatten_inner_join(table_items.at(i), table_filter, flatten_table_items))) {
      LOG_WARN("failed to flatten inner join", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::flatten_inner_join(
    TableItem* table_item, ObIArray<ObRawExpr*>& table_filter, ObIArray<TableItem*>& table_items)
{
  int ret = OB_SUCCESS;
  JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table_item->is_joined_table() || INNER_JOIN != joined_table->joined_type_) {
    ret = table_items.push_back(table_item);
  } else if (OB_FAIL(SMART_CALL(flatten_inner_join(joined_table->left_table_, table_filter, table_items)))) {
    LOG_WARN("failed to faltten inner join", K(ret));
  } else if (OB_FAIL(SMART_CALL(flatten_inner_join(joined_table->right_table_, table_filter, table_items)))) {
    LOG_WARN("failed to faltten inner join", K(ret));
  } else if (OB_FAIL(append(table_filter, joined_table->join_conditions_))) {
    LOG_WARN("failed to append exprs", K(ret));
  }
  return ret;
}

int ObLogPlan::inner_generate_outer_join_detectors(JoinedTable* joined_table, ObIArray<ObRawExpr*>& table_filter,
    ObIArray<ObJoinOrder*>& baserels, ObIArray<ConflictDetector*>& outer_join_detectors)
{
  int ret = OB_SUCCESS;
  ObRelIds table_set;
  ObRelIds left_table_ids;
  ObRelIds right_table_ids;
  ConflictDetector* detector = NULL;
  ObSEArray<ObRawExpr*, 4> left_quals;
  ObSEArray<ObRawExpr*, 4> right_quals;
  ObSEArray<ConflictDetector*, 4> left_detectors;
  ObSEArray<ConflictDetector*, 4> right_detectors;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_FAIL(get_table_ids(joined_table->join_conditions_, table_set))) {
    LOG_WARN("failed to get table ids", K(ret));
    // 1. pushdown where condition
  } else if (OB_FAIL(pushdown_where_filters(joined_table, table_filter, left_quals, right_quals))) {
    LOG_WARN("failed to pushdown where filters", K(ret));
    // 2. pushdown on condition
  } else if (OB_FAIL(pushdown_on_conditions(joined_table, left_quals, right_quals))) {
    LOG_WARN("failed to pushdown on conditions", K(ret));
    // 3. generate left child detectors
  } else if (OB_FAIL(generate_outer_join_detectors(joined_table->left_table_, left_quals, baserels, left_detectors))) {
    LOG_WARN("failed to generate outer join detectors", K(ret));
  } else if (OB_FAIL(append(outer_join_detectors, left_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
    // 4. generate right child detectors
  } else if (OB_FAIL(
                 generate_outer_join_detectors(joined_table->right_table_, right_quals, baserels, right_detectors))) {
    LOG_WARN("failed to generate outer join detectors", K(ret));
  } else if (OB_FAIL(append(outer_join_detectors, right_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
    // 5. create outer join detector
  } else if (OB_FAIL(ConflictDetector::build_confict(get_allocator(), detector))) {
    LOG_WARN("failed to build conflict detector", K(ret));
  } else if (OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null detector", K(ret));
  } else if (OB_FAIL(get_table_ids(joined_table->left_table_, left_table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(get_table_ids(joined_table->right_table_, right_table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(detector->join_info_.table_set_.add_members(table_set))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(detector->L_DS_.add_members(left_table_ids))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(detector->R_DS_.add_members(right_table_ids))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(append(detector->join_info_.on_condition_, joined_table->join_conditions_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(outer_join_detectors.push_back(detector))) {
    LOG_WARN("failed to push back detecotor", K(ret));
  } else {
    detector->is_commutative_ = COMM_PROPERTY[joined_table->joined_type_];
    detector->join_info_.join_type_ = joined_table->joined_type_;
    if (table_set.overlap(left_table_ids) && table_set.overlap(right_table_ids)) {
      detector->is_degenerate_pred_ = false;
    } else {
      detector->is_degenerate_pred_ = true;
    }
    if (CONNECT_BY_JOIN == joined_table->joined_type_) {
      if (OB_FAIL(detector->join_info_.table_set_.add_members(left_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(detector->join_info_.table_set_.add_members(right_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(detector->L_TES_.intersect(detector->join_info_.table_set_, detector->L_DS_))) {
        LOG_WARN("failed to generate L-TES", K(ret));
      } else if (OB_FAIL(detector->R_TES_.intersect(detector->join_info_.table_set_, detector->R_DS_))) {
        LOG_WARN("failed to generate R-TES", K(ret));
      }
    }
    int64_t N = joined_table->join_conditions_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ObRawExpr* expr = joined_table->join_conditions_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (expr->has_flag(IS_JOIN_COND) && OB_FAIL(detector->join_info_.equal_join_condition_.push_back(expr))) {
        LOG_WARN("failed to push back qual", K(ret));
      }
    }
    // 6. generate conflict rules
    for (int64_t i = 0; OB_SUCC(ret) && i < left_detectors.count(); ++i) {
      if (OB_FAIL(generate_conflict_rule(detector, left_detectors.at(i), true, detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_detectors.count(); ++i) {
      if (OB_FAIL(generate_conflict_rule(detector, right_detectors.at(i), false, detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
    // 7. generate conflict detector for table filter
    if (OB_SUCC(ret) && !table_filter.empty()) {
      if (OB_FAIL(ConflictDetector::build_confict(get_allocator(), detector))) {
        LOG_WARN("failed to build conflict detector", K(ret));
      } else if (OB_ISNULL(detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detector", K(ret));
      } else if (OB_FAIL(append(detector->join_info_.where_condition_, table_filter))) {
        LOG_WARN("failed to append expr", K(ret));
      } else if (OB_FAIL(detector->join_info_.table_set_.add_members(left_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(detector->join_info_.table_set_.add_members(right_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(outer_join_detectors.push_back(detector))) {
        LOG_WARN("failed to push back detector", K(ret));
      } else {
        detector->is_degenerate_pred_ = false;
        detector->is_commutative_ = COMM_PROPERTY[INNER_JOIN];
        detector->join_info_.join_type_ = INNER_JOIN;
      }
    }
  }
  return ret;
}

int ObLogPlan::pushdown_where_filters(JoinedTable* joined_table, ObIArray<ObRawExpr*>& table_filter,
    ObIArray<ObRawExpr*>& left_quals, ObIArray<ObRawExpr*>& right_quals)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else {
    ObRelIds left_table_set;
    ObRelIds right_table_set;
    int64_t N = table_filter.count();
    ObSEArray<ObRawExpr*, 4> new_quals;
    ObJoinType join_type = joined_table->joined_type_;
    if (OB_FAIL(get_table_ids(joined_table->left_table_, left_table_set))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(get_table_ids(joined_table->right_table_, right_table_set))) {
      LOG_WARN("failed to get table ids", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      ObRawExpr* qual = table_filter.at(i);
      if (OB_ISNULL(qual)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(qual), K(ret));
      } else if (qual->get_relation_ids().is_empty() && !ObOptimizerUtil::has_hierarchical_expr(*qual)) {
        if (OB_FAIL(left_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (LEFT_OUTER_JOIN == join_type && qual->get_relation_ids().is_subset(left_table_set)) {
        if (OB_FAIL(left_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (RIGHT_OUTER_JOIN == join_type && qual->get_relation_ids().is_subset(right_table_set)) {
        if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(new_quals.push_back(qual))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (T_OP_OR == qual->get_expr_type()) {
        ObOpRawExpr* or_qual = static_cast<ObOpRawExpr*>(qual);
        if (LEFT_OUTER_JOIN == join_type && OB_FAIL(try_split_or_qual(left_table_set, *or_qual, left_quals))) {
          LOG_WARN("failed to split or qual on left table", K(ret));
        } else if (RIGHT_OUTER_JOIN == join_type &&
                   OB_FAIL(try_split_or_qual(right_table_set, *or_qual, right_quals))) {
          LOG_WARN("failed to split or qual on right table", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_filter.assign(new_quals))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::pushdown_on_conditions(
    JoinedTable* joined_table, ObIArray<ObRawExpr*>& left_quals, ObIArray<ObRawExpr*>& right_quals)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else {
    ObRawExpr* qual = NULL;
    ObRelIds left_table_set;
    ObRelIds right_table_set;
    ObSEArray<ObRawExpr*, 4> new_quals;
    ObJoinType join_type = joined_table->joined_type_;
    int64_t N = joined_table->join_conditions_.count();
    if (OB_FAIL(get_table_ids(joined_table->left_table_, left_table_set))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(get_table_ids(joined_table->right_table_, right_table_set))) {
      LOG_WARN("failed to get table ids", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_ISNULL(qual = joined_table->join_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(qual), K(ret));
      } else if (RIGHT_OUTER_JOIN == join_type && !qual->get_relation_ids().is_empty() &&
                 qual->get_relation_ids().is_subset(left_table_set)) {
        if (OB_FAIL(left_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (LEFT_OUTER_JOIN == join_type && !qual->get_relation_ids().is_empty() &&
                 qual->get_relation_ids().is_subset(right_table_set)) {
        if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (CONNECT_BY_JOIN == join_type && !qual->get_relation_ids().is_empty() &&
                 qual->get_relation_ids().is_subset(right_table_set) && !qual->has_flag(CNT_LEVEL) &&
                 !qual->has_flag(CNT_PRIOR) && !qual->has_flag(CNT_ROWNUM)) {
        if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(new_quals.push_back(qual))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (!qual->get_relation_ids().is_empty() && T_OP_OR == qual->get_expr_type()) {
        ObOpRawExpr* or_qual = static_cast<ObOpRawExpr*>(qual);
        if (LEFT_OUTER_JOIN == join_type && OB_FAIL(try_split_or_qual(right_table_set, *or_qual, right_quals))) {
          LOG_WARN("failed to split or qual on right table", K(ret));
        } else if (RIGHT_OUTER_JOIN == join_type && OB_FAIL(try_split_or_qual(left_table_set, *or_qual, left_quals))) {
          LOG_WARN("failed to split or qual on left table", K(ret));
        } else if (CONNECT_BY_JOIN == join_type && !qual->has_flag(CNT_LEVEL) && !qual->has_flag(CNT_ROWNUM) &&
                   !qual->has_flag(CNT_PRIOR) && OB_FAIL(try_split_or_qual(right_table_set, *or_qual, right_quals))) {
          LOG_WARN("failed to split or qual on right table", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(joined_table->join_conditions_.assign(new_quals))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_cross_product_detector(const ObIArray<TableItem*>& table_items, ObIArray<ObRawExpr*>& quals,
    ObIArray<ConflictDetector*>& inner_join_detectors)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  ConflictDetector* detector = NULL;
  if (table_items.count() < 2) {
    // do nothing
  } else if (OB_FAIL(get_table_ids(table_items, table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(ConflictDetector::build_confict(get_allocator(), detector))) {
    LOG_WARN("failed to build conflict detector", K(ret));
  } else if (OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null conflict detector", K(ret));
  } else if (OB_FAIL(detector->L_DS_.add_members(table_ids))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(detector->R_DS_.add_members(table_ids))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(generate_cross_product_conflict_rule(detector, table_items, quals))) {
    LOG_WARN("failed to generate cross product conflict rule", K(ret));
  } else if (OB_FAIL(inner_join_detectors.push_back(detector))) {
    LOG_WARN("failed to push back detector", K(ret));
  } else {
    detector->join_info_.join_type_ = INNER_JOIN;
    detector->is_degenerate_pred_ = true;
    detector->is_commutative_ = true;
  }
  return ret;
}

int ObLogPlan::check_join_info(
    const ObRelIds& left, const ObRelIds& right, const ObIArray<ObRelIds>& base_table_ids, bool& is_connected)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  is_connected = false;
  if (OB_FAIL(table_ids.except(left, right))) {
    LOG_WARN("failed to cal except for rel ids", K(ret));
  } else if (table_ids.is_empty()) {
    is_connected = true;
  } else {
    int64_t N = base_table_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && !is_connected && i < N; ++i) {
      if (table_ids.is_subset(base_table_ids.at(i))) {
        is_connected = true;
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_cross_product_conflict_rule(ConflictDetector* cross_product_detector,
    const ObIArray<TableItem*>& table_items, const ObIArray<ObRawExpr*>& join_conditions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cross_product_detector)) {
    ret = OB_SUCCESS;
    LOG_WARN("unexpect null detector", K(ret));
  } else {
    ObRelIds table_ids;
    bool have_new_connect_info = true;
    ObSEArray<ObRelIds, 8> base_table_ids;
    ObSEArray<ObRelIds, 8> connect_infos;
    LOG_TRACE("start generate cross product conflict rule", K(table_items), K(join_conditions));
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      table_ids.reuse();
      if (OB_FAIL(get_table_ids(table_items.at(i), table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (OB_FAIL(base_table_ids.push_back(table_ids))) {
        LOG_WARN("failed to push back rel ids", K(ret));
      } else if (OB_FAIL(connect_infos.push_back(table_ids))) {
        LOG_WARN("failed to push back rel ids", K(ret));
      }
    }
    ObSqlBitSet<> used_join_conditions;
    while (have_new_connect_info) {
      have_new_connect_info = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions.count(); ++i) {
        ObRawExpr* expr = join_conditions.at(i);
        if (used_join_conditions.has_member(i)) {
          // do nothing
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else {
          ObSqlBitSet<> used_infos;
          ObRelIds connect_tables;
          if (OB_FAIL(connect_tables.add_members(expr->get_relation_ids()))) {
            LOG_WARN("failed to add members", K(ret));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < connect_infos.count(); ++j) {
            bool is_connected = false;
            if (OB_FAIL(check_join_info(expr->get_relation_ids(), connect_infos.at(j), base_table_ids, is_connected))) {
              LOG_WARN("failed to check join info", K(ret));
            } else if (!is_connected) {
              // do nothing
            } else if (OB_FAIL(used_infos.add_member(j))) {
              LOG_WARN("failed to add member", K(ret));
            } else if (connect_tables.add_members(connect_infos.at(j))) {
              LOG_WARN("failed to add members", K(ret));
            }
          }
          if (OB_SUCC(ret) && !used_infos.is_empty()) {
            have_new_connect_info = true;
            if (OB_FAIL(used_join_conditions.add_member(i))) {
              LOG_WARN("failed to add member", K(ret));
            }
            for (int64_t j = 0; OB_SUCC(ret) && j < connect_infos.count(); ++j) {
              if (!used_infos.has_member(j)) {
                // do nothing
              } else if (OB_FAIL(connect_infos.at(j).add_members(connect_tables))) {
                LOG_WARN("failed to add members", K(ret));
              }
            }
            LOG_TRACE("succeed to add new connect info", K(*expr), K(connect_infos));
          }
        }
      }
    }
    ObSEArray<ObRelIds, 8> new_connect_infos;
    for (int64_t i = 0; OB_SUCC(ret) && i < connect_infos.count(); ++i) {
      bool find = false;
      for (int64_t j = 0; !find && j < new_connect_infos.count(); ++j) {
        if (new_connect_infos.at(j).equal(connect_infos.at(i))) {
          find = true;
        }
      }
      if (!find) {
        ret = new_connect_infos.push_back(connect_infos.at(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < base_table_ids.count(); ++i) {
      if (base_table_ids.at(i).num_members() < 2) {
        // do nothing
      } else if (OB_FAIL(add_conflict_rule(
                     base_table_ids.at(i), base_table_ids.at(i), cross_product_detector->cross_product_rule_))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_connect_infos.count(); ++i) {
      if (new_connect_infos.at(i).num_members() < 2) {
        // do nothing
      } else if (OB_FAIL(add_conflict_rule(new_connect_infos.at(i),
                     new_connect_infos.at(i),
                     cross_product_detector->delay_cross_product_rule_))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_connect_infos.count(); ++i) {
      if (new_connect_infos.at(i).num_members() > 1) {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < new_connect_infos.count(); ++j) {
          if (new_connect_infos.at(j).num_members() > 1) {
            ObRelIds bushy_info;
            if (OB_FAIL(bushy_info.add_members(new_connect_infos.at(i)))) {
              LOG_WARN("failed to add members", K(ret));
            } else if (OB_FAIL(bushy_info.add_members(new_connect_infos.at(j)))) {
              LOG_WARN("failed to add members", K(ret));
            } else if (OB_FAIL(bushy_tree_infos_.push_back(bushy_info))) {
              LOG_WARN("failed to push back info", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (new_connect_infos.count() == 1) {
        cross_product_detector->is_redundancy_ = true;
      }
    }
    LOG_TRACE("update bushy tree info", K(bushy_tree_infos_));
  }
  return ret;
}

int ObLogPlan::select_location(ObIArray<ObTablePartitionInfo*>& tbl_part_info_list)
{
  int ret = OB_SUCCESS;
  ObExecContext* exec_ctx = optimizer_context_.get_exec_ctx();
  ObSEArray<const ObTableLocation*, 1> tbl_loc_list;
  ObSEArray<ObPhyTableLocationInfo*, 1> phy_tbl_loc_info_list;
  if (OB_ISNULL(exec_ctx)) {
    LOG_ERROR("exec ctx is NULL", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tbl_part_info_list.count(); ++i) {
    ObTablePartitionInfo* tbl_part_info = tbl_part_info_list.at(i);
    if (OB_ISNULL(tbl_part_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tbl part info is NULL", K(ret), K(i), K(tbl_part_info_list.count()));
    } else if (OB_FAIL(tbl_loc_list.push_back(&tbl_part_info->get_table_location()))) {
      LOG_WARN("fail to push back table location list", K(ret), K(tbl_part_info->get_table_location()));
    } else if (OB_FAIL(phy_tbl_loc_info_list.push_back(&tbl_part_info->get_phy_tbl_location_info_for_update()))) {
      LOG_WARN("fail to push back phy tble loc info", K(ret), K(tbl_part_info->get_phy_tbl_location_info_for_update()));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObLogPlan::select_replicas(
                 *exec_ctx, tbl_loc_list, optimizer_context_.get_local_server_addr(), phy_tbl_loc_info_list))) {
    LOG_WARN("fail to select replicas",
        K(ret),
        K(tbl_loc_list.count()),
        K(optimizer_context_.get_local_server_addr()),
        K(phy_tbl_loc_info_list.count()));
  }
  return ret;
}

int ObLogPlan::select_replicas(ObExecContext& exec_ctx, const ObIArray<const ObTableLocation*>& tbl_loc_list,
    const ObAddr& local_server, ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list)
{
  int ret = OB_SUCCESS;
  bool is_weak = true;
  if (OB_ISNULL(exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    is_weak = !ObDMLStmt::is_dml_write_stmt(exec_ctx.get_my_session()->get_stmt_type());
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_weak && i < tbl_loc_list.count(); i++) {
    bool is_weak_read = false;
    const ObTableLocation* table_location = tbl_loc_list.at(i);
    if (OB_ISNULL(table_location)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table location is NULL", K(ret), K(i), K(tbl_loc_list.count()));
    } else if (OB_FAIL(table_location->get_is_weak_read(exec_ctx, is_weak_read))) {
      LOG_WARN("fail to get is weak read", K(ret), K(*table_location));
    } else if (!is_weak_read) {
      is_weak = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(select_replicas(exec_ctx, is_weak, local_server, phy_tbl_loc_info_list))) {
      LOG_WARN("select replicas failed", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::select_replicas(ObExecContext& exec_ctx, bool is_weak, const ObAddr& local_server,
    ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = exec_ctx.get_my_session();
  ObTaskExecutorCtx& task_exec_ctx = exec_ctx.get_task_exec_ctx();
  ObPartitionService* partition_service = NULL;
  bool is_hit_partition = false;
  ObFollowerFirstFeedbackType follower_first_feedback = FFF_HIT_MIN;
  int64_t route_policy_type = 0;
  bool proxy_priority_hit_support = false;
  if (OB_ISNULL(session) || OB_ISNULL(partition_service = task_exec_ctx.get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret), K(session), K(partition_service));
  } else if (OB_FAIL(session->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy_type))) {
    LOG_WARN("fail to get sys variable", K(ret));
  } else {
    proxy_priority_hit_support = session->get_proxy_cap_flags().is_priority_hit_support();
  }

  if (OB_FAIL(ret)) {
  } else if (is_weak) {
    if (OB_FAIL(ObLogPlan::weak_select_replicas(*partition_service,
            local_server,
            static_cast<ObRoutePolicyType>(route_policy_type),
            proxy_priority_hit_support,
            phy_tbl_loc_info_list,
            is_hit_partition,
            follower_first_feedback))) {
      LOG_WARN("fail to weak select intersect replicas", K(ret), K(local_server), K(phy_tbl_loc_info_list.count()));
    } else {
      ObPartitionKey part_key;
      session->partition_hit().try_set_bool(is_hit_partition);
      if (FFF_HIT_MIN != follower_first_feedback) {
        if (OB_FAIL(session->set_follower_first_feedback(follower_first_feedback))) {
          LOG_WARN("fail to set_follower_first_feedback", K(follower_first_feedback), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        task_exec_ctx.set_need_renew_location_cache(!is_hit_partition);
        if (task_exec_ctx.is_need_renew_location_cache()) {
          for (int64_t i = 0; OB_SUCC(ret) && i < phy_tbl_loc_info_list.count(); ++i) {
            const ObPhyTableLocationInfo* phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
            if (OB_ISNULL(phy_tbl_loc_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("phy tbl loc info is NULL", K(ret), K(i));
            } else {
              const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list =
                  phy_tbl_loc_info->get_phy_part_loc_info_list();
              for (int64_t j = 0; OB_SUCC(ret) && j < phy_part_loc_info_list.count(); ++j) {
                part_key.reset();
                const ObPhyPartitionLocationInfo& phy_part_loc_info = phy_part_loc_info_list.at(j);
                if (OB_FAIL(phy_part_loc_info.get_partition_location().get_partition_key(part_key))) {
                  LOG_WARN("fail to get part key", K(ret), K(phy_part_loc_info.get_partition_location()));
                } else if (OB_FAIL(task_exec_ctx.add_need_renew_partition_keys_distinctly(part_key))) {
                  LOG_WARN("fail to add need renew partition key", K(ret), K(part_key));
                }
              }
            }
          }
        }
      }
    }
  } else {
    const bool sess_in_retry = session->get_is_in_retry_for_dup_tbl();
    if (OB_FAIL(
            ObLogPlan::strong_select_replicas(local_server, phy_tbl_loc_info_list, is_hit_partition, sess_in_retry))) {
      LOG_WARN("fail to strong select replicas", K(ret), K(local_server), K(phy_tbl_loc_info_list.count()));
      int tmp_ret = set_table_location(task_exec_ctx, phy_tbl_loc_info_list);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to set table location", K(tmp_ret), K(phy_tbl_loc_info_list));
      }
    } else {
      session->partition_hit().try_set_bool(is_hit_partition);
      task_exec_ctx.set_need_renew_location_cache(false);
    }
  }
  return ret;
}

int ObLogPlan::set_table_location(
    ObTaskExecutorCtx& task_exec_ctx, ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list)
{
  int ret = OB_SUCCESS;
  ObPhyTableLocation location;
  ObPartitionKey key;
  ObPartitionReplicaLocation partition_replica_location;
  for (int64_t i = 0; i < phy_tbl_loc_info_list.count() && OB_SUCC(ret); i++) {
    ObPhyTableLocationInfo*& info = phy_tbl_loc_info_list.at(i);
    location.reset();
    for (int64_t j = 0; j < info->get_phy_part_loc_info_list().count() && OB_SUCC(ret); j++) {
      const ObPhyPartitionLocationInfo& partition_location_info = info->get_phy_part_loc_info_list().at(j);
      const ObOptPartLoc& part_loc = partition_location_info.get_partition_location();
      if (OB_FAIL(part_loc.get_partition_key(key))) {
        LOG_WARN("fail to get partition key", K(ret), K(part_loc));
      } else {
        partition_replica_location.set_table_id(key.get_table_id());
        partition_replica_location.set_partition_id(key.get_partition_id());
        partition_replica_location.set_partition_cnt(key.get_partition_cnt());
        if (OB_FAIL(location.add_partition_location(partition_replica_location))) {
          LOG_WARN("fail to add partition location", K(ret));
        }
      }
    }  // end for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(task_exec_ctx.get_table_locations().push_back(location))) {
        LOG_WARN("fail to push back", K(ret), K(location));
      }
    }
  }  // end for
  LOG_TRACE("finish set table location", "table_locaiont", task_exec_ctx.get_table_locations());
  return ret;
}

int ObLogPlan::strong_select_replicas(const ObAddr& local_server,
    ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list, bool& is_hit_partition, bool sess_in_retry)
{
  int ret = OB_SUCCESS;
  bool all_is_on_same_server = true;
  ObAddr all_same_server = local_server;
  ObAddr cur_same_server;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_tbl_loc_info_list.count(); ++i) {
    cur_same_server.reset();
    ObPhyTableLocationInfo* phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
    bool is_on_same_server = false;
    if (OB_ISNULL(phy_tbl_loc_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
    } else if (0 == phy_tbl_loc_info->get_partition_cnt()) {
    } else {
      if (!sess_in_retry && phy_tbl_loc_info->is_duplicate_table_not_in_dml()) {
        if (OB_FAIL(phy_tbl_loc_info->all_select_local_replica_or_leader(
                is_on_same_server, cur_same_server, local_server))) {
          LOG_WARN("fail to all select leader", K(ret), K(*phy_tbl_loc_info));
        } else {
          LOG_TRACE("succeed to select replica for duplicate table", K(*phy_tbl_loc_info), K(is_on_same_server));
        }
      } else {
        if (OB_FAIL(phy_tbl_loc_info->all_select_leader(is_on_same_server, cur_same_server))) {
          LOG_WARN("fail to all select leader", K(ret), K(*phy_tbl_loc_info));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing...
      } else if (all_is_on_same_server) {
        if (is_on_same_server) {
          if (0 == i) {
            all_same_server = cur_same_server;
          } else if (all_same_server != cur_same_server) {
            all_is_on_same_server = false;
          }
        } else {
          all_is_on_same_server = false;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (all_is_on_same_server && local_server != all_same_server) {
      is_hit_partition = false;
    } else {
      is_hit_partition = true;
    }
  }
  return ret;
}

int ObLogPlan::get_global_phy_tbl_location_info(ObPhyTableLocationInfoIArray& tbl_infos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_stmt()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(get_stmt()));
  } else {
    const ObTablePartitionInfoArray& tbl_part_infos = get_stmt()->get_query_ctx()->table_partition_infos_;
    ARRAY_FOREACH(tbl_part_infos, i)
    {
      if (OB_ISNULL(tbl_part_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table partition info is null");
      } else if (OB_FAIL(tbl_infos.push_back(tbl_part_infos.at(i)->get_phy_tbl_location_info()))) {
        LOG_WARN("add phy table location info failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::weak_select_replicas(ObPartitionService& partition_service, const ObAddr& local_server,
    ObRoutePolicyType route_type, bool proxy_priority_hit_support,
    ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list, bool& is_hit_partition,
    ObFollowerFirstFeedbackType& follower_first_feedback)
{
  int ret = OB_SUCCESS;
  is_hit_partition = true;
  ObPhyTableLocationInfo* phy_tbl_loc_info = nullptr;
  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
  ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> intersect_server_list(allocator);
  ObRoutePolicy route_policy(local_server, partition_service);
  ObRoutePolicyCtx route_policy_ctx;
  route_policy_ctx.policy_type_ = route_type;
  route_policy_ctx.consistency_level_ = WEAK;
  route_policy_ctx.is_proxy_priority_hit_support_ = proxy_priority_hit_support;

  if (OB_FAIL(route_policy.init())) {
    LOG_WARN("fail to init route policy", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_tbl_loc_info_list.count(); ++i) {
    if (OB_ISNULL(phy_tbl_loc_info = phy_tbl_loc_info_list.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy table loc info is NULL", K(phy_tbl_loc_info), K(i), K(ret));
    } else {
      ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list =
          phy_tbl_loc_info->get_phy_part_loc_info_list_for_update();
      for (int64_t j = 0; OB_SUCC(ret) && j < phy_part_loc_info_list.count(); ++j) {
        ObPhyPartitionLocationInfo& phy_part_loc_info = phy_part_loc_info_list.at(j);
        if (phy_part_loc_info.has_selected_replica()) {  // do nothing
        } else {
          ObIArray<ObRoutePolicy::CandidateReplica>& replica_array =
              phy_part_loc_info.get_partition_location().get_replica_locations();
          if (OB_FAIL(route_policy.init_candidate_replicas(replica_array))) {
            LOG_WARN("fail to init candidate replicas", K(replica_array), K(ret));
          } else if (OB_FAIL(route_policy.calculate_replica_priority(replica_array, route_policy_ctx))) {
            LOG_WARN("fail to calculate replica priority", K(replica_array), K(route_policy_ctx), K(ret));
          } else if (OB_FAIL(route_policy.select_replica_with_priority(
                         route_policy_ctx, replica_array, phy_part_loc_info))) {
            LOG_WARN("fail to select replica", K(replica_array), K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(route_policy.select_intersect_replica(
                 route_policy_ctx, phy_tbl_loc_info_list, intersect_server_list, is_hit_partition))) {
    LOG_WARN("fail to select intersect replica",
        K(route_policy_ctx),
        K(phy_tbl_loc_info_list),
        K(intersect_server_list),
        K(ret));
  }

  if (OB_SUCC(ret)) {
    if (proxy_priority_hit_support) {
      // nothing, current doesn't support
    } else {
      ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
      ObAddrList intersect_servers(allocator);
      if (OB_FAIL(calc_hit_partition_for_compat(
              phy_tbl_loc_info_list, local_server, is_hit_partition, intersect_servers))) {
        LOG_WARN("fail to calc hit partition for compat", K(ret));
      } else {
        if (is_hit_partition && route_policy.is_follower_first_route_policy_type(route_policy_ctx)) {
          if (OB_FAIL(calc_follower_first_feedback(
                  phy_tbl_loc_info_list, local_server, intersect_servers, follower_first_feedback))) {
            LOG_WARN("fail to calc follower first feedback", K(ret));
          }
        }
      }
    }
  }
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    LOG_INFO("selected replica ",
        "intersect_server_list",
        intersect_server_list,
        "\n phy_tbl_loc_info_list",
        phy_tbl_loc_info_list,
        "\n route_policy",
        route_policy,
        "\n route_policy_ctx",
        route_policy_ctx,
        "\n follower_first_feedback",
        follower_first_feedback,
        "\n is_hit_partition",
        is_hit_partition);
  }
  return ret;
}

int ObLogPlan::calc_follower_first_feedback(const ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list,
    const ObAddr& local_server, const ObAddrList& intersect_servers,
    ObFollowerFirstFeedbackType& follower_first_feedback)
{
  INIT_SUCC(ret);
  follower_first_feedback = FFF_HIT_MIN;
  if (intersect_servers.empty()) {
    // nothing, no need feedback
  } else {
    bool is_leader_replica = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_leader_replica && (i < phy_tbl_loc_info_list.count()); ++i) {
      const ObPhyTableLocationInfo* phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
      if (OB_ISNULL(phy_tbl_loc_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
      } else {
        const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list = phy_tbl_loc_info->get_phy_part_loc_info_list();
        if (phy_part_loc_info_list.empty()) {
          // juest defense, when partition location list is empty, treat as it's not leader replica
          is_leader_replica = false;
        }
        for (int64_t j = 0; OB_SUCC(ret) && is_leader_replica && (j < phy_part_loc_info_list.count()); ++j) {
          bool found_server = false;
          const ObPhyPartitionLocationInfo& phy_part_loc_info = phy_part_loc_info_list.at(j);
          const ObIArray<ObRoutePolicy::CandidateReplica>& replica_loc_list =
              phy_part_loc_info.get_partition_location().get_replica_locations();
          for (int64_t k = 0; !found_server && (k < replica_loc_list.count()); ++k) {
            const ObRoutePolicy::CandidateReplica& tmp_replica = replica_loc_list.at(k);
            if (local_server == tmp_replica.server_) {
              found_server = true;
              is_leader_replica = (is_strong_leader(tmp_replica.role_));
            }
          }
          if (!found_server) {  // if not found, just treat as it's not leader
            is_leader_replica = false;
          }
        }
      }
    }

    if (is_leader_replica) {
      follower_first_feedback = FFF_HIT_LEADER;
    } else {
      // nothing, no need feedback
    }
    LOG_TRACE("after calc follower first feedback",
        K(follower_first_feedback),
        K(is_leader_replica),
        K(local_server),
        K(intersect_servers));
  }

  return ret;
}

int ObLogPlan::calc_hit_partition_for_compat(const ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list,
    const ObAddr& local_server, bool& is_hit_partition, ObAddrList& intersect_servers)
{
  int ret = OB_SUCCESS;
  bool can_select_local_server = false;
  is_hit_partition = false;
  if (OB_FAIL(calc_intersect_servers(phy_tbl_loc_info_list, intersect_servers))) {
    LOG_WARN("fail to calc hit partition for compat", K(ret));
  } else if (intersect_servers.empty()) {
    is_hit_partition = true;
  } else {
    ObAddrList::iterator candidate_server_list_iter = intersect_servers.begin();
    for (; OB_SUCC(ret) && !can_select_local_server && candidate_server_list_iter != intersect_servers.end();
         candidate_server_list_iter++) {
      const ObAddr& candidate_server = *candidate_server_list_iter;
      if (local_server == candidate_server) {
        is_hit_partition = true;
        can_select_local_server = true;
      }
    }
  }
  return ret;
}

int ObLogPlan::calc_intersect_servers(
    const ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list, ObAddrList& candidate_server_list)
{
  int ret = OB_SUCCESS;
  bool can_select_one_server = true;
  ObRoutePolicy::CandidateReplica tmp_replica;
  candidate_server_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && can_select_one_server && i < phy_tbl_loc_info_list.count(); ++i) {
    const ObPhyTableLocationInfo* phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
    if (OB_ISNULL(phy_tbl_loc_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
    } else {
      const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list = phy_tbl_loc_info->get_phy_part_loc_info_list();
      for (int64_t j = 0; OB_SUCC(ret) && can_select_one_server && j < phy_part_loc_info_list.count(); ++j) {
        const ObPhyPartitionLocationInfo& phy_part_loc_info = phy_part_loc_info_list.at(j);
        const ObIArray<ObRoutePolicy::CandidateReplica>& replica_loc_list =
            phy_part_loc_info.get_partition_location().get_replica_locations();
        if (0 == i && 0 == j) {
          for (int64_t k = 0; OB_SUCC(ret) && k < replica_loc_list.count(); ++k) {
            if (OB_FAIL(candidate_server_list.push_back(replica_loc_list.at(k).server_))) {
              LOG_WARN("fail to push back candidate server", K(ret), K(k), K(replica_loc_list.at(k)));
            }
          }
        } else {
          ObAddrList::iterator candidate_server_list_iter = candidate_server_list.begin();
          for (; OB_SUCC(ret) && candidate_server_list_iter != candidate_server_list.end();
               candidate_server_list_iter++) {
            const ObAddr& candidate_server = *candidate_server_list_iter;
            bool has_replica = false;
            for (int64_t k = 0; OB_SUCC(ret) && !has_replica && k < replica_loc_list.count(); ++k) {
              if (replica_loc_list.at(k).server_ == candidate_server) {
                has_replica = true;
              }
            }
            if (OB_SUCC(ret) && !has_replica) {
              if (OB_FAIL(candidate_server_list.erase(candidate_server_list_iter))) {
                LOG_WARN("fail to erase from list", K(ret), K(replica_loc_list), K(candidate_server));
              }
            }
          }
          if (OB_SUCC(ret) && candidate_server_list.empty()) {
            can_select_one_server = false;
          }
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::select_one_server(
    const ObAddr& selected_server, ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_tbl_loc_info_list.count(); ++i) {
    ObPhyTableLocationInfo* phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
    if (OB_ISNULL(phy_tbl_loc_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
    } else {
      ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list =
          phy_tbl_loc_info->get_phy_part_loc_info_list_for_update();
      for (int64_t j = 0; OB_SUCC(ret) && j < phy_part_loc_info_list.count(); ++j) {
        ObPhyPartitionLocationInfo& phy_part_loc_info = phy_part_loc_info_list.at(j);
        if (phy_part_loc_info.has_selected_replica()) {
        } else {
          const ObIArray<ObRoutePolicy::CandidateReplica>& replica_loc_list =
              phy_part_loc_info.get_partition_location().get_replica_locations();
          bool replica_is_selected = false;
          for (int64_t k = 0; OB_SUCC(ret) && !replica_is_selected && k < replica_loc_list.count(); ++k) {
            if (selected_server == replica_loc_list.at(k).server_) {
              if (OB_FAIL(phy_part_loc_info.set_selected_replica_idx(k))) {
                LOG_WARN("fail to set selected replica idx", K(ret), K(k), K(phy_part_loc_info));
              } else {
                replica_is_selected = true;
              }
            }
          }
          if (OB_SUCC(ret) && OB_UNLIKELY(!replica_is_selected)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("has no selected replica", K(ret), K(selected_server), K(replica_loc_list));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::init_leading_info_from_joined_tables(TableItem* table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table->is_joined_table()) {
    // do nothing
  } else {
    JoinedTable* joined_table = static_cast<JoinedTable*>(table);
    LeadingInfo hint_info;
    if (OB_FAIL(get_table_ids(table, hint_info.table_set_))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(get_table_ids(joined_table->left_table_, hint_info.left_table_set_))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(get_table_ids(joined_table->right_table_, hint_info.right_table_set_))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(leading_infos_.push_back(hint_info))) {
      LOG_WARN("failed to push back hint info", K(ret));
    } else if (OB_FAIL(SMART_CALL(init_leading_info_from_joined_tables(joined_table->left_table_)))) {
      LOG_WARN("failed to get leading hint info", K(ret));
    } else if (OB_FAIL(SMART_CALL(init_leading_info_from_joined_tables(joined_table->right_table_)))) {
      LOG_WARN("failed to get leading hint info", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::init_leading_info_from_tables(
    const ObIArray<TableItem*>& table_items, const ObIArray<SemiInfo*>& semi_infos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    TableItem* table = table_items.at(i);
    LeadingInfo hint_info;
    ObRelIds table_ids;
    if (OB_FAIL(get_table_ids(table, table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(init_leading_info_from_joined_tables(table))) {
      LOG_WARN("failed to get leading hint infos", K(ret));
    } else if (i == 0) {
      ret = leading_tables_.add_members(table_ids);
    } else if (OB_FAIL(hint_info.left_table_set_.add_members(leading_tables_))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(hint_info.right_table_set_.add_members(table_ids))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(leading_tables_.add_members(table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(hint_info.table_set_.add_members(leading_tables_))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(leading_infos_.push_back(hint_info))) {
      LOG_WARN("failed to push back hint info", K(ret));
    }
  }
  ObSqlBitSet<> right_rel_ids;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
    SemiInfo* semi_info = semi_infos.at(i);
    LeadingInfo hint_info;
    right_rel_ids.reuse();
    if (OB_ISNULL(semi_info)) {
      LOG_WARN("unexpect null semi info", K(ret));
    } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(*stmt, semi_info->right_table_id_, right_rel_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(hint_info.left_table_set_.add_members(leading_tables_))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(hint_info.right_table_set_.add_members(right_rel_ids))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(leading_tables_.add_members(right_rel_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(hint_info.table_set_.add_members(leading_tables_))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(leading_infos_.push_back(hint_info))) {
      LOG_WARN("failed to push back hint info", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::find_join_order_pair(
    uint8_t beg_pos, uint8_t& end_pos, uint8_t ignore_beg_pos, uint8_t& ignore_end_pos, bool& found)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else {
    const ObStmtHint& hint = stmt->get_stmt_hint();
    const ObIArray<std::pair<uint8_t, uint8_t>>& join_order_pairs = hint.join_order_pairs_;
    for (int64_t i = 0; i < join_order_pairs.count(); ++i) {
      if (join_order_pairs.at(i).first == ignore_beg_pos && join_order_pairs.at(i).second == ignore_end_pos) {
        // do nothing
      } else if (join_order_pairs.at(i).second > ignore_end_pos) {
        // do nothing
      } else if (join_order_pairs.at(i).first == beg_pos) {
        if (found && end_pos < join_order_pairs.at(i).second) {
          end_pos = join_order_pairs.at(i).second;
        } else if (!found) {
          end_pos = join_order_pairs.at(i).second;
          found = true;
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::get_table_ids_from_leading(uint8_t pos, ObRelIds& table_ids)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else {
    int32_t id = OB_INVALID;
    const ObStmtHint& hint = stmt->get_stmt_hint();
    const ObIArray<uint64_t>& join_order_ids = hint.join_order_ids_;
    if (pos >= join_order_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect join order id", K(ret));
    } else if ((id = stmt->get_table_bit_index(join_order_ids.at(pos))) == OB_INVALID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table id", K(ret));
    } else if (OB_FAIL(table_ids.add_member(id))) {
      LOG_WARN("failed to add table id", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::init_leading_info_from_leading_pair(uint8_t beg_pos, uint8_t end_pos, ObRelIds& table_set)
{
  int ret = OB_SUCCESS;
  uint8_t pair_end = 0;
  for (int64_t i = beg_pos; OB_SUCC(ret) && i <= end_pos; ++i) {
    LeadingInfo hint_info;
    ObRelIds table_ids;
    bool found = false;
    if (OB_FAIL(find_join_order_pair(i, pair_end, beg_pos, end_pos, found))) {
      LOG_WARN("failed to find join order pair", K(ret));
    } else if (!found && OB_FAIL(get_table_ids_from_leading(i, table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (found && OB_FAIL(SMART_CALL(init_leading_info_from_leading_pair(i, pair_end, table_ids)))) {
      LOG_WARN("failed to get leading hint info from leading pair", K(ret));
    } else if (i == beg_pos) {
      ret = table_set.add_members(table_ids);
    } else if (OB_FAIL(hint_info.left_table_set_.add_members(table_set))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(hint_info.right_table_set_.add_members(table_ids))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(table_set.add_members(table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(hint_info.table_set_.add_members(table_set))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(leading_infos_.push_back(hint_info))) {
      LOG_WARN("failed to push back hint info", K(ret));
    }
    if (OB_SUCC(ret) && found) {
      i = pair_end;
    }
  }
  return ret;
}

int ObLogPlan::init_leading_info_from_leading()
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else {
    const ObStmtHint& hint = stmt->get_stmt_hint();
    const ObIArray<uint64_t>& join_order_ids = hint.join_order_ids_;
    if (join_order_ids.count() == 0) {
      // do nothing
    } else if (OB_FAIL(init_leading_info_from_leading_pair(0, join_order_ids.count() - 1, leading_tables_))) {
      LOG_WARN("failed to get leading hint", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::init_leading_info(const ObIArray<TableItem*>& table_items, const ObIArray<SemiInfo*>& semi_infos)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  leading_tables_.reuse();
  leading_infos_.reuse();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (stmt->get_stmt_hint().join_ordered_) {
    ret = init_leading_info_from_tables(table_items, semi_infos);
  } else if (stmt->get_stmt_hint().join_order_ids_.count() > 0) {
    ret = init_leading_info_from_leading();
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to get leading infos", K(leading_tables_), K(leading_infos_));
  }
  return ret;
}

int ObLogPlan::init_bushy_tree_info(const ObIArray<TableItem*>& table_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < leading_infos_.count(); ++i) {
    const LeadingInfo& info = leading_infos_.at(i);
    if (info.left_table_set_.num_members() > 1 && info.right_table_set_.num_members() > 1) {
      ret = bushy_tree_infos_.push_back(info.table_set_);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    if (OB_FAIL(init_bushy_tree_info_from_joined_tables(table_items.at(i)))) {
      LOG_WARN("failed to init bushy tree info", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to get bushy infos", K(bushy_tree_infos_));
  }
  return ret;
}

int ObLogPlan::init_bushy_tree_info_from_joined_tables(TableItem* table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table->is_joined_table()) {
    // do nothing
  } else {
    JoinedTable* joined_table = static_cast<JoinedTable*>(table);
    if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (joined_table->left_table_->is_joined_table() && joined_table->right_table_->is_joined_table()) {
      ObRelIds table_ids;
      if (OB_FAIL(get_table_ids(table, table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (OB_FAIL(bushy_tree_infos_.push_back(table_ids))) {
        LOG_WARN("failed to push back table ids", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(SMART_CALL(init_bushy_tree_info_from_joined_tables(joined_table->left_table_)))) {
      LOG_WARN("failed to init bushy tree infos", K(ret));
    } else if (OB_FAIL(SMART_CALL(init_bushy_tree_info_from_joined_tables(joined_table->right_table_)))) {
      LOG_WARN("failed to init bushy tree infos", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::check_need_bushy_tree(common::ObIArray<JoinOrderArray>& join_rels, const int64_t level, bool& need)
{
  int ret = OB_SUCCESS;
  need = false;
  if (level >= join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(join_rels.count()), K(level));
  } else if (join_rels.at(level).empty()) {
    need = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && !need && i < bushy_tree_infos_.count(); ++i) {
    const ObRelIds& table_ids = bushy_tree_infos_.at(i);
    if (table_ids.num_members() != level + 1) {
      // do nothing
    } else {
      bool has_generated = false;
      int64_t N = join_rels.at(level).count();
      for (int64_t j = 0; OB_SUCC(ret) && !has_generated && j < N; ++j) {
        ObJoinOrder* join_order = join_rels.at(level).at(j);
        if (OB_ISNULL(join_order)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null join order", K(ret));
        } else if (table_ids.is_subset(join_order->get_tables())) {
          has_generated = true;
        }
      }
      if (OB_SUCC(ret)) {
        need = !has_generated;
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_join_levels_with_DP(common::ObIArray<JoinOrderArray>& join_rels, const int64_t join_level)
{
  int ret = OB_SUCCESS;
  if (!leading_tables_.is_empty() && OB_FAIL(inner_generate_join_levels_with_DP(join_rels, join_level, false))) {
    LOG_WARN("failed to generate join levels with hint", K(ret));
  } else if (1 == join_rels.at(join_level - 1).count()) {
  } else if (OB_FAIL(inner_generate_join_levels_with_DP(join_rels, join_level, true))) {
    LOG_WARN("failed to generate join level in generate_join_orders", K(ret), K(join_level));
  }
  return ret;
}

int ObLogPlan::generate_join_levels_with_linear(
    common::ObIArray<JoinOrderArray>& join_rels, const ObIArray<TableItem*>& table_items, const int64_t join_level)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(preprocess_for_linear(table_items, join_rels))) {
    LOG_WARN("failed to preprocess for linear", K(ret));
  } else if (OB_FAIL(inner_generate_join_levels_with_linear(join_rels, join_level))) {
    LOG_WARN("failed to generate join level in generate_join_orders", K(ret), K(join_level));
  }
  return ret;
}

int ObLogPlan::inner_generate_join_levels_with_DP(
    common::ObIArray<JoinOrderArray>& join_rels, const int64_t join_level, bool ignore_hint)
{
  int ret = OB_SUCCESS;
  bool abort = false;
  for (uint32_t base_level = 1; OB_SUCC(ret) && !abort && base_level < join_level; ++base_level) {
    LOG_TRACE("start to generate one level join order with DP", K(base_level));
    for (uint32_t i = 0; OB_SUCC(ret) && !abort && i <= base_level / 2; ++i) {
      uint32_t right_level = i;
      uint32_t left_level = base_level - 1 - right_level;
      if (right_level > left_level) {
        // do nothing
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status fail", K(ret));
      } else if (OB_FAIL(generate_single_join_level_with_DP(
                     join_rels, left_level, right_level, base_level, ignore_hint, abort))) {
        LOG_WARN("failed to generate join order with dynamic program", K(ret));
      }
      bool need_bushy = false;
      if (OB_FAIL(ret) || right_level > left_level) {
      } else if (abort) {
        LOG_TRACE("abort generate join order", K(ret));
      } else if (join_rels.count() <= base_level) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("Index out of range", K(ret), K(join_rels.count()), K(base_level));
      } else if (OB_FAIL(check_need_bushy_tree(join_rels, base_level, need_bushy))) {
        LOG_WARN("failed to check need bushy tree", K(ret));
      } else if (need_bushy) {
        LOG_TRACE("no valid ZigZag tree or leading hint required, we will enumerate bushy tree");
      } else {
        LOG_TRACE("there is valid ZigZag tree, we will not enumerate bushy tree");
        break;
      }
    }
    LOG_TRACE("succeed to generate on level join order", K(base_level), K(join_rels.at(base_level).count()));
  }
  return ret;
}

int ObLogPlan::inner_generate_join_levels_with_linear(
    common::ObIArray<JoinOrderArray>& join_rels, const int64_t join_level)
{
  int ret = OB_SUCCESS;
  for (uint32_t base_level = 1; OB_SUCC(ret) && base_level < join_level; ++base_level) {
    LOG_TRACE("start to generate one level join order with linear", K(base_level));
    for (uint32_t i = 0; OB_SUCC(ret) && i <= base_level / 2; ++i) {
      uint32_t right_level = i;
      uint32_t left_level = base_level - 1 - right_level;
      if (right_level > left_level) {
        // do nothing
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status fail", K(ret));
      } else if (OB_FAIL(generate_single_join_level_with_linear(join_rels, left_level, right_level, base_level))) {
        LOG_WARN("failed to generate join order with linear", K(ret));
      }
      bool need_bushy = false;
      if (OB_FAIL(ret) || right_level > left_level) {
      } else if (join_rels.count() <= base_level) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("Index out of range", K(ret), K(join_rels.count()), K(base_level));
      } else if (OB_FAIL(check_need_bushy_tree(join_rels, base_level, need_bushy))) {
        LOG_WARN("failed to check need bushy tree", K(ret));
      } else if (need_bushy) {
        LOG_TRACE("no valid ZigZag tree or leading hint required, we will enumerate bushy tree");
      } else {
        LOG_TRACE("there is valid ZigZag tree, we will not enumerate bushy tree");
        break;
      }
    }
    LOG_TRACE("succeed to generate on level join order", K(base_level), K(join_rels.at(base_level).count()));
  }
  return ret;
}

int ObLogPlan::preprocess_for_linear(const ObIArray<TableItem*>& table_items, ObIArray<JoinOrderArray>& join_rels)
{
  int ret = OB_SUCCESS;
  if (join_rels.empty()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("join_rels is empty", K(ret), K(join_rels.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    ObJoinOrder* join_tree = NULL;
    if (OB_FAIL(generate_join_order_with_table_tree(join_rels, table_items.at(i), join_tree))) {
      LOG_WARN("failed to generate join order", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::generate_join_order_with_table_tree(
    ObIArray<JoinOrderArray>& join_rels, TableItem* table, ObJoinOrder*& join_tree)
{
  int ret = OB_SUCCESS;
  JoinedTable* joined_table = NULL;
  ObJoinOrder* left_tree = NULL;
  ObJoinOrder* right_tree = NULL;
  join_tree = NULL;
  if (OB_ISNULL(table) || join_rels.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (!table->is_joined_table()) {
    // find base join rels
    ObIArray<ObJoinOrder*>& single_join_rels = join_rels.at(0);
    for (int64_t i = 0; OB_SUCC(ret) && NULL == join_tree && i < single_join_rels.count(); ++i) {
      ObJoinOrder* base_rel = single_join_rels.at(i);
      if (OB_ISNULL(base_rel)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null base rel", K(ret));
      } else if (table->table_id_ == base_rel->get_table_id()) {
        join_tree = base_rel;
      }
    }
  } else if (OB_FALSE_IT(joined_table = static_cast<JoinedTable*>(table))) {
  } else if (OB_FAIL(
                 SMART_CALL(generate_join_order_with_table_tree(join_rels, joined_table->left_table_, left_tree)))) {
    LOG_WARN("failed to generate join order", K(ret));
  } else if (OB_FAIL(
                 SMART_CALL(generate_join_order_with_table_tree(join_rels, joined_table->right_table_, right_tree)))) {
    LOG_WARN("failed to generate join order", K(ret));
  } else if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null join order", K(ret));
  } else {
    bool is_valid_join = false;
    ObRelIds cur_relids;
    int64_t level = left_tree->get_tables().num_members() + right_tree->get_tables().num_members() - 1;
    if (OB_FAIL(inner_generate_join_order(join_rels, left_tree, right_tree, level, false, false, is_valid_join))) {
      LOG_WARN("failed to generated join order", K(ret));
    } else if (OB_FAIL(cur_relids.add_members(left_tree->get_tables()))) {
      LOG_WARN("fail to add left tree' table ids", K(ret));
    } else if (OB_FAIL(cur_relids.add_members(right_tree->get_tables()))) {
      LOG_WARN("fail to add right tree' table ids", K(ret));
    } else if (OB_FAIL(find_join_rel(join_rels.at(level), cur_relids, join_tree))) {
      LOG_WARN("find_join_rel fails", K(ret), K(level));
    } else if (OB_ISNULL(join_tree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null join order", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::generate_single_join_level_with_DP(ObIArray<JoinOrderArray>& join_rels, uint32_t left_level,
    uint32_t right_level, uint32_t level, bool ignore_hint, bool& abort)
{
  int ret = OB_SUCCESS;
  abort = false;
  if (join_rels.empty() || left_level >= join_rels.count() || right_level >= join_rels.count() ||
      level >= join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(join_rels.count()), K(left_level), K(right_level), K(level));
  } else {
    ObIArray<ObJoinOrder*>& left_rels = join_rels.at(left_level);
    ObIArray<ObJoinOrder*>& right_rels = join_rels.at(right_level);
    ObJoinOrder* left_tree = NULL;
    ObJoinOrder* right_tree = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !abort && i < left_rels.count(); ++i) {
      left_tree = left_rels.at(i);
      if (OB_ISNULL(left_tree)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null join tree", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !abort && j < right_rels.count(); ++j) {
          right_tree = right_rels.at(j);
          bool match_hint = false;
          bool is_legal = true;
          bool is_strict_order = true;
          bool is_valid_join = false;
          if (OB_ISNULL(right_tree)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null join tree", K(ret));
          } else if (!ignore_hint &&
                     OB_FAIL(check_join_hint(
                         left_tree->get_tables(), right_tree->get_tables(), match_hint, is_legal, is_strict_order))) {
            LOG_WARN("failed to check join hint", K(ret));
          } else if (!is_legal) {
            LOG_TRACE("join order conflict with leading hint", K(left_tree->get_tables()), K(right_tree->get_tables()));
          } else if (OB_FAIL(inner_generate_join_order(join_rels,
                         is_strict_order ? left_tree : right_tree,
                         is_strict_order ? right_tree : left_tree,
                         level,
                         match_hint,
                         !match_hint,
                         is_valid_join))) {
            LOG_WARN("failed to generate join order", K(level), K(ret));
          } else if (match_hint && !leading_tables_.is_subset(left_tree->get_tables()) && !is_valid_join) {
            abort = true;
          } else {
            LOG_TRACE("succeed to check join order",
                K(left_tree->get_tables()),
                K(right_tree->get_tables()),
                K(is_valid_join));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_single_join_level_with_linear(
    ObIArray<JoinOrderArray>& join_rels, uint32_t left_level, uint32_t right_level, uint32_t level)
{
  int ret = OB_SUCCESS;
  if (join_rels.empty() || left_level >= join_rels.count() || right_level >= join_rels.count() ||
      level >= join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(join_rels.count()), K(left_level), K(right_level), K(level));
  } else {
    ObIArray<ObJoinOrder*>& left_rels = join_rels.at(left_level);
    ObIArray<ObJoinOrder*>& right_rels = join_rels.at(right_level);
    ObJoinOrder* left_tree = NULL;
    ObJoinOrder* right_tree = NULL;
    if (OB_FAIL(sort_cur_rels(left_rels))) {
      LOG_WARN("failed to sort rels", K(left_rels.count()), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < left_rels.count() && join_rels.at(level).empty(); ++i) {
      left_tree = left_rels.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < right_rels.count(); ++j) {
        right_tree = right_rels.at(j);
        bool is_valid_join = false;
        if (OB_FAIL(inner_generate_join_order(join_rels, left_tree, right_tree, level, false, true, is_valid_join))) {
          LOG_WARN("failed to generate join order", K(level), K(ret));
        } else {
          LOG_TRACE(
              "succeed to check join order", K(left_tree->get_tables()), K(right_tree->get_tables()), K(is_valid_join));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < left_rels.count() && join_rels.at(level).empty(); ++i) {
      left_tree = left_rels.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < right_rels.count(); ++j) {
        right_tree = right_rels.at(j);
        bool is_valid_join = false;
        if (OB_FAIL(inner_generate_join_order(join_rels, left_tree, right_tree, level, false, false, is_valid_join))) {
          LOG_WARN("failed to generate join order", K(level), K(ret));
        } else {
          LOG_TRACE(
              "succeed to check join order", K(left_tree->get_tables()), K(right_tree->get_tables()), K(is_valid_join));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::inner_generate_join_order(ObIArray<JoinOrderArray>& join_rels, ObJoinOrder* left_tree,
    ObJoinOrder* right_tree, uint32_t level, bool hint_force_order, bool delay_cross_product, bool& is_valid_join)
{
  int ret = OB_SUCCESS;
  is_valid_join = false;
  if (join_rels.empty() || level >= join_rels.count() || OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(join_rels.count()), K(left_tree), K(right_tree), K(level));
  } else {
    ObSEArray<ConflictDetector*, 4> valid_detectors;
    ObRelIds cur_relids;
    JoinInfo join_info;
    ObJoinOrder* join_tree = NULL;
    bool is_strict_order = true;
    if (left_tree->get_tables().overlap(right_tree->get_tables())) {
    } else if (OB_FAIL(
                   choose_join_info(left_tree, right_tree, valid_detectors, delay_cross_product, is_strict_order))) {
      LOG_WARN("failed to choose join info", K(ret));
    } else if (valid_detectors.empty()) {
      LOG_TRACE("there is no valid join info for ", K(left_tree->get_tables()), K(right_tree->get_tables()));
    } else if (OB_FAIL(cur_relids.add_members(left_tree->get_tables()))) {
      LOG_WARN("fail to add left tree' table ids", K(ret));
    } else if (OB_FAIL(cur_relids.add_members(right_tree->get_tables()))) {
      LOG_WARN("fail to add right tree' table ids", K(ret));
    } else if (OB_FAIL(find_join_rel(join_rels.at(level), cur_relids, join_tree))) {
      LOG_WARN("find_join_rel fails", K(ret), K(level));
    } else if (OB_FAIL(merge_join_info(valid_detectors, join_info))) {
      LOG_WARN("failed to merge join info", K(ret));
    } else if (NULL != join_tree && level <= 1 && !hint_force_order) {
      is_valid_join = true;
    } else {
      if (!is_strict_order) {
        if (!hint_force_order) {
          std::swap(left_tree, right_tree);
        } else {
          join_info.join_type_ = get_opposite_join_type(join_info.join_type_);
        }
      }
      if (NULL == join_tree) {
        if (OB_ISNULL(join_tree = create_join_order(JOIN))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create join tree", K(ret));
        } else if (OB_FAIL(join_tree->init_join_order(left_tree, right_tree, &join_info))) {
          LOG_WARN("failed to init join order", K(ret));
        } else if (OB_FAIL(join_rels.at(level).push_back(join_tree))) {
          LOG_WARN("failed to push back join order", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(join_tree->merge_conflict_detectors(left_tree, right_tree, valid_detectors))) {
        LOG_WARN("failed to merge conflict detectors", K(ret));
      } else if (OB_FAIL(join_tree->generate_join_paths(left_tree, right_tree, &join_info, hint_force_order))) {
        LOG_WARN("failed to generate join paths for left_tree", K(level), K(ret));
      } else {
        is_valid_join = true;
        LOG_TRACE("succeed to generate join order for ",
            K(left_tree->get_tables()),
            K(right_tree->get_tables()),
            K(is_strict_order),
            K(join_info));
      }
    }
  }
  return ret;
}

int ObLogPlan::is_detector_used(
    ObJoinOrder* left_tree, ObJoinOrder* right_tree, ConflictDetector* detector, bool& is_used)
{
  int ret = OB_SUCCESS;
  is_used = false;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (INNER_JOIN == detector->join_info_.join_type_ && detector->join_info_.where_condition_.empty()) {
  } else if (ObOptimizerUtil::find_item(left_tree->get_conflict_detectors(), detector)) {
    is_used = true;
  } else if (ObOptimizerUtil::find_item(right_tree->get_conflict_detectors(), detector)) {
    is_used = true;
  }
  return ret;
}

int ObLogPlan::choose_join_info(ObJoinOrder* left_tree, ObJoinOrder* right_tree,
    ObIArray<ConflictDetector*>& valid_detectors, bool delay_cross_product, bool& is_strict_order)
{
  int ret = OB_SUCCESS;
  bool is_legal = false;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect empty conflict detectors", K(ret));
  }
  is_strict_order = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < conflict_detectors_.count(); ++i) {
    ConflictDetector* detector = conflict_detectors_.at(i);
    bool is_used = false;
    if (OB_ISNULL(detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("conflict detector is null", K(ret));
    } else if (OB_FAIL(is_detector_used(left_tree, right_tree, detector, is_used))) {
      LOG_WARN("failed to check detector is used", K(ret));
    } else if (is_used) {
      // do nothing
    } else if (OB_FAIL(check_join_legal(
                   left_tree->get_tables(), right_tree->get_tables(), detector, delay_cross_product, is_legal))) {
      LOG_WARN("failed to check join legal", K(ret));
    } else if (!is_legal) {
      LOG_TRACE("left tree join right tree is not legal",
          K(left_tree->get_tables()),
          K(right_tree->get_tables()),
          K(*detector));
      if (OB_FAIL(check_join_legal(
              right_tree->get_tables(), left_tree->get_tables(), detector, delay_cross_product, is_legal))) {
        LOG_WARN("failed to check join legal", K(ret));
      } else if (!is_legal) {
        LOG_TRACE("right tree join left tree is not legal",
            K(right_tree->get_tables()),
            K(left_tree->get_tables()),
            K(*detector));
      } else if (OB_FAIL(valid_detectors.push_back(detector))) {
        LOG_WARN("failed to push back detector", K(ret));
      } else {
        is_strict_order = false;
        LOG_TRACE("succeed to find join info for ",
            K(left_tree->get_tables()),
            K(right_tree->get_tables()),
            K(left_tree->get_conflict_detectors()),
            K(right_tree->get_conflict_detectors()),
            K(*detector));
      }
    } else if (OB_FAIL(valid_detectors.push_back(detector))) {
      LOG_WARN("failed to push back detector", K(ret));
    } else {
      LOG_TRACE("succeed to find join info for ",
          K(left_tree->get_tables()),
          K(right_tree->get_tables()),
          K(left_tree->get_conflict_detectors()),
          K(right_tree->get_conflict_detectors()),
          K(*detector));
    }
  }
  return ret;
}

int ObLogPlan::check_join_info(
    const ObIArray<ConflictDetector*>& valid_detectors, ObJoinType& join_type, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ConflictDetector* detector = NULL;
  bool has_non_inner_join = false;
  is_valid = true;
  join_type = INNER_JOIN;
  if (valid_detectors.empty()) {
    is_valid = false;
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < valid_detectors.count(); ++i) {
    detector = valid_detectors.at(i);
    if (OB_ISNULL(detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null detectors", K(ret));
    } else if (INNER_JOIN == detector->join_info_.join_type_) {
      // do nothing
    } else if (has_non_inner_join) {
      is_valid = false;
    } else {
      has_non_inner_join = true;
      join_type = detector->join_info_.join_type_;
    }
  }
  return ret;
}

int ObLogPlan::merge_join_info(const ObIArray<ConflictDetector*>& valid_detectors, JoinInfo& join_info)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  if (OB_FAIL(check_join_info(valid_detectors, join_info.join_type_, is_valid))) {
    LOG_WARN("failed to check join info", K(ret));
  } else if (!is_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect different join info", K(valid_detectors), K(ret));
  } else {
    ConflictDetector* detector = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_detectors.count(); ++i) {
      detector = valid_detectors.at(i);
      if (OB_ISNULL(detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detectors", K(ret));
      } else if (OB_FAIL(join_info.table_set_.add_members(detector->join_info_.table_set_))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(append_array_no_dup(join_info.where_condition_, detector->join_info_.where_condition_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(append_array_no_dup(join_info.on_condition_, detector->join_info_.on_condition_))) {
        LOG_WARN("failed to append exprs", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (INNER_JOIN == join_info.join_type_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < join_info.where_condition_.count(); ++i) {
          ObRawExpr* qual = join_info.where_condition_.at(i);
          if (OB_ISNULL(qual)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null join qual", K(ret));
          } else if (!qual->has_flag(IS_JOIN_COND)) {
            // do nothing
          } else if (OB_FAIL(join_info.equal_join_condition_.push_back(qual))) {
            LOG_WARN("failed to push back join qual", K(ret));
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < join_info.on_condition_.count(); ++i) {
          ObRawExpr* qual = join_info.on_condition_.at(i);
          if (OB_ISNULL(qual)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null join qual", K(ret));
          } else if (!qual->has_flag(IS_JOIN_COND)) {
            // do nothing
          } else if (OB_FAIL(join_info.equal_join_condition_.push_back(qual))) {
            LOG_WARN("failed to push back join qual", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::distribute_onetime_subquery(ObRawExpr*& qual, bool& can_distribute)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  can_distribute = false;
  ObRawExpr* new_qual = NULL;
  if (OB_ISNULL(qual) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(qual), K(stmt));
  } else if (!qual->has_flag(CNT_SUB_QUERY)) {
    can_distribute = true;
  } else if (OB_FAIL(check_qual_can_distribute(qual, stmt, can_distribute))) {
    LOG_WARN("failed to check subquery can pushdown", K(ret));
  } else if (!can_distribute) {
    // do nothing
  } else if (qual->get_relation_ids().is_empty()) {
    can_distribute = false;
  } else if (OB_FAIL(extract_params_for_subplan_filter(qual, new_qual))) {
    LOG_WARN("failed to extract params for subplan filter", K(ret));
  } else if (OB_ISNULL(new_qual)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_FAIL(new_qual->extract_info())) {
    LOG_WARN("failed extract info", K(ret));
  } else if (OB_FAIL(add_subquery_filter(new_qual))) {
    LOG_WARN("failed to add expr", K(ret));
  } else {
    qual = new_qual;
  }
  return ret;
}

int ObLogPlan::distribute_subquery_qual(ObRawExpr*& qual, bool& can_distribute)
{
  int ret = OB_SUCCESS;
  bool is_semi_subquery = false;
  can_distribute = false;
  if (OB_ISNULL(qual)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(qual));
  } else if (!qual->has_flag(CNT_SUB_QUERY)) {
    can_distribute = true;
  } else if (OB_FAIL(distribute_onetime_subquery(qual, can_distribute))) {
    LOG_WARN("failed to check qual can pushdown", K(ret));
  } else if (can_distribute) {
    // do nothing
  } else if (OB_FAIL(add_subquery_filter(qual))) {
    LOG_WARN("failed to add expr", K(ret));
  } else {
    LOG_TRACE("succeed to distribute subquery qual", K(is_semi_subquery), K(can_distribute));
  }
  return ret;
}

int ObLogPlan::check_qual_can_distribute(ObRawExpr* qual, ObDMLStmt* stmt, bool& can_distribute)
{
  int ret = OB_SUCCESS;
  bool is_onetime_expr = false;
  can_distribute = true;
  if (OB_ISNULL(qual) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(qual), K(stmt), K(ret));
  } else if (!qual->has_flag(CNT_SUB_QUERY)) {
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::check_is_onetime_expr(get_onetime_exprs(), qual, stmt, is_onetime_expr))) {
    LOG_WARN("failed to check is onetime expr", K(ret));
  } else if (is_onetime_expr) {
    // do nothing
  } else if (!is_onetime_expr && qual->has_flag(IS_SUB_QUERY)) {
    can_distribute = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && can_distribute && i < qual->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_qual_can_distribute(qual->get_param_expr(i), stmt, can_distribute)))) {
        LOG_WARN("failed to check qual can push down", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::extract_params_for_subplan_filter(ObRawExpr* expr, ObRawExpr*& new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = expr;
  ObDMLStmt* stmt = NULL;
  ObOptimizerContext* opt_ctx = NULL;
  bool is_onetime_expr = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(opt_ctx = &get_optimizer_context())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(expr), K(stmt), K(opt_ctx), K(ret));
  } else if (expr->has_flag(CNT_SUB_QUERY) &&
             OB_FAIL(ObOptimizerUtil::check_is_onetime_expr(get_onetime_exprs(), expr, stmt, is_onetime_expr))) {
    LOG_WARN("failed to check is onetime expr", K(ret));
  } else if (is_onetime_expr) {
    ObSQLSessionInfo* session_info = const_cast<ObSQLSessionInfo*>(opt_ctx->get_session_info());
    int64_t param_num = ObOptimizerUtil::find_exec_param(get_onetime_exprs(), expr);
    if (param_num >= 0) {
      ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(stmt->get_exec_param_ref_exprs(), param_num);
      if (param_expr != NULL) {
        new_expr = param_expr;
      }
    } else {
      std::pair<int64_t, ObRawExpr*> init_expr;
      if (OB_FAIL(ObRawExprUtils::create_exec_param_expr(
              stmt, opt_ctx->get_expr_factory(), new_expr, session_info, init_expr))) {
        LOG_WARN("create param for stmt error", K(ret));
      } else if (OB_FAIL(onetime_exprs_.push_back(init_expr))) {
        LOG_WARN("push back error", K(ret));
      }
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    ObRawExpr::ExprClass expr_class = expr->get_expr_class();
    switch (expr_class) {
      case ObRawExpr::EXPR_QUERY_REF: {
        break;
      }
      case ObRawExpr::EXPR_CASE_OPERATOR: {
        ObCaseOpRawExpr* case_expr = NULL;
        if (OB_FAIL(opt_ctx->get_expr_factory().create_raw_expr(expr->get_expr_type(), case_expr))) {
          LOG_WARN("create case operator expr failed", K(ret));
        } else if (OB_ISNULL(new_expr = case_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("case_expr is null", K(case_expr));
        } else {
          ret = case_expr->assign(static_cast<ObCaseOpRawExpr&>(*expr));
          if (OB_SUCC(ret)) {
            case_expr->clear_child();
            if (OB_FAIL(extract_params_for_subplan_expr(new_expr, expr))) {
              LOG_WARN("failed to extract child expr info", K(ret), K(*new_expr));
            } else { /*do nothing*/
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_AGGR: {
        ObAggFunRawExpr* aggr_expr = NULL;
        if (OB_FAIL(opt_ctx->get_expr_factory().create_raw_expr(expr->get_expr_type(), aggr_expr))) {
          LOG_WARN("create aggr expr failed", K(ret));
        } else if (OB_ISNULL(new_expr = aggr_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("aggr_expr is null", K(aggr_expr));
        } else {
          ret = aggr_expr->assign(static_cast<ObAggFunRawExpr&>(*expr));
          if (OB_SUCC(ret)) {
            aggr_expr->clear_child();
            if (OB_FAIL(extract_params_for_subplan_expr(new_expr, expr))) {
              LOG_WARN("failed to extract child expr info", K(ret), K(*new_expr));
            } else { /*do nothing*/
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_SYS_FUNC: {
        ObSysFunRawExpr* func_expr = NULL;
        if (OB_FAIL(opt_ctx->get_expr_factory().create_raw_expr(expr->get_expr_type(), func_expr))) {
          LOG_WARN("create system function expr failed", K(ret));
        } else if (OB_ISNULL(new_expr = func_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("func_expr is null", K(func_expr));
        } else {
          ret = func_expr->assign(static_cast<ObSysFunRawExpr&>(*expr));
          if (OB_SUCC(ret)) {
            func_expr->clear_child();
            if (OB_FAIL(extract_params_for_subplan_expr(new_expr, expr))) {
              LOG_WARN("failed to extract child expr info", K(ret), K(*new_expr));
            } else { /*do nothing*/
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_UDF: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case ObRawExpr::EXPR_OPERATOR: {
        ObOpRawExpr* op_expr = NULL;
        if (OB_FAIL(opt_ctx->get_expr_factory().create_raw_expr(expr->get_expr_type(), op_expr))) {
          LOG_WARN("create operator expr failed", K(ret));
        } else if (OB_ISNULL(new_expr = op_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("op_expr is null", K(op_expr));
        } else {
          ret = op_expr->assign(static_cast<ObOpRawExpr&>(*expr));
          if (OB_SUCC(ret)) {
            op_expr->clear_child();
            if (OB_FAIL(extract_params_for_subplan_expr(new_expr, expr))) {
              LOG_WARN("failed to extract child expr info", K(ret), K(*new_expr));
            } else { /*do nothing*/
            }
          }
        }
        break;
      }
      case ObRawExpr::EXPR_INVALID_CLASS:
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr class", K(expr_class), K(ret));
        // should not reach here
        break;
    }  // switch case end
  }
  return ret;
}

int ObLogPlan::extract_params_for_subplan_expr(ObRawExpr*& new_expr, ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_expr) || OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(new_expr), K(expr), K(ret));
  } else {
    int64_t count = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObRawExpr* expr_element = expr->get_param_expr(i);
      if (OB_ISNULL(expr_element)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null poiner passed in", K(i), K(expr_element), K(ret));
      } else if (!expr_element->has_flag(CNT_SUB_QUERY)) {
        if (OB_FAIL(ObOptimizerUtil::add_parameterized_expr(new_expr, expr, expr_element, i))) {
          LOG_WARN("fail to push raw expr", K(ret), K(new_expr));
        } else { /*do nothing*/
        }
      } else {
        ObRawExpr* dest_element = NULL;
        // recursive to child expr extract params
        if (OB_FAIL(SMART_CALL(extract_params_for_subplan_filter(expr_element, dest_element)))) {
          LOG_WARN("failed to extract params", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::add_parameterized_expr(new_expr, expr, dest_element, i))) {
          LOG_WARN("fail to push raw expr", K(ret), K(new_expr));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::try_split_or_qual(const ObRelIds& table_ids, ObOpRawExpr& or_qual, ObIArray<ObRawExpr*>& table_quals)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* new_expr = NULL;
  if (OB_FAIL(
          ObOptimizerUtil::split_or_qual_on_table(get_stmt(), get_optimizer_context(), table_ids, or_qual, new_expr))) {
    LOG_WARN("failed to split or qual on table", K(ret));
  } else if (NULL == new_expr) {
    /* do nothing */
  } else if (new_expr->get_relation_ids().num_members() > 1) {
    // do nothing
  } else if (ObOptimizerUtil::find_equal_expr(table_quals, new_expr)) {
    /* do nothing */
  } else if (OB_FAIL(table_quals.push_back(new_expr))) {
    LOG_WARN("failed to push back new expr", K(ret));
  }
  return ret;
}

int ObLogPlan::generate_subplan(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObArray<ObQueryRefRawExpr*> query_refs;
  if (OB_ISNULL(expr) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(expr), K(get_stmt()));
  } else if (OB_FAIL(get_query_ref_in_expr_tree(expr, get_stmt()->get_current_level(), query_refs))) {
    LOG_WARN("get query reference in expr tree failed", K(ret));
  } else {
    LOG_TRACE("number of subquery ref", K(query_refs.count()), K(get_stmt()->get_current_level()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < query_refs.count(); ++i) {
    if (OB_FAIL(generate_subplan_for_query_ref(query_refs.at(i)))) {
      LOG_WARN("generate subplan for query ref failed", K(ret));
    } else {
      LOG_TRACE("succeed to generate subplan for query ref", K(get_stmt()->get_current_level()), K(query_refs.at(i)));
    }
  }
  return ret;
}

int ObLogPlan::generate_subplan_for_query_ref(ObQueryRefRawExpr* query_ref)
{
  int ret = OB_SUCCESS;
  // check if sub plan has been generated
  SubPlanInfo* subplan_info = NULL;
  ObOptimizerContext& opt_ctx = get_optimizer_context();
  if (OB_FAIL(get_subplan(query_ref, subplan_info))) {
    LOG_WARN("get_subplan() fails", K(ret));
  } else if (NULL == subplan_info && query_ref->is_ref_stmt()) {
    ObSelectStmt* subquery = query_ref->get_ref_stmt();
    if (NULL != subquery) {
      ObArray<std::pair<int64_t, ObRawExpr*>> exec_params;
      ObLogPlan* logical_plan = NULL;
      if (OB_ISNULL(logical_plan = opt_ctx.get_log_plan_factory().create(opt_ctx, *subquery))) {
        LOG_WARN("failed to create plan", K(ret), K(subquery->get_sql_stmt()));
      } else if (OB_FAIL(logical_plan->init_plan_info())) {
        LOG_WARN("failed to init equal sets", K(ret));
      } else if (OB_FAIL(
                     logical_plan->extract_params_for_subplan_in_stmt(get_stmt()->get_current_level(), exec_params))) {
        LOG_WARN("failed to extract params from subplan", K(ret));
      } else if (OB_FAIL(static_cast<ObSelectLogPlan*>(logical_plan)->generate_raw_plan())) {
        LOG_WARN("failed to optimize sub-select", K(ret));
      } else {
        SubPlanInfo* info = static_cast<SubPlanInfo*>(get_allocator().alloc(sizeof(SubPlanInfo)));
        bool has_ref_assign_user_var = false;
        if (OB_ISNULL(info)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to alloc semi info", K(ret));
        } else if (OB_FAIL(subquery->has_ref_assign_user_var(has_ref_assign_user_var))) {
          LOG_WARN("faield to check stmt has assignment ref user var", K(ret));
        } else {
          bool is_initplan = exec_params.empty() && !has_ref_assign_user_var;
          info = new (info) SubPlanInfo(query_ref, logical_plan, is_initplan);
          if (OB_FAIL(info->add_sp_params(exec_params))) {
            LOG_WARN("failed to add sp params to SubPlanInfo", K(ret));
          } else if (OB_FAIL(add_subplan(info))) {
            LOG_WARN("failed to add sp params to rel", K(ret));
          } else {
            query_ref->set_ref_id(get_subplan_size() - 1);
            query_ref->set_ref_operator(logical_plan->get_plan_root());
            logical_plan->set_query_ref(query_ref);
            LOG_TRACE("succ to generate logical plan of sub-select");
          }
        }

        if (OB_FAIL(ret) && NULL != info) {
          info->subplan_ = NULL;  // we leave logical plan to be freed later
          info->~SubPlanInfo();
          info = NULL;
        } else { /* Do nothing */
        }
      }
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }
  return ret;
}

int ObLogPlan::get_query_ref_in_expr_tree(
    ObRawExpr* expr, int32_t expr_level, ObIArray<ObQueryRefRawExpr*>& query_refs) const
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("root is null");
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (!expr->has_flag(CNT_SUB_QUERY)) {
    /*do nothing if no subquery*/
  } else if (expr->is_query_ref_expr()) {
    ObQueryRefRawExpr* query_ref = static_cast<ObQueryRefRawExpr*>(expr);
    if (query_ref->get_ref_stmt() != NULL) {
      if (query_ref->get_expr_level() == expr_level) {
        if (OB_FAIL(query_refs.push_back(query_ref))) {
          LOG_WARN("add query ref to array failed", K(ret));
        } else {
          LOG_TRACE("subquery belong to current level", K(query_ref), K(expr_level), K(*query_ref));
        }
      } else {
        LOG_TRACE("subquery does not belong to current level",
            K(expr_level),
            K(query_ref->get_expr_level()),
            K(query_ref),
            K(*query_ref));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(get_query_ref_in_expr_tree(expr->get_param_expr(i), expr_level, query_refs)))) {
        LOG_WARN("get query reference in expr tree", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::extract_params_for_subplan_in_stmt(
    const int32_t level, ObIArray<std::pair<int64_t, ObRawExpr*>>& exec_params)
{
  int ret = OB_SUCCESS;
  ret = resolve_unsolvable_exprs(level, get_stmt(), exec_params);
  return ret;
}

int ObLogPlan::resolve_unsolvable_exprs(
    const int32_t level, ObDMLStmt* stmt, ObIArray<std::pair<int64_t, ObRawExpr*>>& params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    ObArray<ObSelectStmt*> child_stmts;
    if (OB_FAIL(extract_stmt_params(stmt, level, params))) {
      LOG_WARN("fail to extract param from raw expr", K(ret));
    } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        if (OB_FAIL(SMART_CALL(resolve_unsolvable_exprs(level, child_stmts.at(i), params)))) {
          LOG_WARN("fail to extract param from child stmts", K(ret));
        } else { /* Do nothing */
        }
      }
    }
  }

  return ret;
}

int ObLogPlan::extract_stmt_params(
    ObDMLStmt* stmt, const int32_t level, ObIArray<std::pair<int64_t, ObRawExpr*>>& params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (stmt->get_current_level() <= level) {
  } else if (!stmt->is_select_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid SubQuery type", K(ret), K(stmt->get_stmt_type()));
  } else {
    ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
    // 1, Select Clause
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
      if (OB_FAIL(extract_params(select_stmt->get_select_item(i).expr_, level, params))) {
        LOG_WARN("fail to resolve ref param for select clause", K(ret));
      } else { /* Do nothing */
      }
    }
    // 2.1, join table
    if (OB_SUCC(ret) && select_stmt->get_joined_tables().count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_joined_tables().count(); ++i) {
        JoinedTable* join_table = select_stmt->get_joined_tables().at(i);
        if (OB_ISNULL(join_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("join_table is null", K(ret));
        } else if (OB_FAIL(extract_join_table_condition_params(*join_table, level, params))) {
          LOG_WARN("extract join table condition params failed", K(ret));
        } else { /* Do nothing */
        }
      }
    } else { /* Do nothing */
    }
    // 2.2, semi info
    if (OB_SUCC(ret) && select_stmt->get_semi_info_size() > 0) {
      ObIArray<SemiInfo*>& semi_infos = select_stmt->get_semi_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
        if (OB_ISNULL(semi_infos.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(semi_infos));
        } else if (OB_FAIL(extract_params(semi_infos.at(i)->semi_conditions_, level, params))) {
          LOG_WARN("extract semi condition params failed", K(ret));
        } else { /* Do nothing */
        }
      }
    } else { /* Do nothing */
    }
    // 3, Where Clause
    if (OB_SUCC(ret)) {
      ret = extract_params(select_stmt->get_condition_exprs(), level, params);
    } else { /* Do nothing */
    }
    // 4.1, Group Clause
    if (OB_SUCC(ret)) {
      ret = extract_params(select_stmt->get_group_exprs(), level, params);
    }
    // 4.2, Rollup Clause
    if (OB_SUCC(ret)) {
      ret = extract_params(select_stmt->get_rollup_exprs(), level, params);
    }
    // 4.3, Agg Clause aggregate expr
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); ++i) {
      ObRawExpr* aggr_expr = select_stmt->get_aggr_item(i);
      ret = extract_params(aggr_expr, level, params);
    }
    // 5, Having Clause
    if (OB_SUCC(ret)) {
      ret = extract_params(select_stmt->get_having_exprs(), level, params);
    } else { /* Do nothing */
    }
    // 6, Order Clause
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_order_item_size(); ++i) {
      ret = extract_params(select_stmt->get_order_item(i).expr_, level, params);
    }
    // 7, Limit Clause
    if (OB_SUCC(ret) && select_stmt->get_limit_expr()) {
      ret = extract_params(select_stmt->get_limit_expr(), level, params);
    } else { /* Do nothing */
    }
    if (OB_SUCC(ret) && select_stmt->get_offset_expr()) {
      ret = extract_params(select_stmt->get_offset_expr(), level, params);
    } else { /* Do nothing */
    }
    // 8, start with Clause
    if (OB_SUCC(ret)) {
      ret = extract_params(select_stmt->get_start_with_exprs(), level, params);
    } else { /* Do nothing */
    }
    // 9. connect_by_prior exprs
    if (OB_SUCC(ret)) {
      ret = extract_params(select_stmt->get_connect_by_prior_exprs(), level, params);
    } else { /* Do nothing */
    }

    if (OB_SUCC(ret)) {
      ret = extract_params(get_rownum_exprs(), level, params);
    } else { /* Do nothing */
    }
  }
  return ret;
}

int ObLogPlan::extract_join_table_condition_params(
    JoinedTable& join_table, int32_t level, ObIArray<std::pair<int64_t, ObRawExpr*>>& params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_FAIL(extract_params(join_table.join_conditions_, level, params))) {
    LOG_WARN("extract join condition params failed", K(ret));
  } else {
    if (NULL != join_table.left_table_ && TableItem::JOINED_TABLE == join_table.left_table_->type_) {
      JoinedTable& left_table = *static_cast<JoinedTable*>(join_table.left_table_);
      if (OB_FAIL(SMART_CALL(extract_join_table_condition_params(left_table, level, params)))) {
        LOG_WARN("extract left join table condition param failed", K(ret));
      } else { /* Do nothing */
      }
    } else { /* Do nothing */
    }
    if (OB_SUCC(ret) && NULL != join_table.right_table_ && TableItem::JOINED_TABLE == join_table.right_table_->type_) {
      JoinedTable& right_table = *static_cast<JoinedTable*>(join_table.right_table_);
      if (OB_FAIL(SMART_CALL(extract_join_table_condition_params(right_table, level, params)))) {
        LOG_WARN("extract right join table condition param failed", K(ret));
      } else { /* Do nothing */
      }
    } else { /* Do nothing */
    }
  }
  return ret;
}

int ObLogPlan::extract_params(
    common::ObIArray<ObRawExpr*>& exprs, const int32_t level, ObIArray<std::pair<int64_t, ObRawExpr*>>& params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(extract_params(exprs.at(i), level, params))) {
      LOG_WARN("extract expr params failed", K(ret));
    } else { /* Do nothing */
    }
  }
  return ret;
}

int ObLogPlan::extract_params(ObRawExpr*& expr, const int32_t level, ObIArray<std::pair<int64_t, ObRawExpr*>>& params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  bool can_produce = false;
  if (OB_ISNULL(expr) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), KP(expr), KP(get_stmt()));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (!expr->get_expr_levels().has_member(level) && !expr->is_set_op_expr()) {
    // do nothing
  } else if (OB_FAIL(can_produce_by_upper_stmt(expr, level, can_produce))) {
    LOG_WARN("failed to check expr can produce by stmt", K(ret));
  } else if (can_produce) {
    if (OB_FAIL(extract_param_for_generalized_column(expr, params))) {
      LOG_WARN("extract param for generalized column failed", K(ret));
    }
  } else {
    int64_t count = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr->get_param_expr(i) returns null", K(ret), K(i));
      } else if (OB_FAIL(SMART_CALL(extract_params(expr->get_param_expr(i), level, params)))) {
        LOG_WARN("fail to extract param from raw expr", K(ret), K(expr->get_param_expr(i)));
      } else { /* Do nothing */
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(expr->extract_info())) {
    LOG_WARN("failed to extract expr info", K(ret));
  }
  return ret;
}

int ObLogPlan::can_produce_by_upper_stmt(ObRawExpr* expr, const int32_t stmt_level, bool& can_produce)
{
  int ret = OB_SUCCESS;
  can_produce = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->get_expr_level() == stmt_level || expr->is_const_expr()) {
    can_produce = true;
  } else if (ObOptimizerUtil::has_psedu_column(*expr) || ObOptimizerUtil::has_hierarchical_expr(*expr) ||
             expr->is_set_op_expr() || expr->has_flag(IS_AGG) || expr->has_flag(IS_WINDOW_FUNC) ||
             T_OP_ROW == expr->get_expr_type()) {
    can_produce = false;
  } else if (expr->get_param_count() > 0) {
    can_produce = true;
    int64_t count = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && can_produce && i < count; ++i) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr->get_param_expr(i) returns null", K(ret), K(i));
      } else if (OB_FAIL(SMART_CALL(can_produce_by_upper_stmt(expr->get_param_expr(i), stmt_level, can_produce)))) {
        LOG_WARN("fail to check expr can produce by stmt", K(ret), K(expr->get_param_expr(i)));
      } else { /* Do nothing */
      }
    }
  }
  return ret;
}

int ObLogPlan::extract_param_for_generalized_column(
    ObRawExpr*& general_expr, ObIArray<std::pair<int64_t, ObRawExpr*>>& params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(general_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("general expr is null");
  } else {
    int64_t param_num = ObOptimizerUtil::find_exec_param(params, general_expr);
    if (param_num >= 0) {
      // stmt all_exprs contain all questionmark exprs
      ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(get_stmt()->get_exec_param_ref_exprs(), param_num);
      if (NULL != param_expr) {
        general_expr = param_expr;
      } else { /* Do nothing */
      }
    } else {
      std::pair<int64_t, ObRawExpr*> init_expr;
      if (OB_FAIL(ObRawExprUtils::create_exec_param_expr(
              get_stmt(), optimizer_context_.get_expr_factory(), general_expr, NULL, init_expr))) {
        LOG_WARN("create param for stmt error in extract_params_for_nl", K(ret));
      } else if (OB_FAIL(params.push_back(init_expr))) {
        LOG_WARN("push back error", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogPlan::get_subplan(const ObRawExpr* expr, SubPlanInfo*& info)
{
  int ret = OB_SUCCESS;
  info = NULL;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < get_subplans().count(); ++i) {
    if (OB_ISNULL(get_subplans().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get_subplans().at(i) returns null", K(ret), K(i));
    } else if (get_subplans().at(i)->init_expr_ == expr) {
      info = get_subplans().at(i);
      found = true;
    } else { /* Do nothing */
    }
  }
  return ret;
}

int ObLogPlan::get_subplan_info(const ObRawExpr* expr, ObIArray<SubPlanInfo*>& subplans)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw expr", K(ret), K(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    if (expr->is_query_ref_expr()) {
      SubPlanInfo* info = NULL;
      if (OB_FAIL(get_subplan(expr, info))) {
        LOG_WARN("get_subplan() fails", K(ret));
      } else if (NULL != info) {
        ret = subplans.push_back(info);
      } else { /* Do nothing */
      }
    } else if (expr->is_const_expr()) {
      if (expr->has_flag(IS_PARAM)) {
        int64_t param_value = static_cast<const ObConstRawExpr*>(expr)->get_value().get_unknown();
        ObRawExpr* init_expr = NULL;
        if (NULL == (init_expr = ObOptimizerUtil::find_exec_param(get_onetime_exprs(), param_value))) {
        } else {
          ret = SMART_CALL(get_subplan_info(init_expr, subplans));
        }
      } else { /* Do nothing */
      }
    } else {
      int64_t N = expr->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        if (expr->get_param_expr(i)->has_flag(CNT_SUB_QUERY) || expr->get_param_expr(i)->has_flag(CNT_PARAM)) {
          ret = SMART_CALL(get_subplan_info(expr->get_param_expr(i), subplans));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::find_base_rel(ObIArray<ObJoinOrder*>& base_level, int64_t table_idx, ObJoinOrder*& base_rel)
{
  int ret = OB_SUCCESS;
  ObJoinOrder* cur_rel = NULL;
  bool find = false;
  base_rel = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < base_level.count(); ++i) {
    if (OB_ISNULL(cur_rel = base_level.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_rel));
    } else if (cur_rel->get_tables().has_member(table_idx)) {
      find = true;
      base_rel = cur_rel;
      LOG_TRACE("succeed to find base rel", K(cur_rel->get_tables()), K(table_idx));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObLogPlan::find_join_rel(ObIArray<ObJoinOrder*>& join_level, ObRelIds& relids, ObJoinOrder*& join_rel)
{
  int ret = OB_SUCCESS;
  join_rel = NULL;
  ObJoinOrder* cur_rel = NULL;
  int32_t i = 0;

  bool found = false;
  for (i = 0; OB_SUCC(ret) && !found && i < join_level.count(); ++i) {
    cur_rel = join_level.at(i);
    if (OB_ISNULL(cur_rel)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_rel is null", K(ret), K(cur_rel));
    } else if (cur_rel->get_tables().equal(relids)) {
      join_rel = cur_rel;
      LOG_TRACE("join rel is found");
      found = true;
    } else { /* Do nothing */
    }
  }
  return ret;
}

int ObLogPlan::sort_cur_rels(ObIArray<ObJoinOrder*>& cur_rels)
{
  int ret = OB_SUCCESS;
  int64_t N = cur_rels.count();
  bool swap = true;
  for (int64_t i = 0; OB_SUCC(ret) && swap && i < N; ++i) {
    swap = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < N - 1 - i; ++j) {
      if (OB_ISNULL(cur_rels.at(j)) || OB_ISNULL(cur_rels.at(j + 1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rel is null", K(cur_rels), K(ret));
      } else if (cur_rels.at(j)->get_output_rows() > cur_rels.at(j + 1)->get_output_rows()) {
        ObJoinOrder* tmp = cur_rels.at(j);
        cur_rels.at(j) = cur_rels.at(j + 1);
        cur_rels.at(j + 1) = tmp;
        swap = true;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_function_table_path(AccessPath* ap, ObLogicalOperator*& out_access_path_op)
{
  int ret = OB_SUCCESS;
  ObLogFunctionTable* op = NULL;
  if (OB_ISNULL(op = static_cast<ObLogFunctionTable*>(get_log_op_factory().allocate(*this, LOG_FUNCTION_TABLE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for ObLogFunctionTableScan failed", K(ret));
  } else {
    const TableItem* table_item = NULL;
    CK(OB_NOT_NULL(get_stmt()));
    CK(OB_NOT_NULL(table_item = get_stmt()->get_table_item_by_id(ap->get_table_id())));
    CK(OB_NOT_NULL(table_item));
    CK(table_item->is_function_table());
    CK(OB_NOT_NULL(table_item->function_table_expr_));
    if (OB_SUCC(ret) && table_item->function_table_expr_->has_flag(CNT_SUB_QUERY)) {
      ObLogExprValues* values_op = NULL;
      if (OB_ISNULL(values_op = static_cast<ObLogExprValues*>(log_op_factory_.allocate(*this, LOG_EXPR_VALUES)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Allocate memory for ObLogInsertValues failed", K(ret));
      } else {
        values_op->set_card(1);
        values_op->set_cost(ObOptEstCost::get_cost_params().CPU_TUPLE_COST);
        values_op->set_op_cost(values_op->get_cost());
        values_op->set_est_sel_info(&get_est_sel_info());
        values_op->set_need_columnlized(true);
        op->add_child(values_op);
      }
      ObArray<ObRawExpr*> subquery_exprs;
      OZ(subquery_exprs.push_back(table_item->function_table_expr_));
      OZ(allocate_subplan_filter_as_below(subquery_exprs, op));
    }
    if (OB_SUCC(ret)) {
      // compute property anyway
      OX(op->add_values_expr(table_item->function_table_expr_));
      OX(op->set_table_id(ap->get_table_id()));
      OZ(append(op->get_filter_exprs(), ap->filter_));
      if (OB_FAIL(op->compute_property(ap))) {
        LOG_WARN("failed to compute property", K(ret));
      }
    }
    OX(out_access_path_op = op);
  }
  return ret;
}

int ObLogPlan::allocate_temp_table_path(AccessPath* ap, ObLogicalOperator*& out_access_path_op)
{
  int ret = OB_SUCCESS;
  const TableItem* table_item = NULL;
  ObLogTempTableAccess* op = NULL;
  if (OB_ISNULL(ap) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(ap->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ap), K(get_stmt()), K(table_item), K(ret));
  } else if (OB_ISNULL(
                 op = static_cast<ObLogTempTableAccess*>(log_op_factory_.allocate(*this, LOG_TEMP_TABLE_ACCESS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for ObLogFunctionTableScan failed", K(ret));
  } else {
    op->set_table_id(ap->get_table_id());
    op->set_ref_table_id(ap->get_ref_table_id());
    op->get_table_name().assign_ptr(table_item->table_name_.ptr(), table_item->table_name_.length());
    op->set_last_access(static_cast<ObSelectStmt*>(get_stmt())->is_last_access());
    if (OB_FAIL(op->get_filter_exprs().assign(ap->filter_))) {
      LOG_WARN("failed to assign filter exprs", K(ret));
    } else if (OB_FAIL(op->compute_property(ap))) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      out_access_path_op = op;
    }
  }
  return ret;
}

int ObLogPlan::allocate_access_path(AccessPath* ap, ObLogicalOperator*& out_access_path_op)
{
  int ret = OB_SUCCESS;
  ObLogTableScan* scan = NULL;
  ObJoinOrder* join_order = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  ObLogOpType op_type = LOG_TABLE_SCAN;
  const ObTableSchema* table_schema = NULL;
  const TableItem* table_item = NULL;
  if (OB_ISNULL(ap) || OB_ISNULL(get_stmt()) || OB_ISNULL(join_order = ap->parent_) ||
      OB_ISNULL(ap->get_sharding_info()) || OB_ISNULL(schema_guard = get_optimizer_context().get_sql_schema_guard()) ||
      OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(ap->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ap), K(get_stmt()), K(schema_guard), K(join_order), K(table_item), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ap->table_id_, table_schema))) {
    LOG_WARN("fail to get_table_schema", K(ret));
  } else if (!table_schema) {
    // do nothing
  } else if (share::schema::MATERIALIZED_VIEW == table_schema->get_table_type()) {
    op_type = LOG_MV_TABLE_SCAN;
  } else { /*do nothing*/
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (scan = static_cast<ObLogTableScan*>(get_log_op_factory().allocate(*this, op_type))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate table/index operator", K(ret));
    } else if (OB_FAIL(scan->set_est_row_count_record(ap->est_records_))) {
      LOG_WARN("failed to set estimation info", K(ret));
    } else if (OB_FAIL(append(scan->get_pushdown_filter_exprs(), ap->pushdown_filters_))) {
      LOG_WARN("failed to append pushdown filters", K(ret));
    } else {
      bool is_index_back = ap->est_cost_info_.index_meta_info_.is_index_back_;
      ObSEArray<ObRawExpr*, 4> global_index_filters;
      ObSEArray<ObRawExpr*, 4> remaining_filters;
      scan->set_est_cost_info(&ap->get_cost_table_scan_info());
      scan->set_table_id(ap->get_table_id());
      scan->set_ref_table_id(ap->get_ref_table_id());
      scan->set_index_table_id(ap->get_index_table_id());
      scan->set_scan_direction(ap->order_direction_);
      scan->set_is_index_global(ap->is_global_index_);
      scan->set_is_global_index_back(is_index_back);
      scan->set_phy_location_type(ap->get_sharding_info()->get_location_type());
      scan->set_table_opt_info(ap->table_opt_info_);
      if (is_index_back && ap->is_global_index_) {
        scan->set_index_back(false);
      } else {
        scan->set_index_back(is_index_back);
      }
      if (ap->is_global_index_) {
        if (OB_ISNULL(ap->table_partition_info_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          scan->set_table_partition_info(const_cast<ObTablePartitionInfo*>(ap->table_partition_info_));
        }
      } else {
        scan->set_table_partition_info(&join_order->get_table_partition_info());
      }
      scan->set_sample_info(ap->sample_info_);
      if (NULL != table_schema && table_schema->is_tmp_table()) {
        scan->set_session_id(table_schema->get_session_id());
      }
      if (LOG_MV_TABLE_SCAN == op_type) {
        ObLogMVTableScan* mv_scan = static_cast<ObLogMVTableScan*>(scan);
        const ObIArray<uint64_t>& depend_tids = table_schema->get_depend_table_ids();
        if (depend_tids.count() != 1) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("depend_tid count is not 1", K(depend_tids.count()));
        } else {
          uint64_t depend_tid = depend_tids.at(0);
          mv_scan->set_depend_table_id(depend_tid);
        }
      }
      scan->set_table_row_count(ap->table_row_count_);
      scan->set_output_row_count(ap->output_row_count_);
      scan->set_phy_query_range_row_count(ap->phy_query_range_row_count_);
      scan->set_query_range_row_count(ap->query_range_row_count_);
      scan->set_index_back_row_count(ap->index_back_row_count_);
      scan->set_estimate_method(ap->est_cost_info_.row_est_method_);
      scan->set_is_fake_cte_table(ap->is_cte_path());
      scan->set_pre_query_range(ap->pre_query_range_);
      if (OB_FAIL(scan->set_update_info())) {
        LOG_WARN("Fail to set_update_info", K(ret));
      } else if (!ap->is_inner_path_ && OB_FAIL(scan->set_query_ranges(ap->get_cost_table_scan_info().ranges_))) {
        LOG_WARN("failed to set query ranges", K(ret));
      } else if (OB_FAIL(scan->set_range_columns(ap->get_cost_table_scan_info().range_columns_))) {
        LOG_WARN("failed to set range column", K(ret));
      } else {  // set table name and index name
        scan->set_table_name(table_item->get_table_name());
        scan->set_diverse_path_count(ap->parent_->get_diverse_path_count());
        if (ap->get_index_table_id() != ap->get_ref_table_id()) {
          if (OB_FAIL(store_index_column_ids(*schema_guard, *scan, ap->get_index_table_id()))) {
            LOG_WARN("Failed to store index column id", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (ap->is_global_index_ && is_index_back && ap->est_cost_info_.table_filters_.count() > 0) {
          if (OB_FAIL(get_global_index_filters(*ap, global_index_filters, remaining_filters))) {
            LOG_WARN("failed to get global index filters", K(ret));
          } else if (OB_FAIL(scan->set_filters(global_index_filters))) {
            LOG_WARN("failed to set filters", K(ret));
          } else { /*do nothing*/
          }
        } else {
          if (OB_FAIL(scan->set_filters(ap->filter_))) {
            LOG_WARN("failed to set filters", K(ret));
          } else { /* do nothing */
          }
        }
      } else { /*do nothing*/
      }

      if (OB_SUCC(ret)) {
        // If assign partitions to scan, set partitions
        if (!ap->is_global_index_ && get_stmt()->get_stmt_hint().part_hints_.count() > 0) {
          scan->set_part_hint(get_stmt()->get_stmt_hint().get_part_hint(ap->get_table_id()));
        } else { /* Do nothing */
        }
      } else { /* Do nothing */
      }

      // Get ObRpcScanHint
      if (OB_SUCC(ret)) {
        const ObQueryHint& query_hint = get_optimizer_context().get_query_hint();
        if (INVALID_CONSISTENCY != query_hint.read_consistency_) {
          scan->set_exist_hint(true);
          scan->get_hint().read_consistency_ = query_hint.read_consistency_;
          if (FROZEN == query_hint.read_consistency_) {
            scan->get_hint().frozen_version_ = query_hint.frozen_version_;
          } else { /* Do nothing */
          }
        } else { /* Do nothing */
        }
        // TODO in 052dev, else hint.read_consistency_ = table_schema->get_consistency_level()
        if (query_hint.force_refresh_lc_) {
          scan->set_exist_hint(true);
          scan->get_hint().force_refresh_lc_ = true;
        }
      } else { /* Do nothing */
      }

      // compute equal set
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scan->compute_property(ap))) {
          LOG_WARN("failed to compute property", K(ret));
        }
      }

      // init part/subpart expr for query range prune
      if (OB_SUCC(ret)) {
        uint64_t table_id = scan->get_table_id();
        uint64_t ref_table_id = scan->get_location_table_id();
        share::schema::ObPartitionLevel part_level = share::schema::PARTITION_LEVEL_MAX;
        if (is_virtual_table(ref_table_id) || is_inner_table(ref_table_id) || is_cte_table(ref_table_id) ||
            is_link_table_id(ref_table_id)) {
          // do nothing
        } else {
          ObRawExpr* part_expr = NULL;
          ObRawExpr* subpart_expr = NULL;
          OZ(scan->get_part_exprs(table_id, ref_table_id, part_level, part_expr, subpart_expr));
          scan->set_part_expr(part_expr);
          scan->set_subpart_expr(subpart_expr);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(append(scan->get_startup_exprs(), get_startup_filters()))) {
          LOG_WARN("failed to append startup filters", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (ap->is_global_index_ && is_index_back) {
          ObLogicalOperator* temp_op = NULL;
          if (OB_FAIL(allocate_table_lookup(
                  *ap, scan->get_table_name(), scan->get_index_name(), scan, remaining_filters, temp_op))) {
            LOG_WARN("failed to allocate table lookup operator", K(ret));
          } else if (OB_ISNULL(temp_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null operator", K(ret));
          } else {
            out_access_path_op = temp_op;
          }
        } else {
          out_access_path_op = scan;
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::get_global_index_filters(
    const AccessPath& ap, ObIArray<ObRawExpr*>& global_index_filters, ObIArray<ObRawExpr*>& remaining_filters)
{
  int ret = OB_SUCCESS;
  ObSEArray<bool, 4> filter_before_index_back;
  if (OB_FAIL(ObOptimizerUtil::check_filter_before_indexback(ap.index_id_,
          get_optimizer_context().get_schema_guard(),
          ap.filter_,
          false,
          OB_INVALID_ID,
          filter_before_index_back))) {
    LOG_WARN("Failed to check filter before index back", K(ret));
  } else if (OB_UNLIKELY(filter_before_index_back.count() != ap.filter_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unequal array size", K(filter_before_index_back.count()), K(ap.filter_.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_before_index_back.count(); i++) {
      if (OB_ISNULL(ap.filter_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (filter_before_index_back.at(i) && !ap.filter_.at(i)->has_flag(CNT_SUB_QUERY)) {
        if (OB_FAIL(global_index_filters.push_back(ap.filter_.at(i)))) {
          LOG_WARN("failed to push back filter", K(ret));
        } else { /*do nothing*/
        }
      } else {
        if (OB_FAIL(remaining_filters.push_back(ap.filter_.at(i)))) {
          LOG_WARN("failed to push back filter", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_table_lookup(AccessPath& ap, ObString& table_name, ObString& index_name,
    ObLogicalOperator* child_node, ObIArray<ObRawExpr*>& filter_exprs, ObLogicalOperator*& output_op)
{
  int ret = OB_SUCCESS;
  ObLogTableLookup* table_lookup = NULL;
  ObLogTableScan* table_scan = NULL;
  if (OB_ISNULL(child_node) || OB_ISNULL(get_stmt()) || OB_ISNULL(ap.parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child_node), K(get_stmt()), K(ap.parent_), K(ret));
  } else if (OB_UNLIKELY(!ap.is_global_index_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not global index", K(ap.is_global_index_), K(ret));
  } else if (OB_UNLIKELY(!ap.est_cost_info_.index_meta_info_.is_index_back_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not index back", K(ret));
  } else if (OB_ISNULL(table_lookup = static_cast<ObLogTableLookup*>(
                           get_log_op_factory().allocate(*this, log_op_def::LOG_TABLE_LOOKUP)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate operator", K(ret));
  } else if (OB_ISNULL(table_scan = static_cast<ObLogTableScan*>(
                           get_log_op_factory().allocate(*this, log_op_def::LOG_TABLE_SCAN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate operator", K(ret));
  } else {
    // set up table look up operator
    table_lookup->set_table_id(ap.table_id_);
    table_lookup->set_ref_table_id(ap.ref_table_id_);
    table_lookup->set_index_id(ap.index_id_);
    table_lookup->set_table_name(table_name);
    table_lookup->set_index_name(index_name);
    table_lookup->set_index_back_scan(table_scan);
    table_lookup->set_child(0, child_node);

    // set up index back op
    table_scan->set_table_id(ap.table_id_);
    table_scan->set_ref_table_id(ap.ref_table_id_);
    table_scan->set_index_table_id(ap.ref_table_id_);
    table_scan->set_is_index_global(false);
    table_scan->set_is_global_index_back(false);
    table_scan->set_index_back(false);
    table_scan->set_is_multi_part_table_scan(true);
    table_scan->set_table_partition_info(&ap.parent_->get_table_partition_info());
    table_scan->set_scan_direction(ap.order_direction_);
    table_scan->set_update_info();
    // set up filter
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append(table_scan->get_filter_exprs(), filter_exprs))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(append(table_lookup->get_pushdown_filter_exprs(), ap.pushdown_filters_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(table_lookup->init_calc_part_id_expr())) {
        LOG_WARN("fail to init calc part id expr", K(ret));
      } else {
        output_op = table_lookup;
      }
    }
    // compute property
    if (OB_SUCC(ret)) {
      if (OB_FAIL(output_op->compute_property(&ap))) {
        LOG_WARN("faield to compute property", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::store_index_column_ids(ObSqlSchemaGuard& schema_guard, ObLogTableScan& scan, const int64_t index_id)
{
  int ret = OB_SUCCESS;
  ObString index_name;
  const ObTableSchema* index_schema = NULL;
  if (OB_FAIL(schema_guard.get_table_schema(index_id, index_schema))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("set index name error", K(ret), K(index_id), K(index_schema));
  } else {
    if (index_schema->is_materialized_view()) {
      index_name = index_schema->get_table_name_str();
    } else {
      if (OB_FAIL(index_schema->get_index_name(index_name))) {
        LOG_WARN("fail to get index name", K(index_name), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    scan.set_index_name(index_name);
    for (ObTableSchema::const_column_iterator iter = index_schema->column_begin();
         OB_SUCC(ret) && iter != index_schema->column_end();
         ++iter) {
      if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is null", K(ret), K(iter));
      } else {
        const ObColumnSchemaV2* column_schema = *iter;
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_schema is null", K(ret));
        } else if (OB_FAIL(scan.add_idx_column_id(column_schema->get_column_id()))) {
          LOG_WARN("Fail to add column id to scan", K(ret));
        } else {
        }  // do nothing
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_join_path(JoinPath* join_path, ObLogicalOperator*& out_join_path_op)
{
  int ret = OB_SUCCESS;
  ObJoinOrder* join_order = NULL;
  if (OB_ISNULL(join_path) || OB_ISNULL(join_order = join_path->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(join_path), K(join_order));
  } else {
    Path* left_path = const_cast<Path*>(join_path->left_path_);
    Path* right_path = const_cast<Path*>(join_path->right_path_);
    ObLogicalOperator* left_child = NULL;
    ObLogicalOperator* right_child = NULL;
    ObLogJoin* join_op = NULL;
    if (OB_ISNULL(left_path) || OB_ISNULL(right_path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(left_path), K(right_path));
    } else if (OB_FAIL(create_plan_tree_from_path(left_path, left_child)) ||
               OB_FAIL(create_plan_tree_from_path(right_path, right_child))) {
      LOG_WARN("failed to create plan tree from path", K(ret));
    } else if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get expected null", K(left_child), K(right_child), K(ret));
    } else if (join_path->left_need_sort_ &&
               OB_FAIL(allocate_sort_as_top(left_child, join_path->left_expected_ordering_))) {
      LOG_WARN("failed to allocate sort operator", K(ret));
    } else if (join_path->need_mat_ && (join_path->join_type_ != CONNECT_BY_JOIN) &&
               OB_FAIL(allocate_material_as_top(right_child))) {
      LOG_WARN("failed to allocate material operator", K(ret));
    } else if (join_path->right_need_sort_ &&
               OB_FAIL(allocate_sort_as_top(right_child, join_path->right_expected_ordering_))) {
      LOG_WARN("failed to allocate sort operator", K(ret));
    } else if (OB_ISNULL(join_op = static_cast<ObLogJoin*>(get_log_op_factory().allocate(*this, LOG_JOIN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate join_op operator", K(ret));
    } else if (OB_ISNULL(join_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(join_op));
    } else {
      /*
       * Prefix sort check for outer merge join may have problems
       * for example, select * from t1 left join t2 on t1.a = t2.a where t2.a is null
       * in this case sort for t2 will use prefix sort since t2.a is null in our condition, but it is wrong
       * we should maintain this info when generating merge join
       */
      if ((join_path->join_type_ == LEFT_OUTER_JOIN || join_path->join_type_ == FULL_OUTER_JOIN) &&
          log_op_def::LOG_SORT == right_child->get_type()) {
        static_cast<ObLogSort*>(right_child)->set_prefix_pos(0);
      }
      if ((join_path->join_type_ == RIGHT_OUTER_JOIN || join_path->join_type_ == FULL_OUTER_JOIN) &&
          log_op_def::LOG_SORT == left_child->get_type()) {
        static_cast<ObLogSort*>(left_child)->set_prefix_pos(0);
      }
      if (right_path->is_inner_path() &&
          (LEFT_SEMI_JOIN == join_path->join_type_ || LEFT_ANTI_JOIN == join_path->join_type_)) {
        bool re_est = false;
        if (OB_FAIL(right_child->re_est_cost(join_op, 1, re_est))) {
          LOG_WARN("failed to re est cost", K(ret));
        }
      }
      join_op->set_left_child(left_child);
      join_op->set_right_child(right_child);
      join_op->set_join_type(join_path->join_type_);
      join_op->set_join_algo(join_path->join_algo_);
      join_op->set_anti_or_semi_sel(join_order->get_anti_or_semi_match_sel());
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(join_op->set_nl_params(right_path->nl_params_))) {
        LOG_WARN("set_nl_params fails", K(ret));
      } else if (OB_FAIL(join_op->set_join_conditions(join_path->equal_join_condition_))) {
        LOG_WARN("append error in allocate_join_path", K(ret));
      } else if (IS_OUTER_OR_CONNECT_BY_JOIN(join_path->join_type_)) {
        if (OB_FAIL(extract_level_pseudo_as_param(join_path->other_join_condition_, join_op->get_exec_params()))) {
          LOG_WARN("Failed to extract level pseudo as param", K(ret));
        } else if (OB_FAIL(append(join_op->get_join_filters(), join_path->other_join_condition_))) {
          LOG_WARN("failed to allocate filter", K(ret));
        } else if (OB_FAIL(append(join_op->get_filter_exprs(), join_path->filter_))) {
          LOG_WARN("failed to allocate filter", K(ret));
        } else {
          LOG_TRACE("connect by join exec params",
              K(join_op->get_exec_params()),
              K(join_path->other_join_condition_),
              K(ret));
        }
      } else if (OB_FAIL(append(join_op->get_join_filters(), join_path->other_join_condition_))) {
        LOG_WARN("failed to allocate filter", K(ret));
      } else { /* do nothing */
      }

      if (OB_SUCC(ret) && MERGE_JOIN == join_path->join_algo_) {
        if (OB_FAIL(join_op->set_left_expected_ordering(join_path->left_expected_ordering_))) {
          LOG_WARN("failed to set left_path expected ordering", K(ret));
        } else if (OB_FAIL(join_op->set_right_expected_ordering(join_path->right_expected_ordering_))) {
          LOG_WARN("failed to set right expected ordering", K(ret));
        } else if (OB_FAIL(join_op->set_merge_directions(join_path->merge_directions_))) {
          LOG_WARN("failed to set merge directions", K(ret));
        }
      }

      if (OB_SUCC(ret) && CONNECT_BY_JOIN == join_path->join_type_) {
        ObJoinOrder* left_jo = NULL;
        ObJoinOrder* right_jo = NULL;
        ObSEArray<ObRawExpr*, 8> column_exprs;
        if (OB_ISNULL(join_path->left_path_) || OB_ISNULL(join_path->right_path_) ||
            OB_ISNULL(left_jo = join_path->left_path_->parent_) ||
            OB_ISNULL(right_jo = join_path->right_path_->parent_) || OB_ISNULL(stmt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected NULL", K(ret));
        } else if (OB_FAIL(stmt_->get_column_exprs(left_jo->get_table_id(), column_exprs))) {
          LOG_WARN("failed to get left table columns", K(ret));
        } else if (OB_FAIL(stmt_->get_column_exprs(right_jo->get_table_id(), column_exprs))) {
          LOG_WARN("failed to get right table columns", K(ret));
        } else if (OB_FAIL(join_op->set_connect_by_extra_exprs(column_exprs))) {
          LOG_WARN("failed to set connect by extra exprs", K(ret));
        } else if (OB_FAIL(join_op->set_right_expected_ordering(join_path->right_expected_ordering_))) {
          LOG_WARN("failed to set right expected ordering", K(ret));
        }
        LOG_TRACE("set connect by right expected ordering", K(join_path->right_expected_ordering_));
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_connect_by_prior_exprs(*join_op))) {
          LOG_WARN("fail to add connect_by prior_exprs", K(ret));
        }
      }
      // compute property for join
      // (equal sets, unique sets, est_sel_info, cost, card, width)
      if (OB_SUCC(ret)) {
        if (OB_FAIL(join_op->compute_property(join_path))) {
          LOG_WARN("failed to compute property for join op", K(ret));
        } else {
          out_join_path_op = join_op;
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::add_connect_by_prior_exprs(ObLogJoin& log_join)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = NULL;
  if (CONNECT_BY_JOIN == log_join.get_join_type()) {
    if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(false == get_stmt()->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid stmt", KPC(get_stmt()), K(ret));
    } else {
      select_stmt = static_cast<ObSelectStmt*>(get_stmt());
      if (select_stmt->is_hierarchical_query()) {
        if (OB_FAIL(log_join.set_connect_by_prior_exprs(select_stmt->get_connect_by_prior_exprs()))) {
          LOG_WARN("fail to set connect by priro expr", K(ret));
        }
      }
      LOG_TRACE("cby connect by prior", K(select_stmt->get_connect_by_prior_exprs()), K(select_stmt));
    }
  }
  return ret;
}

int ObLogPlan::allocate_subquery_path(SubQueryPath* subpath, ObLogicalOperator*& out_subquery_path_op)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* root = NULL;
  ObLogSubPlanScan* subplan_scan = NULL;
  if (OB_ISNULL(subpath) || OB_ISNULL(root = subpath->root_) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(subpath), K(root), K(get_stmt()), K(ret));
  } else if (OB_ISNULL(subplan_scan =
                           static_cast<ObLogSubPlanScan*>(get_log_op_factory().allocate(*this, LOG_SUBPLAN_SCAN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate subquery operator", K(ret));
  } else {
    root->mark_is_plan_root();
    subplan_scan->set_subquery_id(subpath->subquery_id_);
    subplan_scan->set_child(ObLogicalOperator::first_child, root);
    // set subquery name
    const TableItem* table_item = get_stmt()->get_table_item_by_id(subplan_scan->get_subquery_id());
    if (OB_ISNULL(table_item)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      subplan_scan->get_subquery_name().assign_ptr(table_item->table_name_.ptr(), table_item->table_name_.length());
      if (OB_FAIL(append(subplan_scan->get_filter_exprs(), subpath->filter_))) {
        LOG_WARN("failed to append filters", K(ret));
      } else if (OB_FAIL(append(subplan_scan->get_pushdown_filter_exprs(), subpath->pushdown_filters_))) {
        LOG_WARN("failed to append pushdown filters", K(ret));
      } else if (OB_FAIL(subplan_scan->compute_property(subpath))) {
        LOG_WARN("failed to compute property", K(ret));
      } else {
        out_subquery_path_op = subplan_scan;
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_material_as_top(ObLogicalOperator*& old_top)
{
  int ret = OB_SUCCESS;
  ObLogMaterial* material_op = NULL;
  if (OB_ISNULL(old_top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(old_top));
  } else if (OB_ISNULL(material_op = static_cast<ObLogMaterial*>(get_log_op_factory().allocate(*this, LOG_MATERIAL)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate material operator", K(ret));
  } else {
    material_op->set_child(ObLogicalOperator::first_child, old_top);
    if (OB_FAIL(material_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      old_top = material_op;
    }
  }
  return ret;
}

int ObLogPlan::create_plan_tree_from_path(Path* path, ObLogicalOperator*& out_plan_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_ISNULL(path->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(path));
  } else if (NULL != path->log_op_) {
    out_plan_tree = path->log_op_;
  } else {
    ObLogicalOperator* op = NULL;
    if (ACCESS == path->path_type_) {
      AccessPath* access_path = static_cast<AccessPath*>(path);
      if (access_path->is_function_table_path()) {
        if (OB_FAIL(allocate_function_table_path(access_path, op))) {
          LOG_WARN("failed to allocate function table path", K(ret));
        }
      } else if (access_path->is_temp_table_path()) {
        if (OB_FAIL(allocate_temp_table_path(access_path, op))) {
          LOG_WARN("failed to allocate access path", K(ret));
        } else { /* Do nothing */
        }
      } else if (OB_FAIL(allocate_access_path(access_path, op))) {
        LOG_WARN("failed to allocate access path", K(ret));
      } else { /* Do nothing */
      }
    } else if (JOIN == path->path_type_) {
      JoinPath* join_path = static_cast<JoinPath*>(path);
      if (OB_FAIL(allocate_join_path(join_path, op))) {
        LOG_WARN("failed to allocate join path", K(ret));
      } else { /* Do nothing */
      }
    } else if (SUBQUERY == path->path_type_) {
      SubQueryPath* join_path = static_cast<SubQueryPath*>(path);
      if (OB_FAIL(allocate_subquery_path(join_path, op))) {
        LOG_WARN("failed to allocate subquery path", K(ret));
      } else { /* Do nothing */
      }
    } else if (FAKE_CTE_TABLE_ACCESS == path->path_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("there is no access belong to FAKE_CTE_TABLE_ACCESS");
    } else { /* Do nothing */
    }

    if (OB_SUCC(ret)) {
      path->log_op_ = op;
      out_plan_tree = op;
    }
  }
  return ret;
}

int ObLogPlan::extract_onetime_subquery_exprs(ObRawExpr* expr, ObIArray<ObRawExpr*>& onetime_subquery_exprs)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> params;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (!expr->has_flag(CNT_EXEC_PARAM)) {
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::extract_params(expr, params))) {
    LOG_WARN("failed to extract_params", K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < params.count(); ++j) {
    ObRawExpr* param = params.at(j);
    ObRawExpr* onetime_expr = NULL;
    if (OB_ISNULL(param) || !param->has_flag(IS_PARAM)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect param expr", K(param), K(ret));
    } else if (OB_ISNULL(onetime_expr = ObOptimizerUtil::find_exec_param(
                             onetime_exprs_, static_cast<ObConstRawExpr*>(param)->get_value().get_unknown()))) {
    } else if (!onetime_expr->has_flag(CNT_SUB_QUERY)) {
      // do nothing
    } else if (OB_FAIL(onetime_subquery_exprs.push_back(onetime_expr))) {
      LOG_WARN("failed to push back onetime expr", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::candi_init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_order_) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(join_order_), K(get_stmt()));
  } else {
    int64_t total_usage = allocator_.total();
    LOG_TRACE("memory usage after generating join order", K(total_usage));
    ObLogicalOperator* root = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < join_order_->get_interesting_paths().count(); i++) {
      if (OB_ISNULL(join_order_->get_interesting_paths().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join_order_->get_join_orders().at(i) is null", K(ret), K(i));
      } else if (OB_FAIL(create_plan_tree_from_path(join_order_->get_interesting_paths().at(i), root))) {
        LOG_WARN("failed to create a path", K(ret));
      } else if (OB_ISNULL(root)) {
        ret = OB_SQL_OPT_GEN_PLAN_FALIED;
        LOG_WARN("root is null", K(ret), K(root));
      } else if (OB_FAIL(append(root->get_filter_exprs(), get_random_exprs()))) {
        LOG_WARN("failed to append random exprs to filter", K(ret));
      } else if (OB_FAIL(append(root->get_filter_exprs(), get_user_var_filters()))) {
        LOG_WARN("failed to append user var to filter", K(ret));
      } else if (OB_FAIL(append_array_no_dup(root->get_startup_exprs(), get_startup_filters()))) {
        LOG_WARN("failed to append startup filters", K(ret));
      } else {
        CandidatePlan cp;
        cp.plan_tree_ = root;
        if (OB_FAIL(candidates_.candidate_plans_.push_back(cp))) {
          LOG_WARN("push back error", K(ret));
        } else {
          // update the 'uninteresting' plan cost
          if (1 == candidates_.candidate_plans_.count() || cp.plan_tree_->get_cost() < candidates_.plain_plan_.first) {
            if (OB_ISNULL(cp.plan_tree_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("cp.plan_tree_ is null", K(ret));
            } else {
              // the cost of the candidate plan should be noted in the root operator
              candidates_.plain_plan_.first = cp.plan_tree_->get_cost();
              // position in the candidate plan list
              candidates_.plain_plan_.second = candidates_.candidate_plans_.count() - 1;
            }
          } else { /* Do nothing */
          }
          LOG_TRACE("add a candidate plan",
              "count",
              candidates_.candidate_plans_.count(),
              K(candidates_.plain_plan_.first),
              K(candidates_.plain_plan_.second));
          int64_t plan_usage = allocator_.total() - total_usage;
          total_usage = allocator_.total();
          LOG_TRACE("memory usage after generate a candidate", K(total_usage), K(plan_usage));
          total_usage = allocator_.total();
        }
      }
    }  // for join orders end
  }
  return ret;
}

int ObLogPlan::get_order_by_exprs(
    ObLogicalOperator& root, ObIArray<ObRawExpr*>& order_by_exprs, ObIArray<ObOrderDirection>* directions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_stmt()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_stmt()->get_order_item_size(); i++) {
      const OrderItem& order_item = get_stmt()->get_order_item(i);
      bool is_const = false;
      bool has_null_reject = false;
      if (OB_ISNULL(order_item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(order_item.expr_, root.get_output_const_exprs(), is_const))) {
        LOG_WARN("check is const expr failed", K(ret));
      } else if (is_const || (order_item.expr_->has_flag(CNT_COLUMN) && !order_item.expr_->has_flag(CNT_SUB_QUERY) &&
                                 !order_item.expr_->get_expr_levels().has_member(get_stmt()->get_current_level()))) {
      } else if (OB_FAIL(order_by_exprs.push_back(order_item.expr_))) {
        LOG_WARN("failed to add order by expr", K(ret));
      } else if (NULL != directions) {
        if (OB_FAIL(ObTransformUtils::has_null_reject_condition(
                get_stmt()->get_condition_exprs(), order_item.expr_, has_null_reject))) {
          LOG_WARN("failed to check null rejection", K(ret));
        } else if (!has_null_reject) {
          ret = directions->push_back(order_item.order_type_);
        } else if (is_ascending_direction(order_item.order_type_)) {
          ret = directions->push_back(default_asc_direction());
        } else {
          ret = directions->push_back(default_desc_direction());
        }
      }
    }
  }

  return ret;
}

int ObLogPlan::make_order_items(
    const common::ObIArray<ObRawExpr*>& exprs, const ObIArray<ObOrderDirection>* dirs, ObIArray<OrderItem>& items)
{
  int ret = OB_SUCCESS;
  if (NULL == dirs) {
    if (OB_FAIL(make_order_items(exprs, items))) {
      LOG_WARN("Failed to make order items", K(ret));
    }
  } else {
    if (OB_FAIL(make_order_items(exprs, *dirs, items))) {
      LOG_WARN("Failed to make order items", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::make_order_items(const common::ObIArray<ObRawExpr*>& exprs, common::ObIArray<OrderItem>& items)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_stmt()));
  } else {
    ObOrderDirection direction = default_asc_direction();
    if (get_stmt()->get_order_item_size() > 0) {
      direction = get_stmt()->get_order_item(0).order_type_;
    } else { /* Do nothing */
    }
    int64_t N = exprs.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      OrderItem key;
      key.expr_ = exprs.at(i);
      key.order_type_ = direction;
      ret = items.push_back(key);
    }
  }
  return ret;
}

int ObLogPlan::make_order_items(
    const ObIArray<ObRawExpr*>& exprs, const ObIArray<ObOrderDirection>& dirs, ObIArray<OrderItem>& items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(exprs.count() != dirs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr and dir count not match", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      OrderItem key;
      if (OB_ISNULL(exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(exprs.at(i)));
      } else if (exprs.at(i)->has_const_or_const_expr_flag()) {
        // do nothing
      } else {
        key.expr_ = exprs.at(i);
        key.order_type_ = dirs.at(i);
        if (OB_FAIL(items.push_back(key))) {
          LOG_WARN("Failed to push array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_sequence()
{
  int ret = OB_SUCCESS;
  double min_cost = 0.0;
  int64_t min_idx = OB_INVALID_INDEX;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < candidates_.candidate_plans_.count(); ++idx) {
    CandidatePlan& plain_plan = candidates_.candidate_plans_.at(idx);
    if (OB_FAIL(allocate_sequence_as_top(plain_plan.plan_tree_))) {
      LOG_WARN("fail alloc candi sequence op", K(idx), K(ret));
    } else if (OB_ISNULL(plain_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null plan tree", K(ret));
    } else if (plain_plan.plan_tree_->get_cost() < min_cost || OB_INVALID_INDEX == min_idx) {
      min_cost = plain_plan.plan_tree_->get_cost();
      min_idx = idx;
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(OB_INVALID_INDEX == min_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Min idx is invalid", K(ret), K(min_idx));
    } else {
      candidates_.plain_plan_.first = min_cost;
      candidates_.plain_plan_.second = min_idx;
    }
  }
  return ret;
}

/*
 * for sequence, old_top may be null
 * |ID|OPERATOR    |NAME|EST. ROWS|COST|
 * -------------------------------------
 * |0 |INSERT      |    |0        |1   |
 * |1 | EXPRESSION |    |1        |1   |
 * |2 |  SEQUENCE  |    |1        |1   |
 */
int ObLogPlan::allocate_sequence_as_top(ObLogicalOperator*& old_top)
{
  int ret = OB_SUCCESS;
  ObLogSequence* sequence = NULL;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Get unexpected null", K(ret), K(old_top), K(get_stmt()));
  } else if (OB_ISNULL(sequence = static_cast<ObLogSequence*>(get_log_op_factory().allocate(*this, LOG_SEQUENCE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate sequence operator", K(ret));
  } else if (OB_FAIL(append_array_no_dup(sequence->get_sequence_ids(), get_stmt()->get_nextval_sequence_ids()))) {
    LOG_WARN("failed to append array no dup", K(ret));
  } else {
    if (NULL != old_top) {
      sequence->set_child(ObLogicalOperator::first_child, old_top);
    }
    if (OB_FAIL(sequence->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      old_top = sequence;
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_order_by(ObIArray<OrderItem>& order_items)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> order_by_exprs;
  ObSEArray<ObOrderDirection, 4> directions;
  ObSEArray<ObRawExpr*, 4> candi_subquery_exprs;
  ObSEArray<OrderItem, 4> candi_order_items;
  ObLogicalOperator* best_plan = NULL;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(get_stmt()->get_order_exprs(candi_subquery_exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter_for_exprs(candi_subquery_exprs))) {
    LOG_WARN("failed to allocate subplan filter for exprs", K(ret));
  } else if (OB_FAIL(get_current_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_order_by_exprs(*best_plan, order_by_exprs, &directions))) {
    LOG_WARN("failed to get order by columns", K(ret));
  } else if (order_by_exprs.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(make_order_items(order_by_exprs, directions, candi_order_items))) {
    LOG_WARN("Failed to make order items", K(ret));
  } else if (OB_FAIL(best_plan->simplify_ordered_exprs(candi_order_items, order_items))) {
    LOG_WARN("failed to simplify ordered exprs", K(ret));
  } else if (order_items.empty()) {
    /*do nothing*/
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
      bool need_sort = false;
      CandidatePlan& plain_plan = candidates_.candidate_plans_.at(i);
      if (OB_ISNULL(plain_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(plain_plan.plan_tree_->check_need_sort_above_node(order_items, need_sort))) {
        LOG_WARN("failed to check plan need sort for order by", K(ret));
      } else if (!need_sort) {
        LOG_TRACE("no need to allocate sort for order-by", K(need_sort));
      } else if (OB_FAIL(allocate_sort_as_top(plain_plan.plan_tree_, order_items))) {
        LOG_WARN("failed to allocate sort as top", K(ret));
      } else {
        LOG_TRACE("succeed to allocate sort for order-by", K(need_sort));
      }
    }
    // keep minimal cost plan or interesting plan
    if (OB_SUCC(ret)) {
      ObSEArray<CandidatePlan, 4> all_plans;
      int64_t check_scope = OrderingCheckScope::CHECK_SET;
      if (OB_FAIL(all_plans.assign(candidates_.candidate_plans_))) {
        LOG_WARN("failed to assign plan", K(ret));
      } else if (OB_FAIL(update_plans_interesting_order_info(all_plans, check_scope))) {
        LOG_WARN("failed to update plans interesting order info", K(ret));
      } else if (OB_FAIL(prune_and_keep_best_plans(all_plans))) {
        LOG_WARN("failed to prune and keep best plans", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_subplan_filter_for_exprs(ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> subquery_exprs;
  ObSEArray<ObRawExpr*, 4> nested_subquery_exprs;
  ObSEArray<ObQueryRefRawExpr*, 4> subqueries;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(exprs, subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (subquery_exprs.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(subquery_exprs, subqueries))) {
    LOG_WARN("failed to get extract query ref expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::extract_target_level_query_ref_expr(
                 subquery_exprs, get_stmt()->get_current_level(), subqueries, nested_subquery_exprs))) {
    LOG_WARN("failed to extract target level query ref expr", K(ret));
  } else if (!nested_subquery_exprs.empty() && OB_FAIL(candi_allocate_subplan_filter(nested_subquery_exprs, false))) {
    LOG_WARN("failed to allocate subplan filter for order by exprs", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter(subquery_exprs, false))) {
    LOG_WARN("failed to allocate subplan filter for order by exprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogPlan::allocate_sort_as_top(
    ObLogicalOperator*& top, const ObIArray<ObRawExpr*>& order_by_exprs, const ObIArray<ObOrderDirection>* directions)
{
  int ret = OB_SUCCESS;
  ObSEArray<OrderItem, 4> order_items;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(ret));
  } else if (OB_FAIL(make_order_items(order_by_exprs, directions, order_items))) {
    LOG_WARN("failed to make order items", K(ret));
  } else if (OB_FAIL(allocate_sort_as_top(top, order_items))) {
    LOG_WARN("failed to allocate sort as top", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogPlan::allocate_group_by_as_top(ObLogicalOperator*& top, const AggregateAlgo algo,
    const ObIArray<ObRawExpr*>& group_by_exprs, const ObIArray<ObRawExpr*>& rollup_exprs,
    const ObIArray<ObAggFunRawExpr*>& agg_items, const ObIArray<ObRawExpr*>& having_exprs,
    const ObIArray<OrderItem>& expected_ordering, const bool from_pivot /* = false*/)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* group_by_op = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(top));
  } else if (OB_ISNULL(group_by_op = get_log_op_factory().allocate(*this, LOG_GROUP_BY))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate group by operator", K(ret));
  } else {
    ObLogGroupBy* group_by = static_cast<ObLogGroupBy*>(group_by_op);
    group_by->set_child(ObLogicalOperator::first_child, top);
    group_by->set_algo_type(algo);
    group_by->set_from_pivot(from_pivot);
    if (OB_FAIL(group_by->set_group_by_exprs(group_by_exprs))) {
      LOG_WARN("failed to set group by columns", K(ret));
    } else if (OB_FAIL(group_by->set_rollup_exprs(rollup_exprs))) {
      LOG_WARN("failed to set rollup columns", K(ret));
    } else if (OB_FAIL(group_by->set_aggr_exprs(agg_items))) {
      LOG_WARN("failed to set aggregation exprs", K(ret));
    } else if (OB_FAIL(group_by->set_expected_ordering(expected_ordering))) {
      LOG_WARN("failed to set expected ordering", K(ret));
    } else if (OB_FAIL(group_by->get_filter_exprs().assign(having_exprs))) {
      LOG_WARN("failed to assign exprs", K(ret));
    } else if (OB_FAIL(group_by->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = group_by;
    }
  }
  return ret;
}

int ObLogPlan::allocate_sort_as_top(ObLogicalOperator*& top, const ObIArray<OrderItem>& sort_keys)
{
  int ret = OB_SUCCESS;
  ObLogSort* sort = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(ret));
  } else if (OB_ISNULL(sort = static_cast<ObLogSort*>(get_log_op_factory().allocate(*this, LOG_SORT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate sort for order by", K(ret));
  } else {
    sort->set_child(ObLogicalOperator::first_child, top);
    if (OB_FAIL(sort->set_sort_keys(sort_keys))) {
      LOG_WARN("failed to set sort keys", K(ret));
    } else if (OB_FAIL(sort->check_prefix_sort())) {
      LOG_WARN("failed to check prefix sort", K(ret));
    } else { /*do nothing*/
    }
  }
  // set plan root, for allocate exchange and adjust sort
  if (OB_SUCC(ret) && top->is_plan_root()) {
    top->set_is_plan_root(false);
    sort->mark_is_plan_root();
    set_plan_root(sort);
  }
  // compute property
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sort->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = sort;
    }
  }
  return ret;
}

/*
 * Limit clause will trigger a cost re-estimation phase based on a uniform distribution assumption.
 * instead of choosing minimal-cost plans, we prefer more reliable plans.
 */
int ObLogPlan::candi_allocate_limit(ObIArray<OrderItem>& order_items)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  bool has_union_child = false;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  } else if (stmt->is_set_stmt()) {
    has_union_child = static_cast<ObSelectStmt*>(stmt)->get_set_op() == ObSelectStmt::UNION;
  } else { /*do nothing*/
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(candi_allocate_limit(stmt->get_limit_expr(),
            stmt->get_offset_expr(),
            stmt->get_limit_percent_expr(),
            order_items,
            has_union_child,
            stmt->is_calc_found_rows(),
            stmt->has_top_limit(),
            stmt->is_fetch_with_ties()))) {
      LOG_WARN("failed to allocate limit operator", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_limit(ObRawExpr* limit_expr, ObRawExpr* offset_expr, ObRawExpr* percent_expr,
    ObIArray<OrderItem>& expect_ordering, const bool has_union_child, const bool is_calc_found_rows,
    const bool is_top_limit, const bool is_fetch_with_ties)
{
  int ret = OB_SUCCESS;
  CandidatePlan temp_plan;
  ObSEArray<CandidatePlan, 8> non_reliable_plans;
  ObSEArray<CandidatePlan, 8> reliable_plans;
  // aggregate the number of parent for each operator
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
    ObLogicalOperator* root = candidates_.candidate_plans_.at(i).plan_tree_;
    if (OB_ISNULL(root)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(root->aggregate_number_of_parent())) {
      LOG_WARN("failed to aggregate number of parent", K(ret));
    } else { /*do nothing*/
    }
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < candidates_.candidate_plans_.count(); ++idx) {
    bool need_copy_tree = false;
    bool need_alloc_limit = false;
    bool is_reliable = false;
    CandidatePlan& plain_plan = candidates_.candidate_plans_.at(idx);
    if (OB_ISNULL(plain_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(plain_plan.plan_tree_->need_copy_plan_tree(need_copy_tree))) {
      LOG_WARN("failed to check whether need copy plan tree", K(ret));
    } else if (!need_copy_tree) {
      temp_plan.plan_tree_ = plain_plan.plan_tree_;
    } else if (OB_FAIL(plan_tree_copy(plain_plan.plan_tree_, temp_plan.plan_tree_))) {
      LOG_WARN("failed to do plan tree copy", K(ret));
    } else { /*do nothing*/
    }

    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_ISNULL(temp_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(if_plan_need_limit(temp_plan.plan_tree_,
                   limit_expr,
                   offset_expr,
                   percent_expr,
                   is_fetch_with_ties,
                   need_alloc_limit))) {
      LOG_WARN("Failed to check limit needed", K(ret));
    } else if (need_alloc_limit && OB_FAIL(allocate_limit_as_top(temp_plan.plan_tree_,
                                       limit_expr,
                                       offset_expr,
                                       percent_expr,
                                       expect_ordering,
                                       has_union_child,
                                       is_calc_found_rows,
                                       is_top_limit,
                                       is_fetch_with_ties))) {
      LOG_WARN("failed to allocate limit", K(ret));
    } else if (OB_ISNULL(temp_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(is_plan_reliable(temp_plan.plan_tree_, is_reliable))) {
      LOG_WARN("failed to check plan is reliable", K(ret));
    } else if (is_reliable) {
      ret = reliable_plans.push_back(temp_plan);
    } else {
      ret = non_reliable_plans.push_back(temp_plan);
    }
  }
  if (OB_SUCC(ret)) {
    int64_t check_scope = OrderingCheckScope::NOT_CHECK;
    if (!reliable_plans.empty()) {
      if (OB_FAIL(update_plans_interesting_order_info(reliable_plans, check_scope))) {
        LOG_WARN("failed to update plans interesting order info", K(ret));
      } else if (OB_FAIL(prune_and_keep_best_plans(reliable_plans))) {
        LOG_WARN("failed to prune and keep best plans", K(ret));
      } else {
        LOG_TRACE("Limit-k clause choose a more reliable plan");
      }
    } else {
      if (OB_FAIL(update_plans_interesting_order_info(non_reliable_plans, check_scope))) {
        LOG_WARN("failed to update plans interesting order info", K(ret));
      } else if (OB_FAIL(prune_and_keep_best_plans(non_reliable_plans))) {
        LOG_WARN("failed to prune and keep best plans", K(ret));
      } else {
        LOG_TRACE("Limit-k clause does not choose a more reliable plan");
      }
    }
  }
  // reset number of parent for each operator
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
    ObLogicalOperator* root = candidates_.candidate_plans_.at(i).plan_tree_;
    if (OB_ISNULL(root)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(root->reset_number_of_parent())) {
      LOG_WARN("failed to reset number of parent", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

/*
 * A plan is reliable if it does not make any uniform assumption during the cost re-estimation phase.
 * In other words, it should satisfy the following two requirements:
 * 1 no operator in the plan has more than 1 children
 * 2 all operators in the plan belong to the set {LOG_LIMIT, LOG_COUNT, LOG_TABLE_LOOKUP,
 *                                                LOG_SUBPLAN_SCAN, LOG_TABLE_SCAN, LOG_UNPIVOT},
 *   and does not have any filters.
 */
int ObLogPlan::is_plan_reliable(const ObLogicalOperator* root, bool& is_reliable)
{
  int ret = OB_SUCCESS;
  is_reliable = false;
  if (OB_ISNULL(root)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN == root->get_type()) {
    const ObCostTableScanInfo* cost_info = static_cast<const ObLogTableScan*>(root)->get_est_cost_info();
    if (OB_ISNULL(cost_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cost info is null", K(ret));
    } else {
      is_reliable = cost_info->table_filters_.empty() && cost_info->postfix_filters_.empty();
    }
  } else if (log_op_def::LOG_SORT == root->get_type()) {
    if (fabs(root->get_op_cost()) < OB_DOUBLE_EPSINON) {
      // this sort operator is allocated to adjust sort, can ignore
      is_reliable = true;
    } else {
      is_reliable = false;
    }
  } else if (root->get_filter_exprs().count() == 0 &&
             (log_op_def::LOG_LIMIT == root->get_type() || log_op_def::LOG_COUNT == root->get_type() ||
                 log_op_def::LOG_TABLE_LOOKUP == root->get_type() || log_op_def::LOG_SUBPLAN_SCAN == root->get_type() ||
                 log_op_def::LOG_UNPIVOT == root->get_type())) {
    is_reliable = true;
  } else {
    is_reliable = false;
  }
  if (OB_SUCC(ret) && is_reliable) {
    bool is_child_reliable = false;
    if (root->get_num_of_child() > 1) {
      is_reliable = false;
    } else if (root->get_num_of_child() == 0) {
      is_reliable = true;
    } else if (OB_ISNULL(root->get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(is_plan_reliable(root->get_child(0), is_child_reliable))) {
      LOG_WARN("failed to check plan is reliable", K(ret));
    } else {
      is_reliable &= is_child_reliable;
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to check plan is reliable", K(is_reliable), K(root));
  }
  return ret;
}

int ObLogPlan::candi_allocate_select_into()
{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 1> select_into_plans;
  ObIArray<CandidatePlan>& candidates = candidates_.candidate_plans_;
  int64_t best_plan_id = candidates_.plain_plan_.second;
  if (OB_UNLIKELY(best_plan_id < 0 || best_plan_id >= candidates.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array pos", K(best_plan_id), K(candidates.count()), K(ret));
  } else {
    CandidatePlan candidate_plan = candidates.at(best_plan_id);
    if (OB_ISNULL(candidate_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(allocate_selectinto_as_top(candidate_plan.plan_tree_))) {
      LOG_WARN("failed to allocate selectinto as top", K(ret));
    } else if (OB_FAIL(select_into_plans.push_back(candidate_plan))) {
      LOG_WARN("failed to push back candidate plan", K(ret));
    } else if (OB_FAIL(prune_and_keep_best_plans(select_into_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogPlan::allocate_selectinto_as_top(ObLogicalOperator*& old_top)
{
  int ret = OB_SUCCESS;
  ObLogSelectInto* select_into = NULL;
  ObSelectStmt* stmt = static_cast<ObSelectStmt*>(get_stmt());
  if (OB_ISNULL(old_top) || OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Get unexpected null", K(ret), K(old_top), K(get_stmt()));
  } else if (OB_ISNULL(
                 select_into = static_cast<ObLogSelectInto*>(get_log_op_factory().allocate(*this, LOG_SELECT_INTO)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for ObLogSelectInto failed", K(ret));
  } else {
    ObSelectIntoItem* into_item = stmt->get_select_into();
    bool need_expected_ordering = stmt->has_order_by() && !stmt->has_limit();
    if (OB_ISNULL(into_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("into item is null", K(ret));
    } else {
      select_into->set_into_type(into_item->into_type_);
      select_into->set_outfile_name(into_item->outfile_name_);
      select_into->set_filed_str(into_item->filed_str_);
      select_into->set_line_str(into_item->line_str_);
      select_into->set_user_vars(into_item->user_vars_);
      select_into->set_is_optional(into_item->is_optional_);
      select_into->set_closed_cht(into_item->closed_cht_);
      select_into->set_child(ObLogicalOperator::first_child, old_top);
      old_top = select_into;
      // compute equal set
      if (OB_SUCC(ret)) {
        if (need_expected_ordering && OB_FAIL(select_into->set_expected_ordering(stmt->get_order_items()))) {
          LOG_WARN("failed to set expected ordering", K(ret));
        } else if (OB_FAIL(select_into->compute_property())) {
          LOG_WARN("failed to compute equal set", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::if_plan_need_limit(ObLogicalOperator* top, ObRawExpr* limit_expr, ObRawExpr* offset_expr,
    ObRawExpr* percent_expr, bool is_fetch_with_ties, bool& need_alloc_limit)
{
  int ret = OB_SUCCESS;
  need_alloc_limit = true;
  if (OB_ISNULL(top) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(get_stmt()), K(ret));
  } else if (log_op_def::instance_of_log_table_scan(top->get_type())) {
    ObLogTableScan* table_scan = static_cast<ObLogTableScan*>(top);
    if (!is_virtual_table(table_scan->get_ref_table_id()) && !get_stmt()->is_calc_found_rows() &&
        (table_scan->get_phy_location_type() == OB_TBL_LOCATION_LOCAL ||
            table_scan->get_phy_location_type() == OB_TBL_LOCATION_REMOTE) &&
        OB_USE_LATE_MATERIALIZATION != get_stmt()->get_stmt_hint().use_late_mat_ && !table_scan->is_sample_scan() &&
        NULL == table_scan->get_limit_expr() && NULL == percent_expr && !is_fetch_with_ties && limit_expr != NULL) {
      // Push down limit onto TableScan
      // 1. TODO: limit and offset on TableScan for virtual table are not handled yet.
      // 2. For cases with distributed sharding_info_, the limit pushing down will be done later
      //    when allocating the EXCHANGE
      // 3. Only do on SELECT log plan, since we can't get sharding_info_ for table_scan now
      //    and we need to use sharding_info_ on SELECT log plan.
      // Reminder: location type is distributed if use intra parallel
      table_scan->set_limit_offset(limit_expr, offset_expr);
      need_alloc_limit = false;
      // Adjust card for table scan
      if (OB_FAIL(table_scan->set_limit_size())) {
        LOG_WARN("Setting limit size for table scan fails", K(ret));
      }
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogPlan::allocate_limit_as_top(ObLogicalOperator*& old_top, ObRawExpr* limit_expr, ObRawExpr* offset_expr,
    ObRawExpr* percent_expr, ObIArray<OrderItem>& expect_ordering, const bool has_union_child,
    const bool is_calc_found_rows, const bool is_top_limit, const bool is_fetch_with_ties)
{
  int ret = OB_SUCCESS;
  ObLogLimit* limit = NULL;
  if (OB_ISNULL(old_top) || OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(old_top), K(get_stmt()), K(ret));
  } else if (OB_ISNULL(limit = static_cast<ObLogLimit*>(get_log_op_factory().allocate(*this, LOG_LIMIT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for limit op", K(ret));
  } else {
    limit->set_limit_count(limit_expr);
    limit->set_limit_offset(offset_expr);
    limit->set_limit_percent(percent_expr);
    limit->set_child(ObLogicalOperator::first_child, old_top);
    limit->set_has_union_child(has_union_child);
    limit->set_calc_found_rows(is_calc_found_rows);
    limit->set_top_limit(is_top_limit);
    limit->set_fetch_with_ties(is_fetch_with_ties);
    if (OB_FAIL(limit->set_expected_ordering(expect_ordering))) {
      LOG_WARN("failed to set expected ordering", K(ret));
    } else if (OB_FAIL(limit->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      old_top = limit;
    }
  }
  return ret;
}

int ObLogPlan::plan_tree_traverse(const TraverseOp& operation, void* ctx)
{
  int ret = OB_SUCCESS;
  int64_t parallel = get_optimizer_context().get_parallel();
  if (OB_ISNULL(get_plan_root()) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_stmt()), K(get_plan_root()));
  } else {
    NumberingCtx numbering_ctx;                   // operator numbering context
    NumberingExchangeCtx numbering_exchange_ctx;  // operator numbering context
    ObArenaAllocator allocator(CURRENT_CONTEXT.get_malloc_allocator());
    allocator.set_label("PlanTreeTraver");
    ObAllocExprContext alloc_expr_ctx(allocator);  // expr allocation context
    AllocExchContext alloc_exch_ctx(parallel);     // exchange allocation context
    AdjustSortContext adjust_sort_ctx;             // exchange allocation context
    uint64_t hash_seed = 0;                        // seed for plan signature
    ObBitSet<256> output_deps;                     // output expr dependencies
    AllocGIContext gi_ctx(parallel);
    ObPxPipeBlockingCtx pipe_block_ctx(get_allocator());
    ObLocationConstraintContext location_constraints;
    AllocMDContext md_ctx;
    GenLinkStmtContext link_ctx;

    // set up context
    switch (operation) {
      case PX_PIPE_BLOCKING: {
        ctx = &pipe_block_ctx;
        if (OB_FAIL(get_plan_root()->init_all_traverse_ctx(pipe_block_ctx))) {
          LOG_WARN("init traverse ctx failed", K(ret));
        }
        break;
      }
      case ALLOC_GI: {
        ctx = &gi_ctx;
        bool is_valid = true;
        if (get_stmt()->is_insert_stmt() && !static_cast<ObInsertStmt*>(get_stmt())->value_from_select()) {
          gi_ctx.is_valid_for_gi_ = false;
        } else if (OB_FAIL(get_plan_root()->should_allocate_gi_for_dml(is_valid))) {
          LOG_WARN("failed to check should allocate gi for dml", K(ret));
        } else {
          gi_ctx.is_valid_for_gi_ = get_stmt()->is_dml_write_stmt() && is_valid;
        }
        break;
      }
      case ALLOC_MONITORING_DUMP: {
        ctx = &md_ctx;
        break;
      }
      case ALLOC_EXCH: {
        ctx = &alloc_exch_ctx;
        break;
      }
      case ADJUST_SORT_OPERATOR: {
        ctx = &adjust_sort_ctx;
        break;
      }
      case PROJECT_PRUNING: {
        ctx = &output_deps;
        break;
      }
      case ALLOC_EXPR: {
        if (OB_FAIL(alloc_expr_ctx.not_produced_exprs_.init())) {
          LOG_WARN("fail to init not_produced_exprs", K(ret));
        } else if (OB_FAIL(alloc_expr_ctx.set_group_replaced_exprs(group_replaced_exprs_))) {
          LOG_WARN("failed to set_group_replaced_exprs", K(ret));
        } else {
          ctx = &alloc_expr_ctx;
        }
        break;
      }
      case OPERATOR_NUMBERING: {
        ctx = &numbering_ctx;
        break;
      }
      case EXCHANGE_NUMBERING: {
        ctx = &numbering_exchange_ctx;
        break;
      }
      case GEN_SIGNATURE: {
        ctx = &hash_seed;
        break;
      }
      case GEN_LOCATION_CONSTRAINT: {
        ctx = &location_constraints;
        break;
      }
      case PX_ESTIMATE_SIZE:
        break;
      case GEN_LINK_STMT: {
        ctx = &link_ctx;
        break;
      }
      case EXPLAIN_COLLECT_WIDTH:
      case EXPLAIN_WRITE_BUFFER:
      case EXPLAIN_WRITE_BUFFER_OUTPUT:
      case EXPLAIN_WRITE_BUFFER_OUTLINE:
      case EXPLAIN_INDEX_SELECTION_INFO:
      default:
        break;
    }
    if (OB_SUCC(ret)) {
      if (((PX_ESTIMATE_SIZE == operation) || (PX_PIPE_BLOCKING == operation) || (PX_RESCAN == operation)) &&
          (is_local_or_remote_plan())) {
        /*do nothing*/
      } else if (ALLOC_GI == operation && is_local_or_remote_plan() &&
                 !(gi_ctx.is_valid_for_gi_ && get_optimizer_context().enable_batch_rpc())) {
        /*do nothing*/
      } else if (OB_FAIL(get_plan_root()->do_plan_tree_traverse(operation, ctx))) {
        LOG_WARN("failed to apply operation to operator", K(operation), K(ret));
      } else {
        // remember signature in plan
        if (GEN_SIGNATURE == operation) {
          if (OB_ISNULL(ctx)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ctx is null", K(ret), K(ctx));
          } else {
            hash_value_ = *static_cast<uint64_t*>(ctx);
            LOG_TRACE("succ to generate plan hash value", "hash_value", hash_value_);
          }
        } else if (ALLOC_EXCH == operation) {
          set_phy_plan_type(alloc_exch_ctx);
          if (!alloc_exch_ctx.exchange_allocated_) {
            set_require_local_execution(true);
          }
          if (OB_FAIL(set_group_replaced_exprs(alloc_exch_ctx.group_push_down_replaced_exprs_))) {
            LOG_WARN("failed to set_group_replaced_exprs", K(ret));
          }
        } else if (GEN_LOCATION_CONSTRAINT == operation) {
          ObSqlCtx* sql_ctx = NULL;
          if (OB_ISNULL(optimizer_context_.get_exec_ctx()) ||
              OB_ISNULL(sql_ctx = optimizer_context_.get_exec_ctx()->get_sql_ctx())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(ret), K(optimizer_context_.get_exec_ctx()), K(sql_ctx));
          } else if (OB_FAIL(remove_duplicate_constraint(location_constraints, *sql_ctx))) {
            LOG_WARN("fail to remove duplicate constraint", K(ret));
          } else if (OB_FAIL(calc_and_set_exec_pwj_map(location_constraints))) {
            LOG_WARN("failed to calc and set exec pwj map", K(ret));
          }
        } else if (ADJUST_SORT_OPERATOR == operation) {
          if (0 != adjust_sort_ctx.exchange_cnt_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid traverse for adjust sort", K(ret), K(adjust_sort_ctx.exchange_cnt_));
          }
        } else if (ALLOC_EXPR == operation) {
          if (OB_FAIL(set_all_exprs(alloc_expr_ctx))) {
            LOG_WARN("fail to set all exprs", K(ret));
          }
        } else if (OPERATOR_NUMBERING == operation) {
          NumberingCtx* num_ctx = static_cast<NumberingCtx*>(ctx);
          max_op_id_ = num_ctx->op_id_;
          LOG_TRACE("trace max operator id", K(max_op_id_), K(this));
        } else { /* Do nothing */
        }
        LOG_TRACE("succ to apply operaion to operator", K(operation), K(ret));
      }
    }
  }

  return ret;
}

int ObLogPlan::change_id_hint_to_idx()
{
  int ret = OB_SUCCESS;
  ObDMLStmt* dml_stmt = get_stmt();
  if (OB_ISNULL(dml_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml stmt is null", K(ret));
  } else {
    ObStmtHint& stmt_hint = dml_stmt->get_stmt_hint();
    if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(stmt_hint.use_merge_ids_, stmt_hint.use_merge_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use merge ids", K(ret));
    } else if (OB_FAIL(
                   dml_stmt->convert_table_ids_to_bitset(stmt_hint.no_use_merge_ids_, stmt_hint.no_use_merge_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for no use merge ids", K(ret));
    } else if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(stmt_hint.use_nl_ids_, stmt_hint.use_nl_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use nl ids", K(ret));
    } else if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(stmt_hint.no_use_nl_ids_, stmt_hint.no_use_nl_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for no use nl ids", K(ret));
    } else if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(stmt_hint.use_bnl_ids_, stmt_hint.use_bnl_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use bnl ids", K(ret));
    } else if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(stmt_hint.no_use_bnl_ids_, stmt_hint.no_use_bnl_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for no use bnl ids", K(ret));
    } else if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(stmt_hint.use_hash_ids_, stmt_hint.use_hash_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use hash ids", K(ret));
    } else if (OB_FAIL(
                   dml_stmt->convert_table_ids_to_bitset(stmt_hint.no_use_hash_ids_, stmt_hint.no_use_hash_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use hash ids", K(ret));
    } else if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(
                   stmt_hint.use_nl_materialization_ids_, stmt_hint.use_nl_materialization_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use hash ids", K(ret));
    } else if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(
                   stmt_hint.no_use_nl_materialization_ids_, stmt_hint.no_use_nl_materialization_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use hash ids", K(ret));
    } else if (OB_FAIL(
                   dml_stmt->convert_table_ids_to_bitset(stmt_hint.pq_distributes_, stmt_hint.pq_distributes_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use hash ids", K(ret));
    } else if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(stmt_hint.pq_maps_, stmt_hint.pq_map_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use hash ids", K(ret));
    } else if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(
                   stmt_hint.px_join_filter_ids_, stmt_hint.px_join_filter_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use hash ids", K(ret));
    } else if (OB_FAIL(dml_stmt->convert_table_ids_to_bitset(
                   stmt_hint.no_px_join_filter_ids_, stmt_hint.no_px_join_filter_idxs_))) {
      LOG_WARN("failed to convert table ids to bitset for use hash ids", K(ret));
    } else {
    }
  }
  return ret;
}

int ObLogPlan::generate_plan_tree()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_stmt()));
  } else {
    // 1.1 generate access paths
    /* random exprs should be split from condition exprs to avoid being pushed down
     * random exprs will be added back in function candi_init*/
    if (OB_FAIL(ObTransformUtils::right_join_to_left(get_stmt()))) {
      LOG_WARN("failed to change right join to left", K(ret));
    } else if (OB_FAIL(change_id_hint_to_idx())) {
      LOG_WARN("failed to change id hint to idx", K(ret));
    } else if (OB_FAIL(generate_join_orders())) {
      LOG_WARN("failed to generate the access path for the single-table query", K(ret), K(get_stmt()->get_sql_stmt()));
    } else if (OB_FAIL(candi_init())) {
      LOG_WARN("failed to initialized the plan candidates from the join order", K(ret));
    } else {
      LOG_TRACE("plan candidates is initialized from the join order",
          "# of candidates",
          candidates_.candidate_plans_.count());
    }
  }
  return ret;
}

void ObLogPlan::set_phy_plan_type(AllocExchContext& ctx)
{
  AllocExchContext::DistrStat status = ctx.get_plan_type();
  if (AllocExchContext::UNINITIALIZED == status || AllocExchContext::LOCAL == status) {
    phy_plan_type_ = OB_PHY_PLAN_LOCAL;
  } else if (AllocExchContext::REMOTE == status) {
    phy_plan_type_ = OB_PHY_PLAN_REMOTE;
  } else {
    phy_plan_type_ = OB_PHY_PLAN_DISTRIBUTED;
  }
}

int ObLogPlan::check_join_legal(const ObRelIds& left_set, const ObRelIds& right_set, ConflictDetector* detector,
    bool delay_cross_product, bool& legal)
{
  int ret = OB_SUCCESS;
  ObRelIds ids;
  legal = true;
  if (OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null detector", K(ret));
  } else if (INNER_JOIN == detector->join_info_.join_type_) {
    if (OB_FAIL(ids.add_members(left_set))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(ids.add_members(right_set))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (!detector->join_info_.table_set_.is_subset(ids)) {
      legal = false;
    }
  } else {
    if (detector->L_TES_.is_subset(left_set) && detector->R_TES_.is_subset(right_set)) {
      // do nothing
    } else if (!detector->is_commutative_) {
      legal = false;
    } else if (detector->R_TES_.is_subset(left_set) && detector->L_TES_.is_subset(right_set)) {
      // do nothing
    } else {
      legal = false;
    }
    if (legal && !detector->is_commutative_) {
      if (left_set.overlap(detector->R_DS_) || right_set.overlap(detector->L_DS_)) {
        legal = false;
      }
    }
  }
  if (OB_FAIL(ret) || !legal) {
    // do nothing
  } else if (detector->is_degenerate_pred_) {
    // check T(left(o)) n S1 != empty ^ T(right(o)) n S2 != empty
    if (detector->L_DS_.overlap(left_set) && detector->R_DS_.overlap(right_set)) {
      // do nothing
    } else if (!detector->is_commutative_) {
      legal = false;
    } else if (detector->R_DS_.overlap(left_set) && detector->L_DS_.overlap(right_set)) {
      // do nothing
    } else {
      legal = false;
    }
  }
  if (OB_FAIL(ret) || !legal) {
    // do nothing
  } else if (OB_FAIL(ids.add_members(left_set))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(ids.add_members(right_set))) {
    LOG_WARN("failed to add members", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && legal && i < detector->CR_.count(); ++i) {
      const ObRelIds& T1 = detector->CR_.at(i).first;
      const ObRelIds& T2 = detector->CR_.at(i).second;
      if (T1.overlap(ids) && !T2.is_subset(ids)) {
        legal = false;
      }
    }

    if (OB_SUCC(ret) && legal) {
      for (int64_t i = 0; OB_SUCC(ret) && legal && i < detector->cross_product_rule_.count(); ++i) {
        const ObRelIds& T1 = detector->cross_product_rule_.at(i).first;
        const ObRelIds& T2 = detector->cross_product_rule_.at(i).second;
        if (T1.overlap(left_set) && !T2.is_subset(left_set)) {
          legal = false;
        } else if (T1.overlap(right_set) && !T2.is_subset(right_set)) {
          legal = false;
        }
      }
    }

    if (OB_SUCC(ret) && delay_cross_product && legal) {
      for (int64_t i = 0; OB_SUCC(ret) && legal && i < detector->delay_cross_product_rule_.count(); ++i) {
        const ObRelIds& T1 = detector->delay_cross_product_rule_.at(i).first;
        const ObRelIds& T2 = detector->delay_cross_product_rule_.at(i).second;
        if (T1.overlap(left_set) && !T2.is_subset(left_set)) {
          legal = false;
        } else if (T1.overlap(right_set) && !T2.is_subset(right_set)) {
          legal = false;
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::check_join_hint(
    const ObRelIds& left_set, const ObRelIds& right_set, bool& match_hint, bool& is_legal, bool& is_strict_order)
{
  int ret = OB_SUCCESS;
  if (!left_set.overlap(leading_tables_) && !right_set.overlap(leading_tables_)) {
    match_hint = false;
    is_legal = true;
    is_strict_order = true;
  } else if (left_set.is_subset(leading_tables_) && right_set.is_subset(leading_tables_)) {
    bool found = false;
    for (int64_t i = 0; !found && i < leading_infos_.count(); ++i) {
      const LeadingInfo& info = leading_infos_.at(i);
      if (left_set.equal(info.left_table_set_) && right_set.equal(info.right_table_set_)) {
        is_strict_order = true;
        found = true;
      } else if (right_set.equal(info.left_table_set_) && left_set.equal(info.right_table_set_)) {
        is_strict_order = false;
        found = true;
      }
    }
    if (!found) {
      is_legal = false;
    } else {
      match_hint = true;
      is_legal = true;
    }
  } else if (leading_tables_.is_subset(left_set)) {
    match_hint = true;
    is_legal = true;
    is_strict_order = true;
  } else {
    is_legal = false;
  }
  return ret;
}

int ObLogPlan::extract_onetime_exprs(
    ObRawExpr* expr, ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, ObBitSet<>& idxs)
{
  int ret = OB_SUCCESS;
  bool is_onetime_expr = false;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), KP(expr), KP(get_stmt()));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_is_onetime_expr(get_onetime_exprs(), expr, get_stmt(), is_onetime_expr))) {
    LOG_WARN("failed to check subquery is onetime expr", K(ret));
  } else if (is_onetime_expr) {
    if (OB_FAIL(extract_subquery_ids(expr, idxs))) {
      LOG_WARN("fail to extract param from raw expr", K(ret));
    } else {
      int64_t param_num = ObOptimizerUtil::find_exec_param(get_onetime_exprs(), expr);
      if (param_num >= 0) {
        ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(get_stmt()->get_exec_param_ref_exprs(), param_num);
        if (NULL != param_expr) {
          if (OB_FAIL(onetime_exprs.push_back(std::pair<int64_t, ObRawExpr*>(param_num, expr)))) {
            LOG_WARN("push back onetime_exprs error", K(ret));
          } else { /*do nothing*/
          }
        } else { /* do nothing */
        }
      } else {
        ObRawExpr* temp_expr = expr;
        std::pair<int64_t, ObRawExpr*> init_expr;
        if (OB_FAIL(ObRawExprUtils::create_exec_param_expr(
                get_stmt(), optimizer_context_.get_expr_factory(), temp_expr, NULL, init_expr))) {
          LOG_WARN("create param for stmt error in extract_params_for_nl", K(ret));
        } else if (OB_FAIL(onetime_exprs.push_back(init_expr))) {
          LOG_WARN("push back error", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  } else {
    int64_t count = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(SMART_CALL(extract_onetime_exprs(expr->get_param_expr(i), onetime_exprs, idxs)))) {
        LOG_WARN("fail to extract param from raw expr", K(ret), K(expr->get_param_expr(i)));
      } else { /* Do nothing */
      }
    }
  }
  return ret;
}

int ObLogPlan::extract_subquery_ids(ObRawExpr* expr, ObBitSet<>& idxs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    ObRawExpr::ExprClass expr_class = expr->get_expr_class();
    switch (expr_class) {
      case ObRawExpr::EXPR_CONST: {
        break;
      }
      case ObRawExpr::EXPR_QUERY_REF: {
        if (OB_FAIL(idxs.add_member(static_cast<ObQueryRefRawExpr*>(expr)->get_ref_id()))) {
          LOG_WARN("add_member fails", K(ret));
        } else { /* Do nothing */
        }
        break;
      }
      case ObRawExpr::EXPR_SET_OP:
      case ObRawExpr::EXPR_COLUMN_REF: {
        break;
      }
      case ObRawExpr::EXPR_CASE_OPERATOR:  // fallthrough
      case ObRawExpr::EXPR_AGGR:           // fallthrough
      case ObRawExpr::EXPR_SYS_FUNC:       // fallthrough
      case ObRawExpr::EXPR_UDF:
      case ObRawExpr::EXPR_OPERATOR: {
        int64_t count = expr->get_param_count();
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          if (OB_ISNULL(expr->get_param_expr(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr->get_param_expr(i) is null", K(ret), K(i));
          } else if (!expr->get_param_expr(i)->has_flag(CNT_SUB_QUERY)) {
          } else if (OB_FAIL(SMART_CALL(extract_subquery_ids(expr->get_param_expr(i), idxs)))) {
            LOG_WARN("fail to extract subquery ids from raw expr", K(ret));
          } else { /* Do nothing */
          }
        }
        break;
      }
      case ObRawExpr::EXPR_INVALID_CLASS:
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr class", K(expr_class), K(ret));
        // should not reach here
        break;
    }  // switch case end
  }

  return ret;
}

int ObLogPlan::print_outline(planText& plan, bool is_hints) const
{
  int ret = OB_SUCCESS;
  char* buf = plan.buf;
  int64_t& buf_len = plan.buf_len;
  int64_t& pos = plan.pos;
  bool is_oneline = plan.is_oneline_;
  OutlineType outline_type = plan.outline_type_;
  const ObQueryCtx* query_ctx = NULL;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL", K(ret));
  } else if (OB_ISNULL(query_ctx = stmt_->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_ctx is NULL", K(ret));
  } else if (!is_oneline && OB_FAIL(BUF_PRINTF("  "))) {
  } else if (!is_hints && OB_FAIL(BUF_PRINTF("/*+"))) {                                      /* Do nothing */
  } else if (OUTLINE_DATA == outline_type && !is_oneline && OB_FAIL(BUF_PRINTF(NEW_LINE))) { /* Do nothing */
  } else if (OUTLINE_DATA == outline_type &&
             OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {           /* Do nothing */
  } else if (OUTLINE_DATA == outline_type && OB_FAIL(BUF_PRINTF("BEGIN_OUTLINE_DATA"))) { /* Do nothing */
  } else if (OB_FAIL(const_cast<ObLogPlan*>(this)->plan_tree_traverse(EXPLAIN_WRITE_BUFFER_OUTLINE, &plan))) {
    LOG_WARN("failed to print output exprs", K(ret));
  } else if (OB_FAIL(stmt_->get_stmt_hint().print_global_hint_for_outline(plan, stmt_))) {
    LOG_WARN("fail to print global hint for outline", K(ret));
  } else if (OB_FAIL(query_ctx->print_qb_name_hint(plan))) {
    LOG_WARN("fail to print qb_name", K(ret));
  } else if (OUTLINE_DATA == outline_type && !is_oneline && OB_FAIL(BUF_PRINTF(NEW_LINE))) { /* Do nothing */
  } else if (OUTLINE_DATA == outline_type &&
             OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {         /* Do nothing */
  } else if (OUTLINE_DATA == outline_type && OB_FAIL(BUF_PRINTF("END_OUTLINE_DATA"))) { /* Do nothing */
  } else if (!is_oneline && OB_FAIL(BUF_PRINTF(NEW_LINE))) {                            /* Do nothing */
  } else if (!is_oneline && OB_FAIL(BUF_PRINTF("  "))) {
  } else if (!is_hints && OB_FAIL(BUF_PRINTF("*/"))) { /* Do nothing */
  }
  return ret;
}

int ObLogPlan::print_outline_oneline(char* buf, int64_t buf_len, int64_t& pos) const
{

  int ret = OB_SUCCESS;
  planText plan(buf, buf_len, EXPLAIN_OUTLINE);
  plan.is_oneline_ = true;
  plan.outline_type_ = OUTLINE_DATA;
  if (OB_FAIL(print_outline(plan))) {
    LOG_WARN("fail to print outline", K(ret), "buf", ObString(static_cast<int32_t>(plan.pos), buf));
  } else {
    pos = plan.pos;
  }
  return ret;
}

double ObLogPlan::get_expr_selectivity(const ObRawExpr* expr, bool& found)
{
  double sel = 1.0;
  found = false;
  if (OB_ISNULL(expr)) {
  } else {
    for (int64_t i = 0; !found && i < predicate_selectivities_.count(); ++i) {
      if (expr == predicate_selectivities_.at(i).expr_) {
        found = true;
        sel = predicate_selectivities_.at(i).sel_;
      }
    }
  }
  return sel;
}

int ObLogPlan::store_param_expr_selectivity(ObRawExpr* src_expr, ObRawExpr* param_expr, ObEstSelInfo& sel_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_expr) || OB_ISNULL(param_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(src_expr), K(param_expr));
  } else {
    bool found_sel = false;
    double sel = get_expr_selectivity(src_expr, found_sel);
    if (!found_sel) {
      LOG_TRACE("Not found param expr selectivity");
      if (OB_FAIL(ObOptEstSel::clause_selectivity(sel_info, src_expr, sel, &get_predicate_selectivities()))) {
        LOG_WARN("Failed to calc selectivity", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = add_var_to_array_no_dup(
                                         get_predicate_selectivities(), ObExprSelPair(param_expr, sel))))) {
        LOG_WARN("failed to add selectivity to plan", K(tmp_ret), K(param_expr), K(sel));
      }
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_subplan_filter_for_where()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> pushdown_subquery;
  ObSEArray<ObRawExpr*, 4> none_pushdown_subquery;
  for (int64_t i = 0; i < subquery_filters_.count(); ++i) {
    ObRawExpr* expr = subquery_filters_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->has_flag(CNT_SUB_QUERY)) {
      ret = none_pushdown_subquery.push_back(expr);
    } else if (OB_FAIL(extract_onetime_subquery_exprs(expr, pushdown_subquery))) {
      LOG_WARN("failed to extract onetime subquery exprs", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(candi_allocate_subplan_filter(pushdown_subquery, false))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter(none_pushdown_subquery, true))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogPlan::candi_allocate_subplan_filter(ObIArray<ObRawExpr*>& subquery_exprs, const bool is_filter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(candi_allocate_subplan_filter(subquery_exprs, is_filter, candidates_))) {
    LOG_WARN("failed to allocate subplan filter as top", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogPlan::candi_allocate_subplan_filter(
    ObIArray<ObRawExpr*>& subquery_exprs, const bool is_filter, All_Candidate_Plans& all_plans)
{
  int ret = OB_SUCCESS;
  ObBitSet<> initplan_idxs;
  ObBitSet<> onetime_idxs;
  ObSEArray<ObLogicalOperator*, 4> subquery_ops;
  ObSEArray<std::pair<int64_t, ObRawExpr*>, 4> params;
  ObSEArray<std::pair<int64_t, ObRawExpr*>, 4> onetime_exprs;
  ObLogicalOperator* best_plan = NULL;
  if (OB_FAIL(all_plans.get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpectd null", K(ret));
  } else if (OB_FAIL(generate_subplan_filter_info(
                 subquery_exprs, best_plan, subquery_ops, params, onetime_exprs, initplan_idxs, onetime_idxs))) {
    LOG_WARN("failed to generated subplan filter info", K(ret));
  } else if (subquery_ops.empty() && !is_filter) {
    /*do nothing*/
  } else if (is_filter && !onetime_exprs.empty() &&
             OB_FAIL(find_and_replace_onetime_expr_with_param(onetime_exprs, subquery_exprs))) {
    LOG_WARN("failed to find and replace onetime expr with param", K(ret));
  } else if (subquery_ops.empty()) {
    if (OB_FAIL(candi_allocate_filter(subquery_exprs))) {
      LOG_WARN("failed to allocate filter as top", K(ret));
    } else { /*do nothing*/
    }
  } else {
    double min_cost = 0.0;
    int64_t min_idx = OB_INVALID_INDEX;
    ObIArray<ObRawExpr*>* filters = NULL;
    if (is_filter) {
      filters = &subquery_exprs;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_plans.candidate_plans_.count(); ++i) {
      CandidatePlan& plain_plan = all_plans.candidate_plans_.at(i);
      if (OB_FAIL(allocate_subplan_filter_as_top(
              plain_plan.plan_tree_, subquery_ops, params, onetime_exprs, initplan_idxs, onetime_idxs, filters))) {
        LOG_WARN("failed to allocate filter", K(ret));
      } else if (OB_ISNULL(plain_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Plan_tree_ is NULL", K(ret));
      } else if (plain_plan.plan_tree_->get_cost() < min_cost || OB_INVALID_INDEX == min_idx) {
        min_cost = plain_plan.plan_tree_->get_cost();
        min_idx = i;
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(OB_INVALID_INDEX == min_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected index", K(ret), K(min_idx));
      } else {
        all_plans.plain_plan_.second = min_idx;
        all_plans.plain_plan_.first = min_cost;
      }
    }
  }
  if (OB_SUCC(ret) && !onetime_exprs.empty()) {
    if (OB_FAIL(add_onetime_exprs(onetime_exprs))) {
      LOG_WARN("failed to add onetime exprs", K(ret));
    } else if (OB_FAIL(adjust_stmt_onetime_exprs(onetime_exprs))) {
      LOG_WARN("failed to adjust stme onetime exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogPlan::generate_subplan_filter_info(const ObIArray<ObRawExpr*>& subquery_exprs, ObLogicalOperator* top_node,
    ObIArray<ObLogicalOperator*>& subquery_ops, ObIArray<std::pair<int64_t, ObRawExpr*>>& params,
    ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, ObBitSet<>& initplan_idxs, ObBitSet<>& onetime_idxs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(top_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSEArray<SubPlanInfo*, 4> subplan_infos;
    ObSEArray<SubPlanInfo*, 4> temp_subplan_infos;
    ObBitSet<> temp_onetime_idxs;
    ObSEArray<std::pair<int64_t, ObRawExpr*>, 4> temp_onetime_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); i++) {
      ObRawExpr* temp_expr = NULL;
      temp_subplan_infos.reuse();
      if (OB_ISNULL(temp_expr = subquery_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(generate_subplan(temp_expr))) {
        LOG_WARN("failed to generate subplan", K(ret));
      } else if (OB_FAIL(get_subplan_info(temp_expr, temp_subplan_infos))) {
        LOG_WARN("failed to generate subplan info", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < temp_subplan_infos.count(); j++) {
          bool need_alloc = false;
          if (OB_FAIL(need_alloc_child_for_subplan_filter(top_node, temp_subplan_infos.at(j)->subplan_, need_alloc))) {
            LOG_WARN(
                "failed to check whether need alloc subplan filter", K(need_alloc), K(top_node->get_type()), K(ret));
          } else if (need_alloc && OB_FAIL(add_var_to_array_no_dup(subplan_infos, temp_subplan_infos.at(j)))) {
            LOG_WARN("failed to push back subplan infos", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < subplan_infos.count(); i++) {
      ObQueryRefRawExpr* ref_subquery_expr = NULL;
      if (OB_ISNULL(subplan_infos.at(i)) || OB_ISNULL(subplan_infos.at(i)->subplan_) ||
          OB_ISNULL(subplan_infos.at(i)->subplan_->get_plan_root()) || OB_ISNULL(subplan_infos.at(i)->init_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(subquery_ops.push_back(subplan_infos.at(i)->subplan_->get_plan_root()))) {
        LOG_WARN("failed to push back child ops", K(ret));
      } else if (OB_FAIL(append(params, subplan_infos.at(i)->sp_params_))) {
        LOG_WARN("failed to append params", K(ret));
      } else if (subplan_infos.at(i)->init_plan_ && OB_FAIL(initplan_idxs.add_member(i + 1))) {
        LOG_WARN("failed to add init plan idxs", K(ret));
      } else {
        ref_subquery_expr = static_cast<ObQueryRefRawExpr*>(subplan_infos.at(i)->init_expr_);
        ref_subquery_expr->set_ref_id(i + 1);
        ref_subquery_expr->set_ref_operator(subplan_infos.at(i)->subplan_->get_plan_root());
      }
    }
    if (OB_SUCC(ret) && !subquery_ops.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); i++) {
        temp_onetime_exprs.reuse();
        temp_onetime_idxs.reuse();
        if (OB_ISNULL(subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(extract_onetime_exprs(subquery_exprs.at(i), temp_onetime_exprs, temp_onetime_idxs))) {
          LOG_WARN("failed to extract onetime exprs", K(ret));
        } else if (OB_FAIL(append(onetime_exprs, temp_onetime_exprs))) {
          LOG_WARN("failed to append onetime exprs", K(ret));
        } else if (OB_FAIL(onetime_idxs.add_members(temp_onetime_idxs))) {
          LOG_WARN("failed to add member", K(ret));
        } else {
          LOG_TRACE("succeed to get onetime exprs", K(*subquery_exprs.at(i)), K(temp_onetime_idxs));
        }
      }
    }
    if (OB_SUCC(ret)) {
      initplan_idxs.del_members(onetime_idxs);
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_filter(const ObIArray<ObRawExpr*>& filter_exprs)
{
  int ret = OB_SUCCESS;
  double sel = 1.0;
  ObLogicalOperator* best_plan = NULL;
  if (OB_FAIL(get_current_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(
                 *best_plan->get_est_sel_info(), filter_exprs, sel, &get_predicate_selectivities()))) {
    LOG_WARN("failed to calc selectivity", K(ret));
  } else {
    /*update the number of rows for the top_node*/
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
      ObLogicalOperator* top = candidates_.candidate_plans_.at(i).plan_tree_;
      if (OB_ISNULL(top)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(append(top->get_filter_exprs(), filter_exprs))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else {
        top->set_card(top->get_card() * sel);
      }
    }
  }
  return ret;
}

int ObLogPlan::find_and_replace_onetime_expr_with_param(
    const ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, ObIArray<ObRawExpr*>& ori_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ori_exprs.count(); i++) {
    if (OB_FAIL(find_and_replace_onetime_expr_with_param(onetime_exprs, ori_exprs.at(i)))) {
      LOG_WARN("failed to find and replace onetime expr with param", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogPlan::find_and_replace_onetime_expr_with_param(
    const ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, ObRawExpr*& ori_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ori_expr) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ori_expr), K(get_stmt()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < onetime_exprs.count(); ++i) {
      int64_t param_idx = onetime_exprs.at(i).first;
      const ObRawExpr* onetime_expr = onetime_exprs.at(i).second;
      ObRawExpr** addr_matched_expr = NULL;
      if (ObOptimizerUtil::is_sub_expr(onetime_expr, ori_expr, addr_matched_expr)) {
        ObRawExpr* exec_param_expr =
            ObOptimizerUtil::find_param_expr(get_stmt()->get_exec_param_ref_exprs(), param_idx);
        if (OB_ISNULL(exec_param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          *addr_matched_expr = exec_param_expr;
        }
      } else { /* Do nothing */
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_subplan_filter_as_top(ObLogicalOperator*& top, const ObIArray<ObLogicalOperator*>& subquery_ops,
    const ObIArray<std::pair<int64_t, ObRawExpr*>>& params,
    const ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, const ObBitSet<>& initplan_idxs,
    const ObBitSet<>& onetime_idxs, const ObIArray<ObRawExpr*>* filters)
{
  int ret = OB_SUCCESS;
  ObLogSubPlanFilter* spf_node = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(ret));
  } else if (OB_ISNULL(spf_node = static_cast<ObLogSubPlanFilter*>(
                           get_log_op_factory().allocate(*this, LOG_SUBPLAN_FILTER)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(spf_node), K(ret));
  } else if (OB_FAIL(spf_node->add_child(top))) {
    LOG_WARN("failed to add child", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_ops.count(); i++) {
      if (OB_FAIL(spf_node->add_child(subquery_ops.at(i)))) {
        LOG_WARN("failed to add children", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != filters && OB_FAIL(append(spf_node->get_filter_exprs(), *filters))) {
        LOG_WARN("failed to append filter exprs", K(ret));
      } else if (OB_FAIL(spf_node->add_exec_params(params))) {
        LOG_WARN("failed to add exec params", K(ret));
      } else if (OB_FAIL(spf_node->add_onetime_exprs(onetime_exprs))) {
        LOG_WARN("failed to add onetime exprs", K(ret));
      } else if (OB_FAIL(spf_node->add_initplan_idxs(initplan_idxs))) {
        LOG_WARN("failed to add init plan idxs", K(ret));
      } else if (OB_FAIL(spf_node->add_onetime_idxs(onetime_idxs))) {
        LOG_WARN("failed to add onetime idxs", K(ret));
      } else if (OB_FAIL(spf_node->compute_property())) {
        LOG_WARN("failed to compute property", K(ret));
      } else {
        top = spf_node;
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_subplan_filter_as_top(
    ObIArray<ObRawExpr*>& subquery_exprs, const bool is_filter, ObLogicalOperator*& top_node)
{
  int ret = OB_SUCCESS;
  All_Candidate_Plans all_plans;
  if (OB_ISNULL(top_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(all_plans.candidate_plans_.push_back(CandidatePlan(top_node)))) {
    LOG_WARN("failed to push back plans", K(ret));
  } else {
    all_plans.plain_plan_ = std::pair<double, int64_t>(top_node->get_cost(), 0);
    if (OB_FAIL(candi_allocate_subplan_filter(subquery_exprs, is_filter, all_plans))) {
      LOG_WARN("failed to allocate subplan filter", K(ret));
    } else if (OB_UNLIKELY(1 != all_plans.candidate_plans_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected arry count", K(ret));
    } else {
      top_node = all_plans.candidate_plans_.at(0).plan_tree_;
      LOG_TRACE("succeed to allocate subplan filter as top", K(subquery_exprs), K(top_node->get_type()));
    }
  }
  return ret;
}

int ObLogPlan::adjust_stmt_onetime_exprs(ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  } else {
    ObSEArray<ObRawExpr*, 8> old_exprs;
    ObSEArray<ObRawExpr*, 8> new_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < onetime_exprs.count(); i++) {
      ObRawExpr* temp_expr = NULL;
      if (OB_ISNULL(onetime_exprs.at(i).second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(old_exprs.push_back(onetime_exprs.at(i).second))) {
        LOG_WARN("failed to push back old exprs", K(ret));
      } else if (OB_ISNULL(temp_expr = ObOptimizerUtil::find_param_expr(
                               get_stmt()->get_exec_param_ref_exprs(), onetime_exprs.at(i).first))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(new_exprs.push_back(temp_expr))) {
        LOG_WARN("get unexpected null", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && !new_exprs.empty()) {
      if (OB_FAIL(get_stmt()->replace_inner_stmt_expr(old_exprs, new_exprs))) {
        LOG_WARN("failed to replace inner stmt expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogPlan::remove_null_direction_for_order_item(
    common::ObIArray<OrderItem>& order_items, common::ObIArray<ObOrderDirection>& directions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null stmt", K(ret));
  } else {
    const ObIArray<ObRawExpr*>& conditions = get_stmt()->get_condition_exprs();
    int64_t N = order_items.count();
    bool has_null_reject = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      OrderItem& order_item = order_items.at(i);
      if (OB_ISNULL(order_item.expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null expr", K(ret), K(order_item.expr_));
      } else if (OB_FAIL(ObTransformUtils::has_null_reject_condition(conditions, order_item.expr_, has_null_reject))) {
        LOG_WARN("failed to check null rejection", K(ret));
      } else if (has_null_reject) {
        if (is_ascending_direction(order_item.order_type_)) {
          order_item.order_type_ = default_asc_direction();
          directions.at(i) = order_item.order_type_;
        } else {
          order_item.order_type_ = default_desc_direction();
          directions.at(i) = order_item.order_type_;
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::need_alloc_child_for_subplan_filter(ObLogicalOperator* root, ObLogPlan* sub_plan, bool& need)
{
  int ret = OB_SUCCESS;
  need = true;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && need && i < root->get_num_of_child(); ++i) {
      if (log_op_def::LOG_SUBPLAN_FILTER == root->get_type() && i > 0) {
        if (OB_ISNULL(root->get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("root->get_child(i) is null", K(ret), K(i));
        } else if (root->get_child(i)->get_plan() == sub_plan) {
          need = false;
        } else { /* Do nothing */
        }
      } else { /* Do nothing */
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && need && i < root->get_num_of_child(); ++i) {
      if (OB_FAIL(need_alloc_child_for_subplan_filter(root->get_child(i), sub_plan, need))) {
        LOG_WARN("need_alloc_subfilter fails", K(ret), K(need));
      } else { /* Do nothing */
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_subplan_filter_as_below(common::ObIArray<ObRawExpr*>& exprs, ObLogicalOperator* top_node)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* first_child = NULL;
  ObSEArray<ObRawExpr*, 4> subquery_exprs;
  if (OB_ISNULL(top_node) || OB_ISNULL(first_child = top_node->get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(top_node), K(first_child));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(exprs, subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (subquery_exprs.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(allocate_subplan_filter_as_top(subquery_exprs, false, first_child))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (OB_ISNULL(first_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first_child), K(ret));
  } else {
    top_node->set_child(0, first_child);
  }
  return ret;
}

int ObLogPlan::candi_allocate_count()
{
  int ret = OB_SUCCESS;
  ObRawExpr* limit_expr = NULL;
  ObSEArray<ObRawExpr*, 4> filter_exprs;
  ObSEArray<ObRawExpr*, 4> start_exprs;
  ObSEArray<ObRawExpr*, 4> subquery_exprs;
  if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(get_rownum_exprs(), subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (!subquery_exprs.empty() && OB_FAIL(candi_allocate_subplan_filter(subquery_exprs, false))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (OB_FAIL(classify_rownum_exprs(get_rownum_exprs(), filter_exprs, start_exprs, limit_expr))) {
    LOG_WARN("failed to classify rownum exprs", K(ret));
  } else {
    double min_cost = 0.0;
    int64_t min_idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
      CandidatePlan& plain_plan = candidates_.candidate_plans_.at(i);
      if (OB_FAIL(allocate_count_as_top(plain_plan.plan_tree_, filter_exprs, start_exprs, limit_expr))) {
        LOG_WARN("failed to allocate count operator", K(ret));
      } else if (OB_ISNULL(plain_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null plan tree", K(ret));
      } else if (plain_plan.plan_tree_->get_cost() < min_cost || OB_INVALID_INDEX == min_idx) {
        min_cost = plain_plan.plan_tree_->get_cost();
        min_idx = i;
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(OB_INVALID_INDEX == min_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Min idx is invalid", K(ret), K(min_idx));
      } else {
        candidates_.plain_plan_.second = min_idx;
        candidates_.plain_plan_.first = min_cost;
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_count_as_top(ObLogicalOperator*& old_top, const common::ObIArray<ObRawExpr*>& filter_exprs,
    const common::ObIArray<ObRawExpr*>& start_exprs, ObRawExpr* limit_expr)
{
  int ret = OB_SUCCESS;
  ObLogCount* count = NULL;
  if (OB_ISNULL(old_top) || OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected null stmt", K(ret), K(old_top), K(get_stmt()));
  } else if (OB_ISNULL(count = static_cast<ObLogCount*>(get_log_op_factory().allocate(*this, LOG_COUNT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory for ObLogCount Failed", K(ret));
  } else if (OB_FAIL(append(count->get_filter_exprs(), filter_exprs))) {
    LOG_WARN("failed to append filter exprs", K(ret));
  } else if (OB_FAIL(append(count->get_startup_exprs(), start_exprs))) {
    LOG_WARN("failed to append start up exprs", K(ret));
  } else {
    count->set_rownum_limit_expr(limit_expr);
    count->set_child(ObLogicalOperator::first_child, old_top);
    // set cost and card
    if (OB_FAIL(count->set_limit_size())) {
      LOG_WARN("set limit size error", K(ret));
    } else if (OB_FAIL(count->compute_property())) {
      LOG_WARN("failed to compute property");
    } else {
      old_top = count;
    }
  }
  return ret;
}

int ObLogPlan::classify_rownum_exprs(const ObIArray<ObRawExpr*>& rownum_exprs, ObIArray<ObRawExpr*>& filter_exprs,
    ObIArray<ObRawExpr*>& start_exprs, ObRawExpr*& limit_expr)
{
  int ret = OB_SUCCESS;
  ObItemType limit_rownum_type = T_INVALID;
  limit_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < rownum_exprs.count(); i++) {
    ObRawExpr* rownum_expr = rownum_exprs.at(i);
    ObRawExpr* const_expr = NULL;
    ObItemType expr_type = T_INVALID;
    bool dummy_flag = false;
    if (OB_FAIL(ObOptimizerUtil::get_rownum_filter_info(rownum_expr, expr_type, const_expr, dummy_flag))) {
      LOG_WARN("failed to check is rownum expr used as filter", K(ret));
    } else if (OB_FAIL(
                   classify_rownum_expr(expr_type, rownum_expr, const_expr, filter_exprs, start_exprs, limit_expr))) {
      LOG_WARN("failed to classify rownum expr", K(ret));
    } else if (const_expr == limit_expr) {
      limit_rownum_type = expr_type;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(limit_expr)) {
    ObRawExpr* limit_int_expr = NULL;
    if (OB_FAIL(ObOptimizerUtil::convert_rownum_filter_as_limit(get_optimizer_context().get_expr_factory(),
            get_optimizer_context().get_session_info(),
            limit_rownum_type,
            limit_expr,
            limit_int_expr))) {
      LOG_WARN("failed to transform rownum filter as limit", K(ret));
    } else {
      limit_expr = limit_int_expr;
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to classify rownum exprs", K(filter_exprs.count()), K(start_exprs.count()), K(limit_expr));
  }
  return ret;
}

int ObLogPlan::classify_rownum_expr(const ObItemType expr_type, ObRawExpr* rownum_expr, ObRawExpr* left_const_expr,
    common::ObIArray<ObRawExpr*>& filter_exprs, common::ObIArray<ObRawExpr*>& start_exprs, ObRawExpr*& limit_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rownum_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(rownum_expr), K(ret));
  } else if ((expr_type == T_OP_LE || expr_type == T_OP_LT) && NULL == limit_expr && NULL != left_const_expr) {
    limit_expr = left_const_expr;
  } else if (expr_type == T_OP_GE || expr_type == T_OP_GT) {
    if (OB_FAIL(start_exprs.push_back(rownum_expr))) {
      LOG_WARN("failed to push back rownum expr", K(ret));
    } else { /*do nothing*/
    }
  } else {
    if (OB_FAIL(filter_exprs.push_back(rownum_expr))) {
      LOG_WARN("failed to push back rownum expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogPlan::get_base_table_ids(ObIArray<uint64_t>& dependency_tables) const
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = NULL;
  const ObQueryCtx* query_ctx = NULL;
  if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(query_ctx = stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(stmt), K(query_ctx));
  }
  ARRAY_FOREACH(query_ctx->table_partition_infos_, i)
  {
    const ObTablePartitionInfo* tbl_info = query_ctx->table_partition_infos_.at(i);
    if (OB_ISNULL(tbl_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tbl_info is null");
    } else if (OB_FAIL(dependency_tables.push_back(tbl_info->get_ref_table_id()))) {
      LOG_WARN("store dependency table failed", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::remove_duplicate_constraint(ObLocationConstraintContext& location_constraint, ObSqlCtx& sql_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remove_duplicate_base_table_constraint(location_constraint))) {
    LOG_WARN("failed to remove duplicate base table constraint", K(ret));
  } else if (OB_FAIL(remove_duplicate_pwj_constraint(location_constraint.strict_constraints_))) {
    LOG_WARN("failed to remove duplicate strict pwj constraint", K(ret));
  } else if (OB_FAIL(remove_duplicate_pwj_constraint(location_constraint.non_strict_constraints_))) {
    LOG_WARN("failed to remove duplicate strict pwj constraint", K(ret));
  } else if (OB_FAIL(sort_pwj_constraint(location_constraint))) {
    LOG_WARN("failed to sort pwj constraint", K(ret));
  } else if (OB_FAIL(sql_ctx.set_location_constraints(location_constraint, get_allocator()))) {
    LOG_WARN("failed to set location constraints", K(ret));
  } else {
    LOG_TRACE("duplicated constraints removed", K(location_constraint));
  }
  return ret;
}

int ObLogPlan::remove_duplicate_base_table_constraint(ObLocationConstraintContext& location_constraint) const
{
  int ret = OB_SUCCESS;
  ObLocationConstraint& base_constraints = location_constraint.base_table_constraints_;
  ObLocationConstraint unique_constraint;
  for (int64_t i = 0; OB_SUCC(ret) && i < base_constraints.count(); ++i) {
    bool find = false;
    int64_t j = 0;
    for (/* do nothing */; !find && j < unique_constraint.count(); ++j) {
      if (base_constraints.at(i) == unique_constraint.at(j)) {
        find = true;
      }
    }
    if (find) {
      --j;
      if (OB_FAIL(replace_pwj_constraints(location_constraint.strict_constraints_, i, j))) {
        LOG_WARN("failed to replace pwj constraints", K(ret));
      } else if (OB_FAIL(replace_pwj_constraints(location_constraint.non_strict_constraints_, i, j))) {
        LOG_WARN("failed to replace pwj constraints", K(ret));
      }
    } else if (OB_FAIL(unique_constraint.push_back(base_constraints.at(i)))) {
      LOG_WARN("failed to push back location constraint", K(ret));
      // do nothing
    }
  }
  if (OB_SUCC(ret) && unique_constraint.count() != base_constraints.count()) {
    if (OB_FAIL(base_constraints.assign(unique_constraint))) {
      LOG_WARN("failed to assign base constraints", K(ret));
    }
    LOG_TRACE("inner duplicates removed", K(location_constraint));
  }

  return ret;
}

int ObLogPlan::replace_pwj_constraints(
    ObIArray<ObPwjConstraint*>& constraints, const int64_t from, const int64_t to) const
{
  int ret = OB_SUCCESS;
  ObPwjConstraint* cur_cons = NULL;
  ObSEArray<ObPwjConstraint*, 4> new_constraints;
  for (int64_t i = 0; OB_SUCC(ret) && i < constraints.count(); ++i) {
    if (OB_ISNULL(cur_cons = constraints.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    }

    for (int64_t j = 0; OB_SUCC(ret) && j < cur_cons->count(); ++j) {
      if (from == cur_cons->at(j)) {
        if (OB_FAIL(cur_cons->remove(j))) {
          LOG_WARN("failed to remove item", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(*cur_cons, to))) {
          LOG_WARN("failed to add var to array no dup");
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (cur_cons->count() > 1 && OB_FAIL(new_constraints.push_back(cur_cons))) {
        LOG_WARN("failed to push back pwj constraint", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && new_constraints.count() < constraints.count()) {
    if (OB_FAIL(constraints.assign(new_constraints))) {
      LOG_WARN("failed to assign new constraints", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::remove_duplicate_pwj_constraint(ObIArray<ObPwjConstraint*>& pwj_constraints) const
{
  int ret = OB_SUCCESS;
  ObPwjConstraint *l_cons = NULL, *r_cons = NULL;
  ObBitSet<> removed_idx;
  ObLocationConstraintContext::InclusionType inclusion_result = ObLocationConstraintContext::NotSubset;

  for (int64_t i = 0; OB_SUCC(ret) && i < pwj_constraints.count(); ++i) {
    if (OB_ISNULL(l_cons = pwj_constraints.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (l_cons->count() < 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected constraint const", K(ret), K(*l_cons));
    } else if (removed_idx.has_member(i)) {
      // do nothing
    } else {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < pwj_constraints.count(); ++j) {
        if (OB_ISNULL(r_cons = pwj_constraints.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(j));
        } else if (OB_FAIL(ObLocationConstraintContext::calc_constraints_inclusion(l_cons, r_cons, inclusion_result))) {
          LOG_WARN("failed to calculate inclusion relation between constraints", K(ret), K(*l_cons), K(*r_cons));
        } else if (ObLocationConstraintContext::LeftIsSuperior == inclusion_result) {
          // Left containts all the elements of the right, remove j
          if (OB_FAIL(removed_idx.add_member(j))) {
            LOG_WARN("failed to add member", K(ret), K(j));
          }
        } else if (ObLocationConstraintContext::RightIsSuperior == inclusion_result) {
          // Right containts all the elements of the left, remove i
          if (OB_FAIL(removed_idx.add_member(i))) {
            LOG_WARN("failed to add member", K(ret), K(i));
          }
        }
      }
    }
  }

  // get unique pwj constraints
  if (OB_SUCC(ret) && !removed_idx.is_empty()) {
    ObSEArray<ObPwjConstraint*, 8> tmp_constraints;
    if (OB_FAIL(tmp_constraints.assign(pwj_constraints))) {
      LOG_WARN("failed to assign strict constraints", K(ret));
    } else {
      pwj_constraints.reuse();
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_constraints.count(); ++i) {
        if (!removed_idx.has_member(i) && OB_FAIL(pwj_constraints.push_back(tmp_constraints.at(i)))) {
          LOG_WARN("failed to push back pwj constraint", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::set_all_exprs(const ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.expr_producers_.count(); i++) {
    const ObRawExpr* expr = ctx.expr_producers_.at(i).expr_;
    OZ(all_exprs_.append(const_cast<ObRawExpr*>(expr)));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.group_replaced_exprs_.count(); i++) {
    ObRawExpr* expr = ctx.group_replaced_exprs_.at(i).first;
    OZ(all_exprs_.append(expr));
    expr = ctx.group_replaced_exprs_.at(i).second;
    OZ(all_exprs_.append(expr));
  }
  OZ(all_exprs_.append(ctx.not_produced_exprs_.get_expr_array()));

  return ret;
}

int ObLogPlan::sort_pwj_constraint(ObLocationConstraintContext& location_constraint) const
{
  int ret = OB_SUCCESS;
  ObIArray<ObPwjConstraint*>& strict_pwj_cons = location_constraint.strict_constraints_;
  ObIArray<ObPwjConstraint*>& non_strict_pwj_cons = location_constraint.non_strict_constraints_;
  ObPwjConstraint* cur_cons = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < strict_pwj_cons.count(); ++i) {
    if (OB_ISNULL(cur_cons = strict_pwj_cons.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else {
      std::sort(&cur_cons->at(0), &cur_cons->at(0) + cur_cons->count());
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < non_strict_pwj_cons.count(); ++i) {
    if (OB_ISNULL(cur_cons = non_strict_pwj_cons.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else {
      std::sort(&cur_cons->at(0), &cur_cons->at(0) + cur_cons->count());
    }
  }
  return ret;
}

/*
 *
 *  Select 1 from t2 connect by level < t2.c1;
 *  We generate a ? expresstion from this level pseudo expr.
 *  Why ?
 *  The level pseudo expr result is generate by connect by join,
 *  and 'level < t2.c1' is a other join condition.
 *  When we generate code for this expression, we will find all input
 *  from children' output. We can find 't2.c1', but we can't find 'level'.
 *  This level pseudo is output of this connect by join.
 *  So finally, we use ? expr to solve this problem.
 *  Just generate this ? expr here, but remember not to replace.
 *  If replace level pesudo expr to ? expr,  say 'level < 5' -> '? < 5',
 *  our optimizer will push this exprs down to the lowest level child.
 *  Do replace at code generate.
 *
 */
int ObLogPlan::extract_level_pseudo_as_param(
    const ObIArray<ObRawExpr*>& join_condition, ObIArray<std::pair<int64_t, ObRawExpr*>>& exec_param)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = nullptr;
  ObSEArray<ObRawExpr*, 16> level_exprs;
  ObSQLSessionInfo* session_info = nullptr;
  for (int64_t i = 0; i < join_condition.count() && OB_SUCC(ret); ++i) {
    ObRawExpr* qual = join_condition.at(i);
    if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL((get_optimizer_context()).get_session_info())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Null poiner passed in", K(stmt), K(ret));
    } else if (OB_FAIL(find_connect_by_level(qual, level_exprs))) {
      LOG_WARN("Failed to find level pseudo", K(ret));
    } else if (level_exprs.empty()) {
      // do nothing
    } else {
      ObOptimizerContext& opt_ctx = get_optimizer_context();
      session_info = const_cast<ObSQLSessionInfo*>(opt_ctx.get_session_info());
      std::pair<int64_t, ObRawExpr*> param_expr;
      ObRawExpr* expr_element = level_exprs.at(0);
      if (OB_FAIL(ObRawExprUtils::create_exec_param_expr(
              stmt, opt_ctx.get_expr_factory(), expr_element, session_info, param_expr))) {
        LOG_WARN("Create param for stmt error in extract_params_for_stmt", K(ret));
      } else if (FALSE_IT(param_expr.second = expr_element)) {
      } else if (OB_FAIL(exec_param.push_back(param_expr))) {
        LOG_WARN("Failed to push back param expr", K(ret));
      } else {
        LOG_TRACE("Get a level pseudo as param", K(param_expr.first), K(*param_expr.second), K(*expr_element));
        break;
      }
    }
  }
  return ret;
}

int ObLogPlan::find_connect_by_level(ObRawExpr* qual, ObIArray<ObRawExpr*>& level_exprs)
{
  int ret = OB_SUCCESS;
  ObRawConnectByLevelVisitor visitor;
  if (OB_FAIL(visitor.check_connectby_filter(qual, level_exprs))) {
    LOG_WARN("Failed to check connectby filter", K(ret));
  } else {
    LOG_TRACE("Find level pseudo", K(level_exprs), K(ret));
  }
  return ret;
}

int ObLogPlan::update_plans_interesting_order_info(ObIArray<CandidatePlan>& candidate_plans, const int64_t check_scope)
{
  int ret = OB_SUCCESS;
  int64_t match_info = OrderingFlag::NOT_MATCH;
  if (check_scope == OrderingCheckScope::NOT_CHECK) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < candidate_plans.count(); i++) {
      CandidatePlan& candidate_plan = candidate_plans.at(i);
      if (OB_ISNULL(candidate_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::compute_stmt_interesting_order(candidate_plan.plan_tree_->get_op_ordering(),
                     get_stmt(),
                     get_is_subplan_scan(),
                     get_equal_sets(),
                     get_const_exprs(),
                     check_scope,
                     match_info))) {
        LOG_WARN("failed to update ordering match info", K(ret));
      } else {
        candidate_plan.plan_tree_->set_interesting_order_info(match_info);
      }
    }
  }
  return ret;
}

int ObLogPlan::prune_and_keep_best_plans(ObIArray<CandidatePlan>& candidate_plans)
{
  int ret = OB_SUCCESS;
  candidates_.candidate_plans_.reset();
  double min_cost = 0.0;
  int64_t min_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < candidate_plans.count(); i++) {
    CandidatePlan& candidate_plan = candidate_plans.at(i);
    if (OB_FAIL(add_candidate_plan(candidates_.candidate_plans_, candidate_plan))) {
      LOG_WARN("failed to add candidate plan", K(ret));
    } else { /*do nothing*/
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
    CandidatePlan& candidate_plan = candidates_.candidate_plans_.at(i);
    if (min_idx == OB_INVALID_INDEX || candidate_plan.plan_tree_->get_cost() < min_cost) {
      min_idx = i;
      min_cost = candidate_plan.plan_tree_->get_cost();
    } else { /*do nothing */
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(min_idx == OB_INVALID_INDEX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Min idx is invalid", K(ret), K(min_idx));
    } else {
      candidates_.plain_plan_.second = min_idx;
      candidates_.plain_plan_.first = min_cost;
      candidates_.first_new_op_ = NULL;
    }
  }
  return ret;
}

int ObLogPlan::add_candidate_plan(ObIArray<CandidatePlan>& current_plans, const CandidatePlan& new_plan)
{
  int ret = OB_SUCCESS;
  bool should_add = true;
  PlanRelationship plan_rel = UNCOMPARABLE;
  for (int64_t i = current_plans.count() - 1; OB_SUCC(ret) && should_add && i >= 0; --i) {
    if (OB_FAIL(compute_plan_relationship(current_plans.at(i), new_plan, plan_rel))) {
      LOG_WARN("failed to compute plan relationship", K(current_plans.at(i)), K(new_plan), K(ret));
    } else if (LEFT_DOMINATED == plan_rel || EQUAL == plan_rel) {
      should_add = false;
    } else if (RIGHT_DOMINATED == plan_rel) {
      if (OB_FAIL(current_plans.remove(i))) {
        LOG_WARN("failed to remove dominated plans", K(i), K(ret));
      } else { /* do nothing*/
      }
    } else { /* do nothing */
    }
  }
  if (OB_SUCC(ret)) {
    if (should_add) {
      if (OB_FAIL(current_plans.push_back(new_plan))) {
        LOG_WARN("failed to add plan", K(ret));
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogPlan::compute_plan_relationship(
    const CandidatePlan& first_plan, const CandidatePlan& second_plan, PlanRelationship& plan_rel)
{
  int ret = OB_SUCCESS;
  plan_rel = UNCOMPARABLE;
  const ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt) || OB_ISNULL(first_plan.plan_tree_) || OB_ISNULL(second_plan.plan_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(first_plan.plan_tree_), K(second_plan.plan_tree_), K(ret));
  } else {
    int64_t left_dominated_count = 0;
    int64_t right_dominated_count = 0;
    int64_t uncompareable_count = 0;
    // compare cost
    if (fabs(first_plan.plan_tree_->get_cost() - second_plan.plan_tree_->get_cost()) < OB_DOUBLE_EPSINON) {
      // do nothing
    } else if (first_plan.plan_tree_->get_cost() < second_plan.plan_tree_->get_cost()) {
      left_dominated_count++;
    } else {
      right_dominated_count++;
    }
    // compare interesting
    bool left_prefix = false;
    bool right_prefix = false;
    const EqualSets& equal_sets = first_plan.plan_tree_->get_ordering_output_equal_sets();
    bool first_has_interesting_order = first_plan.plan_tree_->has_any_interesting_order_info_flag();
    bool second_has_interesting_order = second_plan.plan_tree_->has_any_interesting_order_info_flag();
    if (!first_has_interesting_order && !second_has_interesting_order) {
      // do nothing
    } else if (first_has_interesting_order && !second_has_interesting_order) {
      left_dominated_count++;
    } else if (!first_has_interesting_order && second_has_interesting_order) {
      right_dominated_count++;
    } else {
      if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(first_plan.plan_tree_->get_op_ordering(),
              second_plan.plan_tree_->get_op_ordering(),
              equal_sets,
              get_const_exprs(),
              left_prefix))) {
        LOG_WARN("failed to compute prefix ordering relationship",
            K(ret),
            K(first_plan.plan_tree_->get_op_ordering()),
            K(second_plan.plan_tree_->get_op_ordering()));
      } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(second_plan.plan_tree_->get_op_ordering(),
                     first_plan.plan_tree_->get_op_ordering(),
                     equal_sets,
                     get_const_exprs(),
                     right_prefix))) {
        LOG_WARN("failed to compute prefix ordering relationship",
            K(ret),
            K(first_plan.plan_tree_->get_op_ordering()),
            K(second_plan.plan_tree_->get_op_ordering()));
      } else if (left_prefix && right_prefix) {
        // do nothing
      } else if (left_prefix) {
        right_dominated_count++;
      } else if (right_prefix) {
        left_dominated_count++;
      } else {
        uncompareable_count++;
      }
    }

    bool is_first_pipeline = false;
    bool is_second_pipeline = false;
    // compare pipeline operator
    if (OB_SUCC(ret) && stmt->has_limit()) {
      if (OB_FAIL(is_pipeline_operator(first_plan, is_first_pipeline))) {
        LOG_WARN("failed to examine pipeline operator", K(first_plan), K(ret));
      } else if (OB_FAIL(is_pipeline_operator(second_plan, is_second_pipeline))) {
        LOG_WARN("failed to examine pipeline operator", K(second_plan), K(ret));
      } else if (is_first_pipeline == is_second_pipeline) {
        // do nothing
      } else if (is_first_pipeline && !is_second_pipeline) {
        left_dominated_count++;
      } else {
        right_dominated_count++;
      }
    }
    // compute final result
    if (OB_SUCC(ret)) {
      ObLogicalOperator* first = first_plan.plan_tree_;
      ObLogicalOperator* second = second_plan.plan_tree_;
      if (left_dominated_count > 0 && right_dominated_count == 0 && uncompareable_count == 0) {
        plan_rel = LEFT_DOMINATED;
        LOG_TRACE("first dominated second",
            K(first->get_cost()),
            K(first->get_op_ordering()),
            K(is_first_pipeline),
            K(second->get_cost()),
            K(second->get_op_ordering()),
            K(is_second_pipeline));
      } else if (right_dominated_count > 0 && left_dominated_count == 0 && uncompareable_count == 0) {
        plan_rel = RIGHT_DOMINATED;
        LOG_TRACE("second dominated first",
            K(second->get_cost()),
            K(second->get_op_ordering()),
            K(is_second_pipeline),
            K(first->get_cost()),
            K(first->get_op_ordering()),
            K(is_first_pipeline));
      } else if (left_dominated_count == 0 && right_dominated_count == 0 && uncompareable_count == 0) {
        plan_rel = EQUAL;
      } else {
        plan_rel = UNCOMPARABLE;
      }
    }
  }
  return ret;
}

int ObLogPlan::is_pipeline_operator(const CandidatePlan& first_plan, bool& is_pipeline)
{
  int ret = OB_SUCCESS;
  is_pipeline = false;
  // todo set op ?
  if (OB_ISNULL(first_plan.plan_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null operator", K(ret));
  } else if (first_plan.plan_tree_->get_type() == LOG_GROUP_BY) {
    ObLogGroupBy* group_by = static_cast<ObLogGroupBy*>(first_plan.plan_tree_);
    ObLogicalOperator* child = group_by->get_child(0);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child operator", K(ret));
    } else if (MERGE_AGGREGATE == group_by->get_algo() && LOG_SORT != child->get_type()) {
      is_pipeline = true;
    } else {
      is_pipeline = false;
    }
  } else if (first_plan.plan_tree_->get_type() == LOG_DISTINCT) {
    ObLogDistinct* distinct = static_cast<ObLogDistinct*>(first_plan.plan_tree_);
    ObLogicalOperator* child = distinct->get_child(0);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child operator", K(ret));
    } else if (MERGE_AGGREGATE == distinct->get_algo() && LOG_SORT == child->get_type()) {
      is_pipeline = false;
    } else {
      is_pipeline = true;
    }
  } else if (first_plan.plan_tree_->get_type() == LOG_WINDOW_FUNCTION) {
    ObLogicalOperator* child = first_plan.plan_tree_->get_child(0);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(child));
    } else if (LOG_SORT == child->get_type()) {
      is_pipeline = false;
    } else {
      is_pipeline = true;
    }
  } else if (first_plan.plan_tree_->get_type() == LOG_SORT || first_plan.plan_tree_->get_type() == LOG_MATERIAL) {
    is_pipeline = false;
  } else {
    is_pipeline = true;
  }
  return ret;
}

int ObLogPlan::calc_plan_resource()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = nullptr;
  ObLogicalOperator* plan_root = nullptr;
  int64_t max_parallel_thread_group_count = 0;
  if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(plan_root = get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObPxResourceAnalyzer analyzer;
    if (OB_FAIL(analyzer.analyze(*plan_root, max_parallel_thread_group_count))) {
      LOG_WARN("fail analyze px stmt thread group reservation count", K(ret));
    } else {
      LOG_INFO("max parallel thread group count", K(max_parallel_thread_group_count));
      set_expected_worker_count(max_parallel_thread_group_count);
    }
  }
  return ret;
}

// calculate whether all partitions in same server
int ObLogPlan::is_partition_in_same_server(const ObIArray<const ObPhyTableLocationInfo*>& phy_location_infos,
    bool& is_same, bool& multi_part_table, ObAddr& first_addr)
{
  int ret = OB_SUCCESS;
  int64_t phy_location_count = phy_location_infos.count();
  if (phy_location_count > 0) {
    bool is_first = true;
    ObReplicaLocation replica_location;
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < phy_location_count; ++i) {
      int64_t partition_location_count = phy_location_infos.at(i)->get_partition_cnt();
      if (partition_location_count >= 2 && !multi_part_table) {
        multi_part_table = true;
      }
      if (partition_location_count > 0) {
        for (int64_t j = 0; OB_SUCC(ret) && is_same && j < partition_location_count; ++j) {
          replica_location.reset();
          const ObPhyPartitionLocationInfo& phy_part_loc_info =
              phy_location_infos.at(i)->get_phy_part_loc_info_list().at(j);
          if (OB_FAIL(phy_part_loc_info.get_selected_replica(replica_location))) {
            LOG_WARN("fail to get selected replica", K(ret), K(phy_part_loc_info));
          } else if (!replica_location.is_valid()) {
            LOG_WARN("replica_location is invalid", K(ret), K(replica_location));
          } else {
            if (is_first) {
              // get first replica
              first_addr = replica_location.server_;
              is_same = true;
              is_first = false;
              LOG_TRACE("part_location first replica", K(ret), K(replica_location));
            } else {
              is_same = (replica_location.server_ == first_addr);
              LOG_TRACE("part_location replica", K(ret), K(i), K(replica_location));
            }
          }
        }
      } else {
        LOG_DEBUG("there is no partition_locattion in this phy_location", KP(phy_location_infos.at(i)));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_locations is empty");
  }

  return ret;
}

int ObLogPlan::collect_partition_location_info(
    ObLogicalOperator* root, ObIArray<const ObPhyTableLocationInfo*>& phy_tbl_loc_info_list)
{
  int ret = OB_SUCCESS;
  int64_t child_count = root->get_num_of_child();

  if (child_count != 0) {
    for (int64_t i = 0; i < child_count && OB_SUCC(ret); ++i) {
      if (OB_FAIL(collect_partition_location_info(root->get_child(i), phy_tbl_loc_info_list))) {
        LOG_WARN("fail to get use px", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && LOG_TABLE_SCAN == root->get_type()) {
    ObLogTableScan* tsc = static_cast<ObLogTableScan*>(root);
    const ObTablePartitionInfo* tbl_part_info = tsc->get_table_partition_info();
    if (OB_FAIL(phy_tbl_loc_info_list.push_back(&tbl_part_info->get_phy_tbl_location_info()))) {
      LOG_WARN("fail to push back phy tble loc info", K(ret), K(tbl_part_info->get_phy_tbl_location_info()));
    }
  }
  if (OB_SUCC(ret) && root->is_dml_operator()) {
    ObLogDelUpd* dml = static_cast<ObLogDelUpd*>(root);
    const ObTablePartitionInfo* tbl_part_info = &dml->get_table_partition_info();
    if (OB_FAIL(phy_tbl_loc_info_list.push_back(&tbl_part_info->get_phy_tbl_location_info()))) {
      LOG_WARN("fail to push back phy tble loc info", K(ret), K(tbl_part_info->get_phy_tbl_location_info()));
    }
  }
  return ret;
}

int ObLogPlan::get_max_table_dop_for_plan(ObLogicalOperator* root, bool& disable_table_dop, int64_t& max_table_dop)
{
  int ret = OB_SUCCESS;
  if (disable_table_dop) {
    max_table_dop = 1;
  } else if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log op is null", K(ret));
  } else {
    log_op_def::ObLogOpType op_type = root->get_type();
    if (op_type == log_op_def::LOG_TABLE_SCAN || op_type == log_op_def::LOG_MV_TABLE_SCAN ||
        op_type == log_op_def::LOG_TABLE_LOOKUP || op_type == log_op_def::LOG_INSERT ||
        op_type == log_op_def::LOG_MERGE) {
      uint64_t index_id = OB_INVALID_ID;
      if (op_type == log_op_def::LOG_TABLE_LOOKUP) {
        // LOOK UP
        ObLogTableLookup* look_up = static_cast<ObLogTableLookup*>(root);
        index_id = look_up->get_index_id();
      } else if (op_type == log_op_def::LOG_TABLE_SCAN || op_type == log_op_def::LOG_MV_TABLE_SCAN) {
        // SCAN
        ObLogTableScan* table_scan = static_cast<ObLogTableScan*>(root);
        index_id = table_scan->get_index_table_id();
      } else {
        // INSERT, MERGE
        ObLogDelUpd* dml = static_cast<ObLogDelUpd*>(root);
        index_id = dml->get_index_tid();
      }
      ObSchemaGetterGuard* schema_guard = NULL;
      const ObTableSchema* table_schema = NULL;
      if (index_id == OB_INVALID_ID) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the index id is invalid", K(ret));
      } else if (OB_ISNULL(schema_guard = get_optimizer_context().get_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema guard is null", K(ret));
      } else if (is_link_table_id(index_id)) {
        max_table_dop = 1;
        disable_table_dop = true;
        LOG_TRACE("due to the existence of link table, table dop is disable", K(ret), K(root->get_op_id()));
      } else if (is_cte_table(index_id)) {
        max_table_dop = 1;
        disable_table_dop = true;
        LOG_TRACE("due to the existence of cte table, table dop is disable", K(ret), K(root->get_op_id()));
      } else if (OB_FAIL(schema_guard->get_table_schema(index_id, table_schema))) {
        LOG_WARN("failed get table schema", K(ret), K(index_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the table schema is null", K(ret), K(index_id));
      } else if (table_schema->get_dop() > max_table_dop) {
        max_table_dop = table_schema->get_dop();
      }
    }

    for (int i = 0; i < root->get_num_of_child() && OB_SUCC(ret) && !disable_table_dop; i++) {
      ObLogicalOperator* child = root->get_child(i);
      if (OB_FAIL(get_max_table_dop_for_plan(child, disable_table_dop, max_table_dop))) {
        LOG_WARN("failed to get max table dop", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::init_plan_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  } else if (OB_FAIL(get_stmt()->get_stmt_equal_sets(equal_sets_, allocator_, false, EQUAL_SET_SCOPE::SCOPE_WHERE))) {
    LOG_WARN("failed to get stmt equal sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(get_stmt()->get_condition_exprs(), get_const_exprs()))) {
    LOG_WARN("failed to compute const equivalent exprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

EqualSets* ObLogPlan::create_equal_sets()
{
  EqualSets* esets = NULL;
  void* ptr = NULL;
  if (OB_LIKELY(NULL != (ptr = get_allocator().alloc(sizeof(EqualSets))))) {
    esets = new (ptr) EqualSets();
  }
  return esets;
}

ObEstSelInfo* ObLogPlan::create_est_sel_info(ObLogicalOperator* op)
{
  void* ptr = NULL;
  ObEstSelInfo* est_sel_info = NULL;
  if (OB_LIKELY(NULL != (ptr = get_allocator().alloc(sizeof(ObEstSelInfo))))) {
    est_sel_info = new (ptr) ObEstSelInfo(get_optimizer_context(), get_stmt(), op);
  }
  return est_sel_info;
}

ObJoinOrder* ObLogPlan::create_join_order(PathType type)
{
  void* ptr = NULL;
  ObJoinOrder* join_order = NULL;
  if (OB_LIKELY(NULL != (ptr = get_allocator().alloc(sizeof(ObJoinOrder))))) {
    join_order = new (ptr) ObJoinOrder(&get_allocator(), this, type);
  }
  return join_order;
}

int ObLogPlan::check_enable_plan_expiration(bool& enable) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = NULL;
  bool use_acs = false;
  bool use_spm = false;
  enable = false;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(session = optimizer_context_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!get_stmt()->is_select_stmt()) {
    // do nothing
  } else if (get_phy_plan_type() != OB_PHY_PLAN_LOCAL && get_phy_plan_type() != OB_PHY_PLAN_REMOTE) {
    // do nothing
  } else if (OB_FAIL(session->get_adaptive_cursor_sharing(use_acs))) {
    LOG_WARN("failed to check is acs enabled", K(ret));
  } else if (use_acs) {
    // do nothing
  } else if (OB_FAIL(session->get_use_plan_baseline(use_spm))) {
    LOG_WARN("failed to check is spm enabled", K(ret));
  } else if (use_spm) {
    // do nothing
  } else {
    const ObLogicalOperator* node = root_;
    while (OB_SUCC(ret)) {
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", K(ret));
      } else if (node->get_num_of_child() == 1) {
        node = node->get_child(ObLogicalOperator::first_child);
      } else {
        break;
      }
    }
    if (OB_SUCC(ret) && node->is_table_scan()) {
      enable = (static_cast<const ObLogTableScan*>(node)->get_diverse_path_count() >= 2);
    }
  }
  return ret;
}

bool ObLogPlan::need_consistent_read() const
{
  bool bret = true;
  if (OB_NOT_NULL(root_) && OB_NOT_NULL(get_stmt()) && OB_NOT_NULL(get_stmt()->get_query_ctx())) {
    if (stmt::T_INSERT == get_stmt()->get_stmt_type()) {
      const ObInsertStmt* insert_stmt = static_cast<const ObInsertStmt*>(get_stmt());
      if (!insert_stmt->is_replace() && !insert_stmt->get_insert_up()) {
        uint64_t insert_table_id = insert_stmt->get_insert_table_id();
        const ObQueryCtx* query_ctx = get_stmt()->get_query_ctx();
        bool found_other_table = false;
        for (int64_t i = 0; !found_other_table && i < query_ctx->table_partition_infos_.count(); ++i) {
          ObTablePartitionInfo* part_info = query_ctx->table_partition_infos_.at(i);
          if (OB_NOT_NULL(part_info) && part_info->get_table_id() != insert_table_id) {
            found_other_table = true;
          }
        }
        bret = found_other_table;
      }
    }
  }
  return bret;
}

int ObLogPlan::check_fullfill_safe_update_mode(ObLogicalOperator* op)
{
  int ret = OB_SUCCESS;
  bool is_sql_safe_updates = false;
  bool is_not_fullfill = false;
  const ObSQLSessionInfo* session_info = NULL;
  if (OB_ISNULL(session_info = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_optimizer_context().get_session_info()));
  } else if (OB_FAIL(session_info->get_sql_safe_updates(is_sql_safe_updates))) {
    LOG_WARN("failed to get is safe update mode", K(ret));
  } else if (!is_sql_safe_updates) {
    /*do nothing*/
  } else if (OB_FAIL(do_check_fullfill_safe_update_mode(op, is_not_fullfill))) {
    LOG_WARN("failed to check fullfill safe update mode", K(ret));
  } else if (is_not_fullfill) {
    ret = OB_ERR_SAFE_UPDATE_MODE_NEED_WHERE_OR_LIMIT;
    LOG_WARN("using safe update mode need WHERE or LIMIT", K(ret));
  }
  return ret;
}

int ObLogPlan::do_check_fullfill_safe_update_mode(ObLogicalOperator* op, bool& is_not_fullfill)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(op), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (is_not_fullfill) {
    /*do nothing*/
  } else if (op->get_type() == log_op_def::LOG_TABLE_SCAN) {
    ObLogTableScan* table_scan = static_cast<ObLogTableScan*>(op);
    is_not_fullfill |= table_scan->is_whole_range_scan();
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_not_fullfill && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(do_check_fullfill_safe_update_mode(op->get_child(i), is_not_fullfill))) {
        LOG_WARN("failed to check fullfill safe update mode", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::calc_and_set_exec_pwj_map(ObLocationConstraintContext& location_constraint) const
{
  int ret = OB_SUCCESS;
  ObExecContext* exec_ctx = get_optimizer_context().get_exec_ctx();
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (location_constraint.strict_constraints_.count() > 0 ||
             location_constraint.non_strict_constraints_.count() > 0) {
    ObIArray<LocationConstraint>& base_location_cons = location_constraint.base_table_constraints_;
    ObIArray<ObPwjConstraint*>& strict_cons = location_constraint.strict_constraints_;
    ObIArray<ObPwjConstraint*>& non_strict_cons = location_constraint.non_strict_constraints_;
    const int64_t tbl_count = location_constraint.base_table_constraints_.count();
    ObSEArray<PwjTable, 8> pwj_tables;
    ObPwjComparer strict_pwj_comparer(true);
    ObPwjComparer non_strict_pwj_comparer(false);
    PWJPartitionIdMap pwj_map;
    if (OB_FAIL(pwj_tables.prepare_allocate(tbl_count))) {
      LOG_WARN("failed to prepare allocate pwj tables", K(ret));
    } else if (OB_FAIL(pwj_map.create(8, ObModIds::OB_PLAN_EXECUTE))) {
      LOG_WARN("create pwj map failed", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tbl_count; ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < strict_cons.count(); ++j) {
        const ObPwjConstraint* pwj_cons = strict_cons.at(j);
        if (OB_ISNULL(pwj_cons) || OB_UNLIKELY(pwj_cons->count() <= 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected pwj constraint", K(ret), K(pwj_cons));
        } else if (pwj_cons->at(0) == i) {
          if (OB_FAIL(check_pwj_cons(
                  *pwj_cons, location_constraint.base_table_constraints_, pwj_tables, strict_pwj_comparer, pwj_map))) {
            LOG_WARN("failed to check pwj cons", K(ret));
          }
        }
      }

      for (int64_t j = 0; OB_SUCC(ret) && j < non_strict_cons.count(); ++j) {
        const ObPwjConstraint* pwj_cons = non_strict_cons.at(j);
        if (OB_ISNULL(pwj_cons) || OB_UNLIKELY(pwj_cons->count() <= 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected pwj constraint", K(ret), K(pwj_cons));
        } else if (pwj_cons->at(0) == i) {
          if (OB_FAIL(check_pwj_cons(*pwj_cons,
                  location_constraint.base_table_constraints_,
                  pwj_tables,
                  non_strict_pwj_comparer,
                  pwj_map))) {
            LOG_WARN("failed to check pwj cons", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      PWJPartitionIdMap* exec_pwj_map = NULL;
      if (OB_FAIL(exec_ctx->get_pwj_map(exec_pwj_map))) {
        LOG_WARN("failed to get exec pwj map", K(ret));
      } else if (OB_FAIL(exec_pwj_map->reuse())) {
        LOG_WARN("failed to reuse pwj map", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < base_location_cons.count(); ++i) {
        if (!base_location_cons.at(i).is_multi_part_insert()) {
          PartitionIdArray partition_id_array;
          if (OB_FAIL(pwj_map.get_refactored(i, partition_id_array))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to get refactored", K(ret));
            }
          } else if (OB_FAIL(
                         exec_pwj_map->set_refactored(base_location_cons.at(i).key_.table_id_, partition_id_array))) {
            LOG_WARN("failed to set refactored", K(ret));
          }
        }
      }
    }

    if (pwj_map.created()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = pwj_map.destroy()))) {
        LOG_WARN("failed to destroy pwj map", K(tmp_ret));
      }
    }
  }
  return ret;
}

int64_t ObLogPlan::check_pwj_cons(const ObPwjConstraint& pwj_cons,
    const ObIArray<LocationConstraint>& base_location_cons, ObIArray<PwjTable>& pwj_tables, ObPwjComparer& pwj_comparer,
    PWJPartitionIdMap& pwj_map) const
{
  int ret = OB_SUCCESS;
  bool is_same = true;
  ObShardingInfo* first_sharding = base_location_cons.at(pwj_cons.at(0)).sharding_info_;
  if (OB_ISNULL(first_sharding)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(first_sharding));
  } else {
    if (first_sharding->is_local() || first_sharding->is_remote()) {
      // all tables in pwj constraint are local or remote
      // alreay checked, do nothing
    } else {
      // distribute partition wise join
      LOG_DEBUG("check pwj constraint", K(pwj_cons), K(base_location_cons), K(pwj_tables));
      pwj_comparer.reset();
      for (int64_t i = 0; OB_SUCC(ret) && is_same && i < pwj_cons.count(); ++i) {
        const int64_t table_idx = pwj_cons.at(i);
        PwjTable& table = pwj_tables.at(table_idx);
        bool need_set_refactored = false;
        if (OB_INVALID_ID == table.ref_table_id_) {
          // pwj table no init
          need_set_refactored = true;
          ObShardingInfo* cur_sharding = base_location_cons.at(table_idx).sharding_info_;
          if (OB_ISNULL(cur_sharding)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(table.init(*cur_sharding))) {
            LOG_WARN("failed to init owj table with sharding info", K(ret));
          } else {
            table.ref_table_id_ = base_location_cons.at(table_idx).key_.ref_table_id_;
          }
        } else {
          // pwj table already init, should find partition id array in pwj map
          PartitionIdArray partition_id_array;
          if (OB_FAIL(pwj_map.get_refactored(table_idx, partition_id_array))) {
            if (OB_HASH_NOT_EXIST == ret) {
              LOG_WARN("get refactored not find partition id array", K(ret));
            } else {
              LOG_WARN("failed to get refactored", K(ret));
            }
          } else if (OB_FAIL(table.ordered_partition_ids_.assign(partition_id_array))) {
            LOG_WARN("failed to assign partition id array", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(pwj_comparer.add_table(table, is_same))) {
            LOG_WARN("failed to add table", K(ret));
          } else if (!is_same) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get not same table", K(*first_sharding), K(table));
          } else if (need_set_refactored &&
                     OB_FAIL(pwj_map.set_refactored(table_idx, pwj_comparer.get_partition_id_group().at(i)))) {
            LOG_WARN("failed to set refactored", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
