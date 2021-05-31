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
#include <algorithm>
#include "sql/optimizer/ob_logical_operator.h"
#include "lib/hash_func/murmur_hash.h"
#include "sql/resolver/expr/ob_raw_expr_replacer.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "ob_log_exchange.h"
#include "ob_log_group_by.h"
#include "ob_log_distinct.h"
#include "ob_log_insert.h"
#include "ob_log_join.h"
#include "ob_log_set.h"
#include "ob_log_sort.h"
#include "ob_log_subplan_scan.h"
#include "ob_log_table_scan.h"
#include "ob_log_limit.h"
#include "ob_log_window_function.h"
#include "ob_log_granule_iterator.h"
#include "ob_log_table_lookup.h"
#include "ob_log_update.h"
#include "ob_opt_est_cost.h"
#include "ob_optimizer_util.h"
#include "ob_raw_expr_add_to_context.h"
#include "ob_raw_expr_check_dep.h"
#include "ob_log_count.h"
#include "ob_log_monitoring_dump.h"
#include "ob_log_subplan_filter.h"
#include "ob_log_topk.h"
#include "ob_log_material.h"
#include "ob_log_temp_table_access.h"
#include "ob_log_temp_table_insert.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "common/ob_smart_call.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"

using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::sql::log_op_def;
using oceanbase::share::schema::ObColumnSchemaV2;
using oceanbase::share::schema::ObSchemaGetterGuard;
using oceanbase::share::schema::ObTableSchema;

void AllocExchContext::add_exchange_type(AllocExchContext::DistrStat status)
{
  if (REMOTE == status) {
    // When add REMOTE exchange node, the plan type change to REMOTE
    // only with UNITIALIZED plan type. Other plan type means there other branch.
    plan_type_ = ((plan_type_ != UNINITIALIZED) ? DISTRIBUTED : REMOTE);
  } else if (DISTRIBUTED == status) {
    plan_type_ = DISTRIBUTED;
  } else { /* Do nothing */
  }
}

void ObExchangeInfo::reset()
{
  is_task_order_ = false;
  slice_count_ = 1;
  repartition_type_ = OB_REPARTITION_NO_REPARTITION;
  repartition_ref_table_id_ = OB_INVALID_ID;
  repartition_table_name_.reset();
  calc_part_id_expr_ = NULL;
  repartition_keys_.reset();
  repartition_sub_keys_.reset();
  repartition_func_exprs_.reset();
  keep_ordering_ = false;
  hash_dist_exprs_.reset();
  dist_method_ = ObPQDistributeMethod::MAX_VALUE;
  unmatch_row_dist_method_ = ObPQDistributeMethod::MAX_VALUE;
  px_dop_ = 0;
  px_single_ = false;
  pdml_pkey_ = false;
  slave_mapping_type_ = SlaveMappingType::SM_NONE;
}

int ObExchangeInfo::clone(ObExchangeInfo& exch_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(repartition_keys_.assign(exch_info.repartition_keys_))) {
    LOG_WARN("failed to assign repartition keys", K(ret));
  } else if (OB_FAIL(repartition_sub_keys_.assign(exch_info.repartition_sub_keys_))) {
    LOG_WARN("failed to assign repartition sub keys", K(ret));
  } else if (OB_FAIL(repartition_func_exprs_.assign(exch_info.repartition_func_exprs_))) {
    LOG_WARN("failed to assign part func exprs", K(ret));
  } else if (OB_FAIL(hash_dist_exprs_.assign(exch_info.hash_dist_exprs_))) {
    LOG_WARN("array assign failed", K(ret));
  } else {
    is_task_order_ = exch_info.is_task_order_;
    slice_count_ = exch_info.slice_count_;
    repartition_type_ = exch_info.repartition_type_;
    repartition_ref_table_id_ = exch_info.repartition_ref_table_id_;
    repartition_table_name_ = exch_info.repartition_table_name_;
    calc_part_id_expr_ = exch_info.calc_part_id_expr_;
    keep_ordering_ = exch_info.keep_ordering_;
    dist_method_ = exch_info.dist_method_;
    unmatch_row_dist_method_ = exch_info.unmatch_row_dist_method_;
    px_dop_ = exch_info.px_dop_;
    px_single_ = exch_info.px_single_;
    slave_mapping_type_ = exch_info.slave_mapping_type_;
  }
  return ret;
}

int ObExchangeInfo::set_repartition_info(const ObIArray<ObRawExpr*>& repart_keys,
    const ObIArray<ObRawExpr*>& repart_sub_keys, const ObIArray<ObRawExpr*>& repart_func_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(repartition_keys_.assign(repart_keys))) {
    LOG_WARN("failed to assign keys", K(ret));
  } else if (OB_FAIL(repartition_sub_keys_.assign(repart_sub_keys))) {
    LOG_WARN("failed to set sub repart keys", K(ret));
  } else if (OB_FAIL(repartition_func_exprs_.assign(repart_func_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  return ret;
}

int ObExchangeInfo::init_calc_part_id_expr(ObOptimizerContext& opt_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = NULL;
  ObRawExprFactory& expr_factory = opt_ctx.get_expr_factory();
  int64_t part_expr_cnt = repartition_func_exprs_.count();
  share::schema::ObPartitionLevel part_level = share::schema::PARTITION_LEVEL_ONE;
  ObRawExpr* part_expr = NULL;
  ObRawExpr* subpart_expr = NULL;
  if (part_expr_cnt <= 0 || part_expr_cnt > 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part expr count", K(ret));
  } else {
    if (OB_ISNULL(repartition_func_exprs_.at(0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part func expr is null", K(ret));
    } else if (T_OP_ROW == repartition_func_exprs_.at(0)->get_expr_type()) {
      ObOpRawExpr* op_row_expr = NULL;
      if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, op_row_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < repartition_keys_.count(); i++) {
          if (OB_FAIL(op_row_expr->add_param_expr(repartition_keys_.at(i)))) {
            LOG_WARN("fail to add param expr", K(ret));
          }
        }  // for end
        part_expr = op_row_expr;
      }
    } else {
      part_expr = repartition_func_exprs_.at(0);
    }
  }
  if (OB_SUCC(ret) && part_expr_cnt == 2) {
    part_level = share::schema::PARTITION_LEVEL_TWO;
    if (OB_ISNULL(repartition_func_exprs_.at(1))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part func expr is null", K(ret));
    } else if (T_OP_ROW == repartition_func_exprs_.at(1)->get_expr_type()) {
      ObOpRawExpr* op_row_expr = NULL;
      if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, op_row_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < repartition_sub_keys_.count(); i++) {
          if (OB_FAIL(op_row_expr->add_param_expr(repartition_sub_keys_.at(i)))) {
            LOG_WARN("fail to add param expr", K(ret));
          }
        }  // for end
        subpart_expr = op_row_expr;
      }
    } else {
      subpart_expr = repartition_func_exprs_.at(1);
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(session = opt_ctx.get_session_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_calc_part_id_expr(expr_factory,
                 *session,
                 repartition_ref_table_id_,
                 part_level,
                 part_expr,
                 subpart_expr,
                 calc_part_id_expr_))) {
    LOG_WARN("fail to init calc part id expr", K(ret));
  } else if (OB_ISNULL(calc_part_id_expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init calc part id expr", K(ret));
  }

  return ret;
}

int ObExchangeInfo::append_hash_dist_expr(const common::ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(expr, exprs, OB_SUCC(ret))
  {
    if (OB_ISNULL(*expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL expr", K(ret));
    } else if (OB_FAIL(hash_dist_exprs_.push_back(HashExpr(*expr, (*expr)->get_result_type())))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  return ret;
}

ObPxPipeBlockingCtx::ObPxPipeBlockingCtx(ObIAllocator& alloc) : alloc_(alloc)
{}

ObPxPipeBlockingCtx::~ObPxPipeBlockingCtx()
{
  FOREACH(it, op_ctxs_)
  {
    if (NULL != *it) {
      alloc_.free(*it);
      *it = NULL;
    }
  }
}

ObPxPipeBlockingCtx::OpCtx* ObPxPipeBlockingCtx::alloc()
{
  OpCtx* ctx = NULL;
  void* mem = alloc_.alloc(sizeof(OpCtx));
  if (OB_ISNULL(mem)) {
    LOG_WARN("allocate memory failed");
  } else {
    ctx = new (mem) OpCtx();
  }
  return ctx;
}

uint64_t ObExchangeInfo::hash(uint64_t seed) const
{
  seed = do_hash(is_task_order_, seed);
  seed = do_hash(slice_count_, seed);
  seed = do_hash(repartition_ref_table_id_, seed);
  seed = ObOptimizerUtil::hash_exprs(seed, repartition_keys_);
  seed = ObOptimizerUtil::hash_exprs(seed, repartition_sub_keys_);
  seed = ObOptimizerUtil::hash_exprs(seed, repartition_func_exprs_);
  seed = do_hash(keep_ordering_, seed);
  return seed;
}

int ObAllocExprContext::find(const ObRawExpr* expr, ExprProducer*& producer)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  producer = NULL;
  if (expr_producers_.empty()) {
    // do nothing
  } else if (OB_UNLIKELY(expr_producers_.count() != expr_map_.size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash map has invalid size", K(ret));
  } else if (OB_FAIL(expr_map_.get_refactored(reinterpret_cast<uint64_t>(expr), idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get expr entry from the map", K(ret));
    }
  } else if (idx < 0 || idx >= expr_producers_.count() || OB_UNLIKELY(expr != expr_producers_.at(idx).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index is invalid", K(ret), K(idx));
  } else {
    producer = &expr_producers_.at(idx);
  }
  return ret;
}

int ObAllocExprContext::add(const ExprProducer& producer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_producers_.push_back(producer))) {
    LOG_WARN("failed to push back producer", K(ret));
  } else if (expr_producers_.count() == 1 && expr_map_.create(128, "ExprAlloc")) {
    LOG_WARN("failed to init hash map", K(ret));
  } else if (OB_FAIL(
                 expr_map_.set_refactored(reinterpret_cast<uint64_t>(producer.expr_), expr_producers_.count() - 1))) {
    LOG_WARN("failed to add entry into hash map", K(ret));
  }
  return ret;
}

ObAllocExprContext::~ObAllocExprContext()
{
  expr_map_.destroy();
}

ObLogicalOperator::ObLogicalOperator(ObLogPlan& plan)
    : child_(),
      type_(LOG_OP_INVALID),
      my_plan_(&plan),
      startup_exprs_(),
      const_exprs_(),
      ordering_output_equal_sets_(NULL),
      sharding_output_equal_sets_(NULL),
      fd_item_set_(NULL),
      table_set_(NULL),
      est_sel_info_(NULL),
      stmt_(plan.get_stmt()),
      id_(OB_INVALID_ID),
      branch_id_(OB_INVALID_ID),
      op_id_(OB_INVALID_ID),
      child_id_(0),
      parent_(NULL),
      is_plan_root_(false),
      cost_(0.0),
      op_cost_(0.0),
      card_(0.0),
      width_(0.0),
      sharding_info_(),
      is_added_outline_(false),
      is_added_leading_outline_(false),
      is_added_leading_hints_(false),
      expected_ordering_(),
      traverse_ctx_(NULL),
      is_partition_wise_(false),
      is_block_gi_allowed_(false),
      px_est_size_factor_(),
      dblink_id_(0),  // 0 represent local cluster.
      plan_depth_(0),
      is_at_most_one_row_(false),
      op_ordering_(),
      op_local_ordering_(),
      empty_expr_sets_(plan.get_empty_expr_sets()),
      empty_fd_item_set_(plan.get_empty_fd_item_set()),
      empty_table_set_(plan.get_empty_table_set()),
      num_of_parent_(0),
      interesting_order_info_(OrderingFlag::NOT_MATCH)
{}

ObLogicalOperator::~ObLogicalOperator()
{}

int ObLogicalOperator::clone(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  out = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(ret));
  } else if (OB_ISNULL(op = get_plan()->get_log_op_factory().allocate(*get_plan(), type_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(append(op->filter_exprs_, filter_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(op->pushdown_filter_exprs_, pushdown_filter_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(op->startup_exprs_, startup_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(op->const_exprs_, const_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(op->op_ordering_, op_ordering_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(op->op_local_ordering_, op_local_ordering_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(op->expected_ordering_, expected_ordering_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(op->output_exprs_, output_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else {
    op->is_at_most_one_row_ = is_at_most_one_row_;
    op->ordering_output_equal_sets_ = ordering_output_equal_sets_;
    op->sharding_output_equal_sets_ = sharding_output_equal_sets_;
    op->fd_item_set_ = fd_item_set_;
    op->table_set_ = table_set_;
    op->est_sel_info_ = est_sel_info_;
    op->my_plan_ = my_plan_;
    op->stmt_ = stmt_;
    op->is_plan_root_ = is_plan_root_;
    op->cost_ = cost_;
    op->op_cost_ = op_cost_;
    op->card_ = card_;
    op->width_ = width_;
    op->is_partition_wise_ = is_partition_wise_;
    op->is_block_gi_allowed_ = is_block_gi_allowed_;
    op->px_est_size_factor_ = px_est_size_factor_;
    op->plan_depth_ = plan_depth_;
    out = op;
  }
  return ret;
}

// Set a child operator for certain position
// Note: 'num_of_children_' is also changed accordingly.
void ObLogicalOperator::set_child(int64_t child_num, ObLogicalOperator* child_op)
{
  int ret = OB_SUCCESS;
  for (; OB_SUCC(ret) && child_.count() <= child_num;) {
    if (OB_FAIL(child_.push_back(NULL))) {
      LOG_WARN("failed to enlarge child array", K(ret));
    }
  }
  if (OB_SUCC(ret) && child_.count() > child_num) {
    child_.at(child_num) = child_op;
    child_op->set_child_id(child_num);
  }
}

int ObLogicalOperator::get_parent(ObLogicalOperator* root, ObLogicalOperator*& parent)
{
  int ret = OB_SUCCESS;
  parent = NULL;
  if (NULL != root) {
    for (int64_t i = 0; OB_SUCC(ret) && parent == NULL && i < root->get_num_of_child(); i++) {
      if (OB_ISNULL(root->get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (root->get_child(i) == this) {
        parent = root;
      } else if (OB_FAIL(get_parent(root->get_child(i), parent))) {
        LOG_WARN("failed to get parent", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogicalOperator::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_exprs);
  return ret;
}

int ObLogicalOperator::append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is null", K(ret));
  } else if (!session->use_static_typing_engine()) {
    // do nothing
  } else {
    int64_t pos = raw_exprs.count();
    OZ(raw_exprs.append(startup_exprs_));
    OZ(inner_append_not_produced_exprs(raw_exprs));
    for (; OB_SUCC(ret) && pos < raw_exprs.count(); pos++) {
      ObRawExpr* e = raw_exprs.get_expr_array().at(pos);
      // call get_param_count() to make sure raw expr is constructed.
      if (NULL != e && e->get_param_count() < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL expr or invalid param count", K(ret), K(e->get_expr_type()));
      }
    }
  }

  return ret;
}

int ObLogicalOperator::adjust_parent_child_relationship()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    ObLogicalOperator* child = NULL;
    if (OB_ISNULL(child = get_child(i)) || OB_ISNULL(child->get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(get_child(i)), K(ret));
    } else if (OB_FAIL(child->adjust_parent_child_relationship())) {
      LOG_WARN("failed to adjust parent-child relationship", K(ret));
    } else {
      child->set_parent(this);
      if (type_ == log_op_def::LOG_SET || type_ == log_op_def::LOG_SUBPLAN_SCAN || type_ == log_op_def::LOG_UNPIVOT) {
        child->mark_is_plan_root();
        child->get_plan()->set_plan_root(child);
      }
    }
  }
  return ret;
}

double FilterCompare::get_selectivity(ObRawExpr* expr)
{
  bool found = false;
  double selectivity = 1;
  for (int64_t i = 0; !found && i < predicate_selectivities_.count(); i++) {
    if (predicate_selectivities_.at(i).expr_ == expr) {
      found = true;
      selectivity = predicate_selectivities_.at(i).sel_;
    }
  }
  if (!found) {
    LOG_PRINT_EXPR(WARN, "Failed to get selectivity", expr);
  }
  return selectivity;
}

// Add a child to the end of the array
int ObLogicalOperator::add_child(ObLogicalOperator* child_op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_.push_back(child_op))) {
    LOG_WARN("failed to push back child op", K(ret));
  }
  return ret;
}

int ObLogicalOperator::check_order_unique(bool& order_unique) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid logical operator", K(ret), K(get_stmt()));
  } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(get_op_ordering(),
                 get_table_set(),
                 get_fd_item_set(),
                 get_ordering_output_equal_sets(),
                 get_output_const_exprs(),
                 order_unique))) {
    LOG_WARN("failed to check is order unique", K(ret));
  }
  return ret;
}

int ObLogicalOperator::set_op_ordering(const common::ObIArray<OrderItem>& op_ordering)
{
  int ret = OB_SUCCESS;
  op_ordering_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < op_ordering.count(); ++i) {
    if (OB_FAIL(op_ordering_.push_back(op_ordering.at(i)))) {
      LOG_WARN("failed to push back order item");
    }
  }
  return ret;
}

int ObLogicalOperator::set_local_ordering(const common::ObIArray<OrderItem>& op_local_ordering)
{
  int ret = OB_SUCCESS;
  reset_local_ordering();
  for (int64_t i = 0; OB_SUCC(ret) && i < op_local_ordering.count(); ++i) {
    if (OB_FAIL(op_local_ordering_.push_back(op_local_ordering.at(i)))) {
      LOG_WARN("failed to push back local order item");
    }
  }
  return ret;
}

int ObLogicalOperator::set_expected_ordering(const common::ObIArray<OrderItem>& expected_ordering)
{
  int ret = OB_SUCCESS;
  expected_ordering_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < expected_ordering.count(); ++i) {
    if (OB_FAIL(expected_ordering_.push_back(expected_ordering.at(i)))) {
      LOG_WARN("failed to push back order item");
    }
  }
  return ret;
}

int ObLogicalOperator::transmit_local_ordering()
{
  int ret = OB_SUCCESS;
  reset_local_ordering();
  ObLogicalOperator* child = get_child(first_child);
  if (NULL == child) {
    // do nothing
  } else if (OB_FAIL(set_local_ordering(child->get_local_ordering()))) {
    LOG_WARN("failed to set local ordering", K(ret));
  }
  return ret;
}

int ObLogicalOperator::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = get_child(first_child);
  if (NULL == child) {
    // do nothing
  } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
    LOG_WARN("failed to set op ordering", K(ret));
  } else if (OB_FAIL(transmit_local_ordering())) {
    LOG_WARN("failed to set local ordering", K(ret));
  }
  return ret;
}

int ObLogicalOperator::get_ordering_input_equal_sets(EqualSets& ordering_in_esets) const
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(i));
    } else if (OB_FAIL(append(ordering_in_esets, child->get_ordering_output_equal_sets()))) {
      LOG_WARN("failed to append ordering equal sets", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::get_sharding_input_equal_sets(EqualSets& sharding_in_esets) const
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(i));
    } else if (OB_FAIL(append(sharding_in_esets, child->get_sharding_output_equal_sets()))) {
      LOG_WARN("failed to append ordering equal sets", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::get_input_const_exprs(ObIArray<ObRawExpr*>& const_exprs) const
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(i));
    } else if (OB_FAIL(append(const_exprs, child->get_output_const_exprs()))) {
      LOG_WARN("failed to append output const exprs", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::compute_equal_set()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  EqualSets* ordering_esets = NULL;
  if (OB_ISNULL(my_plan_) || OB_UNLIKELY(get_num_of_child() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is invalid", K(ret), K(get_num_of_child()), K(my_plan_));
  } else if (OB_UNLIKELY(get_num_of_child() == 0)) {
    // do nothing
  } else if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else if (filter_exprs_.empty()) {
    // inherit equal sets from the first child directly
    set_ordering_output_equal_sets(&child->get_ordering_output_equal_sets());
  } else if (OB_ISNULL(ordering_esets = get_plan()->create_equal_sets())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create equal sets", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(&my_plan_->get_allocator(),
                 filter_exprs_,
                 child->get_ordering_output_equal_sets(),
                 *ordering_esets))) {
    LOG_WARN("failed to compute ordering output equal set", K(ret));
  } else {
    set_ordering_output_equal_sets(ordering_esets);
  }
  return ret;
}

int ObLogicalOperator::compute_sharding_equal_sets(const ObIArray<ObRawExpr*>& sharding_conds)
{
  int ret = OB_SUCCESS;
  EqualSets* sharding_esets = NULL;
  if (sharding_conds.empty()) {
    set_sharding_output_equal_sets(&get_ordering_output_equal_sets());
  } else if (OB_ISNULL(sharding_esets = get_plan()->create_equal_sets())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create equal sets", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                 &get_plan()->get_allocator(), sharding_conds, get_ordering_output_equal_sets(), *sharding_esets))) {
    LOG_WARN("failed to compute sharding output equal sets", K(ret));
  } else {
    set_sharding_output_equal_sets(sharding_esets);
  }
  return ret;
}

int ObLogicalOperator::compute_const_exprs()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(my_plan_) || OB_UNLIKELY(get_num_of_child() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is invalid", K(ret), K(get_num_of_child()), K(my_plan_));
  } else if (OB_UNLIKELY(get_num_of_child() == 0)) {
    // do nothing
  } else if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else if (OB_FAIL(append(const_exprs_, child->const_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(filter_exprs_, const_exprs_))) {
    LOG_WARN("failed to compute const conditionexprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_UNLIKELY(get_num_of_child() == 0)) {
    set_fd_item_set(&empty_fd_item_set_);
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator does not have child", K(ret));
  } else {
    set_fd_item_set(&child->get_fd_item_set());
  }
  return ret;
}

int ObLogicalOperator::deduce_const_exprs_and_ft_item_set(ObFdItemSet& fd_item_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> column_exprs;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(my_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(get_stmt()->get_column_exprs(column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().deduce_fd_item_set(
                 get_ordering_output_equal_sets(), column_exprs, get_output_const_exprs(), fd_item_set))) {
    LOG_WARN("falied to remove const in fd item", K(ret));
  }
  return ret;
}

int ObLogicalOperator::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_UNLIKELY(get_num_of_child() == 0)) {
    /*do nothing*/
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator does not have child", K(ret));
  } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
    LOG_WARN("failed to set op ordering", K(ret));
  }
  return ret;
}

int ObLogicalOperator::compute_op_interesting_order_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (get_num_of_child() == 0) {
    set_interesting_order_info(OrderingFlag::NOT_MATCH);
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator does not have child", K(ret));
  } else {
    set_interesting_order_info(child->get_interesting_order_info());
  }
  return ret;
}

int ObLogicalOperator::compute_table_set()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (LOG_SUBPLAN_FILTER == get_type() || get_num_of_child() == 1) {
    if (OB_ISNULL(child = get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      set_table_set(&child->get_table_set());
    }
  } else {
    ObRelIds* table_set = NULL;
    if (OB_ISNULL(get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(table_set = (ObRelIds*)get_plan()->get_allocator().alloc(sizeof(ObRelIds)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      table_set = new (table_set) ObRelIds();
      for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
        if (OB_ISNULL(child = get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          table_set->add_members(child->get_table_set());
        }
      }
      if (OB_SUCC(ret)) {
        set_table_set(table_set);
      }
    }
  }
  return ret;
}

int ObLogicalOperator::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  is_at_most_one_row_ = false;
  if (get_num_of_child() > 0) {
    is_at_most_one_row_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_at_most_one_row_ && i < get_num_of_child(); i++) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!get_child(i)->get_is_at_most_one_row()) {
        is_at_most_one_row_ = false;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogicalOperator::compute_property(Path* path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_ISNULL(path->parent_) || OB_ISNULL(path->parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path is null", K(ret), K(path));
  } else if (OB_FAIL(append(get_output_const_exprs(), path->parent_->get_const_exprs()))) {
    LOG_WARN("failed to append const exprs", K(ret));
  } else {
    set_op_ordering(path->get_ordering());
    set_ordering_output_equal_sets(&path->parent_->get_ordering_output_equal_sets());
    set_fd_item_set(&path->parent_->get_fd_item_set());
    set_table_set(&path->parent_->get_tables());
    set_is_at_most_one_row(path->parent_->get_is_at_most_one_row());
    set_est_sel_info(&path->parent_->get_plan()->get_est_sel_info());

    // set cost, card, width
    set_op_cost(path->op_cost_);
    set_cost(path->cost_);
    if (path->is_inner_path()) {
      set_card(path->inner_row_count_);
    } else {
      set_card(path->parent_->get_output_rows());
    }
    set_width(path->parent_->get_average_output_row_size());
    set_interesting_order_info(path->get_interesting_order_info());
  }
  return ret;
}

int ObLogicalOperator::compute_property()
{
  int ret = OB_SUCCESS;
  bool reuse_property = false;
  ObLogicalOperator* first_op = NULL;
  if (OB_ISNULL(my_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log plan is null", K(ret), K(my_plan_));
  } else if (OB_NOT_NULL(first_op = my_plan_->get_candidate_plans().first_new_op_) && first_op->get_type() == type_) {
    // share property with the first allocated operator of the same type
    if (OB_FAIL(append(get_output_const_exprs(), first_op->get_output_const_exprs()))) {
      LOG_WARN("failed to append const exprs", K(ret));
    } else {
      set_ordering_output_equal_sets(&first_op->get_ordering_output_equal_sets());
      set_sharding_output_equal_sets(&first_op->get_sharding_output_equal_sets());
      set_fd_item_set(&first_op->get_fd_item_set());
      set_est_sel_info(first_op->get_est_sel_info());
      set_table_set(&first_op->get_table_set());
      set_is_at_most_one_row(first_op->get_is_at_most_one_row());
      reuse_property = true;
    }
  } else {
    // compute new property
    if (OB_FAIL(compute_const_exprs())) {
      LOG_WARN("failed to compute compute const exprs", K(ret));
    } else if (OB_FAIL(compute_equal_set())) {
      LOG_WARN("failed to compute equal sets", K(ret));
    } else if (OB_FAIL(compute_fd_item_set())) {
      LOG_WARN("failed to compute fd item sets", K(ret));
    } else if (OB_FAIL(compute_table_set())) {
      LOG_WARN("failed to compute table sets", K(ret));
    } else if (OB_FAIL(compute_one_row_info())) {
      LOG_WARN("failed to compute one row info", K(ret));
    } else if (OB_FAIL(init_est_sel_info())) {
      LOG_WARN("failed to init est sel info", K(ret));
    } else if (share_property()) {
      my_plan_->get_candidate_plans().first_new_op_ = this;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(est_cost())) {
      LOG_WARN("failed to estimate cost", K(ret));
    } else if (OB_FAIL(compute_op_ordering())) {
      LOG_WARN("failed to compute op ordering", K(ret));
    } else if (OB_FAIL(compute_op_interesting_order_info())) {
      LOG_WARN("failed to compute op ordering match info", K(ret));
    } else {
      LOG_TRACE("compute property finished",
          K(get_op_name(type_)),
          K(get_cost()),
          K(reuse_property),
          K(op_ordering_),
          K(const_exprs_),
          K(ordering_output_equal_sets_),
          K(sharding_output_equal_sets_),
          K(fd_item_set_),
          K(is_at_most_one_row_));
    }
  }
  return ret;
}

int ObLogicalOperator::explain_collect_width_pre(void* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret));
  } else {
    planText* explain_ctx = reinterpret_cast<planText*>(ctx);
    planText& plan = *explain_ctx;
    plan_formatter& formatter = plan.formatter;
    char buffer[OB_MAX_PLAN_EXPLAIN_NAME_LENGTH];
    for (int i = 0; OB_SUCC(ret) && i < formatter.num_of_columns; i++) {
      int32_t length = 0;
      switch (i) {
        case Id: {
          snprintf(buffer, sizeof(buffer), "%ld", op_id_);
          length = (int32_t)strlen(buffer);
        } break;
        case Operator: {
          length = get_explain_name_length() + plan.level++;
        } break;
        case Name: {
          if (log_op_def::instance_of_log_table_scan(type_)) {
            ObLogTableScan* scan = reinterpret_cast<ObLogTableScan*>(this);
            length = scan->get_table_name().length();
            if (!scan->get_index_name().empty()) {
              length += 2 + scan->get_index_name().length();  //"(index_name)"
              if (is_descending_direction(scan->get_scan_directioin())) {
                length += 8;  //",REVERSE"
              } else {        /* Do nothing */
              }
            } else {
              if (is_descending_direction(scan->get_scan_directioin())) {
                length += 9;  //"(REVERSE)"
              } else {        /* Do nothing */
              }
            }
          } else if (log_op_def::LOG_SUBPLAN_SCAN == type_ || log_op_def::LOG_UNPIVOT == type_) {
            ObLogSubPlanScan* scan = static_cast<ObLogSubPlanScan*>(this);
            length = scan->get_subquery_name().length();
          } else if (log_op_def::LOG_TABLE_LOOKUP == type_) {
            ObLogTableLookup* table_lookup = reinterpret_cast<ObLogTableLookup*>(this);
            length += table_lookup->get_table_name().length();
          } else if (log_op_def::LOG_EXCHANGE == type_) {
            ObLogExchange* exchange = static_cast<ObLogExchange*>(this);
            if (OB_INVALID_ID != exchange->get_dfo_id()) {
              snprintf(buffer,
                  OB_MAX_PLAN_EXPLAIN_NAME_LENGTH,
                  ":EX%ld%04ld",
                  exchange->get_px_id(),
                  exchange->get_dfo_id());
              length = (int32_t)strlen(buffer);
            } else {
              length = 0;
            }
          } else if (log_op_def::LOG_UPDATE == type_ || log_op_def::LOG_DELETE == type_ ||
                     log_op_def::LOG_INSERT == type_ || log_op_def::LOG_INSERT_ALL == type_) {
            ObLogDelUpd* dml_op = static_cast<ObLogDelUpd*>(this);
            if (dml_op->is_pdml() && dml_op->is_index_maintenance() && nullptr != dml_op->get_all_table_columns() &&
                dml_op->get_all_table_columns()->count() == 1 &&
                dml_op->get_all_table_columns()->at(0).index_dml_infos_.count() == 1) {
              int64_t base_table_len = dml_op->get_all_table_columns()->at(0).table_name_.length();
              int64_t index_table_len =
                  dml_op->get_all_table_columns()->at(0).index_dml_infos_.at(0).index_name_.length();
              int64_t bracket_len = 2;  // base_table(index_table)
              length = base_table_len + index_table_len + bracket_len;
            }
          } else {
            length = 0;
          }
        } break;
        case Est_Rows: {
          //      length = (int) strlen("TBD");
          snprintf(buffer, sizeof(buffer), "%ld", static_cast<int64_t>(ceil(get_card())));
          length = (int32_t)strlen(buffer);
        } break;
        case Cost: {
          snprintf(buffer, sizeof(buffer), "%ld", static_cast<int64_t>(ceil(cost_)));
          length = (int32_t)strlen(buffer);
        } break;

        default: {
          LOG_WARN("Unexpected access to default branch", K(i));
        } break;
      }

      if (i < 0 || i >= formatter.max_plan_column_width) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("i is out of range of [0,max_plan_column_width)", K(ret));
      } else if (formatter.column_width[i] < length) {
        formatter.column_width[i] = length;
      } else { /* Do nothing */
      }
    }
  }

  return ret;
}

int ObLogicalOperator::explain_collect_width_post(void* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret));
  } else {
    planText* explain_ctx = reinterpret_cast<planText*>(ctx);
    explain_ctx->level--;
  }
  return ret;
}

#define NEW_LINE "\n"
#define SEPARATOR "|"
#define SPACE " "
#define PLAN_WRAPPER "="
#define LINE_SEPARATOR "-"

int ObLogicalOperator::explain_index_selection_info_pre(void* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx is null", K(ret));
  } else {
    planText* explain_ctx = reinterpret_cast<planText*>(ctx);
    planText& plan = *explain_ctx;
    char* buf = plan.buf;
    int64_t& buf_len = plan.buf_len;
    int64_t& pos = plan.pos;
    if (OB_FAIL(BUF_PRINTF(NEW_LINE))) { /* Do nothing */
    } else if (OB_FAIL(explain_index_selection_info(buf, buf_len, pos))) {
      LOG_WARN("print index selection info fails", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::explain_write_buffer_post(void* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret));
  } else {
    planText* explain_ctx = reinterpret_cast<planText*>(ctx);
    explain_ctx->level--;
  }
  return ret;
}

int ObLogicalOperator::explain_write_buffer_pre(void* ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret));
  } else {
    planText* explain_ctx = reinterpret_cast<planText*>(ctx);
    planText& plan = *explain_ctx;
    char buffer[OB_MAX_PLAN_EXPLAIN_NAME_LENGTH];
    char* buf = plan.buf;
    int64_t& buf_len = plan.buf_len;
    int64_t& pos = plan.pos;

    // skip the ID
    if (OB_FAIL(BUF_PRINTF(SEPARATOR))) { /* Do nothing */
    } else if (plan.formatter.max_plan_column_width <= 0) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("max_plan_column_width is not greater than 0", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%-*ld", plan.formatter.column_width[0], op_id_))) { /* Do nothing */
    } else if (OB_FAIL(BUF_PRINTF(SEPARATOR))) {
      /* Do nothing */
    }

    for (int i = 1; OB_SUCC(ret) && i < plan.formatter.num_of_columns; i++) {
      int64_t tmp_pos = 0;
      switch (i) {
        case Operator: {
          break;
        }
        case Name: {
          if (log_op_def::instance_of_log_table_scan(type_)) {
            ObLogTableScan* scan = static_cast<ObLogTableScan*>(this);
            const ObString& name = scan->get_table_name();
            databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%.*s", name.length(), name.ptr());
            if (scan->is_index_scan()) {
              const ObString& index_name = scan->get_index_name();
              databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%s", LEFT_BRACKET);
              databuff_printf(
                  buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%.*s", index_name.length(), index_name.ptr());
              if (is_descending_direction(scan->get_scan_directioin())) {
                databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%s", COMMA_REVERSE);
              }
              databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%s", RIGHT_BRACKET);
            } else {
              if (is_descending_direction(scan->get_scan_directioin())) {
                databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%s", BRACKET_REVERSE);
              }
            }
          } else if (log_op_def::LOG_SUBPLAN_SCAN == type_ || log_op_def::LOG_UNPIVOT == type_) {
            ObLogSubPlanScan* scan = reinterpret_cast<ObLogSubPlanScan*>(this);
            ObString& name = scan->get_subquery_name();
            databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%.*s", name.length(), name.ptr());
          } else if (log_op_def::LOG_TEMP_TABLE_INSERT == type_) {
            ObLogTempTableInsert* insert = reinterpret_cast<ObLogTempTableInsert*>(this);
            const ObString& name = insert->get_table_name();
            databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%.*s", name.length(), name.ptr());
          } else if (log_op_def::LOG_TEMP_TABLE_ACCESS == type_) {
            ObLogTempTableAccess* access = reinterpret_cast<ObLogTempTableAccess*>(this);
            const ObString& name = access->get_table_name();
            databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%.*s", name.length(), name.ptr());
          } else if (log_op_def::LOG_TABLE_LOOKUP == type_) {
            ObLogTableLookup* table_lookup = reinterpret_cast<ObLogTableLookup*>(this);
            const ObString& name = table_lookup->get_table_name();
            databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%.*s", name.length(), name.ptr());
          } else if (log_op_def::LOG_EXCHANGE == type_) {
            ObLogExchange* exchange = static_cast<ObLogExchange*>(this);
            if (OB_INVALID_ID != exchange->get_dfo_id()) {
              databuff_printf(buffer,
                  OB_MAX_PLAN_EXPLAIN_NAME_LENGTH,
                  tmp_pos,
                  ":EX%ld%04ld",
                  exchange->get_px_id(),
                  exchange->get_dfo_id());
            }
          } else if (log_op_def::LOG_UPDATE == type_ || log_op_def::LOG_DELETE == type_ ||
                     log_op_def::LOG_INSERT == type_ || log_op_def::LOG_INSERT_ALL == type_) {
            ObLogUpdate* dml_op = static_cast<ObLogUpdate*>(this);
            if (dml_op->is_pdml() && dml_op->is_index_maintenance() && nullptr != dml_op->get_all_table_columns() &&
                dml_op->get_all_table_columns()->count() == 1 &&
                dml_op->get_all_table_columns()->at(0).index_dml_infos_.count() == 1) {
              const ObString& base_table = dml_op->get_all_table_columns()->at(0).table_name_;
              const ObString& index_table = dml_op->get_all_table_columns()->at(0).index_dml_infos_.at(0).index_name_;
              databuff_printf(buffer,
                  OB_MAX_PLAN_EXPLAIN_NAME_LENGTH,
                  tmp_pos,
                  "%.*s(%.*s)",
                  base_table.length(),
                  base_table.ptr(),
                  index_table.length(),
                  index_table.ptr());
            }
          }
          // left padding with space
          int64_t N = plan.formatter.column_width[i] - tmp_pos;
          for (int64_t n = 0; n < N; ++n) {
            databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, " ");
          }
          break;
        }
        case Est_Rows: {
          if (1000000000000 > get_card()) {
            databuff_printf(
                buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%ld", static_cast<int64_t>(ceil(get_card())));
          } else {
            databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%e", get_card());
          }
          break;
        }
        case Cost: {
          if (1000000000000 > cost_) {
            databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%ld", static_cast<int64_t>(ceil(cost_)));
          } else {
            databuff_printf(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH, tmp_pos, "%e", cost_);
          }
          break;
        }
        default: {
          LOG_WARN("Unexpected access to default branch", K(i));
          break;
        }
      }  // end of switch

      if (OB_FAIL(ret)) {
        LOG_WARN("Get unexpected error", K(ret));
      } else if (Operator == i) {
        for (int m = 0; OB_SUCC(ret) && m < plan.level; m++) {
          ret = BUF_PRINTF(SPACE);
        }
        // BUF_PRINTF("%-*s", plan.formatter.column_width[i] - plan.level, get_name());
        int64_t pos_internal = 0;
        if (OB_FAIL(ret)) {
          LOG_WARN("Previous BUF_PRINTF fails", K(ret));
        } else if (OB_FAIL(get_explain_name_internal(buf + pos, buf_len - pos, pos_internal))) {
          LOG_WARN("Getting explain name internal fails");
        } else {
          pos += pos_internal;
        }
        for (int64_t m = 0; OB_SUCC(ret) && m < plan.formatter.column_width[i] - get_explain_name_length() - plan.level;
             ++m) {
          ret = BUF_PRINTF(SPACE);
        }
      } else {
        ret = BUF_PRINTF("%-*s", plan.formatter.column_width[i], buffer);
      }
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(SEPARATOR);
      } else { /* Do nothing */
      }
    }  // for
    if (OB_FAIL(ret)) {
      LOG_WARN("Get unexpected error", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(NEW_LINE))) { /* Do nothing */
    } else {
      plan.level++;
    }
  }

  return ret;
}

int ObLogicalOperator::print_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  ExplainType old_explain_type = EXPLAIN_UNINITIALIZED;
  ExplainType head_explain_type = (EXPLAIN_EXTENDED_NOADDR == old_explain_type ? old_explain_type : type);
  if (EXPLAIN_EXTENDED_NOADDR == type || EXPLAIN_PLANREGRESS == type) {
    old_explain_type = type;
    type = EXPLAIN_BASIC;
  } else { /* Do nothing */
  }
  // print some msg at the head of this print line
  if (OB_FAIL(print_plan_head_annotation(buf, buf_len, pos, head_explain_type))) {
    LOG_WARN("Print my plan annotation fails", K(ret));
  } else { /* Do nothing */
  }

  // print output
  const ObIArray<ObRawExpr*>& output = output_exprs_;
  EXPLAIN_PRINT_EXPRS(output, type);
  if (OB_FAIL(ret)) {
    LOG_WARN("Previous step fails", K(ret));
  } else {
    ret = BUF_PRINTF(", ");
  }

  // print filter
  if (LOG_TABLE_LOOKUP == get_type()) {
    ObLogTableLookup* table_lookup = static_cast<ObLogTableLookup*>(this);
    if (OB_ISNULL(table_lookup->get_index_back_scan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null operator", K(ret));
    } else {
      const ObIArray<ObRawExpr*>& filter = table_lookup->get_index_back_scan()->get_filter_exprs();
      EXPLAIN_PRINT_EXPRS(filter, type);
    }
  } else if (LOG_UNPIVOT != get_type()) {
    const ObIArray<ObRawExpr*>& filter = filter_exprs_;
    EXPLAIN_PRINT_EXPRS(filter, type);
  }

  // print startup filter
  const ObIArray<ObRawExpr*>& startup_filter = startup_exprs_;
  if (OB_FAIL(ret)) {
    LOG_WARN("Previous step fails", K(ret));
  } else if (!startup_filter.empty()) {
    ret = BUF_PRINTF(", ");
    EXPLAIN_PRINT_EXPRS(startup_filter, type);
  } else { /* Do nothing */
  }

  if (OB_SUCC(ret) && EXPLAIN_UNINITIALIZED != old_explain_type) {
    type = old_explain_type;
  } else { /* Do nothing */
  }
  if (OB_FAIL(print_my_plan_annotation(buf, buf_len, pos, type))) {
    LOG_WARN("Print my plan annotation fails", K(ret));
  } else { /* Do nothing */
  }

  return ret;
}

int ObLogicalOperator::explain_write_buffer_output_pre(void* ctx)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(ctx));
  if (OB_SUCC(ret)) {
    planText* explain_ctx = reinterpret_cast<planText*>(ctx);
    planText& plan = *explain_ctx;
    char* buf = plan.buf;
    int64_t& buf_len = plan.buf_len;
    int64_t& pos = plan.pos;
    OC((BUF_PRINTF)(NEW_LINE));
    OC((BUF_PRINTF)("  "));
    OC((BUF_PRINTF)("%ld", op_id_));
    OC((BUF_PRINTF)(" - "));
    OC((print_plan_annotation)(buf, buf_len, pos, plan.format));
  }
  return ret;
}

int ObLogicalOperator::explain_write_buffer_outline_pre(void* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret));
  } else {
    planText* plan_text = reinterpret_cast<planText*>(ctx);
    if (OB_FAIL(print_outline(*plan_text))) {
      LOG_WARN("fail to print outline", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::do_pre_traverse_operation(const TraverseOp& op, void* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_plan()));
  } else {
    switch (op) {
      case PX_PIPE_BLOCKING: {
        ObPxPipeBlockingCtx* pipe_blocking_ctx = static_cast<ObPxPipeBlockingCtx*>(ctx);
        if (OB_ISNULL(pipe_blocking_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ctx", K(ret));
        } else if (OB_FAIL(px_pipe_blocking_pre(*pipe_blocking_ctx))) {
          LOG_WARN("blocking px pipe failed", K(ret));
        }
        break;
      }
      case PX_RESCAN: {
        if (OB_FAIL(px_rescan_pre())) {
          LOG_WARN("blocking px pipe failed", K(ret));
        }
        break;
      }
      case ALLOC_GI: {
        // do nothing
        AllocGIContext* alloc_gi_ctx = static_cast<AllocGIContext*>(ctx);
        if (OB_ISNULL(alloc_gi_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          if (get_num_of_child() >= 2) {
            alloc_gi_ctx->add_multi_child_op_count();
          }
          if (OB_FAIL(allocate_granule_pre(*alloc_gi_ctx))) {
            LOG_WARN("allocate granule pre failed", K(ret));
          } else if (alloc_gi_ctx->alloc_gi_ && OB_FAIL(allocate_granule_nodes_above(*alloc_gi_ctx))) {
            LOG_WARN("allocate granule iterator failed", K(ret));
          }
        }
        break;
      }
      case ALLOC_MONITORING_DUMP: {
        AllocMDContext* md_ctx = static_cast<AllocMDContext*>(ctx);
        op_id_ = md_ctx->org_op_id_++;
        break;
      }
      case ALLOC_EXPR: {
        ObAllocExprContext* alloc_expr_context = static_cast<ObAllocExprContext*>(ctx);
        if (OB_ISNULL(alloc_expr_context)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alloc_expr_context is NULL", K(ret));
        } else if (is_plan_root() && OB_FAIL(add_plan_root_exprs(*alloc_expr_context))) {
          LOG_WARN("failed to set plan root output", K(ret));
        } else if (OB_FAIL(allocate_expr_pre(*alloc_expr_context))) {
          LOG_WARN("failed to do allocate expr pre", K(ret));
        } else {
          LOG_TRACE("succeed to do allcoate expr pre", K(get_type()), K(ret));
        }
        break;
      }
      case ADJUST_SORT_OPERATOR: {
        AdjustSortContext* adjust_sort_ctx = static_cast<AdjustSortContext*>(ctx);
        if (LOG_EXCHANGE == get_type() && !static_cast<ObLogExchange*>(this)->get_is_remote()) {
          adjust_sort_ctx->has_exchange_ = true;
        }
        if (OB_FAIL(set_exchange_cnt_pre(adjust_sort_ctx))) {
          LOG_WARN("fail to set top exchange", K(ret));
        }
        break;
      }
      case RE_CALC_OP_COST: {
        break;
      }
      case ALLOC_DUMMY_OUTPUT: {
        break;
      }
      case PROJECT_PRUNING: {
        if (OB_FAIL(project_pruning_pre())) {
          LOG_WARN("Project pruning pre error", K(ret));
        } else { /* Do nothing */
        }
        break;
      }
      case ALLOC_EXCH: {
        if (is_plan_root()) {
          get_plan()->set_plan_root(this);
          ObSQLSessionInfo* session = NULL;
          if (OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("session info is null", K(ret), K(session));
          } else if (session->get_is_in_retry_for_dup_tbl()) {
            // do nothing
          } else if (get_parent() != NULL) {
            // do nothing
          } else if (OB_FAIL(reselect_duplicate_table_replica())) {
            LOG_WARN("failed to reselect duplicate table replica", K(ret));
          }
        } else { /* Do nothing */
        }
        break;
      }
      case OPERATOR_NUMBERING: {
        if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx is null", K(ret));
        } else if (OB_FAIL(numbering_operator_pre(*static_cast<NumberingCtx*>(ctx)))) {
          LOG_WARN("numbering operator pre failed", K(ret));
        }
        break;
      }
      case EXCHANGE_NUMBERING: {
        if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx is null", K(ret));
        } else if (OB_FAIL(numbering_exchange_pre(*static_cast<NumberingExchangeCtx*>(ctx)))) {
          LOG_WARN("fail numbering exchange pre", K(ret));
        }
        break;
      }
      case GEN_SIGNATURE: {
        break;
      }
      case GEN_LOCATION_CONSTRAINT: {
        break;
      }
      case REORDER_PROJECT_COLUMNS: {
        break;
      }
      case EXPLAIN_COLLECT_WIDTH: {
        if (OB_FAIL(explain_collect_width_pre(ctx))) {
          LOG_WARN("explain_collect_width_pre fails", K(ret));
        } else { /* Do nothing */
        }
        break;
      }
      case EXPLAIN_WRITE_BUFFER: {
        if (OB_FAIL(explain_write_buffer_pre(ctx))) {
          LOG_WARN("explain_write_buffer_pre fails", K(ret));
        } else { /* Do nothing */
        }
        break;
      }
      case EXPLAIN_WRITE_BUFFER_OUTPUT: {
        if (OB_FAIL(explain_write_buffer_output_pre(ctx))) {
          LOG_WARN("explain_write_buffer_output_pre fails", K(ret));
        }
        break;
      }
      case EXPLAIN_WRITE_BUFFER_OUTLINE: {
        if (OB_FAIL(explain_write_buffer_outline_pre(ctx))) {
          LOG_WARN("explain_write_buffer_outline_pre fails", K(ret));
        }
        break;
      }
      case EXPLAIN_INDEX_SELECTION_INFO: {
        if (OB_FAIL(explain_index_selection_info_pre(ctx))) {
          LOG_WARN("explain index_selection_info_pre", K(ret));
        }
        break;
      }
      case PX_ESTIMATE_SIZE: {
        ret = px_estimate_size_factor_pre();
        break;
      }
      case CG_PREPARE: {
        ObSQLSessionInfo* session = NULL;
        CK(NULL != get_plan() && NULL != (session = get_plan()->get_optimizer_context().get_session_info()));
        if (OB_SUCC(ret) && session->use_static_typing_engine()) {
          FOREACH_X(e, output_exprs_, OB_SUCC(ret))
          {
            CK(NULL != *e);
            // Add remove_const() to above const expr, except:
            // - remove_const() already added. (has CNT_VOLATILE_CONST flag)
            // - is dynamic param store (has IS_EXEC_PARAM flag). Because question mark expr of
            //   dynamic param store may be passed by operator output. e.g.:
            //
            //   select /*+NO_REWRITE*/ count(*) from t1 group by 1 + (select 1 > all (select 0));
            //
            if ((*e)->has_const_or_const_expr_flag() && !(*e)->has_flag(CNT_VOLATILE_CONST) &&
                !(*e)->has_flag(IS_EXEC_PARAM)) {
              ObRawExpr* remove_const_expr = NULL;
              OZ(ObRawExprUtils::build_remove_const_expr(
                  get_plan()->get_optimizer_context().get_expr_factory(), *session, *e, remove_const_expr));
              CK(NULL != remove_const_expr);
              OZ(get_plan()->get_all_exprs().append(remove_const_expr));
              if (OB_SUCC(ret)) {
                *e = remove_const_expr;
              }
            }
          }
        }
        break;
      }
      case ALLOC_LINK: {
        break;
      }
      case GEN_LINK_STMT: {
        GenLinkStmtContext* link_ctx = static_cast<GenLinkStmtContext*>(ctx);
        if (OB_ISNULL(link_ctx)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("link stmt is NULL", K(ret));
        } else if (OB_FAIL(generate_link_sql_pre(*link_ctx))) {
          LOG_WARN("failed to gen link sql", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected access of default branch", K(op), K(ret));
        break;
      }
    }
  }

  return ret;
}

int ObLogicalOperator::re_calc_cost()
{
  int ret = OB_SUCCESS;
  if (get_num_of_child() > 0) {
    double cur_cost = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op's child_i is null", K(ret));
      } else {
        cur_cost += get_child(i)->get_cost();
      }
    }
    if (OB_SUCC(ret)) {
      set_cost(cur_cost + get_op_cost());
    }
  }
  return ret;
}

int ObLogicalOperator::inner_adjust_sort_operator(AdjustSortContext* adjust_sort_ctx, bool& need_remove)
{
  int ret = OB_SUCCESS;
  need_remove = false;
  if (LOG_SORT == get_type()) {  // remove redundancy sort_op
    bool need_sort = true;
    if (0 == static_cast<ObLogSort*>(this)->get_sort_keys().count()) {
      need_remove = true;
    } else if (NULL != get_child(0) && LOG_EXCHANGE == get_child(0)->get_type() &&
               !static_cast<ObLogExchange*>(get_child(0))->is_keep_order()) {
      // set merge sort
      if (OB_FAIL(set_merge_sort(this, adjust_sort_ctx, need_remove))) {
        LOG_WARN("failed to set merge sort", K(ret));
      }
    } else if (OB_FAIL(check_need_sort_below_node(0, static_cast<ObLogSort*>(this)->get_sort_keys(), need_sort))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else {
      need_remove = !need_sort;
    }
    // check if need prefix sort
    if (OB_SUCC(ret) && !need_remove) {
      ObLogSort* sort_op = static_cast<ObLogSort*>(this);
      if (OB_FAIL(sort_op->check_prefix_sort())) {
        LOG_WARN("failed to check prefix sort", K(ret));
      } else if (sort_op->get_prefix_pos() > 0) {
        /*do nothing*/
      } else if (OB_FAIL(sort_op->check_local_merge_sort())) {
        LOG_WARN("failed to check local merge sort", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::remove_sort()
{
  int ret = OB_SUCCESS;
  if (LOG_SORT == get_type()) {
    ObLogicalOperator* child_op = NULL;
    if (1 != get_num_of_child()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op child count must be 1", K(ret), K(get_num_of_child()));
    } else if (OB_ISNULL(child_op = get_child(first_child)) || OB_ISNULL(get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is null or plan is null", K(ret), K(child_op));
    } else {
      ObLogicalOperator* parent_op = get_parent();
      child_op->set_parent(parent_op);
      if (NULL != parent_op) {
        for (int64_t i = 0; OB_SUCC(ret) && i < parent_op->get_num_of_child(); i++) {
          if (this == parent_op->get_child(i)) {
            parent_op->set_child(i, child_op);
          }
        }
      }
      if (is_plan_root()) {
        child_op->mark_is_plan_root();
        get_plan()->set_plan_root(child_op);
      }
    }
  }
  return ret;
}

int ObLogicalOperator::adjust_sort_operator(AdjustSortContext* adjust_sort_ctx)
{
  int ret = OB_SUCCESS;
  bool need_remove = false;
  const int64_t idx = get_child_id();
  if (LOG_SORT == get_type()) {  // remove redundancy sort_op
    if (OB_FAIL(inner_adjust_sort_operator(adjust_sort_ctx, need_remove))) {
      LOG_WARN("failed to adjust sort method", K(ret));
    } else if (!need_remove && NULL != get_parent() &&
               OB_FAIL(get_parent()->check_need_sort_below_node(idx, get_expected_ordering(), need_remove))) {
      LOG_WARN("failed to check need sort", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (need_remove && OB_FAIL(remove_sort())) {
        LOG_WARN("failed to remove child sort", K(ret));
      } else if (!need_remove && OB_FAIL(static_cast<ObLogSort*>(this)->check_prefix_sort())) {
        LOG_WARN("failed to check prefix sort", K(ret));
      }
    }
  } else {
    if (OB_FAIL(transmit_op_ordering())) {
      LOG_WARN("failed to transmit op ordering", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
      /* UNUSED */
      bool child_need_remove = false;
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is NULL", K(i), K(*this));
      } else if (LOG_SORT == get_child(i)->get_type() &&
                 OB_FAIL(get_child(i)->inner_adjust_sort_operator(adjust_sort_ctx, child_need_remove))) {
        LOG_WARN("failed to adjust sort method", K(ret));
      } else if (child_need_remove && OB_FAIL(get_child(i)->remove_sort())) {
        LOG_WARN("failed to remove child sort", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::set_exchange_cnt_pre(AdjustSortContext* adjust_sort_ctx)
{
  int ret = OB_SUCCESS;
  if (LOG_EXCHANGE == get_type()) {
    ++adjust_sort_ctx->exchange_cnt_;
  }
  return ret;
}

int ObLogicalOperator::set_exchange_cnt_post(AdjustSortContext* adjust_sort_ctx)
{
  int ret = OB_SUCCESS;
  if (LOG_EXCHANGE == get_type()) {
    --adjust_sort_ctx->exchange_cnt_;
  }
  return ret;
}

int ObLogicalOperator::do_post_traverse_operation(const TraverseOp& op, void* ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = NULL;
  CK(!OB_ISNULL(get_plan()), !OB_ISNULL(get_stmt()));

  if (OB_SUCC(ret)) {
    switch (op) {
      case PX_PIPE_BLOCKING: {
        ObPxPipeBlockingCtx* pipe_blocking_ctx = static_cast<ObPxPipeBlockingCtx*>(ctx);
        CK(OB_NOT_NULL(pipe_blocking_ctx));
        OC((px_pipe_blocking_post)(*pipe_blocking_ctx));
        break;
      }
      case ALLOC_GI: {
        AllocGIContext* alloc_gi_ctx = static_cast<AllocGIContext*>(ctx);
        if (OB_ISNULL(alloc_gi_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          if (get_num_of_child() >= 2) {
            alloc_gi_ctx->delete_multi_child_op_count();
          }
          if (OB_FAIL(allocate_granule_post(*alloc_gi_ctx))) {
            LOG_WARN("failed to allocate granule post", K(ret));
          } else if (alloc_gi_ctx->alloc_gi_ && OB_FAIL(allocate_granule_nodes_above(*alloc_gi_ctx))) {
            LOG_WARN("failed to allcoate granule nodes", K(ret));
          }
        }
        break;
      }
      case ALLOC_EXPR: {
        ObAllocExprContext* alloc_expr_ctx = static_cast<ObAllocExprContext*>(ctx);
        if (OB_ISNULL(alloc_expr_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(allocate_expr_post(*alloc_expr_ctx))) {
          LOG_WARN("failed to do allocate expr post", K(ret));
        } else {
          if (is_plan_root() && NULL == get_parent()) {
            bool all_produced = false;
            OC((all_expr_produced)(&(alloc_expr_ctx->expr_producers_), all_produced));
            if (OB_SUCC(ret)) {
              if (!all_produced) {
                // do nothing
                ret = OB_ERROR;
                LOG_WARN("failed to produce all required expressions",
                    "operator",
                    get_name(),
                    "expr_producers",
                    alloc_expr_ctx->expr_producers_,
                    K(ret));
              } else {
                LOG_TRACE("succ to produce every needed items",
                    "operator",
                    get_name(),
                    "# of items",
                    alloc_expr_ctx->expr_producers_.count());
              }
            }
          } else {
            // The top operator is responsible for putting the needed expr into request
          }
        }
        break;
      }
      case ALLOC_EXCH: {
        AllocExchContext* alloc_exch_ctx = reinterpret_cast<AllocExchContext*>(ctx);
        if (OB_ISNULL(alloc_exch_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(allocate_exchange_post(alloc_exch_ctx))) {
          LOG_WARN("failed to allocate exchange post", K(ret));
        } else if (OB_FAIL(check_is_uncertain_plan())) {
          LOG_WARN("failed to set location type", K(ret));
        } else if (OB_FAIL(replace_generated_agg_expr(alloc_exch_ctx->group_push_down_replaced_exprs_))) {
          LOG_WARN("failed to replace generated agg expr", K(ret));
        } else if (OB_FAIL(compute_sharding_equal_sets(alloc_exch_ctx->sharding_conds_))) {
          LOG_WARN("failed to compute sharding equal sets", K(ret));
        } else if (is_plan_root()) {
          ObLogicalOperator* top = this;
          if (get_stmt()->is_select_stmt() && !get_stmt()->has_limit() &&
              !static_cast<ObSelectStmt*>(get_stmt())->has_select_into() &&
              !static_cast<ObSelectStmt*>(get_stmt())->need_temp_table_trans() &&
              !static_cast<ObSelectStmt*>(get_stmt())->is_temp_table() && get_stmt()->has_order_by() &&
              !get_stmt()->is_order_siblings() && log_op_def::LOG_SORT != top->get_type() &&
              AllocExchContext::DistrStat::DISTRIBUTED == alloc_exch_ctx->plan_type_ &&
              OB_FAIL(allocate_stmt_order_by_above(top))) {
            LOG_WARN("failed to allocate stmt order by", K(ret));
          } else if (NULL == top->get_parent()) {
            // this is the final root operator
            ObExchangeInfo exch_info;
            bool is_remote = (AllocExchContext::DistrStat::UNINITIALIZED == alloc_exch_ctx->plan_type_);
            if (sharding_info_.is_local() || sharding_info_.is_match_all()) {
              /*do nothing*/
            } else if (sharding_info_.is_remote() ||
                       (log_op_def::LOG_SORT != top->get_type() && log_op_def::LOG_MATERIAL != top->get_type())) {
              ret = top->allocate_exchange_nodes_above(is_remote, *alloc_exch_ctx, exch_info);
            } else {
              ret = top->allocate_exchange(alloc_exch_ctx, exch_info);
            }
          } else {
            // plan root, but not the final root
            if (log_op_def::LOG_SORT == top->get_type() && log_op_def::LOG_SET != top->get_parent()->get_type() &&
                sharding_info_.is_distributed()) {
              ObExchangeInfo exch_info;
              if (OB_FAIL(top->allocate_exchange(alloc_exch_ctx, exch_info))) {
                LOG_WARN("failed to allocate exchange for sort node", K(ret));
              } else {
                top->get_sharding_info().set_location_type(OB_TBL_LOCATION_LOCAL);
              }
            } else if (log_op_def::LOG_SORT == top->get_type() && top->get_sharding_info().is_uninitial()) {
              if (OB_UNLIKELY(top->get_num_of_child() != 1) || OB_ISNULL(top->get_child(0))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected child", K(ret), K(top->get_num_of_child()));
              } else if (OB_FAIL(
                             top->get_sharding_info().copy_with_part_keys(top->get_child(0)->get_sharding_info()))) {
                LOG_WARN("failed to copy sharding info", K(ret));
              }
            }
          }
        }
        break;
      }
      case ADJUST_SORT_OPERATOR: {
        AdjustSortContext* adjust_sort_ctx = static_cast<AdjustSortContext*>(ctx);
        if (OB_SUCC(ret) && OB_FAIL(set_exchange_cnt_post(adjust_sort_ctx))) {
          LOG_WARN("failed to reset top exchange", K(ret));
        }
        if (adjust_sort_ctx->has_exchange_) {
          ret = adjust_sort_operator(adjust_sort_ctx);
        }
        OC((set_sort_topn)());
        if (OB_SUCC(ret) && LOG_LIMIT == get_type() && static_cast<ObLogLimit*>(this)->get_limit_percent() != NULL) {
          if (OB_ISNULL(get_child(first_child))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Limit's child not expected to be NULL", K(ret));
          } else if (get_child(first_child)->get_type() == LOG_MATERIAL ||
                     (get_child(first_child)->get_type() == LOG_SORT && get_child(first_child)->is_block_op())) {
            /*do nothing*/
          } else if (OB_FAIL(allocate_material_below(0))) {
            LOG_WARN("failed to allocate material below", K(ret));
          }
        }
        break;
      }
      case RE_CALC_OP_COST: {
        if (OB_FAIL(this->re_calc_cost())) {
          LOG_WARN("failed to re calc cost", K(ret));
        }
        break;
      }
      case ALLOC_DUMMY_OUTPUT: {
        ret = allocate_dummy_output_access();
        break;
      }
      case ALLOC_MONITORING_DUMP: {
        if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Ctx is null", K(ret));
        } else if (OB_FAIL(alloc_md_post(*static_cast<AllocMDContext*>(ctx)))) {
          LOG_WARN("Failed to alloc monitroing dump operator", K(ret));
        }
        break;
      }
      case OPERATOR_NUMBERING: {
        if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx is null", K(ret));
        } else {
          numbering_operator_post(*static_cast<NumberingCtx*>(ctx));
        }
        break;
      }
      case EXCHANGE_NUMBERING: {
        if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx is null", K(ret));
        } else if (OB_FAIL(numbering_exchange_post(*static_cast<NumberingExchangeCtx*>(ctx)))) {
          LOG_WARN("fail numbering exchange post", K(ret));
        }
        break;
      }
      case GEN_SIGNATURE: {
        if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx is null", K(ret));
        } else {
          uint64_t* seed = reinterpret_cast<uint64_t*>(ctx);
          *seed = hash(*seed);
          LOG_TRACE("", "operator", get_name(), "hash_value", *seed);
        }
        break;
      }
      case GEN_LOCATION_CONSTRAINT: {
        ret = gen_location_constraint(ctx);
        break;
      }
      case REORDER_PROJECT_COLUMNS: {
        ret = reordering_project_columns();
        break;
      }
      case EXPLAIN_COLLECT_WIDTH: {
        explain_collect_width_post(ctx);
        break;
      }
      case EXPLAIN_WRITE_BUFFER: {
        explain_write_buffer_post(ctx);
        break;
      }
      case EXPLAIN_WRITE_BUFFER_OUTPUT: {
        break;
      }
      case EXPLAIN_WRITE_BUFFER_OUTLINE: {
        reset_outline_state();
        break;
      }
      case EXPLAIN_INDEX_SELECTION_INFO: {
        break;
      }
      case PX_ESTIMATE_SIZE: {
        ret = px_estimate_size_factor_post();
        break;
      }
      case CG_PREPARE: {
        break;
      }
      case ALLOC_LINK: {
        if (OB_FAIL(allocate_link_post())) {
          LOG_WARN("failed to allocate link post", K(ret));
        }
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

int ObLogicalOperator::do_plan_tree_traverse(const TraverseOp& operation, void* ctx)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(do_pre_traverse_operation(operation, ctx))) {
    LOG_WARN("failed to perform traverse operation", K(ret), "operator", get_name(), K(operation));
  } else {
    LOG_TRACE("succ to perform pre traverse operation", "operator", get_name(), K(operation));
  }

  if (!(ALLOC_EXCH == operation && LOG_LINK == get_type()) &&
      !(GEN_LINK_STMT == operation && 0 != get_dblink_id() &&
          (LOG_SET == get_type() || LOG_SUBPLAN_SCAN == get_type()))) {
    // top-down traverse all our children
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_child(i) is null", K(ret), K(i));
      } else if (OB_FAIL(SMART_CALL(get_child(i)->do_plan_tree_traverse(operation, ctx)))) {
        LOG_WARN("failed to bottom-up traverse operator", "operator", get_name(), K(operation), K(ret));
      } else { /*do nothing*/
      }
    }
  }

  // post_traverse_operation
  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_post_traverse_operation(operation, ctx))) {
      LOG_WARN("failed to perform post traverse action", K(ret), K(operation));
    } else {
      LOG_TRACE("succ to perform post traverse action", K(operation), "operator", get_name());
    }
  } else { /*do nothing*/
  }

  return ret;
}

int ObLogicalOperator::should_allocate_gi_for_dml(bool& is_valid)
{
  int ret = OB_SUCCESS;
  if (LOG_JOIN == get_type() || LOG_SET == get_type() || !get_expected_ordering().empty()) {
    is_valid = false;
  } else if (LOG_INSERT == get_type()) {
    const ObLogInsert* log_insert = static_cast<const ObLogInsert*>(this);
    if (!log_insert->is_multi_part_dml()) {
      is_valid = false;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(get_child(i)->should_allocate_gi_for_dml(is_valid))) {
      LOG_WARN("failed to check should allocate gi for dml", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogicalOperator::set_sort_topn()
{
  int ret = OB_SUCCESS;
  ObRawExpr* limit_count_expr = NULL;
  ObRawExpr* limit_offset_expr = NULL;
  ObLogSort* op_sort = NULL;
  ObOptimizerContext* optm_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  bool is_fetch_with_ties = false;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(optm_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(session = optm_ctx->get_session_info())) {
    LOG_WARN("get unexpected null", K(get_plan()), K(optm_ctx), K(session), K(ret));
  } else if (LOG_LIMIT == get_type()) {
    bool need_calc = false;
    ObLogLimit* op_limit = static_cast<ObLogLimit*>(this);
    is_fetch_with_ties = op_limit->is_fetch_with_ties();
    if (OB_FAIL(op_limit->need_calc_found_rows(need_calc))) {
      LOG_WARN("call need_calc_found_rows failed", K(ret));
    } else if (need_calc) {
      /*do nothing*/
    } else {
      limit_count_expr = op_limit->get_limit_count();
      limit_offset_expr = op_limit->get_limit_offset();
    }
  } else if (LOG_COUNT == get_type() && get_filter_exprs().count() == 0) {
    limit_count_expr = static_cast<ObLogCount*>(this)->get_rownum_limit_expr();
  } else { /*do nothing*/
  }

  // locate sort operator
  if (OB_SUCC(ret) && NULL != limit_count_expr) {
    ObLogicalOperator* current_op = this->get_child(first_child);
    while (NULL != current_op && NULL == op_sort) {
      if (LOG_SORT == current_op->get_type()) {
        op_sort = static_cast<ObLogSort*>(current_op);
      } else if (LOG_SUBPLAN_SCAN == current_op->get_type() && 0 == current_op->get_filter_exprs().count()) {
        current_op = current_op->get_child(first_child);
      } else {
        current_op = NULL;
      }
    }
  }
  // push down the limit into sort
  if (OB_SUCC(ret) && NULL != op_sort && NULL != limit_count_expr) {
    int64_t limit_count = 0;
    int64_t limit_offset = 0;
    bool is_null_value = false;
    if (OB_FAIL(ObTransformUtils::get_limit_value(limit_count_expr,
            get_plan()->get_stmt(),
            get_plan()->get_optimizer_context().get_params(),
            optm_ctx->get_session_info(),
            &optm_ctx->get_allocator(),
            limit_count,
            is_null_value))) {
      LOG_WARN("failed to get limit count num", K(ret));
    } else if (!is_null_value && OB_FAIL(ObTransformUtils::get_limit_value(limit_offset_expr,
                                     get_plan()->get_stmt(),
                                     get_plan()->get_optimizer_context().get_params(),
                                     optm_ctx->get_session_info(),
                                     &optm_ctx->get_allocator(),
                                     limit_offset,
                                     is_null_value))) {
      LOG_WARN("failed to get limit offset num", K(ret));
    } else {
      limit_count = is_null_value ? 0 : limit_count + limit_offset;
      LOG_TRACE("sort rows count: get_card()", K(op_sort->get_card()));
      if (static_cast<double>(limit_count) < op_sort->get_card() * TOPN_ROWS_RATIO || limit_count < TOPN_LIMIT_COUNT) {
        ObRawExpr* pushdown_limit_count = NULL;
        OZ(ObTransformUtils::make_pushdown_limit_count(
            optm_ctx->get_expr_factory(), *session, limit_count_expr, limit_offset_expr, pushdown_limit_count));
        if (OB_SUCC(ret)) {
          op_sort->set_topn_count(pushdown_limit_count);
          op_sort->set_fetch_with_ties(is_fetch_with_ties);
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::to_json(char* buf, const int64_t buf_len, int64_t& pos, json::Value*& ret_val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_plan()));
  } else {
    ObIAllocator* allocator = &get_plan()->get_allocator();

    // this operator
    ret_val = NULL;
    json::Pair* id = NULL;
    json::Pair* op = NULL;
    json::Pair* name = NULL;
    json::Pair* rows = NULL;
    json::Pair* cost = NULL;
    //    json::Pair *op_cost = NULL;
    json::Pair* output = NULL;

    Value* id_value = NULL;
    Value* op_value = NULL;
    Value* name_value = NULL;
    Value* rows_value = NULL;
    Value* cost_value = NULL;
    //    Value *op_cost_value = NULL;
    Value* output_value = NULL;
    if (OB_ISNULL(ret_val = (Value*)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(id = (Pair*)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(op = (Pair*)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(name = (Pair*)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(rows = (Pair*)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(cost = (Pair*)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } /*else if (OB_ISNULL(op_cost = (Pair *)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    }*/
    else if (OB_ISNULL(output = (Pair*)allocator->alloc(sizeof(Pair)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(id_value = (Value*)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(op_value = (Value*)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(name_value = (Value*)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(rows_value = (Value*)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(cost_value = (Value*)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } /*else if (OB_ISNULL(op_cost_value = (Value *)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } */
    else if (OB_ISNULL(output_value = (Value*)allocator->alloc(sizeof(Value)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    } else if (OB_ISNULL(ret_val = new (ret_val) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(id = new (id) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(op = new (op) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(name = new (name) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(rows = new (rows) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(cost = new (cost) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } /*else if (OB_ISNULL(op_cost = new(op_cost) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    }*/
    else if (OB_ISNULL(output = new (output) Pair())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Pair");
    } else if (OB_ISNULL(id_value = new (id_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(op_value = new (op_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(name_value = new (name_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(rows_value = new (rows_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else if (OB_ISNULL(cost_value = new (cost_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } /*else if (OB_ISNULL(op_cost_value = new (op_cost_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json value");
    }*/
    else if (OB_ISNULL(output_value = new (output_value) Value())) {
      ret = OB_ERROR;
      LOG_WARN("failed to new json Value");
    } else {
      ret_val->set_type(JT_OBJECT);
      id_value->set_type(JT_NUMBER);
      // TBD
      id_value->set_int(id_);
      id->name_ = ID;
      id->value_ = id_value;

      op_value->set_type(JT_STRING);
      // TBD
      op_value->set_string(const_cast<char*>(this->get_name()), static_cast<int32_t>(strlen(get_name())));
      op->name_ = OPERATOR;
      op->value_ = op_value;

      name_value->set_type(JT_STRING);
      // TBD
      name_value->set_string(const_cast<char*>(this->get_name()), static_cast<int32_t>(strlen(get_name())));
      name->name_ = NAME;
      name->value_ = name_value;

      rows_value->set_type(JT_NUMBER);
      // TBD
      rows_value->set_int(static_cast<int>(get_card()));
      rows->name_ = ROWS;
      rows->value_ = rows_value;

      cost_value->set_type(JT_NUMBER);
      // TBD
      cost_value->set_int(static_cast<int>(cost_));
      cost->name_ = COST;
      cost->value_ = cost_value;

      //      op_cost_value->set_type(JT_NUMBER);
      //      op_cost_value->set_int(static_cast<int>(op_cost_));
      //      op_cost->name_ = OPCOST;
      //      op_cost->value_ = op_cost_value;
      // output expressions
      int64_t N = output_exprs_.count();
      if (0 < N) {
        output_value->set_type(JT_ARRAY);
        Value* expr_value = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
          if (OB_ISNULL(expr_value = (Value*)allocator->alloc(sizeof(Value)))) {
            // best effort
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("failed to allocate json value");
          } else if (OB_ISNULL(expr_value = new (expr_value) Value())) {
            ret = OB_ERROR;
            LOG_WARN("failed to new json Value");
          } else if (OB_ISNULL(output_exprs_.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("output_exprs_.at(i) is unexpected null", K(ret), K(i));
          } else {
            // generate string for expr
            int64_t pos_prev = pos;
            int64_t tmp_pos = 0;
            if (OB_FAIL(output_exprs_.at(i)->get_name(buf + pos_prev, buf_len - pos_prev, tmp_pos))) {
              LOG_WARN("failed to get_name", K(ret));
            } else {
              pos = pos_prev + tmp_pos;
              // set value
              expr_value->set_type(JT_STRING);
              expr_value->set_string(buf + pos_prev, static_cast<int32_t>(pos - pos_prev));
              output_value->array_add(expr_value);
            }
          }
        }
      } else {
        output_value->set_type(JT_STRING);
        LOG_TRACE("", K(pos));
        if (OB_FAIL(BUF_PRINTF("nil"))) {
          /* Do Nothing */
        } else {
          output_value->set_string(buf + pos, (int)strlen("nil"));
          LOG_TRACE("", K(pos));
          LOG_TRACE("", K(pos));
        }
      }

      if (OB_SUCC(ret)) {
        output->name_ = "output";
        output->value_ = output_value;
        ret_val->object_add(id);
        ret_val->object_add(op);
        ret_val->object_add(name);
        ret_val->object_add(rows);
        ret_val->object_add(cost);
        //        ret_val->object_add(op_cost);
        ret_val->object_add(output);
        // child operator
        int64_t num_of_child = get_num_of_child();
        Pair* child = NULL;
        const uint64_t OB_MAX_JSON_CHILD_NAME_LENGTH = 64;
        char name_buf[OB_MAX_JSON_CHILD_NAME_LENGTH];
        int64_t name_buf_size = OB_MAX_JSON_CHILD_NAME_LENGTH;
        for (int64_t i = 0; OB_SUCC(ret) && i < num_of_child; ++i) {
          int64_t child_name_pos = snprintf(name_buf, name_buf_size, "CHILD_%ld", i + 1);
          ObString child_name(child_name_pos, name_buf);
          if (OB_ISNULL(child = (Pair*)allocator->alloc(sizeof(Pair)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("no memory");
          } else if (OB_ISNULL(child = new (child) Pair())) {
            ret = OB_ERROR;
            LOG_WARN("failed to new json Pair");
          } else if (OB_ISNULL(get_child(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get_child(i) returns null", K(ret), K(i));
          } else if (OB_FAIL(ob_write_string(*allocator, child_name, child->name_))) {
            LOG_WARN("failed to write string", K(ret));
            /* Do nothing */
          } else if (OB_FAIL(get_child(i)->to_json(buf, buf_len, pos, child->value_))) {
            LOG_WARN("to_json fails", K(ret), K(i));
          } else {
            ret_val->object_add(child);
          }
        }
      } else { /* Do nothing */
      }
    }
  }

  return ret;
}

int ObLogicalOperator::mark_expr_produced(
    ObRawExpr* expr, uint64_t branch_id, uint64_t producer_id, ObAllocExprContext& gen_expr_ctx, bool& found)
{
  int ret = OB_SUCCESS;
  ExprProducer* expr_producer = NULL;
  found = false;
  if (OB_ISNULL(expr) || OB_INVALID_ID == branch_id || OB_INVALID_ID == producer_id) {
    ret = OB_ERROR;
    LOG_WARN("Get invalid id", K(ret), K(branch_id), K(producer_id));
  } else if (OB_FAIL(gen_expr_ctx.find(expr, expr_producer))) {
    LOG_WARN("failed to find expr producer", K(ret));
  } else if (NULL != expr_producer) {
    // mark as produced
    found = true;
    if (expr_producer->producer_branch_ == OB_INVALID_ID) {
      expr_producer->producer_branch_ = branch_id;
      expr_producer->producer_id_ = producer_id;
      LOG_TRACE("expr is marked as produced.", K(expr_producer->expr_), K(branch_id), K(expr_producer->consumer_id_));
    }
  } else {
    ExprProducer new_expr_producer(expr, id_);
    new_expr_producer.producer_branch_ = branch_id;
    new_expr_producer.producer_id_ = producer_id;
    ret = gen_expr_ctx.add(new_expr_producer);
    LOG_TRACE("not found, push_back(expr_producer)", "name", get_name(), K(new_expr_producer));
  }
  return ret;
}

int ObLogicalOperator::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_exprs_to_ctx(ctx, get_filter_exprs()))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else {
    LOG_TRACE("succeed to allocate expr pre", K(id_), K(get_name()));
  }
  return ret;
}

int ObLogicalOperator::reordering_project_columns()
{
  return OB_SUCCESS;
}

int ObLogicalOperator::get_next_producer_id(ObLogicalOperator* node, uint64_t& producer_id)
{
  int ret = OB_SUCCESS;
  producer_id = OB_INVALID_ID;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    bool is_find = false;
    while (NULL != node && !is_find) {
      if (IS_EXPR_PASSBY_OPER(node->get_type())) {
        node = node->get_child(first_child);
      } else {
        is_find = true;
        producer_id = node->get_operator_id();
      }
    }
    if (OB_SUCC(ret) && !is_find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not find next producer id", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::check_param_expr_should_be_added(const ObRawExpr* param_expr, bool& should_add)
{
  int ret = OB_SUCCESS;
  should_add = false;
  if (OB_ISNULL(param_expr) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(param_expr), K(get_plan()));
  } else if (param_expr->has_flag(IS_PARAM)) {
    if (ObOptimizerUtil::is_param_expr_correspond_subquey(
            *(static_cast<const ObConstRawExpr*>(param_expr)), get_plan()->get_onetime_exprs())) {
      should_add = true;
    }
  } else if (param_expr->has_flag(IS_CONST) || param_expr->has_flag(IS_USER_VARIABLE)) {
    should_add = false;
  } else {
    should_add = true;
  }
  return ret;
}

int ObLogicalOperator::add_exprs_to_ctx(ObAllocExprContext& ctx, const ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  uint64_t producer_id = OB_INVALID_ID;
  if (OB_FAIL(get_next_producer_id(this, producer_id))) {
    LOG_WARN("failed to get next producer id", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, exprs, producer_id))) {
    LOG_WARN("failed to add exprs to ctx", K(exprs), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::add_exprs_to_ctx(
    ObAllocExprContext& ctx, const ObIArray<ObRawExpr*>& input_exprs, uint64_t producer_id)
{
  int ret = OB_SUCCESS;
  ret = add_exprs_to_ctx(ctx, input_exprs, producer_id, id_);
  return ret;
}

int ObLogicalOperator::add_exprs_to_ctx(
    ObAllocExprContext& ctx, const ObIArray<ObRawExpr*>& input_exprs, uint64_t producer_id, uint64_t consumer_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 32> shared_exprs;
  ObSEArray<ObRawExpr*, 32> fix_producer_exprs;
  if (OB_FAIL(extract_specific_exprs(input_exprs, &ctx.expr_producers_, fix_producer_exprs, shared_exprs))) {
    LOG_WARN("failed to extract column input_exprs", K(ret));
  } else {
    ObRawExpr* raw_expr = NULL;
    ExprProducer* raw_producer = NULL;
    // for fix producer exprs, set consumer id, but not set producer id
    for (int64_t i = 0; OB_SUCC(ret) && i < fix_producer_exprs.count(); i++) {
      if (OB_ISNULL(raw_expr = fix_producer_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ctx.find(raw_expr, raw_producer))) {
        LOG_WARN("failed to find expr producer", K(ret));
      } else if (NULL != raw_producer) {
        if (OB_INVALID_ID == raw_producer->consumer_id_) {
          raw_producer->consumer_id_ = consumer_id;
        } else {
          raw_producer->consumer_id_ = std::max(raw_producer->consumer_id_, consumer_id);
        }
        LOG_TRACE("succeed to update consumer id for fix producer exprs",
            K(*raw_expr),
            K(raw_producer->producer_id_),
            K(consumer_id),
            K(get_name()));
      } else {
        ExprProducer expr_producer(raw_expr, consumer_id);
        if (OB_FAIL(ctx.add(expr_producer))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else {
          LOG_TRACE("succeed to add fix producer exprs",
              K(*raw_expr),
              K(producer_id),
              K(consumer_id),
              K(expr_producer),
              K(get_name()));
        }
      }
    }
    // add shared exprs, should set both producer id and consumer id
    for (int64_t i = 0; OB_SUCC(ret) && i < shared_exprs.count(); i++) {
      uint64_t consumer_id = OB_INVALID_ID;
      if (OB_ISNULL(raw_expr = shared_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ctx.find(raw_expr, raw_producer))) {
        LOG_WARN("failed to find raw expr producer", K(ret));
      } else if (NULL != raw_producer) {
        // update the raw_producer id
        raw_producer->is_shared_ = true;
        if (OB_INVALID_ID == raw_producer->producer_id_) {
          raw_producer->producer_id_ = producer_id;
        } else {
          raw_producer->producer_id_ = std::min(raw_producer->producer_id_, producer_id);
        }
        LOG_TRACE("succeed to update shared expr producer id",
            K(raw_expr),
            K(*raw_expr),
            K(producer_id),
            KPC(raw_producer),
            K(get_name()));
      } else if (OB_FAIL(find_consumer_id_for_shared_expr(&ctx.expr_producers_, raw_expr, consumer_id))) {
        LOG_WARN("failed to find sharable expr consumer id", K(ret));
      } else if (OB_INVALID_ID == consumer_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected conusmer id", K(*raw_expr), K(ret));
      } else {
        ExprProducer expr_producer(raw_expr, consumer_id, producer_id);
        expr_producer.is_shared_ = true;
        if (OB_FAIL(ctx.add(expr_producer))) {
          LOG_WARN("failed to push balck raw_expr", K(ret));
        } else {
          LOG_TRACE("succeed to add shared exprs",
              K(raw_expr),
              K(*raw_expr),
              K(producer_id),
              K(expr_producer.consumer_id_),
              K(consumer_id),
              K(expr_producer),
              K(get_name()));
        }
      }
    }
    // add input expressions, should set both producer and consumer id
    for (int64_t i = 0; OB_SUCC(ret) && i < input_exprs.count(); i++) {
      if (OB_ISNULL(raw_expr = input_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (ObOptimizerUtil::find_item(fix_producer_exprs, raw_expr) ||
                 ObOptimizerUtil::find_item(shared_exprs, raw_expr)) {
        /*do nothing*/
      } else if (OB_FAIL(ctx.find(raw_expr, raw_producer))) {
        LOG_WARN("failed to raw expr producer", K(ret));
      } else if (NULL != raw_producer) {
        // update the raw_producer id
        if (OB_ISNULL(raw_producer)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          raw_producer->producer_id_ = producer_id;
          LOG_TRACE("succeed to update input expr producer id",
              K(raw_expr),
              K(*raw_expr),
              K(producer_id),
              K(consumer_id),
              KPC(raw_producer),
              K(get_name()));
        }
      } else {
        ExprProducer expr_producer(raw_expr, consumer_id, producer_id);
        if (OB_FAIL(ctx.add(expr_producer))) {
          LOG_WARN("failed to push balck raw_expr", K(ret));
        } else {
          LOG_TRACE("succeed to add input exprs",
              K(raw_expr),
              K(*raw_expr),
              K(producer_id),
              K(expr_producer.consumer_id_),
              K(consumer_id),
              K(expr_producer),
              K(get_name()));
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::add_exprs_to_ctx(ObAllocExprContext& ctx, const ObIArray<ObColumnRefRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr* raw_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
    ExprProducer* raw_producer = NULL;
    if (OB_ISNULL(raw_expr = column_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ctx.find(raw_expr, raw_producer))) {
      LOG_WARN("failed to find raw producer", K(ret));
    } else if (NULL != raw_producer) {
      // find, do nothing
    } else {
      ExprProducer expr_producer(raw_expr, id_);
      if (OB_FAIL(ctx.add(expr_producer))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        LOG_TRACE("succeed to add column exprs", K(raw_expr), K(*raw_expr), K(id_), K(id_));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::add_expr_to_output(const ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  bool found = false;
  int64_t N = output_exprs_.count();
  for (int64_t i = 0; !found && i < N; i++) {
    if (output_exprs_.at(i) == expr) {
      found = true;
    } else { /* Do nothing */
    }
  }
  if (!found && OB_FAIL(output_exprs_.push_back(const_cast<ObRawExpr*>(expr)))) {
    LOG_WARN("failed to add expr to output", K(expr), K(*expr), K(id_), K(get_name()));
  } else {
    LOG_TRACE("succ to add expr to output", K(expr), K(*expr), K(id_), K(get_name()), K(found));
  }
  return ret;
}

int ObLogicalOperator::extract_specific_exprs(const ObIArray<ObRawExpr*>& exprs, ObIArray<ExprProducer>* ctx,
    ObIArray<ObRawExpr*>& fix_producer_exprs, ObIArray<ObRawExpr*>& shared_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(extract_specific_exprs(exprs.at(i), ctx, fix_producer_exprs, shared_exprs))) {
      LOG_WARN("failed to extract fix producer exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogicalOperator::extract_specific_exprs(ObRawExpr* raw_expr, ObIArray<ExprProducer>* ctx,
    ObIArray<ObRawExpr*>& fix_producer_exprs, ObIArray<ObRawExpr*>& shared_exprs)
{
  int ret = OB_SUCCESS;
  bool change_ctx = false;
  if (OB_ISNULL(raw_expr) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(raw_expr), K(ctx), K(ret));
  } else if (is_fix_producer_expr(*raw_expr)) {
    if (OB_FAIL(add_var_to_array_no_dup(fix_producer_exprs, raw_expr))) {
      LOG_WARN("failed to add expr", K(ret));
    } else {
      change_ctx = true;
    }
  } else if (!raw_expr->has_const_or_const_expr_flag() && is_shared_expr(ctx, raw_expr)) {
    if (OB_FAIL(add_var_to_array_no_dup(shared_exprs, raw_expr))) {
      LOG_WARN("failed to add expr", K(ret));
    } else {
      change_ctx = true;
    }
  } else { /*do nothing*/
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ExprProducer, 8> temp_ctx;
    ObIArray<ExprProducer>* new_ctx = ctx;
    if (change_ctx) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx->count(); i++) {
        if (ctx->at(i).expr_ == raw_expr) {
          /*do nothing*/
        } else {
          ret = temp_ctx.push_back(ctx->at(i));
        }
      }
      if (OB_SUCC(ret)) {
        new_ctx = &temp_ctx;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); ++i) {
      ret = SMART_CALL(extract_specific_exprs(raw_expr->get_param_expr(i), new_ctx, fix_producer_exprs, shared_exprs));
    }
  }
  return ret;
}

bool ObLogicalOperator::is_fix_producer_expr(const ObRawExpr& expr)
{
  bool ret = false;
  if (expr.has_flag(IS_AGG) || expr.has_flag(IS_WINDOW_FUNC) || expr.has_flag(IS_ROWNUM) || expr.has_flag(IS_SET_OP) ||
      expr.has_flag(IS_COLUMN) || expr.has_flag(IS_SEQ_EXPR) || (T_PDML_PARTITION_ID == expr.get_expr_type()) ||
      (expr.has_flag(IS_PARAM) && ObOptimizerUtil::is_param_expr_correspond_subquey(
                                      static_cast<const ObConstRawExpr&>(expr), get_plan()->get_onetime_exprs())) ||
      expr.has_flag(IS_SUB_QUERY) || expr.has_flag(IS_CONNECT_BY_ROOT) || expr.has_flag(IS_SYS_CONNECT_BY_PATH) ||
      expr.has_flag(IS_PRIOR) ||
      (expr.has_flag(IS_PSEUDO_COLUMN) &&
          static_cast<const ObPseudoColumnRawExpr&>(expr).is_hierarchical_query_type())) {
    ret = true;
  }
  return ret;
}

bool ObLogicalOperator::is_shared_expr(const ObIArray<ExprProducer>* ctx, const ObRawExpr* expr)
{
  bool bret = false;
  if (NULL != ctx) {
    for (int64_t i = 0; !bret && i < ctx->count(); i++) {
      bret = ObOptimizerUtil::is_point_based_sub_expr(expr, ctx->at(i).expr_);
    }
  }
  return bret;
}

int ObLogicalOperator::find_consumer_id_for_shared_expr(
    const ObIArray<ExprProducer>* ctx, const ObRawExpr* expr, uint64_t& consumer_id)
{
  int ret = OB_SUCCESS;
  consumer_id = OB_INVALID_ID;
  if (OB_ISNULL(ctx) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx->count(); i++) {
      if (OB_ISNULL(ctx->at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (ObOptimizerUtil::is_point_based_sub_expr(expr, ctx->at(i).expr_)) {
        if (OB_INVALID_ID == consumer_id) {
          consumer_id = ctx->at(i).consumer_id_;
        } else {
          consumer_id = std::max(consumer_id, ctx->at(i).consumer_id_);
        }
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObIArray<ExprProducer>& producers = ctx.expr_producers_;
  bool produced = false;
  do {
    produced = false;
    int64_t N = producers.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (id_ >= producers.at(i).producer_id_ || OB_INVALID_ID == producers.at(i).producer_id_) {
        // producer_id_ is valid means already produced or assigned to other op
        const ObRawExpr* expr = producers.at(i).expr_;
        if (OB_INVALID_ID == producers.at(i).producer_branch_) {  // not produced yet
          LOG_PRINT_EXPR(DEBUG, "try to produce expr", producers.at(i).expr_, "Operator", get_name());
          bool can_be_produced_tmp = false;
          if (OB_FAIL(expr_can_be_produced(expr, ctx, can_be_produced_tmp))) {
            LOG_WARN("expr_can_be_produced fails", K(ret));
          } else if (can_be_produced_tmp) {
            /**
             * Althouth I can produce it, but I don't produce it, unless the customer is myself or the
             * expr is my output.
             * And I am a root, my output is fixed, so I must not add anything to my output.
             */
            if (is_plan_root()) {
              if (producers.at(i).consumer_id_ == id_ || ObOptimizerUtil::find_item(output_exprs_, expr) ||
                  (get_type() == LOG_GROUP_BY && static_cast<ObLogGroupBy*>(this)->is_my_aggr_expr(expr))) {
                producers.at(i).producer_branch_ = branch_id_;
                producers.at(i).producer_id_ = id_;
                LOG_PRINT_EXPR(DEBUG, "mark expr as produced by me", producers.at(i).expr_, "Operator", get_name());
              } else {
                LOG_PRINT_EXPR(
                    DEBUG, "skip expr, let other guys produce", producers.at(i).expr_, "Operator", get_name());
              }
            } else {
              // produced by me
              produced = true;
              producers.at(i).producer_branch_ = branch_id_;
              producers.at(i).producer_id_ = id_;
              LOG_PRINT_EXPR(DEBUG, "mark expr as produced by me", producers.at(i).expr_, "Operator", get_name());
              if (producers.at(i).consumer_id_ > id_) {
                // add to output
                ret = add_expr_to_output(expr);
              } else { /*do nothing*/
              }
            }
          } else {
            LOG_PRINT_EXPR(DEBUG, "expr can not be produced", producers.at(i).expr_, "Operator", get_name());
          }
        } else {
          if (producers.at(i).consumer_id_ > id_) {
            bool has_input = has_expr_as_input(expr);
            if (has_input && !is_plan_root()) {
              if (OB_FAIL(add_expr_to_output(expr))) {
                LOG_WARN("failed to add expr to output", K(ret));
              } else {
                LOG_PRINT_EXPR(DEBUG,
                    "expr is already produced, added into output expr",
                    expr,
                    "Operator",
                    get_name(),
                    "Is Root",
                    is_plan_root(),
                    "Is Input",
                    has_input,
                    "Consumer id",
                    producers.at(i).consumer_id_,
                    "Id",
                    id_);
              }
            } else {
              LOG_PRINT_EXPR(DEBUG,
                  "expr is already produced, but not added into output expr",
                  expr,
                  "Operator",
                  get_name(),
                  "Is Root",
                  is_plan_root(),
                  "Is Input",
                  has_input,
                  "Consumer id",
                  producers.at(i).consumer_id_,
                  "Id",
                  id_);
            }
          } else {
            LOG_PRINT_EXPR(DEBUG,
                "expr is already produced, but not added into output expr",
                expr,
                "Operator",
                get_name(),
                "Is Root",
                is_plan_root(),
                "Consumer id",
                producers.at(i).consumer_id_,
                "Id",
                id_);
          }
        }
      } else { /* Do nothing */
      }
    }
  } while (OB_SUCC(ret) && produced);
  if (OB_SUCC(ret)) {
    // reorder all the filter exprs according to their selectivities
    ret = reorder_filter_exprs();
  } else { /* Do nothing */
  }

  if (OB_SUCC(ret) && OB_FAIL(append_not_produced_exprs(ctx.not_produced_exprs_))) {
    LOG_WARN("fail to append not produced exprs", K(ret));
  }

  return ret;
}

int ObLogicalOperator::expr_can_be_produced(
    const ObRawExpr* expr, ObAllocExprContext& gen_expr_ctx, bool& can_be_produced)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr) || OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(expr), K(get_plan()), K(stmt));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    can_be_produced = true;
    LOG_TRACE(
        "checking if expr can be produced", K(expr), K(expr->get_expr_class()), K(get_name()), K(id_), K(branch_id_));
    if (expr->has_flag(IS_PARAM)) {
      can_be_produced = true;
    } else if (expr->is_set_op_expr()) {
      // does not rely on other exprs
      can_be_produced = true;
    } else if (expr->is_op_expr() || expr->is_case_op_expr() || expr->is_aggr_expr() || expr->is_sys_func_expr() ||
               expr->is_udf_expr() || expr->is_win_func_expr()) {
      // For ObOpRawExpr and ObAggRawExpr, we check if their inputs has been produced
      const ObRawExpr* param_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && can_be_produced && i < expr->get_param_count(); i++) {
        param_expr = expr->get_param_expr(i);
        LOG_TRACE("checking if input expr has been produced", K(param_expr), K(get_name()));
        bool has_been_produced = false;
        if (OB_ISNULL(param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param_expr is null", K(ret));
        } else if (OB_FAIL(expr_has_been_produced(param_expr, gen_expr_ctx, has_been_produced))) {
          LOG_WARN("failed to check whether expr_has_been_produced", K(ret));
        } else if ((param_expr->has_generalized_column() ||
                       param_expr->has_flag(CNT_PARAM))  // check if param_expr is param
                   && !has_been_produced) {
          // expressions that do not contain columns are considered to be const and produced
          bool can_be_produced_flag2 = false;
          if (OB_FAIL(SMART_CALL(expr_can_be_produced(param_expr, gen_expr_ctx, can_be_produced_flag2)))) {
            LOG_WARN("expr_can_be_produced fails", K(ret));
          } else if (!can_be_produced_flag2) {
            // if any of their inputs is not produced yet, the expr cannot be produced
            can_be_produced = false;
            LOG_TRACE("input expr has not yet been produced",
                K(param_expr),
                K(*param_expr),
                K(get_name()),
                K(id_),
                K(branch_id_));
          } else {
            LOG_TRACE("input expr not yet produced, but can be",
                K(param_expr),
                K(*param_expr),
                K(get_name()),
                K(id_),
                K(branch_id_));
          }
        } else {
          LOG_TRACE("input expr has been produced", K(param_expr), K(get_name()), K(id_), K(branch_id_));
        }
      }
    } else if (expr->is_query_ref_expr()) {
      // only subplan filter can produce Unary Expr
      const ObQueryRefRawExpr* query_expr = static_cast<const ObQueryRefRawExpr*>(expr);
      const ObLogicalOperator* ref_op = query_expr->get_ref_operator();
      if (LOG_SUBPLAN_FILTER == get_type() && stmt->get_current_level() == query_expr->get_expr_level()) {
        can_be_produced = static_cast<ObLogSubPlanFilter*>(this)->is_my_subquery_expr(query_expr);
        if (can_be_produced) {
          LOG_TRACE("subquery expr can be produced", K(get_type()), K(id_), K(branch_id_));
        } else {
          LOG_TRACE("subquery expr can not be produced", K(get_type()), K(id_), K(branch_id_));
        }

      } else {
        can_be_produced = false;
        LOG_TRACE(
            "subquery expr can only be produced by subplan filter operator", K(get_type()), K(id_), K(branch_id_));
      }
    } else if (!(expr->has_flag(CNT_COLUMN) || expr->has_flag(CNT_AGG))) {
      // expressions that do not contain columns are considered to be const and produced
      can_be_produced = true;
      LOG_TRACE(
          "const expr can always be produced", "operator", get_name(), "operator_id", id_, "branch_id", branch_id_);
    } else {
      // for other expressions(ie. column), if it has not been produced, then
      // its producer has not been visited yet.
      can_be_produced = false;
    }
  }

  // only count operator can produce rownum expr
  // only group-by can produce aggregation function
  // only set operator can produce set expression
  // only window_function operator can produce window function
  // only connect_by operator can produce hierarchical query pseudo column
  // only sequence operator can produce sequence expr
  // only light granule operator can produce stmt_id() expr
  if (OB_SUCC(ret) && can_be_produced) {
    if (!expr->has_flag(IS_ROWNUM)) {
    } else if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2250 && LOG_COUNT != get_type()) {
      can_be_produced = false;
    } else if (LOG_COUNT != get_type()) {
      ExprProducer* expr_producer = NULL;
      if (OB_FAIL(gen_expr_ctx.find(expr, expr_producer))) {
        LOG_WARN("failed to find expr producer", K(ret));
      } else if (NULL == expr_producer) {
        // do nothing
      } else if (expr_producer->consumer_id_ != id_) {
        can_be_produced = false;
      }
    }
    if (expr->has_flag(IS_SEQ_EXPR) && LOG_SEQUENCE != get_type()) {
      can_be_produced = false;
    } else if ((T_OP_CONNECT_BY_ROOT == expr->get_expr_type() || T_FUN_SYS_CONNECT_BY_PATH == expr->get_expr_type() ||
                   (expr->is_pseudo_column_expr() &&
                       static_cast<const ObPseudoColumnRawExpr*>(expr)->is_hierarchical_query_type())) &&
               (LOG_JOIN != get_type() || CONNECT_BY_JOIN != static_cast<const ObLogJoin*>(this)->get_join_type())) {
      can_be_produced = false;
    } else if (expr->is_aggr_expr() && LOG_GROUP_BY != get_type()) {
      can_be_produced = false;
    } else if (expr->is_aggr_expr() && LOG_GROUP_BY == get_type()) {  // not aggr of mine
      can_be_produced = static_cast<ObLogGroupBy*>(this)->is_my_aggr_expr(expr);
    } else if (expr->has_flag(IS_SET_OP) && LOG_SET != get_type()) {
      can_be_produced = false;
    } else if (expr->has_flag(IS_SET_OP) && LOG_SET == get_type()) {
      ret = static_cast<ObLogSet*>(this)->is_my_set_expr(expr, can_be_produced);
    } else if (expr->has_flag(IS_WINDOW_FUNC) && LOG_WINDOW_FUNCTION != get_type()) {
      can_be_produced = false;
    } else if (expr->has_flag(IS_WINDOW_FUNC) && LOG_WINDOW_FUNCTION == get_type()) {
      ret = static_cast<ObLogWindowFunction*>(this)->is_my_window_expr(expr, can_be_produced);
    } else { /*do nothing*/
    }
  } else { /* do nothing */
  }

  return ret;
}

int ObLogicalOperator::expr_has_been_produced(
    const ObRawExpr* expr, ObAllocExprContext& gen_expr_ctx, bool& has_been_produced)
{
  int ret = OB_SUCCESS;
  has_been_produced = false;
  ExprProducer* expr_producer = NULL;
  if (OB_FAIL(gen_expr_ctx.find(expr, expr_producer))) {
    LOG_WARN("failed to find expr producer", K(ret));
  } else if (NULL == expr_producer) {
    // not find
    /*
     *  The expression is only visible if its producer branch id is
     *  lager than the current branch. The reason is illustrated in
     *  the following graph:
     *
     *            op1(b_id = 0)
     *           /   \
     *          /     \
     *       op2(0)   op3(1)
     *
     *  In this case, 'b_id' is the branch id for each operator.
     *  op3 should not see any expressions that are produced
     *  by op2 whose branch id is smaller than op3's.
     *
     *  Note that we rely on the operator numbering to mark the branch
     *  id of each operator, without which the algorithm is not going to work.
     */
  } else if ((OB_INVALID_ID != expr_producer->producer_branch_ && expr_producer->producer_branch_ >= branch_id_) ||
             (expr->has_flag(IS_ROWNUM) && OB_INVALID_ID != expr_producer->producer_id_)) {
    has_been_produced = true;
  }
  return ret;
}

int ObLogicalOperator::should_use_sequential_execution(bool& use_seq_exec) const
{
  int ret = OB_SUCCESS;
  use_seq_exec = true;
  const int64_t num_of_child = get_num_of_child();
  ObLogicalOperator* child_op = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && use_seq_exec && i < num_of_child; ++i) {
    if (OB_ISNULL(child_op = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(get_child(i)), K(ret));
    } else if (child_op->get_card() <= SEQUENTIAL_EXECUTION_THRESHOLD &&
               (child_op->get_sharding_info().is_local() || child_op->get_sharding_info().is_remote())) {
      /*do noting*/
    } else {
      use_seq_exec = false;
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_exchange(AllocExchContext* ctx, ObExchangeInfo& exch_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx));
  } else if (OB_FAIL(allocate_exchange_nodes_above(false, *ctx, exch_info))) {
    LOG_WARN("failed to allocate exchange nodes above", "operator", get_name());
  } else {
    LOG_TRACE("allocate exchange nodes above", K(get_name()));
  }
  return ret;
}

int ObLogicalOperator::choose_best_distribution_method(AllocExchContext& ctx, uint64_t candidate_method, bool sm_hint,
    JoinDistAlgo& best_method, SlaveMappingType& sm_type) const
{
  int ret = OB_SUCCESS;
  double current_cost = 0;
  double l_broadcast_cost = 0;
  double r_broadcast_cost = 0;
  double l_repartition_cost = 0;
  double r_repartition_cost = 0;
  double hash_hash_cost = 0;
  double b2host_none_cost = 0;
  double partition_wise_cost = 0;
  best_method = DIST_INVALID_METHOD;
  sm_type = SM_NONE;
  if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_PARTITION_WISE)) {
    partition_wise_cost = 0;
    UPDATE_CURRENT_JOIN_DIST_METHOD(best_method, current_cost, DIST_PARTITION_WISE, partition_wise_cost);
    // rule: if can partition wise join, remove all other distribution method
    candidate_method = DIST_PARTITION_WISE;
  }
  if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_PARTITION_NONE)) {
    if (OB_FAIL(get_repartition_cost(first_child, l_repartition_cost))) {
      LOG_WARN("failed to get left partition cost", K(ret));
    } else {
      UPDATE_CURRENT_JOIN_DIST_METHOD(best_method, current_cost, DIST_PARTITION_NONE, l_repartition_cost);
      remove_join_dist_flag(candidate_method, DIST_HASH_HASH);
      remove_join_dist_flag(candidate_method, DIST_BROADCAST_NONE);
      remove_join_dist_flag(candidate_method, DIST_BC2HOST_NONE);
    }
  }

  if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_NONE_PARTITION)) {
    if (OB_FAIL(get_repartition_cost(second_child, r_repartition_cost))) {
      LOG_WARN("failed to get right partition cost", K(ret));
    } else {
      UPDATE_CURRENT_JOIN_DIST_METHOD(best_method, current_cost, DIST_NONE_PARTITION, r_repartition_cost);
      remove_join_dist_flag(candidate_method, DIST_HASH_HASH);
      remove_join_dist_flag(candidate_method, DIST_NONE_BROADCAST);
    }
  }

  if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_BROADCAST_NONE)) {
    if (OB_FAIL(get_broadcast_cost(first_child, ctx, l_broadcast_cost))) {
      LOG_WARN("failed to get left broadcast cost", K(ret));
    } else {
      UPDATE_CURRENT_JOIN_DIST_METHOD(best_method, current_cost, DIST_BROADCAST_NONE, l_broadcast_cost);
    }
  }

  if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_NONE_BROADCAST)) {
    if (OB_FAIL(get_broadcast_cost(second_child, ctx, r_broadcast_cost))) {
      LOG_WARN("failed to get right broadcast cost", K(ret));
    } else {
      UPDATE_CURRENT_JOIN_DIST_METHOD(best_method, current_cost, DIST_NONE_BROADCAST, r_broadcast_cost);
    }
  }

  if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_HASH_HASH)) {
    if (OB_FAIL(get_hash_hash_cost(hash_hash_cost))) {
      LOG_WARN("failed to get hash hash cost", K(ret));
    } else {
      UPDATE_CURRENT_JOIN_DIST_METHOD(best_method, current_cost, DIST_HASH_HASH, hash_hash_cost);
    }
  }

  if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_BC2HOST_NONE)) {
    if (OB_FAIL(get_b2host_cost(first_child, ctx, b2host_none_cost))) {
      LOG_WARN("failed to get b2host cost", K(ret));
    } else {
      UPDATE_CURRENT_JOIN_DIST_METHOD(best_method, current_cost, DIST_BC2HOST_NONE, b2host_none_cost);
    }
  }
  if (OB_SUCC(ret) && DIST_INVALID_METHOD == best_method && has_join_dist_flag(candidate_method, DIST_PULL_TO_LOCAL)) {
    best_method = DIST_PULL_TO_LOCAL;
  }

  if (OB_SUCC(ret) && sm_hint &&
      (best_method == DIST_BROADCAST_NONE || best_method == DIST_NONE_BROADCAST || best_method == DIST_PARTITION_NONE ||
          best_method == DIST_NONE_PARTITION || best_method == DIST_PARTITION_WISE)) {
    if (best_method == DIST_PARTITION_WISE) {
      sm_type = SM_PWJ_HASH_HASH;
    } else if (best_method == DIST_PARTITION_NONE || best_method == DIST_NONE_PARTITION) {
      sm_type = SM_PPWJ_HASH_HASH;
    } else if (best_method == DIST_BROADCAST_NONE) {
      sm_type = SM_PPWJ_BCAST_NONE;
    } else if (best_method == DIST_NONE_BROADCAST) {
      sm_type = SM_PPWJ_NONE_BCAST;
    } else {
      sm_type = SM_NONE;
    }
  }
  LOG_TRACE("distribute method cost",
      K(b2host_none_cost),
      K(hash_hash_cost),
      K(r_broadcast_cost),
      K(l_broadcast_cost),
      K(l_repartition_cost),
      K(r_repartition_cost),
      K(partition_wise_cost),
      K(sm_type));
  return ret;
}

// please do not remove this function, will be used in someday
int ObLogicalOperator::should_use_slave_mapping(bool& enable_sm) const
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  int64_t sm_threshold = 0;
  ObSQLSessionInfo* sess_info = NULL;
  enable_sm = false;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt()) || OB_ISNULL(left_child = get_child(first_child)) ||
      OB_ISNULL(right_child = get_child(second_child)) ||
      OB_ISNULL(sess_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child), K(ret));
  } else if (OB_FAIL(sess_info->get_sys_variable(share::SYS_VAR__OB_PX_SLAVE_MAPPING_THRESHOLD, sm_threshold))) {
    LOG_WARN("failed to get system variable", K(ret));
  } else if (sm_threshold < 100 || LOG_TABLE_SCAN != left_child->get_type() ||
             LOG_TABLE_SCAN != right_child->get_type()) {
    enable_sm = false;
  } else {
    int64_t left_part_cnt = 1;
    int64_t right_part_cnt = 1;
    if (OB_FAIL(left_child->get_sharding_info().get_total_part_cnt(left_part_cnt))) {
      LOG_WARN("failed to get total part cnt", K(ret));
    } else if (OB_FAIL(right_child->get_sharding_info().get_total_part_cnt(right_part_cnt))) {
      LOG_WARN("failed to get total part cnt", K(ret));
    } else {
      int64_t partition_cnt = std::max(left_part_cnt, right_part_cnt);
      int64_t parallel = get_plan()->get_optimizer_context().get_parallel();
      if (parallel * 100 > partition_cnt * sm_threshold) {
        enable_sm = true;
      } else {
        enable_sm = false;
      }
    }
  }
  return ret;
}

int ObLogicalOperator::get_broadcast_cost(int64_t child_idx, AllocExchContext& ctx, double& cost) const
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  cost = 0.0;
  if (OB_ISNULL(child = get_child(child_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(child_idx), K(ret));
  } else {
    cost = child->get_card() * child->get_width() * ctx.parallel_;
  }
  return ret;
}

int ObLogicalOperator::get_repartition_cost(int64_t child_idx, double& cost) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_child(child_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    cost = get_child(child_idx)->get_card() * get_child(child_idx)->get_width();
  }
  return ret;
}

int ObLogicalOperator::get_hash_hash_cost(double& cost) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_child(first_child)) || OB_ISNULL(get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_child(first_child)), K(get_child(second_child)), K(ret));
  } else {
    cost = get_child(first_child)->get_card() * get_child(first_child)->get_width();
    cost += get_child(second_child)->get_card() * get_child(second_child)->get_width();
  }
  return ret;
}

int ObLogicalOperator::get_b2host_cost(int64_t child_idx, AllocExchContext& ctx, double& cost) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_child(child_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid op", K(ret));
  } else {
    const int64_t server_count = (0 == ctx.servers_.count()) ? 1 : ctx.servers_.count();
    cost += get_child(child_idx)->get_card() * get_child(child_idx)->get_width();
    cost = cost * server_count;
  }
  return ret;
}

int ObLogicalOperator::check_if_match_repart(const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& src_join_keys,
    const ObIArray<ObRawExpr*>& target_join_keys, const ObLogicalOperator& target_child, bool& is_match_repart)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> target_part_keys;
  bool is_base_table_scan = false;
  is_match_repart = false;
  if (!target_child.get_sharding_info().is_distributed()) {
    /* do nothing */
  } else if (OB_FAIL(check_is_table_scan(target_child, is_base_table_scan))) {
    LOG_WARN("failed to check whether is base table scan", K(ret));
  } else if (!is_base_table_scan) {
    /*do nothing*/
  } else if (OB_FAIL(target_child.get_sharding_info().get_all_partition_keys(target_part_keys, true))) {
    LOG_WARN("failed to get partition keys", K(ret));
  } else if (OB_FAIL(ObShardingInfo::check_if_match_repart(
                 equal_sets, src_join_keys, target_join_keys, target_part_keys, is_match_repart))) {
    LOG_WARN("failed to check if match repartition", K(ret));
  } else {
    LOG_TRACE("succeed to check whether matching repartition", K(is_match_repart));
  }
  return ret;
}

int ObLogicalOperator::check_is_table_scan(const ObLogicalOperator& op, bool& is_table_scan)
{
  int ret = OB_SUCCESS;
  is_table_scan = false;
  const ObLogicalOperator* cur_op = &op;
  while (NULL != cur_op) {
    if (LOG_TABLE_SCAN == cur_op->get_type()) {
      is_table_scan = true;
      cur_op = NULL;
    } else if (LOG_MATERIAL == cur_op->get_type() || LOG_JOIN_FILTER == cur_op->get_type() ||
               LOG_SORT == cur_op->get_type() || LOG_TABLE_LOOKUP == cur_op->get_type() ||
               LOG_SUBPLAN_SCAN == cur_op->get_type()) {
      cur_op = cur_op->get_child(first_child);
    } else {
      cur_op = NULL;
    }
  }
  return ret;
}

int ObLogicalOperator::get_repartition_table_info(
    ObLogicalOperator& op, ObString& table_name, uint64_t& table_id, uint64_t& ref_table_id)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  ref_table_id = OB_INVALID_ID;
  ObLogicalOperator* cur_op = &op;
  while (OB_NOT_NULL(cur_op) && LOG_TABLE_SCAN != cur_op->get_type()) {
    if (LOG_MATERIAL == cur_op->get_type() || LOG_JOIN_FILTER == cur_op->get_type() || LOG_SORT == cur_op->get_type() ||
        LOG_TABLE_LOOKUP == cur_op->get_type() || LOG_SUBPLAN_SCAN == cur_op->get_type()) {
      cur_op = cur_op->get_child(first_child);
    } else {
      cur_op = NULL;
    }
  }

  if (OB_ISNULL(cur_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObLogTableScan* scan_op = static_cast<ObLogTableScan*>(cur_op);
    table_id = scan_op->get_table_id();
    table_name = scan_op->get_table_name();
    if (scan_op->get_is_index_global()) {
      ref_table_id = scan_op->get_index_table_id();
    } else {
      ref_table_id = scan_op->get_ref_table_id();
    }
  }
  return ret;
}

int ObLogicalOperator::compute_sharding_and_allocate_exchange(AllocExchContext* ctx, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& hash_left_keys, const ObIArray<ObRawExpr*>& hash_right_keys,
    const ObIArray<ObExprCalcType>& hash_calc_types, const ObIArray<ObRawExpr*>& left_keys,
    const ObIArray<ObRawExpr*>& right_keys, ObLogicalOperator& left_child, ObLogicalOperator& right_child,
    const JoinDistAlgo best_method, const SlaveMappingType sm_type, const ObJoinType join_type,
    ObShardingInfo& sharding_info)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo l_exch_info;
  ObExchangeInfo r_exch_info;
  l_exch_info.dist_method_ = ObPQDistributeMethod::NONE;
  r_exch_info.dist_method_ = ObPQDistributeMethod::NONE;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (JoinDistAlgo::DIST_PARTITION_WISE == best_method) {
    if (SM_NONE == sm_type) {
      if (left_child.get_sharding_info().is_distributed()) {
        sharding_info.copy_with_part_keys(left_child.get_sharding_info());
      } else {
        sharding_info.copy_with_part_keys(right_child.get_sharding_info());
      }
    } else {
      if (OB_FAIL(compute_hash_distribution_info(
              hash_left_keys, hash_right_keys, hash_calc_types, l_exch_info, r_exch_info, sharding_info))) {
        LOG_WARN("failed to compute hash distribution info", K(ret));
      } else {
        l_exch_info.set_slave_mapping_type(sm_type);
        r_exch_info.set_slave_mapping_type(sm_type);
        sharding_info.reset();
        sharding_info.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
      }
    }
  } else if (JoinDistAlgo::DIST_PARTITION_NONE == best_method) {
    ObPQDistributeMethod::Type unmatch_method = ObPQDistributeMethod::DROP;
    if (LEFT_ANTI_JOIN == join_type || LEFT_OUTER_JOIN == join_type || FULL_OUTER_JOIN == join_type) {
      unmatch_method = ObPQDistributeMethod::RANDOM;
    }
    if (OB_FAIL(compute_repartition_distribution_info(
            equal_sets, left_keys, right_keys, right_child, l_exch_info, sharding_info))) {
      LOG_WARN("failed to compute repartition distrubution info", K(ret));
    } else {
      l_exch_info.unmatch_row_dist_method_ = unmatch_method;
      LOG_TRACE("succeed to compute repartition distribution info", K(l_exch_info), K(sharding_info));
      if (SM_NONE != sm_type) {
        if (OB_FAIL(compute_hash_distribution_info(
                hash_left_keys, hash_right_keys, hash_calc_types, l_exch_info, r_exch_info, sharding_info))) {
          LOG_WARN("failed to compute hash distribution info", K(ret));
        } else {
          l_exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_HASH;
          r_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
          l_exch_info.set_slave_mapping_type(sm_type);
          r_exch_info.set_slave_mapping_type(sm_type);
          sharding_info.reset();
          sharding_info.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
        }
      }
    }
  } else if (JoinDistAlgo::DIST_NONE_PARTITION == best_method) {
    ObPQDistributeMethod::Type unmatch_method = ObPQDistributeMethod::DROP;
    if (RIGHT_ANTI_JOIN == join_type || RIGHT_OUTER_JOIN == join_type || FULL_OUTER_JOIN == join_type) {
      unmatch_method = ObPQDistributeMethod::RANDOM;
    }
    if (OB_FAIL(compute_repartition_distribution_info(
            equal_sets, right_keys, left_keys, left_child, r_exch_info, sharding_info))) {
      LOG_WARN("failed to compute repartition distribution_info", K(ret));
    } else {
      r_exch_info.unmatch_row_dist_method_ = unmatch_method;
      LOG_TRACE("succeed to compute repartition distribution info", K(r_exch_info), K(sharding_info));
      if (SM_NONE != sm_type) {
        if (OB_FAIL(compute_hash_distribution_info(
                hash_left_keys, hash_right_keys, hash_calc_types, l_exch_info, r_exch_info, sharding_info))) {
          LOG_WARN("failed to compute hash distribution info", K(ret));
        } else {
          l_exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_HASH;
          r_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
          l_exch_info.set_slave_mapping_type(sm_type);
          r_exch_info.set_slave_mapping_type(sm_type);
          sharding_info.reset();
          sharding_info.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
        }
      }
    }
  } else if (JoinDistAlgo::DIST_BC2HOST_NONE == best_method) {
    sharding_info.set_location_type(right_child.get_sharding_info().get_location_type());
    if (right_child.get_sharding_info().is_sharding()) {
      l_exch_info.dist_method_ = ObPQDistributeMethod::BC2HOST;
    } else {
      l_exch_info.dist_method_ = ObPQDistributeMethod::MAX_VALUE;
    }
  } else if (JoinDistAlgo::DIST_BROADCAST_NONE == best_method) {
    if (SM_NONE == sm_type) {
      if (OB_FAIL(sharding_info.copy_with_part_keys(right_child.get_sharding_info()))) {
        LOG_WARN("failed to assign sharding info", K(ret));
      } else if (right_child.get_sharding_info().is_sharding()) {
        l_exch_info.dist_method_ = ObPQDistributeMethod::BROADCAST;
      } else {
        l_exch_info.dist_method_ = ObPQDistributeMethod::MAX_VALUE;
      }
    } else {
      if (OB_FAIL(compute_repartition_distribution_info(
              equal_sets, left_keys, right_keys, right_child, l_exch_info, sharding_info))) {
        LOG_WARN("failed to compute repartition distrubution info", K(ret));
      } else {
        l_exch_info.repartition_type_ = OB_REPARTITION_NO_REPARTITION;
        l_exch_info.dist_method_ = ObPQDistributeMethod::SM_BROADCAST;
        l_exch_info.set_slave_mapping_type(sm_type);
        sharding_info.reset();
        sharding_info.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
      }
    }
  } else if (JoinDistAlgo::DIST_NONE_BROADCAST == best_method) {
    if (SM_NONE == sm_type) {
      if (OB_FAIL(sharding_info.copy_with_part_keys(left_child.get_sharding_info()))) {
        LOG_WARN("failed to assign sharding info", K(ret));
      } else if (left_child.get_sharding_info().is_sharding()) {
        r_exch_info.dist_method_ = ObPQDistributeMethod::BROADCAST;
      } else {
        r_exch_info.dist_method_ = ObPQDistributeMethod::MAX_VALUE;
      }
    } else {
      if (OB_FAIL(compute_repartition_distribution_info(
              equal_sets, right_keys, left_keys, left_child, r_exch_info, sharding_info))) {
        LOG_WARN("failed to compute repartition distribution_info", K(ret));
      } else {
        r_exch_info.repartition_type_ = OB_REPARTITION_NO_REPARTITION;
        r_exch_info.dist_method_ = ObPQDistributeMethod::SM_BROADCAST;
        r_exch_info.set_slave_mapping_type(sm_type);
        sharding_info.reset();
        sharding_info.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
      }
    }
  } else if (JoinDistAlgo::DIST_HASH_HASH == best_method) {
    if (OB_FAIL(compute_hash_distribution_info(
            hash_left_keys, hash_right_keys, hash_calc_types, l_exch_info, r_exch_info, sharding_info))) {
      LOG_WARN("failed to compute hash distribution info", K(ret));
    } else { /* do nothing*/
    }
  } else if (JoinDistAlgo::DIST_PULL_TO_LOCAL == best_method) {
    if (left_child.get_sharding_info().is_sharding()) {
      l_exch_info.dist_method_ = ObPQDistributeMethod::MAX_VALUE;
    }
    if (right_child.get_sharding_info().is_sharding()) {
      r_exch_info.dist_method_ = ObPQDistributeMethod::MAX_VALUE;
    }
    sharding_info.set_location_type(OB_TBL_LOCATION_LOCAL);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected distribution method", K(best_method), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (ObPQDistributeMethod::NONE != l_exch_info.dist_method_ &&
        OB_FAIL(left_child.allocate_exchange(ctx, l_exch_info))) {
      LOG_WARN("failed to allocate exchange", K(ret));
    } else if (ObPQDistributeMethod::NONE != r_exch_info.dist_method_ &&
               OB_FAIL(right_child.allocate_exchange(ctx, r_exch_info))) {
      LOG_WARN("failed to allocate exchange", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_sharding_conds(ctx->sharding_conds_, best_method, join_type, l_exch_info, r_exch_info))) {
      LOG_WARN("failed to update sharding conds", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogicalOperator::update_sharding_conds(ObIArray<ObRawExpr*>& sharding_conds, const JoinDistAlgo best_method,
    const ObJoinType join_type, const ObExchangeInfo& left_exch_info, const ObExchangeInfo& right_exch_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> left_part_keys;
  ObSEArray<ObRawExpr*, 8> right_part_keys;
  ObLogicalOperator* left_child = get_child(first_child);
  ObLogicalOperator* right_child = get_child(second_child);
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(left_child), K(right_child));
  } else if (!sharding_info_.is_distributed() || (!IS_LEFT_STYLE_JOIN(join_type) && !IS_RIGHT_STYLE_JOIN(join_type))) {
    // do nothing
  } else if (DIST_PARTITION_NONE == best_method) {
    if (LEFT_ANTI_JOIN == join_type || LEFT_OUTER_JOIN == join_type || FULL_OUTER_JOIN == join_type) {
      // unmatched joined records do not share the same sharding info
    } else if (OB_FAIL(append(left_part_keys, left_exch_info.repartition_keys_))) {
      LOG_WARN("failed to append left part keys", K(ret));
    } else if (OB_FAIL(append(left_part_keys, left_exch_info.repartition_sub_keys_))) {
      LOG_WARN("failed to append left part sub keys", K(ret));
    } else if (OB_FAIL(right_child->get_sharding_info().get_all_partition_keys(right_part_keys, true))) {
      LOG_WARN("failed to get right partition keys", K(ret));
    }
  } else if (DIST_NONE_PARTITION == best_method) {
    if (RIGHT_ANTI_JOIN == join_type || RIGHT_OUTER_JOIN == join_type || FULL_OUTER_JOIN == join_type) {
      // do nothing
    } else if (OB_FAIL(left_child->get_sharding_info().get_all_partition_keys(left_part_keys, true))) {
      LOG_WARN("failed to get left partition keys", K(ret));
    } else if (OB_FAIL(append(right_part_keys, right_exch_info.repartition_keys_))) {
      LOG_WARN("failed to append right part keys", K(ret));
    } else if (OB_FAIL(append(right_part_keys, right_exch_info.repartition_sub_keys_))) {
      LOG_WARN("failed to append right part sub keys", K(ret));
    }
  } else if (DIST_HASH_HASH == best_method) {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_exch_info.hash_dist_exprs_.count(); ++i) {
      if (OB_FAIL(left_part_keys.push_back(left_exch_info.hash_dist_exprs_.at(i).expr_))) {
        LOG_WARN("failed to push back left hash distribution key", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_exch_info.hash_dist_exprs_.count(); ++i) {
      if (OB_FAIL(right_part_keys.push_back(right_exch_info.hash_dist_exprs_.at(i).expr_))) {
        LOG_WARN("failed to push back right hash distribution key", K(ret));
      }
    }
  } else if (DIST_PARTITION_WISE == best_method) {
    if (OB_FAIL(left_child->get_sharding_info().get_all_partition_keys(left_part_keys, true))) {
      LOG_WARN("failed to get left partition keys", K(ret));
    } else if (OB_FAIL(right_child->get_sharding_info().get_all_partition_keys(right_part_keys, true))) {
      LOG_WARN("failed to get right partition keys", K(ret));
    }
  }
  if (OB_SUCC(ret) && left_part_keys.count() == right_part_keys.count()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_part_keys.count(); ++i) {
      ObRawExpr* equal_expr = NULL;
      if (OB_ISNULL(left_part_keys.at(i)) || OB_ISNULL(right_part_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("params have null", K(ret));
      } else if (ObOptimizerUtil::is_expr_equivalent(
                     left_part_keys.at(i), right_part_keys.at(i), get_ordering_output_equal_sets())) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                     get_plan()->get_optimizer_context().get_session_info(),
                     left_part_keys.at(i),
                     right_part_keys.at(i),
                     equal_expr))) {
        LOG_WARN("failed to create equal expr", K(ret));
      } else if (OB_FAIL(sharding_conds.push_back(equal_expr))) {
        LOG_WARN("failed to push back equal expr", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::compute_repartition_distribution_info(const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& src_keys, const ObIArray<ObRawExpr*>& target_keys, ObLogicalOperator& target_child,
    ObExchangeInfo& exch_info, ObShardingInfo& sharding_info)
{
  int ret = OB_SUCCESS;
  ObString table_name;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t ref_table_id = OB_INVALID_ID;
  ObShardingInfo& target_sharding = target_child.get_sharding_info();
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_repartition_table_info(target_child, table_name, table_id, ref_table_id))) {
    LOG_WARN("failed to get repart table info", K(ret));
  } else if (OB_FAIL(sharding_info.copy_with_part_keys(target_sharding))) {
    LOG_WARN("failed to deep coyp child op sharding infos", K(ret));
  } else if (OB_FAIL(compute_repartition_func_info(equal_sets,
                 src_keys,
                 target_keys,
                 target_sharding,
                 get_plan()->get_optimizer_context().get_expr_factory(),
                 exch_info))) {
    LOG_WARN("failed to compute repartition func info", K(ret));
  } else {
    exch_info.dist_method_ = ObPQDistributeMethod::PARTITION;
    exch_info.repartition_ref_table_id_ = ref_table_id;
    exch_info.repartition_table_name_ = table_name;
    exch_info.slice_count_ = sharding_info.get_part_cnt();
    if (OB_FAIL(ret)) {
    } else if (share::schema::PARTITION_LEVEL_ONE == target_sharding.get_part_level()) {
      exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL;
    } else if (share::schema::PARTITION_LEVEL_TWO == target_sharding.get_part_level()) {
      if (target_sharding.is_partition_single()) {
        exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB;
      } else if (target_sharding.is_subpartition_single()) {
        exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST;
      } else {
        exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_TWO_LEVEL;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected partition level", K(target_sharding.get_part_level()));
    }
    if (OB_SUCC(ret)) {
      ObSQLSessionInfo* session = NULL;
      if (OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("session is null", K(ret));
      } else if (!session->use_static_typing_engine()) {
        // do nothing
      } else if (OB_FAIL(exch_info.init_calc_part_id_expr(get_plan()->get_optimizer_context()))) {
        LOG_WARN("fail to calc part id expr", K(ret), K(exch_info));
      } else {
        LOG_DEBUG("repart exch info", K(exch_info));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::compute_hash_distribution_info(const ObIArray<ObRawExpr*>& left_keys,
    const ObIArray<ObRawExpr*>& right_keys, const ObIArray<ObExprCalcType>& calc_types, ObExchangeInfo& l_exch_info,
    ObExchangeInfo& r_exch_info, ObShardingInfo& sharding_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left_keys.count() != calc_types.count()) || OB_UNLIKELY(right_keys.count() != calc_types.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(left_keys.count()), K(right_keys.count()), K(calc_types.count()), K(ret));
  } else {
    bool use_left = true;
    bool use_right = true;
    ObRawExpr* join_expr = NULL;
    ObRawExpr* left_expr = NULL;
    ObRawExpr* right_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < left_keys.count(); i++) {
      if (OB_ISNULL(left_keys.at(i)) || OB_ISNULL(right_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(left_keys.at(i)), K(right_keys.at(i)), K(ret));
      } else if (OB_FAIL(l_exch_info.hash_dist_exprs_.push_back(
                     ObExchangeInfo::HashExpr(left_keys.at(i), calc_types.at(i)))) ||
                 OB_FAIL(r_exch_info.hash_dist_exprs_.push_back(
                     ObExchangeInfo::HashExpr(right_keys.at(i), calc_types.at(i))))) {
        LOG_WARN("array push back failed", K(ret));
      } else {
        if (!ObSQLUtils::is_same_type_for_compare(left_keys.at(i)->get_result_type(), calc_types.at(i))) {
          use_left = false;
        }
        if (!ObSQLUtils::is_same_type_for_compare(right_keys.at(i)->get_result_type(), calc_types.at(i))) {
          use_right = false;
        }
      }
    }
    if (OB_SUCC(ret)) {
      l_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
      r_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
      sharding_info.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
      if (use_left) {
        ret = sharding_info.get_partition_keys().assign(left_keys);
      } else if (use_right) {
        ret = sharding_info.get_partition_keys().assign(right_keys);
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogicalOperator::compute_repartition_func_info(const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& src_keys,
    const ObIArray<ObRawExpr*>& target_keys, const ObShardingInfo& target_sharding, ObRawExprFactory& expr_factory,
    ObExchangeInfo& exch_info)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = NULL;
  ObSEArray<ObRawExpr*, 4> old_exprs;
  ObSEArray<ObRawExpr*, 4> new_exprs;
  ObSEArray<ObRawExpr*, 4> repart_exprs;
  ObSEArray<ObRawExpr*, 4> repart_sub_exprs;
  ObSEArray<ObRawExpr*, 4> repart_func_exprs;
  // get repart exprs
  bool skip_part = target_sharding.is_partition_single();
  bool skip_subpart = target_sharding.is_subpartition_single();
  if (OB_ISNULL(get_plan()) || OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(session_info), K(ret));
  } else if (!skip_part &&
             OB_FAIL(get_repartition_keys(
                 equal_sets, src_keys, target_keys, target_sharding.get_partition_keys(), repart_exprs))) {
    LOG_WARN("failed to get repartition keys", K(ret));
  } else if (!skip_subpart &&
             OB_FAIL(get_repartition_keys(
                 equal_sets, src_keys, target_keys, target_sharding.get_sub_partition_keys(), repart_sub_exprs))) {
    LOG_WARN("failed to get repartition keys", K(ret));
  } else if (!skip_part && (OB_FAIL(append(old_exprs, target_sharding.get_partition_keys())) ||
                               OB_FAIL(append(new_exprs, repart_exprs)))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (!skip_subpart && (OB_FAIL(append(old_exprs, target_sharding.get_sub_partition_keys())) ||
                                  OB_FAIL(append(new_exprs, repart_sub_exprs)))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < target_sharding.get_partition_func().count(); i++) {
      ObRawExpr* repart_func_expr = NULL;
      ObRawExpr* target_func_expr = target_sharding.get_partition_func().at(i);
      if ((0 == i && skip_part) || (1 == i && skip_subpart)) {
        ObConstRawExpr* const_expr = NULL;
        ObRawExpr* dummy_expr = NULL;
        int64_t const_value = 1;
        if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                get_plan()->get_optimizer_context().get_expr_factory(), ObIntType, const_value, const_expr))) {
          LOG_WARN("Failed to build const expr", K(ret));
        } else if (OB_ISNULL(dummy_expr = const_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(dummy_expr->formalize(session_info))) {
          LOG_WARN("Failed to formalize a new expr", K(ret));
        } else if (OB_FAIL(repart_func_exprs.push_back(dummy_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_ISNULL(target_func_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(
                     ObRawExprUtils::copy_expr(expr_factory, target_func_expr, repart_func_expr, COPY_REF_DEFAULT))) {
        LOG_WARN("failed to deep copy the partition fuc raw expr");
      } else if (OB_ISNULL(repart_func_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_equal_expr(old_exprs, new_exprs, repart_func_expr))) {
        LOG_WARN("failed to replace general expr", K(ret));
      } else if (OB_FAIL(repart_func_exprs.push_back(repart_func_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(exch_info.set_repartition_info(repart_exprs, repart_sub_exprs, repart_func_exprs))) {
        LOG_WARN("failed to set repartition keys", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogicalOperator::get_repartition_keys(const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& src_keys,
    const ObIArray<ObRawExpr*>& target_keys, const ObIArray<ObRawExpr*>& target_part_keys,
    ObIArray<ObRawExpr*>& src_part_keys)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src_keys.count() != target_keys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(src_keys.count()), K(target_keys.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < target_part_keys.count(); i++) {
      if (OB_ISNULL(target_part_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        bool is_find = false;
        for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < target_keys.count(); j++) {
          if (OB_ISNULL(target_keys.at(j)) || OB_ISNULL(src_keys.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(target_keys.at(j)), K(src_keys.at(j)), K(ret));
          } else if (ObOptimizerUtil::is_expr_equivalent(target_part_keys.at(i), target_keys.at(j), equal_sets) &&
                     target_part_keys.at(i)->get_result_type().get_type_class() ==
                         src_keys.at(j)->get_result_type().get_type_class() &&
                     target_part_keys.at(i)->get_result_type().get_collation_type() ==
                         src_keys.at(j)->get_result_type().get_collation_type() &&
                     !ObObjCmpFuncs::is_otimestamp_cmp(target_part_keys.at(i)->get_result_type().get_type(),
                         src_keys.at(j)->get_result_type().get_type()) &&
                     !ObObjCmpFuncs::is_datetime_timestamp_cmp(target_part_keys.at(i)->get_result_type().get_type(),
                         src_keys.at(j)->get_result_type().get_type())) {
            if (OB_FAIL(src_part_keys.push_back(src_keys.at(j)))) {
              LOG_WARN("failed to push back keys", K(ret));
            } else {
              is_find = true;
            }
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret) && !is_find) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not find part expr", K(target_part_keys.at(i)), K(src_keys), K(target_keys), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::reorder_filter_exprs()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpeced null", K(ret), K(get_plan()));
  } else {
    FilterCompare filter_compare(get_plan()->get_predicate_selectivities());
    std::sort(filter_exprs_.begin(), filter_exprs_.end(), filter_compare);
  }
  return ret;
}

int ObLogicalOperator::allocate_exchange_nodes_above(bool is_remote, AllocExchContext& ctx, ObExchangeInfo& exch_info)
{
  int ret = OB_SUCCESS;
  //  op         exchange
  //   |    ->    |
  //  other      op
  //              |
  //             other
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()));
  } else {
    ObLogicalOperator* producer = NULL;
    ObLogicalOperator* consumer = NULL;
    ObLogOperatorFactory& factory = get_plan()->get_log_op_factory();
    if (OB_ISNULL(producer = factory.allocate(*(get_plan()), LOG_EXCHANGE)) ||
        OB_ISNULL(consumer = factory.allocate(*(get_plan()), LOG_EXCHANGE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate exchange nodes", K(producer), K(consumer), K(ret));
    } else {
      if (NULL != get_parent()) {
        bool found_child = false;
        for (int64_t i = 0; OB_SUCC(ret) && !found_child && i < get_parent()->get_num_of_child(); ++i) {
          if (get_parent()->get_child(i) == this) {
            get_parent()->set_child(i, consumer);
            found_child = true;
          } else { /*do nothing*/
          }
        }
        consumer->set_parent(get_parent());
      } else { /*do nothing*/
      }
      producer->set_child(first_child, this);
      set_parent(producer);
      consumer->set_child(first_child, producer);
      producer->set_parent(consumer);

      reinterpret_cast<ObLogExchange*>(consumer)->set_task_order(exch_info.is_task_order_);
      reinterpret_cast<ObLogExchange*>(producer)->set_to_producer();
      reinterpret_cast<ObLogExchange*>(consumer)->set_to_consumer();
      reinterpret_cast<ObLogExchange*>(producer)->set_is_remote(is_remote);
      reinterpret_cast<ObLogExchange*>(consumer)->set_is_remote(is_remote);

      exch_info.keep_ordering_ = (sharding_info_.is_remote() || sharding_info_.is_local()) && (1 == ctx.parallel_);
      if (exch_info.px_dop_ <= 0) {
        if (my_plan_->get_optimizer_context().get_parallel_rule() == PXParallelRule::MANUAL_TABLE_DOP) {
          int64_t dfo_table_dop = -1;
          bool found_base_table = false;
          if (OB_FAIL(calc_current_dfo_table_dop(this, dfo_table_dop, found_base_table))) {
            LOG_WARN("failed to get current dfo table dop", K(ret));
          } else if (dfo_table_dop < 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid dfo table dop", K(ret), K(dfo_table_dop));
          } else {
            exch_info.px_dop_ = dfo_table_dop;
          }
        } else {
          exch_info.px_dop_ = ctx.parallel_;
        }
        LOG_TRACE("Exchange set dop", K(ctx.parallel_), K(exch_info.px_dop_));
      }

      ctx.add_exchange_type(is_remote ? AllocExchContext::REMOTE : AllocExchContext::DISTRIBUTED);
      ctx.exchange_allocated_ = true;
      if (is_plan_root()) {
        set_is_plan_root(false);
        consumer->mark_is_plan_root();
        get_plan()->set_plan_root(consumer);
      } else { /*do nothing*/
      }

      if (sharding_info_.is_single()) {
        exch_info.px_single_ = true;
        exch_info.px_dop_ = 1;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(reinterpret_cast<ObLogExchange*>(producer)->set_exchange_info(exch_info))) {
          LOG_WARN("failed to set exchange info for producer");
        } else if (OB_FAIL(producer->compute_property())) {
          LOG_WARN("failed to compute equal set", K(ret));
        } else if (OB_FAIL(producer->replace_generated_agg_expr(ctx.group_push_down_replaced_exprs_))) {
          LOG_WARN("failed to replace agg exprs", K(ret));
        } else if (OB_FAIL(reinterpret_cast<ObLogExchange*>(consumer)->set_exchange_info(exch_info))) {
          LOG_WARN("failed to set exchange info for producer");
        } else if (OB_FAIL(consumer->compute_property())) {
          LOG_WARN("failed to compute equal set", K(ret));
        } else if (OB_FAIL(consumer->replace_generated_agg_expr(ctx.group_push_down_replaced_exprs_))) {
          LOG_WARN("failed to replace agg exprs", K(ret));
        } else if (OB_FAIL(static_cast<ObLogExchange*>(consumer)->update_sharding_conds(ctx))) {
          LOG_WARN("failed to update sharding conditions", K(ret));
        } else { /*do nothing*/
        }
      }
      LOG_TRACE("succ to allocate exchange nodes above operator", K(get_name()), K(get_cost()), K(get_card()), K(ctx));
    }
  }

  return ret;
}

int ObLogicalOperator::gen_location_constraint(void* ctx)
{
  int ret = OB_SUCCESS;
  bool is_union_all = false;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret));
  } else {
    ObLocationConstraintContext* loc_cons_ctx = reinterpret_cast<ObLocationConstraintContext*>(ctx);

    if (get_num_of_child() == 0) {
      if (log_op_def::LOG_TABLE_SCAN == get_type()) {
        // base table constraints for TABLE SCAN
        LocationConstraint loc_cons;
        ObLogTableScan* log_scan_op = dynamic_cast<ObLogTableScan*>(this);
        if (log_scan_op->get_is_fake_cte_table()) {
          // do nothing
        } else if (log_scan_op->get_dblink_id() != 0) {
          // dblink table, execute at other cluster
        } else if (OB_FAIL(get_tbl_loc_cons_for_scan(loc_cons))) {
          LOG_WARN("failed to get location constraint for table scan op", K(ret));
        } else if (OB_FAIL(loc_cons_ctx->base_table_constraints_.push_back(loc_cons))) {
          LOG_WARN("failed to push back location constraint", K(ret));
        } else if (OB_FAIL(strict_pwj_constraint_.push_back(loc_cons_ctx->base_table_constraints_.count() - 1))) {
          LOG_WARN("failed to push back location constraint offset", K(ret));
        } else if (OB_FAIL(non_strict_pwj_constraint_.push_back(loc_cons_ctx->base_table_constraints_.count() - 1))) {
          LOG_WARN("failed to push back location constraint offset", K(ret));
        }
      }
    } else if (get_num_of_child() > 0) {

      int64_t add_count = 0;
      bool is_pdml = false;
      if (log_op_def::LOG_SET == get_type()) {
        ObLogSet* set_op = static_cast<ObLogSet*>(this);
        is_union_all =
            (ObSelectStmt::UNION == set_op->get_set_op() && !set_op->is_set_distinct()) || set_op->is_recursive_union();
      }

      if (log_op_def::LOG_INSERT == get_type()) {
        bool is_multi_part_dml = false;
        LocationConstraint loc_cons;
        if (OB_FAIL(get_tbl_loc_cons_for_insert(loc_cons, is_multi_part_dml))) {
          LOG_WARN("failed to get location constraint for insert op", K(ret));
        } else if (!is_multi_part_dml) {
          if (OB_FAIL(loc_cons_ctx->base_table_constraints_.push_back(loc_cons))) {
            LOG_WARN("failed to push back location constraint", K(ret));
          } else if (OB_FAIL(strict_pwj_constraint_.push_back(loc_cons_ctx->base_table_constraints_.count() - 1))) {
            LOG_WARN("failed to push back location constraint offset", K(ret));
          } else if (OB_FAIL(non_strict_pwj_constraint_.push_back(loc_cons_ctx->base_table_constraints_.count() - 1))) {
            LOG_WARN("failed to push back location constraint offset", K(ret));
          } else {
            ++add_count;
          }
        } else if (OB_FAIL(loc_cons_ctx->base_table_constraints_.push_back(loc_cons))) {
          LOG_WARN("failed to push back location constraint", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (log_op_def::LOG_DELETE == get_type() || log_op_def::LOG_UPDATE == get_type()) {
          ObLogDelUpd* dml_log_op = static_cast<ObLogDelUpd*>(this);
          if (dml_log_op->is_pdml()) {
            LocationConstraint loc_cons;
            is_pdml = true;
            if (OB_FAIL(get_tbl_loc_cons_for_pdml_index(loc_cons))) {
              LOG_WARN("failed to get table location constraint for pdml index", K(ret), K(get_name()));
            } else if (OB_FAIL(loc_cons_ctx->base_table_constraints_.push_back(loc_cons))) {
              LOG_WARN("failed to push back location constraint", K(ret));
            } else if (OB_FAIL(strict_pwj_constraint_.push_back(loc_cons_ctx->base_table_constraints_.count() - 1))) {
              LOG_WARN("failed to push back location constraint offset", K(ret));
            } else if (OB_FAIL(
                           non_strict_pwj_constraint_.push_back(loc_cons_ctx->base_table_constraints_.count() - 1))) {
              LOG_WARN("failed to push back location constraint offset", K(ret));
            } else {
              ++add_count;
              LOG_TRACE("add location constraint for pdml index maintain op",
                  K(ret),
                  K(loc_cons),
                  K(get_name()),
                  "base_table_cons",
                  loc_cons_ctx->base_table_constraints_);
            }
          }
        }
      }

      bool need_add_strict = true;
      bool need_add_non_strict = true;
      for (int64_t i = 0; OB_SUCC(ret) && !is_pdml && i < get_num_of_child(); ++i) {
        if (OB_ISNULL(get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(i));
        } else if (LOG_EXCHANGE != get_child(i)->get_type()) {
          LOG_TRACE("get child location constraint",
              K(i),
              K(get_child(i)->strict_pwj_constraint_),
              K(get_child(i)->non_strict_pwj_constraint_));
          if (get_child(i)->strict_pwj_constraint_.count() <= 0 &&
              get_child(i)->non_strict_pwj_constraint_.count() <= 0) {
          } else if (get_child(i)->strict_pwj_constraint_.count() <= 0 ||
                     get_child(i)->non_strict_pwj_constraint_.count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("strict pwj constraint and non strict pwj constraint count not valid",
                K(ret),
                K(get_child(i)->strict_pwj_constraint_.count()),
                K(get_child(i)->non_strict_pwj_constraint_.count()));
          } else {
            ++add_count;
            if (need_add_strict) {
              if (OB_FAIL(append(strict_pwj_constraint_, get_child(i)->strict_pwj_constraint_))) {
                LOG_WARN("failed to append child pwj constraint", K(ret));
              } else if (is_union_all) {
                need_add_strict = false;
              }
            }
            if (need_add_non_strict) {
              if (OB_FAIL(append(non_strict_pwj_constraint_, get_child(i)->non_strict_pwj_constraint_))) {
                LOG_WARN("failed to append child pwj constraint", K(ret));
              } else if (!is_union_all) {
                need_add_non_strict = false;
              }
            }
          }
        }
      }  // for end

      if (OB_SUCC(ret) && add_count > 1) {
        if (is_union_all) {
          if (OB_FAIL(loc_cons_ctx->non_strict_constraints_.push_back(&non_strict_pwj_constraint_))) {
            LOG_WARN("failed to push back pwj constraint");
          }
        } else if (OB_FAIL(loc_cons_ctx->strict_constraints_.push_back(&strict_pwj_constraint_))) {
          LOG_WARN("fail to push back pwj constraint", K(ret));
        }
      }
    } else { /* do nothing */
    }
  }

  return ret;
}

int ObLogicalOperator::get_tbl_loc_cons_for_pdml_index(LocationConstraint& loc_cons)
{
  int ret = OB_SUCCESS;
  ObLogDelUpd* dml_log_op = static_cast<ObLogDelUpd*>(this);
  if (OB_ISNULL(dml_log_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml logical operator is null", K(ret), K(dml_log_op));
  } else {
    loc_cons.phy_loc_type_ = dml_log_op->get_phy_location_type();
    loc_cons.key_.table_id_ = dml_log_op->get_table_id();
    loc_cons.key_.ref_table_id_ = dml_log_op->get_index_tid();
    ObShardingInfo& sharding = dml_log_op->get_sharding_info();
    loc_cons.sharding_info_ = &sharding;
    if (sharding.get_part_cnt() > 1 && sharding.is_distributed()) {
      if (sharding.is_partition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SinglePartition);
      } else if (sharding.is_subpartition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SingleSubPartition);
      }
    }
  }
  return ret;
}

int ObLogicalOperator::get_tbl_loc_cons_for_scan(LocationConstraint& loc_cons)
{
  int ret = OB_SUCCESS;
  ObLogTableScan* log_scan_op = dynamic_cast<ObLogTableScan*>(this);
  if (OB_ISNULL(log_scan_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_scan_op));
  } else {
    loc_cons.phy_loc_type_ = log_scan_op->get_phy_location_type();
    loc_cons.key_.table_id_ = log_scan_op->get_table_id();
    loc_cons.key_.ref_table_id_ = log_scan_op->get_location_table_id();
    ObShardingInfo& sharding = log_scan_op->get_sharding_info();
    loc_cons.sharding_info_ = &sharding;
    if (sharding.get_part_cnt() > 1 && sharding.is_distributed()) {
      if (sharding.is_partition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SinglePartition);
      } else if (sharding.is_subpartition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SingleSubPartition);
      }
    }

    LOG_TRACE("initialized table's location constraint for table scan op", K(loc_cons));
  }
  return ret;
}

int ObLogicalOperator::get_tbl_loc_cons_for_insert(LocationConstraint& loc_cons, bool& is_multi_part_dml)
{
  int ret = OB_SUCCESS;
  ObLogInsert* log_insert_op = static_cast<ObLogInsert*>(this);
  if (OB_ISNULL(log_insert_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_insert_op));
  } else {
    is_multi_part_dml = log_insert_op->is_multi_part_dml();
    loc_cons.phy_loc_type_ = log_insert_op->get_phy_location_type();
    loc_cons.key_.table_id_ = log_insert_op->get_loc_table_id();
    loc_cons.key_.ref_table_id_ = log_insert_op->get_index_tid();
    ObShardingInfo& sharding = log_insert_op->get_sharding_info();
    loc_cons.sharding_info_ = &sharding;
    if (is_multi_part_dml) {
      loc_cons.add_constraint_flag(LocationConstraint::IsMultiPartInsert);
    } else if (sharding.get_part_cnt() > 1 && sharding.is_distributed()) {
      if (sharding.is_partition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SinglePartition);
      } else if (sharding.is_subpartition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SingleSubPartition);
      }
    }
    LOG_TRACE("initialized table's location constraint for insert op", K(loc_cons), K(is_multi_part_dml));
  }

  return ret;
}

int ObLogicalOperator::allocate_exchange_nodes_below(int64_t index, AllocExchContext& ctx, ObExchangeInfo& exch_info)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  //   op         op
  //    |          |
  //   child_i -> exchange
  //    |          |
  //   other      child_i
  //               |
  //              other
  if (OB_ISNULL(child = get_child(index)) || OB_ISNULL(get_plan())) {
    ret = OB_ERROR;
    LOG_WARN("invalid argument", K(ret), K(child), K(index), K(get_plan()));
  } else {
    ObLogicalOperator* producer = NULL;
    ObLogicalOperator* consumer = NULL;
    ObLogOperatorFactory& factory = get_plan()->get_log_op_factory();
    if (OB_ISNULL(producer = factory.allocate(*(get_plan()), LOG_EXCHANGE)) ||
        OB_ISNULL(consumer = factory.allocate(*(get_plan()), LOG_EXCHANGE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate exchange nodes", K(producer), K(consumer), K(ret));
    } else {
      producer->set_child(first_child, child);
      child->set_parent(producer);
      consumer->set_child(first_child, producer);
      producer->set_parent(consumer);
      set_child(index, consumer);
      consumer->set_parent(this);
      reinterpret_cast<ObLogExchange*>(producer)->set_to_producer();
      reinterpret_cast<ObLogExchange*>(consumer)->set_to_consumer();
      reinterpret_cast<ObLogExchange*>(producer)->set_is_remote(false);
      reinterpret_cast<ObLogExchange*>(consumer)->set_is_remote(false);
      reinterpret_cast<ObLogExchange*>(consumer)->set_task_order(exch_info.is_task_order_);

      exch_info.keep_ordering_ =
          (child->get_sharding_info().is_remote() || child->get_sharding_info().is_local()) && (1 == ctx.parallel_);

      if (exch_info.px_dop_ <= 0) {
        if (my_plan_->get_optimizer_context().get_parallel_rule() == PXParallelRule::MANUAL_TABLE_DOP) {
          int64_t dfo_table_dop = -1;
          bool found_base_table = false;
          if (OB_FAIL(calc_current_dfo_table_dop(producer->get_child(first_child), dfo_table_dop, found_base_table))) {
            LOG_WARN("failed to get current dfo table dop", K(ret));
          } else if (dfo_table_dop < 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid dfo table dop", K(ret), K(dfo_table_dop));
          } else {
            exch_info.px_dop_ = dfo_table_dop;
          }
        } else {
          exch_info.px_dop_ = ctx.parallel_;
        }
        LOG_TRACE("Exchange set dop", K(ctx.parallel_), K(exch_info.px_dop_));
      }

      ctx.add_exchange_type(AllocExchContext::DISTRIBUTED);
      ctx.exchange_allocated_ = true;

      if (child->get_sharding_info().is_single()) {
        exch_info.px_single_ = true;
        exch_info.px_dop_ = 1;
      }
      // compute equal set
      if (OB_SUCC(ret)) {
        if (OB_FAIL(reinterpret_cast<ObLogExchange*>(producer)->set_exchange_info(exch_info))) {
          LOG_WARN("failed to set exchange info for producer", K(ret));
        } else if (OB_FAIL(producer->compute_property())) {
          LOG_WARN("failed to compute equal set", K(ret));
        } else if (OB_FAIL(producer->replace_generated_agg_expr(ctx.group_push_down_replaced_exprs_))) {
          LOG_WARN("failed to replace agg expr", K(ret));
        } else if (OB_FAIL(reinterpret_cast<ObLogExchange*>(consumer)->set_exchange_info(exch_info))) {
          LOG_WARN("failed to set exchange info for producer", K(ret));
        } else if (OB_FAIL(consumer->compute_property())) {
          LOG_WARN("failed to compute equal set", K(ret));
        } else if (OB_FAIL(consumer->replace_generated_agg_expr(ctx.group_push_down_replaced_exprs_))) {
          LOG_WARN("failed to replace agg expr", K(ret));
        } else if (OB_FAIL(static_cast<ObLogExchange*>(consumer)->update_sharding_conds(ctx))) {
          LOG_WARN("failed to update sharding conditions", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_grouping_style_exchange_below(
    AllocExchContext* ctx, ObIArray<ObRawExpr*>& partition_exprs)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  sharding_info_.reset();
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(ret));
  } else if (!partition_exprs.empty()) {
    if (OB_FAIL(exch_info.append_hash_dist_expr(partition_exprs))) {
      LOG_WARN("append hash dist expr failed", K(ret));
    } else if (OB_FAIL(sharding_info_.get_partition_keys().assign(partition_exprs))) {
      LOG_WARN("failed to assign expr", K(ret));
    } else {
      exch_info.dist_method_ = ObPQDistributeMethod::HASH;
      sharding_info_.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
    }
  } else {
    exch_info.dist_method_ = ObPQDistributeMethod::MAX_VALUE;
    sharding_info_.set_location_type(OB_TBL_LOCATION_LOCAL);
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(allocate_exchange_nodes_below(first_child, *ctx, exch_info))) {
    LOG_WARN("failed to allocate exchange nodes below", K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObLogicalOperator::allocate_stmt_order_by_above(ObLogicalOperator*& top)
{
  int ret = OB_SUCCESS;
  ObLogPlan* log_plan = get_plan();
  ObSEArray<ObRawExpr*, 4> order_by_exprs;
  ObSEArray<ObOrderDirection, 8> directions;
  if (OB_ISNULL(log_plan) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected null", K(log_plan), K(top), K(ret));
  } else if (OB_FAIL(log_plan->get_order_by_exprs(*top, order_by_exprs, &directions))) {
    LOG_WARN("failed to get order by columns", K(ret));
  } else if (order_by_exprs.empty()) {
    // do nothing
  } else {
    ObLogicalOperator* old_node = top;
    ObLogicalOperator* parent = top->get_parent();
    ObSEArray<ObRawExpr*, 4> need_order_exprs;
    ObSEArray<OrderItem, 4> need_order_items;
    if (OB_FAIL(top->simplify_ordered_exprs(order_by_exprs, need_order_exprs))) {
      LOG_WARN("failed to simplify ordered exprs", K(ret));
    } else if (OB_UNLIKELY(need_order_exprs.empty())) {
      /*do nothing*/
    } else if (OB_FAIL(
                   ObOptimizerUtil::make_sort_keys(order_by_exprs, directions, need_order_exprs, need_order_items))) {
      LOG_WARN("failed to make sort keys", K(ret));
    } else if (OB_FAIL(log_plan->allocate_sort_as_top(top, need_order_items))) {
      LOG_WARN("failed to allocate sort above top", K(ret));
    } else if (OB_ISNULL(top)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(top->adjust_parent_child_relationship())) {
      LOG_WARN("failed to adjust parent-child relationship", K(ret));
    } else if (NULL != parent) {
      bool found_child = false;
      for (int64_t i = 0; OB_SUCC(ret) && !found_child && i < parent->get_num_of_child(); ++i) {
        if (parent->get_child(i) == old_node) {
          parent->set_child(i, top);
          top->set_parent(parent);
          found_child = true;
        } else { /*do nothing*/
        }
      }
    } else { /*do nothing*/
    }

    if (OB_SUCC(ret) && is_plan_root()) {
      set_is_plan_root(false);
      top->mark_is_plan_root();
      get_plan()->set_plan_root(top);
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogicalOperator::check_if_is_prefix(const ObIArray<ObRawExpr*>* order_exprs,
    const ObIArray<OrderItem>* order_items, ObLogicalOperator* op, bool& is_prefix)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Stmt should not be NULL", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 6> const_exprs;
    const ObIArray<OrderItem>* partition_order = NULL;
    EqualSets ordering_input_esets;
    if (NULL == (partition_order = get_op_ordering_in_parts(op))) {
    } else if (partition_order->count() <= 0) {
    } else if (OB_FAIL(get_ordering_input_equal_sets(ordering_input_esets))) {
      LOG_WARN("failed to get ordering input equal sets", K(ret));
    } else if (OB_FAIL(get_input_const_exprs(const_exprs))) {
      LOG_WARN("failed to get ordering input equal sets", K(ret));
    } else if (NULL != order_items) {
      if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(
              *order_items, *partition_order, ordering_input_esets, const_exprs, is_prefix))) {
        LOG_WARN("Failed to check prefix ordering", K(ret));
      }
    } else if (NULL == order_exprs) {
      ObSEArray<ObRawExpr*, 6> order_by_exprs;
      ObSEArray<ObOrderDirection, 6> directions;
      if (OB_ISNULL(get_plan()) || OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Plan is NULL", K(ret));
      } else if (OB_FAIL(get_plan()->get_order_by_exprs(*op, order_by_exprs, &directions))) {
        LOG_WARN("failed to get order by columns", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(
                     order_by_exprs, *partition_order, ordering_input_esets, const_exprs, is_prefix, &directions))) {
        LOG_WARN("Failed to check is_prefix_ordering", K(ret));
      } else {
      }  // do nothing
    } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(
                   *order_exprs, *partition_order, ordering_input_esets, const_exprs, is_prefix))) {
      LOG_WARN("Failed to check is prefix ordering", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObLogicalOperator::simplify_ordered_exprs(
    const ObIArray<OrderItem>& candi_sort_key, ObIArray<OrderItem>& opt_sort_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(get_fd_item_set(),
          get_ordering_output_equal_sets(),
          get_output_const_exprs(),
          opt_sort_key,
          candi_sort_key))) {
    LOG_WARN("failed to simplify exprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::simplify_ordered_exprs(const ObIArray<ObRawExpr*>& candi_exprs, ObIArray<ObRawExpr*>& opt_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(
          get_fd_item_set(), get_ordering_output_equal_sets(), get_output_const_exprs(), opt_exprs, candi_exprs))) {
    LOG_WARN("failed to simplify exprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::simplify_exprs(const ObIArray<ObRawExpr*>& candi_exprs, ObIArray<ObRawExpr*>& opt_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOptimizerUtil::simplify_exprs(
          get_fd_item_set(), get_ordering_output_equal_sets(), get_output_const_exprs(), opt_exprs, candi_exprs))) {
    LOG_WARN("failed to simplify exprs", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

/**
 * @brief ObLogicalOperator::check_need_sort_above_node
 * check whether need an extra sort-op to provide the expected_ordering
 * @return
 */
int ObLogicalOperator::check_need_sort_above_node(
    const ObIArray<OrderItem>& expected_order_items, bool& need_sort) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_stmt()));
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(expected_order_items,
                 get_op_ordering(),
                 get_fd_item_set(),
                 get_ordering_output_equal_sets(),
                 get_output_const_exprs(),
                 get_is_at_most_one_row(),
                 need_sort))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::check_need_sort_above_node(const ObIArray<ObRawExpr*>& expected_order_exprs,
    const ObIArray<ObOrderDirection>* expected_order_directions, bool& need_sort) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_stmt()));
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(expected_order_exprs,
                 expected_order_directions,
                 get_op_ordering(),
                 get_fd_item_set(),
                 get_ordering_output_equal_sets(),
                 get_output_const_exprs(),
                 get_is_at_most_one_row(),
                 need_sort))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::check_need_sort_below_node(
    const int64_t index, const common::ObIArray<OrderItem>& expected_order_items, bool& need)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = get_child(index);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(child->check_need_sort_above_node(expected_order_items, need))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::check_need_sort_for_local_order(
    const int64_t index, const common::ObIArray<OrderItem>* order_items, bool& need)
{
  int ret = OB_SUCCESS;
  need = true;
  ObLogicalOperator* child = get_child(index);
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt) || OB_ISNULL(order_items)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(order_items));
  } else if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(*order_items,
                 child->get_local_ordering(),
                 child->get_fd_item_set(),
                 child->get_ordering_output_equal_sets(),
                 child->get_output_const_exprs(),
                 child->get_is_at_most_one_row(),
                 need))) {
    LOG_WARN("failed to check need sort", K(ret));
  }
  return ret;
}

int ObLogicalOperator::check_need_sort_for_grouping_op(
    const int64_t index, const ObIArray<OrderItem>& order_items, bool& need_sort)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 6> sort_exprs;
  ObSEArray<ObOrderDirection, 6> directions;
  if (OB_FAIL(ObOptimizerUtil::split_expr_direction(order_items, sort_exprs, directions))) {
    LOG_WARN("failed to split expr and directions", K(ret));
  } else if (OB_FAIL(check_need_sort_for_grouping_op(index, sort_exprs, directions, need_sort))) {
    LOG_WARN("failed to check need sort for grouping op", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::check_need_sort_for_grouping_op(const int64_t index, const ObIArray<ObRawExpr*>& order_exprs,
    const ObIArray<ObOrderDirection>& directions, bool& need_sort)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  ObSEArray<ObRawExpr*, 8> temp_order_exprs;
  ObSEArray<ObOrderDirection, 8> temp_directions;
  bool dummy_ordering_used = false;
  if (OB_ISNULL(child = get_child(index)) || OB_ISNULL(child->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(temp_order_exprs.assign(order_exprs))) {
    LOG_WARN("failed to assign expr", K(ret));
  } else if (OB_FAIL(temp_directions.assign(directions))) {
    LOG_WARN("failed to assign directions", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(temp_order_exprs,
                 child->get_op_ordering(),
                 child->get_ordering_output_equal_sets(),
                 child->get_output_const_exprs(),
                 dummy_ordering_used,
                 temp_directions))) {
    LOG_WARN("failed to adjust exprs by ordering", K(ret));
  } else if (OB_FAIL(child->check_need_sort_above_node(temp_order_exprs, &temp_directions, need_sort))) {
    LOG_WARN("failed to check need sort above node", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::get_order_item_by_plan(ObIArray<OrderItem>& order_items)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObRawExpr*> order_by_exprs;
  common::ObSEArray<ObOrderDirection, 6> directions;
  if (OB_FAIL(get_plan()->get_order_by_exprs(*this, order_by_exprs, &directions))) {
    LOG_WARN("failed to get order by columns", K(ret));
  } else if (OB_FAIL(get_plan()->make_order_items(order_by_exprs, directions, order_items))) {
    LOG_WARN("failed to make order items", K(ret));
  }
  return ret;
}

const ObIArray<OrderItem>* ObLogicalOperator::get_op_ordering_in_parts(ObLogicalOperator* op)
{
  int ret = OB_SUCCESS;
  const ObIArray<OrderItem>* order_key = NULL;
  int64_t child_idx = 0;
  while (OB_SUCC(ret) && NULL != op) {
    if (op->is_keep_input_ordering()) {
      // For logical op have more than one child,
      // only keep left child ording, thought as no order change.
      if (instance_of_log_table_scan(op->get_type())) {
        if (static_cast<ObLogTableScan*>(op)->get_order_with_parts().count() > 0) {
          order_key = &(static_cast<ObLogTableScan*>(op)->get_order_with_parts());
        }
        break;
      } else {
        op = op->get_child(child_idx);
      }
    } else {
      break;
    }
  }
  return order_key;
}

bool ObLogicalOperator::is_keep_input_ordering()
{
  bool no_order_change = false;
  if ((instance_of_log_table_scan(type_)) || LOG_LIMIT == type_ || LOG_SUBPLAN_FILTER == type_ ||
      LOG_DISTINCT == type_ || LOG_MATERIAL == type_) {
    no_order_change = true;
  } else if (LOG_GROUP_BY == type_) {
    const ObLogGroupBy* op = static_cast<const ObLogGroupBy*>(this);
    if (MERGE_AGGREGATE == op->get_algo()) {
      no_order_change = true;
    }
  } else if (LOG_JOIN == type_) {
    // As we not support wise join, this not used now.
    // When distribute, will pull data to current server.
    // TODO, wise-join and join with distribute data,
    // set exchange to partition_order
    const ObLogJoin* op = static_cast<const ObLogJoin*>(this);
    if (NESTED_LOOP_JOIN == op->get_join_algo() &&
        (INNER_JOIN == op->get_join_type() || LEFT_OUTER_JOIN == op->get_join_type() ||
            LEFT_SEMI_JOIN == op->get_join_type() || LEFT_ANTI_JOIN == op->get_join_type())) {
      no_order_change = true;
    } else if (MERGE_JOIN == op->get_join_algo() &&
               (INNER_JOIN == op->get_join_type() || LEFT_OUTER_JOIN == op->get_join_type() ||
                   RIGHT_OUTER_JOIN == op->get_join_type())) {
      no_order_change = true;
    }
  } else {
  }  // do nothing
  return no_order_change;
}

// under Gi operator, find the table scan whose table id is same as the argument table id
int ObLogicalOperator::get_table_scan(ObLogicalOperator*& tsc, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  // Under GI, there are no GI, exchange
  if (LOG_GRANULE_ITERATOR == get_type() || LOG_EXCHANGE == get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operator type", K(get_type()), K(ret));
  } else if (nullptr == tsc && instance_of_log_table_scan(get_type()) &&
             table_id == static_cast<ObLogTableScan*>(this)->get_table_id()) {
    tsc = this;
  }
  for (int64_t i = 0; OB_SUCC(ret) && nullptr == tsc && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get_child(i) is null", K(ret), K(i));
    } else if (OB_FAIL(SMART_CALL(get_child(i)->get_table_scan(tsc, table_id)))) {
      LOG_WARN("failed to set op ordering in parts recursively", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_material(const int64_t index)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = get_child(index);
  ObLogPlan* plan = get_plan();
  ObLogPlan* child_plan = NULL;
  ObLogicalOperator* op = NULL;
  if (OB_ISNULL(child) || OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child or plan is NULL", K(ret), K(child), K(plan));
  } else if (OB_ISNULL(child_plan = child->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child plan is NULL", K(ret));
  } else {
    ObLogOperatorFactory& factory = plan->get_log_op_factory();
    if (OB_ISNULL(op = factory.allocate(*child_plan, log_op_def::LOG_MATERIAL))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate material operator", K(ret));
    } else {
      op->set_child(0, child);
      child->set_parent(op);
      op->set_parent(this);
      set_child(index, op);
      if (OB_FAIL(op->compute_property())) {
        LOG_WARN("failed to compute property");
      }
      if (child->is_plan_root()) {
        child->set_is_plan_root(false);
        op->mark_is_plan_root();
        child_plan->set_plan_root(op);
      }
    }
  }
  return ret;
}

int ObLogicalOperator::check_and_allocate_material(const int64_t index)
{
  int ret = OB_SUCCESS;
  bool need_material = false;
  if (OB_ISNULL(get_child(index))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_child(index)->check_exchange_rescan(need_material))) {
    LOG_WARN("get_child(index)->check_exchange_rescan() fails", K(ret));
  } else if (need_material) {
    ret = allocate_material(index);
    LOG_TRACE("succeed to allocate materialize op", K(ret));
  } else { /* Do nothing */
  }
  return ret;
}

int ObLogicalOperator::check_exchange_rescan(bool& need_rescan)
{
  int ret = OB_SUCCESS;
  need_rescan = false;
  if (LOG_EXCHANGE == get_type()) {
    need_rescan = true;
  } else if (LOG_MATERIAL == get_type()) {
    /*no nothing, stop checking deeply*/
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !need_rescan && i < get_num_of_child(); ++i) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_child(i) returns null", K(i), K(ret));
      } else if (OB_FAIL(get_child(i)->check_exchange_rescan(need_rescan))) {
        LOG_WARN("get_child(i)->check_exchange_rescan() fails", K(ret));
      } else { /* Do nothing */
      }
    }
  }
  return ret;
}

int ObLogicalOperator::check_has_exchange_below(bool& has_exchange) const
{
  int ret = OB_SUCCESS;
  has_exchange = false;
  if (LOG_EXCHANGE == get_type()) {
    has_exchange = true;
  } else {
    ObLogicalOperator* child_op = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !has_exchange && i < get_num_of_child(); ++i) {
      if (OB_ISNULL(child_op = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_child(i) returns null", K(i), K(ret));
      } else if (OB_FAIL(child_op->check_has_exchange_below(has_exchange))) {
        LOG_WARN("failed to check if child operator has exchange below", KPC(child_op), K(ret));
      } else { /* Do nothing */
      }
    }
  }
  return ret;
}

int ObLogicalOperator::all_expr_produced(ObIArray<ExprProducer>* ctx, bool& all_produced)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret));
  } else {
    all_produced = true;
    int64_t N = ctx->count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (ctx->at(i).not_produced()) {
        all_produced = false;
        // LOG_PRINT_EXPR's expr param can be NULL.
        LOG_PRINT_EXPR(
            WARN, "Failed to produce expression", ctx->at(i).expr_, "producer", ctx->at(i), "Operator", get_name());
      } else {
        LOG_PRINT_EXPR(DEBUG, "Succ to produce expression", ctx->at(i).expr_, "Operator", get_name());
      }
    }
  }
  return ret;
}

uint64_t ObLogicalOperator::hash(uint64_t seed) const
{
  seed = do_hash(type_, seed);
  seed = hash_output_exprs(seed);
  seed = hash_filter_exprs(seed);
  seed = hash_pushdown_filter_exprs(seed);
  seed = hash_startup_exprs(seed);
  seed = do_hash(id_, seed);
  seed = do_hash(get_num_of_child(), seed);

  LOG_TRACE("operator hash", K(get_op_name(type_)));
  return seed;
}

bool ObLogicalOperator::has_expr_as_input(const ObRawExpr* expr) const
{
  bool has_expr = false;
  for (int i = 0; !has_expr && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      LOG_WARN("get_child(i) returns null", K(i));
    } else if (ObOptimizerUtil::find_item(get_child(i)->get_output_exprs(), expr)) {
      has_expr = true;
    } else { /*do nothing*/
    }
  }
  return has_expr;
}

int ObLogicalOperator::numbering_operator_pre(NumberingCtx& ctx)
{
  int ret = OB_SUCCESS;
  op_id_ = ctx.op_id_++;
  plan_depth_ = ctx.plan_depth_++;
  if (LOG_COUNT == get_type()) {
    ObRawExpr* rownum_expr = NULL;
    if (OB_ISNULL(get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is null", K(ret));
    } else if (OB_FAIL(get_stmt()->get_rownum_expr(rownum_expr))) {
      LOG_WARN("get rownum expr failed", K(ret));
    } else if (OB_ISNULL(rownum_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no rownum expr in stmt of count operator", K(ret));
    } else {
      ObSysFunRawExpr* sys_rownum_expr = static_cast<ObSysFunRawExpr*>(rownum_expr);
      sys_rownum_expr->set_op_id(op_id_);
    }
  }
  if (ctx.going_up_) {
    ctx.branch_id_++;
    ctx.going_up_ = false;
  } else { /* Do nothing */
  }

  if (OB_INVALID_ID == branch_id_) {
    branch_id_ = ctx.branch_id_;
  } else { /* Do nothing */
  }
  return ret;
}

void ObLogicalOperator::numbering_operator_post(NumberingCtx& ctx)
{
  id_ = ctx.num_++;
  ctx.plan_depth_--;
  if (!ctx.going_up_) {
    ctx.going_up_ = true;
  } else { /* Do nothing */
  }
}

int ObLogicalOperator::alloc_md_post(AllocMDContext& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObQueryCtx* query_ctx = nullptr;
  if (OB_ISNULL(stmt_) || OB_ISNULL(query_ctx = stmt_->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else {
    const ObIArray<ObMonitorHint>& monitor_ids = query_ctx->get_monitor_ids();
    ARRAY_FOREACH(monitor_ids, idx)
    {
      if (monitor_ids.at(idx).id_ == op_id_) {
        if (OB_FAIL(allocate_monitoring_dump_node_above(monitor_ids.at(idx).flags_, op_id_))) {
          LOG_WARN("Failed to allocate monitoring dump", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::numbering_exchange_pre(NumberingExchangeCtx& ctx)
{
  int ret = OB_SUCCESS;
  if (LOG_EXCHANGE == type_) {
    ObLogExchange* exchange_op = static_cast<ObLogExchange*>(this);
    if (exchange_op->is_px_consumer() && exchange_op->is_rescanable()) {
      ret = ctx.push_px(ctx.next_px());
    }
  }
  return ret;
}

int ObLogicalOperator::numbering_exchange_post(NumberingExchangeCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (LOG_EXCHANGE == type_) {
    ObLogExchange* exchange_op = static_cast<ObLogExchange*>(this);
    if (exchange_op->is_px_producer()) {
      int64_t px_id = OB_INVALID_ID;
      int64_t dfo_id = OB_INVALID_ID;
      if (OB_FAIL(ctx.next_dfo(px_id, dfo_id))) {
        LOG_WARN("get next dfo id fail", K(ret));
      } else {
        exchange_op->set_dfo_id(dfo_id);
        exchange_op->set_px_id(px_id);
      }
    } else if (exchange_op->is_px_consumer() && exchange_op->is_rescanable()) {
      ret = ctx.pop_px();
    }
  }
  return ret;
}

int ObLogicalOperator::px_estimate_size_factor_pre()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(parent_)) {
    PxOpSizeFactor parent_factor = parent_->px_est_size_factor_;
    if (LOG_EXCHANGE != type_ && parent_factor.has_granule_child_factor()) {
      px_est_size_factor_.merge_factor(parent_factor.get_granule_child_factor());
    }
  }
  if (LOG_GRANULE_ITERATOR == type_) {
    bool partition_granule = false;
    ObLogGranuleIterator* log_gi = static_cast<ObLogGranuleIterator*>(this);
    if (OB_FAIL(log_gi->is_partition_gi(partition_granule))) {
      LOG_WARN("failed to judge partition gi", K(ret));
    } else {
      if (partition_granule) {
        px_est_size_factor_.partition_granule_child_ = true;
      } else {
        px_est_size_factor_.block_granule_child_ = true;
      }
    }
  }
  return ret;
}

int ObLogicalOperator::px_estimate_size_factor_post()
{
  int ret = OB_SUCCESS;
  // first process self, then some special process like exchange,join, then process parent
  if (instance_of_log_table_scan(type_) || LOG_TABLE_LOOKUP == type_) {
    if (!px_est_size_factor_.has_granule()) {
      px_est_size_factor_.single_partition_table_scan_ = true;
    }
  } else if (LOG_GRANULE_ITERATOR == type_) {
    bool partition_granule = false;
    ObLogGranuleIterator* log_gi = static_cast<ObLogGranuleIterator*>(this);
    px_est_size_factor_.revert_leaf_factor();
    if (OB_FAIL(log_gi->is_partition_gi(partition_granule))) {
      LOG_WARN("failed to judge partition gi", K(ret));
    } else {
      if (partition_granule) {
        px_est_size_factor_.partition_granule_parent_ = true;
      } else {
        px_est_size_factor_.block_granule_parent_ = true;
      }
    }
  } else if (LOG_EXCHANGE == type_) {
    ObLogExchange* exchange = static_cast<ObLogExchange*>(this);
    if (exchange->is_px_producer()) {
      px_est_size_factor_.revert_all();
      if (ObPQDistributeMethod::BROADCAST == exchange->get_dist_method()) {
        px_est_size_factor_.broadcast_exchange_ = true;
      } else if (ObPQDistributeMethod::PARTITION == exchange->get_dist_method()) {
        px_est_size_factor_.pk_exchange_ = true;
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(parent_)) {
    PxOpSizeFactor child_factor = px_est_size_factor_;
    if (1 < parent_->get_num_of_child()) {
      if (LOG_JOIN == parent_->type_) {
        ObLogJoin* join = static_cast<ObLogJoin*>(parent_);
        // revert right of hash join exchange factor
        if (JoinAlgo::HASH_JOIN == join->get_join_algo() && join->get_child(second_child) == this) {
          child_factor.revert_exchange();
        }
      }
    }
    if (1 < get_num_of_child()) {
      child_factor.revert_exchange();
    }
    parent_->px_est_size_factor_.merge_factor(child_factor);
    LOG_TRACE("trace estimate size factor",
        K(id_),
        "op factor",
        px_est_size_factor_,
        "parent factor",
        parent_->px_est_size_factor_);
  }
  return ret;
}

int ObLogicalOperator::px_rescan_pre()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("px rescan pre", KP(this), "operator", get_name(), "is_plan_root", is_plan_root(), KP(get_parent()));
  if (is_plan_root() && nullptr != get_parent()) {
    LOG_TRACE("root parent info", KP(this), KP(get_parent()), "name", get_parent()->get_name());
  }

  if ((is_plan_root() && nullptr == get_parent()) ||
      (NULL != get_parent() && get_parent()->get_type() == LOG_TEMP_TABLE_TRANSFORMATION)) {
    if (LOG_EXCHANGE == type_) {
      ObLogExchange* exchange_op = static_cast<ObLogExchange*>(this);
      if (exchange_op->is_consumer()) {
        exchange_op->set_rescanable(true);
      }
    } else {
      for (int64_t i = first_child; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
        if (OB_ISNULL(get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get_child(i) returns null", K(i), K(ret));
        } else if (OB_FAIL(get_child(i)->mark_child_exchange_rescanable())) {
          LOG_WARN("mark child ex-receive as px op fail", K(ret));
        }
      }
    }
  } else if (LOG_SUBPLAN_FILTER == type_) {
    if (OB_FAIL(check_subplan_filter_child_exchange_rescanable())) {
      LOG_WARN("mark child ex-receive as px op fail", K(ret));
    }
  } else if (LOG_JOIN == type_ && JoinAlgo::NESTED_LOOP_JOIN == static_cast<ObLogJoin*>(this)->get_join_algo() &&
             !(static_cast<ObLogJoin*>(this)->get_join_type() == CONNECT_BY_JOIN &&
                 static_cast<ObLogJoin*>(this)->is_nlj_without_param_down())) {
    if (OB_ISNULL(get_child(second_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (LOG_MATERIAL == get_child(second_child)->get_type()) {
      /*do nothing*/
    } else if (OB_FAIL(get_child(second_child)->mark_child_exchange_rescanable())) {
      LOG_WARN("mark child ex-receive as px op fail", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::mark_child_exchange_rescanable()
{
  int ret = OB_SUCCESS;
  if (LOG_EXCHANGE == get_type()) {
    ObLogExchange* exchange_op = static_cast<ObLogExchange*>(this);
    if (exchange_op->is_consumer()) {
      exchange_op->set_rescanable(true);
    }
    // no more iterations for the subtree as we should only
    // mark the top exchange rescanable. other dfos belongs
    // to this px tree
  } else {
    for (int64_t i = first_child; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_child(i) returns null", K(i), K(ret));
      } else if (OB_FAIL(SMART_CALL(get_child(i)->mark_child_exchange_rescanable()))) {
        LOG_WARN("mark child ex-receive as px op fail", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_dummy_access()
{
  int ret = OB_SUCCESS;

  ObLogTableScan* myself = static_cast<ObLogTableScan*>(this);
  /* if we are 'lucky', we may have one in the column items. */
  ObDMLStmt* stmt = NULL;
  ObLogPlan* plan = NULL;
  ObOptimizerContext* ctx = NULL;
  const ObTableSchema* index_schema = NULL;
  const TableItem* table_item = NULL;
  if (OB_UNLIKELY(!(instance_of_log_table_scan(type_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Only table scan operator can call this func", K(ret));
  } else if (OB_ISNULL(plan = get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
             OB_ISNULL(ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(plan), K(stmt), K(ctx));
  } else if (myself->get_is_fake_cte_table()) {
    ObConstRawExpr* dummy_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(ctx->get_expr_factory(), ObIntType, 1, dummy_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_ISNULL(dummy_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dummy expr is null");
    } else if (OB_FAIL(dummy_expr->extract_info())) {
      LOG_WARN("extract expr info failed", K(ret));
    } else if (OB_FAIL(myself->get_access_exprs().push_back(dummy_expr))) {
      LOG_WARN("push back to access expr failed", K(ret));
    }
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(myself->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table item by table id failed", K(myself->get_table_id()));
  } else {
    const ObColumnSchemaV2* column_schema = NULL;
    if (is_link_table_id(myself->get_ref_table_id())) {
      const ObTableSchema* lt_schema = NULL;
      // select 1 from dblink.t1@link; don't have row so get the first column
      ObSqlSchemaGuard* sg = ctx->get_sql_schema_guard();
      if (OB_ISNULL(sg) || OB_FAIL(sg->get_table_schema(myself->get_ref_table_id(), lt_schema)) ||
          OB_ISNULL(lt_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("failed to get dblink schema guard", K(ret), K(myself->get_ref_table_id()));
      } else if (OB_UNLIKELY(lt_schema->get_column_count() <= 0)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("table schema has no column", K(ret));
      } else {
        ObTableSchema::const_column_iterator cs_iter = lt_schema->column_begin();
        ObTableSchema::const_column_iterator cs_iter_end = lt_schema->column_end();
        for (; OB_SUCC(ret) && cs_iter != cs_iter_end; cs_iter++) {
          CK(OB_NOT_NULL(*cs_iter));
          if (OB_SUCC(ret)) {
            const ObColumnSchemaV2& column_schema1 = **cs_iter;
            if (column_schema1.is_hidden() || column_schema1.is_invisible_column()) {
              continue;
            } else {
              column_schema = static_cast<const ObColumnSchemaV2*>(*cs_iter);
              break;
            }
          }
        }
      }
      if (OB_ISNULL(column_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("table schema has no column", K(ret));
      }
    } else {
      /* get index schema first */
      ObSchemaGetterGuard* schema_guard = ctx->get_schema_guard();
      if (OB_ISNULL(schema_guard) ||
          OB_FAIL(schema_guard->get_table_schema(myself->get_index_table_id(), index_schema)) ||
          OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("Get unexpected null", K(ret), K(schema_guard), K(index_schema));
      } else if (OB_UNLIKELY(index_schema->get_column_count() <= 0)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("table schema has no column", K(ret));
      }
    }

    /* OK. we have to generate one column expr ourselves. */
    if (OB_SUCC(ret)) {
      if (is_link_table_id(myself->get_ref_table_id())) {
        // do nothing
      } else {
        // Get rowkey's first column.If no rowkey, than get first column of index schema.
        uint64_t column_id = OB_INVALID_ID;
        ObSEArray<share::schema::ObColDesc, 4> col_ids;
        const bool no_virtual = true;
        if (OB_FAIL(index_schema->get_column_ids(col_ids, no_virtual))) {
          LOG_WARN("get_column_ids failed", K(ret));
        } else if (OB_UNLIKELY(col_ids.count() <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid col_ids", K(ret), K(col_ids));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < col_ids.count(); ++i) {
            column_id = col_ids.at(i).col_id_;
            if (OB_ISNULL(column_schema = index_schema->get_column_schema(column_id))) {
              ret = OB_SCHEMA_ERROR;
              LOG_WARN("Failed to get column schema from index_schema", K(ret), K(column_id));
            } else if (column_schema->is_virtual_generated_column() || column_schema->is_rowid_pseudo_column()) {
              column_schema = NULL;
              LOG_DEBUG("got virtual column ignore", K(ret));
            } else {
              break;
            }
          }
          if (OB_ISNULL(column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column_schema is NULL", K(ret), K(col_ids));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObColumnRefRawExpr* expr = NULL;
        ObRawExpr* default_value_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_column_expr(ctx->get_expr_factory(), *column_schema, expr))) {
          LOG_WARN("build column expr failed", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to create raw expr for dummy output", K(ret));
        } else if (OB_FAIL(expr->formalize(const_cast<ObSQLSessionInfo*>(ctx->get_session_info())))) {
          LOG_WARN("formalize expr failed", K(ret));
        } else if (OB_FAIL(myself->get_access_exprs().push_back(expr))) {
          LOG_WARN("failed to add access exprs", K(ret));
        } else if (OB_FAIL(plan->get_all_exprs().append(static_cast<ObRawExpr*>(expr)))) {
          LOG_WARN("fail to add dummy access expr", K(ret));
        } else {
          LOG_TRACE("add a dummy access expr",
              K(expr),
              "table_id",
              myself->get_table_id(),
              "index_id",
              myself->get_index_table_id());
          ColumnItem dummy_col_item;
          expr->set_ref_id(table_item->table_id_, column_schema->get_column_id());
          expr->set_column_attr(table_item->get_table_name(), column_schema->get_column_name_str());
          dummy_col_item.table_id_ = expr->get_table_id();
          dummy_col_item.column_id_ = expr->get_column_id();
          dummy_col_item.column_name_ = expr->get_column_name();
          dummy_col_item.set_default_value(column_schema->get_cur_default_value());
          dummy_col_item.set_default_value_expr(default_value_expr);
          dummy_col_item.expr_ = expr;
          LOG_TRACE("fill dummy_col_item", K(dummy_col_item), K(column_schema->is_default_expr_v2_column()), K(lbt()));
          if (OB_FAIL(stmt->add_column_item(dummy_col_item))) {
            LOG_WARN("add column item to stmt failed", K(ret));
          } else if (share::is_oracle_mapping_real_virtual_table(myself->get_ref_table_id())) {
            if (OB_FAIL(myself->add_mapping_column_for_vt(expr))) {
              LOG_WARN("failed to add mapping column for virtual table", K(ret));
            } else if (OB_ISNULL(expr->get_real_expr())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("real expr is null", K(ret));
            } else if (OB_FAIL(plan->get_all_exprs().append(expr->get_real_expr()))) {
              LOG_WARN("fail to add dummy access expr", K(ret));
            }
          }
        }
      }
    } else { /* Do nothing */
    }
  }

  return ret;
}
int ObLogicalOperator::allocate_dummy_output()
{
  int ret = OB_SUCCESS;
  ObLogPlan* plan = NULL;
  ObOptimizerContext* opt_ctx = NULL;
  ObConstRawExpr* dummy_expr = NULL;
  if (OB_ISNULL(plan = get_plan()) || OB_ISNULL(opt_ctx = &plan->get_optimizer_context()) ||
      OB_ISNULL(opt_ctx->get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(plan), K(opt_ctx));
  } else if (OB_FAIL(opt_ctx->get_expr_factory().create_raw_expr(T_INT, dummy_expr))) {
    LOG_WARN("create raw expr failed", K(ret));
  } else if (OB_ISNULL(dummy_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dummy expr is null");
  } else {
    ObObj val;
    val.set_int(1);
    dummy_expr->set_value(val);
    if (OB_FAIL(dummy_expr->formalize(opt_ctx->get_session_info()))) {
      LOG_WARN("dummy expr formalize failed", K(ret));
    } else if (OB_FAIL(output_exprs_.push_back(dummy_expr))) {
      LOG_WARN("push back to output expr failed", K(ret));
    } else if (OB_FAIL(plan->get_all_exprs().append(static_cast<ObRawExpr*>(dummy_expr)))) {
      LOG_WARN("fail to add dummy output expr", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_dummy_output_access()
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(get_plan()), K(opt_ctx));
  } else {
    if (IS_EXPR_PASSBY_OPER(type_)) {
      if (!is_plan_root()) {
        if (OB_ISNULL(get_child(first_child))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get_child(first_child) returns unexpected null", K(ret));
        } else if (get_child(first_child)->is_dml_operator() &&
                   !static_cast<ObLogDelUpd*>(get_child(first_child))->is_returning()) {
        } else if (type_ == log_op_def::LOG_GRANULE_ITERATOR &&
                   get_plan()->get_optimizer_context().is_batched_multi_stmt()) {
          ret = append_array_no_dup(output_exprs_, get_child(first_child)->get_output_exprs());
        } else {
          ObRawExpr* partition_id_expr = NULL;
          if (log_op_def::LOG_EXCHANGE == type_) {
            for (int64_t i = 0; i < output_exprs_.count() && OB_ISNULL(partition_id_expr); i++) {
              ObRawExpr* expr = output_exprs_.at(i);
              if (expr->get_expr_type() == T_PDML_PARTITION_ID) {
                partition_id_expr = expr;
              }
            }
          }
          if (OB_FAIL(output_exprs_.assign(get_child(first_child)->get_output_exprs()))) {
            LOG_WARN("copy_exprs fails", K(ret));
          } else if (OB_NOT_NULL(partition_id_expr)) {
            LOG_DEBUG("append the partition id expr to output exprs", K(*partition_id_expr));
            if (OB_FAIL(add_var_to_array_no_dup(output_exprs_, partition_id_expr))) {
              LOG_WARN("failed to add partition id expr to output exprs", K(ret));
            }
          } else {
            LOG_DEBUG("succ to assign output_exprs_", K(output_exprs_));
          }
        }
      } else { /* Do nothing */
      }
    } else if (!is_dml_operator()) {
      /*
       * If it is table scan without any access exprs, we need to generate one
       */
      if (OB_SUCC(ret)) {
        if (instance_of_log_table_scan(type_)) {
          if (0 == static_cast<ObLogTableScan*>(this)->get_access_exprs().count() && OB_FAIL(allocate_dummy_access())) {
            LOG_WARN("Allocate dummy access error");
          } else { /* Do nothing */
          }
          // As some access column may removed in PROJECT_PRUNING and add in DUMMY, so index_back_check need to check
          // after dummy access. Like sql has partitioned table, but need not to access partition expr and some table
          // scan in join need not to access any expr before dummy.
          //@TODO Estimating access path cost need to consider partition expr to estimate index_back more exactly.
          if (OB_SUCC(ret) && OB_FAIL(static_cast<ObLogTableScan*>(this)->index_back_check())) {
            LOG_WARN("Index back check error", K(ret));
          } else { /* Do nothing */
          }
        } else { /* Do nothing */
        }
      } else { /* Do nothing */
      }

      if (OB_SUCC(ret) && 0 == output_exprs_.count()) {
        ret = allocate_dummy_output();
      } else { /* Do nothing */
      }
    } else { /* Do nothing */
    }
  }

  if (OB_SUCC(ret) && LOG_TABLE_LOOKUP == type_) {
    ObLogTableScan* index_back_scan = static_cast<ObLogTableLookup*>(this)->get_index_back_scan();
    if (OB_ISNULL(index_back_scan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(index_back_scan), K(ret));
    } else if (OB_FAIL(index_back_scan->allocate_dummy_output_access())) {
      LOG_WARN("failed to allocate dummy output access", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogicalOperator::check_output_dep(ObIArray<ObRawExpr*>& child_output, PPDeps& deps)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("start to check output exprs", K(type_), K(output_exprs_), K(filter_exprs_), K(child_output), K(deps));
  ObRawExprCheckDep dep_checker(child_output, deps, is_table_scan());
  if (OB_FAIL(check_output_dep_common(dep_checker))) {
    LOG_WARN("check_output_dep_common fails", K(ret));
  } else if (OB_FAIL(check_output_dep_specific(dep_checker))) {
    LOG_WARN("check_output_dep_specific fails", K(ret));
  } else {
    LOG_TRACE("succeed to check output exprs", K(type_), K(deps));
  }
  return ret;
}

int ObLogicalOperator::check_output_dep_common(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  // output
  for (int64_t i = 0; OB_SUCC(ret) && i < output_exprs_.count(); i++) {
    if (OB_ISNULL(output_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output_exprs_.at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*output_exprs_.at(i)))) {
      LOG_WARN("failed to check output_exprs_.at(i)", K(ret), K(i));
    } else {
    }
  }
  // filters
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs_.count(); i++) {
    if (OB_ISNULL(filter_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("filter_exprs_.at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*filter_exprs_.at(i)))) {
      LOG_WARN("failed to check filter_exprs_.at(i)", K(ret), K(i));
    } else {
    }
  }
  return ret;
}

int ObLogicalOperator::project_pruning_pre()
{
  int ret = OB_SUCCESS;
  bool insert_without_prune =
      ((LOG_INSERT == type_ || LOG_MERGE == type_) && !static_cast<ObLogInsert*>(this)->is_returning() &&
          !static_cast<ObLogInsert*>(this)->is_pdml());
  if (LOG_TABLE_SCAN == type_ && static_cast<ObLogTableScan*>(this)->get_is_global_index_back() &&
      static_cast<ObLogTableScan*>(this)->get_is_index_global()) {
    /*do nothing*/
  } else {
    if (NULL != parent_ && !is_plan_root() && !insert_without_prune) {
      PPDeps deps;
      if (OB_FAIL(parent_->check_output_dep(get_output_exprs(), deps))) {
        LOG_WARN("parent_->check_output_dep() fails", K(ret));
      } else {
        project_pruning(get_output_exprs(),
            deps,
            "output expr is pruned beca use: "
            "neither my parent output_exprs nor filter_exprs is dependent on it");
      }
    } else { /* Do nothing */
    }

    if (OB_SUCC(ret) && LOG_TABLE_SCAN == type_ && !static_cast<ObLogTableScan*>(this)->is_for_update() &&
        static_cast<ObLogTableScan*>(this)->get_access_exprs().count() > 1) {
      PPDeps deps;
      ObLogTableScan* table_scan = static_cast<ObLogTableScan*>(this);
      if (OB_FAIL(check_output_dep(table_scan->get_access_exprs(), deps))) {
        LOG_WARN("check_output_dep fails", K(ret));
      } else {
        project_pruning(table_scan->get_access_exprs(),
            deps,
            "access expr is pruned because: "
            "neither my output_exprs nor filter_exprs is dependent on it");
      }
    }
  }
  if (OB_SUCC(ret) && LOG_TABLE_LOOKUP == type_) {
    ObLogTableScan* index_back_scan = static_cast<ObLogTableLookup*>(this)->get_index_back_scan();
    if (OB_ISNULL(index_back_scan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(index_back_scan), K(ret));
    } else if (OB_FAIL(index_back_scan->project_pruning_pre())) {
      LOG_WARN("failed to do project pruning", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

void ObLogicalOperator::project_pruning(ObIArray<ObRawExpr*>& exprs, PPDeps& deps, const char* reason)
{
  int64_t i = 0;
  int64_t j = 0;
  LOG_TRACE("start to do project pruning", K(type_), K(exprs), K(deps));
  for (i = 0, j = 0; i < exprs.count(); i++) {
    if (deps.has_member(static_cast<int32_t>(i)) || T_ORA_ROWSCN == exprs.at(i)->get_expr_type()) {
      exprs.at(j++) = exprs.at(i);
    } else {
      LOG_TRACE("project pruning remove expr",
          K(exprs.at(i)),
          K(*exprs.at(i)),
          K(i),
          K(id_),
          K(get_name()),
          "reason",
          ObString(reason),
          K(lbt()));
    }
  }
  // remove the extra ones
  while (i > j) {
    exprs.pop_back();
    i--;
  }
}

int ObLogicalOperator::add_plan_root_exprs(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObLogPlan* plan = NULL;
  ObDMLStmt* stmt = NULL;
  uint64_t producer_id = OB_INVALID_ID;
  if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(plan = get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(plan), K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt* stmt = static_cast<ObSelectStmt*>(get_stmt());
    if (stmt->is_select_into_outfile()) {
      // do nothing
    } else {
      ret = stmt->get_select_exprs(output_exprs_, LOG_UNPIVOT == type_ && stmt->is_unpivot_select());
    }
    LOG_DEBUG("finish to get select exprs", "operator", get_name(), K(output_exprs_), KPC(stmt));
  } else if (stmt->is_returning()) {
    ObDelUpdStmt* del_upd_stmt = static_cast<ObDelUpdStmt*>(stmt);
    if (OB_FAIL(append(output_exprs_, del_upd_stmt->get_returning_exprs()))) {
      LOG_WARN("failed to append returning exprs into output", K(ret));
    }
  } else { /*do nothing*/
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < output_exprs_.count(); ++i) {
    if (OB_FAIL(replace_expr_action(ctx.group_replaced_exprs_, output_exprs_.at(i)))) {
      LOG_WARN("failed to replacer old aggr expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && !output_exprs_.empty()) {
    if (OB_FAIL(add_exprs_to_ctx(ctx, output_exprs_))) {
      LOG_WARN("failed to add exprs to context", K(ret));
    } else {
      LOG_TRACE("succeed to add plan root output exprs", K(output_exprs_), K(producer_id));
    }
  }
  return ret;
}

int ObLogicalOperator::replace_generated_agg_expr(const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (0 < to_replace_exprs.count()) {
    FOREACH_CNT_X(it, get_op_ordering(), OB_SUCC(ret))
    {
      if (OB_FAIL(replace_expr_action(to_replace_exprs, it->expr_))) {
        LOG_WARN("replace agg expr failed", K(ret));
      }
    }
    FOREACH_CNT_X(it, get_local_ordering(), OB_SUCC(ret))
    {
      if (OB_FAIL(replace_expr_action(to_replace_exprs, it->expr_))) {
        LOG_WARN("replace agg expr failed", K(ret));
      }
    }
    FOREACH_CNT_X(it, get_expected_ordering(), OB_SUCC(ret))
    {
      if (OB_FAIL(replace_expr_action(to_replace_exprs, it->expr_))) {
        LOG_WARN("replace agg expr failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(replace_exprs_action(to_replace_exprs, get_filter_exprs()))) {
      LOG_WARN("failed to replace agg expr", K(ret));
    } else if (OB_FAIL(replace_exprs_action(to_replace_exprs, get_output_exprs()))) {
      LOG_WARN("failed to replace agg expr", K(ret));
    } else if (OB_FAIL(inner_replace_generated_agg_expr(to_replace_exprs))) {
      LOG_WARN("failed to inner replace agg expr", K(ret));
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }
  return ret;
}

int ObLogicalOperator::inner_replace_generated_agg_expr(
    const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  // do operator specific
  UNUSED(to_replace_exprs);
  return ret;
}

/*
 * pair: orig_expr  new_expr
 */
int ObLogicalOperator::replace_exprs_action(
    const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& to_replace_exprs, ObIArray<ObRawExpr*>& dest_exprs)
{
  int ret = OB_SUCCESS;
  int64_t src_num = to_replace_exprs.count();
  int64_t dest_num = dest_exprs.count();
  if (src_num > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_num; ++i) {
      ObRawExpr*& cur_expr = dest_exprs.at(i);
      if (OB_FAIL(replace_expr_action(to_replace_exprs, cur_expr))) {
        LOG_WARN("failed to do replace agg expr action", K(ret));
      } else { /* Do nothing */
      }
    }
  } else { /* Do nothing */
  }
  return ret;
}

int ObLogicalOperator::replace_expr_action(
    const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& to_replace_exprs, ObRawExpr*& dest_expr)
{
  int ret = OB_SUCCESS;
  int64_t src_num = to_replace_exprs.count();
  if (src_num > 0) {
    int64_t N = to_replace_exprs.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (dest_expr == to_replace_exprs.at(i).first) {
        dest_expr = to_replace_exprs.at(i).second;
      } else {
        ObRawExprReplacer replacer(to_replace_exprs.at(i).first, to_replace_exprs.at(i).second);
        if (OB_ISNULL(dest_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest_expr is null", K(ret));
        } else if (OB_FAIL(replacer.replace(*dest_expr))) {
          LOG_WARN("failed to replace expr", K(ret));
        } else { /* Do nothing */
        }
      }
    }
  } else { /* Do nothing */
  }
  return ret;
}

// use the direction of order_by exprs if exists. otherwise use ASC;invoke by distinct or group by
// operator who do not care the output direction;using direction of order_by exprs may avoid one sorting
int ObLogicalOperator::make_order_keys(ObIArray<ObRawExpr*>& exprs, ObIArray<OrderItem>& order_keys)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got plan is NULL", K(ret));
  } else if (OB_FAIL(get_plan()->make_order_items(exprs, order_keys))) {
    LOG_WARN("failed to make order items", K(ret));
  } else {
  }
  return ret;
}

int ObLogicalOperator::print_qb_name(planText& plan_text)
{
  int ret = OB_SUCCESS;
  ObString stmt_name;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  OutlineType type = plan_text.outline_type_;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL", K(ret));
  } else if (USED_HINT == type && OB_FAIL(stmt_->get_stmt_name(stmt_name))) {
    LOG_WARN("fail to get stmt name", K(ret));
  } else if (OUTLINE_DATA == type && OB_FAIL(stmt_->get_stmt_org_name(stmt_name))) {
    LOG_WARN("fail to get stmt name", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::QB_NAME, stmt_name.length(), stmt_name.ptr()))) {
    LOG_WARN("fail to print buffer", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::traverse_join_plan_ldr(
    planText& plan_text, TraverseType traverse_type, JoinTreeType join_tree_type)
{
  int ret = OB_SUCCESS;

  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;

  int64_t first_traverse_idx = 0;
  int64_t second_traverse_idx = 0;
  if (LEFT_DEEP == join_tree_type) {
    first_traverse_idx = first_child;
    second_traverse_idx = second_child;
  } else if (RIGHT_DEEP == join_tree_type) {
    first_traverse_idx = second_child;
    second_traverse_idx = first_child;
  } else { /*do nothing*/
  }

  // traverse the left child tree.
  if (LOG_JOIN == type_) {
    if (OB_ISNULL(get_child(first_traverse_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(first_traverse_idx));
    } else if (get_num_of_child() != 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected child num", "child_num", get_num_of_child());
    } else if (OB_FAIL(BUF_PRINTF("("))) {
    } else if (OB_FAIL(
                   get_child(first_traverse_idx)->traverse_join_plan_ldr(plan_text, traverse_type, join_tree_type))) {
      LOG_WARN("fail to traverse join plan ldr", K(ret));
    } else { /*do nothing*/
    }
  } else {
    switch (traverse_type) {
      case LEADING: {
        if (!is_scan_operator(type_)) {
          if (get_num_of_child() != 1 && LOG_SUBPLAN_FILTER != type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected child num", "child num", get_num_of_child(), K(type_), K(ret));
          } else if (OB_ISNULL(get_child(first_child))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("first child is NULL", K(ret));
          } else if (OB_FAIL(
                         get_child(first_child)->traverse_join_plan_ldr(plan_text, traverse_type, join_tree_type))) {
            LOG_WARN("fail to traverse join plan ldr", K(ret));
          } else { /*do nothing*/
          }
        } else if (OB_FAIL(print_operator_for_leading(plan_text))) {
          LOG_WARN("fail to print operator for leading", K(ret));
        } else { /*do nothing*/
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected traverse type", K(ret), K(traverse_type));
      }
    }
  }
  // traverse the right child tree.
  if (OB_SUCC(ret) && LOG_JOIN == type_) {
    if (OB_ISNULL(get_child(second_traverse_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("second child is null", K(ret), K(second_traverse_idx));
    } else if (get_num_of_child() != 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected child num", "child_num", get_num_of_child());
    } else if (OB_FAIL(
                   get_child(second_traverse_idx)->traverse_join_plan_ldr(plan_text, traverse_type, join_tree_type))) {
      LOG_WARN("fail to traverse join plan ldr");
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret) && LOG_JOIN == type_) {
    OutlineType outline_type = plan_text.outline_type_;
    if (outline_type == OUTLINE_DATA) {
      is_added_leading_outline_ = true;
    } else if (outline_type == USED_HINT) {
      is_added_leading_hints_ = true;
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }

  return ret;
}

int ObLogicalOperator::traverse_join_plan_pre(
    planText& plan_text, TraverseType traverse_type, JoinTreeType join_tree_type, bool is_need_print)
{
  int ret = OB_SUCCESS;
  switch (traverse_type) {
    case JOIN_METHOD:
    case MATERIAL_NL:
    case PQ_DIST:
    case PQ_MAP: {
      if (!is_scan_operator(type_)) { /*do nothing*/
      } else if (is_need_print && OB_FAIL(print_operator_for_use_join(plan_text))) {
        LOG_WARN("fail to print operator for leading", K(ret));
      } else { /*do nothing*/
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected traverse type", K(ret), K(traverse_type));
    }
  }

  if (OB_SUCC(ret) && !is_scan_operator(type_)) {
    if (LOG_JOIN == type_) {
      if (get_num_of_child() != 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected child numm", "child_num", get_num_of_child());
      } else {
        int64_t first_traverse_idx = 0;
        int64_t second_traverse_idx = 0;
        if (LEFT_DEEP == join_tree_type) {
          first_traverse_idx = first_child;
          second_traverse_idx = second_child;
        } else if (RIGHT_DEEP == join_tree_type) {
          first_traverse_idx = second_child;
          second_traverse_idx = first_child;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected join tree type", K(join_tree_type));
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(get_child(first_traverse_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child is null", K(ret), K(first_traverse_idx));
          } else if (is_need_print && OB_FAIL(get_child(first_traverse_idx)
                                                  ->traverse_join_plan_pre(plan_text, traverse_type, join_tree_type))) {
            LOG_WARN("fail to traverse join plan pre");
          } else if (OB_ISNULL(get_child(second_traverse_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child is null", K(ret), K(second_traverse_idx));
          } else if (OB_FAIL(get_child(second_traverse_idx)
                                 ->traverse_join_plan_pre(plan_text, traverse_type, join_tree_type))) {
            LOG_WARN("fail to traverse join plan pre");
          }
        }
      }
    } else {
      if (get_num_of_child() != 1 && LOG_SUBPLAN_FILTER != type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected child num", "child num", get_num_of_child(), K(type_), K(ret));
      } else if (OB_ISNULL(get_child(first_child))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is NULL", K(ret));
      } else if (OB_FAIL(get_child(first_child)->traverse_join_plan_pre(plan_text, traverse_type, join_tree_type))) {
        LOG_WARN("fail to traverse join", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

bool ObLogicalOperator::is_scan_operator(log_op_def::ObLogOpType type)
{
  return LOG_TABLE_SCAN == type || LOG_MV_TABLE_SCAN == type || LOG_SUBPLAN_SCAN == type ||
         LOG_FUNCTION_TABLE == type || LOG_UNPIVOT == type;
}

int ObLogicalOperator::inner_set_merge_sort(ObLogicalOperator* producer, ObLogicalOperator* consumer,
    ObLogicalOperator* op_sort, bool& need_remove, bool global_order)
{
  int ret = OB_SUCCESS;
  need_remove = false;  // default
  if (OB_ISNULL(producer) || OB_ISNULL(consumer) || OB_ISNULL(op_sort)) {
    // do nothing
  } else if (LOG_SORT != op_sort->get_type() || LOG_EXCHANGE != consumer->get_type() ||
             LOG_EXCHANGE != producer->get_type() || producer != consumer->get_child(first_child)) {
    // do nothing
  } else {
    ObLogExchange* producer_exchange = static_cast<ObLogExchange*>(producer);
    ObLogExchange* consumer_exchange = static_cast<ObLogExchange*>(consumer);
    ObLogSort* sort = static_cast<ObLogSort*>(op_sort);
    // ATTENTION: CONST is been droped in `ObLogSort::set_sort_keys`
    if (OB_SUCC(ret) && 0 < sort->get_sort_keys().count()) {
      // just use sort keys, avoid output_exprs are incorrect
      consumer_exchange->set_is_merge_sort(true);
      consumer_exchange->set_local_order(!global_order);
      need_remove = true;
      if (global_order && OB_FAIL(producer_exchange->set_op_ordering(sort->get_sort_keys()))) {
        LOG_WARN("failed to set op ordering", K(ret));
      } else if (!global_order && OB_FAIL(producer_exchange->set_local_ordering(sort->get_sort_keys()))) {
        LOG_WARN("failed to set local ordering", K(ret));
      } else if (OB_FAIL(consumer_exchange->set_sort_keys(sort->get_sort_keys()))) {
        LOG_WARN("failed to set op ordering", K(ret));
      } else {
        LOG_TRACE("sort keys of exchange",
            K(ret),
            K(consumer_exchange->get_sort_keys()),
            K(consumer_exchange->is_task_order()));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::set_merge_sort(ObLogicalOperator* op, AdjustSortContext* adjust_sort_ctx, bool& need_remove)
{
  int ret = OB_SUCCESS;
  //      sort            ordered exchange(merge sort)
  //       |                    |
  //    exchange    =>        exchange
  //       |                    |
  //     exchange             ordered
  //       |
  //     ordered
  ObLogicalOperator* consumer = NULL;
  ObLogicalOperator* producer = NULL;
  bool need_sort = true;
  bool is_prefix = false;
  if (NULL == op || NULL == (consumer = op->get_child(first_child)) ||
      NULL == (producer = consumer->get_child(first_child)) || NULL == (producer->get_child(first_child))) {
    // do nothing
  } else if (LOG_SORT != op->get_type() || LOG_EXCHANGE != consumer->get_type() ||
             LOG_EXCHANGE != producer->get_type() || static_cast<ObLogExchange*>(consumer)->get_is_remote()) {
    // do nothing
  } else if (static_cast<ObLogExchange*>(consumer)->is_keep_order()) {
    // 1. keep_order
    if (OB_FAIL(check_need_sort_below_node(0, op->get_op_ordering(), need_sort))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else {
      need_remove = !need_sort;
    }
  } else if (OB_FAIL(
                 producer->check_need_sort_below_node(0, static_cast<ObLogSort*>(op)->get_op_ordering(), need_sort))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (!need_sort) {
    if (OB_FAIL(consumer->check_if_is_prefix(NULL, &op->get_op_ordering(), producer->get_child(0), is_prefix))) {
      LOG_WARN("check if is prefix failed", K(ret));
    } else if (is_prefix) {
      // 2. task_order
      //  if no px, then convert exchange-in to task order receive
      //  else convert exchange-in to merge sort receive
      need_remove = true;
      // Under px exchange-in will be merge sort receive
      // and input will keep ordering. it's different with no px
      ObLogExchange* consumer_exchange = static_cast<ObLogExchange*>(consumer);
      ObLogExchange* producer_exchange = static_cast<ObLogExchange*>(producer);
      ObLogSort* sort = static_cast<ObLogSort*>(op);
      consumer_exchange->set_is_merge_sort(true);
      consumer_exchange->set_local_order(false);
      if (OB_FAIL(producer_exchange->set_op_ordering(sort->get_sort_keys()))) {
        LOG_WARN("failed to set op ordering", K(ret));
      } else if (OB_FAIL(consumer_exchange->set_sort_keys(sort->get_sort_keys()))) {
        LOG_WARN("failed to set op ordering", K(ret));
      } else {
        LOG_TRACE("sucess to task order for px", K(ret), K(consumer_exchange->is_task_order()));
      }
    } else if (OB_FAIL(inner_set_merge_sort(producer, consumer, op, need_remove, true))) {
      LOG_WARN("fail to set global merge sort", K(ret));
    }
  } else if (adjust_sort_ctx->exchange_cnt_ > 0 &&
             OB_FAIL(producer->check_need_sort_for_local_order(
                 0, &static_cast<ObLogSort*>(op)->get_op_ordering(), need_sort))) {
    LOG_WARN("fail to check need sort for local order", K(ret));
  } else if (!need_sort) {
    if (OB_FAIL(inner_set_merge_sort(producer, consumer, op, need_remove, false))) {
      LOG_WARN("fail to set local merge sort", K(ret));
    }
  } else if (static_cast<ObLogExchange*>(consumer)->is_pq_dist()) {
    // do not push down sort for repartition exchange, and pq distribute
  } else if (OB_FAIL(producer->allocate_sort_below(0, op->get_op_ordering()))) {
    LOG_WARN("failed to push down sort", K(ret));
  } else if (OB_ISNULL(producer->get_child(0)) || OB_UNLIKELY(producer->get_child(0)->get_type() != LOG_SORT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child op", K(ret));
  } else if (OB_FAIL(static_cast<ObLogSort*>(producer->get_child(0))->check_local_merge_sort())) {
    LOG_WARN("failed to check local merge sort", K(ret));
  } else if (OB_FAIL(inner_set_merge_sort(producer, consumer, op, need_remove, true))) {
    LOG_WARN("failed to set merge sort", K(ret));
  }
  return ret;
}

int ObLogicalOperator::check_sharding_compatible_with_reduce_expr(const ObShardingInfo& sharding,
    const ObIArray<ObRawExpr*>& reduce_exprs, const EqualSets& equal_sets, bool& compatible) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> part_exprs;
  ObSEArray<ObRawExpr*, 4> part_column_exprs;
  compatible = false;
  if (OB_FAIL(sharding.get_all_partition_keys(part_exprs, true))) {
    LOG_WARN("failed to get all part keys", K(ret));
  } else if (0 == part_exprs.count()) {
    compatible = false;
  } else if (ObOptimizerUtil::subset_exprs(part_exprs, reduce_exprs, equal_sets)) {
    compatible = true;
    LOG_TRACE(
        "succeed to check sharding compatible info", K(compatible), K(part_exprs), K(reduce_exprs), K(equal_sets));
  } else if (ObRawExprUtils::all_column_exprs(part_exprs)) {
    /*do nothing*/
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_exprs, part_column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (ObOptimizerUtil::subset_exprs(part_column_exprs, reduce_exprs, equal_sets)) {
    compatible = true;
    LOG_TRACE("succeed to check sharding compatiable info",
        K(compatible),
        K(part_column_exprs),
        K(reduce_exprs),
        K(equal_sets));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::get_percentage_value(ObRawExpr* percentage_expr, double& percentage, bool& is_null_value)
{
  int ret = OB_SUCCESS;
  const ParamStore* params = NULL;
  const ObDMLStmt* stmt = NULL;
  percentage = 0.0;
  is_null_value = false;
  if (NULL == percentage_expr) {
    /*do nothing*/
  } else if (T_DOUBLE == percentage_expr->get_expr_type()) {
    percentage = static_cast<ObConstRawExpr*>(percentage_expr)->get_value().get_double();
  } else if (T_NULL == percentage_expr->get_expr_type()) {
    is_null_value = true;
  } else {
    ObObj value;
    if (OB_ISNULL(get_plan()) || OB_ISNULL(params = get_plan()->get_optimizer_context().get_params()) ||
        OB_ISNULL(stmt = get_plan()->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(get_plan()), K(params), K(stmt), K(ret));
    } else if (T_QUESTIONMARK == percentage_expr->get_expr_type()) {
      int64_t idx = static_cast<ObConstRawExpr*>(percentage_expr)->get_value().get_unknown();
      if (idx < 0 || idx >= params->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Idx is invalid", K(idx), K(params->count()), K(ret));
      } else {
        value = params->at(idx);
      }
    } else if ((percentage_expr->has_flag(IS_CALCULABLE_EXPR) || percentage_expr->is_const_expr()) &&
               (percentage_expr->get_result_type().is_double())) {
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(stmt->get_stmt_type(),
              get_plan()->get_optimizer_context().get_session_info(),
              percentage_expr,
              value,
              params,
              get_plan()->get_allocator()))) {
        LOG_WARN("Failed to get const or calculable expr value", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected limit expr type", K(percentage_expr->get_expr_type()), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (value.is_double()) {
        percentage = value.get_double();
      } else if (value.is_null()) {
        is_null_value = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !is_null_value) {
    if (percentage < 0.0) {
      percentage = 0.0;
    } else if (percentage > 100.0) {
      percentage = 100.0;
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogicalOperator::explain_print_partitions(
    const ObIArray<int64_t>& partitions, const bool two_level, char* buf, int64_t& buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const int64_t count = partitions.count();
  if (OB_ISNULL(buf) || buf_len <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments erro", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(BUF_PRINTF("partitions("))) {
    LOG_WARN("Failed to printf partitions", K(ret));
  } else if (0 == count) {
    ret = BUF_PRINTF("nil");
  } else {
    bool continuous = false;
    bool cont_flag = false;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      cont_flag = false;
      const int64_t id = partitions.at(idx);
      if (!continuous) {
        if (idx < count - 1) {
          const int64_t n_id = partitions.at(idx + 1);
          if (two_level) {
            const int64_t p_id = extract_part_idx(id);
            const int64_t s_id = extract_subpart_idx(id);
            const int64_t np_id = extract_part_idx(n_id);
            const int64_t ns_id = extract_subpart_idx(n_id);
            if (p_id == np_id && s_id == ns_id - 1) {
              ret = BUF_PRINTF("p%lusp[%lu-", p_id, s_id);
              continuous = true;
            } else {
              ret = BUF_PRINTF("p%lusp%lu", p_id, s_id);
            }
          } else if (id == n_id - 1) {
            ret = BUF_PRINTF("p[%lu-", id);
            continuous = true;
          } else {
            ret = BUF_PRINTF("p%lu", id);
          }
        } else if (two_level) {
          const int64_t p_id = extract_part_idx(id);
          const int64_t s_id = extract_subpart_idx(id);
          ret = BUF_PRINTF("p%lusp%lu", p_id, s_id);
        } else {
          ret = BUF_PRINTF("p%lu", id);
        }
      } else {
        if (idx >= count - 1) {
          continuous = false;
        } else if (two_level) {
          const int64_t n_id = partitions.at(idx + 1);
          const int64_t p_id = extract_part_idx(id);
          const int64_t s_id = extract_subpart_idx(id);
          const int64_t np_id = extract_part_idx(n_id);
          const int64_t ns_id = extract_subpart_idx(n_id);
          if (p_id != np_id || s_id != (ns_id - 1)) {
            continuous = false;
          }
        } else if (id != partitions.at(idx + 1) - 1) {
          continuous = false;
        } else {
        }

        if (continuous) {
          cont_flag = true;
        } else if (two_level) {
          const int64_t s_id = extract_subpart_idx(id);
          ret = BUF_PRINTF("%lu]", s_id);
        } else {
          ret = BUF_PRINTF("%lu]", id);
        }
      }
      if (!cont_flag && !continuous && (idx < count - 1)) {
        ret = BUF_PRINTF(", ");
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = BUF_PRINTF(")");
  }
  return ret;
}

int ObLogicalOperator::is_need_print_join_method(const planText& plan_text, JoinAlgo join_algo, bool& is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  bool is_used_hint = false;
  if (OUTLINE_DATA == plan_text.outline_type_) {
    is_need = true;
  } else if (USED_HINT == plan_text.outline_type_) {
    if (OB_FAIL(is_used_join_type_hint(join_algo, is_used_hint))) {
      LOG_WARN("fail to judge whether use join type", K(ret), K(join_algo), K(is_used_hint));
    } else {
      is_need = is_used_hint;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected outline type", K(ret), K(plan_text.outline_type_));
  }
  return ret;
}

int ObLogicalOperator::is_need_print_operator_for_leading(const planText& plan_text, bool& is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  bool is_used_in_leading = false;
  if (OUTLINE_DATA == plan_text.outline_type_) {
    is_need = true;
  } else if (USED_HINT == plan_text.outline_type_) {
    if (OB_FAIL(is_used_in_leading_hint(is_used_in_leading))) {
      LOG_WARN("fail to judge whether usein leading hint", K(ret), K(is_used_in_leading));
    } else {
      is_need = is_used_in_leading;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected outline type", K(ret), K(plan_text.outline_type_));
  }

  return ret;
}

int ObLogicalOperator::print_operator_for_use_join(planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  if (OB_FAIL(print_operator_for_outline(plan_text))) {
    LOG_WARN("fail to print operator for outline", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(" "))) {
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::print_operator_for_leading(planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_need = false;
  if (OB_FAIL(is_need_print_operator_for_leading(plan_text, is_need))) {
    LOG_WARN("fail to judge whether need print operator for leading", K(ret));
  } else if (!is_need) {  // no need to print this operator for leading
  } else if (OB_FAIL(print_operator_for_outline(plan_text))) {
    LOG_WARN("fail to print operator for outline", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(" "))) {
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::allocate_granule_pre(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

int ObLogicalOperator::allocate_granule_post(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

int ObLogicalOperator::px_pipe_blocking_pre(ObPxPipeBlockingCtx& ctx)
{
  typedef ObPxPipeBlockingCtx::OpCtx OpCtx;
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  OpCtx* op_ctx = static_cast<OpCtx*>(traverse_ctx_);
  OpCtx* child_op_ctx = NULL;
  if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL traverse ctx", K(ret));
  }
  for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
    if (NULL == get_child(i)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL child", K(ret));
    } else if (OB_ISNULL(child_op_ctx = static_cast<OpCtx*>(get_child(i)->traverse_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL traverse ctx", K(ret));
    } else {
      child_op_ctx->dfo_depth_ = op_ctx->dfo_depth_;
      child_op_ctx->out_.set_exch(op_ctx->out_.is_exch() && !is_block_input(i));
    }
  }
  return ret;
}

int ObLogicalOperator::px_pipe_blocking_post(ObPxPipeBlockingCtx& ctx)
{
  typedef ObPxPipeBlockingCtx::OpCtx OpCtx;
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  OpCtx* op_ctx = static_cast<OpCtx*>(traverse_ctx_);
  if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL traverse ctx", K(ret));
  } else if (op_ctx->dfo_depth_ < 0) {
    // nothing to do, the current operator is not processed by PX
  } else {
    bool got_in_exch = false;
    int64_t child_dfo_cnt = 0;
    for (int64_t i = get_num_of_child() - 1; OB_SUCC(ret) && i >= 0; i--) {
      auto child = get_child(i);
      OpCtx* child_op_ctx = NULL;
      if (OB_ISNULL(child) || OB_ISNULL(child_op_ctx = static_cast<OpCtx*>(child->traverse_ctx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL child or NULL child traverse ctx", K(ret));
      } else {
        if (child_op_ctx->has_dfo_below_) {
          child_dfo_cnt += 1;
        }
        if (child_op_ctx->in_.is_exch() && !is_block_input(i)) {
          if (child_dfo_cnt > 1 && !is_consume_child_1by1()) {
            if (child->get_type() == LOG_DISTINCT && static_cast<ObLogDistinct*>(child)->get_algo() == HASH_AGGREGATE) {
              static_cast<ObLogDistinct*>(child)->set_block_mode(true);
              LOG_DEBUG("distinct block mode", K(lbt()));
            } else if (OB_FAIL(allocate_material(i))) {
              LOG_WARN("allocate material failed", K(ret));
            }
          } else if (!got_in_exch) {
            got_in_exch = true;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      op_ctx->in_.set_exch(got_in_exch);
      op_ctx->has_dfo_below_ = child_dfo_cnt > 0;
      LOG_TRACE("pipe blocking ctx", K(get_name()), K(*op_ctx));
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_granule_nodes_above(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  bool partition_granule = false;
  //  op    granule iterator
  //   |    ->    |
  //  other      op
  //              |
  //             other
  if (!ctx.alloc_gi_) {
    // do nothing
  } else if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_plan()));
  } else if (LOG_TABLE_SCAN != get_type() && LOG_JOIN != get_type() && LOG_SET != get_type() &&
             LOG_GROUP_BY != get_type() && LOG_DISTINCT != get_type() && LOG_SUBPLAN_FILTER != get_type() &&
             LOG_WINDOW_FUNCTION != get_type() && LOG_UPDATE != get_type() && LOG_DELETE != get_type() &&
             LOG_INSERT != get_type() && LOG_MERGE != get_type() && LOG_FOR_UPD != get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Only special op can allocate a granule iterator", K(get_type()));
  } else {
    ObLogicalOperator* log_op = NULL;
    ObLogOperatorFactory& factory = get_plan()->get_log_op_factory();
    if (OB_ISNULL(log_op = factory.allocate(*(get_plan()), LOG_GRANULE_ITERATOR))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate exchange nodes", K(log_op));
    } else {
      ObLogGranuleIterator* gi_op = static_cast<ObLogGranuleIterator*>(log_op);
      if (NULL != get_parent()) {
        bool found_child = false;
        for (int64_t i = 0; OB_SUCC(ret) && !found_child && i < get_parent()->get_num_of_child(); ++i) {
          if (get_parent()->get_child(i) == this) {
            get_parent()->set_child(i, gi_op);
            found_child = true;
          } else { /*do nothing*/
          }
        }
        gi_op->set_parent(get_parent());
      } else { /*do nothing*/
      }
      set_parent(gi_op);
      gi_op->set_child(first_child, this);
      gi_op->set_cost(get_cost());
      gi_op->set_card(get_card());
      gi_op->set_width(get_width());
      gi_op->set_parallel(ctx.parallel_);
      gi_op->set_partition_count(ctx.partition_count_);
      gi_op->set_hash_part(ctx.hash_part_);
      gi_op->set_tablet_size(ctx.tablet_size_);

      if (ctx.is_in_pw_affinity_state()) {
        gi_op->add_flag(GI_AFFINITIZE);
        gi_op->add_flag(GI_PARTITION_WISE);
      }

      if (ctx.is_in_partition_wise_state() && !is_block_gi_allowed_) {
        gi_op->add_flag(GI_PARTITION_WISE);
      }

      if (ctx.is_in_slave_mapping()) {
        gi_op->add_flag(GI_SLAVE_MAPPING);
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(gi_op->is_partition_gi(partition_granule))) {
          LOG_WARN("failed judge partition granule", K(ret));
        }
      }

      if (ctx.force_partition() || partition_granule) {
        gi_op->add_flag(GI_FORCE_PARTITION_GRANULE);
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(gi_op->compute_property())) {
          LOG_WARN("failed to compute equal sets", K(ret));
        }
      }

      if (is_plan_root()) {
        set_is_plan_root(false);
        gi_op->mark_is_plan_root();
        get_plan()->set_plan_root(gi_op);
      } else { /*do nothing*/
      }

      LOG_TRACE("succ to allocate granule iterator nodes above operator",
          K(get_name()),
          K(get_cost()),
          K(get_card()),
          K(ctx.is_in_pw_affinity_state()),
          K(ctx.is_in_partition_wise_state()));
    }
  }
  /*
   * we try to allocate gi. whether success or failed, we reset the alloc_gi_.
   * if we forget reset this var, may get some wrong log plan.
   * */
  ctx.alloc_gi_ = false;
  return ret;
}

int ObLogicalOperator::set_granule_nodes_affinity(AllocGIContext& ctx, int64_t child_index)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  if (child_index >= get_num_of_child()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set granule affinity failed", K(ret));
  } else {
    ObLogicalOperator* child_op = get_child(child_index);
    if (LOG_GRANULE_ITERATOR == child_op->get_type()) {
      static_cast<ObLogGranuleIterator*>(child_op)->add_flag(GI_AFFINITIZE);
      if (ctx.enable_gi_partition_pruning_) {
        static_cast<ObLogGranuleIterator*>(child_op)->add_flag(GI_ENABLE_PARTITION_PRUNING);
      }
    } else if (0 == child_op->get_num_of_child()) {
      LOG_TRACE("No GI operator found");
    } else if (child_op->get_num_of_child() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set granule affinity failed, the child must be single child operator",
          K(ret),
          K(child_op->get_num_of_child()),
          K(child_op->get_type()));
    } else if (OB_FAIL(child_op->set_granule_nodes_affinity(ctx, 0))) {
      LOG_WARN("set granule affinity failed", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::find_first_recursive(const log_op_def::ObLogOpType type, ObLogicalOperator*& op)
{
  int ret = OB_SUCCESS;
  op = NULL;
  if (type == get_type()) {
    op = this;
  } else if (NULL == get_child(first_child)) {
    /*do nothing*/
  } else {
    ret = get_child(first_child)->find_first_recursive(type, op);
  }
  return ret;
}

void ObLogicalOperator::clear_all_traverse_ctx()
{
  traverse_ctx_ = NULL;
  for (int64_t i = 0; i < get_num_of_child(); i++) {
    if (NULL != get_child(i)) {
      get_child(i)->clear_all_traverse_ctx();
    }
  }
}

int ObLogicalOperator::push_down_limit(AllocExchContext* ctx, ObRawExpr* limit_count_expr, ObRawExpr* limit_offset_expr,
    bool should_push_limit, bool is_fetch_with_ties, ObLogicalOperator*& exchange_point)
{
  int ret = OB_SUCCESS;
  /*
   * In order to get accurate results, the new limit count should be calculated as below:
   * 1. For offset_expr_ == NULL, new_limit_count = limit_count_;
   * 2. For offset_expr_ != NULL, new_limit_count = limit_count_expr = limit_offset_expr.
   */
  bool need_sort = false;
  ObLogicalOperator* child = NULL;
  ObLogSort* child_sort = NULL;
  ObLogicalOperator* child_limit = NULL;
  ObRawExpr* new_limit_count_expr = NULL;
  exchange_point = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(get_plan()), K(child), K(ret));
  } else if (log_op_def::LOG_SORT == child->get_type()) {
    need_sort = true;
    exchange_point = child;
  } else {
    exchange_point = this;
  }
  // push down sort
  if (OB_SUCC(ret) && need_sort) {
    if (OB_FAIL(exchange_point->allocate_sort_below(0, static_cast<ObLogSort*>(exchange_point)->get_sort_keys()))) {
      LOG_WARN("failed to allocate child sort", K(ret));
    } else if (OB_ISNULL(child = exchange_point->get_child(first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_UNLIKELY(log_op_def::LOG_SORT != child->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected operator type", K(child->get_type()), K(ret));
    } else if (FALSE_IT(child_sort = static_cast<ObLogSort*>(child))) {
      /*do nothing*/
    } else {
      child_sort->set_card(exchange_point->get_card());
      child_sort->set_op_cost(exchange_point->get_op_cost());
      child_sort->set_width(exchange_point->get_width());
      child_sort->set_topn_count(static_cast<ObLogSort*>(exchange_point)->get_topn_count());
    }
  }
  // push down limit expr
  if (OB_SUCC(ret) && should_push_limit) {
    if (OB_ISNULL(limit_count_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(limit_count_expr), K(ret));
    } else if (OB_FAIL(
                   ObTransformUtils::make_pushdown_limit_count(get_plan()->get_optimizer_context().get_expr_factory(),
                       *get_plan()->get_optimizer_context().get_session_info(),
                       limit_count_expr,
                       limit_offset_expr,
                       new_limit_count_expr))) {
      LOG_WARN("generate pushdown limit count expr failed", K(ret));
    }
    // push down limit expr
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(child = exchange_point->get_child(first_child))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(exchange_point), K(ret));
      } else if ((log_op_def::instance_of_log_table_scan(child->get_type())) &&
                 !is_virtual_table(static_cast<ObLogTableScan*>(child)->get_ref_table_id()) &&
                 NULL == static_cast<ObLogTableScan*>(child)->get_limit_expr()) {
        // Do NOT allocate LIMIT operator, and push down limit onto table scan directly.
        ObLogTableScan* table_scan = static_cast<ObLogTableScan*>(child);
        table_scan->set_limit_offset(new_limit_count_expr, NULL);
      } else {
        if (OB_FAIL(exchange_point->allocate_limit_below(first_child, new_limit_count_expr))) {
          LOG_WARN("failed to allocte limit below", K(ret));
        } else if (OB_ISNULL(child_limit = exchange_point->get_child(first_child))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(child_limit), K(child), K(ret));
        } else if (OB_FAIL(child_limit->set_expected_ordering(get_expected_ordering()))) {
          LOG_WARN("failed to set expected ordering", K(ret));
        } else if (OB_FAIL(child_limit->replace_generated_agg_expr(ctx->group_push_down_replaced_exprs_))) {
          LOG_WARN("failed to replace agg expr", K(ret));
        } else {
          static_cast<ObLogLimit*>(child_limit)->set_fetch_with_ties(is_fetch_with_ties);
          child_limit->set_card(get_card());
          child_limit->set_op_cost(get_op_cost());
          child_limit->set_width(get_width());
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_gi_recursively(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get_child(i) returns unexpected null", K(ret), K(i));
    } else if (LOG_EXCHANGE == get_child(i)->get_type()) {
      // do not allocate gi over a dfo bound.
    } else if (OB_FAIL(get_child(i)->allocate_gi_recursively(ctx))) {
      LOG_WARN("failed to bottom-up traverse operator", K(ret), K(get_name()));
    } else { /* Do nothing */
    }
  }
  if (OB_SUCC(ret)) {
    if (LOG_TABLE_SCAN == get_type()) {
      ctx.alloc_gi_ = true;
      if (OB_FAIL(allocate_granule_nodes_above(ctx))) {
        LOG_WARN("allocate gi above table scan failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::init_est_sel_info()
{
  int ret = OB_SUCCESS;
  bool pass_op = log_op_def::LOG_SORT == get_type() || log_op_def::LOG_EXCHANGE == get_type() ||
                 log_op_def::LOG_MATERIAL == get_type() || log_op_def::LOG_WINDOW_FUNCTION == get_type() ||
                 log_op_def::LOG_COUNT == get_type() || log_op_def::LOG_GRANULE_ITERATOR == get_type() ||
                 log_op_def::LOG_TABLE_LOOKUP == get_type() || log_op_def::LOG_SEQUENCE == get_type() ||
                 log_op_def::LOG_MONITORING_DUMP == get_type();
  int64_t num_of_child = get_num_of_child();
  if (OB_ISNULL(my_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret));
  } else if (OB_NOT_NULL(est_sel_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("est sel info should be null", K(ret));
  } else if (pass_op) {
    // do nothing
  } else if (OB_ISNULL(est_sel_info_ = my_plan_->create_est_sel_info(this))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create est sel info", K(ret));
  } else if (log_op_def::LOG_SET == get_type() || log_op_def::LOG_SUBPLAN_FILTER == get_type()) {
    num_of_child = 1;
  }

  if (pass_op) {
    if (get_num_of_child() == 0) {
      /*do nothing*/
    } else if (OB_ISNULL(get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected child", K(ret), K(get_num_of_child()));
    } else {
      est_sel_info_ = get_child(0)->get_est_sel_info();
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < num_of_child; i++) {
      ObLogicalOperator* child = get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error num of child", K(ret), K(i), K(child));
      } else if (OB_ISNULL(child->get_est_sel_info())) {
        // do nothing
      } else if (OB_FAIL(est_sel_info_->append_table_stats(child->get_est_sel_info()))) {
        LOG_WARN("failed to assign first child", K(ret));
      } else if (OB_FAIL(est_sel_info_->update_table_stats(child))) {
        LOG_WARN("failed to update table statics from right", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::pw_allocate_granule_pre(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  /* (partition wise join/union/group/window function below)
   *                   |
   *                 OP(1)
   *                   |
   *             --------------
   *             |            |
   *            OP(2)        ...
   *             |
   *            ...
   *   OP(1) will set his id to gi allocate ctx as reset token,
   *   and in the post stage OP(1) will reset the state of
   *   gi allocate ctx.
   * */
  if (!ctx.exchange_above()) {
    LOG_TRACE("no exchange above, do nothing", K(sharding_info_));
  } else if (!ctx.is_in_partition_wise_state() && !ctx.is_in_pw_affinity_state() && is_partition_wise_) {
    ctx.set_in_partition_wise_state(this);
    LOG_TRACE("in find partition wise state", K(sharding_info_), K(*this));
  }
  return ret;
}

int ObLogicalOperator::pw_allocate_granule_post(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  /*
   * (partition wise join/union/group/window function below)
   *                   |
   *                 OP(1)
   *                   |
   *             --------------
   *             |            |
   *            OP(2)     ...
   *             |
   *            ...
   *   OP(1) will reset the state of gi allocate ctx.
   *   As the ctx has record the state was changed by OP(1),
   *   so OP(2) cann't reset this state.
   * */
  if (!ctx.exchange_above()) {
    LOG_TRACE("no exchange above, do nothing", K(sharding_info_), K(ctx));
  } else if (ctx.is_op_set_pw(this)) {
    // In partition-wise join case, when GI is above group by/window function with pw attribute,
    // it doesn't support rescan before,
    // so it allocates the GI just above tsc and set attribute gi_random.
    // However, at most one gi_random can be supported within a DFO,
    // so that won't work in the plan found in this bug.
    // Now we support GI rescan in partition_wise_state so we just push up the GI here in such case
    if (ctx.is_in_partition_wise_state()) {
      ctx.alloc_gi_ = true;
      if (OB_FAIL(allocate_granule_nodes_above(ctx))) {
        LOG_WARN("allocate gi above table scan failed", K(ret));
      }
    } else if (ctx.is_in_pw_affinity_state()) {
      if (OB_FAIL(allocate_gi_recursively(ctx))) {
        LOG_WARN("failed to allocate gi recursively", K(ret));
      }
    }
    IGNORE_RETURN ctx.reset_info();
  }
  return ret;
}

int ObLogicalOperator::allocate_link_post()
{
  /**
   * if all child nodes have same dblink id, just set dblink id of current node,
   * otherwise, add link op above those childs whose dblink id is not OB_INVALID_ID.
   */
  int ret = OB_SUCCESS;
  bool is_connect_by = false;
  if (LOG_JOIN == type_) {
    ObLogJoin* join = static_cast<ObLogJoin*>(this);
    is_connect_by = (CONNECT_BY_JOIN == join->get_join_type());
  }
  if (LOG_COUNT == type_ || LOG_DISTINCT == type_ || LOG_GROUP_BY == type_ || (LOG_JOIN == type_ && !is_connect_by) ||
      LOG_SET == type_ || LOG_SORT == type_ ||
      // LOG_SUBPLAN_SCAN == type_ ||
      LOG_TABLE_SCAN == type_) {
    // white list of supported logical operator.
    uint64_t dblink_id = (get_num_of_child() > 0 && OB_NOT_NULL(get_child(0))) ? get_child(0)->get_dblink_id() : 0;
    for (int64_t i = 1; OB_SUCC(ret) && i < get_num_of_child(); i++) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_child(i) returns unexpected null", K(ret), K(i));
      } else if (dblink_id != get_child(i)->get_dblink_id()) {
        dblink_id = 0;
        break;
      }
    }
    if (OB_SUCC(ret) && 0 != dblink_id) {
      set_dblink_id(dblink_id);
    }
  }
  // the code above is used to op that have child,
  // the code below is used to all op.
  if (OB_SUCC(ret)) {
    if (0 != get_dblink_id()) {
      /*
       * ---------------------------------------------
       * |0 |MERGE UNION DISTINCT|    |6        |75  |
       * |1 | TABLE SCAN         |T1  |3        |37  |
       * |2 | TABLE SCAN         |T2  |3        |37  |
       * =============================================
       * DO NOT use is_plan_root(), op 0/1/2 all return true, use 'NULL == get_parent()'.
       */
      if (NULL == get_parent() && OB_FAIL(allocate_link_node_above(-1))) {
        LOG_WARN("allocate link node above failed", K(ret));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
        if (OB_ISNULL(get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get_child(i) returns unexpected null", K(ret), K(i));
        } else if (0 == get_child(i)->get_dblink_id()) {
          // skip.
        } else if (OB_FAIL(get_child(i)->allocate_link_node_above(i))) {
          LOG_WARN("allocate link node above failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::generate_link_sql_pre(GenLinkStmtContext& link_ctx)
{
  UNUSED(link_ctx);
  return OB_SUCCESS;
}

int ObLogicalOperator::allocate_link_node_above(int64_t child_idx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* link = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_plan()));
  } else if (OB_ISNULL(link = get_plan()->get_log_op_factory().allocate(*(get_plan()), LOG_LINK))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate link node");
  } else if (OB_FAIL(link->get_sharding_info().copy_without_part_keys(get_sharding_info()))) {
    LOG_WARN("failed to deep copy sharding info from child", K(ret));
  } else if (OB_FAIL(link->set_op_ordering(get_op_ordering()))) {
    LOG_WARN("failed to copy op ordering info from child", K(ret));
  } else {
    /**
     * now we pull all link table rows to local, then add exchange op if need repart, like:
     *
     *         join                        join
     *           |                           |
     *     -------------       =>      -------------
     *     |           |               |           |
     * part_table  link_table      part_table   exchange
     *               (local)                    (repart)
     *                                             |
     *                                         link_table
     *                                           (local)
     *
     * the problem is the data from link table maybe transfer twice on network,
     * because we can only use sql to communicate between different clusters,
     * and sql have no repart ability, so we must repart all data in local cluster.
     * BUT, the problem may be solved in certain situation, such as part_table and link_table
     * can wise join regardless phy location, the transmit op only do data-transfer-job
     * without any repart job.
     * ps:
     * to achieve this optimization, we need add partition info into link table schema,
     * then every local part read another part from remote cluster using "from t1 partition(p0)",
     * YES we use from clause "partition(p0)" instead of exchange op, remember that we can not
     * execute exchange op in remote cluster even if there is no need to repart.
     */
    link->set_dblink_id(get_dblink_id());
    link->get_sharding_info().set_location_type(OB_TBL_LOCATION_LOCAL);
    if (NULL != get_parent()) {
      get_parent()->set_child(child_idx, link);
      link->set_parent(get_parent());
    } else {
      link->set_is_plan_root(true);
      this->set_is_plan_root(false);
      get_plan()->set_plan_root(link);
    }
    link->set_child(0, this);
    this->set_parent(link);
    link->set_dblink_id(get_dblink_id());
    if (OB_FAIL(link->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    }
  }
  return ret;
}

bool AllocGIContext::is_in_partition_wise_state()
{
  return GIS_IN_PARTITION_WISE == state_;
};

void AllocGIContext::set_in_partition_wise_state(ObLogicalOperator* op_ptr)
{
  state_ = GIS_IN_PARTITION_WISE;
  pw_op_ptr_ = op_ptr;
}

bool AllocGIContext::is_op_set_pw(ObLogicalOperator* op_ptr)
{
  return pw_op_ptr_ == op_ptr;
}

bool AllocGIContext::try_set_out_partition_wise_state(ObLogicalOperator* op_ptr)
{
  bool result = false;
  if (pw_op_ptr_ == op_ptr) {
    state_ = GIS_NORMAL;
    result = true;
  }
  return result;
}

int AllocGIContext::set_pw_affinity_state()
{
  int ret = OB_SUCCESS;
  if (GIS_IN_PARTITION_WISE == state_) {
    state_ = GIS_PARTITION_WITH_AFFINITY;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set partition-wise-affinity state without partition-wise state is forbidden", K(ret));
  }
  return ret;
}

void AllocGIContext::reset_info()
{
  state_ = GIS_NORMAL;
  multi_child_op_above_count_in_dfo_ = 0;
  enable_gi_partition_pruning_ = false;
}

AllocGIContext::GIState AllocGIContext::get_state()
{
  return state_;
}

bool AllocGIContext::managed_by_gi()
{
  return GIS_NORMAL != state_;
}

bool AllocGIContext::is_in_pw_affinity_state()
{
  return GIS_PARTITION_WITH_AFFINITY == state_;
}

int AllocGIContext::push_current_dfo_dop(int64_t dop)
{
  int ret = OB_SUCCESS;
  parallel_ = dop;
  if (OB_FAIL(dfo_table_dop_stack_.push_back(dop))) {
    LOG_WARN("failed to push back table dop", K(ret));
  }
  return ret;
}

int AllocGIContext::pop_current_dfo_dop()
{
  int ret = OB_SUCCESS;
  if (dfo_table_dop_stack_.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of stack element is not right", K(ret), K(dfo_table_dop_stack_.count()));
  } else {
    dfo_table_dop_stack_.pop_back();
    if (dfo_table_dop_stack_.count() > 0) {
      parallel_ = dfo_table_dop_stack_.at(dfo_table_dop_stack_.count() - 1);
    }
  }
  return ret;
}

int64_t ObLinkStmt::to_string(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int32_t pos = 0;
  if (OB_FAIL(gen_stmt_fmt(buf, static_cast<int32_t>(buf_len), pos))) {
    LOG_WARN("failed to gen stmt fmt", K(ret));
    pos = 0;
  } else {
    char* ch = buf;
    char* stmt_end = buf + pos - 3;
    while (ch < stmt_end) {
      if (0 == ch[0] && 0 == ch[1]) {
        uint16_t param_idx = *(uint16_t*)(ch + 2);
        ch[0] = '$';
        if (param_idx > 999) {
          ch[1] = 'M';
          ch[2] = 'A';
          ch[3] = 'X';
        } else {
          ch[3] = static_cast<char>('0' + param_idx % 10);
          param_idx /= 10;
          ch[2] = static_cast<char>('0' + param_idx % 10);
          param_idx /= 10;
          ch[1] = static_cast<char>('0' + param_idx % 10);
        }
        ch += 4;
      } else {
        ch++;
      }
    }
  }
  return pos;
}

int ObLogicalOperator::aggregate_number_of_parent()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexected null", K(ret));
    } else if (OB_FAIL(SMART_CALL(get_child(i)->aggregate_number_of_parent()))) {
      LOG_WARN("failed to check whether need copy plan tree", K(ret));
    } else {
      get_child(i)->num_of_parent_++;
    }
  }
  return ret;
}

int ObLogicalOperator::reset_number_of_parent()
{
  int ret = OB_SUCCESS;
  num_of_parent_ = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexected null", K(ret));
    } else if (OB_FAIL(SMART_CALL(get_child(i)->reset_number_of_parent()))) {
      LOG_WARN("failed to check whether need copy plan tree", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogicalOperator::need_copy_plan_tree(bool& need_copy)
{
  int ret = OB_SUCCESS;
  need_copy = num_of_parent_ > 1;
  for (int64_t i = 0; OB_SUCC(ret) && !need_copy && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexected null", K(ret));
    } else if (OB_FAIL(SMART_CALL(get_child(i)->need_copy_plan_tree(need_copy)))) {
      LOG_WARN("failed to check whether need copy plan tree", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

/*
 * serial, local, remote, match_all belong to the basic sharding info
 */
int ObLogicalOperator::compute_basic_sharding_info(AllocExchContext* ctx, bool& is_basic)
{
  int ret = OB_SUCCESS;
  bool has_fake_cte = false;
  ObSEArray<ObShardingInfo*, 4> input_shardings;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(input_shardings.push_back(&get_child(i)->get_sharding_info()))) {
      LOG_WARN("failed to push back sharding info", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(ObLogicalOperator::compute_basic_sharding_info(input_shardings, sharding_info_, is_basic))) {
    LOG_WARN("failed to check basic sharding info", K(ret));
  } else if (!is_basic && OB_FAIL(compute_recursive_cte_sharding_info(ctx, this, is_basic))) {
    LOG_WARN("failed to compute recursive cte sharding info", K(ret));
  } else {
    LOG_TRACE("succeed to check basic sharding info", K(is_basic), K(sharding_info_));
  }
  return ret;
}

int ObLogicalOperator::compute_recursive_cte_sharding_info(AllocExchContext* ctx, ObLogicalOperator* op, bool& is_basic)
{
  int ret = OB_SUCCESS;
  is_basic = false;
  if (OB_ISNULL(ctx) || OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(op), K(ctx), K(ret));
  } else {
    int64_t fake_cte_pos = -1;
    ObLogicalOperator* child = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && fake_cte_pos == -1 && i < op->get_num_of_child(); i++) {
      bool has_fake_cte = false;
      if (OB_ISNULL(child = op->get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(check_contain_fake_cte_table(child, has_fake_cte))) {
        LOG_WARN("failed to check whether contain fake cte table", K(ret));
      } else if (has_fake_cte) {
        fake_cte_pos = i;
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && fake_cte_pos != -1) {
      for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
        ObExchangeInfo exch_info;
        if (OB_ISNULL(child = op->get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (i == fake_cte_pos) {
          ret = SMART_CALL(compute_recursive_cte_sharding_info(ctx, child, is_basic));
        } else if (child->get_sharding_info().is_sharding() && OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
          LOG_WARN("failed to allocate exchange", K(ret));
        } else if (child->get_sharding_info().is_sharding() &&
                   FALSE_IT(child->get_sharding_info().set_location_type(ObTableLocationType::OB_TBL_LOCATION_LOCAL))) {
          /*do nothing*/
        } else if (OB_FAIL(op->check_and_allocate_material(i))) {
          LOG_WARN("failed to check and allocate material", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_SUCC(ret)) {
        is_basic = true;
        sharding_info_.set_location_type(ObTableLocationType::OB_TBL_LOCATION_LOCAL);
      }
    }
  }
  return ret;
}

int ObLogicalOperator::check_contain_fake_cte_table(ObLogicalOperator* op, bool& has_fake_cte)
{
  int ret = OB_SUCCESS;
  has_fake_cte = false;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (op->get_type() == log_op_def::ObLogOpType::LOG_SET && static_cast<ObLogSet*>(op)->is_recursive_union()) {
    /*do nothing*/
  } else if (op->is_table_scan() && static_cast<ObLogTableScan*>(op)->get_is_fake_cte_table()) {
    has_fake_cte = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !has_fake_cte && i < op->get_num_of_child(); i++) {
      if (OB_ISNULL(op->get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_contain_fake_cte_table(op->get_child(i), has_fake_cte)))) {
        LOG_WARN("failed to check is recursive cte", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogicalOperator::compute_basic_sharding_info(
    ObIArray<ObShardingInfo*>& input_shardings, ObShardingInfo& output_sharding, bool& is_basic)
{
  int ret = OB_SUCCESS;
  int64_t local_num = 0;
  int64_t remote_num = 0;
  int64_t match_all_num = 0;
  const ObShardingInfo* sharding = NULL;
  const ObShardingInfo* first_local = NULL;
  const ObShardingInfo* first_remote = NULL;
  const ObShardingInfo* first_match_all = NULL;
  int64_t sharding_num = input_shardings.count();
  is_basic = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_basic && i < sharding_num; i++) {
    if (OB_ISNULL(sharding = input_shardings.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(sharding), K(ret));
    } else if (sharding->is_local()) {
      local_num++;
      if (NULL == first_local) {
        first_local = sharding;
      } else { /*do nothing*/
      }
    } else if (sharding->is_remote()) {
      bool is_same = false;
      remote_num++;
      if (NULL == first_remote) {
        first_remote = sharding;
      } else if (OB_FAIL(ObShardingInfo::is_physically_equal_partitioned(*first_remote, *sharding, is_same))) {
        LOG_WARN("failed to check is physicall equal partitioned", K(ret));
      } else if (!is_same) {
        is_basic = false;
      } else { /*do nothing*/
      }
    } else if (sharding->is_match_all()) {
      match_all_num++;
      if (NULL == first_match_all) {
        first_match_all = sharding;
      } else { /*do nothing*/
      }
    } else if (sharding->is_distributed()) {
      is_basic = false;
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret) && is_basic) {
    if (local_num > 0 && (local_num + match_all_num == sharding_num)) {
      if (OB_ISNULL(first_local)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(first_local), K(ret));
      } else {
        is_basic = true;
        output_sharding.copy_with_part_keys(*first_local);
      }
    } else if (remote_num > 0 && (remote_num + match_all_num == sharding_num)) {
      if (OB_ISNULL(first_remote)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(first_remote), K(ret));
      } else {
        is_basic = true;
        output_sharding.copy_with_part_keys(*first_remote);
      }
    } else if (match_all_num == sharding_num) {
      if (OB_ISNULL(first_match_all)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(match_all_num), K(ret));
      } else {
        is_basic = true;
        output_sharding.copy_with_part_keys(*first_match_all);
      }
    } else {
      is_basic = false;
    }
  }
  return ret;
}

int ObLinkStmt::init(GenLinkStmtContext* link_ctx)
{
  int ret = OB_SUCCESS;
  ObString question_mark_str("?");
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(from_strs_.push_back(question_mark_str))) {
    LOG_WARN("failed to push_back question mark", K(ret));
  } else if (FALSE_IT(tmp_buf_len_ = 256 * 1024)) {
    // nothing.
  } else if (OB_ISNULL(tmp_buf_ = static_cast<char*>(alloc_.alloc(tmp_buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("falied to alloc tmp buf", K(ret), K(tmp_buf_len_));
  } else {
    is_inited_ = true;
    link_ctx_ = link_ctx;
  }
  return ret;
}

void ObLinkStmt::reset()
{
  // DO NOT reset alloc_.
  select_strs_.reset();
  from_strs_.reset();
  where_strs_.reset();
  groupby_strs_.reset();
  having_strs_.reset();
  orderby_strs_.reset();
  limit_str_.reset();
  offset_str_.reset();
  alloc_.free(tmp_buf_);
  tmp_buf_ = NULL;
  tmp_buf_len_ = 0;
  is_inited_ = false;
  is_distinct_ = false;
}

bool ObLinkStmt::is_same_output_exprs_ignore_seq(const ObRawExprIArray& output_exprs)
{
  bool is_same = true;
  if (root_output_exprs_.count() == output_exprs.count()) {
    for (int i = 0; is_same && i < output_exprs.count(); i++) {
      is_same = is_contain(root_output_exprs_, output_exprs.at(i));
    }
  } else {
    is_same = false;
  }
  return is_same;
}

int ObLinkStmt::append_nl_param_idx(int64_t param_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(link_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("link ctx is NULL", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(link_ctx_->nl_param_idxs_, param_idx))) {
    LOG_WARN("failed to add param_idx to nl_param_idxs", K(ret), K(param_idx));
  }
  return ret;
}

int ObLinkStmt::try_fill_select_strs(const ObRawExprIArray& exprs)
{
  int ret = OB_SUCCESS;
  if (!is_select_strs_empty() || is_same_output_exprs_ignore_seq(exprs)) {
    // nothing.
  } else if (OB_FAIL(fill_exprs(root_output_exprs_, SEP_COMMA_, select_strs_))) {
    LOG_WARN("failed to fill select exprs", K(ret));
  }
  return ret;
}

int ObLinkStmt::force_fill_select_strs()
{
  int ret = OB_SUCCESS;
  if (!is_select_strs_empty()) {
    // nothing.
  } else if (OB_FAIL(fill_exprs(root_output_exprs_, SEP_COMMA_, select_strs_))) {
    LOG_WARN("failed to fill select exprs", K(ret));
  }
  return ret;
}

int ObLinkStmt::append_select_strs(const ObRawExprIArray& param_columns)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_exprs(param_columns, SEP_COMMA_, select_strs_))) {
    LOG_WARN("failed to append select exprs", K(ret));
  }
  return ret;
}

int ObLinkStmt::set_select_distinct()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_distinct_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("set distinct flag again", K(ret));
  } else {
    is_distinct_ = true;
  }
  return ret;
}

int ObLinkStmt::fill_from_set(ObSelectStmt::SetOperator set_type, bool is_distinct)
{
  // '?' => '?' 'union [all]'/'intersect'/'minus' '?'
  int ret = OB_SUCCESS;
  ObStringListIter iter;
  if (OB_FAIL(get_first_table_blank(from_strs_, iter))) {
    LOG_WARN("failed to get first table blank in from strs", K(ret));
  } else if (iter == from_strs_.end()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no question mark in from_strs", K(ret));
  } else if (!is_select_strs_empty() && OB_FAIL(from_strs_.insert(iter, LEFT_BRACKET_))) {
    LOG_WARN("failed to insert '('", K(ret));
  } else if (OB_FAIL(from_strs_.insert(iter, TABLE_BLANK_))) {
    LOG_WARN("failed to insert '?'", K(ret));
  } else if (OB_FAIL(from_strs_.insert(iter, set_type_str(set_type)))) {
    LOG_WARN("failed to insert set type str", K(ret));
  } else if (ObSelectStmt::UNION == set_type && !is_distinct && OB_FAIL(from_strs_.insert(iter, UNION_ALL_))) {
    LOG_WARN("failed to insert union all", K(ret));
  } else if (!is_select_strs_empty() && FALSE_IT(iter++) && OB_FAIL(from_strs_.insert(iter, RIGHT_BRACKET_))) {
    LOG_WARN("failed to insert ')'", K(ret));
  }
  return ret;
}

int ObLinkStmt::fill_from_strs(const TableItem& table_item)
{
  int ret = OB_SUCCESS;
  const ObString& database_name = table_item.database_name_;
  const ObString& table_name = table_item.table_name_;
  const ObString& alias_name = table_item.alias_name_;
  char* name_buf = NULL;
  int64_t name_buf_len = database_name.length() + table_name.length() + alias_name.length() + 2;  // 2 = '.' + ' '
  int32_t name_len = 0;
  ObStringListIter iter;

  if (OB_FAIL(get_first_table_blank(from_strs_, iter))) {
    LOG_WARN("failed to get first table blank in from strs", K(ret));
  } else if (iter == from_strs_.end()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no question mark in from_strs", K(ret));
  } else if (OB_ISNULL(name_buf = static_cast<char*>(alloc_.alloc(name_buf_len)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to alloc name", K(name_buf));
  } else {
    // [database_name.]table_name [alias_name]
    if (!database_name.empty()) {
      MEMCPY(name_buf + name_len, database_name.ptr(), database_name.length());
      name_len += database_name.length();
      name_buf[name_len++] = SEP_DOT_[0];
    }
    MEMCPY(name_buf + name_len, table_name.ptr(), table_name.length());
    name_len += table_name.length();
    if (!alias_name.empty()) {
      name_buf[name_len++] = SEP_SPACE_[0];
      MEMCPY(name_buf + name_len, alias_name.ptr(), alias_name.length());
      name_len += alias_name.length();
    }
    *iter = ObString(name_len, name_buf);
  }
  return ret;
}

int ObLinkStmt::fill_from_strs(const ObLinkStmt& sub_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_from_strs(sub_stmt, NULL_STR_))) {
    LOG_WARN("failed to fill from strs", K(ret));
  }
  return ret;
}

int ObLinkStmt::fill_from_strs(const ObLinkStmt& sub_stmt, const ObString& alias_name)
{
  int ret = OB_SUCCESS;
  char* sub_stmt_buf = NULL;
  int32_t sub_stmt_buf_len = sub_stmt.get_total_size() + alias_name.length() + 3;  // '(' + ')' + ' '
  int32_t sub_stmt_len = 1;                                                        // skip '('
  ObStringListIter iter;

  if (OB_FAIL(get_first_table_blank(from_strs_, iter))) {
    LOG_WARN("failed to get first table blank in from strs", K(ret));
  } else if (iter == from_strs_.end()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no question mark in from_strs", K(ret));
  } else if (OB_ISNULL(sub_stmt_buf = static_cast<char*>(alloc_.alloc(sub_stmt_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc sub stmt buf", K(ret), K(sub_stmt_buf_len));
  } else if (OB_FAIL(sub_stmt.gen_stmt_fmt(sub_stmt_buf, sub_stmt_buf_len, sub_stmt_len))) {
    LOG_WARN("failed to gen sub stmt fmt", K(ret), K(sub_stmt));
  } else {
    sub_stmt_buf[0] = LEFT_BRACKET_[0];
    sub_stmt_buf[sub_stmt_len++] = RIGHT_BRACKET_[0];
    if (!alias_name.empty()) {
      sub_stmt_buf[sub_stmt_len++] = SEP_SPACE_[0];
      MEMCPY(sub_stmt_buf + sub_stmt_len, alias_name.ptr(), alias_name.length());
      sub_stmt_len += alias_name.length();
    }
    // rewrite "?" to sub stmt.
    *iter = ObString(sub_stmt_len, sub_stmt_buf);
  }
  return ret;
}

int ObLinkStmt::fill_from_strs(ObJoinType join_type, const ObRawExprIArray& join_conditions,
    const ObRawExprIArray& join_filters, const ObRawExprIArray& pushdown_filters)
{
  int ret = OB_SUCCESS;
  ObStringListIter iter;
  /**
   * from_strs: "?" => "(" "?" "[left / right / full] join" "?" "on" "cond1" "and cond2" ")"
   *            cond may have @1 if join is nested loop, @1 should be filled in other op
   *            like table_scan.
   *            on cond comes from equal_conds, other_conds and nl_params.
   */
  if (OB_FAIL(get_first_table_blank(from_strs_, iter))) {
    LOG_WARN("failed to get first table blank in from strs", K(ret));
  } else if (iter == from_strs_.end()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no question mark in from_strs", K(ret));
  } else if (OB_FAIL(from_strs_.insert(iter, LEFT_BRACKET_))) {
    LOG_WARN("failed to insert '('", K(ret));
  } else if (OB_FAIL(from_strs_.insert(iter, TABLE_BLANK_))) {
    LOG_WARN("failed to insert '?'", K(ret));
  } else if (OB_FAIL(from_strs_.insert(iter, join_type_str(join_type)))) {
    LOG_WARN("failed to insert join type str", K(ret));
  } else if (OB_FAIL(from_strs_.insert(iter, TABLE_BLANK_))) {
    LOG_WARN("failed to insert '?'", K(ret));
  } else if (OB_FAIL(from_strs_.insert(iter, JOIN_ON_))) {
    LOG_WARN("failed to insert 'on'", K(ret));
  } else if (OB_FAIL(fill_exprs(join_conditions, SEP_AND_, from_strs_, iter))) {
    LOG_WARN("failed to fill link stmt from strs with join conditions", K(ret));
  } else if (OB_FAIL(fill_exprs(join_filters, SEP_AND_, from_strs_, iter))) {
    LOG_WARN("failed to fill link stmt from strs with join filters", K(ret));
  } else if (OB_FAIL(fill_exprs(pushdown_filters, SEP_AND_, from_strs_, iter))) {
    LOG_WARN("failed to fill link stmt from strs with pushdown filters", K(ret));
  } else {
    // rewrite "?" to ")".
    *iter = RIGHT_BRACKET_;
  }
  return ret;
}

int ObLinkStmt::fill_where_strs(const ObRawExprIArray& exprs, bool skip_nl_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_exprs(exprs, SEP_AND_, where_strs_, skip_nl_param))) {
    LOG_WARN("failed to fill where strs", K(ret));
  }
  return ret;
}

int ObLinkStmt::fill_where_rownum(const ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  ObStringListIter iter = where_strs_.end();
  if (OB_ISNULL(expr)) {
    // nothing.
  } else if (where_strs_.size() > 0 && OB_FAIL(where_strs_.insert(iter, SEP_AND_))) {
    LOG_WARN("failed to append sep and", K(ret));
  } else if (OB_FAIL(where_strs_.insert(iter, ROWNUM_))) {
    LOG_WARN("failed to append rownum", K(ret));
  } else if (OB_FAIL(fill_expr(expr, LESS_EQUAL_, where_strs_))) {
    LOG_WARN("failed to fill where strs with rownum limit", K(ret));
  }
  return ret;
}

int ObLinkStmt::fill_groupby_strs(const ObRawExprIArray& exprs)
{
  int ret = OB_SUCCESS;
  if (groupby_strs_.size() > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group by clause already filled", K(groupby_strs_));
  } else if (OB_FAIL(fill_exprs(exprs, SEP_COMMA_, groupby_strs_))) {
    LOG_WARN("failed to fill group by strs", K(ret), K(exprs));
  }
  return ret;
}

int ObLinkStmt::fill_having_strs(const ObRawExprIArray& exprs)
{
  int ret = OB_SUCCESS;
  if (having_strs_.size() > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("having clause already filled", K(having_strs_));
  } else if (OB_FAIL(fill_exprs(exprs, SEP_AND_, having_strs_))) {
    LOG_WARN("failed to fill having strs", K(ret), K(exprs));
  }
  return ret;
}

int ObLinkStmt::fill_orderby_strs(const ObOrderItemIArray& order_items, const ObRawExprIArray& output_exprs)
{
  /*
   * OceanBase(ADMIN@TEST)>explain
   *     -> select a - 3, x from test.t1@my_link1 union select c * 2, y from test.t2@my_link1 order by 1;
   * | =================================================
   * |ID|OPERATOR             |NAME|EST. ROWS|COST   |
   * -------------------------------------------------
   * |0 |LINK                 |    |0        |1070149|
   * |1 | MERGE UNION DISTINCT|    |200000   |1070149|
   * |2 |  SORT               |    |100000   |496872 |
   * |3 |   TABLE SCAN        |T1  |100000   |61860  |
   * |4 |  SORT               |    |100000   |496872 |
   * |5 |   TABLE SCAN        |T2  |100000   |61860  |
   * =================================================
   *
   * Outputs & filters:
   * -------------------------------------
   *   0 - output([UNION((T1.A - 3), (T2.C * 2))], [UNION(cast(T1.X, VARCHAR(10 BYTE)), cast(T2.Y, VARCHAR(10 BYTE)))]),
   * filter(nil), dblink_id=1100611139403793, link_stmt=(select (T1.A - 3), T1.X from (select T1.A, T1.X from T1) T1)
   * union (select (T2.C * 2), T2.Y from (select T2.C, T2.Y from T2) T2) order by 1 asc, 2 asc 1 - output([UNION((T1.A -
   * 3), (T2.C * 2))], [UNION(cast(T1.X, VARCHAR(10 BYTE)), cast(T2.Y, VARCHAR(10 BYTE)))]), filter(nil) 2 -
   * output([(T1.A - 3)], [cast(T1.X, VARCHAR(10 BYTE))]), filter(nil), sort_keys([(T1.A - 3), ASC], [cast(T1.X,
   * VARCHAR(10 BYTE)), ASC]) 3 - output([(T1.A - 3)], [cast(T1.X, VARCHAR(10 BYTE))]), filter(nil), access([T1.A],
   * [T1.X]), partitions(p0) 4 - output([(T2.C * 2)], [cast(T2.Y, VARCHAR(10 BYTE))]), filter(nil), sort_keys([(T2.C *
   * 2), ASC], [cast(T2.Y, VARCHAR(10 BYTE)), ASC]) 5 - output([(T2.C * 2)], [cast(T2.Y, VARCHAR(10 BYTE))]),
   * filter(nil), access([T2.C], [T2.Y]), partitions(p0)
   *
   * that is why we need output_exprs, the output of op 1 'MERGE UNION DISTINCT' have no
   * column alias name, the op_ordering of op 1 'LINK' is 'UNION((T1.A - 3), (T2.C * 2))'.
   * both 'order by UNION((T1.A - 3), (T2.C * 2))' and 'order by T1.A - 3' cause error:
   * invalid identifier 'A' in 'order clause'.
   * so the only way is 'order by 1', we can compare the raw expr pointer in order_items
   * and output_exprs to get the index.
   */
  int ret = OB_SUCCESS;
  ObConstRawExpr index_expr;
  ObObj index_value;
  ObRawExpr* item_expr = NULL;
  if (orderby_strs_.size() > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("order by clause already filled", K(orderby_strs_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); i++) {
    int64_t item_index = get_order_item_index(order_items.at(i).expr_, output_exprs);
    if (item_index < 0) {
      item_expr = order_items.at(i).expr_;
    } else {
      index_value.set_int(item_index + 1);
      index_expr.set_value(index_value);
      item_expr = static_cast<ObRawExpr*>(&index_expr);
    }
    if (OB_FAIL(fill_expr(item_expr, SEP_COMMA_, orderby_strs_))) {
      LOG_WARN("failed to fill expr", K(ret), K(i));
    } else if ((order_items.at(i).order_type_ == NULLS_FIRST_ASC || order_items.at(i).order_type_ == NULLS_LAST_ASC) &&
               OB_FAIL(orderby_strs_.push_back((ORDER_ASC_)))) {
      LOG_WARN("failed to push back asc", K(ret));
    } else if ((order_items.at(i).order_type_ == NULLS_FIRST_DESC ||
                   order_items.at(i).order_type_ == NULLS_LAST_DESC) &&
               OB_FAIL(orderby_strs_.push_back((ORDER_DESC_)))) {
      LOG_WARN("failed to push back desc", K(ret));
    }
  }
  return ret;
}

int ObLinkStmt::fill_exprs(const ObRawExprIArray& exprs, const ObString& sep, ObStringList& strs, bool skip_nl_param)
{
  int ret = OB_SUCCESS;
  ObStringListIter iter = strs.end();
  if (OB_FAIL(fill_exprs(exprs, sep, strs, iter, skip_nl_param))) {
    LOG_WARN("failed to fill exprs", K(ret));
  }
  return ret;
}

int ObLinkStmt::fill_exprs(
    const ObRawExprIArray& exprs, const ObString& sep, ObStringList& strs, ObStringListIter& iter, bool skip_nl_param)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    bool in_nl_param = false;
    if (skip_nl_param && OB_FAIL(expr_in_nl_param(exprs.at(i), in_nl_param))) {
      LOG_WARN("failed to judge if expr in nl param", K(ret), K(i), KPC(exprs.at(i)));
    } else if (in_nl_param) {
      // nothing.
    } else if (OB_FAIL(fill_expr(exprs.at(i), sep, strs, iter))) {
      LOG_WARN("failed to fill expr", K(ret), K(i), KPC(exprs.at(i)));
    }
  }
  return ret;
}

int ObLinkStmt::fill_expr(const ObRawExpr* expr, const ObString& sep, ObStringList& strs)
{
  int ret = OB_SUCCESS;
  ObStringListIter iter = strs.end();
  if (OB_FAIL(fill_expr(expr, sep, strs, iter))) {
    LOG_WARN("failed to fill expr", K(ret));
  }
  return ret;
}

int ObLinkStmt::fill_expr(const ObRawExpr* expr, const ObString& sep, ObStringList& strs, ObStringListIter& iter)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObRawExprPrinter expr_printer;
  expr_printer.init(tmp_buf_, tmp_buf_len_, &pos, NULL);
  char* expr_buf = NULL;
  int64_t expr_buf_len = 0;
  bool has_alias = false;
  bool need_bracket = false;
  const ObString& alias_name = OB_NOT_NULL(expr) ? expr->get_root_alias_column_name() : NULL_STR_;
  if (OB_ISNULL(expr) || OB_ISNULL(tmp_buf_) || tmp_buf_len_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr or tmp buf is NULL", K(ret), KP(expr), KP(tmp_buf_), K(tmp_buf_len_));
  } else if (OB_FAIL(expr_printer.do_print(const_cast<ObRawExpr*>(expr), T_DBLINK_SCOPE))) {
    LOG_WARN("failed to print raw expr", K(ret), KPC(expr));
  } else if (FALSE_IT(expr_buf_len = pos)) {
    // do nothing
  } else if (OB_ISNULL(expr_buf = static_cast<char*>(alloc_.alloc(expr_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc expr buf", K(ret));
  } else if (strs.size() > 0 && OB_FAIL(strs.insert(iter, sep))) {
    LOG_WARN("failed to append sep", K(ret), K(sep));
  } else if (OB_FAIL(strs.insert(iter, ObString(expr_buf_len, expr_buf)))) {
    LOG_WARN("failed to insert str", K(ret), "expr_str", ObString(pos, expr_buf));
  } else {
    int64_t tmp_len = pos;
    LOG_DEBUG("fill expr buf", K(ObString(tmp_buf_)));
    pos = (need_bracket ? 1 : 0);
    MEMCPY(expr_buf + pos, tmp_buf_, tmp_len);
    pos += tmp_len;
    if (has_alias) {
      expr_buf[pos++] = SEP_SPACE_[0];
      MEMCPY(expr_buf + pos, alias_name.ptr(), alias_name.length());
      pos += alias_name.length();
    }
    if (need_bracket) {
      expr_buf[0] = LEFT_BRACKET_[0];
      expr_buf[pos++] = RIGHT_BRACKET_[0];
    }
  }
  return ret;
}

int ObLinkStmt::get_first_table_blank(ObStringList& strs, ObStringListIter& iter) const
{
  int ret = OB_SUCCESS;
  for (iter = strs.begin(); OB_SUCC(ret) && iter != strs.end(); iter++) {
    if (*iter == TABLE_BLANK_) {
      break;
    }
  }
  return ret;
}

int ObLinkStmt::gen_stmt_fmt(char* buf, int32_t buf_len, int32_t& pos) const
{
  int ret = OB_SUCCESS;
  const ObString& select_clause = is_distinct_ ? SELECT_DIS_CLAUSE_ : SELECT_CLAUSE_;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt buf is NULL", K(ret));
  } else if (OB_FAIL(merge_string_list(select_strs_, select_clause, buf, buf_len, pos))) {
    LOG_WARN("failed to merge output strs", K(ret), K(select_strs_));
  } else if (OB_FAIL(merge_string_list(from_strs_, from_clause_str(), buf, buf_len, pos))) {
    LOG_WARN("failed to merge from strs", K(ret), K(from_strs_));
  } else if (OB_FAIL(merge_string_list(where_strs_, WHERE_CLAUSE_, buf, buf_len, pos))) {
    LOG_WARN("failed to merge where strs", K(ret), K(where_strs_));
  } else if (OB_FAIL(merge_string_list(groupby_strs_, GROUPBY_CLAUSE_, buf, buf_len, pos))) {
    LOG_WARN("failed to merge groupby strs", K(ret), K(groupby_strs_));
  } else if (OB_FAIL(merge_string_list(having_strs_, HAVING_CLAUSE_, buf, buf_len, pos))) {
    LOG_WARN("failed to merge having strs", K(ret), K(having_strs_));
  } else if (OB_FAIL(merge_string_list(orderby_strs_, ORDERBY_CLAUSE_, buf, buf_len, pos))) {
    LOG_WARN("failed to merge orderby strs", K(ret), K(orderby_strs_));
  } else if (OB_FAIL(append_string(limit_str_, LIMIT_CLAUSE_, buf, buf_len, pos))) {
    LOG_WARN("failed to merge limit str", K(ret), K(limit_str_));
  } else if (OB_FAIL(append_string(offset_str_, OFFSET_CLAUSE_, buf, buf_len, pos))) {
    LOG_WARN("failed to merge offset str", K(ret), K(offset_str_));
  }
  return ret;
}

int ObLinkStmt::get_nl_param_columns(
    ObRawExprIArray& param_exprs, ObRawExprIArray& access_exprs, ObRawExprIArray& param_columns)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); i++) {
    bool in_nl_param = false;
    if (OB_ISNULL(param_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(i), K(ret));
    } else if (!param_exprs.at(i)->has_flag(CNT_EXEC_PARAM)) {
      // nothing.
    } else if (OB_FAIL(expr_in_nl_param(param_exprs.at(i), in_nl_param))) {
      LOG_WARN("failed to judge if expr in nl param", K(ret), K(i), K(*param_exprs.at(i)));
    } else if (!in_nl_param) {
      // nothing.
    } else if (OB_FAIL(get_nl_param_columns(param_exprs.at(i), access_exprs, param_columns))) {
      LOG_WARN("failed to get nl param columns", K(ret), K(i), K(*param_exprs.at(i)));
    }
  }
  return ret;
}

int32_t ObLinkStmt::get_total_size() const
{
  const ObString& select_clause = is_distinct_ ? SELECT_DIS_CLAUSE_ : SELECT_CLAUSE_;
  return get_total_size(select_strs_, select_clause) + get_total_size(from_strs_, FROM_CLAUSE_) +
         get_total_size(where_strs_, WHERE_CLAUSE_) + get_total_size(groupby_strs_, GROUPBY_CLAUSE_) +
         get_total_size(having_strs_, HAVING_CLAUSE_) + get_total_size(orderby_strs_, ORDERBY_CLAUSE_) +
         get_total_size(limit_str_, LIMIT_CLAUSE_) + get_total_size(offset_str_, OFFSET_CLAUSE_);
}

int32_t ObLinkStmt::get_total_size(const ObStringList& str_list, const ObString& clause) const
{
  int32_t total_len = 0;
  for (ObStringListConstIter iter = str_list.begin(); iter != str_list.end(); iter++) {
    const ObString& str = *iter;
    total_len += str.length();
  }
  return total_len > 0 ? total_len + clause.length() : 0;
}

int32_t ObLinkStmt::get_total_size(const ObStringIArray& str_array, const ObString& clause) const
{
  int32_t total_len = 0;
  for (int64_t i = 0; i < str_array.count(); i++) {
    total_len += str_array.at(i).length();
  }
  return total_len > 0 ? total_len + clause.length() : 0;
}

int32_t ObLinkStmt::get_total_size(const ObString& str, const ObString& clause) const
{
  return str.length() > 0 ? str.length() + clause.length() : 0;
}

int ObLinkStmt::merge_string_list(
    const ObStringList& str_list, const ObString& clause, char* buf, int32_t buf_len, int32_t& pos) const
{
  int ret = OB_SUCCESS;
  if (!str_list.empty() && !clause.empty() && OB_FAIL(append_string(clause, buf, buf_len, pos))) {
    LOG_WARN("failed to merge clause", K(ret));
  }
  for (ObStringListConstIter iter = str_list.begin(); OB_SUCC(ret) && iter != str_list.end(); iter++) {
    const ObString& str = *iter;
    if (OB_FAIL(append_string(str, buf, buf_len, pos))) {
      LOG_WARN("failed to merge string", K(ret));
    }
  }
  return ret;
}

int ObLinkStmt::append_string(
    const ObString str, const ObString& clause, char* buf, int32_t buf_len, int32_t& pos) const
{
  int ret = OB_SUCCESS;
  if (!str.empty() && !clause.empty() && OB_FAIL(append_string(clause, buf, buf_len, pos))) {
    LOG_WARN("failed to merge clause", K(ret));
  } else if (OB_FAIL(append_string(str, buf, buf_len, pos))) {
    LOG_WARN("failed to merge string", K(ret));
  }
  return ret;
}

int ObLinkStmt::append_string(const ObString str, char* buf, int32_t buf_len, int32_t& pos) const
{
  int ret = OB_SUCCESS;
  if (str.empty()) {
    // need handle or just do nothing?
  } else if (pos + str.length() > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is not enough", K(ret), K(str), K(pos), K(buf_len));
  } else /*if (str[0] > 0)*/ {
    // normal string.
    MEMCPY(buf + pos, str.ptr(), str.length());
    pos += str.length();
  }
  return ret;
}

const ObString& ObLinkStmt::join_type_str(ObJoinType join_type) const
{
  static const ObString join_type_strs[] = {
      " unknown join",
      " inner join ",
      " left join ",
      " right join ",
      " full join ",
      " left join ",
      " right join ",
      " left join ",
      " right join ",
      " connect by ",
  };
  if (OB_UNLIKELY(join_type < UNKNOWN_JOIN || join_type > CONNECT_BY_JOIN)) {
    join_type = UNKNOWN_JOIN;
  }
  return join_type_strs[join_type];
}

const ObString& ObLinkStmt::set_type_str(ObSelectStmt::SetOperator set_type) const
{
  static const ObString set_type_strs[] = {
      " none ",
      " union ",
      " intersect ",
      " minus ",
      " recursive ",
  };
  if (OB_UNLIKELY(set_type <= ObSelectStmt::NONE || set_type >= ObSelectStmt::SET_OP_NUM)) {
    set_type = ObSelectStmt::NONE;
  }
  return set_type_strs[set_type];
}

const ObString& ObLinkStmt::from_clause_str() const
{
  return is_select_strs_empty() ? NULL_STR_ : FROM_CLAUSE_;
}

int ObLinkStmt::expr_in_nl_param(ObRawExpr* expr, bool& in_nl_param)
{
  in_nl_param = false;
  return do_expr_in_nl_param(expr, in_nl_param);
}

int ObLinkStmt::do_expr_in_nl_param(ObRawExpr* expr, bool& in_nl_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret));
  } else if (OB_ISNULL(link_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("link ctx is NULL", K(ret));
  } else if (expr->get_expr_type() == T_QUESTIONMARK) {
    ObConstRawExpr* const_expr = static_cast<ObConstRawExpr*>(expr);
    if (has_exist_in_array(link_ctx_->nl_param_idxs_, const_expr->get_value().get_unknown())) {
      in_nl_param = true;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !in_nl_param && i < expr->get_param_count(); i++) {
      ObRawExpr* child_expr = expr->get_param_expr(i);
      if (OB_FAIL(do_expr_in_nl_param(child_expr, in_nl_param))) {
        LOG_WARN("failed to judge if expr in nl param", K(ret), K(i), KPC(child_expr));
      }
    }
  }
  return ret;
}

int ObLinkStmt::get_nl_param_columns(
    ObRawExpr* param_expr, ObRawExprIArray& access_exprs, ObRawExprIArray& param_columns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is NULL", K(ret));
  } else if (param_expr->is_column_ref_expr()) {
    if (!has_exist_in_array(access_exprs, param_expr) && OB_FAIL(add_var_to_array_no_dup(param_columns, param_expr))) {
      LOG_WARN("failed to add nl param column", K(ret), K(param_expr));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_expr->get_param_count(); i++) {
      ObRawExpr* child_expr = param_expr->get_param_expr(i);
      if (OB_FAIL(get_nl_param_columns(child_expr, access_exprs, param_columns))) {
        LOG_WARN("failed to get nl param columns", K(ret), K(i), KPC(child_expr));
      }
    }
  }
  return ret;
}

int64_t ObLinkStmt::get_order_item_index(const ObRawExpr* order_item_expr, const ObRawExprIArray& output_exprs)
{
  int64_t index = -1;
  if (OB_NOT_NULL(order_item_expr)) {
    const ObRawExpr* order_item_root_expr = order_item_expr->get_root_orig_expr();
    for (int64_t i = 0; i < output_exprs.count(); i++) {
      if (OB_NOT_NULL(output_exprs.at(i)) && order_item_expr == output_exprs.at(i) &&
          order_item_root_expr == output_exprs.at(i)->get_root_orig_expr()) {
        index = i;
        break;
      }
    }
  }
  return index;
}

const ObString ObLinkStmt::TABLE_BLANK_ = "?";
const ObString ObLinkStmt::JOIN_ON_ = " on (1 = 1)";
const ObString ObLinkStmt::LEFT_BRACKET_ = "(";
const ObString ObLinkStmt::RIGHT_BRACKET_ = ")";
const ObString ObLinkStmt::SEP_DOT_ = ".";
const ObString ObLinkStmt::SEP_COMMA_ = ", ";
const ObString ObLinkStmt::SEP_AND_ = " and ";
const ObString ObLinkStmt::SEP_SPACE_ = " ";
const ObString ObLinkStmt::UNION_ALL_ = "all ";
const ObString ObLinkStmt::ORDER_ASC_ = " asc";
const ObString ObLinkStmt::ORDER_DESC_ = " desc";
const ObString ObLinkStmt::ROWNUM_ = "rownum";
const ObString ObLinkStmt::LESS_EQUAL_ = " <= ";
const ObString ObLinkStmt::SELECT_CLAUSE_ = "select ";
const ObString ObLinkStmt::SELECT_DIS_CLAUSE_ = "select distinct ";
const ObString ObLinkStmt::FROM_CLAUSE_ = " from ";
const ObString ObLinkStmt::WHERE_CLAUSE_ = " where ";
const ObString ObLinkStmt::GROUPBY_CLAUSE_ = " group by ";
const ObString ObLinkStmt::HAVING_CLAUSE_ = " having ";
const ObString ObLinkStmt::ORDERBY_CLAUSE_ = " order by ";
const ObString ObLinkStmt::LIMIT_CLAUSE_ = " limit ";
const ObString ObLinkStmt::OFFSET_CLAUSE_ = " offset ";
const ObString ObLinkStmt::NULL_STR_ = "";

void ObLogicalOperator::set_parent(ObLogicalOperator* parent)
{
  if (parent) {
    LOG_TRACE("set parent",
        "op_name",
        this->get_name(),
        "parent_op_name",
        parent->get_name(),
        "before",
        parent_,
        "after",
        parent,
        K(lbt()));
  } else {
    LOG_TRACE("set parent", "op_name", this->get_name(), "before", parent_, "after", parent, K(lbt()));
  }
  parent_ = parent;
}

int ObLogicalOperator::allocate_monitoring_dump_node_above(uint64_t flags, uint64_t dst_op_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_plan()));
  } else if (LOG_EXCHANGE == get_type() && (static_cast<ObLogExchange*>(this)->is_producer() ||
                                               (static_cast<ObLogExchange*>(this)->is_consumer() &&
                                                   static_cast<ObLogExchange*>(this)->get_is_remote()))) {
    // Do nothing.
  } else {
    ObLogicalOperator* log_op = NULL;
    ObLogOperatorFactory& factory = get_plan()->get_log_op_factory();
    if (OB_ISNULL(log_op = factory.allocate(*(get_plan()), LOG_MONITORING_DUMP))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to allocate exchange nodes", K(log_op));
    } else {
      ObLogMonitoringDump* monitoring_dump = static_cast<ObLogMonitoringDump*>(log_op);
      if (NULL != get_parent()) {
        bool found_child = false;
        for (int64_t i = 0; OB_SUCC(ret) && !found_child && i < get_parent()->get_num_of_child(); ++i) {
          if (get_parent()->get_child(i) == this) {
            get_parent()->set_child(i, monitoring_dump);
            found_child = true;
          } else { /*do nothing*/
          }
        }
        monitoring_dump->set_parent(get_parent());
      } else { /*do nothing*/
      }
      set_parent(monitoring_dump);
      monitoring_dump->set_child(first_child, this);
      monitoring_dump->set_cost(get_cost());
      monitoring_dump->set_card(get_card());
      monitoring_dump->set_width(get_width());
      monitoring_dump->set_flags(flags);
      monitoring_dump->set_dst_op_id(dst_op_id);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(monitoring_dump->compute_property())) {
          LOG_WARN("Failed to compute equal sets", K(ret));
        }
      }

      if (is_plan_root()) {
        set_is_plan_root(false);
        monitoring_dump->mark_is_plan_root();
        get_plan()->set_plan_root(monitoring_dump);
      } else { /*do nothing*/
      }

      LOG_TRACE("succ to allocate monitoring dump node above operator",
          K(get_name()),
          K(get_cost()),
          K(get_card()),
          K(flags));
    }
  }
  return ret;
}

/**
 * allocate_xxxxx_below
 *
 *       this                       this
 *         |                          |
 *       child         ==>          xxxxx
 *                                    |
 *                                  child
 */
int ObLogicalOperator::allocate_distinct_below(
    int64_t index, ObIArray<ObRawExpr*>& distinct_exprs, const AggregateAlgo algo)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  if (OB_ISNULL(get_child(index)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(get_child(index)), K(get_plan()));
  } else if (OB_ISNULL(op = get_plan()->get_log_op_factory().allocate(*get_plan(), log_op_def::LOG_DISTINCT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for log distinct operator", K(ret));
  } else {
    op->set_child(first_child, get_child(index));
    op->set_parent(this);
    get_child(index)->set_parent(op);
    set_child(index, op);
    ObLogDistinct* distinct = static_cast<ObLogDistinct*>(op);
    distinct->set_algo_type(algo);
    if (OB_FAIL(distinct->set_distinct_exprs(distinct_exprs))) {
      LOG_WARN("failed to set distinct exprs", K(ret));
    } else if (OB_FAIL(distinct->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      LOG_TRACE("allocate distinct bellow", "parent", get_name(), "child", get_child(index)->get_name());
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_sort_below(const int64_t index, const ObIArray<OrderItem>& order_keys)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  if (OB_ISNULL(get_child(index)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(get_child(index)), K(get_plan()));
  } else if (OB_ISNULL(op = get_plan()->get_log_op_factory().allocate(*get_plan(), log_op_def::LOG_SORT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for log sort operator", K(ret));
  } else {
    op->set_child(first_child, get_child(index));
    op->set_parent(this);
    get_child(index)->set_parent(op);
    set_child(index, op);
    ObLogSort* sort = static_cast<ObLogSort*>(op);
    if (OB_FAIL(sort->set_sort_keys(order_keys))) {
      LOG_WARN("failed to set sort keys", K(ret));
    } else if (OB_FAIL(sort->check_prefix_sort())) {
      LOG_WARN("failed to check prefix sort", K(ret));
    } else if (OB_FAIL(op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      LOG_TRACE("allocate sort bellow", "parent", get_name(), "child", get_child(index)->get_name());
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_topk_below(const int64_t index, ObSelectStmt* stmt)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  if (OB_ISNULL(get_child(index)) || OB_ISNULL(get_plan()) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(get_child(index)), K(get_plan()), K(stmt));
  } else if (OB_ISNULL(op = get_plan()->get_log_op_factory().allocate(*get_plan(), log_op_def::LOG_TOPK))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for log topk operator", K(ret));
  } else {
    op->set_child(first_child, get_child(index));
    op->set_parent(this);
    get_child(index)->set_parent(op);
    set_child(index, op);
    ObLogTopk* topk = static_cast<ObLogTopk*>(op);
    ObStmtHint& stmt_hint = stmt->get_stmt_hint();
    if (OB_FAIL(topk->set_topk_params(stmt->get_limit_expr(),
            stmt->get_offset_expr(),
            stmt_hint.sharding_minimum_row_count_,
            stmt_hint.topk_precision_))) {
      LOG_WARN("failed to set topk params", K(ret));
    } else if (OB_FAIL(topk->set_topk_size())) {
      LOG_WARN("failed to set topk size", K(ret));
    } else if (OB_FAIL(topk->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      LOG_TRACE("allocate topk bellow", "parent", get_name(), "child", get_child(index)->get_name());
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_material_below(const int64_t index)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  if (OB_ISNULL(get_child(index)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(get_child(index)), K(get_plan()));
  } else if (OB_ISNULL(op = get_plan()->get_log_op_factory().allocate(*get_plan(), log_op_def::LOG_MATERIAL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for log material operator", K(ret));
  } else {
    op->set_child(first_child, get_child(index));
    op->set_parent(this);
    get_child(index)->set_parent(op);
    set_child(index, op);
    ObLogMaterial* material = static_cast<ObLogMaterial*>(op);
    if (OB_FAIL(material->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      LOG_TRACE("allocate material bellow", "parent", get_name(), "child", get_child(index)->get_name());
    }
  }
  return ret;
}

int ObLogicalOperator::calc_current_dfo_table_dop(ObLogicalOperator* root, int64_t& table_dop, bool& found_base_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the logical op is null", K(ret));
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
      } else if (OB_ISNULL(schema_guard = my_plan_->get_optimizer_context().get_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema guard is null", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(index_id, table_schema))) {
        LOG_WARN("failed get table schema", K(ret), K(index_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table schema, the table schema is null", K(ret), K(index_id));
      } else if (table_schema->get_dop() < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table dop", K(ret), K(table_schema->get_dop()));
      } else if (!found_base_table) {
        found_base_table = true;
        table_dop = table_schema->get_dop();
      } else if (table_schema->get_dop() > table_dop) {
        table_dop = table_schema->get_dop();
      }
    } else if (!found_base_table && log_op_def::LOG_EXCHANGE == op_type && table_dop < 0) {
      ObLogExchange* exchange = static_cast<ObLogExchange*>(root);
      if (exchange->get_px_dop() < 1) {
        table_dop = 1;
      } else {
        table_dop = exchange->get_px_dop();
      }
    }
    if (op_type != log_op_def::LOG_EXCHANGE) {
      for (int i = 0; i < root->get_num_of_child() && OB_SUCC(ret); i++) {
        ObLogicalOperator* child = root->get_child(i);
        if (OB_FAIL(calc_current_dfo_table_dop(child, table_dop, found_base_table))) {
          LOG_WARN("failed to get current dfo table dop", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_limit_below(const int64_t index, ObRawExpr* limit_expr)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  double limit_count = 0.0;
  bool dummy_is_null_value = false;
  ObLogicalOperator* op = NULL;
  if (OB_ISNULL(limit_expr) || OB_ISNULL(get_child(index)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(limit_expr), K(get_child(index)), K(get_plan()));
  } else if (OB_ISNULL(op = get_plan()->get_log_op_factory().allocate(*get_plan(), log_op_def::LOG_LIMIT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for log limit operator", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_limit_value(limit_expr,
                 get_plan()->get_stmt(),
                 get_plan()->get_optimizer_context().get_params(),
                 get_plan()->get_optimizer_context().get_session_info(),
                 &get_plan()->get_optimizer_context().get_allocator(),
                 value,
                 dummy_is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else {
    limit_count = static_cast<double>(value);
    op->set_child(first_child, get_child(index));
    op->set_parent(this);
    get_child(index)->set_parent(op);
    set_child(index, op);
    ObLogLimit* limit = static_cast<ObLogLimit*>(op);
    limit->set_limit_count(limit_expr);
    if (OB_FAIL(limit->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_group_by_below(
    int64_t index, ObIArray<ObRawExpr*>& group_by_exprs, const AggregateAlgo algo, const bool from_pivot)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  if (OB_ISNULL(get_child(index)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(get_child(index)), K(get_plan()));
  } else if (OB_ISNULL(op = get_plan()->get_log_op_factory().allocate(*get_plan(), log_op_def::LOG_GROUP_BY))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for log group by operator", K(ret));
  } else {
    op->set_child(first_child, get_child(index));
    op->set_parent(this);
    get_child(index)->set_parent(op);
    set_child(index, op);
    ObLogGroupBy* group_by = static_cast<ObLogGroupBy*>(op);
    group_by->set_algo_type(algo);
    group_by->set_from_pivot(from_pivot);
    if (OB_FAIL(group_by->set_group_by_exprs(group_by_exprs))) {
      LOG_WARN("failed to set group by exprs", K(ret));
    } else if (OB_FAIL(group_by->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      LOG_TRACE("allocate group by bellow", "parent", get_name(), "child", get_child(index)->get_name());
    }
  }
  return ret;
}

int ObLogicalOperator::check_match_remote_sharding(const ObAddr& server, bool& is_match) const
{
  int ret = OB_SUCCESS;
  if (LOG_TABLE_SCAN == get_type()) {
    const ObLogTableScan* table_scan = static_cast<const ObLogTableScan*>(this);
    if (OB_ISNULL(table_scan->get_table_partition_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(check_match_server_addr(
                   table_scan->get_table_partition_info()->get_phy_tbl_location_info(), server, is_match))) {
      LOG_WARN("failed to check if match server addr", K(ret));
    } else { /*do nothing*/
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < get_num_of_child(); ++i) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(get_child(i)->check_match_remote_sharding(server, is_match)))) {
        LOG_WARN("failed to check child has global index", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::check_match_server_addr(
    const ObPhyTableLocationInfo& phy_loc_info, const ObAddr& server, bool& is_match)
{
  int ret = OB_SUCCESS;
  share::ObReplicaLocation replica_loc;
  const ObPhyPartitionLocationInfoIArray& locations = phy_loc_info.get_phy_part_loc_info_list();
  is_match = false;
  if (1 != locations.count()) {
    is_match = false;
  } else if (OB_FAIL(locations.at(0).get_selected_replica(replica_loc))) {
    LOG_WARN("fail to get selected replica", K(ret), K(locations.at(0)));
  } else if (!replica_loc.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("replica location is invalid", K(ret), K(replica_loc));
  } else {
    is_match = (replica_loc.server_ == server);
  }
  return ret;
}

int ObLogicalOperator::print_operator_for_use_join_filter(planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  if (OB_FAIL(print_operator_for_outline(plan_text))) {
    LOG_WARN("fail to print operator for outline", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(" "))) {
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogicalOperator::get_part_exprs(uint64_t table_id, uint64_t ref_table_id, schema::ObPartitionLevel& part_level,
    ObRawExpr*& part_expr, ObRawExpr*& subpart_expr)
{
  int ret = OB_SUCCESS;
  part_expr = NULL;
  subpart_expr = NULL;
  ObDMLStmt* stmt = NULL;
  share::schema::ObSchemaGetterGuard* schema_guard = NULL;
  const share::schema::ObTableSchema* table_schema = NULL;
  ObLogPlan* log_plan = NULL;
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(log_plan = get_plan()) || OB_ISNULL(stmt = log_plan->get_stmt()) || OB_INVALID_ID == ref_table_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id", K(ref_table_id), K(ret));
  } else if (OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ref_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_schema));
  } else {
    part_level = table_schema->get_part_level();
    part_expr = stmt->get_part_expr(table_id, ref_table_id);
    subpart_expr = stmt->get_subpart_expr(table_id, ref_table_id);
  }

  return ret;
}

int ObLogicalOperator::gen_calc_part_id_expr(uint64_t table_id, uint64_t ref_table_id, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  expr = NULL;
  ObSQLSessionInfo* session = NULL;
  share::schema::ObPartitionLevel part_level = share::schema::PARTITION_LEVEL_MAX;
  ObRawExpr* part_expr = NULL;
  ObRawExpr* subpart_expr = NULL;
  if (OB_ISNULL(get_plan()) || OB_INVALID_ID == ref_table_id) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null");
  } else if (!session->use_static_typing_engine()) {
    // do nothing
  } else if (OB_FAIL(get_part_exprs(table_id, ref_table_id, part_level, part_expr, subpart_expr))) {
    LOG_WARN("fail to get part exprs", K(ret));
  } else {
    ObRawExprFactory& expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
    if (OB_FAIL(ObRawExprUtils::build_calc_part_id_expr(
            expr_factory, *session, ref_table_id, part_level, part_expr, subpart_expr, expr))) {
      LOG_WARN("fail to build table location expr", K(ret));
    }
  }

  return ret;
}

int ObLogicalOperator::prune_weak_part_exprs(
    const AllocExchContext& ctx, const ObIArray<ObRawExpr*>& input_exprs, ObIArray<ObRawExpr*>& output_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_exprs.count(); ++i) {
    if (check_weak_part_expr(input_exprs.at(i), ctx.weak_part_exprs_)) {
      // remove
    } else if (OB_FAIL(output_exprs.push_back(input_exprs.at(i)))) {
      LOG_WARN("failed to push back input expr", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::prune_weak_part_exprs(const AllocExchContext& ctx, const ObIArray<ObRawExpr*>& left_input_exprs,
    const ObIArray<ObRawExpr*>& right_input_exprs, ObIArray<ObRawExpr*>& left_output_exprs,
    ObIArray<ObRawExpr*>& right_output_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left_input_exprs.count() != right_input_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "the array size is expected to be the same", K(ret), K(left_input_exprs.count()), K(right_input_exprs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < left_input_exprs.count(); ++i) {
    if (check_weak_part_expr(left_input_exprs.at(i), ctx.weak_part_exprs_) ||
        check_weak_part_expr(right_input_exprs.at(i), ctx.weak_part_exprs_)) {
      // do nothing
    } else if (OB_FAIL(left_output_exprs.push_back(left_input_exprs.at(i)))) {
      LOG_WARN("failed to push back array", K(ret));
    } else if (OB_FAIL(right_output_exprs.push_back(right_input_exprs.at(i)))) {
      LOG_WARN("failed to push back array", K(ret));
    }
  }
  return ret;
}

bool ObLogicalOperator::check_weak_part_expr(const ObRawExpr* expr, const ObIArray<ObRawExpr*>& weak_part_exprs)
{
  bool is_weak_part = false;
  for (int64_t i = 0; !is_weak_part && i < weak_part_exprs.count(); ++i) {
    is_weak_part = ObOptimizerUtil::is_sub_expr(weak_part_exprs.at(i), expr);
  }
  return is_weak_part;
}

int ObLogicalOperator::reselect_duplicate_table_replica()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 4> candi_addrs;
  if (OB_FAIL(get_duplicate_table_replica(candi_addrs))) {
    LOG_WARN("failed to get duplicate table replicas", K(ret));
  } else if (candi_addrs.empty()) {
    // do nothing
  } else if (OB_FAIL(set_duplicate_table_replica(candi_addrs))) {
    LOG_WARN("failed to set dupliate table replica", K(ret));
  }
  return ret;
}

int ObLogicalOperator::get_duplicate_table_replica(ObIArray<ObAddr>& candi_addrs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    ObSEArray<ObAddr, 4> child_candi_addrs;
    ObSEArray<ObAddr, 4> new_candi_addrs;
    ObLogicalOperator* child = NULL;
    if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(child));
    } else if (OB_FAIL(SMART_CALL(child->get_duplicate_table_replica(child_candi_addrs)))) {
      LOG_WARN("failed to select duplicate table replica", K(ret));
    } else if (i == 0) {
      if (OB_FAIL(new_candi_addrs.assign(child_candi_addrs))) {
        LOG_WARN("failed to assign candi addrs", K(ret));
      }
    } else if (OB_FAIL(ObOptimizerUtil::intersect(candi_addrs, child_candi_addrs, new_candi_addrs))) {
      LOG_WARN("failed to compute intersect", K(ret));
    }
    if (OB_SUCC(ret) && new_candi_addrs.empty()) {
      // there is no server in common
      if (!child_candi_addrs.empty()) {
        // choose replica for the current child
        if (OB_FAIL(child->set_duplicate_table_replica(child_candi_addrs))) {
          LOG_WARN("failed to set duplicate table replica", K(ret));
        }
      }
      if (OB_SUCC(ret) && !candi_addrs.empty()) {
        // choose replicas for all children in the left
        for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
          if (OB_FAIL(get_child(j)->set_duplicate_table_replica(candi_addrs))) {
            LOG_WARN("failed to set duplicate table replica", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(candi_addrs.assign(new_candi_addrs))) {
      LOG_WARN("failed to assign new candi addrs", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::set_duplicate_table_replica(const ObIArray<ObAddr>& addrs)
{
  int ret = OB_SUCCESS;
  ObAddr chosen;
  if (OB_ISNULL(get_plan()) || OB_UNLIKELY(addrs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(get_plan()), K(addrs.empty()));
  } else if (ObOptimizerUtil::find_item(addrs, get_plan()->get_optimizer_context().get_local_server_addr())) {
    chosen = get_plan()->get_optimizer_context().get_local_server_addr();
  } else {
    chosen = addrs.at(0);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(SMART_CALL(set_duplicate_table_replica(chosen)))) {
      LOG_WARN("failed to set duplicate table replica", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::set_duplicate_table_replica(const ObAddr& addr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret));
    } else if (OB_FAIL(get_child(i)->set_duplicate_table_replica(addr))) {
      LOG_WARN("failed to set duplicate table replica", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::check_is_uncertain_plan()
{
  int ret = OB_SUCCESS;
  bool is_uncertain = false;
  ObLogPlan* child_plan = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_uncertain && i < get_num_of_child(); ++i) {
    if (OB_ISNULL(get_child(i)) || OB_ISNULL(child_plan = get_child(i)->get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(get_child(i)), K(child_plan));
    } else if (child_plan->get_location_type() == ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN) {
      is_uncertain = true;
    }
  }
  if (OB_SUCC(ret) && is_uncertain) {
    get_plan()->set_location_type(ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN);
  }
  return ret;
}

int ObLogicalOperator::check_fulfill_cut_ratio_condition(int64_t dop, double ndv, bool& is_fulfill)
{
  int ret = OB_SUCCESS;
  uint64_t groupby_nopushdown_cut_ratio = 1;
  ObLogicalOperator* child_op = NULL;
  ObSQLSessionInfo* session_info = NULL;
  is_fulfill = false;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(child_op = get_child(first_child)) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child_op), K(get_plan()), K(session_info), K(ret));
  } else if (OB_FAIL(session_info->get_sys_variable(
                 share::SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO, groupby_nopushdown_cut_ratio))) {
    LOG_WARN("failed to get session variable", K(ret));
  } else {
    double dop_ndv = ndv;
    double dop_card = child_op->get_card();
    if (dop > 1) {
      dop_card = dop_card / dop;
      dop_ndv = ObOptEstSel::scale_distinct(dop_card, child_op->get_card(), ndv);
    }
    is_fulfill = (dop_card / dop_ndv) >= static_cast<double>(groupby_nopushdown_cut_ratio);
  }
  return ret;
}

int ObLogicalOperator::alloc_partition_id_expr(
    uint64_t table_id, ObAllocExprContext& ctx, ObPseudoColumnRawExpr*& partition_id_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", KP(stmt_), K(table_id), K(ret));
  } else if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_plan()->get_optimizer_context().get_expr_factory().create_raw_expr(
                 T_PDML_PARTITION_ID, partition_id_expr))) {
    LOG_WARN("failed to create pdml partition id pseudo column expr", K(ret));
  } else if (OB_ISNULL(partition_id_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pseudo column expr is null", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 1> part_id_expr;
    partition_id_expr->set_data_type(ObIntType);
    partition_id_expr->set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
    partition_id_expr->set_table_id(table_id);
    partition_id_expr->add_relation_id(get_plan()->get_stmt()->get_table_bit_index(table_id));
    partition_id_expr->set_expr_level(get_plan()->get_stmt()->get_current_level());
    if (OB_FAIL(part_id_expr.push_back(partition_id_expr))) {
      LOG_WARN("failed to push back partition id expr", K(ret));
    } else if (OB_FAIL(add_exprs_to_ctx(ctx, part_id_expr))) {
      LOG_WARN("failed to add exprs to ctx", K(ret));
    }
    LOG_DEBUG("alloc partition id expr for the consumer",
        K(get_name()),
        K(partition_id_expr->get_table_id()),
        K(partition_id_expr),
        KPC(partition_id_expr));
  }
  return ret;
}

int ObLogicalOperator::check_subplan_filter_child_exchange_rescanable()
{
  int ret = OB_SUCCESS;
  if (LOG_SUBPLAN_FILTER != type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operator type", K(ret), K(type_));
  } else {
    bool has_onetime_expr = false;
    ObLogSubPlanFilter* sub_plan_filter = static_cast<ObLogSubPlanFilter*>(this);
    const common::ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs = sub_plan_filter->get_onetime_exprs();
    for (int64_t i = (int64_t)get_num_of_child() - 1; OB_SUCC(ret) && i >= first_child; --i) {
      bool is_onetime_expr = false;
      if (i != first_child) {
        for (int64_t j = 0; OB_SUCC(ret) && !is_onetime_expr && j < onetime_exprs.count(); ++j) {
          if (OB_FAIL(ObOptEstCost::is_subquery_one_time(onetime_exprs.at(j).second, i, is_onetime_expr))) {
            LOG_WARN("failed to check if subquery is one time expr", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (is_onetime_expr) {
            has_onetime_expr = true;
          } else if (OB_FAIL(get_child(i)->mark_child_exchange_rescanable())) {
            LOG_WARN("mark child ex-receive as px op fail", K(ret));
          }
        }
      } else if (has_onetime_expr && OB_FAIL(get_child(i)->mark_child_exchange_rescanable())) {
        LOG_WARN("mark child ex-receive as px op fail", K(ret));
      }
    }
  }
  return ret;
}
