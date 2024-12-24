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
#include "sql/engine/ob_operator_factory.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "lib/hash_func/murmur_hash.h"
#include "sql/resolver/expr/ob_raw_expr_replacer.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/code_generator/ob_static_engine_cg.h"
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
#include "ob_log_update.h"
#include "ob_log_merge.h"
#include "ob_opt_est_cost.h"
#include "ob_optimizer_util.h"
#include "ob_raw_expr_add_to_context.h"
#include "ob_raw_expr_check_dep.h"
#include "ob_log_count.h"
#include "ob_log_monitoring_dump.h"
#include "ob_log_subplan_filter.h"
#include "ob_log_topk.h"
#include "ob_log_material.h"
#include "ob_log_join_filter.h"
#include "ob_log_temp_table_access.h"
#include "ob_log_temp_table_insert.h"
#include "ob_log_function_table.h"
#include "ob_log_json_table.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "ob_log_err_log.h"
#include "ob_log_temp_table_transformation.h"
#include "ob_log_expr_values.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_query_range.h"
#include "sql/optimizer/ob_opt_est_parameter_normal.h"


using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::sql::log_op_def;
using oceanbase::share::schema::ObSchemaGetterGuard;
using oceanbase::share::schema::ObTableSchema;
using oceanbase::share::schema::ObColumnSchemaV2;

int ObExchangeInfo::init_calc_part_id_expr(ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObRawExprFactory &expr_factory = opt_ctx.get_expr_factory();
  int64_t part_expr_cnt = repartition_func_exprs_.count();
  share::schema::ObPartitionLevel part_level = share::schema::PARTITION_LEVEL_ONE;
  ObRawExpr *part_expr = NULL;
  ObRawExpr *subpart_expr = NULL;
  if (part_expr_cnt <= 0 || part_expr_cnt > 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part expr count", K(ret));
  } else {
    if (OB_ISNULL(repartition_func_exprs_.at(0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part func expr is null", K(ret));
    } else if (T_OP_ROW == repartition_func_exprs_.at(0)->get_expr_type()) {
      ObOpRawExpr *op_row_expr = NULL;
      if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, op_row_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < repartition_keys_.count(); i ++) {
          if (OB_FAIL(op_row_expr->add_param_expr(repartition_keys_.at(i)))) {
            LOG_WARN("fail to add param expr", K(ret));
          }
        } // for end
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
      ObOpRawExpr *op_row_expr = NULL;
      if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, op_row_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < repartition_sub_keys_.count(); i ++) {
          if (OB_FAIL(op_row_expr->add_param_expr(repartition_sub_keys_.at(i)))) {
            LOG_WARN("fail to add param expr", K(ret));
          }
        } // for end
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
  } else if (OB_FAIL(ObRawExprUtils::build_calc_tablet_id_expr(expr_factory,
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
  } else {
    if (MayAddIntervalPart::YES == may_add_interval_part_) {
      calc_part_id_expr_->set_may_add_interval_part(may_add_interval_part_);
    }
    if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST == repartition_type_) {
      calc_part_id_expr_->set_partition_id_calc_type(CALC_IGNORE_SUB_PART);
    } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB == repartition_type_) {
      calc_part_id_expr_->set_partition_id_calc_type(CALC_IGNORE_FIRST_PART);
    }
  }
  return ret;
}

int ObExchangeInfo::append_hash_dist_expr(const common::ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObRawExpr *raw_expr = NULL;
    if (OB_ISNULL(raw_expr = exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(hash_dist_exprs_.push_back(HashExpr(raw_expr, raw_expr->get_result_type())))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObExchangeInfo::assign(ObExchangeInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_keys_.assign(other.sort_keys_))) {
    LOG_WARN("failed to assign sort keys", K(ret));
  } else if (OB_FAIL(repartition_keys_.assign(other.repartition_keys_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(repartition_sub_keys_.assign(other.repartition_sub_keys_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(repartition_func_exprs_.assign(other.repartition_func_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(hash_dist_exprs_.assign(other.hash_dist_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(popular_values_.assign(other.popular_values_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(weak_sharding_.assign(other.weak_sharding_))) {
    LOG_WARN("failed to assign weak sharding", K(ret));
  } else if (OB_FAIL(repart_all_tablet_ids_.assign(other.repart_all_tablet_ids_))) {
    LOG_WARN("failed to assign partition ids", K(ret));
  } else if (OB_FAIL(server_list_.assign(other.server_list_))) {
    LOG_WARN("failed to assign server list", K(ret));
  } else {
    is_remote_ = other.is_remote_;
    is_task_order_ = other.is_task_order_;
    is_merge_sort_ = other.is_merge_sort_;
    is_sort_local_order_ = other.is_sort_local_order_;
    slice_count_ = other.slice_count_;
    repartition_type_ = other.repartition_type_;
    repartition_ref_table_id_ = other.repartition_ref_table_id_;
    repartition_table_id_ = other.repartition_table_id_;
    repartition_table_name_ = other.repartition_table_name_;
    calc_part_id_expr_ = other.calc_part_id_expr_;
    dist_method_ = other.dist_method_;
    unmatch_row_dist_method_ = other.unmatch_row_dist_method_;
    null_row_dist_method_ = other.null_row_dist_method_;
    slave_mapping_type_ = other.slave_mapping_type_;
    strong_sharding_ = other.strong_sharding_;
    parallel_ = other.parallel_;
    server_cnt_ = other.server_cnt_;
  }
  return ret;
}

int AllocOpContext::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(visited_map_.create(2, "HashAllocOp"))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (OB_FAIL(disabled_op_set_.create(10))) {
    LOG_WARN("failed to create hash set", K(ret));
  }
  return ret;
}
AllocOpContext::~AllocOpContext()
{
  visited_map_.destroy();
  disabled_op_set_.destroy();
}

/**
 * To avoid allocating duplicate node above current op
*/
int AllocOpContext::visit(uint64_t op_id, uint8_t flag)
{
  int ret = OB_SUCCESS;
    ObIArray<uint64_t>* op_ids = visited_map_.get(flag);
  if (OB_ISNULL(op_ids)) {
    ObSEArray<uint64_t, 8> new_op_ids;
    if (OB_FAIL(visited_map_.set_refactored(flag, new_op_ids))) {
      LOG_WARN("fail to set_refactored", K(ret));
    } else {
      op_ids = visited_map_.get(flag);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(op_ids)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op ids is null", K(ret));
    } else if (!has_exist_in_array(*op_ids, op_id)) {
      if (OB_FAIL(op_ids->push_back(op_id))) {
        LOG_WARN("failed to push back op ids", K(ret));
      }
    }
  }
  return ret;
}

/**
 * Return true if a op_type node is already allocated above current op
*/
bool AllocOpContext::is_visited(uint64_t op_id, uint8_t flag)
{
  bool ret = false;
  ObIArray<uint64_t>* op_ids = visited_map_.get(flag);
  if (NULL != op_ids && has_exist_in_array(*op_ids, op_id)) {
    ret = true;
  }
  return ret;
}

ObPxPipeBlockingCtx::ObPxPipeBlockingCtx(ObIAllocator &alloc) : alloc_(alloc)
{
}

ObPxPipeBlockingCtx::~ObPxPipeBlockingCtx()
{
  FOREACH(it, op_ctxs_) {
    if (NULL != *it) {
      alloc_.free(*it);
      *it = NULL;
    }
  }
}

ObPxPipeBlockingCtx::OpCtx *ObPxPipeBlockingCtx::alloc()
{
  OpCtx *ctx = NULL;
  void *mem = alloc_.alloc(sizeof(OpCtx));
  if (OB_ISNULL(mem)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "allocate memory failed");
  } else {
    ctx = new(mem)OpCtx();
  }
  return ctx;
}

int ObAllocExprContext::find(const ObRawExpr *expr, ExprProducer *&producer)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  producer = NULL;
  if (expr_producers_.empty()) {
    // do nothing
  } else if (OB_UNLIKELY(expr_producers_.count() != expr_map_.size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash map has invalid size", K(ret));
  } else if (OB_FAIL(expr_map_.get_refactored(reinterpret_cast<uint64_t>(expr),
                                              idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get expr entry from the map", K(ret));
    }
  } else if (idx < 0 || idx >= expr_producers_.count() ||
             OB_UNLIKELY(expr != expr_producers_.at(idx).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index is invalid", K(ret), K(idx));
  } else {
    producer = &expr_producers_.at(idx);
  }
  return ret;
}

int ObAllocExprContext::add(const ExprProducer &producer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_producers_.push_back(producer))) {
    LOG_WARN("failed to push back producer", K(ret));
  } else if (expr_producers_.count() == 1 &&
             OB_FAIL(expr_map_.create(128, "ExprAlloc"))) {
    LOG_WARN("failed to init hash map", K(ret));
  } else if (OB_FAIL(expr_map_.set_refactored(reinterpret_cast<uint64_t>(producer.expr_),
                                              expr_producers_.count() - 1))) {
    LOG_WARN("failed to add entry into hash map", K(ret));
  } else if (OB_FAIL(add_flattern_expr(producer.expr_))) {
    LOG_WARN("failed to add flattern expr", K(ret));
  }
  return ret;
}

int ObAllocExprContext::add_flattern_expr(const ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  int64_t ref_cnt = 0;
  if (OB_FAIL(flattern_expr_map_.get_refactored(reinterpret_cast<uint64_t>(expr),
                                                        ref_cnt)))
  {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(flattern_expr_map_.set_refactored(reinterpret_cast<uint64_t>(expr),
                                                    1))) {
        LOG_WARN("failed to add entry into hash map", K(ret));
      }
    } else {
      LOG_WARN("failed to get expr entry from the map", K(ret));
    }
  } else if (OB_UNLIKELY(ref_cnt < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref_cnt is invalid", K(ret), K(ref_cnt));
  } else if (OB_FAIL(flattern_expr_map_.set_refactored(reinterpret_cast<uint64_t>(expr),
                                                       ref_cnt + 1, 1))) {
    LOG_WARN("failed to add entry into hash map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    ret = SMART_CALL(add_flattern_expr(expr->get_param_expr(i)));
  }
  return ret;
}

int ObAllocExprContext::get_expr_ref_cnt(const ObRawExpr* expr, int64_t &ref_cnt)
{
  int ret = OB_SUCCESS;
  ref_cnt = 0;
  if (OB_FAIL(flattern_expr_map_.get_refactored(reinterpret_cast<uint64_t>(expr),
                                                ref_cnt)))
  {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get expr entry from the map", K(ret));
    }
  }
  return ret;
}

ObAllocExprContext::~ObAllocExprContext()
{
  expr_map_.destroy();
  flattern_expr_map_.destroy();
}

ObLogicalOperator::ObLogicalOperator(ObLogPlan &plan)
  : child_(),
    type_(LOG_OP_INVALID),
    my_plan_(&plan),
    startup_exprs_(),
    output_const_exprs_(),
    output_equal_sets_(NULL),
    fd_item_set_(NULL),
    table_set_(NULL),
    id_(OB_INVALID_ID),
    branch_id_(OB_INVALID_ID),
    op_id_(OB_INVALID_ID),
    parent_(NULL),
    is_plan_root_(false),
    cost_(0.0),
    op_cost_(0.0),
    card_(0.0),
    width_(0.0),
    traverse_ctx_(NULL),
    exchange_allocated_(false),
    phy_plan_type_(ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED),
    location_type_(ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED),
    is_partition_wise_(false),
    px_est_size_factor_(),
    dblink_id_(OB_INVALID_ID),   // OB_INVALID_ID represent local cluster.
    plan_depth_(0),
    contain_fake_cte_(false),
    contain_pw_merge_op_(false),
    contain_das_op_(false),
    contain_match_all_fake_cte_(false),
    strong_sharding_(NULL),
    weak_sharding_(),
    is_pipelined_plan_(false),
    is_nl_style_pipelined_plan_(false),
    is_at_most_one_row_(false),
    is_local_order_(false),
    is_range_order_(false),
    op_ordering_(),
    empty_expr_sets_(plan.get_empty_expr_sets()),
    empty_fd_item_set_(plan.get_empty_fd_item_set()),
    empty_table_set_(plan.get_empty_table_set()),
    interesting_order_info_(OrderingFlag::NOT_MATCH),
    parallel_(ObGlobalHint::UNSET_PARALLEL),
    op_parallel_rule_(OpParallelRule::OP_DOP_RULE_MAX),
    available_parallel_(ObGlobalHint::DEFAULT_PARALLEL),
    server_cnt_(1),
    need_late_materialization_(false),
    op_exprs_(),
    inherit_sharding_index_(-1),
    need_osg_merge_(false),
    max_px_thread_branch_(OB_INVALID_INDEX),
    max_px_group_branch_(OB_INVALID_INDEX),
    need_re_est_child_cost_(false)

{
}

ObLogicalOperator::~ObLogicalOperator()
{}

const ObDMLStmt* ObLogicalOperator::get_stmt() const
{
  const ObDMLStmt *ret = (get_plan() != NULL ? get_plan()->get_stmt() : NULL);
  return ret;
}

void ObLogicalOperator::set_child(int64_t child_num,
                                  ObLogicalOperator *child_op)
{
  int ret = OB_SUCCESS;
  for (; OB_SUCC(ret) && child_.count() <= child_num;) {
    if (OB_FAIL(child_.push_back(NULL))) {
      LOG_WARN("failed to enlarge child array", K(ret));
    }
  }
  if (OB_SUCC(ret) && child_.count() > child_num) {
    child_.at(child_num) = child_op;
  }
  if (NULL != child_op) {
    child_op->set_parent(this);
  }
}

int ObLogicalOperator::get_parent(ObLogicalOperator *root, ObLogicalOperator *&parent)
{
  int ret = OB_SUCCESS;
  parent = NULL;
  if (NULL != root) {
    for (int64_t i = 0;
        OB_SUCC(ret) && parent == NULL && i < root->get_num_of_child(); i++) {
      if (OB_ISNULL(root->get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (root->get_child(i) == this) {
        parent = root;
      } else if (OB_FAIL(get_parent(root->get_child(i), parent))) {
        LOG_WARN("failed to get parent", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

double FilterCompare::get_selectivity(ObRawExpr *expr)
{
  bool found = false;
  double selectivity = 1;
  if (OB_NOT_NULL(expr) && T_FUN_LABEL_SE_LABEL_VALUE_CMP_LE == expr->get_expr_type()) {
    // security filter should be calc firstly
    found = true;
    selectivity = -1.0;
  }
  for (int64_t i = 0; !found && i < predicate_selectivities_.count(); i++) {
    if (predicate_selectivities_.at(i).expr_ == expr) {
      found = true;
      selectivity = predicate_selectivities_.at(i).sel_;
    }
  }
  return selectivity;
}

int ObLogicalOperator::get_card_without_filter(double &card)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child_op = NULL;
  if (OB_NOT_NULL(child_op = get_child(ObLogicalOperator::first_child))) {
    card = child_op->get_card();
  } else {
    card = 1.0;
  }
  return ret;
}

// Add a child to the end of the array
int ObLogicalOperator::add_child(ObLogicalOperator *child_op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_.push_back(child_op))) {
    LOG_WARN("failed to push back child op", K(ret));
  }
  return ret;
}

int ObLogicalOperator::add_child(const ObIArray<ObLogicalOperator*> &child_ops)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_ops.count(); i++) {
    if (OB_ISNULL(child_ops.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(child_.push_back(child_ops.at(i)))) {
      LOG_WARN("failed to push back child ops", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogicalOperator::set_op_ordering(const common::ObIArray<OrderItem> &op_ordering)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(op_ordering_.assign(op_ordering))) {
    LOG_WARN("failed to assign ordering", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogicalOperator::get_input_equal_sets(EqualSets &input_esets) const
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(i));
    } else if (OB_FAIL(append(input_esets,
                              child->get_output_equal_sets()))) {
      LOG_WARN("failed to append ordering equal sets", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::get_input_const_exprs(ObIArray<ObRawExpr *> &const_exprs) const
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
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
  ObLogicalOperator *child = NULL;
  EqualSets *ordering_esets = NULL;
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
    set_output_equal_sets(&child->get_output_equal_sets());
  } else if (OB_ISNULL(ordering_esets = get_plan()->create_equal_sets())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create equal sets", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                       &my_plan_->get_allocator(),
                       filter_exprs_,
                       child->get_output_equal_sets(),
                       *ordering_esets))) {
    LOG_WARN("failed to compute ordering output equal set", K(ret));
  } else {
    set_output_equal_sets(ordering_esets);
  }
  return ret;
}

int ObLogicalOperator::compute_const_exprs()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(my_plan_) || OB_UNLIKELY(get_num_of_child() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is invalid", K(ret), K(get_num_of_child()), K(my_plan_));
  } else if (OB_UNLIKELY(get_num_of_child() == 0)) {
    /*do nothing*/
  } else if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else if (OB_FAIL(append(output_const_exprs_, child->output_const_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (filter_exprs_.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(filter_exprs_, output_const_exprs_))) {
    LOG_WARN("failed to compute const conditionexprs", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObLogicalOperator::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
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

int ObLogicalOperator::deduce_const_exprs_and_ft_item_set(ObFdItemSet &fd_item_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> column_exprs;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(my_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(get_stmt()->get_column_exprs(column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().deduce_fd_item_set(
                                                          get_output_equal_sets(),
                                                          column_exprs,
                                                          get_output_const_exprs(),
                                                          fd_item_set))) {
    LOG_WARN("falied to remove const in fd item", K(ret));
  }
  return ret;
}

int ObLogicalOperator::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_UNLIKELY(get_num_of_child() == 0)) {
  } else if (OB_ISNULL(child= get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
    LOG_WARN("failed to set op ordering", K(ret));
  } else {
    is_local_order_ = child->get_is_local_order();
    is_range_order_ = child->get_is_range_order();
  }
  return ret;
}

int ObLogicalOperator::compute_op_interesting_order_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (get_num_of_child() == 0 || op_ordering_.empty()) {
    set_interesting_order_info(OrderingFlag::NOT_MATCH);
  } else if (OB_ISNULL(child= get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator does not have child", K(ret));
  } else {
    set_interesting_order_info(child->get_interesting_order_info());
  }
  return ret;
}

int ObLogicalOperator::compute_op_parallel_and_server_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (get_num_of_child() == 0) {
    //do nothing
  } else if (OB_UNLIKELY(1 < get_num_of_child())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi child operator must override this function", K(ret), K(get_num_of_child()));
  } else if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null child", K(ret), K(child));
  } else if (OB_FAIL(get_server_list().assign(child->get_server_list()))) {
    LOG_WARN("failed to assign server list", K(ret));
  } else {
    set_parallel(child->get_parallel());
    set_server_cnt(child->get_server_cnt());
    if (is_single()) {
      set_available_parallel(child->get_available_parallel());
    }
  }
  return ret;
}


int ObLogicalOperator::compute_normal_multi_child_parallel_and_server_info()
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator *max_parallel_child = NULL;
  bool max_parallel_from_exch = false;
  ObPQDistributeMethod::Type child_distribute_method_type = ObPQDistributeMethod::NONE;
  int64_t max_available_parallel = ObGlobalHint::DEFAULT_PARALLEL;
  const ObLogicalOperator *child = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set operator i-th child is null", K(ret), K(i));
    } else if (0 == i) {
      max_parallel_child = child;
      max_available_parallel = max_parallel_child->get_available_parallel();
      max_parallel_from_exch = LOG_EXCHANGE == max_parallel_child->get_type();
    } else if (!max_parallel_from_exch && LOG_EXCHANGE == child->get_type()) {
      // do nothing
    } else {
      if (max_parallel_child->get_parallel() < child->get_parallel() ||
          (max_parallel_from_exch && LOG_EXCHANGE != child->get_type() && !child->is_match_all())) {
        max_available_parallel = child->get_available_parallel();
        max_parallel_child = child;
        max_parallel_from_exch = LOG_EXCHANGE == max_parallel_child->get_type();
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(max_parallel_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ", K(ret), K(max_parallel_child));
  } else if (OB_FAIL(get_server_list().assign(max_parallel_child->get_server_list()))) {
    LOG_WARN("failed to assign server list", K(ret));
  } else {
    set_parallel(max_parallel_child->get_parallel());
    set_server_cnt(max_parallel_child->get_server_cnt());
    if (is_single()) {
      set_available_parallel(max_available_parallel);
    }
  }
  return ret;
}

int ObLogicalOperator::set_parallel_and_server_info_for_match_all()
{
  int ret = OB_SUCCESS;
  get_server_list().reuse();
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()));
  } else if (OB_FAIL(get_server_list().push_back(get_plan()->get_optimizer_context().get_local_server_addr()))) {
    LOG_WARN("failed to push back server list", K(ret));
  } else {
    set_parallel(ObGlobalHint::DEFAULT_PARALLEL);
    set_op_parallel_rule(OpParallelRule::OP_DAS_DOP);
    set_server_cnt(1);
  }
  return ret;
}

int ObLogicalOperator::est_width()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (get_num_of_child() == 0) {
    /*do nothing*/
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    width_ = child->get_width();
  }
  return ret;
}

int ObLogicalOperator::compute_table_set()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (LOG_SUBPLAN_FILTER == get_type() || get_num_of_child() == 1) {
    // subplan filter 只能看到左表，外层不应该看到右表
    if (OB_ISNULL(child = get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      set_table_set(&child->get_table_set());
    }
  } else {
    ObRelIds *table_set = NULL;
    if (OB_ISNULL(get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(table_set = (ObRelIds*) get_plan()->get_allocator().alloc(sizeof(ObRelIds)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      table_set = new(table_set) ObRelIds();
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
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogicalOperator::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (get_num_of_child() == 0) {
    /*do nothing*/
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(weak_sharding_.assign(child->get_weak_sharding()))) {
    LOG_WARN("failed to assign sharding info", K(ret));
  } else {
    strong_sharding_ = child->get_strong_sharding();
    inherit_sharding_index_ = ObLogicalOperator::first_child;
  }
  return ret;
}

int ObLogicalOperator::compute_plan_type()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (is_local()) {
    phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_LOCAL;
  } else if (is_remote()) {
    phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_REMOTE;
  } else if (is_distributed()) {
    phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED;
  } else {
    phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED;
  }
  bool child_has_exchange = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(child), K(ret));
    } else {
      if (child->is_exchange_allocated()) {
        child_has_exchange = true;
      }
      if (ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN == child->get_location_type()) {
        location_type_ = ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN;
      }
    }
  }
  if (OB_SUCC(ret) && child_has_exchange) {
    exchange_allocated_ = true;
    phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED;
  }
  return ret;
}

int ObLogicalOperator::compute_op_other_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    // compute contains fake cte
    if (OB_SUCC(ret)) {
      if (get_type() == log_op_def::ObLogOpType::LOG_SET &&
          static_cast<ObLogSet*>(this)->is_recursive_union()) {
        /*do nothing*/
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && !contain_fake_cte_ && i < get_num_of_child(); i++) {
          if (OB_ISNULL(get_child(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else {
            contain_fake_cte_ |= get_child(i)->get_contains_fake_cte();
          }
        }
      }
    }

    // compute contains fake cte match all sharding
    if (OB_SUCC(ret)) {
      if (get_type() == log_op_def::ObLogOpType::LOG_SET &&
          static_cast<ObLogSet*>(this)->is_recursive_union()) {
        /*do nothing*/
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && !contain_match_all_fake_cte_ && i < get_num_of_child(); i++) {
          if (OB_ISNULL(get_child(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else {
            contain_match_all_fake_cte_ |= get_child(i)->get_contains_match_all_fake_cte();
          }
        }
      }
    }

    // compute contains merge style op
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && !contain_pw_merge_op_ && i < get_num_of_child(); i++) {
        if (OB_ISNULL(get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          contain_pw_merge_op_ |= get_child(i)->get_contains_pw_merge_op() &&
                                  log_op_def::LOG_EXCHANGE != get_child(i)->get_type();
        }
      }
      if (OB_SUCC(ret) && !contain_pw_merge_op_) {
        if (log_op_def::LOG_GROUP_BY == get_type()) {
          ObLogGroupBy *group_by = static_cast<ObLogGroupBy*>(this);
          contain_pw_merge_op_ = !group_by->get_group_by_exprs().empty() &&
                               (AggregateAlgo::MERGE_AGGREGATE == group_by->get_algo()) &&
                               is_partition_wise();
        } else if (log_op_def::LOG_DISTINCT == get_type()) {
          ObLogDistinct *distinct = static_cast<ObLogDistinct*>(this);
          contain_pw_merge_op_ = AggregateAlgo::MERGE_AGGREGATE == distinct->get_algo() &&
                               is_partition_wise();
        } else if (log_op_def::LOG_SET == get_type()) {
          ObLogSet *set = static_cast<ObLogSet*>(this);
          contain_pw_merge_op_ = set->is_set_distinct() && SetAlgo::MERGE_SET == set->get_algo() &&
                               is_partition_wise();
        } else if (log_op_def::LOG_WINDOW_FUNCTION == get_type()) {
          contain_pw_merge_op_ = is_block_op() &&
                               is_partition_wise();
        } else { /*do nothing*/ }
      }
    }

    // compute contains das op
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && !contain_das_op_ && i < get_num_of_child(); i++) {
        if (OB_ISNULL(get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          contain_das_op_ |= get_child(i)->get_contains_das_op();
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !need_osg_merge_ && i < get_num_of_child(); i++) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        need_osg_merge_ |= get_child(i)->need_osg_merge();
      }
    }
  }
  return ret;
}

int ObLogicalOperator::compute_pipeline_info()
{
  int ret = OB_SUCCESS;
  is_nl_style_pipelined_plan_ = false;
  if (is_block_op()) {
    is_pipelined_plan_ = false;
  } else {
    is_pipelined_plan_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_pipelined_plan_ && i < get_num_of_child(); ++i) {
      ObLogicalOperator *child = get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null child", K(ret));
      } else {
        is_pipelined_plan_ &= child->is_pipelined_plan();
      }
    }
  }
  return ret;
}

int ObLogicalOperator::compute_property(Path *path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_ISNULL(path->parent_) || OB_ISNULL(path->parent_->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path is null", K(ret), K(path));
  } else if (OB_FAIL(append(get_output_const_exprs(), path->parent_->get_output_const_exprs()))) {
    LOG_WARN("failed to append const exprs", K(ret));
  } else if (OB_FAIL(set_op_ordering(path->get_ordering()))) {
    LOG_WARN("failed to set ordering", K(ret));
  } else if (OB_FAIL(set_weak_sharding(path->weak_sharding_))) {
    LOG_WARN("failed to set weak sharding", K(ret));
  } else {
    set_output_equal_sets(&path->parent_->get_output_equal_sets());
    set_fd_item_set(&path->parent_->get_fd_item_set());
    set_table_set(&path->parent_->get_tables());
    set_is_at_most_one_row(path->parent_->get_is_at_most_one_row());
    set_strong_sharding(path->strong_sharding_);
    set_exchange_allocated(path->exchange_allocated_);
    set_phy_plan_type(path->phy_plan_type_);
    set_location_type(path->location_type_);
    set_contains_fake_cte(path->contain_fake_cte_);
    set_contains_pw_merge_op(path->contain_pw_merge_op_);
    set_contains_match_all_fake_cte(path->contain_match_all_fake_cte_);
    set_contains_das_op(path->contain_das_op_);
    is_pipelined_plan_ = path->is_pipelined_path();
    is_nl_style_pipelined_plan_ = path->is_nl_style_pipelined_path();

    //set cost, card, width
    set_op_cost(path->op_cost_);
    set_cost(path->cost_);
    set_card(path->get_path_output_rows());
    set_width(path->parent_->get_output_row_size());
    set_interesting_order_info(path->get_interesting_order_info());
    set_is_local_order(path->is_local_order_);
    set_is_range_order(path->is_range_order_);
    set_parallel(path->parallel_);
    set_op_parallel_rule(path->op_parallel_rule_);
    set_available_parallel(path->available_parallel_),
    set_server_cnt(path->server_cnt_);
    if (OB_FAIL(server_list_.assign(path->server_list_))) {
      LOG_WARN("failed to assign path's server list to op", K(ret));
    } else if (OB_FAIL(ambient_card_.assign(path->parent_->get_ambient_card()))) {
      LOG_WARN("failed to assign ambient cards", K(ret));
    } else if (OB_FAIL(check_property_valid())) {
      LOG_WARN("failed to check property valid", K(ret), KPC(path));
    } else {
      LOG_TRACE("compute property finished",
                K(get_op_name(type_)),
                K(get_cost()),
                K(is_local_order_),
                K(is_range_order_),
                K(op_ordering_),
                K(is_at_most_one_row_),
                K(output_const_exprs_),
                K(output_equal_sets_),
                K(strong_sharding_),
                K(weak_sharding_),
                K(fd_item_set_),
                K(phy_plan_type_),
                K(location_type_));
    }
  }
  return ret;
}

int ObLogicalOperator::check_need_parallel_valid(int64_t need_parallel) const {
  int ret = OB_SUCCESS;
  if (get_parallel() == need_parallel) {
    /* do nothing */
  } else if (ObGlobalHint::DEFAULT_PARALLEL < need_parallel && (is_local() || is_remote())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(ret), K(get_parallel()), K(need_parallel),
                                              K(is_single()));
  }
  return ret;
}

int ObLogicalOperator::re_est_cost(EstimateCostInfo &param, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  const int64_t parallel = (ObGlobalHint::UNSET_PARALLEL == param.need_parallel_ || is_match_all())
                           ? get_parallel() : param.need_parallel_;
  param.need_parallel_ = parallel;
  param.need_row_count_ = (get_card() <= param.need_row_count_ || 0 > param.need_row_count_)
                          ? -1 : param.need_row_count_;
  double op_cost = 0.0;
  bool contain_false_filter = false;
  card = 0.0;
  cost = 0.0;
  if (!param.need_re_est(get_parallel(), get_card())) {  // no need to re est cost
    card = get_card();
    cost = get_cost();
  } else if (OB_FAIL(check_need_parallel_valid(parallel))) {
    LOG_WARN("failed to check need parallel valid", K(ret));
  } else if (OB_FAIL(SMART_CALL(do_re_est_cost(param, card, op_cost, cost)))) {
    LOG_WARN("failed to do re est operator", K(ret));
  } else if (OB_FAIL(check_contain_false_startup_filter(contain_false_filter))) {
    LOG_WARN("failed to check startup filter", K(ret));
  } else if (contain_false_filter && FALSE_IT(card = 0.0)) {
    // never reach
  } else if (!param.override_) {
    /* do nothing */
  } else if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    set_op_cost(op_cost);
    set_cost(cost);
    set_card(card);
    get_plan()->get_optimizer_context().set_max_parallel(parallel);
    if (get_parallel() != parallel) {
      set_parallel(parallel);
      set_op_parallel_rule(OpParallelRule::OP_INHERIT_DOP);
    }
  }
  return ret;
}

int ObLogicalOperator::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  //default by pass operator
  if (1 == get_num_of_child()) {
    ObLogicalOperator *child = NULL;
    const int64_t parallel = param.need_parallel_;
    if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) ||
        OB_ISNULL(get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(child), K(ret));
    } else if (OB_UNLIKELY(parallel < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected parallel degree", K(parallel), K(ret));
    } else {
      double selectivity = 1.0;
      double child_card = child->get_card();
      double child_cost = child->get_cost();
      ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
      op_cost = ObOptEstCost::cost_get_rows(child_card / parallel, opt_ctx);
      if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
        LOG_WARN("failed to re est cost", K(ret));
      } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                                get_plan()->get_selectivity_ctx(),
                                                                get_filter_exprs(),
                                                                selectivity,
                                                                get_plan()->get_predicate_selectivities()))) {
        LOG_WARN("failed to calculate selectivity", K(ret));
      } else {
        cost = child_cost + op_cost;
        card = child_card * selectivity;
      }
    }
  } else if (0 == get_num_of_child()) {
    card = get_card();
    cost = get_cost();
    op_cost = get_op_cost();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi child op called default do_re_est_cost function", K(ret), K(get_type()));
  }
  return ret;
}

int ObLogicalOperator::get_limit_offset_value(ObRawExpr *percent_expr,
                                              ObRawExpr *limit_expr,
                                              ObRawExpr *offset_expr,
                                              double &limit_percent,
                                              int64_t &limit_count,
                                              int64_t &offset_count)
{
  int ret = OB_SUCCESS;
  limit_count = -1;
  offset_count = 0;
  limit_percent = -1.0;
  ObOptimizerContext *opt_ctx = NULL;
  bool is_null_value = false;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(opt_ctx));
  } else if ((NULL != percent_expr && percent_expr->has_flag(CNT_DYNAMIC_PARAM))
             || (NULL != limit_expr && limit_expr->has_flag(CNT_DYNAMIC_PARAM))
             || (NULL != offset_expr && offset_expr->has_flag(CNT_DYNAMIC_PARAM))) {
    //limit/offset/percent ? do nothing
  } else if (NULL != percent_expr &&
             OB_FAIL(ObTransformUtils::get_percentage_value(percent_expr,
                                                            get_stmt(),
                                                            opt_ctx->get_params(),
                                                            opt_ctx->get_exec_ctx(),
                                                            &opt_ctx->get_allocator(),
                                                            limit_percent,
                                                            is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else if (!is_null_value && limit_expr != NULL &&
             OB_FAIL(ObTransformUtils::get_limit_value(limit_expr,
                                                       opt_ctx->get_params(),
                                                       opt_ctx->get_exec_ctx(),
                                                       &opt_ctx->get_allocator(),
                                                       limit_count,
                                                       is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else if (!is_null_value && offset_expr != NULL &&
             OB_FAIL(ObTransformUtils::get_limit_value(offset_expr,
                                                       opt_ctx->get_params(),
                                                       opt_ctx->get_exec_ctx(),
                                                       &opt_ctx->get_allocator(),
                                                       offset_count,
                                                       is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else if (is_null_value) {
    limit_count = 0;
    offset_count = 0;
    limit_percent = -1;
  }

  LOG_DEBUG("get limit offset value", K(limit_count), K(offset_count), K(limit_percent));
  return ret;
}

int ObLogicalOperator::compute_property()
{
  int ret = OB_SUCCESS;
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
  } else if (OB_FAIL(compute_pipeline_info())) {
    LOG_WARN("failed to compute pipeline info", K(ret));
  } else if (OB_FAIL(compute_sharding_info())) {
    LOG_WARN("failed to compute sharding info", K(ret));
  } else if (OB_FAIL(compute_plan_type())) {
    LOG_WARN("failed to compute plan type", K(ret));
  } else if (OB_FAIL(compute_op_other_info())) {
    LOG_WARN("failed to check and replace aggr exprs", K(ret));
  } else if (OB_FAIL(compute_op_ordering())) {
    LOG_WARN("failed to compute op ordering", K(ret));
  } else if (OB_FAIL(compute_op_interesting_order_info())) {
    LOG_WARN("failed to compute op ordering match info", K(ret));
  } else if (OB_FAIL(compute_op_parallel_and_server_info())) {
    LOG_WARN("failed to compute op server info", K(ret));
  } else if (OB_FAIL(est_width())) {
    LOG_WARN("failed to compute width", K(ret));
  } else if (OB_FAIL(est_cost())) {
    LOG_WARN("failed to estimate cost", K(ret));
  } else if (OB_FAIL(est_ambient_card())) {
    LOG_WARN("failed to est ambient card");
  } else if (OB_FAIL(check_property_valid())) {
    LOG_WARN("failed to check property valid", K(ret));
  } else {
    LOG_TRACE("compute property finished",
              K(get_op_name(type_)),
              K(get_cost()),
              K(get_card()),
              K(is_local_order_),
              K(is_range_order_),
              K(op_ordering_),
              K(is_at_most_one_row_),
              K(output_const_exprs_),
              K(output_equal_sets_),
              K(strong_sharding_),
              K(weak_sharding_),
              K(fd_item_set_),
              K(phy_plan_type_),
              K(location_type_),
              K(contain_fake_cte_),
              K(contain_pw_merge_op_),
              K(contain_das_op_),
              K(width_));
  }

  return ret;
}

int ObLogicalOperator::est_ambient_card()
{
  int ret = OB_SUCCESS;
  if (1 == get_num_of_child()) {
    if (OB_FAIL(inner_est_ambient_card_by_child(ObLogicalOperator::first_child))) {
      LOG_WARN("failed to est ambient cards by first child", K(ret), K(get_type()));
    }
  } else if (0 == get_num_of_child()) {
    // do nothing
    // ambient cardinality of the leaf node is inited by the path
  } else {
    // ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi child op called default est_ambient_card function", K(ret), K(get_type()));
  }
  return ret;
}

int ObLogicalOperator::inner_est_ambient_card_by_child(int64_t child_idx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_UNLIKELY(child_idx >= get_num_of_child()) ||
      OB_ISNULL(child = get_child(child_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(ambient_card_.assign(child->get_ambient_card()))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    for (int64_t i = 0; i < ambient_card_.count(); i ++) {
      ambient_card_.at(i) = ObOptSelectivity::scale_distinct(get_card(), child->get_card(), ambient_card_.at(i));
    }
  }
  return ret;
}

int ObLogicalOperator::check_property_valid() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObGlobalHint::DEFAULT_PARALLEL > get_parallel()
                  || get_server_cnt() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has invalid parallel or server info", K(ret), K(get_parallel()),
                                                    K(get_server_list()), K(get_server_cnt()));
  }
  return ret;
}

int ObLogicalOperator::get_plan_item_info(PlanText &plan_text,
                                          ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  plan_item.cardinality_ = static_cast<int64_t>(ceil(get_card()));
  plan_item.cost_ = static_cast<int64_t>(ceil(cost_));
  plan_item.bytes_ = static_cast<int64_t>(ceil(get_width()));
  plan_item.time_ = static_cast<int64_t>(ceil(cost_));
  plan_item.id_ = static_cast<int64_t>(op_id_);
  plan_item.parent_id_ = -1;
  if (get_parent()) {
    plan_item.parent_id_ = static_cast<int64_t>(get_parent()->op_id_);
  }
  const ObIArray<ObRawExpr*> &output = output_exprs_;
  const ObIArray<ObRawExpr*> &startup_filter = startup_exprs_;
  if (OB_SUCC(ret)) {
    BEGIN_BUF_PRINT;
    BUF_PRINTF("%s", get_name());
    END_BUF_PRINT(plan_item.operation_,
                  plan_item.operation_len_);
  }
  if (OB_SUCC(ret) && get_plan() && get_plan()->get_stmt()) {
    ObString qb_name;
    if (OB_FAIL(get_plan()->get_stmt()->get_qb_name(qb_name))) {
      LOG_WARN("failed to get qb name", K(ret));
    } else {
      BEGIN_BUF_PRINT;
      BUF_PRINTF("%.*s", qb_name.length(), qb_name.ptr());
      END_BUF_PRINT(plan_item.qblock_name_,
                    plan_item.qblock_name_len_);
    }
  }

  // print output
  if (OB_SUCC(ret) && !output.empty()) {
    BEGIN_BUF_PRINT;
    EXPLAIN_PRINT_EXPRS(output, type);
    END_BUF_PRINT(plan_item.projection_,
                  plan_item.projection_len_);
  }
  // print filter
  if (OB_SUCC(ret) && !filter_exprs_.empty() && LOG_UNPIVOT != get_type()) {
    const ObIArray<ObRawExpr *> &filter = filter_exprs_;
    BEGIN_BUF_PRINT;
    EXPLAIN_PRINT_EXPRS(filter, type);
    END_BUF_PRINT(plan_item.filter_predicates_,
                  plan_item.filter_predicates_len_);
  }
  // print startup filter
  if (OB_SUCC(ret) && !startup_filter.empty()) {
    BEGIN_BUF_PRINT;
    EXPLAIN_PRINT_EXPRS(startup_filter, type);
    END_BUF_PRINT(plan_item.startup_predicates_,
                  plan_item.startup_predicates_len_);
  }

  if (LOG_LINK_TABLE_SCAN != get_type()) {
    // print vectorized execution batch row count
    ObPhyOperatorType phy_type = PHY_INVALID;
    bool is_root_job = is_plan_root() && (get_parent() == nullptr);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObStaticEngineCG::get_phy_op_type(*this,
                                                         phy_type,
                                                         is_root_job))) {
      /* Do nothing */
    } else if (NULL != get_plan() &&
               get_plan()->get_optimizer_context().get_batch_size() > 0 &&
               ObOperatorFactory::is_vectorized(phy_type)) {
      plan_item.rowset_ = get_plan()->get_optimizer_context().get_batch_size();
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObLogicalOperator::print_outline_data(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  UNUSED(plan_text);
  return ret;
}

int ObLogicalOperator::print_used_hint(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  UNUSED(plan_text);
  return ret;
}

int ObLogicalOperator::print_outline_table(PlanText &plan_text, const TableItem *table_item) const
{
  int ret = OB_SUCCESS;
  ObTableInHint table_hint;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(table_item));
  } else if (OB_FALSE_IT(table_hint.set_table(*table_item))) {
  } else if (OB_FAIL(table_hint.print_table_in_hint(plan_text))) {
    LOG_WARN("failed to print table hint", K(ret));
  }
  return ret;
}

int ObLogicalOperator::do_pre_traverse_operation(const TraverseOp &op, void *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_plan()));
  } else {
    switch (op) {
    case PX_PIPE_BLOCKING: {
      if (get_plan()->get_optimizer_context().get_max_parallel() > 1) {
        ObPxPipeBlockingCtx *pipe_blocking_ctx = static_cast<ObPxPipeBlockingCtx *>(ctx);
        if (OB_ISNULL(pipe_blocking_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ctx", K(ret));
        } else if (OB_FAIL(px_pipe_blocking_pre(*pipe_blocking_ctx))) {
          LOG_WARN("blocking px pipe failed", K(ret));
        }
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
      //do nothing
      AllocGIContext *alloc_gi_ctx = static_cast<AllocGIContext *>(ctx);
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
    case ALLOC_OP: {
      if (OB_ISNULL(ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ctx is null", K(ret));
      } else if (OB_FAIL(alloc_op_pre(*static_cast<AllocOpContext *>(ctx)))) {
        LOG_WARN("alloc op pre failed", K(ret));
      }
      break;
    }
    case ALLOC_EXPR: {
      ObSQLSessionInfo *session = NULL;
      ObAllocExprContext *alloc_expr_context = static_cast<ObAllocExprContext *>(ctx);
      if (OB_ISNULL(alloc_expr_context) ||
          OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(session), K(ret));
      } else if (is_plan_root() && OB_FAIL(adjust_plan_root_output_exprs())) {
        LOG_WARN("failed to set plan root output", K(ret));
      } else if (is_plan_root() && OB_FAIL(collecte_inseparable_exprs(*alloc_expr_context))) {
        LOG_WARN("failed to set plan root output", K(ret));
      } else if (OB_FAIL(allocate_expr_pre(*alloc_expr_context))) {
        LOG_WARN("failed to do allocate expr pre", K(ret));
      } else {
        LOG_TRACE("succeed to do allocate expr pre", K(get_type()), K(get_name()), K(get_op_id()), K(ret));
      }
      break;
    }
    case RUNTIME_FILTER: {
      break;
    }
    case PROJECT_PRUNING: {
      if (OB_FAIL(project_pruning_pre())) {
        LOG_WARN("Project pruning pre error", K(ret));
      } else { /* Do nothing */ }
      break;
    }
    case OPERATOR_NUMBERING: {
      if (OB_ISNULL(ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ctx is null", K(ret));
      } else if (OB_FAIL(numbering_operator_pre(*static_cast<NumberingCtx *>(ctx)))) {
        LOG_WARN("numbering operator pre failed", K(ret));
      }
      break;
    }
    case EXCHANGE_NUMBERING: {
      if (OB_ISNULL(ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ctx is null", K(ret));
      } else if (OB_FAIL(numbering_exchange_pre(*static_cast<NumberingExchangeCtx *>(ctx)))) {
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
    case PX_ESTIMATE_SIZE: {
      ret = px_estimate_size_factor_pre();
      break;
    }
    case ALLOC_STARTUP_EXPR:
      break;
    case COLLECT_BATCH_EXEC_PARAM: {
      if (OB_FAIL(collect_batch_exec_param_pre(ctx))) {
        LOG_WARN("failed to gen batch exec param pre", K(ret));
      }
      break;
    }
    case ADJUST_SCAN_DIRECTION: {
      if (LOG_SORT == get_type()) {
        ObLogSort *log_sort = static_cast<ObLogSort *>(this);
        if (NULL != log_sort->get_topn_filter_node() &&
            LOG_TABLE_SCAN == log_sort->get_topn_filter_node()->get_type()) {
          ObLogTableScan *log_tsc = static_cast<ObLogTableScan *>(log_sort->get_topn_filter_node());
          if (OB_FAIL(log_tsc->try_adjust_scan_direction(log_sort->get_sort_keys()))) {
            LOG_WARN("failed to adjust table scan direction", K(ret));
          }
        }
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

int ObLogicalOperator::do_post_traverse_operation(const TraverseOp &op, void *ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  CK(
    !OB_ISNULL(get_plan()),
    !OB_ISNULL(get_stmt()));

  if (OB_SUCC(ret)) {
    switch (op) {
      case PX_PIPE_BLOCKING: {
        if (get_plan()->get_optimizer_context().get_max_parallel() > 1) {
          ObPxPipeBlockingCtx *pipe_blocking_ctx = static_cast<ObPxPipeBlockingCtx *>(ctx);
          CK(OB_NOT_NULL(pipe_blocking_ctx));
          OC( (px_pipe_blocking_post)(*pipe_blocking_ctx) );
        } else if (get_type() == LOG_EXCHANGE) {
          // parallel = 1,it must use single-dfo
          ObLogExchange *exchange = static_cast<ObLogExchange *>(this);
          if (OB_ISNULL(exchange)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: exchange is null", K(ret));
          } else {
            exchange->set_old_unblock_mode(false);
            LOG_TRACE("pipe blocking ctx", K(get_name()));
          }
        }
        break;
      }
    	case ALLOC_GI: {
    	  AllocGIContext *alloc_gi_ctx = static_cast<AllocGIContext *>(ctx);
    	  if (OB_ISNULL(alloc_gi_ctx)) {
    	    ret = OB_ERR_UNEXPECTED;
    	    LOG_WARN("get unexpected null", K(ret));
    	  } else {
    	    if (get_num_of_child() >= 2) {
    	      alloc_gi_ctx->delete_multi_child_op_count();
    	    }
    	    if (OB_FAIL(allocate_granule_post(*alloc_gi_ctx))) {
    	      LOG_WARN("failed to allocate granule post", K(ret));
    	    } else if (alloc_gi_ctx->alloc_gi_ &&
    	               OB_FAIL(allocate_granule_nodes_above(*alloc_gi_ctx))) {
    	      LOG_WARN("failed to allcoate granule nodes", K(ret));
    	    }
    	  }
    		break;
    	}
      case ALLOC_EXPR: {
        ObAllocExprContext *alloc_expr_ctx = static_cast<ObAllocExprContext *>(ctx);
        if (OB_ISNULL(alloc_expr_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(allocate_expr_post(*alloc_expr_ctx))) {
          LOG_WARN("failed to do allocate expr post", K(ret));
        } else { /* Do nothing */ }
        break;
      }
      case RUNTIME_FILTER: {
        AllocBloomFilterContext *alloc_bf_ctx = static_cast<AllocBloomFilterContext *>(ctx);
        CK( OB_NOT_NULL(alloc_bf_ctx));
        OC( (allocate_runtime_filter_for_hash_join)(*alloc_bf_ctx));
        if (OB_FAIL(ret)) {
        } else if (LOG_SORT == get_type()
                   && OB_FAIL(static_cast<ObLogSort *>(this)
                                  ->try_allocate_pushdown_topn_runtime_filter())) {
          LOG_WARN("failed to allocate topn runtime filter for sort");
        }
        break;
      }
      case ALLOC_OP: {
        if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx is null", K(ret));
        } else if (OB_FAIL(alloc_op_post(*static_cast<AllocOpContext *>(ctx)))) {
          LOG_WARN("failed to alloc op for monitering post",  K(ret));
        }
        break;
      }
      case OPERATOR_NUMBERING: {
        if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx is null", K(ret));
        } else {
          numbering_operator_post(*static_cast<NumberingCtx *>(ctx));
        }
        break;
      }
      case EXCHANGE_NUMBERING: {
        if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx is null", K(ret));
        } else if (LOG_EXCHANGE == type_) {
          if (OB_FAIL(numbering_exchange_post(*static_cast<NumberingExchangeCtx *>(ctx)))) {
            LOG_WARN("fail numbering exchange post", K(ret));
          } else if (static_cast<ObLogExchange*>(this)->is_rescanable() &&
             OB_FAIL(static_cast<ObLogExchange*>(this)->gen_px_pruning_table_locations())) {
            LOG_WARN("fail to gen px pruning table locations", K(ret));
          }
        }
        break;
      }
      case GEN_SIGNATURE: {
        if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx is null", K(ret));
        } else {
          uint64_t *seed = reinterpret_cast<uint64_t *>(ctx);
          *seed = hash(*seed);
          LOG_TRACE("", "operator", get_name(), "hash_value", *seed);
        }
        break;
      }
      case GEN_LOCATION_CONSTRAINT: {
        ret = gen_location_constraint(ctx);
        break;
      }
      case PX_ESTIMATE_SIZE: {
        ret = px_estimate_size_factor_post();
        break;
      }
      case ALLOC_STARTUP_EXPR: {
        if (OB_FAIL(allocate_startup_expr_post())) {
          LOG_WARN("failed to alloc startup expr post", K(ret));
        }
        break;
      }
      case COLLECT_BATCH_EXEC_PARAM: {
        if (OB_FAIL(collect_batch_exec_param_post(ctx))) {
          LOG_WARN("failed to gen batch exec param post",  K(ret));
        }
        break;
      }
      case ADJUST_SCAN_DIRECTION: {
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

int ObLogicalOperator::do_plan_tree_traverse(const TraverseOp &operation, void *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_pre_traverse_operation(operation, ctx))) {
    LOG_WARN("failed to perform traverse operation", K(ret), "operator", get_name(), K(operation));
  } else {
    LOG_TRACE("succ to perform pre traverse operation", "operator", get_name(), K(operation));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get_child(i) is null", K(ret), K(i));
    } else if (OB_FAIL(SMART_CALL(get_child(i)->do_plan_tree_traverse(operation, ctx)))) {
      LOG_WARN("failed to bottom-up traverse operator", "operator", get_name(), K(operation), K(ret));
    } else { /*do nothing*/ }
  }

  // post_traverse_operation
  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_post_traverse_operation(operation, ctx))) {
      LOG_WARN("failed to perform post traverse action", K(ret), K(operation));
    } else {
      LOG_TRACE("succ to perform post traverse action", K(operation), "operator", get_name());
    }
  } else { /*do nothing*/ }

  return ret;
}

int ObLogicalOperator::should_allocate_gi_for_dml(bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (LOG_JOIN == get_type() || LOG_SET == get_type()) {
    is_valid = false;
  } else if (LOG_INSERT == get_type()) {
    const ObLogInsert *log_insert = static_cast<const ObLogInsert *>(this);
    if (!log_insert->is_multi_part_dml()) {
      is_valid = false;
    }
  } else if (LOG_GROUP_BY == get_type()) {
    ObLogGroupBy *log_group_by = static_cast<ObLogGroupBy*>(this);
    is_valid = (AggregateAlgo::MERGE_AGGREGATE != log_group_by->get_algo());
  } else if (LOG_DISTINCT == get_type()) {
    ObLogDistinct *log_distinct = static_cast<ObLogDistinct*>(this);
    is_valid = (AggregateAlgo::MERGE_AGGREGATE != log_distinct->get_algo());
  } else if (LOG_WINDOW_FUNCTION == get_type()) {
    is_valid = false;
  } else { /*do nothing*/ }

  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(get_child(i)->should_allocate_gi_for_dml(is_valid))) {
      LOG_WARN("failed to check should allocate gi for dml", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogicalOperator::mark_expr_produced(ObRawExpr *expr,
                                          uint64_t branch_id,
                                          uint64_t producer_id,
                                          ObAllocExprContext &gen_expr_ctx)
{
  int ret = OB_SUCCESS;
  ExprProducer *expr_producer = NULL;
  if (OB_ISNULL(expr) || OB_INVALID_ID == branch_id || OB_INVALID_ID == producer_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(expr), K(branch_id), K(producer_id));
  } else if (OB_FAIL(gen_expr_ctx.find(expr, expr_producer))) {
    LOG_WARN("failed to find expr producer", K(ret));
  } else if (OB_ISNULL(expr_producer)) {
    ExprProducer new_expr_producer(expr, id_);
    new_expr_producer.producer_branch_ = branch_id;
    new_expr_producer.producer_id_ = producer_id;
    if (OB_FAIL(gen_expr_ctx.add(new_expr_producer))) {
      LOG_WARN("failed to add expr producer", K(ret));
    } else { /*do nothing*/ }
  } else {
    // mark as produced
    if (expr_producer->producer_branch_ == OB_INVALID_ID) {
      expr_producer->producer_branch_ = branch_id;
      expr_producer->producer_id_ = producer_id;
      LOG_TRACE("expr is marked as produced.", K(expr_producer->expr_), K(branch_id),
          K(expr_producer->consumer_id_), K(producer_id));
    }
  }
  return ret;
}

int ObLogicalOperator::collecte_inseparable_exprs(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (stmt->is_dblink_stmt() && stmt->is_select_stmt()) {
    const ObSelectStmt *sel_stmt = static_cast<const ObSelectStmt *>(stmt);
    if (OB_FAIL(sel_stmt->get_select_exprs(ctx.inseparable_exprs_))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_expr_pre(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> all_exprs;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else if (OB_FAIL(get_plan()->get_optimizer_context().get_all_exprs().append(all_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(extract_non_const_exprs(all_exprs,
                                             op_exprs_))) {
    LOG_WARN("failed to extract const exprs", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, op_exprs_))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else {
    LOG_TRACE("succeed to allocate expr pre", K(id_), K(op_exprs_.count()),
        K(op_exprs_), K(get_name()), K(is_plan_root()));
  }
  return ret;
}

int ObLogicalOperator::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_array_no_dup(all_exprs, get_filter_exprs()))) {
    LOG_WARN("failed to append filter exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_exprs, get_startup_exprs()))) {
    LOG_WARN("failed to get start up exprs", K(ret));
  } else if (is_plan_root() && OB_FAIL(append_array_no_dup(all_exprs, get_output_exprs()))) {
    LOG_WARN("failed to get output exprs", K(ret));
  } else { /*do noting*/ }
  return ret;
}

int ObLogicalOperator::get_next_producer_id(ObLogicalOperator *node,
                                            uint64_t &producer_id)
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

int ObLogicalOperator::build_and_put_pack_expr(ObIArray<ObRawExpr*> &output_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *pack_expr = NULL;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_optimizer_context().get_exec_ctx()) ||
      OB_ISNULL(get_plan()->get_optimizer_context().get_exec_ctx()->get_physical_plan_ctx()) ||
      OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(child), K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_pack_expr(
       get_plan()->get_optimizer_context().get_expr_factory(),
       get_plan()->get_optimizer_context().is_ps_protocol(),
       get_plan()->get_optimizer_context().get_session_info(),
       get_plan()->get_optimizer_context().get_exec_ctx()->get_physical_plan_ctx()->get_field_array(),
       output_exprs,
       pack_expr))) {
    LOG_WARN("failed to build pack expr", K(ret));
  } else if (OB_ISNULL(pack_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(output_exprs.reuse())) {
    /*do nothing*/
  } else if (OB_FAIL(output_exprs.push_back(pack_expr))) {
    LOG_WARN("failed to push back pack expr", K(ret));
  } else if (OB_FAIL(child->add_op_exprs(pack_expr))) {
    LOG_WARN("failed to add op exprs", K(ret));
  } else {
    get_plan()->get_optimizer_context().set_packed(true);
  }
  return ret;
}

int ObLogicalOperator::build_and_put_into_outfile_expr(const ObSelectIntoItem *into_item,
                                                       ObIArray<ObRawExpr*> &output_exprs)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  ObRawExpr *to_outfile_expr = NULL;
  uint64_t producer_id = OB_INVALID_ID;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(into_item) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(into_item), K(child), K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_to_outfile_expr(
                                    get_plan()->get_optimizer_context().get_expr_factory(),
                                    get_plan()->get_optimizer_context().get_session_info(),
                                    into_item,
                                    output_exprs,
                                    to_outfile_expr))) {
    LOG_WARN("failed to build_to_outfile_expr", K(*into_item), K(ret));
  } else if (OB_ISNULL(to_outfile_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(output_exprs.reuse())) {
    /*do nothing*/
  } else if (OB_FAIL(output_exprs.push_back(to_outfile_expr))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED == get_phy_plan_type() &&
             OB_FAIL(put_into_outfile_expr(to_outfile_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogicalOperator::put_into_outfile_expr(ObRawExpr *to_outfile_expr)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(to_outfile_expr) ||
      OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(to_outfile_expr), K(child), K(ret));
  } else {
    bool is_find = false;
    while (NULL != child && !is_find && 1 == child->get_num_of_child()) {
      if (IS_EXPR_PASSBY_OPER(child->get_type()) ||
          (log_op_def::LOG_EXCHANGE == (child->get_type()) &&
           static_cast<ObLogExchange*>(child)->is_consumer())) {
        child = child->get_child(first_child);
      } else {
        is_find = true;
      }
    }
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(child->add_op_exprs(to_outfile_expr))) {
      LOG_WARN("failed to add op exprs", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogicalOperator::add_exprs_to_ctx(ObAllocExprContext &ctx,
                                        const ObIArray<ObRawExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  uint64_t producer_id = OB_INVALID_ID;
  if (OB_FAIL(get_next_producer_id(this, producer_id))) {
    LOG_WARN("failed to get next producer id", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, exprs, producer_id))) {
    LOG_WARN("failed to add exprs to ctx", K(exprs), K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogicalOperator::add_exprs_to_ctx(ObAllocExprContext &ctx,
                                        const ObIArray<ObRawExpr*> &input_exprs,
                                        uint64_t producer_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_exprs.count(); i++) {
    if (OB_FAIL(add_expr_to_ctx(ctx, input_exprs.at(i), producer_id))) {
      LOG_WARN("failed to add expr to ctx", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::add_expr_to_ctx(ObAllocExprContext &ctx,
                                       ObRawExpr* expr,
                                       uint64_t producer_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> shared_exprs;
  ObRawExpr *raw_expr = NULL;
  ExprProducer *raw_producer = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(extract_shared_exprs(expr,
                                          ctx,
                                          0,
                                          shared_exprs))) {
    LOG_WARN("failed to extract column input_exprs", K(ret));
  }
  // add shared exprs, should set both producer id and consumer id
  for (int64_t i = 0; OB_SUCC(ret) && i < shared_exprs.count(); i++) {
    uint64_t consumer_id = OB_INVALID_ID;
    uint64_t real_producer_id = producer_id;
    if (OB_ISNULL(raw_expr = shared_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ctx.find(raw_expr, raw_producer))) {
      LOG_WARN("failed to find raw expr producer", K(ret));
    } else if (NULL != raw_producer) {
      // update the raw_producer id
      real_producer_id = std::min(raw_producer->producer_id_, producer_id);
      if (OB_INVALID_ID == raw_producer->producer_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid id", KPC(raw_producer), K(producer_id), K(ret));
      } else if (!can_update_producer_id_for_shared_expr(raw_expr)) {
        // do nothing
      } else if (OB_FAIL(find_producer_id_for_shared_expr(raw_expr, real_producer_id))) {
        LOG_WARN("failed to find sharable expr producer id", K(ret));
      } else {
        raw_producer->producer_id_ = real_producer_id;
      }
      LOG_TRACE("succeed to update shared expr producer id", KP(raw_expr),
                K(producer_id), K(consumer_id), KPC(raw_producer), K(get_name()));
    } else if (OB_FAIL(find_consumer_id_for_shared_expr(&ctx.expr_producers_, raw_expr,
                                                        consumer_id))) {
      LOG_WARN("failed to find sharable expr consumer id", K(ret));
    } else if (OB_FAIL(find_producer_id_for_shared_expr(raw_expr, real_producer_id))) {
      LOG_WARN("failed to find sharable expr producer id", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == consumer_id || OB_INVALID_ID == real_producer_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected conusmer id or producer id", K(*raw_expr), K(ret));
    } else {
      ExprProducer expr_producer(raw_expr, consumer_id, real_producer_id);
      if (OB_FAIL(ctx.add(expr_producer))) {
        LOG_WARN("failed to push balck raw_expr", K(ret));
      } else {
        LOG_TRACE("succeed to add shared exprs", KP(raw_expr), K(producer_id), K(consumer_id),
                                                 K(expr_producer), K(get_name()));
      }
    }
  }
  // add input expression, should set both producer and consumer id
  if (OB_SUCC(ret)) {
    uint64_t consumer_id = id_;
    if (ObOptimizerUtil::find_item(shared_exprs, expr)) {
      /*do nothing*/
    } else if (OB_FAIL(ctx.find(expr, raw_producer))) {
      LOG_WARN("failed to raw expr producer", K(ret));
    } else if (NULL != raw_producer) {
      // update the raw_producer id, this branch seems to be unreachable
      if (OB_ISNULL(raw_producer)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_UNLIKELY(!expr->is_dynamic_const_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), KPC(expr));
      } else {
        if (OB_INVALID_ID == raw_producer->producer_id_) {
          raw_producer->producer_id_ = producer_id;
        } else {
          raw_producer->producer_id_ = std::min(raw_producer->producer_id_, producer_id);
        }
        LOG_TRACE("succeed to update input expr producer id", KP(expr),
            KPC(expr), K(producer_id), K(consumer_id), KPC(raw_producer), K(get_name()));
      }
    } else {
      ExprProducer expr_producer(expr, consumer_id, producer_id);
      if (OB_FAIL(ctx.add(expr_producer))) {
        LOG_WARN("failed to push balck expr", K(ret));
      } else {
        LOG_TRACE("succeed to add input expr", KP(expr), K(producer_id), K(consumer_id),
                                               K(expr_producer), K(get_name()));
      }
    }
  }
  return ret;
}

bool ObLogicalOperator::can_update_producer_id_for_shared_expr(const ObRawExpr *expr)
{
  bool can = true;
  if (NULL != expr && expr->has_flag(IS_DYNAMIC_PARAM)) {
    if (log_op_def::LOG_SUBPLAN_FILTER == get_type()) {
      can = static_cast<ObLogSubPlanFilter*>(this)->is_my_exec_expr(expr);
    } else if (log_op_def::LOG_JOIN == get_type()) {
      can = static_cast<ObLogJoin*>(this)->is_my_exec_expr(expr);
    } else {
      can = false;
    }
  } else if (NULL != expr && expr->has_flag(IS_ROWNUM)) {
    can = (log_op_def::LOG_COUNT == get_type());
  }
  return can;
}

int ObLogicalOperator::extract_non_const_exprs(const ObIArray<ObRawExpr*> &input_exprs,
                                               ObIArray<ObRawExpr*> &non_const_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_exprs.count(); i++) {
    if (OB_ISNULL(expr = input_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(expr));
    } else if (expr->is_static_const_expr() ||
               expr->has_flag(IS_DYNAMIC_USER_VARIABLE)) {
      /*do nothing*/
    } else if (OB_FAIL(add_var_to_array_no_dup(non_const_exprs, expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogicalOperator::extract_shared_exprs(const ObIArray<ObRawExpr*> &exprs,
                                            ObAllocExprContext &ctx,
                                            ObIArray<ObRawExpr*> &shard_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(extract_shared_exprs(exprs.at(i),
                                             ctx,
                                             0,
                                             shard_exprs))) {
      LOG_WARN("failed to extract fix producer exprs", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogicalOperator::extract_shared_exprs(ObRawExpr *raw_expr,
                                             ObAllocExprContext &ctx,
                                             int64_t parent_ref_cnt,
                                             ObIArray<ObRawExpr*> &shard_exprs)
{
  int ret = OB_SUCCESS;
  int64_t ref_cnt = 0;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(raw_expr), K(ret));
  } else if (raw_expr->is_static_const_expr()) {
    /*do nothing*/
  } else if (T_OP_ROW == raw_expr->get_expr_type()) {
    /*do nothing*/
  } else if (OB_FAIL(ctx.get_expr_ref_cnt(raw_expr, ref_cnt))) {
    LOG_WARN("failed to get expr ref cnt", K(ret));
  } else if (ref_cnt <= parent_ref_cnt) {
    /*do nothing*/
  } else if (OB_FAIL(add_var_to_array_no_dup(shard_exprs, raw_expr))) {
    LOG_WARN("failed to add var to array", K(ret));
  }

  if (!ObOptimizerUtil::find_item(ctx.inseparable_exprs_, raw_expr) &&
      !raw_expr->is_match_against_expr()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); ++i) {
      ret = SMART_CALL(extract_shared_exprs(raw_expr->get_param_expr(i),
                                            ctx,
                                            ref_cnt,
                                            shard_exprs));
    }
  }
  return ret;
}

int ObLogicalOperator::find_consumer_id_for_shared_expr(const ObIArray<ExprProducer> *ctx,
                                                        const ObRawExpr *expr,
                                                        uint64_t &consumer_id)
{
  int ret = OB_SUCCESS;
  consumer_id = OB_INVALID_ID;
  if (OB_ISNULL(ctx) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx->count(); i++) {
      bool need_check_status = (i + 1) % 1000 == 0;
      if (OB_ISNULL(ctx->at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (need_check_status &&
                 OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status fail", K(ret));
      } else if (ObOptimizerUtil::is_point_based_sub_expr(expr, ctx->at(i).expr_)) {
        if (OB_INVALID_ID == consumer_id) {
          consumer_id = ctx->at(i).consumer_id_;
        } else {
          consumer_id = std::max(consumer_id, ctx->at(i).consumer_id_);
        }
      } else { /*do nothing*/}
    }
  }
  return ret;
}

int ObLogicalOperator::find_producer_id_for_shared_expr(const ObRawExpr *expr,
                                                        uint64_t &producer_id)
{
  int ret = OB_SUCCESS;
  bool need_pushdown = false;
  bool can_pushdown = false;
  uint64_t pushdown_producer_id = OB_INVALID_ID;
  if (OB_FAIL(check_need_pushdown_expr(producer_id, need_pushdown))) {
    LOG_WARN("failed to check need pushdown expr");
  } else if (!need_pushdown) {
    // do nothing
  } else if (OB_FAIL(check_can_pushdown_expr(expr, can_pushdown))) {
    LOG_WARN("failed to check can push down expr", K(ret));
  } else if (!can_pushdown) {
    // do nothing
  } else if (OB_FAIL(get_pushdown_producer_id(expr, pushdown_producer_id))) {
    LOG_WARN("failed to get pushdown producer id", K(ret));
  } else if (OB_INVALID_ID != pushdown_producer_id) {
    producer_id = pushdown_producer_id;
  }
  return ret;
}

// check whether need pushdown expr according to the plan tree structure
int ObLogicalOperator::check_need_pushdown_expr(const uint64_t producer_id,
                                                bool &need_pushdown)
{
  int ret = OB_SUCCESS;
  need_pushdown = false;
  if (producer_id < id_ ) {
    // do nothing
  } else if (child_.empty()) {
    // do nothing
  } else if (ObLogOpType::LOG_GROUP_BY == get_type() ||
             ObLogOpType::LOG_SORT == get_type() ||
             ObLogOpType::LOG_JOIN == get_type() ||
             ObLogOpType::LOG_DISTINCT == get_type() ||
             ObLogOpType::LOG_UPDATE == get_type() ||
             ObLogOpType::LOG_DELETE == get_type() ||
             ObLogOpType::LOG_INSERT == get_type() ||
             ObLogOpType::LOG_WINDOW_FUNCTION == get_type() ||
             ObLogOpType::LOG_SELECT_INTO == get_type() ||
             ObLogOpType::LOG_TOPK == get_type() ||
             ObLogOpType::LOG_COUNT == get_type() ||
             ObLogOpType::LOG_MERGE == get_type()) {
    need_pushdown = true;
  }
  return ret;
}

// check whether need pushdown expr according to the expr type
int ObLogicalOperator::check_can_pushdown_expr(const ObRawExpr *expr,
                                               bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  bool is_contain = false;
  can_pushdown = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (expr->is_const_expr()) {
    // do nothing
  } else if (child_.count() > 1 && expr->has_flag(CNT_OP_PSEUDO_COLUMN)) {
    // do nothing, I have no idea to pushdown the op pseudo column into which child op
  } else if (OB_FAIL(contain_my_fixed_expr(expr, is_contain))) {
    LOG_WARN("failed to check contain my fixed expr", K(ret));
  } else if (!is_contain) {
    can_pushdown = true;
  }
  return ret;
}

int ObLogicalOperator::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  UNUSED(expr);
  is_fixed = false;
  return OB_SUCCESS;
}

int ObLogicalOperator::contain_my_fixed_expr(const ObRawExpr *expr,
                                             bool &is_contain)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(is_my_fixed_expr(expr, is_contain))) {
    LOG_WARN("failed to check is my fixed expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_contain && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(contain_my_fixed_expr(expr->get_param_expr(i), is_contain)))) {
      LOG_WARN("failed to check contain my fixed expr", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::get_pushdown_producer_id(const ObRawExpr *expr, uint64_t &producer_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  producer_id = OB_INVALID_ID;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < child_.count(); ++i) {
    ObLogicalOperator *node = child_.at(i);
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child op", K(ret));
    } else if (node->is_expr_operator()) {
      // do nothing
    } else if (!expr->get_relation_ids().is_subset(node->get_table_set())) {
      // do nothing
    } else if (OB_FAIL(get_next_producer_id(node, producer_id))) {
      LOG_WARN("failed to get next producer id", K(ret));
    } else {
      found = true;
    }
  }
  return ret;
}

int ObLogicalOperator::get_pushdown_producer_id(uint64_t &producer_id)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    // do nothing
  } else if (OB_FAIL(get_next_producer_id(child, producer_id))) {
    LOG_WARN("failed to get next producer id", K(ret));
  }
  return ret;
}

int ObLogicalOperator::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *parent = NULL;
  ObLogicalOperator *child = NULL;
  if (IS_EXPR_PASSBY_OPER(type_) && !is_plan_root()) {
    if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(output_exprs_.assign(child->get_output_exprs()))) {
      LOG_WARN("failed to assign output exprs", K(ret));
    } else { /*do nothing*/ }
  } else if (log_op_def::LOG_EXCHANGE == type_ &&
             static_cast<ObLogExchange*>(this)->get_is_remote() &&
             static_cast<ObLogExchange*>(this)->is_producer()) {
    if (OB_ISNULL(parent = get_parent()) ||
        OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(parent), K(child), K(ret));
    } else if (OB_FAIL(output_exprs_.assign(parent->get_output_exprs()))) {
      LOG_WARN("failed to assign output exprs", K(ret));
    } else if (OB_FAIL(child->get_output_exprs().assign(parent->get_output_exprs()))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else { /*do nothing*/ }
  } else {
    ObIArray<ExprProducer> &producers = ctx.expr_producers_;

    for (int64_t i = 0; OB_SUCC(ret) && i < producers.count(); i++) {
      if (id_ >= producers.at(i).producer_id_) {
        ObRawExpr *expr = producers.at(i).expr_;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_INVALID_ID == producers.at(i).producer_branch_ &&
                    producers.at(i).producer_id_ == id_) { // not produced yet
          LOG_TRACE("try to produce expr", K(*expr), K(get_name()));
          bool can_be_produced = false;
          if (OB_FAIL(expr_can_be_produced(expr, ctx, can_be_produced))) {
            LOG_WARN("expr_can_be_produced fails", K(ret));
          } else if (can_be_produced) {
            producers.at(i).producer_branch_ = branch_id_;
            producers.at(i).producer_id_ = id_; // dummy assign
            LOG_TRACE("expr can be produced now", K(*expr), K(get_name()), K(id_));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr can not be produced now", K(*expr), K(get_name()), K(id_));
          }
        } else { /*do nothing*/ }

        if (OB_SUCC(ret) && OB_INVALID_ID != producers.at(i).producer_branch_ && producers.at(i).consumer_id_ > id_) {
          if (!is_plan_root() && (is_child_output_exprs(expr) || producers.at(i).producer_id_ == id_)) {
            if (OB_FAIL(add_var_to_array_no_dup(output_exprs_, expr))) {
              LOG_WARN("failed to add expr to output", K(ret));
            } else {
              LOG_TRACE("expr is added into output expr", K(*expr), K(get_name()),
                  K(producers.at(i).consumer_id_), K(id_));
            }
          } else {
            LOG_TRACE("expr is not added into output expr",
                K(*expr), K(get_name()), K(producers.at(i).consumer_id_), K(id_));
          }
        }
      } else { /* do nothing */ }
    }
  }
  return ret;
}

int ObLogicalOperator::expr_can_be_produced(const ObRawExpr *expr,
                                            ObAllocExprContext &expr_ctx,
                                            bool &can_be_produced)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  const ObRawExpr *dependant_expr = NULL;
  can_be_produced = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (0 == expr->get_param_count()) {
    LOG_TRACE("checking if expr can be produced", K(expr), K(*expr), K(get_name()),
        K(id_), K(branch_id_));
    if (OB_FAIL(expr_has_been_produced(expr, expr_ctx, can_be_produced))) {
      LOG_WARN("failed to check whether expr_has_been_produced", K(ret));
    } else if (can_be_produced) {
      LOG_TRACE("expr has been produced", K(expr), K(get_name()), K(id_), K(branch_id_));
    } else if (!expr->has_flag(CNT_COLUMN)) {
      can_be_produced = true;
      LOG_TRACE("expr can be produced", K(expr), K(*expr), K(get_name()),
              K(id_), K(branch_id_));
    } else if (expr->is_column_ref_expr() &&
               NULL != (dependant_expr = static_cast<const ObColumnRefRawExpr*>(expr)->get_dependant_expr())) {
      if (OB_FAIL(expr_can_be_produced(dependant_expr, expr_ctx, can_be_produced))) {
        LOG_WARN("failed to check expr can be produced", K(ret));
      } else { /*do nothing*/ }
    } else {
      can_be_produced = false;
      LOG_TRACE("expr can not be produced", K(expr), K(*expr), K(get_name()),
          K(id_), K(branch_id_));
    }
  } else {
    LOG_TRACE("checking if expr can be produced", K(expr), K(*expr), K(expr->get_param_count()),
        K(get_name()), K(id_), K(branch_id_));
    can_be_produced = true;
    for (int64_t i = 0; OB_SUCC(ret) && can_be_produced && i < expr->get_param_count(); i++) {
      const ObRawExpr *param_expr = expr->get_param_expr(i);
      bool param_produced = false;
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ", K(ret));
      } else if (OB_FAIL(expr_has_been_produced(param_expr, expr_ctx, param_produced))) {
        LOG_WARN("failed to check whether expr_has_been_produced", K(ret));
      } else if (param_produced) {
        LOG_TRACE("input expr has been produced", K(param_expr), K(*param_expr),
            K(get_name()), K(id_), K(branch_id_));
      } else if (OB_FAIL(SMART_CALL(expr_can_be_produced(param_expr, expr_ctx, param_produced)))) {
        LOG_WARN("failed to check expr can be produced", K(ret));
      } else if (!param_produced) {
        // if any of their inputs is not produced yet, the expr cannot be produced
        can_be_produced = false;
        LOG_TRACE("input expr has not yet been produced", K(param_expr), K(*param_expr),
                  K(get_name()), K(id_), K(branch_id_));
      } else {
        LOG_TRACE("input expr not yet produced, but can be", K(param_expr), K(*param_expr),
                  K(get_name()), K(id_), K(branch_id_));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::expr_has_been_produced(const ObRawExpr *expr,
                                              ObAllocExprContext &expr_ctx,
                                              bool &has_been_produced)
{
  int ret = OB_SUCCESS;
  has_been_produced = false;
  ExprProducer *expr_producer = NULL;
  if (OB_FAIL(expr_ctx.find(expr, expr_producer)))  {
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
  } else if (OB_INVALID_ID != expr_producer->producer_branch_
              && expr_producer->producer_branch_ >= branch_id_) {
    has_been_produced = true;
  }
  return ret;
}

bool ObLogicalOperator::is_child_output_exprs(const ObRawExpr *expr) const
{
  bool is_child_output = false;
  for (int i = 0; !is_child_output && i < get_num_of_child(); i++) {
    if (NULL != get_child(i) &&
        ObOptimizerUtil::find_item(get_child(i)->get_output_exprs(), expr)) {
      is_child_output = true;
    } else { /*do nothing*/ }
  }
  return is_child_output;
}

int ObLogicalOperator::check_is_table_scan(const ObLogicalOperator &op,
                                           bool &is_table_scan)
{
  int ret = OB_SUCCESS;
  is_table_scan = false;
  const ObLogicalOperator *cur_op = &op;
  while (NULL != cur_op) {
    if (LOG_TABLE_SCAN == cur_op->get_type()) {
      is_table_scan = true;
      cur_op = NULL;
    } else if (LOG_MATERIAL == cur_op->get_type() ||
               LOG_JOIN_FILTER == cur_op->get_type() ||
               LOG_SORT == cur_op->get_type() ||
               LOG_SUBPLAN_SCAN == cur_op->get_type()) {
      cur_op = cur_op->get_child(first_child);
    } else {
      cur_op = NULL;
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
  } else if (OB_FAIL(reorder_filters_exprs(get_plan()->get_predicate_selectivities(),
                                           filter_exprs_))) {
    LOG_WARN("reorder filter exprs failed", K(ret));
  } else if (log_op_def::LOG_JOIN == get_type()) {
    ObLogJoin *join_op = static_cast<ObLogJoin *>(this);
    if (OB_FAIL(reorder_filters_exprs(get_plan()->get_predicate_selectivities(),
                                      join_op->get_join_filters()))) {
      LOG_WARN("reorder join filters failed", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::reorder_filters_exprs(common::ObIArray<ObExprSelPair> &predicate_selectivities,
                                             ObIArray<ObRawExpr *> &filter_exprs)
{
  int ret = OB_SUCCESS;
  double card = 0;
  FilterCompare filter_compare(predicate_selectivities);
  common::ObSEArray<ObExprRankPair, 4> filter_ranks;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()));
  } else if (OB_FAIL(get_card_without_filter(card))) {
    LOG_WARN("get num of rows to be filtered failed", K(ret));
  } else if (card < 1.0) {
    card = 1.0;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); ++i) {
    double cost_per_tuple = 0.0;
    double sel = filter_compare.get_selectivity(filter_exprs.at(i));
    double rank = 0;
    if (sel < 0) {
      // security filter should be calc firstly
      rank = -NAN;
    } else if (OB_FAIL(ObOptEstCost::calc_pred_cost_per_row(filter_exprs.at(i),
                                                            card,
                                                            cost_per_tuple,
                                                            get_plan()->get_optimizer_context()))) {
      LOG_WARN("calc pred cost failed", K(ret));
    } else {
      rank = (sel - 1) / cost_per_tuple;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(filter_ranks.push_back(ObExprRankPair(rank, filter_exprs.at(i))))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(filter_ranks.begin(), filter_ranks.end(), ObExprRankPairCompare());
    for(int64_t i = 0; i < filter_ranks.count(); ++i) {
      filter_exprs.at(i) = filter_ranks.at(i).second;
    }
  }
  return ret;
}

/**
 * 生成计划的location约束，有以下三种约束：
 * base_table_constraints_:
 *    基表location约束，包括TABLE_SCAN算子上的基表和INSERT算子上的基表
 * strict_pwj_constraint_:
 *    严格partition wise join约束，要求同一个分组内的基表分区逻辑上和物理上都相等。
 *    每个分组是一个array，保存了对应基表在base_table_constraints_中的偏移
 * non_strict_pwj_constraint_:
 *    严格partition wise join约束，要求用一个分组内的基表分区物理上相等。
 *    每个分组是一个array，保存了对应基表在base_table_constraints_中的偏移
 */
int ObLogicalOperator::gen_location_constraint(void *ctx)
{
  int ret = OB_SUCCESS;
  bool is_union_all_set_pw = false;
  bool is_setop_ext_pw = false;
  bool is_join_ext_pw = false;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret), K(ctx), K(get_stmt()), K(get_plan()));
  } else {
    ObLocationConstraintContext *loc_cons_ctx = reinterpret_cast<ObLocationConstraintContext *>(ctx);

    if (get_num_of_child() == 0) {
      if (log_op_def::LOG_TABLE_SCAN == get_type()) {
        // base table constraints for TABLE SCAN
        LocationConstraint loc_cons;
        ObDupTabConstraint dup_rep_cons;
        bool found_dup_con = false;
        ObLogTableScan *log_scan_op = dynamic_cast<ObLogTableScan *>(this);
        if (log_scan_op->get_contains_fake_cte()) {
          // do nothing
        } else if (log_scan_op->use_das()) {
          // not add to constraints for das
        } else if (OB_INVALID_ID != log_scan_op->get_dblink_id()) {
          // dblink table, execute at other cluster
        } else if (OB_FAIL(get_tbl_loc_cons_for_scan(loc_cons))) {
          LOG_WARN("failed to get location constraint for table scan op", K(ret));
        } else if (OB_FAIL(get_dup_replica_cons_for_scan(dup_rep_cons, found_dup_con))) {
          LOG_WARN("failed to get duplicate table replica constraint for table scan op", K(ret));
        } else if (found_dup_con &&
                   OB_FAIL(loc_cons_ctx->dup_table_replica_cons_.push_back(dup_rep_cons))) {
          LOG_WARN("failed to push back location constraint", K(ret));
        } else if (OB_FAIL(loc_cons_ctx->base_table_constraints_.push_back(loc_cons))) {
          LOG_WARN("failed to push back location constraint", K(ret));
        } else if (EXTERNAL_TABLE == log_scan_op->get_table_type()) {
          // do not add pwj constraints for external table
        } else if (OB_FAIL(strict_pwj_constraint_.push_back(
                    loc_cons_ctx->base_table_constraints_.count() - 1))) {
          LOG_WARN("failed to push back location constraint offset", K(ret));
        } else if (OB_FAIL(non_strict_pwj_constraint_.push_back(
                    loc_cons_ctx->base_table_constraints_.count() - 1))) {
          LOG_WARN("failed to push back location constraint offset", K(ret));
        }
      } else if (log_op_def::LOG_TEMP_TABLE_ACCESS == get_type()) {
        ObLogTempTableAccess *access = static_cast<ObLogTempTableAccess*>(this);
        ObLogicalOperator *insert_op = NULL;
        if (OB_FAIL(access->get_temp_table_plan(insert_op))) {
          LOG_WARN("failed to get temp table plan", K(ret));
        } else if (OB_ISNULL(insert_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null operator", K(ret));
        } else if (OB_FAIL(append(strict_pwj_constraint_, insert_op->strict_pwj_constraint_))) {
          LOG_WARN("failed to append child pwj constraint", K(ret));
        } else if (OB_FAIL(append(non_strict_pwj_constraint_, insert_op->non_strict_pwj_constraint_))) {
          LOG_WARN("failed to append child pwj constraint", K(ret));
        }
      }
    } else if (get_num_of_child() > 0) {
      /**
       * 当前算子从孩子算子（insert包括自身）上继承的pwj约束的次数，如果继承次数超过1次，
       * 说明有多个pwj约束可以合并，就需要将当前算子的pwj约束添加到ctx中
       * e.g.        join3
       *            /    \         base_table_constraints = [t1,t2,t3,t4]
       *         join1   join2
       * 假设join1上有严格约束(0,1), join2上有严格约束(2,3), 那么join3应该继承并合并左右孩子节点的约束(0,1,2,3)
       */

      int64_t add_count = 0;
      bool is_pdml = false;
      if (log_op_def::LOG_SET == get_type()) {
        ObLogSet *set_op = static_cast<ObLogSet *>(this);
        is_union_all_set_pw = (ObSelectStmt::UNION == set_op->get_set_op() && !set_op->is_set_distinct()) ||
                              set_op->is_recursive_union();
        is_union_all_set_pw = is_union_all_set_pw && (DistAlgo::DIST_SET_PARTITION_WISE == set_op->get_distributed_algo());
        is_setop_ext_pw = (DistAlgo::DIST_EXT_PARTITION_WISE == set_op->get_distributed_algo());
      } else if (log_op_def::LOG_JOIN == get_type()) {
        ObLogJoin *join_op = static_cast<ObLogJoin *>(this);
        is_join_ext_pw = (DistAlgo::DIST_EXT_PARTITION_WISE == join_op->get_dist_method());
      }

      if (log_op_def::LOG_INSERT == get_type() &&
          !get_stmt()->has_instead_of_trigger() &&
          static_cast<ObLogInsert*>(this)->is_insert_select()) {
        // base table constraints for INSERT
        // multi part insert只记录基表location约束，非multi part insert需要同时记录
        // 基表location约束和partition wise join约束
        bool is_multi_part_dml = false;
        LocationConstraint loc_cons;
        if (OB_FAIL(get_tbl_loc_cons_for_insert(loc_cons, is_multi_part_dml))) {
          LOG_WARN("failed to get location constraint for insert op", K(ret));
        } else if (!is_multi_part_dml) {
          // 非multi part insert
          if (OB_FAIL(loc_cons_ctx->base_table_constraints_.push_back(loc_cons))) {
            LOG_WARN("failed to push back location constraint", K(ret));
          } else if (OB_FAIL(strict_pwj_constraint_.push_back(
                      loc_cons_ctx->base_table_constraints_.count() - 1))) {
            LOG_WARN("failed to push back location constraint offset", K(ret));
          } else if (OB_FAIL(non_strict_pwj_constraint_.push_back(
                      loc_cons_ctx->base_table_constraints_.count() - 1))) {
            LOG_WARN("failed to push back location constraint offset", K(ret));
          } else {
            ++add_count;
          }
        } else {
          bool find = false;
          ObIArray<ObTablePartitionInfo *> & partition_infos =
              get_plan()->get_optimizer_context().get_table_partition_info();
          for (int64_t i = 0; !find && i < partition_infos.count(); ++i) {
            const ObTablePartitionInfo *cur_info = partition_infos.at(i);
            if (OB_NOT_NULL(cur_info) &&
                cur_info->get_table_id() == loc_cons.key_.table_id_ &&
                cur_info->get_ref_table_id() == loc_cons.key_.ref_table_id_) {
              find = true;
            }
          }
          if (!find) {
            // local multi part insert, remove location constraint of insert table because
            // we didn't add it's table location.
          } else if (OB_FAIL(loc_cons_ctx->base_table_constraints_.push_back(loc_cons))) {
            LOG_WARN("failed to push back location constraint", K(ret));
          }
        }
      }

      // 处理pdml index维护的location 约束
      // 目前支持的pdml logical op包含（insert，update，delete）
      // pdml logical op中需要维护phy table location的条件是：
      // 1. 当前逻辑算子是pdml
      // 2. 当前算子是用于维护global index table
      if (OB_SUCC(ret)) {
        if (log_op_def::LOG_DELETE == get_type()
            || log_op_def::LOG_UPDATE == get_type()) {
          ObLogDelUpd *dml_log_op = static_cast<ObLogDelUpd *>(this);
          if (dml_log_op->is_pdml()) {
            LocationConstraint loc_cons;
            is_pdml = true;
            if (OB_FAIL(get_tbl_loc_cons_for_pdml_index(loc_cons))) {
              LOG_WARN("failed to get table location constraint for pdml index",
                K(ret), K(get_name()));
            } else if (OB_FAIL(loc_cons_ctx->base_table_constraints_.push_back(loc_cons))) {
              LOG_WARN("failed to push back location constraint", K(ret));
            } else if (OB_FAIL(strict_pwj_constraint_.push_back(
                        loc_cons_ctx->base_table_constraints_.count() - 1))) {
              LOG_WARN("failed to push back location constraint offset", K(ret));
            } else if (OB_FAIL(non_strict_pwj_constraint_.push_back(
                        loc_cons_ctx->base_table_constraints_.count() - 1))) {
              LOG_WARN("failed to push back location constraint offset", K(ret));
            } else {
              ++add_count;
              LOG_TRACE("add location constraint for pdml index maintain op",
                K(ret), K(loc_cons), K(get_name()),
                "base_table_cons", loc_cons_ctx->base_table_constraints_);
            }
          }
        }
      }

      bool need_add_strict = true;
      bool need_add_non_strict = true;
      int64_t i = 0;
      if (log_op_def::LOG_TEMP_TABLE_TRANSFORMATION == get_type()) {
        // each branch of temp_table_transformation is isolated
        // only need to inherit the last branch
        i = get_num_of_child() - 1;
      }
      for (/* do nothing */; OB_SUCC(ret) && !is_pdml && i < get_num_of_child(); ++i) {
        if (OB_ISNULL(get_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(i));
        } else if (LOG_EXCHANGE != get_child(i)->get_type()) {
          /*
          *
          *  MULTI PARTITION INSERT
          *     EXCHANGE IN DISTR
          *       EXCHANGE OUT DISTR
          *         MERGE JOIN
          *          TABLE SCAN    t1
          *          TABLE LOOKUP
          *            TABLE SCAN  t2(idx_t2)
          *
          *  t1的分区和idx_t2的分区需在同一个节点上，否则partition wise join 报4016
          *  因此table lookup 需要继承子节点的约束
          */
          LOG_TRACE("get child location constraint", K(i),
              K(get_child(i)->strict_pwj_constraint_), K(get_child(i)->non_strict_pwj_constraint_));
          if (get_child(i)->strict_pwj_constraint_.count() <= 0 &&
              get_child(i)->non_strict_pwj_constraint_.count() <= 0) {
            // 孩子节点没有记录pwj约束，说明这个分支上没有table scan或者存在exchange
          } else if (get_child(i)->strict_pwj_constraint_.count() <= 0 ||
                     get_child(i)->non_strict_pwj_constraint_.count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("strict pwj constraint and non strict pwj constraint count not valid",
                        K(ret), K(get_child(i)->strict_pwj_constraint_.count()),
                        K(get_child(i)->non_strict_pwj_constraint_.count()));
          } else {
            // 所有算子都会继承第一个可继承孩子节点的严格pwj约束和非严格pw约束
            // set pw类型union all算子会继承所有孩子节点的非严格pw约束
            // extended pw join/set不继承其他严格和非严格pw约束
            ++add_count;
            if (need_add_strict) {
              if (OB_FAIL(append(strict_pwj_constraint_, get_child(i)->strict_pwj_constraint_))) {
                LOG_WARN("failed to append child pwj constraint", K(ret));
              } else if (is_union_all_set_pw || is_setop_ext_pw || is_join_ext_pw) {
                need_add_strict = false;
              }
            }
            if (need_add_non_strict) {
              if (OB_FAIL(append(non_strict_pwj_constraint_, get_child(i)->non_strict_pwj_constraint_))) {
                LOG_WARN("failed to append child pwj constraint", K(ret));
              } else if (!is_union_all_set_pw) {
                need_add_non_strict = false;
              }
            }
          }
        }
      } // for end

      if (OB_SUCC(ret) && add_count > 1) {
        if (is_union_all_set_pw) {
          if (OB_FAIL(loc_cons_ctx->non_strict_constraints_.push_back(&non_strict_pwj_constraint_))) {
            LOG_WARN("failed to push back pwj constraint");
          }
        } else if (OB_FAIL(loc_cons_ctx->strict_constraints_.push_back(&strict_pwj_constraint_))) {
          LOG_WARN("fail to push back pwj constraint", K(ret));
        }
        LOG_TRACE("succ to gen location cons", K(is_union_all_set_pw), K(is_setop_ext_pw), K(is_join_ext_pw),
                  K(strict_pwj_constraint_), K(non_strict_pwj_constraint_));
      }
    } else { /* do nothing */ }
  }

  return ret;
}

int ObLogicalOperator::get_tbl_loc_cons_for_pdml_index(LocationConstraint &loc_cons)
{
  int ret = OB_SUCCESS;
  ObLogDelUpd *dml_log_op = static_cast<ObLogDelUpd *>(this);
  ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
  ObShardingInfo *sharding = nullptr;
  if (OB_ISNULL(dml_log_op) || OB_ISNULL(sharding = dml_log_op->get_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml logical operator is null", K(ret), K(dml_log_op));
  } else if (OB_FAIL(dml_log_op->get_table_location_type(location_type))) {
    LOG_WARN("failed to get location type", K(ret));
  } else {
    loc_cons.phy_loc_type_ = location_type;
    loc_cons.key_.table_id_ = dml_log_op->get_table_id();
    loc_cons.key_.ref_table_id_ = dml_log_op->get_index_tid();
    loc_cons.table_partition_info_ = dml_log_op->get_table_partition_info();
    if (sharding->get_part_cnt() > 1 && sharding->is_distributed()) {
      if (sharding->is_partition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SinglePartition);
      } else if (sharding->is_subpartition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SingleSubPartition);
      }
    }
  }
  return ret;
}

int ObLogicalOperator::get_tbl_loc_cons_for_scan(LocationConstraint &loc_cons)
{
  int ret = OB_SUCCESS;
  ObLogTableScan *log_scan_op = dynamic_cast<ObLogTableScan *>(this);
  ObShardingInfo *sharding = nullptr;
  if (OB_ISNULL(log_scan_op) || OB_ISNULL(sharding = log_scan_op->get_sharding())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_scan_op), K(sharding));
  } else if (OB_FAIL(log_scan_op->get_phy_location_type(loc_cons.phy_loc_type_))) {
    LOG_WARN("failed to get phy location type", K(ret));
  } else {
    loc_cons.key_.table_id_ = log_scan_op->get_table_id();
    loc_cons.table_partition_info_ = log_scan_op->get_table_partition_info();
    loc_cons.key_.ref_table_id_ = log_scan_op->get_real_index_table_id();
    if (NULL != sharding->get_phy_table_location_info() &&
        sharding->get_phy_table_location_info()->is_duplicate_table_not_in_dml()) {
      loc_cons.add_constraint_flag(LocationConstraint::DupTabNotInDML);
    }
    if (sharding->get_part_cnt() > 1 && sharding->is_distributed()) {
      if (sharding->is_partition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SinglePartition);
      } else if (sharding->is_subpartition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SingleSubPartition);
      }
    }

    LOG_TRACE("initialized table's location constraint for table scan op", K(loc_cons));
  }
  return ret;
}

int ObLogicalOperator::get_dup_replica_cons_for_scan(ObDupTabConstraint &dup_rep_cons,
                                                     bool &found_dup_con)
{
  int ret = OB_SUCCESS;
  ObLogTableScan *log_scan_op = dynamic_cast<ObLogTableScan *>(this);
  ObShardingInfo *sharding = NULL;
  if (OB_ISNULL(log_scan_op) ||
      OB_ISNULL(sharding = log_scan_op->get_sharding())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_scan_op));
  } else if (NULL != sharding->get_phy_table_location_info()) {
    // is duplicate table
    if (log_scan_op->get_advisor_table_id() != OB_INVALID_ID &&
        sharding->get_phy_table_location_info()->is_duplicate_table_not_in_dml()) {
      dup_rep_cons.first_  = log_scan_op->get_table_id();
      dup_rep_cons.second_ = log_scan_op->get_advisor_table_id();
      found_dup_con = true;
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObLogicalOperator::get_tbl_loc_cons_for_insert(LocationConstraint &loc_cons, bool &is_multi_part_dml)
{
  int ret = OB_SUCCESS;
  ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
  ObLogInsert *log_insert_op = static_cast<ObLogInsert *>(this);
  ObShardingInfo *sharding = nullptr;
  if (OB_ISNULL(log_insert_op) || OB_ISNULL(sharding = log_insert_op->get_sharding())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_insert_op), K(sharding));
  } else if (OB_FAIL(log_insert_op->get_table_location_type(location_type))) {
    LOG_WARN("failed to get location type", K(ret));
  } else {
    is_multi_part_dml = log_insert_op->is_multi_part_dml();
    loc_cons.phy_loc_type_ = location_type;
    loc_cons.key_.table_id_ = log_insert_op->get_loc_table_id();
    loc_cons.key_.ref_table_id_ = log_insert_op->get_index_tid();
    loc_cons.table_partition_info_ = log_insert_op->get_table_partition_info();
    if (is_multi_part_dml) {
      loc_cons.add_constraint_flag(LocationConstraint::IsMultiPartInsert);
    } else if (sharding->get_part_cnt() > 1 && sharding->is_distributed()) {
      if (sharding->is_partition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SinglePartition);
      } else if (sharding->is_subpartition_single()) {
        loc_cons.add_constraint_flag(LocationConstraint::SingleSubPartition);
      }
    }
    LOG_TRACE("initialized table's location constraint for insert op", K(loc_cons), K(is_multi_part_dml));
  }

  return ret;
}

// under Gi operator, find the table scan whose table id is same as the argument table id
int ObLogicalOperator::get_table_scan(ObLogicalOperator *&tsc, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  // Under GI, there are no GI, exchange
  if (LOG_GRANULE_ITERATOR == get_type() || LOG_EXCHANGE == get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operator type", K(get_type()), K(ret));
  } else if (nullptr == tsc && log_op_def::LOG_TABLE_SCAN == get_type() && table_id == static_cast<ObLogTableScan*>(this)->get_table_id()) {
    tsc = this;
  }
  for (int64_t i = 0; OB_SUCC(ret) && nullptr == tsc && i < get_num_of_child(); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get_child(i) is null", K(ret), K(i));
    } else if (OB_FAIL(SMART_CALL(get_child(i)->get_table_scan(tsc, table_id)))) {
      LOG_WARN("failed to set op ordering in parts recursively", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogicalOperator::find_all_tsc(ObIArray<ObLogicalOperator *> &tsc_ops,
                                    ObLogicalOperator *root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the log op is null", K(ret));
  } else if (log_op_def::LOG_EXCHANGE != get_type()) {
    for(int64_t i = 0; i < root->get_num_of_child(); i++) {
      ObLogicalOperator *child = root->get_child(i);
      if (OB_FAIL(find_all_tsc(tsc_ops, child))) {
        LOG_WARN("failed to find all tsc", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && log_op_def::LOG_TABLE_SCAN == root->get_type()) {
    if (OB_FAIL(tsc_ops.push_back(root))) {
      LOG_WARN("failed to push back tsc ops", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_material(const int64_t index)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  ObLogicalOperator *child = NULL;
  ObLogPlan *child_plan = NULL;
  ObLogicalOperator* mat_op = NULL;
  if (OB_ISNULL(plan = get_plan()) || OB_ISNULL(child = get_child(index)) ||
      OB_ISNULL(child_plan = child->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child), K(plan), K(child_plan));
  } else if (OB_ISNULL(mat_op = child_plan->get_log_op_factory().allocate(*child_plan, log_op_def::LOG_MATERIAL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate material operator", K(ret));
  } else {
    mat_op->set_child(0, child);
    child->set_parent(mat_op);
    mat_op->set_parent(this);
    set_child(index, mat_op);
    if (OB_FAIL(mat_op->compute_property())) {
      LOG_WARN("failed to compute property");
    } else {
      mat_op->set_op_cost(0.0);
      mat_op->set_cost(child->get_cost());
    }
    //把Material当作下层LogPlan的顶点，否则没人会给这个Material分配表达式
    if (OB_SUCC(ret) && child->is_plan_root()) {
      if (OB_FAIL(mat_op->get_output_exprs().assign(child->get_output_exprs()))) {
        LOG_WARN("failed to assign output exprs", K(ret));
      } else {
        mat_op->mark_is_plan_root();
        child_plan->set_plan_root(mat_op);
        child->set_is_plan_root(false);
        child->get_output_exprs().reuse();
      }
    }
  }
  return ret;
}

int ObLogicalOperator::check_exchange_rescan(bool &need_rescan)
{
  int ret = OB_SUCCESS;
  need_rescan = false;
  if (LOG_EXCHANGE == get_type()) {
    need_rescan = true;
  } else if (LOG_MATERIAL == get_type()) { //TODO:遇到其他物化类算子也可以停止探测，但目前它们不支持真正意义的物化
    /*no nothing, stop checking deeply*/
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !need_rescan && i < get_num_of_child(); ++i) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_child(i) returns null", K(i), K(ret));
      } else if (OB_FAIL(get_child(i)->check_exchange_rescan(need_rescan))) {
        LOG_WARN("get_child(i)->check_exchange_rescan() fails", K(ret));
      } else { /* Do nothing */ }
    }
  }
  return ret;
}

int ObLogicalOperator::check_has_op_below(const log_op_def::ObLogOpType target_type, bool &has) const
{
  int ret = OB_SUCCESS;
  has = false;
  if (target_type == get_type()) {
    has = true;
  } else {
    ObLogicalOperator *child_op = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !has && i < get_num_of_child(); ++i) {
      if (OB_ISNULL(child_op = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_child(i) returns null", K(i), K(ret));
      } else if (OB_FAIL(SMART_CALL(child_op->check_has_op_below(target_type, has)))) {
        LOG_WARN("failed to check if child operator has exchange below", KPC(child_op), K(ret));
      } else { /* Do nothing */ }
    }
  }
  return ret;
}

uint64_t ObLogicalOperator::hash(uint64_t seed) const
{
  seed = do_hash(type_, seed);
  seed = do_hash(id_, seed);
  LOG_TRACE("operator hash", K(get_op_name(type_)));
  return seed;
}

int ObLogicalOperator::numbering_operator_pre(NumberingCtx &ctx)
{
  int ret = OB_SUCCESS;
  op_id_ = ctx.op_id_++;
  plan_depth_ = ctx.plan_depth_++;
  if (LOG_COUNT == get_type()) {
    ObRawExpr *rownum_expr = NULL;
    if (OB_ISNULL(get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is null", K(ret));
    } else if (OB_FAIL(get_stmt()->get_rownum_expr(rownum_expr))) {
      LOG_WARN("get rownum expr failed", K(ret));
    } else if (OB_ISNULL(rownum_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no rownum expr in stmt of count operator", K(ret));
    } else {
      ObSysFunRawExpr *sys_rownum_expr = static_cast<ObSysFunRawExpr *>(rownum_expr);
      sys_rownum_expr->set_op_id(op_id_);
    }
  }
  if (ctx.going_up_) {
    ctx.branch_id_++;
    ctx.going_up_ = false;
  } else { /* Do nothing */ }

  if (OB_INVALID_ID == branch_id_) {
    branch_id_ = ctx.branch_id_;
  } else { /* Do nothing */ }
  return ret;
}

void ObLogicalOperator::numbering_operator_post(NumberingCtx &ctx)
{
  id_ = ctx.num_++;
  ctx.plan_depth_--;
  if (!ctx.going_up_) {
    ctx.going_up_ = true;
  } else { /* Do nothing */ }
}

int ObLogicalOperator::refine_dop_by_hint()
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = nullptr;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt()) ||
      OB_ISNULL(query_ctx = get_plan()->get_stmt()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (LOG_EXCHANGE == type_
             && static_cast<ObLogExchange*>(this)->is_producer()
             && this->get_parallel() > 1) {
    // note: don't change single-step dfo sched. query's dop, which can't be parallized
    ObLogExchange *producer = static_cast<ObLogExchange*>(this);
    const ObIArray<ObDopHint>& dops = query_ctx->get_global_hint().dops_;
    ARRAY_FOREACH(dops, idx) {
      int64_t px_id = dops.at(idx).dfo_ / 10000;
      int64_t dfo_id = dops.at(idx).dfo_ % 10000;
      const int64_t hint_dop = static_cast<int64_t>(dops.at(idx).dop_);
      if (px_id == producer->get_px_id() && dfo_id == producer->get_dfo_id() && 1 < hint_dop) {
        producer->set_parallel(hint_dop);
        LOG_DEBUG("XXXX: set op dop to hint value",
                 K(hint_dop), K(px_id), K(dfo_id), K(op_id_));
      }
    }
  }
  return ret;
}

// disable material op allocation in following cases:
// rownum, nlj, subplan filter, remote execution.s
int ObLogicalOperator::alloc_op_pre(AllocOpContext& ctx)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = nullptr;
  if (OB_ISNULL(my_plan_) || OB_ISNULL(my_plan_->get_stmt()) ||
      OB_ISNULL(query_ctx = my_plan_->get_stmt()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (query_ctx->get_global_hint().alloc_op_hints_.empty()){
    /*no ops will be allocated, skip*/
  } else {
    if (!ctx.gen_temp_op_id_) {
      if (OB_FAIL(gen_temp_op_id(ctx))) {
        LOG_WARN("fail to gen temp op id", K(ret));
      }
      ctx.gen_temp_op_id_ = true;
    }
    // disable nodes in COUNT-rownum situation
    if (OB_SUCC(ret) && LOG_COUNT == get_type()) {
      ObRawExpr *rownum_expr = NULL;
      if (OB_ISNULL(get_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret));
      } else if (OB_FAIL(get_stmt()->get_rownum_expr(rownum_expr))) {
        LOG_WARN("get rownum expr failed", K(ret));
      } else if (OB_ISNULL(rownum_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no rownum expr in stmt of count operator", K(ret));
      } else {
        ObSysFunRawExpr *sys_rownum_expr = static_cast<ObSysFunRawExpr *>(rownum_expr);
        sys_rownum_expr->set_op_id(op_id_);
      }
    }
    ObLogicalOperator *rownum_op = NULL;
    if (OB_SUCC(ret) && OB_SUCC(find_rownum_expr(op_id_, rownum_op)) && rownum_op != NULL) {
      uint64_t op_id = rownum_op->get_op_id();
      ObLogicalOperator *parent = rownum_op->get_parent();
      while (OB_SUCC(ret) && op_id != op_id_ && parent != NULL) {
        ret = ctx.disabled_op_set_.set_refactored(op_id_);
        if (ret != OB_SUCCESS && ret != OB_HASH_EXIST) {
          LOG_WARN("set_refactored fail", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
        op_id = parent->get_op_id();
        parent = parent->get_parent();
      }
    }
    // disable all right childs of nlj and spf
    if (OB_SUCC(ret) &&
        (log_op_def::LOG_SUBPLAN_FILTER == get_type() ||
        (log_op_def::LOG_JOIN == get_type() &&
        NESTED_LOOP_JOIN == static_cast<ObLogJoin*>(this)->get_join_algo()))) {
      if (get_num_of_child() < 2 || OB_ISNULL(get_child(second_child))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get the second child of nested loop join", K(ret));
      } else if (OB_FAIL(get_child(second_child)->recursively_disable_alloc_op_above(ctx))) {
        LOG_WARN("fail to disable alloc op above", K(ret));
      }
    }
    // disable nodes of a remote plan
    if (OB_SUCC(ret) &&
        (get_plan()->get_optimizer_context().get_exec_ctx()->get_sql_ctx()->is_remote_sql_
        || OB_PHY_PLAN_REMOTE == get_plan()->get_optimizer_context().get_phy_plan_type())) {
      ret = ctx.disabled_op_set_.set_refactored(op_id_);
      if (ret != OB_SUCCESS && ret != OB_HASH_EXIST) {
        LOG_WARN("set_refactored fail", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObLogicalOperator::alloc_op_post(AllocOpContext& ctx)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = my_plan_->get_stmt()->get_query_ctx(); // has already checked in alloc_op_pre
  if (query_ctx->get_global_hint().alloc_op_hints_.empty()){
    /*no ops will be allocated, skip*/
  } else {
    const ObIArray<ObAllocOpHint> &alloc_op_hints = query_ctx->get_global_hint().alloc_op_hints_;
    // There won't be too many 'blocking or tracing' hints, so it is acceptable for us to traverse the entire array three times:
    // Allocate `all` level nodes first
    // Allocate `dfo` level nodes srcond
    // Allocate enumerate level nodes finally
    for (int64_t i = 0; OB_SUCC(ret) && i < alloc_op_hints.count(); ++i) {
      if (ObAllocOpHint::OB_ALL == alloc_op_hints.at(i).alloc_level_ &&
          OB_FAIL(alloc_nodes_above(ctx, alloc_op_hints.at(i).flags_))) {
        LOG_WARN("fail to alloc op at all level", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < alloc_op_hints.count(); ++i) {
      if (ObAllocOpHint::OB_DFO == alloc_op_hints.at(i).alloc_level_ &&
          (log_op_def::LOG_EXCHANGE == type_ &&
          static_cast<ObLogExchange*>(this)->is_consumer()) &&
          OB_FAIL(alloc_nodes_above(ctx, alloc_op_hints.at(i).flags_))) {
        LOG_WARN("fail to alloc op at dfo level", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < alloc_op_hints.count(); ++i) {
      if (ObAllocOpHint::OB_ENUMERATE == alloc_op_hints.at(i).alloc_level_ &&
          alloc_op_hints.at(i).id_ == op_id_ &&
          OB_FAIL(alloc_nodes_above(ctx, alloc_op_hints.at(i).flags_))) {
        LOG_WARN("fail to alloc op at enumerate level", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::numbering_exchange_pre(NumberingExchangeCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (LOG_EXCHANGE == type_) {
    ObLogExchange *exchange_op = static_cast<ObLogExchange*>(this);
    if (exchange_op->is_px_consumer() && exchange_op->is_rescanable()) {
      ret = ctx.push_px(ctx.next_px());
    }
  }
  return ret;
}

int ObLogicalOperator::numbering_exchange_post(NumberingExchangeCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (LOG_EXCHANGE == type_) {
    ObLogExchange *exchange_op = static_cast<ObLogExchange*>(this);
    if (exchange_op->is_px_producer()) {
      int64_t px_id = OB_INVALID_ID;
      int64_t dfo_id = OB_INVALID_ID;
      if (OB_FAIL(ctx.next_dfo(px_id, dfo_id))) {
        LOG_WARN("get next dfo id fail", K(ret));
      } else {
        exchange_op->set_dfo_id(dfo_id);
        exchange_op->set_px_id(px_id);
        // refine dop by DOP() hint
        if (OB_FAIL(refine_dop_by_hint())) {
          LOG_WARN("fail refine dfo dop", K(ret), K(dfo_id), K(px_id));
        }
      }
    } else if (exchange_op->is_px_consumer() && exchange_op->is_rescanable()) {
      ret = ctx.pop_px();
    }
  }
  return ret;
}

/*
 * 算法；每个节点在先序pre时进行push，后序post时pop，同时pop之前将自身的factor merge到parent上，
 * 这样可以将需要传递的factor传递给整颗树
 *            o(1)
 *          /       \
 *         o(2)     o(7)
 *      /       \
 *     o(3)     o(6)
 *    /   \
 *   o(4) o(5)
 * 数字表示入栈的顺序，即push操作
 */
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
    ObLogGranuleIterator *log_gi = static_cast<ObLogGranuleIterator*>(this);
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

// 这里统一将child的factor合并到parent，而不是通过从parent拉child的方式来处理facotr
int ObLogicalOperator::px_estimate_size_factor_post()
{
  int ret = OB_SUCCESS;
  // first process self, then some special process like exchange,join, then process parent
  if (LOG_TABLE_SCAN == type_) {
    if (!px_est_size_factor_.has_granule()) {
      px_est_size_factor_.single_partition_table_scan_ = true;
    }
  } else if (LOG_GRANULE_ITERATOR == type_) {
    bool partition_granule = false;
    ObLogGranuleIterator *log_gi = static_cast<ObLogGranuleIterator*>(this);
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
    ObLogExchange *exchange = static_cast<ObLogExchange*>(this);
    if (exchange->is_px_producer()) {
      // 跨shuffle，则reset所有，仅保留exchange相关
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
        ObLogJoin *join = static_cast<ObLogJoin*>(parent_);
        // revert right of hash join exchange factor
        if (JoinAlgo::HASH_JOIN == join->get_join_algo()
          && join->get_child(second_child) == this) {
          child_factor.revert_exchange();
        }
      }
    }
    if (1 < get_num_of_child()) {
      // broadcast只服务于HashJoin，如果上层不是HashJoin的二元或多元Operator，则去掉Broadcast设置
      // 但其实有些plan也是基于Broadcast的，如
      //  HashJoin
      //    Union all
      //      Exchange(Broadcast)
      //        Ta
      //      Exchange(Broadcast)
      //        Tb
      //    Tc
      child_factor.revert_exchange();
    }
    parent_->px_est_size_factor_.merge_factor(child_factor);
    LOG_TRACE("trace estimate size factor", K(id_),
      "op factor", px_est_size_factor_,
      "parent factor", parent_->px_est_size_factor_);
  }
  return ret;
}

int ObLogicalOperator::px_rescan_pre()
{
  int ret = OB_SUCCESS;
  /* 一共有 3 中生成 QC 的场景：
   * 1. 顶层算子是 EXCHANGE，本身就是 QC
   * 2. 顶层算子下的一层 EXCHANGE 都是 QC
   * 3. SUBPLAN FILTER 下的一层 EXCHANGE 算子 (first_child 除外)
   * 4. Nested Loop Join (both children are pulled to local execution)
   * 5. Recursive union all
   */
  if ((is_plan_root() && nullptr == get_parent()) ||
       (NULL != get_parent() && get_parent()->get_type() == LOG_TEMP_TABLE_TRANSFORMATION)) {
    if (OB_FAIL(mark_child_exchange_rescanable())) {
      LOG_WARN("failed to mark child exchange rescanable", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_FAIL(ret)) {
  } else if (LOG_SUBPLAN_FILTER == type_) {
    if (OB_FAIL(check_subplan_filter_child_exchange_rescanable())) {
      LOG_WARN("mark child ex-receive as px op fail", K(ret));
    } else if (get_plan()->get_optimizer_context().get_max_parallel() <= 1 &&
        !static_cast<ObLogSubPlanFilter*>(this)->get_exec_params().empty() &&
        get_plan()->get_optimizer_context().enable_px_batch_rescan()) {
      common::ObIArray<bool>&enable_px_batch_rescans =
          static_cast<ObLogSubPlanFilter*>(this)->get_px_batch_rescans();
      bool find_px = false;
      bool nested_rescan = false;
      for (int i = 0; i < get_num_of_child() && OB_SUCC(ret); ++i) {
        find_px = false;
        nested_rescan = false;
        if (0 == i) {
          enable_px_batch_rescans.push_back(false);
        } else if (static_cast<ObLogSubPlanFilter*>(this)->get_onetime_idxs().has_member(i)) {
          find_px = false;
        } else if (OB_FAIL(get_child(i)->find_nested_dis_rescan(nested_rescan, false))) {
          LOG_WARN("fail to find nested rescan", K(ret));
        } else if (nested_rescan) {
          /*do nothing*/
        } else if (OB_FAIL(get_child(i)->find_px_for_batch_rescan(log_op_def::LOG_SUBPLAN_FILTER,
              get_op_id(), find_px))) {
          LOG_WARN("fail to find px for batch rescan", K(ret));
        }
        if (OB_SUCC(ret) && 0 != i) {
          if (OB_FAIL(enable_px_batch_rescans.push_back(find_px))) {
            LOG_WARN("fail to push back find px", K(ret));
          }
        }
      }
    }
  } else if (LOG_JOIN == type_ &&
             JoinAlgo::NESTED_LOOP_JOIN == static_cast<ObLogJoin*>(this)->get_join_algo()
             && !(static_cast<ObLogJoin*>(this)->get_join_type() == CONNECT_BY_JOIN
                  && static_cast<ObLogJoin*>(this)->is_nlj_without_param_down())) {
    if (OB_ISNULL(get_child(second_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (LOG_MATERIAL == get_child(second_child)->get_type()) {
      /*do nothing*/
    } else if (OB_FAIL(get_child(second_child)->mark_child_exchange_rescanable())) {
      LOG_WARN("mark child ex-receive as px op fail", K(ret));
    } else if (static_cast<ObLogJoin*>(this)->get_join_type() != CONNECT_BY_JOIN &&
               static_cast<ObLogJoin*>(this)->is_nlj_with_param_down() &&
               !IS_SEMI_ANTI_JOIN(static_cast<ObLogJoin*>(this)->get_join_type()) &&
               get_plan()->get_optimizer_context().get_max_parallel() <= 1 &&
               get_plan()->get_optimizer_context().enable_px_batch_rescan()) {
      bool find_px = false;
      bool nested_rescan = false;
      if (OB_FAIL(get_child(second_child)->find_nested_dis_rescan(nested_rescan, false))) {
       LOG_WARN("fail to find nested rescan", K(ret));
      } else if (nested_rescan) {
        /*do nothing*/
      } else if (OB_FAIL(get_child(second_child)->find_px_for_batch_rescan(log_op_def::LOG_JOIN,
            get_op_id(), find_px))) {
          LOG_WARN("fail to find px for batch rescan", K(ret));
      } else if (find_px) {
        static_cast<ObLogJoin*>(this)->set_px_batch_rescan(true);
      }
    }
  } else if (LOG_SET == type_ &&
            static_cast<ObLogSet*>(this)->is_recursive_union()) {
    // recursive union all need to restart plan when rescan right child.
    if (OB_ISNULL(get_child(second_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
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
    ObLogExchange *exchange_op = static_cast<ObLogExchange*>(this);
    if (exchange_op->is_consumer()) {
      exchange_op->set_rescanable(true);
    }
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

int ObLogicalOperator::check_output_dependance(ObIArray<ObRawExpr *> &child_output, PPDeps &deps)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> exprs;
  LOG_TRACE("start to check output exprs", K(type_), K(child_output), K(deps));
  ObRawExprCheckDep dep_checker(child_output, deps, false);
  if (OB_FAIL(append(exprs, op_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(exprs, output_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(dep_checker.check(exprs))) {
    LOG_WARN("failed to check output exprs", K(ret));
  } else {
    LOG_TRACE("succeed to check output exprs", K(exprs), K(type_), K(deps));
  }
  return ret;
}

int ObLogicalOperator::project_pruning_pre()
{
  int ret = OB_SUCCESS;
  // delete exprs who appeared in current op's output_exprs
  // but not used by it's parent's output_exprs_
  if (NULL != parent_ && !is_plan_root() &&
      LOG_EXPR_VALUES != type_ &&
      !(LOG_EXCHANGE == type_ && static_cast<ObLogExchange*>(this)->get_is_remote()) &&
      LOG_VALUES_TABLE_ACCESS != type_) {
    PPDeps deps;
    if (OB_FAIL(parent_->check_output_dependance(get_output_exprs(), deps))) {
      LOG_WARN("parent_->check_output_dep() fails", K(ret));
    } else {
      do_project_pruning(get_output_exprs(), deps);
    }
  } else { /* do nothing */ }

  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (LOG_TABLE_SCAN == type_) {
    ObLogTableScan *table_scan = static_cast<ObLogTableScan*>(this);
    PPDeps deps;
    if (OB_FAIL(check_output_dependance(table_scan->get_access_exprs(), deps))) {
      LOG_WARN("check_output_dep fails", K(ret));
    } else {
      do_project_pruning(table_scan->get_access_exprs(), deps);
    }
    if (OB_SUCC(ret) && OB_FAIL(table_scan->index_back_check())) {
      LOG_WARN("failed to check index back", K(ret));
    } else { /* Do nothing */ }
  } else if (LOG_SUBPLAN_SCAN == type_) {
    ObLogSubPlanScan *subplan_scan = static_cast<ObLogSubPlanScan*>(this);
    PPDeps deps;
    if (OB_FAIL(check_output_dependance(subplan_scan->get_access_exprs(), deps))) {
      LOG_WARN("check_output_dep fails", K(ret));
    } else {
      do_project_pruning(subplan_scan->get_access_exprs(), deps);
    }
  } else if (LOG_TEMP_TABLE_ACCESS == type_) {
    ObLogTempTableAccess *temp_scan = static_cast<ObLogTempTableAccess*>(this);
    PPDeps deps;
    if (OB_FAIL(check_output_dependance(temp_scan->get_access_exprs(), deps))) {
      LOG_WARN("check_output_dep fails", K(ret));
    } else {
      do_project_pruning(temp_scan->get_access_exprs(), deps);
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(try_add_remove_const_exprs())) {
    LOG_WARN("failed to add remove const exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

void ObLogicalOperator::do_project_pruning(ObIArray<ObRawExpr *> &exprs, PPDeps &deps)
{
  int64_t i = 0;
  int64_t j = 0;
  LOG_TRACE("start to do project pruning", K(type_), K(exprs), K(deps));
  for (i = 0, j = 0; i < exprs.count(); i++) {
    if (deps.has_member(static_cast<int32_t>(i))
        || T_ORA_ROWSCN == exprs.at(i)->get_expr_type()) {
      exprs.at(j++) = exprs.at(i);
    } else {
      LOG_TRACE("project pruning remove expr", K(exprs.at(i)), K(*exprs.at(i)),
          K(get_name()), K(lbt()));
    }
  }
  while (i > j) {
    exprs.pop_back();
    i--;
  }
}

int ObLogicalOperator::try_add_remove_const_exprs()
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0) {
    // do nothing
  } else if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(ret));
  } else {
    FOREACH_X(e, output_exprs_, OB_SUCC(ret)) {
      // Add remove_const() to above const expr, except:
      // - remove_const() already added. (has CNT_VOLATILE_CONST flag)
      // - is dynamic param store (has CNT_DYNAMIC_PARAM flag). Because question mark expr of
      //   dynamic param store may be passed by operator output. e.g.:
      //
      if (OB_ISNULL((*e))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if ((*e)->is_const_expr() &&
                 !(*e)->has_flag(CNT_VOLATILE_CONST) &&
                 !(log_op_def::LOG_EXCHANGE == get_type()
                   && static_cast<ObLogExchange*>(this)->is_producer()
                   && (*e)->has_flag(CNT_DYNAMIC_PARAM))) {
        ObRawExpr *remove_const_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_remove_const_expr(
                                    get_plan()->get_optimizer_context().get_expr_factory(),
                                    *get_plan()->get_optimizer_context().get_session_info(),
                                    *e,
                                    remove_const_expr))) {
          LOG_WARN("failed to build remove const expr", K(output_exprs_), K(*e), K(ret));
        } else if (OB_ISNULL(remove_const_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(get_plan()->get_optimizer_context().get_all_exprs().append(remove_const_expr))) {
          LOG_WARN("faield to append exprs", K(ret));
        } else {
          *e = remove_const_expr;
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::adjust_plan_root_output_exprs()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObSelectIntoItem *into_item = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (output_exprs_.empty()) {
    /*do nothing*/
  } else if (stmt->is_select_stmt() &&
             FALSE_IT(into_item = static_cast<const ObSelectStmt*>(stmt)->get_select_into())) {
    /*do nothing*/
  } else if (NULL == get_parent()) {
    bool need_pack = false;
    if (OB_FAIL(check_stmt_can_be_packed(stmt, need_pack))) {
      LOG_WARN("failed to check stmt can be pack", K(ret));
    } else if (need_pack && OB_FAIL(build_and_put_pack_expr(output_exprs_))) {
      LOG_WARN("failed to add pack expr to context", K(ret));
    }
    LOG_TRACE("succeed to adjust plan root output exprs", K(output_exprs_));
  }
  return ret;
}

int ObLogicalOperator::set_plan_root_output_exprs()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (stmt->is_select_stmt()) {
    const ObSelectStmt *sel_stmt = static_cast<const ObSelectStmt*>(get_stmt());
    bool is_unpivot = (LOG_UNPIVOT == type_ && sel_stmt->is_unpivot_select());
    uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
    if (!sel_stmt->has_select_into() && OB_FAIL(sel_stmt->get_select_exprs(output_exprs_, is_unpivot))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else { /*do nothing*/ }
  } else if (stmt->is_returning()) {
    const ObDelUpdStmt *del_upd_stmt = static_cast<const ObDelUpdStmt *>(stmt);
    if (OB_FAIL(append(output_exprs_, del_upd_stmt->get_returning_exprs()))) {
      LOG_WARN("failed to append returning exprs into output", K(ret));
    }
  } else { /*do nothing*/ }

  return ret;
}

int ObLogicalOperator::check_stmt_can_be_packed(const ObDMLStmt *stmt, bool &need_pack)
{
  int ret = OB_SUCCESS;
  need_pack = false;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    bool has_var_assign = get_plan()->get_optimizer_context().has_var_assign();
    bool is_var_assign_only_in_root = get_plan()->get_optimizer_context().is_var_assign_only_in_root_stmt();
    need_pack = stmt->is_select_stmt() && (!session_info->is_inner()) && LOG_EXCHANGE == type_
                 && (ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED == get_phy_plan_type()) && !has_var_assign;
  }
  return ret;
}

int ObLogicalOperator::replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (!replacer.empty()) {
    FOREACH_CNT_X(it, get_op_ordering(), OB_SUCC(ret)) {
      if (OB_FAIL(replace_expr_action(replacer, it->expr_))) {
        LOG_WARN("replace expr failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(replace_exprs_action(replacer, get_filter_exprs()))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer, get_output_exprs()))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (OB_FAIL(inner_replace_op_exprs(replacer))) {
      LOG_WARN("failed to inner replace expr", K(ret));
    } else { /* Do nothing */ }
  } else { /* Do nothing */ }
  return ret;
}

int ObLogicalOperator::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  //do operator specific
  UNUSED(replacer);
  return ret;
}

/*
 * pair: orig_expr  new_expr
 */
int ObLogicalOperator::replace_exprs_action(
        ObRawExprReplacer &replacer,
        ObIArray<ObRawExpr *> &dest_exprs)
{
  int ret = OB_SUCCESS;
  int64_t dest_num = dest_exprs.count();
  if (!replacer.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_num; ++i) {
      ObRawExpr *&cur_expr = dest_exprs.at(i);
      if (OB_FAIL(replace_expr_action(replacer, cur_expr))) {
        LOG_WARN("failed to do replace expr action", K(ret));
      } else { /* Do nothing */ }
    }
  } else { /* Do nothing */ }
  return ret;
}

int ObLogicalOperator::replace_expr_action(ObRawExprReplacer &replacer, ObRawExpr *&dest_expr)
{
  int ret = OB_SUCCESS;
  if (!replacer.empty()) {
    if (OB_FAIL(replacer.replace(dest_expr))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (replacer.get_replace_happened()) {
      ObSQLSessionInfo *session_info = get_plan()->get_optimizer_context().get_session_info();
      if (OB_ISNULL(session_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is null", K(ret));
      } else if (OB_FAIL(dest_expr->formalize(session_info))) {
        LOG_WARN("fail to formalize expr", K(ret));
      }
    }
  } else { /* Do nothing */ }
  return ret;
}

int ObLogicalOperator::check_sharding_compatible_with_reduce_expr(
                       const ObIArray<ObRawExpr*> &reduce_exprs,
                       bool &compatible) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> part_exprs;
  ObSEArray<ObRawExpr*, 4> part_column_exprs;
  compatible = false;
  if (NULL == strong_sharding_) {
    /*do nothing*/
  } else if (OB_FAIL(strong_sharding_->get_all_partition_keys(part_exprs, true))) {
    LOG_WARN("failed to get all part keys", K(ret));
  } else if (0 == part_exprs.count()) {
    compatible = false;
  } else if (ObOptimizerUtil::subset_exprs(part_exprs,
                                           reduce_exprs,
                                           get_output_equal_sets())) {
    compatible = true;
  } else if (ObRawExprUtils::is_all_column_exprs(part_exprs)) {
    /*do nothing*/
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_exprs,
                                                          part_column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (ObOptimizerUtil::subset_exprs(part_column_exprs,
                                           reduce_exprs,
                                           get_output_equal_sets())) {
    compatible = true;
  } else { /*do nothing*/ }

  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to check sharding compatiable info", K(compatible), K(part_column_exprs),
        K(reduce_exprs));
  }
  return ret;
}

struct Compare
{
  Compare() {}
  bool operator()(const ObLogicalOperator::PartInfo &l, const ObLogicalOperator::PartInfo &r)
  {
    bool less = false;
    if (l.part_id_ < r.part_id_) {
      less = true;
    } else if (l.part_id_  == r.part_id_ && l.subpart_id_ < r.subpart_id_) {
      less = true;
    }
    return less;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(Compare);
};

struct CopyableComparer
{
  CopyableComparer(Compare &compare) : compare_(compare) {}
  bool operator()(const ObLogicalOperator::PartInfo &l, const ObLogicalOperator::PartInfo &r)
  {
    return compare_(l, r);
  }
  Compare &compare_;
};

int ObLogicalOperator::explain_print_partitions(ObTablePartitionInfo &table_partition_info,
                                                char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const ObCandiTabletLocIArray &partitions =
    table_partition_info.get_phy_tbl_location_info().get_phy_part_loc_info_list();
  uint64_t ref_table_id = table_partition_info.get_phy_tbl_location_info().get_ref_table_id();
  uint64_t table_id = table_partition_info.get_table_id();
  const bool two_level = (share::schema::PARTITION_LEVEL_TWO
                          == table_partition_info.get_part_level());
  ObSEArray<ObLogicalOperator::PartInfo, 64> part_infos;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  const share::schema::ObTableSchema *table_schema = NULL;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(get_plan())
      || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())
      || OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())
      || OB_ISNULL(stmt = get_plan()->get_stmt())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("NULL pointer error", K(get_plan()), K(opt_ctx), K(schema_guard), K(ret));
  } else if (partitions.count() == 0) {
    // do nothing
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("fail to get index schema", K(ret), K(ref_table_id), K(table_id));
  }
  int64_t N = partitions.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    ObLogicalOperator::PartInfo part_info;
    const ObOptTabletLoc &part_loc = partitions.at(i).get_partition_location();
    if (is_virtual_table(ref_table_id)) {
      if (VirtualSvrPair::EMPTY_VIRTUAL_TABLE_TABLET_ID == part_loc.get_partition_id()) {

      } else {
        part_info.part_id_ = part_loc.get_partition_id();
        OZ(part_infos.push_back(part_info));
      }
    } else if (!table_schema->is_partitioned_table()) {
      part_info.part_id_ = 0;
      OZ(part_infos.push_back(part_info));
    } else {
      const ObTabletID &tablet_id = part_loc.get_tablet_id();
      if (table_schema->is_external_table()) {
        for (int64_t j = 0; OB_SUCC(ret) && j < table_schema->get_partition_num(); j++) {
          if (OB_ISNULL(table_schema->get_part_array())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("table shcema is invalid", K(ret));
          } else {
            ObPartition *partition = table_schema->get_part_array()[j];
            if (OB_ISNULL(partition)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error", K(ret));
            } else if (partition->get_part_id() == part_loc.get_partition_id()) {
              part_info.part_id_ = j;
            }
            LOG_TRACE("show external table partition", K(tablet_id), KPC(partition), K(partitions.at(i)), K(part_info));
          }
        }
        OZ(part_infos.push_back(part_info));
      } else {
        OZ(table_schema->get_part_idx_by_tablet(tablet_id, part_info.part_id_, part_info.subpart_id_));
        OZ(part_infos.push_back(part_info));
      }
      // if (OB_FAIL(ret) || part_info.part_id_ == OB_INVALID_INDEX) {
      //   //do nothing
      // } else if (!table_schema->is_external_table() || common::ObTabletID::INVALID_TABLET_ID == tablet_id.id()) {
      //   //do nothing
      // } else {
      // }
      LOG_TRACE("explain print partition", K(tablet_id), K(part_info), K(ref_table_id));
    }
  }
  if (OB_SUCC(ret)) {
    Compare cmp;
    lib::ob_sort(part_infos.begin(), part_infos.end(), CopyableComparer(cmp));
    if (OB_FAIL(ObLogicalOperator::explain_print_partitions(part_infos, two_level,
                                                            buf, buf_len, pos))) {
      LOG_WARN("Failed to print partitions");
    } else { }//do nothing
  }
  return ret;
}

int ObLogicalOperator::explain_print_partitions(
    const ObIArray<ObLogicalOperator::PartInfo> &part_infos,
    const bool two_level,
    char *buf,
    int64_t &buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t count = part_infos.count();
  if (OB_ISNULL(buf)
      || buf_len <=0
      || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments erro", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(BUF_PRINTF("partitions("))) {
    LOG_WARN("Failed to printf partitions", K(ret));
  } else if (0 == count){
    ret = BUF_PRINTF("nil");
  } else {
    bool continuous = false;
    bool cont_flag = false;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      cont_flag = false;
      const int64_t id = part_infos.at(idx).part_id_;
      if (!continuous) {
        if (idx < count -1) {
          const int64_t n_id = part_infos.at(idx + 1).part_id_;
          if (two_level) {
            const int64_t p_id = id;
            const int64_t s_id = part_infos.at(idx).subpart_id_;
            const int64_t np_id = n_id;
            const int64_t ns_id = part_infos.at(idx + 1).subpart_id_;
            if (p_id == np_id && s_id == ns_id - 1) {
              ret = BUF_PRINTF("p%lusp[%lu-", p_id, s_id);
              continuous = true;
            } else {
              ret = BUF_PRINTF("p%lusp%lu", p_id, s_id);
            }
          } else if (id == n_id -1) {
            ret = BUF_PRINTF("p[%lu-", id);
            continuous = true;
          } else {
            ret = BUF_PRINTF("p%lu", id);
          }
        } else if(two_level) {
          const int64_t p_id = id;
          const int64_t s_id = part_infos.at(idx).subpart_id_;
          ret = BUF_PRINTF("p%lusp%lu", p_id, s_id);
        } else {
          ret = BUF_PRINTF("p%lu", id);
        }
      } else {
        if (idx >= count -1) {
          continuous = false;
        } else if (two_level) {
          const int64_t n_id = part_infos.at(idx + 1).part_id_;
          const int64_t p_id = id;
          const int64_t s_id = part_infos.at(idx).subpart_id_;
          const int64_t np_id = n_id;
          const int64_t ns_id = part_infos.at(idx + 1).subpart_id_;
          if (p_id != np_id ||
              s_id != (ns_id - 1)) {
            continuous = false;
          }
        } else if (id != part_infos.at(idx + 1).part_id_ - 1) {
          continuous = false;
        } else { }

        if (continuous) {
          cont_flag = true;
        } else if (two_level) {
          const int64_t s_id = part_infos.at(idx).subpart_id_;
          ret = BUF_PRINTF("%lu]", s_id);
        } else {
          ret = BUF_PRINTF("%lu]", id);
        }
      }
      if (!cont_flag && !continuous && (idx < count -1)) {
        ret = BUF_PRINTF(", ");
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = BUF_PRINTF(")");
  }
  return ret;
}

int ObLogicalOperator::allocate_granule_pre(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

int ObLogicalOperator::allocate_granule_post(AllocGIContext &ctx)
{
	int ret = OB_SUCCESS;
	UNUSED(ctx);
	return ret;
}

int ObLogicalOperator::px_pipe_blocking_pre(ObPxPipeBlockingCtx &ctx)
{
  typedef ObPxPipeBlockingCtx::OpCtx OpCtx;
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  OpCtx *op_ctx = static_cast<OpCtx *>(traverse_ctx_);
  OpCtx *child_op_ctx = NULL;
  if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL traverse ctx", K(ret));
  }
  for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
    if (NULL == get_child(i)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL child", K(ret));
    } else if (OB_ISNULL(child_op_ctx = static_cast<OpCtx *>(
        get_child(i)->traverse_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL traverse ctx", K(ret));
    } else {
      child_op_ctx->dfo_depth_ = op_ctx->dfo_depth_;
      child_op_ctx->out_.set_exch(op_ctx->out_.is_exch() && !is_block_input(i));
    }
  }
  return ret;
}

int ObLogicalOperator::px_pipe_blocking_post(ObPxPipeBlockingCtx &ctx)
{
  typedef ObPxPipeBlockingCtx::OpCtx OpCtx;
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  OpCtx *op_ctx = static_cast<OpCtx *>(traverse_ctx_);
  if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL traverse ctx", K(ret));
  } else if (op_ctx->dfo_depth_ < 0) {
    // nothing to do, the current operator is not processed by PX
  } else {
    bool got_in_exch = false;
    int64_t child_dfo_cnt = 0;
    int64_t max_dfo_child_idx = -1;
    for (int64_t i = get_num_of_child() - 1; OB_SUCC(ret) && i >= 0; i--) {
      auto child = get_child(i);
      OpCtx *child_op_ctx = NULL;
      if (OB_ISNULL(child)
          || OB_ISNULL(child_op_ctx = static_cast<OpCtx *>(child->traverse_ctx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL child or NULL child traverse ctx", K(ret));
      } else {
        if (child_op_ctx->has_dfo_below_) {
          child_dfo_cnt += 1;
          max_dfo_child_idx = max(max_dfo_child_idx, i);
        } else {
        }
        if (child_op_ctx->in_.is_exch() && !is_block_input(i)) {
          if (child_dfo_cnt > 1 && !is_consume_child_1by1()) {
            if (child->get_type() == LOG_DISTINCT
                && static_cast<ObLogDistinct*>(child)->get_algo() ==  HASH_AGGREGATE
                && !static_cast<ObLogDistinct *>(child)->is_push_down()) {
              static_cast<ObLogDistinct*>(child)->set_block_mode(true);
              LOG_DEBUG("distinct block mode", K(lbt()));
            } else if (OB_FAIL(allocate_material(i))) {
              LOG_WARN("allocate material failed", K(ret));
            }
          } else if (!got_in_exch) {
            got_in_exch = true;
          }
        }
        if (OB_SUCC(ret)) {
          bool need_alloc = false;
          if (LOG_JOIN == child->get_type()) {
            ObLogJoin *join = static_cast<ObLogJoin*>(child);
            // why we need allocate material op for BLOCK operator HJ?
            // Shared hash join requires strong synchronization between threads
            // If thread 1 get data from SHJ, others will wait it finish processing
            // Now dfc server block channel of thread 1, dead lock is happending
            if (join->is_shared_hash_join() && OB_FAIL(need_alloc_material_for_shared_hj(*child, need_alloc))) {
              LOG_WARN("check need allocate material failed", K(ret));
            } else if (need_alloc) {
              OZ (allocate_material(i));
            }
          } else if (LOG_WINDOW_FUNCTION == child->get_type()) {
            /*
             Window function participators require sync responds from datahub between threads.
             Consider the following scene : (DOP = 2)

                THREAD 2 : TRANSMIT 2 ---> RECV 2 ---> WF 2       |---------|
                               |______                            |         |
                                      |                           | datahub |
                                      |                           |         |
                THREAD 1 : TRANSMIT 1 ---> RECV 1 ---> WF 1 ----> |---------|

             1. WF1 has already sent the first dop partition sets to the datahub.
                It is waiting for a response from datahub and thread 1 is blocked now.
             2. Datahub is waiting for the msg from WF2.
                It will respond WF1 after getting the msg from WF2.
             3. TRANSMIT2 is going to send datas to RECV1 and RECV2 through the DTL channel
                          in order to collect the first dop partition sets for WF2.
                WF2 will not send msg to datahub unless collect the first dop partition sets.
             4. But thread 1 is blocked and RECV1 is not processing.
                So TRANSMIT2 can not send datas to RECV1.
                Then DFC server blocks the DTL channel, dead lock happends.
            */
            ObLogWindowFunction *wf = static_cast<ObLogWindowFunction*>(child);
            if (wf->is_participator()
                && OB_FAIL(need_alloc_material_for_push_down_wf(*child, need_alloc))) {
              LOG_WARN("check need allocate material failed", K(ret));
            } else if (need_alloc) {
              OZ (child->allocate_material(0));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (LOG_SET == get_type() && !got_in_exch) {
        ObLogSet *set_op = static_cast<ObLogSet *>(this);
        bool is_union = ObSelectStmt::UNION == set_op->get_set_op();
        if (is_union && max_dfo_child_idx > 0) {
          got_in_exch = true;
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

int ObLogicalOperator::has_block_parent_for_shj(bool &has_non_block_shj)
{
  int ret = OB_SUCCESS;
  if (LOG_EXCHANGE == type_) {
    // don't recursive to do
  } else {
    if (LOG_JOIN == type_) {
      ObLogJoin *join = static_cast<ObLogJoin*>(this);
      if (join->is_shared_hash_join()) {
        has_non_block_shj = true;
      }
    }
    for (int64_t i = get_num_of_child() - 1; OB_SUCC(ret) && !has_non_block_shj && i >= 0; i--) {
      bool tmp_has_shj = false;
      auto child = get_child(i);
      if (OB_FAIL(child->has_block_parent_for_shj(tmp_has_shj))) {
        LOG_WARN("failed to process block parent", K(ret));
      } else if (tmp_has_shj) {
        if (!is_block_input(i)) {
          // need to find whether the parent is blocked
          has_non_block_shj = true;
          break;
        }
      }
    } // end for
  }
  return ret;
}

int ObLogicalOperator::check_has_temp_table_access(ObLogicalOperator *cur,
                                                   bool &has_temp_table_access)
{
  int ret = OB_SUCCESS;
  has_temp_table_access = false;
  ObLogicalOperator *first_child = NULL;
  if (NULL == cur) {
    /* do nothing */
  } else if (log_op_def::LOG_TEMP_TABLE_ACCESS == cur->get_type()) {
    has_temp_table_access = true;
  } else if (0 >= cur->get_num_of_child()) {
    /* do nothing */
  } else if (OB_ISNULL(first_child = cur->get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (OB_FAIL(SMART_CALL(check_has_temp_table_access(first_child, has_temp_table_access)))) {
    LOG_WARN("failed to check has temp table access", K(ret));
  }
  return ret;
}

int ObLogicalOperator::allocate_granule_nodes_above(AllocGIContext &ctx)
{
	int ret = OB_SUCCESS;
	bool partition_granule = false;
  bool has_temp_table_access = false;
  const ObDMLStmt *stmt = NULL;
  //  op    granule iterator
  //   |    ->    |
  //  other      op
  //              |
  //             other
	if (!ctx.alloc_gi_) {
		//do nothing
	} else if (OB_ISNULL(get_plan()) || OB_ISNULL(get_sharding()) ||
             OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_plan()), K(get_sharding()));
  } else if (!get_plan()->get_optimizer_context().get_temp_table_infos().empty() &&
             OB_FAIL(check_has_temp_table_access(this, has_temp_table_access))) {
    LOG_WARN("failed to check has temp table access", K(ret));
  } else if (has_temp_table_access || get_contains_fake_cte()) {
    // do not allocate granule nodes above temp table access now
    LOG_TRACE("do not allocate granule iterator due to temp table", K(get_name()));
  } else if (LOG_TABLE_SCAN != get_type()
             && LOG_JOIN != get_type()
             && LOG_SET != get_type()
             && LOG_GROUP_BY != get_type()
             && LOG_DISTINCT != get_type()
             && LOG_SUBPLAN_FILTER != get_type()
             && LOG_WINDOW_FUNCTION != get_type()
             && LOG_UPDATE != get_type()
             && LOG_DELETE != get_type()
             && LOG_INSERT != get_type()
             && LOG_MERGE != get_type()
             && LOG_FOR_UPD != get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Only special op can allocate a granule iterator", K(get_type()));
  } else {
    ObLogicalOperator *log_op = NULL;
    ObLogOperatorFactory &factory = get_plan()->get_log_op_factory();
    if (OB_ISNULL(log_op = factory.allocate(*(get_plan()), LOG_GRANULE_ITERATOR))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate exchange nodes", K(log_op));
    } else {
      ObLogGranuleIterator *gi_op = static_cast<ObLogGranuleIterator *>(log_op);
      if (NULL != get_parent()) {
        //check topN sort
        if (stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_3, COMPAT_VERSION_4_3_0,
                                                            COMPAT_VERSION_4_3_2) &&
            LOG_SORT == get_parent()->get_type()) {
          ObLogSort *parent = static_cast<ObLogSort*>(get_parent());
          if (parent->is_local_merge_sort() &&
              NULL != parent->get_topn_expr()) {
            gi_op->add_flag(GI_FORCE_PARTITION_GRANULE);
          }
        }
        bool found_child = false;
        for (int64_t i = 0; OB_SUCC(ret) && !found_child && i < get_parent()->get_num_of_child(); ++i) {
          if (get_parent()->get_child(i) == this) {
            get_parent()->set_child(i, gi_op);
            found_child = true;
          } else { /*do nothing*/ }
        }
        gi_op->set_parent(get_parent());
      } else { /*do nothing*/ }
      set_parent(gi_op);
      gi_op->set_child(first_child, this);
      gi_op->set_cost(get_cost());
      gi_op->set_card(get_card());
      gi_op->set_width(get_width());
      gi_op->set_parallel(get_parallel());
      gi_op->set_partition_count(ctx.partition_count_);
      gi_op->set_hash_part(ctx.hash_part_);
      gi_op->set_tablet_size(ctx.tablet_size_ > 0 ? ctx.tablet_size_ : OB_DEFAULT_TABLET_SIZE);

      if (ctx.is_in_pw_affinity_state()) {
        gi_op->add_flag(GI_AFFINITIZE);
        gi_op->add_flag(GI_PARTITION_WISE);
      }
      if (LOG_TABLE_SCAN == get_type()) {
        if (static_cast<ObLogTableScan*>(this)->is_text_retrieval_scan() || static_cast<ObLogTableScan*>(this)->is_vec_idx_scan()) {
          gi_op->add_flag(GI_FORCE_PARTITION_GRANULE);
        }
        if (static_cast<ObLogTableScan *>(this)->get_join_filter_info().is_inited_) {
          ObLogTableScan *table_scan = static_cast<ObLogTableScan*>(this);
          ObOpPseudoColumnRawExpr *tablet_id_expr = NULL;
          if (OB_FAIL(generate_pseudo_partition_id_expr(tablet_id_expr))) {
            LOG_WARN("fail alloc partition id expr", K(ret));
          } else {
            gi_op->set_tablet_id_expr(tablet_id_expr);
            gi_op->set_join_filter_info(table_scan->get_join_filter_info());
            ObLogJoinFilter *jf_create_op = gi_op->get_join_filter_info().log_join_filter_create_op_;
            jf_create_op->set_paired_join_filter(gi_op);
            gi_op->add_flag(GI_USE_PARTITION_FILTER);
          }
        }
      } else if (LOG_GROUP_BY == get_type()) {
        if (static_cast<ObLogGroupBy*>(this)->force_partition_gi()) {
          gi_op->add_flag(GI_PARTITION_WISE);
        }
      } else if (LOG_DISTINCT == get_type()) {
        if (static_cast<ObLogDistinct*>(this)->force_partition_gi()) {
          gi_op->add_flag(GI_PARTITION_WISE);
        }
      } else if (ctx.is_in_partition_wise_state()) {
        gi_op->add_flag(GI_PARTITION_WISE);
      } else { /*do nothing*/ }

      if (ctx.is_in_slave_mapping()) {
        gi_op->add_flag(GI_SLAVE_MAPPING);
      }

      if (OB_SUCC(ret) && LOG_TABLE_SCAN == get_type()
          && EXTERNAL_TABLE == static_cast<ObLogTableScan *>(this)->get_table_type()) {
        if (ctx.force_partition()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("external table do not support partition GI", K(ret));
        } else {
          gi_op->set_used_by_external_table();
        }
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

      if (OB_SUCC(ret) && is_plan_root()) {
        if (OB_FAIL(gi_op->get_output_exprs().assign(output_exprs_))) {
          LOG_WARN("failed to assign output exprs", K(ret));
        } else {
          gi_op->mark_is_plan_root();
          get_plan()->set_plan_root(gi_op);
          set_is_plan_root(false);
          output_exprs_.reuse();
        }
      } else { /*do nothing*/ }

      if (OB_SUCC(ret) && LOG_TABLE_SCAN == get_type()
          && static_cast<ObLogTableScan *>(this)->get_px_rf_info().is_inited_) {
        ObLogTableScan *table_scan = static_cast<ObLogTableScan *>(this);
        if (OB_FAIL(gi_op->set_px_rf_info(table_scan->get_px_rf_info()))) {
          LOG_WARN("failed to set px rf info", K(ret));
        }
      }
      LOG_TRACE("succ to allocate granule iterator nodes above operator", K(get_name()),
                K(get_cost()), K(get_card()), K(ctx.is_in_pw_affinity_state()),
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

int ObLogicalOperator::set_granule_nodes_affinity(AllocGIContext &ctx, int64_t child_index)
{
	int ret = OB_SUCCESS;
	UNUSED(ctx);
  if (LOG_EXCHANGE == get_type()) {
    // do not set affinity of another dfo.
  } else if (child_index >= get_num_of_child()) {
		ret = OB_ERR_UNEXPECTED;
		LOG_WARN("set granule affinity failed", K(ret));
	} else {
		ObLogicalOperator *child_op = get_child(child_index);
		if (LOG_GRANULE_ITERATOR == child_op->get_type()) {
			static_cast<ObLogGranuleIterator*>(child_op)->add_flag(GI_AFFINITIZE);
    } else {
        for (int64_t i = 0; i < child_op->get_num_of_child() && OB_SUCC(ret); i++) {
          if (OB_FAIL(child_op->set_granule_nodes_affinity(ctx, i))) {
            LOG_WARN("set granule affinity failed", K(ret));
          }
        }
    }
	}
	return ret;
}

int ObLogicalOperator::find_first_recursive(
    const log_op_def::ObLogOpType type, ObLogicalOperator *&op)
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

int ObLogicalOperator::allocate_gi_recursively(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  if (LOG_TABLE_SCAN == get_type()) {
    ObLogTableScan *scan = static_cast<ObLogTableScan*>(this);
    if (!scan->use_das()) {
      ctx.alloc_gi_ = true;
    } else {
      ctx.alloc_gi_ = false;
    }
    if (OB_FAIL(allocate_granule_nodes_above(ctx))) {
      LOG_WARN("allocate gi above table scan failed", K(ret));
    }
  } else if (is_fully_partition_wise()) {
    ctx.alloc_gi_ = true;
    if (OB_FAIL(allocate_granule_nodes_above(ctx))) {
      LOG_WARN("allocate gi above table scan failed", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
      if (OB_ISNULL(get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get_child(i) returns unexpected null", K(ret), K(i));
      } else if (LOG_EXCHANGE == get_child(i)->get_type()) {
        //do not allocate gi over a dfo bound.
      } else if (OB_FAIL(get_child(i)->allocate_gi_recursively(ctx))) {
        LOG_WARN("failed to bottom-up traverse operator", K(ret), K(get_name()));
      } else { /* Do nothing */ }
    }
  }
  return ret;
}

int ObLogicalOperator::pw_allocate_granule_pre(AllocGIContext &ctx)
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
    LOG_TRACE("no exchange above, do nothing");
  } else if (!ctx.is_in_partition_wise_state()
      && !ctx.is_in_pw_affinity_state()
      && is_partition_wise()) {
    ctx.set_in_partition_wise_state(this);
    LOG_TRACE("in find partition wise state", K(*this));
  }
  return ret;
}

int ObLogicalOperator::pw_allocate_granule_post(AllocGIContext &ctx)
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
    LOG_TRACE("no exchange above, do nothing", K(ctx));
  } else if (ctx.is_op_set_pw(this)) {
    // In partition-wise join case, when GI is above group by/window function with pw attribute,
    // it doesn't support rescan before,
    // so it allocates the GI just above tsc and set attribute gi_random.
    // However, at most one gi_random can be supported within a DFO,
    // so that won't work in the plan found in this bug.
    // Now we support GI rescan in partition_wise_state so we just push up the GI here in such case
    //
    if (ctx.is_in_partition_wise_state() && is_fully_partition_wise()) {
      ctx.alloc_gi_ = true;
      if (OB_FAIL(allocate_granule_nodes_above(ctx))) {
        LOG_WARN("allocate gi above table scan failed", K(ret));
      }
    } else if (ctx.is_in_partition_wise_state() || ctx.is_in_pw_affinity_state()) {
      if (OB_FAIL(allocate_gi_recursively(ctx))) {
        LOG_WARN("failed to allocate gi recursively", K(ret));
      }
    }
    IGNORE_RETURN ctx.reset_info();
  }
  return ret;
}

int ObLogicalOperator::allocate_startup_expr_post()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    if (OB_FAIL(allocate_startup_expr_post(i))) {
      LOG_WARN("failed to allocate startup expr post", K(i), K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_startup_expr_post(int64_t child_idx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = get_child(child_idx);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null child", K(ret));
  } else if (is_dml_operator() ||
            log_op_def::LOG_TEMP_TABLE_INSERT == get_type()) {
    //do nothing
  } else if (child->get_startup_exprs().empty()) {
    //do nothing
  } else {
    ObSEArray<ObRawExpr*, 4> non_startup_exprs, new_startup_exprs;
    ObIArray<ObRawExpr*> &startup_exprs = child->get_startup_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < startup_exprs.count(); ++i) {
      if (OB_ISNULL(startup_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (startup_exprs.at(i)->has_flag(CNT_ROWNUM)) {
        if (OB_FAIL(non_startup_exprs.push_back(startup_exprs.at(i)))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(new_startup_exprs.push_back(startup_exprs.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOptimizerUtil::append_exprs_no_dup(get_startup_exprs(), new_startup_exprs))) {
        LOG_WARN("failed to add startup exprs", K(ret));
      } else {
        //exchange out上面的startup filter保留，用于控制当前dfo提前终止
        bool mark_exchange_out = false;
        if (log_op_def::LOG_EXCHANGE == child->get_type()) {
          ObLogExchange *exchange_out = static_cast<ObLogExchange*>(child);
          if (exchange_out->is_px_producer()) {
            if (log_op_def::LOG_EXCHANGE == get_type()) {
              ObLogExchange *exchange_in = static_cast<ObLogExchange*>(this);
              if (!exchange_in->is_rescanable()) {
                mark_exchange_out = true;
              }
            }
          }
        }
        if (!mark_exchange_out) {
          if (OB_FAIL(child->get_startup_exprs().assign(non_startup_exprs))) {
            LOG_WARN("failed to assign exprs", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

bool AllocGIContext::is_in_partition_wise_state()
{
  return GIS_IN_PARTITION_WISE == state_;
};

void AllocGIContext::set_in_partition_wise_state(ObLogicalOperator *op_ptr)
{
  state_ = GIS_IN_PARTITION_WISE;
  pw_op_ptr_ = op_ptr;
}

bool AllocGIContext::is_in_affinity_state()
{
  return GIS_AFFINITY == state_;
};

void AllocGIContext::set_in_affinity_state(ObLogicalOperator *op_ptr)
{
  state_ = GIS_AFFINITY;
  pw_op_ptr_ = op_ptr;
}
bool AllocGIContext::is_op_set_pw(ObLogicalOperator *op_ptr)
{
  return pw_op_ptr_ == op_ptr;
}

bool AllocGIContext::try_set_out_partition_wise_state(ObLogicalOperator *op_ptr)
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
  if (GIS_IN_PARTITION_WISE == state_ || GIS_AFFINITY == state_) {
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
  alloc_gi_ = false;
}

AllocGIContext::GIState AllocGIContext::get_state()
{
  return state_;
}

// consistent with LogJoin/LogSet::allocate_granule_post, gi is not allocated in normal/affinity state.
bool AllocGIContext::managed_by_gi()
{
  return GIS_NORMAL != state_ && GIS_AFFINITY != state_;
}

bool AllocGIContext::is_in_pw_affinity_state()
{
  return GIS_PARTITION_WITH_AFFINITY == state_;
}

void ObLogicalOperator::set_parent(ObLogicalOperator *parent)
{
  if (parent) {
  LOG_TRACE("set parent",
           "op_name", this->get_name(),
           "parent_op_name", parent->get_name(),
           "before", parent_, "after", parent, K(lbt()));
  } else {
    LOG_TRACE("set parent",
             "op_name", this->get_name(),
             "before", parent_, "after", parent, K(lbt()));
  }
  parent_ = parent;
}

int ObLogicalOperator::allocate_material_node_above()
{
  int ret = OB_SUCCESS;
  /**
    1. Don't allocate another material node above a material node
    2. Don't allocate another material node behind a material node
    3. Don't allocate a material node above a trasnsmit node
  */
  if (log_op_def::LOG_MATERIAL == type_
      || (NULL != get_parent() && log_op_def::LOG_MATERIAL == get_parent()->type_)
      || (log_op_def::LOG_EXCHANGE == get_type()
          && static_cast<ObLogExchange *>(this)->is_producer())
      || (LOG_JOIN_FILTER == get_type()
          && static_cast<ObLogJoinFilter *>(this)->use_realistic_runtime_bloom_filter_size())) {
    /*do nothing*/
  } else if (NULL != get_parent()) {
    // If current node is a leaf node, allocate a material node between current and its parent
    bool found_child = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found_child && i < get_parent()->get_num_of_child(); ++i) {
      if (get_parent()->get_child(i) == this) {
        if (OB_FAIL(get_parent()->allocate_material(i))) {
          LOG_WARN("fail to allocate material", K(ret));
        }
        found_child = true;
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!found_child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not find self in parent child", K(ret), K(op_id_), K(get_op_name(type_)));
    }
  } else if (is_plan_root()) {
    // If current node is the root node, allocate a material node as new root
    ObLogicalOperator* top = this;
    if (OB_ISNULL(get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get unexpected null", K(ret), K(get_plan()));
    } else if (OB_FAIL(get_plan()->allocate_material_as_top(top))) {
      LOG_WARN("failed to allocate material as top", K(ret));
    } else if (OB_FAIL(top->get_output_exprs().assign(output_exprs_))) {
      LOG_WARN("failed to assign output exprs", K(ret));
    } else {
      set_parent(top);
      get_plan()->set_plan_root(top);
      set_is_plan_root(false);
      top->mark_is_plan_root();
      output_exprs_.reuse();
    }
  } else { /*do nothing*/ }

  return ret;
}

int ObLogicalOperator::allocate_monitoring_dump_node_above(uint64_t flags, uint64_t dst_op_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_plan()));
  } else if (LOG_EXCHANGE == get_type() &&
             (static_cast<ObLogExchange*>(this)->is_producer() ||
             (static_cast<ObLogExchange*>(this)->is_consumer() && static_cast<ObLogExchange*>(this)->get_is_remote()))) {
    // Do nothing.
  } else if (LOG_JOIN_FILTER == get_type()
             && static_cast<ObLogJoinFilter *>(this)->use_realistic_runtime_bloom_filter_size()) {
    // Do nothing.
  } else {
    ObLogicalOperator *log_op = NULL;
    ObLogOperatorFactory &factory = get_plan()->get_log_op_factory();
    if (OB_ISNULL(log_op = factory.allocate(*(get_plan()), LOG_MONITORING_DUMP))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to allocate monitoring dump nodes", K(log_op));
    } else {
      ObLogMonitoringDump *monitoring_dump = static_cast<ObLogMonitoringDump *>(log_op);
      if (NULL != get_parent()) {
        bool found_child = false;
        for (int64_t i = 0; OB_SUCC(ret) && !found_child && i < get_parent()->get_num_of_child(); ++i) {
          if (get_parent()->get_child(i) == this) {
            get_parent()->set_child(i, monitoring_dump);
            found_child = true;
          } else { /*do nothing*/ }
        }
        monitoring_dump->set_parent(get_parent());
      } else { /*do nothing*/ }
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

      if (OB_SUCC(ret) && is_plan_root()) {
        if (OB_FAIL(monitoring_dump->get_output_exprs().assign(output_exprs_))) {
          LOG_WARN("failed to assign output exprs", K(ret));
        } else {
          monitoring_dump->mark_is_plan_root();
          get_plan()->set_plan_root(monitoring_dump);
          set_is_plan_root(false);
          output_exprs_.reuse();
        }
      }
      LOG_TRACE("succ to allocate monitoring dump node above operator", K(get_name()),
                K(get_cost()), K(get_card()), K(flags));
    }
  }
  return ret;
}

int ObLogicalOperator::add_join_filter_info(
    ObLogicalOperator *join_filter_create_op,
    ObLogicalOperator *join_filter_use_op,
    ObRawExpr *join_filter_expr,
    RuntimeFilterType type)
{
  int ret = OB_SUCCESS;
  ObLogJoinFilter *join_filter_create = static_cast<ObLogJoinFilter *>(join_filter_create_op);
  ObLogJoinFilter *join_filter_use = static_cast<ObLogJoinFilter *>(join_filter_use_op);
  int64_t p2p_sequence_id = OB_INVALID_ID;
  if (OB_ISNULL(join_filter_create) || OB_ISNULL(join_filter_use) ||
      OB_ISNULL(join_filter_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null operator", K(join_filter_use), K(join_filter_create_op), K(ret));
  } else if (OB_FAIL(join_filter_use->add_join_filter_expr(join_filter_expr))) {
    LOG_WARN("fail to add join filter use", K(ret));
  } else if (OB_FAIL(join_filter_use->add_join_filter_type(type))) {
    LOG_WARN("fail to add join filter use", K(ret));
  } else if (OB_FAIL(join_filter_create->add_join_filter_type(type))) {
    LOG_WARN("fail to add join filter use", K(ret));
  } else if (OB_FAIL(PX_P2P_DH.generate_p2p_dh_id(p2p_sequence_id))) {
    LOG_WARN("fail to generate p2p dh id", K(ret));
  } else if (OB_FAIL(join_filter_create->add_p2p_sequence_id(p2p_sequence_id))) {
    LOG_WARN("fail to add join filter use", K(ret));
  } else if (OB_FAIL(join_filter_use->add_p2p_sequence_id(p2p_sequence_id))) {
    LOG_WARN("fail to add join filter use", K(ret));
  }
  return ret;
}

int ObLogicalOperator::add_partition_join_filter_info(
    ObLogicalOperator *join_filter_create_op,
    RuntimeFilterType type)
{
  int ret = OB_SUCCESS;
  int64_t p2p_sequence_id = OB_INVALID_ID;
  ObLogJoinFilter *join_filter_create = static_cast<ObLogJoinFilter *>(join_filter_create_op);
  if (OB_ISNULL(join_filter_create)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null operator", K(join_filter_create_op), K(ret));
  } else if (OB_FAIL(join_filter_create->add_join_filter_type(type))) {
    LOG_WARN("fail to add join filter use", K(ret));
  } else if (OB_FAIL(PX_P2P_DH.generate_p2p_dh_id(p2p_sequence_id))) {
    LOG_WARN("fail to generate p2p dh id", K(ret));
  } else if (OB_FAIL(join_filter_create->add_p2p_sequence_id(p2p_sequence_id))) {
    LOG_WARN("fail to add join filter use", K(ret));
  } else {
    ObLogJoin *join_op = static_cast<ObLogJoin*>(this);
    ObLogJoinFilter *join_filter_create = static_cast<ObLogJoinFilter *>(join_filter_create_op);
    if (LOG_JOIN != get_type()) {
    //do nothing
    } else {
      for (int i = 0; i < join_op->get_join_conditions().count(); ++i) {
        if (OB_FAIL(join_filter_create->get_is_null_safe_cmps().push_back(false))) {
          LOG_WARN("fail to push back is null safe flag", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::cal_runtime_filter_compare_func(
    ObLogJoinFilter *join_filter_use,
    ObRawExpr *join_use_expr,
    ObRawExpr *join_create_expr)
{
  int ret = OB_SUCCESS;
  common::ObCollationType cs_type = join_use_expr->get_collation_type();
  if (ob_is_string_or_lob_type(join_use_expr->get_result_type().get_type())
      || ob_is_string_or_lob_type(join_create_expr->get_result_type().get_type())) {
    if (OB_UNLIKELY(join_use_expr->get_collation_type() != join_create_expr->get_collation_type())) {
      // it's ok if one side of the join is a null type because null types can always be compared.
      if (ob_is_null(join_use_expr->get_result_type().get_type())) {
        // use create's cs_type
        cs_type = join_create_expr->get_collation_type();
      } else if (ob_is_null(join_create_expr->get_result_type().get_type())) {
        // use use's cs_type
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("collation type not match", K(join_use_expr->get_result_type()),
            K(join_create_expr->get_result_type()));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObCmpFunc cmp_func;
    const ObScale scale = ObDatumFuncs::max_scale(join_use_expr->get_result_type().get_scale(),
                                                  join_create_expr->get_result_type().get_scale());
    bool has_lob_header = is_lob_storage(join_use_expr->get_data_type())
                          || is_lob_storage(join_create_expr->get_data_type());
    cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
                                  join_use_expr->get_data_type(),
                                  join_create_expr->get_data_type(),
                                  lib::is_oracle_mode()? NULL_LAST : NULL_FIRST,
                                  cs_type,
                                  scale,
                                  lib::is_oracle_mode(),
                                  has_lob_header);
    if (OB_ISNULL(cmp_func.cmp_func_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cmp_func_ is null", K(join_use_expr->get_result_type()), K(join_create_expr->get_result_type()));
    } else {
      join_filter_use->add_join_filter_cmp_funcs(cmp_func.cmp_func_);
    }
  }
  return ret;
}

int ObLogicalOperator::generate_runtime_filter_expr(
      ObLogicalOperator *op,
      ObLogicalOperator *join_filter_create_op,
      ObLogicalOperator *join_filter_use_op,
      double join_filter_rate,
      RuntimeFilterType type)
{
  int ret = OB_SUCCESS;
  ObLogJoinFilter *join_filter_use = static_cast<ObLogJoinFilter *>(join_filter_use_op);
  ObLogJoinFilter *join_filter_create = static_cast<ObLogJoinFilter *>(join_filter_create_op);
  common::ObIArray<ObRawExpr *> &exprs = op->get_filter_exprs();
  ObRawExprFactory &expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
  common::ObIArray<ObRawExpr *> &join_use_exprs = join_filter_use->get_join_exprs();
  common::ObIArray<ObRawExpr *> &join_create_exprs = join_filter_create->get_join_exprs();
  ObOpRawExpr *join_filter_expr = NULL;
  ObSQLSessionInfo *session_info = get_plan()->get_optimizer_context().get_session_info();
  if (LOG_JOIN != get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid operator type", K(get_type()), K(ret));
  } else if (OB_UNLIKELY(join_use_exprs.count() != join_create_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join_use_exprs's size doesn't match join_create_exprs's size",
        K(join_use_exprs.count()), K(join_create_exprs.count()));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_RUNTIME_FILTER, join_filter_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else {
    join_filter_expr->set_runtime_filter_type(type);
    ObLogJoin *join_op = static_cast<ObLogJoin*>(this);
    for (int i = 0; i < join_use_exprs.count() && OB_SUCC(ret); ++i) {
      ObRawExpr *join_use_expr = join_use_exprs.at(i);
      ObRawExpr *join_create_expr = join_create_exprs.at(i);
      if (OB_ISNULL(join_use_expr) || OB_ISNULL(join_create_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join_use_expr or join_create_expr is NULL!", K(join_use_expr), K(join_create_expr));
      } else if (OB_FAIL(join_filter_expr->add_param_expr(join_use_exprs.at(i)))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else if (join_filter_use->get_join_filter_cmp_funcs().count() < join_use_exprs.count()
          && OB_FAIL(cal_runtime_filter_compare_func(join_filter_use, join_use_expr, join_create_expr))) {
        LOG_WARN("fail to cal compare function", K(ret));
      } else {
        if (i < join_op->get_join_conditions().count()
            && OB_NOT_NULL(join_op->get_join_conditions().at(i))) {
          if (T_OP_NSEQ == join_op->get_join_conditions().at(i)->get_expr_type()) {
            join_filter_expr->set_with_null_equal_cond(true);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(join_filter_expr->formalize(session_info))) {
        LOG_WARN("fail to formalize expr", K(ret));
      } else if (OB_FAIL(exprs.push_back(join_filter_expr))) {
        LOG_WARN("fail to to push back expr", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(get_plan()->get_predicate_selectivities(),
          ObExprSelPair(join_filter_expr, join_filter_rate)))) {
        LOG_WARN("fail to add join filter expr", K(ret));
      } else if (OB_FAIL(add_join_filter_info(join_filter_create_op,
          join_filter_use_op, join_filter_expr, type))) {
        LOG_WARN("fail to add join filter info", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::check_can_extract_query_range_by_rf(
    ObLogicalOperator *scan_node,
    ObLogicalOperator *join_filter_create_op,
    ObLogicalOperator *join_filter_use_op,
    bool &can_extract_query_range,
    ObIArray<int64_t> &prefix_col_idxs)
{
  // if the join key can reorder to prefix with range column, then it can be extract query range
  // output the prefix matched column sequence of join key

  // eg1:
  // join key: A.c1 = B.c1, range column of table B: (c2)
  // result: can_extract_query_range = false

  // eg2:
  // join key: A.c1 = B.c1, range column of table B: (c1)
  // result: can_extract_query_range = true, prefix_col_idxs = [0]

  // eg3:
  // join key: A.c1 = B.c1 && A.c2 = B.c2, range column of table B: (c2, c1),
  // result: can_extract_query_range = true, prefix_col_idxs = [1, 0]

  // eg4:
  // join key: A.c1 = B.c1 && A.c2 = B.c2 && A.c3=B.c3, range column of table B: (c3, c1),
  // result: can_extract_query_range = true, prefix_col_idxs = [2, 0],

  int ret = OB_SUCCESS;
  ObLogTableScan *scan_op = nullptr;
  if (OB_ISNULL(scan_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan node is null", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN != scan_node->get_type()) {
    // maybe temp table access op, can not extract query range
    can_extract_query_range = false;
  } else if (FALSE_IT(scan_op = static_cast<ObLogTableScan*>(scan_node))) {
  } else if (OB_ISNULL(scan_op->get_pre_graph()) ) {
    // for virtual table, the pre_query_range may be null,
    // can not extract query range by runtime filter
    can_extract_query_range = false;
  } else if (!scan_op->get_pre_graph()->is_precise_whole_range()) {
    // already has query range which is not whole range,
    // do not extract query range by runtime filter
    can_extract_query_range = false;
  } else if (OB_ISNULL(join_filter_create_op) || OB_ISNULL(join_filter_use_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObLogJoinFilter *join_filter_create = static_cast<ObLogJoinFilter *>(join_filter_create_op);
    ObLogJoinFilter *join_filter_use = static_cast<ObLogJoinFilter *>(join_filter_use_op);
    common::ObIArray<ObRawExpr *> &join_create_exprs = join_filter_create->get_join_exprs();
    common::ObIArray<ObRawExpr *> &join_use_exprs = join_filter_use->get_join_exprs();
    // for each range column, check if there is any join key matches
    const ObIArray<ColumnItem> &range_columns = scan_op->get_range_columns();
    for (int64_t i = 0; i < range_columns.count() && OB_SUCC(ret); ++i) {
      int64_t col_idx = OB_INVALID_ID;
      ObRawExpr *range_column_expr = range_columns.at(i).expr_;
      for (int j = 0; j < join_use_exprs.count() && OB_SUCC(ret); ++j) {
        ObRawExpr *join_use_expr = join_use_exprs.at(j);
        ObRawExpr *join_create_expr = join_create_exprs.at(j);
        if (OB_ISNULL(join_use_expr) || OB_ISNULL(join_create_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("join_use_expr is null or join_create_expr is null", K(ret), K(join_use_expr),
                   K(join_create_expr));
        }
        if (join_use_expr == range_column_expr
            && join_create_expr->get_result_type() == join_use_expr->get_result_type()) {
          // get the matched join key sequence
          col_idx = j;
          break;
        }
      }
      if (OB_INVALID_ID == col_idx) {
        // there is no join key match with this range column
        // stop traversing the remian range column
        break;
      } else {
        // if at least one join key prefix match with range columns
        // mark this runtime filter can_extract_query_range
        can_extract_query_range = true;
        if (OB_FAIL(prefix_col_idxs.push_back(col_idx))) {
          LOG_WARN("failed to push back col_idx");
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::try_prepare_rf_query_range_info(
    ObLogicalOperator *join_filter_create_op,
    ObLogicalOperator *join_filter_use_op,
    ObLogicalOperator *scan_node,
    const JoinFilterInfo &info)
{
  int ret = OB_SUCCESS;
  int64_t op_id = -1;
  bool can_extract_query_range = false;
  ObSEArray<int64_t, 4> prefix_col_idxs;

  if (OB_FAIL(check_can_extract_query_range_by_rf(
                 scan_node, join_filter_create_op, join_filter_use_op, can_extract_query_range,
                 prefix_col_idxs))) {
    LOG_WARN("failed to check can extrace query range by runtime filter");
  } else if (!can_extract_query_range) {
    // do nothing
  } else {
    // 1. set query range info to join filter, it will passed to runtime filter message later
    if (OB_ISNULL(scan_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan node is null", K(ret));
    } else if (log_op_def::LOG_TABLE_SCAN != scan_node->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan_node type dismatch", K(ret), K(scan_node->get_type()));
    } else {
      ObLogTableScan *table_scan = static_cast<ObLogTableScan *>(scan_node);
      op_id = table_scan->get_op_id();
      ObLogJoinFilter *join_filter_create = static_cast<ObLogJoinFilter *>(join_filter_create_op);
      int64_t range_column_cnt = table_scan->get_range_columns().count();
      join_filter_create->set_probe_table_id(info.ref_table_id_);
      join_filter_create->set_range_column_cnt(range_column_cnt);
      if (OB_FAIL(join_filter_create->set_rf_prefix_col_idxs(prefix_col_idxs))) {
        LOG_WARN("failed to set rf_prefix_col_idxs");
      }

      // 2 set runtime filter p2p id info to tabls scan op, it will passed to GI op later.
      if (OB_SUCC(ret)) {
        ObPxRFStaticInfo px_rf_info;
        ObSEArray<int64_t, 2> p2p_dh_ids;
        int64_t rf_count = join_filter_create->get_join_filter_types().count();
        for (int64_t i = 0; OB_SUCC(ret) && i < rf_count; ++i) {
          RuntimeFilterType type = join_filter_create->get_join_filter_types().at(i);
          if (RuntimeFilterType::IN == type || RuntimeFilterType::RANGE == type) {
            if (OB_FAIL(
                  p2p_dh_ids.push_back(join_filter_create->get_p2p_sequence_ids().at(i)))) {
              LOG_WARN("fail to push_back", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(px_rf_info.init(p2p_dh_ids, join_filter_create->is_shared_join_filter()))) {
            LOG_WARN("fail to init rf_query_range_info", K(ret));
          } else if (OB_FAIL(table_scan->set_px_rf_info(px_rf_info))) {
            LOG_WARN("fail to set rf_query_range_info to scan op", K(ret));
          }
        }
      }
    }
  }
  LOG_TRACE("check runtime filter can extract query range", K(ret), K(op_id),
            K(can_extract_query_range), K(prefix_col_idxs));
  return ret;
}

int ObLogicalOperator::create_runtime_filter_info(ObLogicalOperator *op,
    ObLogicalOperator *join_filter_create_op,
    ObLogicalOperator *join_filter_use_op,
    double join_filter_rate)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(join_filter_create_op) || OB_ISNULL(join_filter_use_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null operator", K(op), K(join_filter_create_op));
  } else if (get_plan()->get_optimizer_context().enable_in_filter() &&
      OB_FAIL(generate_runtime_filter_expr(op, join_filter_create_op,
      join_filter_use_op, join_filter_rate, RuntimeFilterType::IN))) {
    LOG_WARN("fail to generate range filter expr", K(ret));
  } else if (get_plan()->get_optimizer_context().enable_range_filter() &&
      OB_FAIL(generate_runtime_filter_expr(op, join_filter_create_op,
      join_filter_use_op, join_filter_rate, RuntimeFilterType::RANGE))) {
    LOG_WARN("fail to generate range filter expr", K(ret));
  } else if (get_plan()->get_optimizer_context().enable_bloom_filter() &&
      OB_FAIL(generate_runtime_filter_expr(op, join_filter_create_op,
      join_filter_use_op, join_filter_rate, RuntimeFilterType::BLOOM_FILTER))) {
    LOG_WARN("fail to generate range filter expr", K(ret));
  } else {
    ObLogJoin *join_op = static_cast<ObLogJoin*>(this);
    ObLogJoinFilter *join_filter_create = static_cast<ObLogJoinFilter *>(join_filter_create_op);
    if (LOG_JOIN != get_type()) {
    //do nothing
    } else {
      for (int i = 0; OB_SUCC(ret) && i < join_op->get_join_conditions().count(); ++i) {
        if (OB_FAIL(join_filter_create->get_is_null_safe_cmps().push_back(
              T_OP_NSEQ == join_op->get_join_conditions().at(i)->get_expr_type()))) {
          LOG_WARN("fail to push back is null safe flag", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::find_table_scan(ObLogicalOperator* root_op,
                                       uint64_t table_id,
                                       ObLogicalOperator* &scan_op,
                                       bool& table_scan_has_exchange,
                                       bool &has_px_coord)
{
  int ret = OB_SUCCESS;
  scan_op = NULL;
  if (OB_ISNULL(root_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null logical operator", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN == root_op->get_type()) {
    ObLogTableScan *scan = static_cast<ObLogTableScan *>(root_op);
    if (scan->get_table_id() == table_id) {
      scan_op = root_op;
    }
  } else if (log_op_def::LOG_TEMP_TABLE_ACCESS == root_op->get_type()) {
    ObLogTempTableAccess *scan = static_cast<ObLogTempTableAccess *>(root_op);
    if (scan->get_table_id() == table_id) {
      scan_op = root_op;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && NULL == scan_op && i < root_op->get_num_of_child(); ++i) {
      ObLogicalOperator *child = root_op->get_child(i);
      if (OB_FAIL(SMART_CALL(find_table_scan(child,
                                             table_id,
                                             scan_op,
                                             table_scan_has_exchange,
                                             has_px_coord)))) {
        LOG_WARN("failed to find operator", K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != scan_op) {
      if (log_op_def::LOG_EXCHANGE == root_op->get_type()) {
        table_scan_has_exchange = true;
        ObLogExchange* exch_op = static_cast<ObLogExchange*>(root_op);
        if (exch_op->is_px_coord()) {
          has_px_coord = true;
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::check_sort_key_can_pushdown_to_tsc_detail(
    ObLogicalOperator *op, ObRawExpr *candidate_sk_expr, uint64_t table_id,
    ObLogicalOperator *&scan_op, bool &find_table_scan, bool &table_scan_has_exchange,
    bool &has_px_coord)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null logical operator", K(ret));
  } else if (op->is_block_op()) {
  } else {
    switch (op->get_type()) {
      case LOG_GROUP_BY: {
        if (OB_FAIL(check_sort_key_can_pushdown_to_tsc_for_gby(
                op, candidate_sk_expr, table_id, scan_op, find_table_scan,
                table_scan_has_exchange, has_px_coord))) {
          LOG_WARN("failed to check group by");
        }
        break;
      }

      case LOG_WINDOW_FUNCTION: {
        if (OB_FAIL(check_sort_key_can_pushdown_to_tsc_for_winfunc(
                op, candidate_sk_expr, table_id, scan_op, find_table_scan, table_scan_has_exchange,
                has_px_coord))) {
          LOG_WARN("failed to check window function");
        }
        break;
      }

      case LOG_JOIN: {
        if (OB_FAIL(check_sort_key_can_pushdown_to_tsc_for_join(
                op, candidate_sk_expr, table_id, scan_op, find_table_scan, table_scan_has_exchange,
                has_px_coord))) {
          LOG_WARN("failed to check join");
        }
        break;
      }

      case LOG_DISTINCT:
      case LOG_EXCHANGE: {
        ObLogicalOperator *child = op->get_child(first_child);
        if (op->is_block_input(first_child)) {
        } else if (OB_FAIL(SMART_CALL(check_sort_key_can_pushdown_to_tsc_detail(
                       child, candidate_sk_expr, table_id, scan_op, find_table_scan,
                       table_scan_has_exchange, has_px_coord)))) {
          LOG_WARN("failed to check", K(ret));
        }
        break;
      }
      case LOG_TABLE_SCAN: {
        ObLogTableScan *scan = static_cast<ObLogTableScan *>(op);
        if (scan->get_table_id() == table_id) {
          bool has_exec_param = false;
          if (scan->use_das()) {
            LOG_TRACE("[TopN Filter]can not pushdown to das table scan");
          } else if (OB_FAIL(scan->has_exec_param(has_exec_param))) {
            LOG_WARN("failed to has_exec_param");
          } else if (has_exec_param) {
            LOG_TRACE("[TopN Filter]can not pushdown to tsc with exec param");
          } else {
            scan_op = op;
            find_table_scan = true;
          }
        }
        break;
      }
      case LOG_TEMP_TABLE_ACCESS: {
        ObLogTempTableAccess *scan = static_cast<ObLogTempTableAccess *>(op);
        if (scan->get_table_id() == table_id) {
          scan_op = op;
          find_table_scan = true;
        }
        break;
      }
      case LOG_SORT:
      case LOG_MATERIAL: {
        LOG_TRACE("[TopN Filter]can not pushdown across sort or material");
        break;
      }
      default: {
        break;
      }
    }
  }
  if (OB_SUCC(ret) && find_table_scan && log_op_def::LOG_EXCHANGE == op->get_type()) {
    table_scan_has_exchange = true;
    ObLogExchange *exch_op = static_cast<ObLogExchange *>(op);
    if (exch_op->is_px_coord()) {
      has_px_coord = true;
    }
  }
  return ret;
}

int ObLogicalOperator::check_sort_key_can_pushdown_to_tsc_for_gby(
    ObLogicalOperator *op, ObRawExpr *candidate_sk_expr, uint64_t table_id,
    ObLogicalOperator *&scan_op, bool &find_table_scan, bool &table_scan_has_exchange,
    bool &has_px_coord)
{
  int ret = OB_SUCCESS;
  ObLogGroupBy *group_by = static_cast<ObLogGroupBy *>(op);
  const common::ObIArray<ObRawExpr *> &group_by_exprs = group_by->get_group_by_exprs();
  // only the sort key is in group by expr, topn filter can pushdown
  if (!is_contain(group_by_exprs, candidate_sk_expr)) {
  } else {
    ObLogicalOperator *child = group_by->get_child(first_child);
    if (group_by->is_block_input(first_child)) {
    } else if (OB_FAIL(SMART_CALL(check_sort_key_can_pushdown_to_tsc_detail(
                   child, candidate_sk_expr, table_id, scan_op, find_table_scan,
                   table_scan_has_exchange, has_px_coord)))) {
      LOG_WARN("failed to check", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::check_sort_key_can_pushdown_to_tsc_for_winfunc(
    ObLogicalOperator *op, ObRawExpr *candidate_sk_expr, uint64_t table_id,
    ObLogicalOperator *&scan_op, bool &find_table_scan, bool &table_scan_has_exchange,
    bool &has_px_coord)
{
  int ret = OB_SUCCESS;
  ObLogWindowFunction *log_win_func = static_cast<ObLogWindowFunction *>(op);
  ObSEArray<ObRawExpr *, 4> partition_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < log_win_func->get_window_exprs().count(); ++i) {
    ObWinFunRawExpr *win_expr = log_win_func->get_window_exprs().at(i);
    if (OB_ISNULL(win_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("window function expr is null", K(ret));
    } else if (i == 0) {
      if (OB_FAIL(partition_exprs.assign(win_expr->get_partition_exprs()))) {
        LOG_WARN("failed to assign partition exprs", K(ret));
      }
    } else if (OB_FAIL(ObOptimizerUtil::intersect_exprs(
                   partition_exprs, win_expr->get_partition_exprs(), partition_exprs))) {
      LOG_WARN("failed to intersect expr array", K(ret));
    } else if (partition_exprs.empty()) {
      break;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (candidate_sk_expr->has_flag(CNT_WINDOW_FUNC)) {
    LOG_TRACE("[TopN Filter]countain win func, can not pushdown");
  } else if (!is_contain(partition_exprs, candidate_sk_expr)) {
    LOG_TRACE("[TopN Filter]contain none partition by expr, can not pushdown");
  } else {
    ObLogicalOperator *child = log_win_func->get_child(first_child);
    if (log_win_func->is_block_input(first_child)) {
    } else if (OB_FAIL(SMART_CALL(check_sort_key_can_pushdown_to_tsc_detail(
                   child, candidate_sk_expr, table_id, scan_op, find_table_scan,
                   table_scan_has_exchange, has_px_coord)))) {
      LOG_WARN("failed to check", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::check_sort_key_can_pushdown_to_tsc_for_join(
    ObLogicalOperator *op, ObRawExpr *candidate_sk_expr, uint64_t table_id,
    ObLogicalOperator *&scan_op, bool &find_table_scan, bool &table_scan_has_exchange,
    bool &has_px_coord)
{
  int ret = OB_SUCCESS;
  ObLogJoin *log_join = static_cast<ObLogJoin *>(op);
  ObJoinType join_type = log_join->get_join_type();
  if (FULL_OUTER_JOIN == join_type || CONNECT_BY_JOIN == join_type) {
    // can not pushdown
    LOG_TRACE("[TopN Filter]can not pushdown across full outer join and connnect by join");
  } else if (LEFT_OUTER_JOIN == join_type || LEFT_SEMI_JOIN == join_type
             || LEFT_ANTI_JOIN == join_type) {
    // output in left
    ObLogicalOperator *child = log_join->get_child(first_child);
    if (log_join->is_block_input(first_child)) {
      LOG_TRACE("[TopN Filter]can not pushdown across left join but block left");
    } else if (OB_FAIL(SMART_CALL(check_sort_key_can_pushdown_to_tsc_detail(
                   child, candidate_sk_expr, table_id, scan_op, find_table_scan,
                   table_scan_has_exchange, has_px_coord)))) {
      LOG_WARN("failed to check", K(ret));
    }
  } else if (RIGHT_OUTER_JOIN == join_type || RIGHT_ANTI_JOIN == join_type
             || RIGHT_SEMI_JOIN == join_type) {
    // output in right
    ObLogicalOperator *child = log_join->get_child(second_child);
    if (log_join->is_block_input(second_child)) {
      LOG_TRACE("[TopN Filter]can not pushdown across right join but block right");
    } else if (OB_FAIL(SMART_CALL(check_sort_key_can_pushdown_to_tsc_detail(
                   child, candidate_sk_expr, table_id, scan_op, find_table_scan,
                   table_scan_has_exchange, has_px_coord)))) {
      LOG_WARN("failed to check", K(ret));
    }
  } else if (INNER_JOIN == join_type) {
    for (int64_t i = 0; OB_SUCC(ret) && nullptr == scan_op && i < log_join->get_num_of_child();
         ++i) {
      ObLogicalOperator *child = log_join->get_child(i);
      if (log_join->is_block_input(i)) {
        continue;
      } else if (OB_FAIL(SMART_CALL(check_sort_key_can_pushdown_to_tsc_detail(
                     child, candidate_sk_expr, table_id, scan_op, find_table_scan,
                     table_scan_has_exchange, has_px_coord)))) {
        LOG_WARN("failed to check", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::check_sort_key_can_pushdown_to_tsc(
    ObLogicalOperator *root_op,
    common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> &candidate_sk_exprs,
    uint64_t table_id, ObLogicalOperator *&scan_op, bool &table_scan_has_exchange,
    bool &has_px_coord, int64_t &effective_sk_cnt)
{
  int ret = OB_SUCCESS;
  scan_op = nullptr;
  bool find_table_scan = true;
  if (OB_ISNULL(root_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null logical operator", K(ret));
  }
  for (int64_t i = 0; i < candidate_sk_exprs.count() && OB_SUCC(ret) && find_table_scan; ++i) {
    // for every expr, set to false and check
    find_table_scan = false;
    ObRawExpr *candidate_sk_expr = candidate_sk_exprs.at(i);
    if (OB_FAIL(check_sort_key_can_pushdown_to_tsc_detail(root_op, candidate_sk_expr, table_id,
                                                          scan_op, find_table_scan,
                                                          table_scan_has_exchange, has_px_coord))) {
      LOG_WARN("failed to check_sort_key_can_pushdown_to_tsc_detail");
    } else if (find_table_scan) {
      effective_sk_cnt = i + 1;
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_partition_join_filter(const ObIArray<JoinFilterInfo> &infos,
                                                      int64_t &filter_id)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *filter_create = NULL;
  ObLogJoinFilter *join_filter_create = NULL;
  ObLogOperatorFactory &factory = get_plan()->get_log_op_factory();
  CK(LOG_JOIN == get_type());
  DistAlgo join_dist_algo = static_cast<ObLogJoin*>(this)->get_join_distributed_method();
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null plan", K(ret));
  }
  for (int i = 0; i < infos.count() && OB_SUCC(ret); ++i) {
    filter_create = NULL;
    bool right_has_exchange = false;
    bool right_has_px_coord = false;
    const JoinFilterInfo &info = infos.at(i);
    ObLogTableScan *scan_op = NULL;
    ObLogicalOperator *node = NULL;
    if (!info.need_partition_join_filter_) {
      continue;
    } else if (OB_ISNULL(info.sharding_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null sharding", K(ret));
    } else if (OB_FAIL(find_table_scan(get_child(second_child),
                                       info.table_id_,
                                       node,
                                       right_has_exchange,
                                       right_has_px_coord))) {
      LOG_WARN("failed to find table scan", K(ret));
    } else if (right_has_px_coord) {
      //not support now
    } else if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table scan", K(ret));
    } else if (log_op_def::LOG_TABLE_SCAN != node->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect operator type", K(ret));
    } else if (FALSE_IT(scan_op = static_cast<ObLogTableScan*>(node))) {
    } else if (scan_op->is_part_join_filter_created()) {
      /*do nothing*/
    } else if (OB_ISNULL(filter_create = factory.allocate(*(get_plan()), LOG_JOIN_FILTER))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate exchange nodes", K(ret));
    } else if (OB_FAIL(add_partition_join_filter_info(filter_create, RuntimeFilterType::BLOOM_FILTER))) {
      // only bloom filter.
      LOG_WARN("fail to add parttition join filter info", K(ret));
    } else {
      bool is_shared_hash_join = static_cast<ObLogJoin*>(this)->is_shared_hash_join();
      ObPxBFStaticInfo bf_info;
      join_filter_create = static_cast<ObLogJoinFilter *>(filter_create);
      join_filter_create->set_is_create_filter(true);
      join_filter_create->set_filter_id(filter_id);
      join_filter_create->set_child(first_child, get_child(first_child));
      get_child(first_child)->set_parent(join_filter_create);
      join_filter_create->set_parent(this);
      set_child(first_child, join_filter_create);
      join_filter_create->set_filter_length(info.sharding_->get_part_cnt() * 2);
      join_filter_create->set_is_use_filter_shuffle(right_has_exchange);
      if ((is_partition_wise_ || DistAlgo::DIST_PARTITION_NONE == join_dist_algo) && !right_has_exchange) {
          join_filter_create->set_is_no_shared_partition_join_filter();
      } else {
          join_filter_create->set_is_shared_partition_join_filter();
      }
      if (get_plan()->get_optimizer_context().get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_3_5) &&
         DistAlgo::DIST_PARTITION_NONE == join_dist_algo) {
        ObLogicalOperator* child = get_child(first_child);
        if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null child", K(ret));
        } else if (OB_ISNULL(child=child->get_child(first_child))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null child", K(ret));
        } else if (OB_ISNULL(child=child->get_child(first_child))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null child", K(ret));
        } else if (LOG_EXCHANGE != child->get_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect child type", K(ret));
        } else {
          ObLogExchange *exch_op = static_cast<ObLogExchange*>(child);
          if (exch_op->get_calc_part_id_expr()->get_partition_id_calc_type() ==
              info.calc_part_id_expr_->get_partition_id_calc_type()) {
            join_filter_create->set_tablet_id_expr(exch_op->get_calc_part_id_expr());
          } else {
            join_filter_create->set_tablet_id_expr(info.calc_part_id_expr_);
          }
        }
      } else {
        join_filter_create->set_tablet_id_expr(info.calc_part_id_expr_);
      }
      OZ(join_filter_create->compute_property());
      OZ(bf_info.init(get_plan()->get_optimizer_context().get_session_info()->get_effective_tenant_id(),
          filter_id, GCTX.get_server_id(),
          join_filter_create->is_shared_join_filter(),
          info.skip_subpart_,
          join_filter_create->get_p2p_sequence_ids().at(0),
          right_has_exchange, join_filter_create));
      scan_op->set_join_filter_info(bf_info);
      scan_op->set_part_join_filter_created(true);
      filter_id++;
      for (int j = 0; j < info.lexprs_.count() && OB_SUCC(ret); ++j) {
        ObRawExpr *expr = info.lexprs_.at(j);
        CK(OB_NOT_NULL(expr));
        OZ(join_filter_create->get_join_exprs().push_back(expr));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_normal_join_filter(const ObIArray<JoinFilterInfo> &infos,
                                                   int64_t &filter_id)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *filter_create = NULL;
  ObLogicalOperator *filter_use = NULL;
  ObLogJoinFilter *join_filter_create = NULL;
  ObLogJoinFilter *join_filter_use = NULL;
  ObLogOperatorFactory &factory = get_plan()->get_log_op_factory();
  CK(LOG_JOIN == get_type());
  DistAlgo join_dist_algo = static_cast<ObLogJoin*>(this)->get_join_distributed_method();
  bool enable_bloom_filter = get_plan()->get_optimizer_context().enable_bloom_filter();
  bool can_join_filter_material = false;
  int64_t valied_join_filter_count = 0;
  int64_t extra_hash_count = 1; // at least one for hash join
  bool has_shared_join_filter = false;
  bool realistic_runtime_bloom_filter_size = !GCONF._preset_runtime_bloom_filter_size;
  if (realistic_runtime_bloom_filter_size && enable_bloom_filter
      && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_3_0) {
    can_join_filter_material = true;
  }
  if (OB_SUCC(ret)) {
    for (int i = 0; i < infos.count() && OB_SUCC(ret); ++i) {
      bool right_has_exchange = false;
      bool right_has_px_coord = false;
      filter_create = NULL;
      filter_use = NULL;
      join_filter_create = nullptr;
      join_filter_use = nullptr;
      const JoinFilterInfo &info = infos.at(i);
      ObLogicalOperator *node = NULL;
      if (!info.can_use_join_filter_) {
        //do nothing
      } else if (OB_ISNULL(filter_create = factory.allocate(*(get_plan()), LOG_JOIN_FILTER))
          || OB_ISNULL(filter_use = factory.allocate(*(get_plan()), LOG_JOIN_FILTER))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate join filter nodes", K(ret));
      } else if (OB_FAIL(find_table_scan(get_child(second_child),
                                         info.table_id_,
                                         node,
                                         right_has_exchange,
                                         right_has_px_coord))) {
        LOG_WARN("failed to find table scan", K(ret));
      } else if (right_has_px_coord) {
        //no support now
      } else if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table scan", K(ret));
      } else {
        join_filter_create = static_cast<ObLogJoinFilter *>(filter_create);
        join_filter_use = static_cast<ObLogJoinFilter *>(filter_use);
        join_filter_create->set_paired_join_filter(join_filter_use);
        join_filter_use->set_paired_join_filter(join_filter_create);
        join_filter_create->set_is_create_filter(true);
        join_filter_use->set_is_create_filter(false);
        join_filter_create->set_filter_id(filter_id);
        join_filter_use->set_filter_id(filter_id);
        join_filter_create->set_child(first_child, get_child(first_child));
        get_child(first_child)->set_parent(join_filter_create);
        join_filter_create->set_parent(this);
        set_child(first_child, join_filter_create);
        join_filter_create->set_filter_length(join_filter_create->
            get_child(first_child)->get_card());
        join_filter_use->set_filter_length(
            join_filter_create->get_child(first_child)->get_card());
        for (int64_t i = 0; OB_SUCC(ret) && i < node->get_parent()->get_num_of_child(); ++i) {
          if (node->get_parent()->get_child(i) == node) {
            node->get_parent()->set_child(i, join_filter_use);
	          break;
          }
        }
        if (OB_SUCC(ret)) {
          join_filter_use->set_parent(node->get_parent());
          node->set_parent(join_filter_use);
          join_filter_use->set_child(first_child, node);
          if (right_has_exchange) {
            join_filter_create->set_is_use_filter_shuffle(true);
            join_filter_use->set_is_use_filter_shuffle(true);
          }

          if ((DistAlgo::DIST_BC2HOST_NONE == join_dist_algo) || right_has_exchange) {
            join_filter_create->set_is_shared_join_filter();
            join_filter_use->set_is_shared_join_filter();
            has_shared_join_filter = true;
            int64_t max_wait_time_ms = 0;
            if (OB_FAIL(calc_rf_max_wait_time(node, info.filter_table_id_, max_wait_time_ms))) {
              LOG_WARN("failed to calc_rf_max_wait_time");
            } else {
              join_filter_use->set_rf_max_wait_time(max_wait_time_ms);
            }
          } else {
            join_filter_create->set_is_non_shared_join_filter();
            join_filter_use->set_is_non_shared_join_filter();
          }

          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(node->get_plan())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FALSE_IT(join_filter_use->set_plan(*node->get_plan()))) {
          } else if (node->is_plan_root()) {
            if (OB_FAIL(join_filter_use->get_output_exprs().assign(node->get_output_exprs()))) {
              LOG_WARN("failed to assign output exprs", K(ret));
            } else {
              join_filter_use->mark_is_plan_root();
              node->get_plan()->set_plan_root(join_filter_use);
              node->set_is_plan_root(false);
              node->get_output_exprs().reuse();
            }
          }
          OZ(join_filter_create->compute_property());
          OZ(join_filter_use->compute_property());
        }
        filter_id++;
        for (int j = 0; j < info.lexprs_.count() && OB_SUCC(ret); ++j) {
          ObRawExpr *lexpr = info.lexprs_.at(j);
          ObRawExpr *rexpr = info.rexprs_.at(j);
          CK(OB_NOT_NULL(lexpr) && OB_NOT_NULL(rexpr));
          OZ(join_filter_create->get_join_exprs().push_back(lexpr));
          OZ(join_filter_use->get_join_exprs().push_back(rexpr));
        }
        OZ(create_runtime_filter_info(node,
            join_filter_create, join_filter_use, info.join_filter_selectivity_));
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(try_prepare_rf_query_range_info(join_filter_create, join_filter_use,
                                                           node, info))) {
          LOG_WARN("failed to prepare_rf_query_range_info");
        }
        if (OB_SUCC(ret) && LOG_TABLE_SCAN == node->get_type()) {
          ObLogTableScan *scan = static_cast<ObLogTableScan*>(node);
          scan->set_use_column_store(info.use_column_store_);
        }

        if (OB_SUCC(ret) && can_join_filter_material) {
          valied_join_filter_count++;
          join_filter_create->get_jf_material_control_info().enable_material_ = true;
          if (join_filter_create->get_join_exprs().count()
              != static_cast<ObLogJoin *>(this)->get_equal_join_conditions().count()) {
            // if hash key is different, need to calculate new hash value
            extra_hash_count++;
          } else {
            // if hash key is same, reuse hash join's hash value
            join_filter_create->get_jf_material_control_info().hash_id_ = 0;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && can_join_filter_material && valied_join_filter_count > 0
      && OB_NOT_NULL(join_filter_create)) {
    /* for plan like below
                        Hash Join
                        /
               Join Filter Create (A, Material Controller)
                      /
            Join Filter Create (B)
                    /
          Join Filter Create (C)
      Only the top join filter create op (A) has the responsibility to material data and calculate
      hash value, join filter create op (B) join filter create op (C) is controled by
      join filter create op (A).

      During exection, only the join filter create op A will store the row, B and C will read the
      store to build join filter.

      The format of the materialed row:
                    extra                                          data
      hash(for hash join) hash(for A) hash(for B) hash(for C)   c1  c2  c3
    */

    join_filter_create->get_jf_material_control_info().is_controller_ = true;
    join_filter_create->get_jf_material_control_info().extra_hash_count_ = extra_hash_count;
    join_filter_create->get_jf_material_control_info().join_filter_count_ = valied_join_filter_count;
    join_filter_create->get_jf_material_control_info().each_sqc_has_full_data_ =
        DIST_BC2HOST_NONE == join_dist_algo;
    join_filter_create->get_jf_material_control_info().need_sync_row_count_ = has_shared_join_filter;

    ObLogJoin *hash_join = static_cast<ObLogJoin *>(this);
    hash_join->get_jf_material_control_info() = join_filter_create->get_jf_material_control_info();
    hash_join->get_jf_material_control_info().hash_id_ = 0;

    if (extra_hash_count > 1) {
      ObLogicalOperator *cur_op = hash_join;
      ObLogJoinFilter *cur_join_filter_create_op = nullptr;
      int64_t hash_id = 1;
      while (LOG_JOIN_FILTER == cur_op->get_child(first_child)->get_type()) {
        cur_op = cur_op->get_child(first_child);
        cur_join_filter_create_op = static_cast<ObLogJoinFilter *>(cur_op);
        if (cur_join_filter_create_op->get_jf_material_control_info().hash_id_ == 0) {
          // already reuse hash join's hash value, skip
          continue;
        } else {
          cur_join_filter_create_op->get_jf_material_control_info().hash_id_ = hash_id;
          hash_id++;
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::calc_rf_max_wait_time(const ObLogicalOperator *node, uint64_t table_id,
                                             int64_t &max_wait_time_ms)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t net_work_speed = 100 * 1000L; // Byte per ms, i.e 100MB/s
  static constexpr int64_t max_wait_time_ms_upper_limit = INT64_MAX >> 10; // in case of overflow
  double tsc_output_rows = 0;
  double totol_probe_table_size = 0;
  ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is null", K(ret));
  } else if (table_id != OB_INVALID_ID && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_3_0) {
    max_wait_time_ms = 10; // at least 10ms
    const OptTableMetas &table_metas = get_plan()->get_basic_table_metas();
    const OptTableMeta *table_meta = nullptr;
    if (OB_ISNULL(table_meta = table_metas.get_table_meta_by_table_id(table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_meta unexpected null", K(table_id));
    } else if (FALSE_IT(tsc_output_rows = table_meta->get_rows())) {
    } else {
      totol_probe_table_size = node->get_width() * tsc_output_rows; // Unit: Byte
      max_wait_time_ms = std::max((int64_t)(totol_probe_table_size / net_work_speed), max_wait_time_ms);
      max_wait_time_ms = std::min(max_wait_time_ms_upper_limit, max_wait_time_ms);// in case of overflow
      LOG_TRACE("print calc max wait ms", K(node->get_width()), K(tsc_output_rows),
                K(totol_probe_table_size), K(max_wait_time_ms));
    }
  }
  return ret;
}

int ObLogicalOperator::allocate_runtime_filter_for_hash_join(AllocBloomFilterContext &ctx)
{
  int ret = OB_SUCCESS;
  ObLogJoin *join_op = static_cast<ObLogJoin*>(this);
  if (LOG_JOIN != get_type()) {
    //do nothing
  } else if (join_op->get_join_filter_infos().empty()) {
    //do nothing
  } else if (OB_FAIL(allocate_partition_join_filter(join_op->get_join_filter_infos(),
                                                    ctx.filter_id_))) {
    LOG_WARN("fail to allocate partition join filter", K(ret));
  } else if (OB_FAIL(allocate_normal_join_filter(join_op->get_join_filter_infos(),
                                                 ctx.filter_id_))) {
    LOG_WARN("fail to allocate normal join filter", K(ret));
  }
  return ret;
}

int ObLogicalOperator::generate_pseudo_partition_id_expr(ObOpPseudoColumnRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObExprResType res_type;
  res_type.set_type(ObIntType);
  res_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);

  ObOptimizerContext &ctx = get_plan()->get_optimizer_context();
  if (OB_FAIL(ObRawExprUtils::build_op_pseudo_column_expr(ctx.get_expr_factory(),
                                                          T_PDML_PARTITION_ID,
                                                          "PARTITION_ID",
                                                          res_type,
                                                          expr))) {
    LOG_WARN("build operator pseudo column failed", K(ret));
  } else if (OB_FAIL(expr->formalize(ctx.get_session_info()))) {
    LOG_WARN("expr formalize failed", K(ret));
  }

  return ret;
}

int ObLogicalOperator::find_nested_dis_rescan(bool &find, bool nested)
{
  int ret = OB_SUCCESS;
  bool is_end = false;
  if (find) {
  } else if (LOG_EXCHANGE == get_type()) {
    if (nested) {
      find = true;
    }
    is_end = true;
  } else if (LOG_SUBPLAN_FILTER == get_type() ||
        (LOG_JOIN == get_type() &&
        JoinAlgo::NESTED_LOOP_JOIN == static_cast<ObLogJoin*>(this)->get_join_algo())) {
    nested = true;
  }
  if (!is_end) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
      ObLogicalOperator *child = NULL;
      if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(get_child(i)), K(ret));
      } else if (OB_FAIL(SMART_CALL(child->find_nested_dis_rescan(find, nested)))) {
        LOG_WARN("fail to find px for batch rescan", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::pre_check_can_px_batch_rescan(bool &find_nested_rescan,
                                                     bool &find_rescan_px,
                                                     bool nested) const
{
  int ret = OB_SUCCESS;
  if (find_nested_rescan) {
  } else if (LOG_EXCHANGE == get_type()) {
    const ObLogExchange *op = static_cast<const ObLogExchange *>(this);
    find_nested_rescan |= nested;
    find_rescan_px |= !nested && op->is_consumer() && !op->is_task_order();
  } else {
    nested = LOG_SUBPLAN_FILTER == get_type() ||
             (LOG_JOIN == get_type() &&
              JoinAlgo::NESTED_LOOP_JOIN == static_cast<const ObLogJoin*>(this)->get_join_algo());
    for (int64_t i = 0; !find_nested_rescan && OB_SUCC(ret) && i < get_num_of_child(); i++) {
      const ObLogicalOperator *child = NULL;
      if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(get_child(i)), K(ret));
      } else if (OB_FAIL(SMART_CALL(child->pre_check_can_px_batch_rescan(find_nested_rescan, find_rescan_px, nested)))) {
        LOG_WARN("fail to find px for batch rescan", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::check_subplan_filter_child_exchange_rescanable()
{
  int ret = OB_SUCCESS;
  // 对于subplan filter右孩子如果是onetime expr,
  // 则不需要将该子孩子exhange标记为px coord, 此时subpaln filter左孩子必须标记为px coord.
  // 对于subplan filter右孩子没有onetime expr的场景,
  // 从second child起, 均需要标记为px coord.
  // 右孩子们是否标记为px coord取决于是否需要rescan.
  // 左孩子是否标记为px, 取决于右子孩子是否有onetime expr, 原因是需要先获取expr值, 再下压至左孩子,
  // 详见issue
  if (OB_UNLIKELY(LOG_SUBPLAN_FILTER != type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operator type", K(ret), K(type_));
  } else {
    bool has_onetime_expr = false;
    ObLogSubPlanFilter *sub_plan_filter = static_cast<ObLogSubPlanFilter*>(this);
    for (int64_t i = get_num_of_child() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (i != 0) {
        if (sub_plan_filter->get_onetime_idxs().has_member(i)) {
          has_onetime_expr = true;
        } else if (OB_FAIL(get_child(i)->mark_child_exchange_rescanable())) {
          LOG_WARN("mark child ex-receive as px op fail", K(ret));
        } else { /*do nothing*/ }
      } else if (has_onetime_expr && OB_FAIL(get_child(i)->mark_child_exchange_rescanable())) {
        LOG_WARN("mark child ex-receive as px op fail", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogicalOperator::get_part_column_exprs(const uint64_t table_id,
                                             const uint64_t ref_table_id,
                                             ObIArray<ObRawExpr *> &part_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is nullptr");
  } else {
    ret = get_plan()->get_part_column_exprs(table_id, ref_table_id, part_exprs);
  }
  return ret;
}

int ObLogicalOperator::find_px_for_batch_rescan(const log_op_def::ObLogOpType op_type,
    const int64_t op_id, bool &find)
{
  int ret = OB_SUCCESS;
  if (LOG_SUBPLAN_FILTER == get_type() ||
      (LOG_JOIN == get_type() &&
       JoinAlgo::NESTED_LOOP_JOIN == static_cast<ObLogJoin*>(this)->get_join_algo())) {
    /*do nothing*/
  } else if (LOG_EXCHANGE == get_type()) {
    ObLogExchange *op = static_cast<ObLogExchange *>(this);
    if (op->is_rescanable() && !op->is_task_order()) {
      op->set_px_batch_op_id(op_id);
      op->set_px_batch_op_type(op_type);
      find = true;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
      ObLogicalOperator *child = NULL;
      if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(get_child(i)), K(ret));
      } else if (OB_FAIL(SMART_CALL(child->find_px_for_batch_rescan(op_type, op_id, find)))) {
        LOG_WARN("fail to find px for batch rescan", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::add_op_exprs(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(op_exprs_.push_back(expr))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogicalOperator::find_shuffle_join_filter(bool &find) const
{
  int ret = OB_SUCCESS;
  find = false;
  if (log_op_def::LOG_EXCHANGE == get_type()) {
    /* do nothing */
  } else if (log_op_def::LOG_JOIN == get_type()) {
    const ObIArray<JoinFilterInfo> &infos = static_cast<const ObLogJoin*>(this)->get_join_filter_infos();
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < infos.count(); ++i) {
      find = !infos.at(i).in_current_dfo_;
    }
    const ObLogicalOperator *child = NULL;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < get_num_of_child(); i++) {
      if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(child));
      } else if (OB_FAIL(SMART_CALL(child->find_shuffle_join_filter(find)))) {
        LOG_WARN("failed to find shuffle join filter", K(ret));
      }
    }
  } else {
    const ObLogicalOperator *child = NULL;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < get_num_of_child(); i++) {
      if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(child));
      } else if (OB_FAIL(SMART_CALL(child->find_shuffle_join_filter(find)))) {
        LOG_WARN("failed to find shuffle join filter", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::get_pushdown_op(log_op_def::ObLogOpType op_type, const ObLogicalOperator *&op) const
{
  int ret = OB_SUCCESS;
  op = NULL;
  const ObLogicalOperator *child = NULL;
  if (get_type() == op_type) {
    op = this;
  } else if (get_num_of_child() < 1) {
    /* do nothing */
  } else if (LOG_EXCHANGE != get_type() && LOG_SORT != get_type()
             && LOG_GRANULE_ITERATOR != get_type()
             && LOG_TOPK != get_type()
             && LOG_MATERIAL != get_type()) {
    /* do nothing */
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(child));
  } else if (OB_FAIL(SMART_CALL(child->get_pushdown_op(op_type, op)))) {
    LOG_WARN("failed to check push down", K(ret));
  }
  return ret;
}

int ObLogicalOperator::need_alloc_material_for_shared_hj(ObLogicalOperator &curr_op, bool &need_alloc)
{
  int ret = OB_SUCCESS;
  need_alloc = false;
  ObLogicalOperator *parent = curr_op.get_parent();
  bool stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (nullptr == parent || LOG_EXCHANGE == curr_op.type_) {
    //do nothing
  } else {
    bool end_traverse = false;
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < parent->get_num_of_child(); ++i) {
      if (OB_ISNULL(parent->get_child(i))) {
        ret =  OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected got nullptr child", K(ret), K(i));
      } else if (parent->get_child(i) == &curr_op) {
        if (parent->is_block_input(i)) {
          end_traverse = true;
        }
        break;
      }
    }
    for (; OB_SUCC(ret) && !end_traverse && i < parent->get_num_of_child(); ++i) {
      if (OB_ISNULL(parent->get_child(i))) {
        ret =  OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected got nullptr child", K(ret), K(i));
      } else if (parent->get_child(i) == &curr_op) {
      } else if (parent->get_child(i)->is_exchange_allocated()) {
        end_traverse = true;
        need_alloc = true;
      }
    }
    if (OB_SUCC(ret) && !end_traverse) {
      OZ (need_alloc_material_for_shared_hj(*parent, need_alloc));
    }
  }
  return ret;
}

int ObLogicalOperator::need_alloc_material_for_push_down_wf(
    ObLogicalOperator &curr_op, bool &need_alloc)
{
  int ret = OB_SUCCESS;
  bool stack_overflow = false;
  int64_t child_num = curr_op.get_num_of_child();
  if (OB_FAIL(check_stack_overflow(stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (LOG_EXCHANGE == curr_op.type_) {
    need_alloc = true;
  } else if (0 == child_num) { // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !need_alloc && i < child_num; ++i) {
      if (OB_ISNULL(curr_op.get_child(i))) {
        ret =  OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected got nullptr child", K(ret), K(i));
      } else if (curr_op.is_block_input(i)) { // do nothing
      } else if (OB_FAIL(need_alloc_material_for_push_down_wf(*curr_op.get_child(i), need_alloc))) {
        LOG_WARN("check need_alloc_material_for_push_down_wf failed",
                 K(ret), K(curr_op), K(i), KPC(curr_op.get_child(i)));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::collect_batch_exec_param_pre(void* ctx)
{
  int ret = OB_SUCCESS;
  ObBatchExecParamCtx* param_ctx = static_cast<ObBatchExecParamCtx*>(ctx);
  if (OB_ISNULL(param_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (ObLogOpType::LOG_JOIN == get_type() &&
             NESTED_LOOP_JOIN == static_cast<ObLogJoin*>(this)->get_join_algo()) {
    if (OB_FAIL(param_ctx->params_idx_.push_back(param_ctx->exec_params_.count()))) {
      LOG_WARN("failed to push back params idx", K(ret));
    }
  } else if (ObLogOpType::LOG_SUBPLAN_FILTER == get_type()) {
    if (OB_FAIL(param_ctx->params_idx_.push_back(param_ctx->exec_params_.count()))) {
      LOG_WARN("failed to push back params idx", K(ret));
    }
  } else if (param_ctx->params_idx_.empty()) {
    /* do nothing */
  } else {
    ObSEArray<ObRawExpr*, 4> param_exprs;
    if (OB_FAIL(ObRawExprUtils::extract_params(get_filter_exprs(), param_exprs))) {
      LOG_WARN("extract params failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_params(get_startup_exprs(), param_exprs))) {
      LOG_WARN("extract params failed", K(ret));
    } else if (ObLogOpType::LOG_TABLE_SCAN == get_type() &&
               OB_FAIL(ObRawExprUtils::extract_params(static_cast<ObLogTableScan*>(this)->get_range_conditions(),
                                                      param_exprs))) {
      LOG_WARN("extract params failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); ++i) {
      ObRawExpr *expr = param_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (expr->has_flag(IS_DYNAMIC_PARAM)) {
        ObBatchExecParamCtx::ExecParam param(expr, branch_id_);
        if (OB_FAIL(param_ctx->exec_params_.push_back(param))) {
          LOG_WARN("failed to push back exec params", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogicalOperator::collect_batch_exec_param_post(void* ctx)
{
  int ret = OB_SUCCESS;
  ObBatchExecParamCtx* param_ctx = static_cast<ObBatchExecParamCtx*>(ctx);
  if (OB_ISNULL(param_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (ObLogOpType::LOG_SUBPLAN_FILTER == get_type()) {
    ObLogSubPlanFilter* filter_op = static_cast<ObLogSubPlanFilter*>(this);
    if (OB_FAIL(collect_batch_exec_param(ctx,
                                          filter_op->get_exec_params(),
                                          filter_op->get_above_pushdown_left_params(),
                                          filter_op->get_above_pushdown_right_params()))) {
      LOG_WARN("failed to collect batch exec param", K(ret));
    }
  } else if (ObLogOpType::LOG_JOIN == get_type() &&
             NESTED_LOOP_JOIN == static_cast<ObLogJoin*>(this)->get_join_algo()) {
    ObLogJoin* join_op = static_cast<ObLogJoin*>(this);
    if (OB_FAIL(collect_batch_exec_param(ctx,
                                         join_op->get_nl_params(),
                                         join_op->get_above_pushdown_left_params(),
                                         join_op->get_above_pushdown_right_params()))) {
      LOG_WARN("failed to collect batch exec param", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::pick_out_startup_filters()
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = get_plan();
  ObOptimizerContext *opt_ctx = NULL;
  ObArray<ObRawExpr *> filter_exprs;
  if (OB_ISNULL(plan) || OB_ISNULL(opt_ctx = &plan->get_optimizer_context())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("NULL pointer error", K(plan), K(opt_ctx), K(ret));
  } else if (OB_FAIL(filter_exprs.assign(filter_exprs_))) {
    LOG_WARN("assign filter exprs failed", K(ret));
  } else {
    filter_exprs_.reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); ++i) {
    ObRawExpr *qual = filter_exprs.at(i);
    if (OB_ISNULL(qual)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (qual->is_static_const_expr()) {
      if (OB_FAIL(startup_exprs_.push_back(qual))) {
        LOG_WARN("add filter expr failed", K(i), K(ret));
      } else { /* Do nothing */ }
    } else if (OB_FAIL(filter_exprs_.push_back(qual))) {
      LOG_WARN("add filter expr failed", K(i), K(ret));
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObLogicalOperator::check_contain_false_startup_filter(bool &contain_false)
{
  int ret = OB_SUCCESS;
  contain_false = false;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(get_plan()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < startup_exprs_.count(); ++i) {
    ObRawExpr *qual = startup_exprs_.at(i);
    if (OB_ISNULL(qual)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::check_is_static_false_expr(
        get_plan()->get_optimizer_context(), *qual, contain_false))) {
      LOG_WARN("failed to check is static false", K(ret));
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObLogicalOperator::collect_batch_exec_param(void* ctx,
                                                const ObIArray<ObExecParamRawExpr*> &exec_params,
                                                ObIArray<ObExecParamRawExpr *> &left_above_params,
                                                ObIArray<ObExecParamRawExpr *> &right_above_params)
{
  int ret = OB_SUCCESS;
  int64_t start = -1;
  ObBatchExecParamCtx* param_ctx = static_cast<ObBatchExecParamCtx*>(ctx);
  if (OB_ISNULL(param_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_UNLIKELY(param_ctx->params_idx_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params idx is null", K(ret));
  } else if (OB_FAIL(param_ctx->params_idx_.pop_back(start))) {
    LOG_WARN("failed to pop back idx", K(ret));
  } else if (OB_UNLIKELY(start > param_ctx->exec_params_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params idx is large than count", K(ret));
  } else {
    for (int64_t i = param_ctx->exec_params_.count() - 1; OB_SUCC(ret) && i >= start; --i) {
      const ObBatchExecParamCtx::ExecParam& param = param_ctx->exec_params_.at(i);
      if (!ObOptimizerUtil::find_item(exec_params, param.expr_)) {
        /* do nothing */
      } else if (OB_FAIL(param_ctx->exec_params_.remove(i))) {
        LOG_WARN("failed to remove params", K(ret));
      }
    }
    for (int64_t i = start; OB_SUCC(ret) && i < param_ctx->exec_params_.count(); ++i) {
      const ObBatchExecParamCtx::ExecParam& param = param_ctx->exec_params_.at(i);
      if (param.branch_id_ > branch_id_ &&
          OB_FAIL(right_above_params.push_back(static_cast<ObExecParamRawExpr*>(param.expr_)))) {
        LOG_WARN("failed to push back right params", K(ret));
      } else if (param.branch_id_ <= branch_id_ &&
                 OB_FAIL(left_above_params.push_back(static_cast<ObExecParamRawExpr*>(param.expr_)))) {
        LOG_WARN("failed to push back left params", K(ret));
      }
    }
  }
  return ret;
}

// Recursively search in all subexpressions
int ObLogicalOperator::find_rownum_expr_recursively(const uint64_t &count_op_id,
                                                    ObLogicalOperator *&rownum_op,
                                                    const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (!expr->has_flag(CNT_ROWNUM)) {
    /* expr do not have rownum, need not to search recursively, do nothing */
  } else if (expr->get_expr_type() == T_FUN_SYS_ROWNUM
             && static_cast<const ObSysFunRawExpr *>(expr)->get_op_id() == count_op_id) {
    rownum_op = this;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && NULL == rownum_op && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(
            find_rownum_expr_recursively(count_op_id, rownum_op, expr->get_param_expr(i))))) {
        LOG_WARN("fail to find rownum expr recursively", K(ret));
      }
    }
  }
  LOG_DEBUG("find_rownum_expr_recursively finished", K(expr->get_param_count()),
           K(expr->get_expr_type()), K(NULL == rownum_op));
  return ret;
}
int ObLogicalOperator::find_rownum_expr(const uint64_t &count_op_id, ObLogicalOperator *&rownum_op,
                                        const ObIArray<ObRawExpr *> &exprs)
{
  LOG_DEBUG("find_rownum_expr begin", K(exprs.count()), K(NULL == rownum_op));
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == rownum_op && i < exprs.count(); i++) {
    ObRawExpr *expr = exprs.at(i);
    ret = find_rownum_expr_recursively(count_op_id, rownum_op, expr);
    LOG_DEBUG(
        "find_rownum_expr_recursively done:", K(expr->get_expr_type()),
        K(NULL == rownum_op), K(i), K(expr->get_param_count()));
  }
  return ret;
}
// - output expr
// - join conditions: equal ("=")
// - join conditions: filter (">", "<", ">=", "<=")
int ObLogicalOperator::find_rownum_expr(const uint64_t &count_op_id, ObLogicalOperator *&rownum_op)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("find_rownum_expr debug: ", K(get_name()), K(count_op_id));
  if (OB_FAIL(find_rownum_expr(count_op_id, rownum_op, get_filter_exprs()))) {
    LOG_WARN("failure encountered during find rownum expr", K(ret));
  } else if (OB_FAIL(find_rownum_expr(count_op_id, rownum_op, get_output_exprs()))) {
    LOG_WARN("failure encountered during find rownum expr", K(ret));
  } else if (NULL == rownum_op && get_type() == log_op_def::LOG_JOIN) {
    ObLogJoin *join_op = dynamic_cast<ObLogJoin *>(this);
    // NO NPE check for join_op as it should NOT be nullptr
    if (OB_ISNULL(join_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join op is null", K(ret));
    } else if (OB_FAIL(
                 find_rownum_expr(count_op_id, rownum_op, join_op->get_other_join_conditions()))) {
      LOG_WARN("failure encountered during find rownum expr", K(ret));
    } else if (OB_FAIL(
                 find_rownum_expr(count_op_id, rownum_op, join_op->get_equal_join_conditions()))) {
      LOG_WARN("failure encountered during find rownum expr", K(ret));
    }
  }
  for (int64_t i = 0; NULL == rownum_op && OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_FAIL(SMART_CALL(get_child(i)->find_rownum_expr(count_op_id, rownum_op)))) {
      LOG_WARN("fail to find rownum expr", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::recursively_disable_alloc_op_above(AllocOpContext& ctx)
{
  int ret = OB_SUCCESS;
  ret = ctx.disabled_op_set_.set_refactored(op_id_);
  if (ret != OB_SUCCESS && ret != OB_HASH_EXIST) {
    LOG_WARN("set_refactored fail", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  LOG_DEBUG("disable alloc op above", K(op_id_), K(type_));
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_FAIL(SMART_CALL(get_child(i)->recursively_disable_alloc_op_above(ctx)))) {
      LOG_WARN("fail to disable alloc op above", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::gen_temp_op_id(AllocOpContext& ctx)
{
  int ret = OB_SUCCESS;
  op_id_ = ctx.next_op_id_++;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get_child(i) is null", K(ret), K(i));
    } else if (OB_FAIL(get_child(i)->gen_temp_op_id(ctx))) {
      LOG_WARN("fail to generate original op id", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::alloc_nodes_above(AllocOpContext& ctx, const uint64_t &flags)
{
  int ret = OB_SUCCESS;
  if (flags & ObAllocOpHint::OB_MATERIAL
      && !ctx.is_visited(op_id_, ObAllocOpHint::OB_MATERIAL)) {
    ret = ctx.disabled_op_set_.exist_refactored(op_id_);
    if (OB_HASH_EXIST == ret) {
      /*op cant not add material, skip*/
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(allocate_material_node_above())) {
        LOG_WARN("failed to allocate material above", K(ret));
      } else if (OB_FAIL(ctx.visit(op_id_, ObAllocOpHint::OB_MATERIAL))) {
        LOG_WARN("failed to visit alloc op", K(ret));
      }
    } else {
      LOG_WARN("exist_refactored fail", K(ret));
    }
  }
  if (OB_SUCC(ret)
        && ((flags & ObAllocOpHint::OB_MONITOR_STAT)
              || (flags & ObAllocOpHint::OB_MONITOR_TRACING))
             && !ctx.is_visited(op_id_, ObAllocOpHint::OB_MONITOR_STAT | ObAllocOpHint::OB_MONITOR_TRACING)) {
    if (OB_FAIL(allocate_monitoring_dump_node_above(flags, op_id_))) {
      LOG_WARN("failed to allocate monitoring dump above", K(ret));
    } else if (OB_FAIL(ctx.visit(op_id_, ObAllocOpHint::OB_MONITOR_STAT | ObAllocOpHint::OB_MONITOR_TRACING))) {
      LOG_WARN("failed to visit alloc op", K(ret));
    }
  }
  return ret;
}

int ObLogicalOperator::check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index)
{
  int ret = OB_SUCCESS;
  if (get_num_of_child() > 0) {
    if (LOG_GRANULE_ITERATOR == get_type() ||
        LOG_TEMP_TABLE_TRANSFORMATION == get_type() ||
        LOG_TEMP_TABLE_INSERT == get_type() ||
        LOG_SUBPLAN_SCAN == get_type() ||
        LOG_SUBPLAN_FILTER == get_type() ||
        LOG_MATERIAL == get_type() ||
        LOG_JOIN_FILTER == get_type() ||
        LOG_FOR_UPD == get_type() ||
        LOG_COUNT == get_type() ||
        LOG_LIMIT == get_type() ||
        LOG_STAT_COLLECTOR == get_type() ||
        LOG_OPTIMIZER_STATS_GATHERING == get_type() ||
        LOG_SELECT_INTO == get_type()) {
      used = false;
    } else {
      used = true;
    }
    if (LOG_TEMP_TABLE_TRANSFORMATION == get_type()) {
      inherit_child_ordering_index = get_num_of_child()-1;
    } else {
      inherit_child_ordering_index = first_child;
    }
  } else {
    used = false;
    inherit_child_ordering_index = -1;
  }
  return ret;
}

int ObLogicalOperator::open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_DECLARE_ARG)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
    if (OB_ISNULL(get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is null", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_block_op()) {
    // block operator is usually single-child, so it doesn't matter whether consume child 1by1 actually.
    if (is_consume_child_1by1()) {
      // open and close child one by one.
      for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
        child = get_child(i);
       if (OB_FAIL(SMART_CALL(child->open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_ARG)))) {
          LOG_WARN("open px resource analyze failed", K(ret));
        } else if (OB_FAIL(SMART_CALL(child->close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_ARG)))) {
          LOG_WARN("close px resource analyze failed", K(ret));
        }
      }
    } else {
      // open all then close all
      for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
        if (OB_FAIL(SMART_CALL(get_child(i)->open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_ARG)))) {
          LOG_WARN("open px resource analyze failed", K(ret));
        }
      }
      for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
        if (OB_FAIL(SMART_CALL(get_child(i)->close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_ARG)))) {
          LOG_WARN("close px resource analyze failed", K(ret));
        }
      }
    }
  } else if (is_consume_child_1by1()) {
    // open and close all block input. for example: first child of HASH JOIN, HASH SET(except UNION)
    int64_t child_idx = 0;
    for (; child_idx < get_num_of_child() && is_block_input(child_idx) && OB_SUCC(ret); child_idx++) {
      child = get_child(child_idx);
      if (OB_FAIL(SMART_CALL(child->open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_ARG)))) {
        LOG_WARN("open px resource analyze failed", K(ret));
      } else if (OB_FAIL(SMART_CALL(child->close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_ARG)))) {
        LOG_WARN("close px resource analyze failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(find_max_px_resource_child(OPEN_PX_RESOURCE_ANALYZE_ARG, child_idx))) {
      LOG_WARN("find max px resource child failed", K(ret));
    } else {
      for (; child_idx < get_num_of_child() && OB_SUCC(ret); child_idx++) {
        if (child_idx == max_px_thread_branch_ || child_idx == max_px_group_branch_) {
          // skip, open later
        } else if (OB_FAIL(SMART_CALL(get_child(child_idx)->open_px_resource_analyze(
                        OPEN_PX_RESOURCE_ANALYZE_ARG)))) {
          LOG_WARN("child open px resource analyze failed", K(ret));
        } else if (OB_FAIL(SMART_CALL(get_child(child_idx)->close_px_resource_analyze(
                                                              CLOSE_PX_RESOURCE_ANALYZE_ARG)))) {
          LOG_WARN("child close px resource analyze failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == max_px_thread_branch_ ||
                            OB_INVALID_INDEX == max_px_group_branch_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child with max parallel thread/group not found", K(ret));
      } else if (max_px_thread_branch_ >= get_num_of_child()) {
        // all inputs are block, do nothing.
      } else if (OB_FAIL(SMART_CALL(get_child(max_px_thread_branch_)->open_px_resource_analyze(
                                    OPEN_PX_RESOURCE_ANALYZE_ARG)))) {
        LOG_WARN("child open px resource analyze failed", K(ret));
      // open both child with max thread and child with max group. may be inaccurate, by design.
      } else if (max_px_group_branch_ != max_px_thread_branch_ &&
                OB_FAIL(SMART_CALL(get_child(max_px_group_branch_)->open_px_resource_analyze(
                                    OPEN_PX_RESOURCE_ANALYZE_ARG)))) {
        LOG_WARN("child open px resource analyze failed", K(ret));
      }
    }
  } else {
    // non block op and not consume child one by one, open all children. example: nlj, merge union distinct.
    for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(get_child(i)->open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_ARG)))) {
        LOG_WARN("open px resource analyze failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalOperator::close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_DECLARE_ARG)
{
  int ret = OB_SUCCESS;
  // close children that are opened and not closed in open_px_resource_analyze.
  ObLogicalOperator *child = NULL;
   if (is_block_op()) {
    // do nothing because all children have been closed.
  } else if (is_consume_child_1by1()) {
    if (max_px_thread_branch_ >= get_num_of_child()) {
      // all children are block input and have been closed, do nothing.
    } else if (OB_FAIL(SMART_CALL(get_child(max_px_thread_branch_)->close_px_resource_analyze(
                            CLOSE_PX_RESOURCE_ANALYZE_ARG)))) {
      LOG_WARN("child open px resource analyze failed", K(ret));
    // close both child with max thread and child with max group.
    } else if (max_px_group_branch_ != max_px_thread_branch_ &&
              OB_FAIL(SMART_CALL(get_child(max_px_group_branch_)->close_px_resource_analyze(
                            CLOSE_PX_RESOURCE_ANALYZE_ARG)))) {
      LOG_WARN("child open px resource analyze failed", K(ret));
    }
  } else {
    for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(get_child(i)->close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_ARG)))) {
        LOG_WARN("open px resource analyze failed", K(ret));
      }
    }
  }
  return ret;
}


/* search for child with max running thread/group count.
 *                                              NESTEDLOOP JOIN
 *                               UNION ALL                           PX4(dop=5)
 *                  HASH JOIN               PX3(dop=5)
 *          PX1(dop=10)       PX2(dop=2)
 * When nlj is outputting data, both union-all and PX4 are opened, the data of UNION-ALL may be from
 *   HASH JOIN or PX3, we need to know when the px work count reaches the maximum.
 * When the data of UNION-ALL is from HASH JOIN, the px worker count of the plan equals to PX2 + PX4 = 7.
 * When the data of UNION-ALL is from PX3, the px worker count of the plan equals to PX3 + PX4 = 10.
 * Consider that before UNION-ALL output data, PX1 has to be opened and closed,
  *  so the final expected_px_worker_cnt = max(PX1, max(PX2 + PX4, PX3 + PX4) = 10.
 * This function is to search for the child of UNION-ALL kept in open state when UNION-ALL is open.
 * In the above plan, the result is 1.
*/
int ObLogicalOperator::find_max_px_resource_child(OPEN_PX_RESOURCE_ANALYZE_DECLARE_ARG,
                                                  int64_t first_nonblock_child)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(first_nonblock_child >= get_num_of_child() - 1)) {
    max_px_thread_branch_ = first_nonblock_child;
    max_px_group_branch_ = first_nonblock_child;
    LOG_TRACE("[PxResAnaly] find max px resource child", K(get_op_id()), K(max_px_thread_branch_),
            K(max_px_group_branch_));
  } else if (OB_INVALID_INDEX == max_px_thread_branch_) {
    int64_t ori_thread_cnt = cur_parallel_thread_count;
    int64_t ori_group_cnt = cur_parallel_group_count;
    int64_t max_child_thread_cnt = -1;
    int64_t max_child_group_cnt = -1;
    ObLogicalOperator *child = NULL;
    bool append_map = false;
    for (int64_t i = first_nonblock_child; i < get_num_of_child() && OB_SUCC(ret); i++) {
      if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(child->open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_ARG)))) {
        LOG_WARN("child open px resource analyze failed", K(ret));
      } else {
        int64_t thread_inc = cur_parallel_thread_count - ori_thread_cnt;
        int64_t group_inc = cur_parallel_group_count - ori_group_cnt;
        if (thread_inc > max_child_thread_cnt) {
          max_child_thread_cnt = thread_inc;
          max_px_thread_branch_ = i;
          if (group_inc == max_child_group_cnt) {
            // make max_px_group_branch_ be equal to max_px_thread_branch_ if possible.
            max_px_group_branch_ = i;
          }
        }
        if (group_inc > max_child_group_cnt) {
          max_child_group_cnt = group_inc;
          max_px_group_branch_ = i;
          if (thread_inc == max_child_thread_cnt) {
            max_px_thread_branch_ = i;
          }
        }
        if (OB_FAIL(SMART_CALL(child->close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_ARG)))) {
          LOG_WARN("child close px resource analyze failed", K(ret));
        } else {
          OB_ASSERT(cur_parallel_thread_count == ori_thread_cnt);
          OB_ASSERT(cur_parallel_group_count == ori_group_cnt);
        }
      }
    }
    LOG_TRACE("[PxResAnaly] find max px resource child", K(get_op_id()), K(max_px_thread_branch_),
            K(max_px_group_branch_));
  }
  return ret;
}

int ObLogicalOperator::check_op_orderding_used_by_parent(bool &used)
{
  int ret = OB_SUCCESS;
  used = true;
  bool inherit_child_ordering = true;
  int64_t inherit_child_ordering_index = -1;
  ObLogicalOperator *parent = get_parent();
  ObLogicalOperator *child = this;
  while (OB_SUCC(ret) && NULL != child) {
    if (child->is_plan_root()) {
      ObLogPlan *plan = child->get_plan();
      const ObDMLStmt *stmt = NULL;
      if (OB_ISNULL(plan) || OB_ISNULL(stmt=plan->get_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null param", K(ret));
      } else if (0 == stmt->get_order_item_size()) {
        //do nothing
      } else {
        used = true;
        break;
      }
      if (NULL == parent) {
        break;
      }
    }
    if (OB_SUCC(ret) && NULL != parent) {
      if (OB_FAIL(parent->check_use_child_ordering(used, inherit_child_ordering_index))) {
        LOG_WARN("failed to check use child ordering", K(ret));
      } else if (OB_FALSE_IT(inherit_child_ordering = child == parent->get_child(inherit_child_ordering_index))) {
      } else if (used || !inherit_child_ordering) {
        break;
      } else {
        child = parent;
        parent = parent->get_parent();
      }
    }
  }
  return ret;
}

int ObLogicalOperator::check_contain_dist_das(const ObIArray<ObAddr> &exec_server_list,
                                              bool &contain_dist_das) const
{
  int ret = OB_SUCCESS;
  contain_dist_das = false;
  if (!get_contains_das_op()) {
    contain_dist_das = false;
  } else if (LOG_TABLE_SCAN == get_type() && static_cast<const ObLogTableScan*>(this)->use_das()) {
    if (1 != exec_server_list.count()
        || 1 != get_server_list().count()
        || exec_server_list.at(0) != get_server_list().at(0)) {
      contain_dist_das = true;
    }
  } else {
    ObLogicalOperator *child = NULL;
    for (int64_t i = 0; !contain_dist_das && OB_SUCC(ret) && i < get_num_of_child(); ++i) {
      if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is null", K(ret), K(i));
      } else if (OB_FAIL(SMART_CALL(child->check_contain_dist_das(exec_server_list, contain_dist_das)))) {
        LOG_WARN("failed to smart call check contain dist das", K(ret));
      }
    }
  }
  return ret;
}

bool ObLogicalOperator::is_parallel_more_than_part_cnt() const
{
  if (NULL == strong_sharding_) {
    return false;
  } else if (strong_sharding_->get_part_cnt() < 1) {
    return false;
  } else {
    return get_parallel() > strong_sharding_->get_part_cnt();
  }
}

int64_t ObLogicalOperator::get_part_cnt() const
{
  if (NULL == strong_sharding_) {
    return 0;
  } else {
    return strong_sharding_->get_part_cnt();
  }
}
