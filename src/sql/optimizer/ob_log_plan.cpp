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
#include "lib/utility/utility.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_server_locality_cache.h"
#include "share/stat/ob_opt_column_stat_cache.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_intersect_route_policy.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_log_join_filter.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_sequence.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_select_into.h"
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_log_del_upd.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_function_table.h"
#include "sql/optimizer/ob_log_json_table.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_log_temp_table_insert.h"
#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_log_temp_table_transformation.h"
#include "sql/optimizer/ob_px_resource_analyzer.h"
#include "common/ob_smart_call.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/optimizer/ob_log_err_log.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_del_upd.h"
#include "sql/optimizer/ob_log_insert_all.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/optimizer/ob_log_px_object_sample.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/optimizer/ob_update_log_plan.h"
#include "sql/optimizer/ob_insert_log_plan.h"
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/ob_optimizer_trace_impl.h"
#include "sql/optimizer/ob_explain_note.h"
#include "share/ob_lob_access_utils.h"
#ifdef OB_BUILD_SPM
#include "sql/spm/ob_spm_define.h"
#endif
#include "sql/optimizer/ob_log_values_table_access.h"
#include "sql/optimizer/ob_log_statistics_collector.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::sql::log_op_def;
using share::schema::ObTableSchema;
using share::schema::ObColumnSchemaV2;
using share::schema::ObSchemaGetterGuard;

#include "sql/optimizer/ob_join_property.map"
static const char *ExplainColumnName[] =
{
  "ID",
  "OPERATOR",
  "NAME",
  "EST. ROWS",
  "COST"
};

ObLogPlan::ObLogPlan(ObOptimizerContext &ctx, const ObDMLStmt *stmt)
  : optimizer_context_(ctx),
    allocator_(ctx.get_allocator()),
    stmt_(stmt),
    log_op_factory_(allocator_),
    candidates_(),
    group_replaced_exprs_(),
    stat_partition_id_expr_(nullptr),
    query_ref_(NULL),
    root_(NULL),
    sql_text_(),
    hash_value_(0),
    subplan_infos_(),
    outline_print_flags_(0),
    onetime_exprs_(),
    join_order_(NULL),
    id_order_map_allocer_(RELORDER_HASHBUCKET_SIZE,
                          ObWrapperAllocator(&allocator_)),
    bucket_allocator_wrapper_(&allocator_),
    relid_joinorder_map_(),
    join_path_set_allocer_(JOINPATH_SET_HASHBUCKET_SIZE,
                           ObWrapperAllocator(&allocator_)),
    join_path_set_(),
    recycled_join_paths_(),
    pred_sels_(),
    multi_stmt_rowkey_pos_(),
    equal_sets_(),
    max_op_id_(OB_INVALID_ID),
    is_subplan_scan_(false),
    is_parent_set_distinct_(false),
    is_rescan_subplan_(false),
    disable_child_batch_rescan_(false),
    temp_table_info_(NULL),
    const_exprs_(),
    hash_dist_info_(),
    insert_stmt_(NULL),
    basic_table_metas_(),
    update_table_metas_(),
    selectivity_ctx_(ctx, this, stmt),
    alloc_sfu_list_(),
    onetime_copier_(NULL),
    nonrecursive_plan_for_fake_cte_(NULL),
    need_accurate_cardinality_(false)
{
}

ObLogPlan::~ObLogPlan()
{
  if (NULL != join_order_) {
    join_order_->~ObJoinOrder();
    join_order_ = NULL;
  }

  for(int64_t i = 0; i< subplan_infos_.count(); ++i) {
    if (NULL != subplan_infos_.at(i)) {
      subplan_infos_.at(i)->~SubPlanInfo();
      subplan_infos_.at(i) = NULL;
    } else { /* Do nothing */ }
  }
}

void ObLogPlan::destory()
{
  if (NULL != onetime_copier_) {
    onetime_copier_->~ObRawExprCopier();
    onetime_copier_ = NULL;
  }
  group_replacer_.destroy();
  window_function_replacer_.destroy();
  gen_col_replacer_.destroy();
  onetime_replacer_.destroy();
  stat_gather_replacer_.destroy();
}

double ObLogPlan::get_optimization_cost()
{
  double opt_cost = 0.0;
  if (OB_NOT_NULL(root_)) {
    opt_cost = root_->get_cost();
  }
  return opt_cost;
}

int ObLogPlan::make_candidate_plans(ObLogicalOperator *top)
{
  int ret = OB_SUCCESS;
  candidates_.reuse();
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(candidates_.candidate_plans_.push_back(CandidatePlan(top)))) {
    LOG_WARN("push back error", K(ret));
  } else {
    candidates_.plain_plan_.first = top->get_cost();
    candidates_.plain_plan_.second = 0;
  }
  return ret;
}

int64_t ObLogPlan::to_string(char *buf,
                             const int64_t buf_len,
                             ExplainType type,
                             const ObExplainDisplayOpt &display_opt) const
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(type);
  return 0;
}

/*
 * 找出from items中对应的table items
 */
int ObLogPlan::get_from_table_items(const ObIArray<FromItem> &from_items,
                                    ObIArray<TableItem *> &table_items)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(stmt), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); i++) {
      const FromItem &from_item = from_items.at(i);
      TableItem *temp_table_item = NULL;
      if (!from_item.is_joined_) {
        //如果是基表或SubQuery
        temp_table_item = stmt->get_table_item_by_id(from_item.table_id_);
      } else {
        //如果是Joined table
        temp_table_item = stmt->get_joined_table(from_item.table_id_);
      }
      if (OB_FAIL(table_items.push_back(temp_table_item))) {
        LOG_WARN("failed to push back table item", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::get_table_ids(TableItem *table_item,
                            ObRelIds &table_ids)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(table_item) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item or stmt", K(ret));
  } else if (!table_item->is_joined_table()) {
    if (OB_FAIL(table_ids.add_member(stmt->get_table_bit_index(table_item->table_id_)))) {
      LOG_WARN("failed to add members", K(ret));
    }
  } else {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table_item);
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->single_table_ids_.count(); ++i) {
      if (OB_FAIL(table_ids.add_member(
                    stmt->get_table_bit_index(joined_table->single_table_ids_.at(i))))) {
        LOG_WARN("failed to add member", K(ret), K(joined_table->single_table_ids_.at(i)));
      } else { /* do nothing. */ }
    }
  }
  return ret;
}

int ObLogPlan::get_table_ids(const uint64_t table_id,
                             ObRelIds &rel_ids)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  int32_t idx = OB_INVALID_INDEX;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_INDEX ==
             (idx = stmt->get_table_bit_index(table_id)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table id", K(ret), K(table_id), KPC(stmt));
  } else if (OB_FAIL(rel_ids.add_member(idx))) {
    LOG_WARN("failed to add members", K(ret));
  }
  return ret;
}

int ObLogPlan::get_table_ids(const ObIArray<uint64_t> &table_ids,
                             ObRelIds &rel_ids)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
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

int ObLogPlan::get_table_ids(const ObIArray<TableItem*> &table_items,
                            ObRelIds& table_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    TableItem *item = table_items.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (OB_FAIL(get_table_ids(item, table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::get_table_ids(const ObIArray<ObRawExpr*> &exprs,
                            ObRelIds& table_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(table_ids.add_members(expr->get_relation_ids()))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::is_joined_table_filter(const ObIArray<TableItem*> &table_items,
                                      const ObRawExpr *expr,
                                      bool &is_filter)
{
  int ret = OB_SUCCESS;
  is_filter = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr or stmt", K(ret));
  } else if (expr->get_relation_ids().is_empty()) {
    //do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_filter && i < table_items.count(); ++i) {
      TableItem *item = table_items.at(i);
      ObRelIds joined_table_ids;
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null semi item", K(ret));
      } else if (!item->is_joined_table()) {
        //do nothing
      } else if (OB_FAIL(get_table_ids(item,
                                      joined_table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (joined_table_ids.is_superset(expr->get_relation_ids())) {
        is_filter = true;
      }
    }
  }
  return ret;
}

int ObLogPlan::find_inner_conflict_detector(const ObIArray<ConflictDetector*> &inner_conflict_detectors,
                                            const ObRelIds &rel_ids,
                                            ConflictDetector* &detector)
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
      //do nothing
    } else if (temp_detector->join_info_.table_set_.equal(rel_ids)) {
      detector = temp_detector;
    }
  }
  return ret;
}

/**
 * 检查两个join运算是否满足结合律
 * 如果是left outer join assoc left outer join
 * 需要满足第二个连接的条件拒绝第二张表的空值
 * 如果是full outer join assoc left outer join
 * 需要满足第二个连接的条件拒绝第二张表的空值
 * 如果是full outer join assoc full outer join
 * 需要满足第二个连接的条件拒绝第二张表的空值
 */
int ObLogPlan::satisfy_associativity_rule(const ConflictDetector &left,
                                          const ConflictDetector &right,
                                          bool &is_satisfy)

{
  int ret = OB_SUCCESS;
  if (!ASSOC_PROPERTY[left.join_info_.join_type_][right.join_info_.join_type_]) {
    is_satisfy = false;
  } else if ((LEFT_OUTER_JOIN == left.join_info_.join_type_ ||
              FULL_OUTER_JOIN == left.join_info_.join_type_) &&
             LEFT_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p23 reject null on A(e2)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                        right.join_info_.on_conditions_,
                        left.R_DS_.is_subset(right.L_DS_) ? left.R_DS_ : right.L_DS_,
                        is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ &&
             FULL_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p12、p23 reject null on A(e2)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                        left.join_info_.on_conditions_,
                        left.R_DS_.is_subset(right.L_DS_) ? left.R_DS_ : right.L_DS_,
                        is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    } else if (!is_satisfy) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                        right.join_info_.on_conditions_,
                        left.R_DS_.is_subset(right.L_DS_) ? left.R_DS_ : right.L_DS_,
                        is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else {
    is_satisfy = true;
  }
  LOG_TRACE("succeed to check assoc", K(left), K(right), K(is_satisfy));
  return ret;
}

/**
 * 检查两个join运算是否满足左交换律
 * 对于left outer join、full outer join
 * 需要满足额外的空值拒绝条件
 */
int ObLogPlan::satisfy_left_asscom_rule(const ConflictDetector &left,
                                        const ConflictDetector &right,
                                        bool &is_satisfy)

{
  int ret = OB_SUCCESS;
  if (!L_ASSCOM_PROPERTY[left.join_info_.join_type_][right.join_info_.join_type_]) {
    is_satisfy = false;
  } else if (LEFT_OUTER_JOIN == left.join_info_.join_type_ &&
             FULL_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p12 reject null on A(e1)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                          left.join_info_.on_conditions_,
                                          left.L_DS_,
                                          is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ &&
             LEFT_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p13 reject null on A(e1)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                          right.join_info_.on_conditions_,
                                          left.L_DS_,
                                          is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ &&
             FULL_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p12、p13 reject null on A(e1)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                          left.join_info_.on_conditions_,
                                          left.L_DS_,
                                          is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    } else if (!is_satisfy) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                                right.join_info_.on_conditions_,
                                                left.L_DS_,
                                                is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else {
    is_satisfy = true;
  }
  LOG_TRACE("succeed to check l-asscom", K(left), K(right), K(is_satisfy));
  return ret;
}

/**
 * 检查两个join运算是否满足右交换律
 * 对于left outer join、full outer join
 * 需要满足额外的空值拒绝条件
 */
int ObLogPlan::satisfy_right_asscom_rule(const ConflictDetector &left,
                                        const ConflictDetector &right,
                                        bool &is_satisfy)

{
  int ret = OB_SUCCESS;
  if (!R_ASSCOM_PROPERTY[left.join_info_.join_type_][right.join_info_.join_type_]) {
    is_satisfy = false;
  } else if (FULL_OUTER_JOIN == left.join_info_.join_type_ &&
             FULL_OUTER_JOIN == right.join_info_.join_type_) {
    //需要满足p12、p23 reject null on A(e3)
    if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                          left.join_info_.on_conditions_,
                                          right.R_DS_,
                                          is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    } else if (!is_satisfy) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_conditions(
                                                right.join_info_.on_conditions_,
                                                right.R_DS_,
                                                is_satisfy))) {
      LOG_WARN("failed to check is null reject conditions", K(ret));
    }
  } else {
    is_satisfy = true;
  }
  LOG_TRACE("succeed to check r-asscom", K(left), K(right), K(is_satisfy));
  return ret;
}

/**
 * 加入冲突规则left --> right
 * 简化规则：
 * A -> B, A -> C
 * 简化为：A -> (B,C)
 * A -> B, C -> B
 * 简化为：(A,C) -> B
 *
 */
int ObLogPlan::add_conflict_rule(const ObRelIds &left,
                                const ObRelIds &right,
                                ObIArray<std::pair<ObRelIds, ObRelIds>> &rules)
{
  int ret = OB_SUCCESS;
  ObRelIds *left_handle_rule = NULL;
  ObRelIds *right_handle_rule = NULL;
  bool find = false;
  for (int64_t i =0; !find && i < rules.count(); ++i) {
    if (rules.at(i).first.equal(left) && rules.at(i).second.equal(right)) {
      find = true;
    } else if (rules.at(i).first.equal(left)) {
      left_handle_rule = &rules.at(i).second;
    } else if (rules.at(i).second.equal(right)) {
      right_handle_rule = &rules.at(i).first;
    }
  }
  if (find) {
    //do nothing
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

int ObLogPlan::generate_conflict_rule(ConflictDetector *parent,
                                      ConflictDetector *child,
                                      bool is_left_child,
                                      ObIArray<std::pair<ObRelIds, ObRelIds>> &rules)
{
  int ret = OB_SUCCESS;
  bool is_satisfy = false;
  ObRelIds ids;
  if (OB_ISNULL(parent) || OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null detector", K(ret));
  } else if (child->is_redundancy_) {
    //do nothing
  } else if (is_left_child) {
    LOG_TRACE("generate left child conflict rule for ", K(*parent), K(*child));
    //check assoc(o^a, o^b)
    if (OB_FAIL(satisfy_associativity_rule(*child, *parent, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy assoc");
    } else if (OB_FAIL(ids.intersect(child->L_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      //CR += T(right(o^a)) -> T(left(o^a))
      if (OB_FAIL(add_conflict_rule(child->R_DS_, child->L_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else if (OB_FAIL(add_conflict_rule(child->R_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict1", K(rules));
      }
    } else {
      //CR += T(right(o^a)) -> T(left(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->R_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else if (OB_FAIL(add_conflict_rule(child->R_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict2", K(rules));
      }
    }
    //check l-asscom(o^a, o^b)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(satisfy_left_asscom_rule(*child, *parent, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy l-asscom");
    } else if (OB_FAIL(ids.intersect(child->R_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      //CR += T(left(o^a)) -> T(right(o^a))
      if (OB_FAIL(add_conflict_rule(child->L_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict3", K(rules));
      }
    } else {
      //CR += T(left(o^a)) -> T(right(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->L_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict4", K(rules));
      }
    }
  } else {
    LOG_TRACE("generate right child conflict rule for ", K(*parent), K(*child));
    //check assoc(o^b, o^a)
    if (OB_FAIL(satisfy_associativity_rule(*parent, *child, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy assoc");
    } else if (OB_FAIL(ids.intersect(child->R_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      //CR += T(left(o^a)) -> T(right(o^a))
      if (OB_FAIL(add_conflict_rule(child->L_DS_, child->R_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict5", K(rules));
      }
    } else {
      //CR += T(left(o^a)) -> T(right(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->L_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict6", K(rules));
      }
    }
    //check r-asscom(o^b, o^a)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(satisfy_right_asscom_rule(*parent, *child, is_satisfy))) {
      LOG_WARN("failed to check satisfy assoc", K(ret));
    } else if (is_satisfy) {
      LOG_TRACE("satisfy r-asscom");
    } else if (OB_FAIL(ids.intersect(child->L_DS_, child->join_info_.table_set_))) {
      LOG_WARN("failed to cal intersect table ids", K(ret));
    } else if (ids.is_empty()) {
      //CR += T(right(o^a)) -> T(left(o^a))
      if (OB_FAIL(add_conflict_rule(child->R_DS_, child->L_DS_, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict7", K(rules));
      }
    } else {
      //CR += T(right(o^a)) -> T(left(o^a)) n T(quals)
      if (OB_FAIL(add_conflict_rule(child->R_DS_, ids, rules))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      } else {
        LOG_TRACE("succeed to add condlict8", K(rules));
      }
    }
  }
  return ret;
}

int ObLogPlan::get_base_table_items(const ObDMLStmt *stmt,
                                    ObIArray<TableItem*> &base_tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
    TableItem *item = NULL;
    if (OB_FAIL(stmt->get_from_table(i, item))) {
      LOG_WARN("failed to get from table", K(ret), K(i));
    } else if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table item", K(ret));
    } else if (!item->is_joined_table()) {
      ret = base_tables.push_back(item);
    } else {
      JoinedTable *joined_table = static_cast<JoinedTable*>(item);
      for (int64_t j = 0; OB_SUCC(ret) && j < joined_table->single_table_ids_.count(); ++j) {
        TableItem *table = stmt->get_table_item_by_id(joined_table->single_table_ids_.at(j));
        ret = base_tables.push_back(table);
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
    SemiInfo *semi_info = NULL;
    if (OB_ISNULL(semi_info = stmt->get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null semi info", K(ret), K(i));
    } else {
      TableItem *table = stmt->get_table_item_by_id(semi_info->right_table_id_);
      ret = base_tables.push_back(table);
    }
  }
  return ret;
}

//1. 添加基本表的ObJoinOrder结构到base level
//2. 添加Semi Join的右支block到base level
//3. 条件下推到基表
//4. 初始化动态规划数据结构，即每层ObJoinOrders
//5. 生成第一级ObJoinOrder， 即单表路径
//6. 选择location
//7. 设置第一级ObJoinOrder的sharding info
//8. 依次进行下一层级的规划过程(generate_join_levels())
//9. 取出最后一级的ObJoinOrder，输出
int ObLogPlan::generate_join_orders()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObSEArray<ObRawExpr*, 8> quals;
  ObSEArray<TableItem*, 8> from_table_items;
  ObSEArray<TableItem*, 8> base_table_items;
  JoinOrderArray base_level;
  int64_t join_level = 0;
  common::ObArray<JoinOrderArray> join_rels;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(stmt), K(ret));
  } else if (OB_FAIL(get_from_table_items(stmt->get_from_items(), from_table_items))) {
    LOG_WARN("failed to get table items", K(ret));
  } else if (OB_FAIL(get_base_table_items(stmt, base_table_items))) {
    LOG_WARN("failed to flatten table items", K(ret));
  } else if (OB_FAIL(append(quals, stmt->get_condition_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(quals, get_pushdown_filters()))) {
    LOG_WARN("failed to append exprs", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pre_process_quals(from_table_items,
                                  stmt->get_semi_infos(),
                                  quals))) {
      LOG_WARN("failed to distribute special quals", K(ret));
    } else if (OB_FAIL(generate_base_level_join_order(base_table_items,
                                                      base_level))) {
      LOG_WARN("fail to generate base level join order", K(ret));
    } else if (OB_FAIL(init_function_table_depend_info(base_table_items))) {
      LOG_WARN("failed to init function table depend infos", K(ret));
    } else if (OB_FAIL(init_json_table_depend_info(base_table_items))) {
      LOG_WARN("failed to init json table depend infos", K(ret));
    } else if (OB_FAIL(init_lateral_table_depend_info(base_table_items))) {
      LOG_WARN("failed to init lateral table depend info", K(ret));
    } else if (OB_FAIL(generate_conflict_detectors(from_table_items,
                                                  stmt->get_semi_infos(),
                                                  quals,
                                                  base_level))) {
      LOG_WARN("failed to generate conflict detectors", K(ret));
    }  else {
      //初始化动规数据结构
      join_level = base_level.count(); //需要连接的层次数
      if (OB_UNLIKELY(join_level < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected join level", K(ret), K(join_level));
      } else if (OB_FAIL(join_rels.prepare_allocate(join_level))) {
        LOG_WARN("failed to prepare allocate join rels", K(ret));
      } else if (OB_FAIL(join_rels.at(0).assign(base_level))) {
        LOG_WARN("failed to assign base level join order", K(ret));
      } else if (OB_FAIL(prepare_ordermap_pathset(base_level))) {
        LOG_WARN("failed to prepare order map and path set", K(ret));
      }
    }
  }

  //生成第一级Array：单表路径
  OPT_TRACE_TITLE("GENERATE BASE PATH");
  for (int64_t i = 0; OB_SUCC(ret) && i < join_level; ++i) {
    if (OB_ISNULL(join_rels.at(0).at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join_rels_.at(0).at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(mock_base_rel_detectors(join_rels.at(0).at(i)))) {
      LOG_WARN("failed to mock base rel detectors", K(ret));
    } else {
      OPT_TRACE("create base path for ", join_rels.at(0).at(i));
      OPT_TRACE_BEGIN_SECTION;
      ret = join_rels.at(0).at(i)->generate_base_paths();
      OPT_TRACE_MEM_USED;
      OPT_TRACE_END_SECTION;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append(get_optimizer_context().get_deduce_info(),
                              join_rels.at(0).at(i)->get_deduce_info()))) {
      LOG_WARN("failed to append deduce info", K(ret));
    }
  }

  //枚举join order
  //如果有leading hint就在这里按leading hint指定的join order枚举,
  //如果根据leading hint没有枚举到有效join order，就忽略hint重新枚举。
  if (OB_SUCC(ret)) {
    OPT_TRACE_TITLE("BASIC TABLE STATISTICS");
    OPT_TRACE_STATIS(stmt, get_basic_table_metas());
    OPT_TRACE_TITLE("UPDATE TABLE STATS");
    OPT_TRACE_STATIS(stmt, get_update_table_metas());
    OPT_TRACE_TITLE("START GENERATE JOIN ORDER");
    if (OB_FAIL(init_bushy_tree_info(from_table_items))) {
      LOG_WARN("failed to init bushy tree infos", K(ret));
    } else if (OB_FAIL(init_width_estimation_info(stmt))) {
      LOG_WARN("failed to init width estimation info", K(ret));
    } else if (OB_FAIL(generate_join_levels_with_IDP(join_rels))) {
      LOG_WARN("failed to generate join levels with dynamic program", K(ret));
    } else if (join_rels.at(join_level - 1).count() < 1 &&
               OB_FAIL(generate_join_levels_with_orgleading(join_rels))) {
      LOG_WARN("failed to enum with greedy", K(ret));
    } else if (1 != join_rels.at(join_level - 1).count()) {
      ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
      LOG_WARN("No final JoinOrder generated",
                K(ret), K(join_level), K(join_rels.at(join_level -1).count()));
    } else if (OB_ISNULL(join_order_ = join_rels.at(join_level -1).at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null join order", K(ret), K(join_order_));
    } else if (OB_UNLIKELY(join_order_->get_interesting_paths().empty())) {
      ret = OB_ERR_NO_PATH_GENERATED;
      LOG_WARN("No final join path generated", K(ret), K(*join_order_));
    } else {
      LOG_TRACE("succeed to generate join order", K(ret));
      OPT_TRACE("SUCCEED TO GENERATE JOIN ORDER, try path count:",
                        join_order_->get_total_path_num(),
                        ",interesting path count:", join_order_->get_interesting_paths().count());
      OPT_TRACE_TIME_USED;
      OPT_TRACE_MEM_USED;
    }
  }
  return ret;
}

int ObLogPlan::pre_process_push_subq(ObIArray<ObRawExpr*> &quals)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  ObSEArray<ObRelIds, 4> connected_table_ids;
  bool has_outline_data = false;
  if (OB_ISNULL(query_ctx = get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected NULL", K(ret), K(query_ctx));
  } else if (OB_FAIL(get_connected_table_ids(quals, connected_table_ids))) {
    LOG_WARN("failed to get connected table ids", K(ret));
  } else {
    has_outline_data = query_ctx->get_query_hint().has_outline_data();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    ObRawExpr* expr = quals.at(i);
    bool force_push_subq = false;
    bool force_no_push_subq = false;
    bool need_push_subq = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null qual", K(ret), K(i));
    } else if (!expr->has_flag(CNT_SUB_QUERY)) {
      // do nothing
    } else if (OB_FAIL(check_push_subq_validity(expr, force_push_subq, force_no_push_subq))) {
      LOG_WARN("failed to check one push subq hint", K(ret), K(i), KPC(expr));
    } else if (force_push_subq || force_no_push_subq || has_outline_data) {
      // handle hint or outline
      if (force_push_subq && OB_FAIL(push_subq_exprs_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (OB_FAIL(check_subq_need_push(expr, connected_table_ids, need_push_subq))) {
      LOG_WARN("failed to check subquery expr need push", K(ret));
    } else if (need_push_subq && OB_FAIL(push_subq_exprs_.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::get_connected_table_ids(const ObIArray<ObRawExpr*> &quals,
                                       ObIArray<ObRelIds> &connected_table_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> join_cond_quals;
  ObSEArray<ObSEArray<TableItem*, 4>, 8> all_connected_tables;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    ObRawExpr *qual = quals.at(i);
    if (OB_ISNULL(qual)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null qual", K(ret));
    } else if (!qual->has_flag(IS_JOIN_COND)) {
      // do nothing
    } else if (OB_FAIL(join_cond_quals.push_back(qual))) {
      LOG_WARN("failed to push back qual", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTransformUtils::cartesian_tables_pre_split(stmt->get_table_items(),
                                                                  join_cond_quals,
                                                                  all_connected_tables))) {
    LOG_WARN("failed to split cartesian table", K(ret));
  } else if (OB_FAIL(connected_table_ids.prepare_allocate(all_connected_tables.count()))) {
    LOG_WARN("failed to prepare allocate connected_table_ids", K(ret), K(all_connected_tables.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_connected_tables.count(); ++i) {
    if (OB_FAIL(stmt->get_table_rel_ids(all_connected_tables.at(i), connected_table_ids.at(i)))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::check_push_subq_validity(const ObRawExpr *expr,
                                        bool &force_push,
                                        bool &force_no_push)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null expr", K(ret), K(expr));
  } else if (expr->is_query_ref_expr()) {
    const ObSelectStmt *ref_stmt = static_cast<const ObQueryRefRawExpr*>(expr)->get_ref_stmt();
    if (expr->get_ref_count() > 1) {
      force_no_push = true;
    } else if (OB_ISNULL(ref_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(ref_stmt));
    } else {
      const ObHint *push_subq_hint = ref_stmt->get_stmt_hint().get_normal_hint(T_PUSH_SUBQ);
      if (NULL != push_subq_hint) {
        force_push = push_subq_hint->is_enable_hint();
        force_no_push = push_subq_hint->is_disable_hint();
      }
    }
  } else if (!expr->has_flag(CNT_SUB_QUERY)) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !(force_push || force_no_push) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_push_subq_validity(expr->get_param_expr(i), force_push, force_no_push)))) {
        LOG_WARN("failed to check param expr push subq hint", K(ret), K(i), KPC(expr));
      }
    }
  }
  return ret;
}

int ObLogPlan::check_subq_need_push(ObRawExpr *expr,
                                    ObIArray<ObRelIds> &connected_table_ids,
                                    bool &need_push_subq)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  const ObColumnRefRawExpr *col_expr = NULL;
  const ObRawExpr *subq_expr = NULL;
  bool is_valid_pattern = false;
  bool has_other_quals = false;
  bool is_match_index = false;
  need_push_subq = false;
  if (OB_ISNULL(expr) || OB_ISNULL(query_ctx = get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr), K(query_ctx));
  } else if (!query_ctx->check_opt_compat_version(COMPAT_VERSION_4_2_5_BP4)) {
    // do nothing
  } else if (OB_FAIL(check_push_subq_expr_pattern(expr,
                                                  col_expr,
                                                  subq_expr,
                                                  is_valid_pattern))) {
    LOG_WARN("failed to check push subq expr pattern", K(ret));
  } else if (!is_valid_pattern) {
    // do nothing
  } else if (OB_FAIL(check_push_subq_has_other_quals(col_expr,
                                                     subq_expr,
                                                     connected_table_ids,
                                                     has_other_quals))) {
    LOG_WARN("failed to check push subq has other quals", K(ret));
  } else if (has_other_quals) {
    // do nothing
  } else if (OB_FAIL(check_push_subq_expr_match_index(expr, col_expr, is_match_index))) {
    LOG_WARN("failed to check push subq expr match index", K(ret));
  } else if (!is_match_index) {
    // do nothing
  } else {
    need_push_subq = true;
  }
  return ret;
}

int ObLogPlan::check_push_subq_expr_pattern(const ObRawExpr *expr,
                                            const ObColumnRefRawExpr *&col_expr,
                                            const ObRawExpr *&subq_expr,
                                            bool &is_valid_pattern)
{
  int ret = OB_SUCCESS;
  col_expr = NULL;
  subq_expr = NULL;
  is_valid_pattern = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (!IS_BASIC_CMP_OP(expr->get_expr_type())) {
    // do nothing
  } else if (T_OP_LIKE == expr->get_expr_type()) {
    if (OB_UNLIKELY(3 != expr->get_param_count())
             || OB_ISNULL(expr->get_param_expr(0))
             || OB_ISNULL(expr->get_param_expr(1))
             || OB_ISNULL(expr->get_param_expr(2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected like condition", K(ret), KPC(expr));
    } else if (expr->get_param_expr(0)->is_column_ref_expr()
               && expr->get_param_expr(1)->has_flag(CNT_SUB_QUERY)
               && expr->get_param_expr(2)->is_static_const_expr()) {
      col_expr = static_cast<const ObColumnRefRawExpr *>(expr->get_param_expr(0));
      subq_expr = expr->get_param_expr(1);
      is_valid_pattern = !col_expr->get_relation_ids().overlap(subq_expr->get_relation_ids());
    }
  } else if (OB_UNLIKELY(2 != expr->get_param_count())
             || OB_ISNULL(expr->get_param_expr(0))
             || OB_ISNULL(expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected simple or range condition", K(ret), KPC(expr));
  } else if (expr->get_param_expr(0)->is_column_ref_expr()
             && expr->get_param_expr(1)->has_flag(CNT_SUB_QUERY)) {
    col_expr = static_cast<const ObColumnRefRawExpr *>(expr->get_param_expr(0));
    subq_expr = expr->get_param_expr(1);
    is_valid_pattern = !col_expr->get_relation_ids().overlap(subq_expr->get_relation_ids());
  } else if (expr->get_param_expr(1)->is_column_ref_expr()
             && expr->get_param_expr(0)->has_flag(CNT_SUB_QUERY)) {
    col_expr = static_cast<const ObColumnRefRawExpr *>(expr->get_param_expr(1));
    subq_expr = expr->get_param_expr(0);
    is_valid_pattern = !col_expr->get_relation_ids().overlap(subq_expr->get_relation_ids());
  }
  return ret;
}

int ObLogPlan::check_push_subq_has_other_quals(const ObColumnRefRawExpr *col_expr,
                                               const ObRawExpr *subq_expr,
                                               ObIArray<ObRelIds> &connected_table_ids,
                                               bool &has_other_quals)
{
  int ret = OB_SUCCESS;
  has_other_quals = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < connected_table_ids.count(); ++i) {
    if (connected_table_ids.at(i).overlap(col_expr->get_relation_ids())) {
      if (connected_table_ids.at(i).overlap(subq_expr->get_relation_ids())) {
        has_other_quals = true;
      }
      break;
    }
  }
  return ret;
}

int ObLogPlan::check_push_subq_expr_match_index(ObRawExpr *expr,
                                                const ObColumnRefRawExpr *col_expr,
                                                bool &is_match_index)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObSEArray<uint64_t, 4> index_ids;
  is_match_index = false;
  if (OB_ISNULL(expr) || OB_ISNULL(col_expr) || OB_ISNULL(get_stmt())
      || OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(col_expr->get_table_id()))
      || OB_ISNULL(schema_guard = get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr), K(col_expr), K(get_stmt()), K(table_item), K(schema_guard));
  } else if (!table_item->is_basic_table()) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::get_valid_index_id(schema_guard,
                                                          get_stmt(),
                                                          table_item,
                                                          index_ids))) {
    LOG_WARN("failed to get valid index ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_match_index && i < index_ids.count(); ++i) {
      const ObTableSchema *index_schema = NULL;
      ObSEArray<uint64_t, 4> index_column_ids;
      uint64_t index_id = index_ids.at(i);
      if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
        LOG_WARN("failed to get index table schema", K(ret), K(index_id));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null index schema", K(ret));
      } else if (OB_FAIL(index_schema->get_rowkey_column_ids(index_column_ids))) {
        LOG_WARN("failed to get all column ids", K(ret));
      } else if (!index_column_ids.empty() && index_column_ids.at(0) == col_expr->get_column_id()) {
        is_match_index = true;
      }
    }
  }
  return ret;
}

int ObLogPlan::prepare_ordermap_pathset(const JoinOrderArray base_level)
{
  int ret = OB_SUCCESS;
  if (!relid_joinorder_map_.created() &&
      OB_FAIL(relid_joinorder_map_.create(RELORDER_HASHBUCKET_SIZE,
                                          &id_order_map_allocer_,
                                          &bucket_allocator_wrapper_))) {
    LOG_WARN("create hash map failed", K(ret));
  } else if (!join_path_set_.created() &&
             OB_FAIL(join_path_set_.create(JOINPATH_SET_HASHBUCKET_SIZE,
                                          &join_path_set_allocer_,
                                          &bucket_allocator_wrapper_))) {
    LOG_WARN("create hash set failed", K(ret));
  } else {
    int64_t join_level = base_level.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < join_level; ++i) {
      ObJoinOrder *join_order = base_level.at(i);
      if (OB_ISNULL(join_order)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid join order", K(ret));
      } else if (OB_FAIL(relid_joinorder_map_.set_refactored(join_order->get_tables(), join_order))) {
        LOG_WARN("failed to add basic join orders into map", K(ret));
      }
    }
  }
  return ret;
}

//生成单表ObJoinOrder结构, 并设置ObJoinOrder中table_set_
int ObLogPlan::generate_base_level_join_order(const ObIArray<TableItem*> &table_items,
                                              ObIArray<ObJoinOrder*> &base_level)
{
  int ret = OB_SUCCESS;
  ObJoinOrder *this_jo = NULL;
  //首先加入基表
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
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::split_or_quals(const ObIArray<TableItem*> &table_items,
                              ObIArray<ObRawExpr*> &quals)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> new_quals;
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    if (OB_FAIL(split_or_quals(table_items,
                               quals.at(i),
                               new_quals))) {
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

int ObLogPlan::split_or_quals(const ObIArray<TableItem*> &table_items,
                              ObRawExpr *qual,
                              ObIArray<ObRawExpr*> &new_quals)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  bool is_filter = false;
  if (OB_ISNULL(qual)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null qual", K(ret));
  } else if (T_OP_OR != qual->get_expr_type()) {
    //do nothing
  } else if (!qual->has_flag(CNT_SUB_QUERY) &&
              OB_FAIL(is_joined_table_filter(table_items, qual, is_filter))) {
    LOG_WARN("failed to check is joined table filter", K(ret));
  } else if (is_filter) {
    //joined table内部谓词等到下推on condition再拆分，否则会重复拆分or谓词
  } else {
    ObOpRawExpr *or_qual = static_cast<ObOpRawExpr*>(qual);
    for (int64_t j = 0; OB_SUCC(ret) && j < table_items.count(); ++j) {
      TableItem *table_item = table_items.at(j);
      table_ids.reuse();
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (OB_FAIL(get_table_ids(table_item, table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (!table_ids.overlap(or_qual->get_relation_ids())) {
        //do nothing
      } else if (qual->has_flag(CNT_SUB_QUERY) ||
                (!table_ids.is_superset(or_qual->get_relation_ids()))) {
        ret= try_split_or_qual(table_ids, *or_qual, new_quals);
      }
    }
  }
  return ret;
}

int ObLogPlan::pre_process_quals(const ObIArray<TableItem*> &table_items,
                                 const ObIArray<SemiInfo*> &semi_infos,
                                 ObIArray<ObRawExpr*> &quals)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> normal_quals;
  if (OB_FAIL(pre_process_push_subq(quals))) {
    LOG_WARN("failed to pre process push subq", K(ret));
  }
  //1. where conditions
  for (int64_t i = 0; OB_SUCC(ret) && i < quals.count(); ++i) {
    ObRawExpr *qual = quals.at(i);
    if (OB_FAIL(adjust_expr_with_onetime(qual))) {
      LOG_WARN("failed to try replace onetime subquery", K(ret));
    } else if (OB_ISNULL(qual)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null qual", K(ret));
    } else if (qual->has_flag(CNT_ROWNUM)) {
      ret = add_rownum_expr(qual);
    } else if (qual->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(split_or_quals(table_items, qual, quals))) {
        LOG_WARN("failed to split or quals", K(ret));
      } else if (ObOptimizerUtil::find_item(push_subq_exprs_, qual)) {
        ret = normal_quals.push_back(qual);
      } else {
        ret = add_subquery_filter(qual);
      }
    } else if (qual->is_const_expr()) {
      bool is_static_false = false;
      if (OB_FAIL(ObOptimizerUtil::check_is_static_false_expr(optimizer_context_, *qual, is_static_false))) {
        LOG_WARN("failed to check is static false", K(ret));
      } else if (is_static_false) {
        if (OB_FAIL(normal_quals.push_back(qual))) {
          LOG_WARN("failed to push back");
        }
      } else {
        if (OB_FAIL(add_startup_filter(qual))) {
          LOG_WARN("failed to add startup filter", K(ret));
        }
      }
    } else if (!qual->is_deterministic() && !qual->has_flag(CNT_ASSIGN_EXPR)) {
      ret = add_special_expr(qual);
    } else if (ObOptimizerUtil::has_hierarchical_expr(*qual)) {
      ret = normal_quals.push_back(qual);
    } else {
      ret = normal_quals.push_back(qual);
    }
    if (OB_SUCC(ret) && qual->has_flag(CNT_ONETIME) && !qual->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(add_subquery_filter(qual))) {
        LOG_WARN("failed to push back subquery filter", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(quals.assign(normal_quals))) {
      LOG_WARN("failed to assign quals", K(ret));
    }
  }
  //2. on conditions
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    ret = pre_process_quals(table_items.at(i));
  }
  //3. semi conditions
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
    ObRawExpr *expr = semi_info->semi_conditions_.at(i);
    if (OB_FAIL(adjust_expr_with_onetime(expr))) {
      LOG_WARN("failed to try replace onetime subquery", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(expr));
    } else if (!expr->has_flag(CNT_ONETIME) || expr->has_flag(CNT_SUB_QUERY)) {
      bool force_push_subq = false;
      bool force_no_push_subq = false;
      if (expr->has_flag(CNT_SUB_QUERY)
          && OB_FAIL(check_push_subq_validity(expr, force_push_subq, force_no_push_subq))) {
        LOG_WARN("failed to check one push subq hint", K(ret), K(i), KPC(expr));
      } else if (force_push_subq && OB_FAIL(push_subq_exprs_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (OB_FAIL(add_subquery_filter(expr))) {
      LOG_WARN("failed to add subquery filter", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::pre_process_quals(TableItem *table_item)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = static_cast<JoinedTable*>(table_item);
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table_item->is_joined_table()) {
    //do nothing
  } else if (OB_FAIL(SMART_CALL(pre_process_quals(joined_table->left_table_)))) {
    LOG_WARN("failed to distribute special quals", K(ret));
  } else if (OB_FAIL(SMART_CALL(pre_process_quals(joined_table->right_table_)))) {
    LOG_WARN("failed to distribute special quals", K(ret));
  } else {
    if (FULL_OUTER_JOIN == joined_table->joined_type_ &&
             !ObOptimizerUtil::has_equal_join_conditions(joined_table->join_conditions_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("full outer join without equal join conditions is not supported now", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "full outer join without equal join conditions");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->join_conditions_.count(); ++i) {
      ObRawExpr *expr = joined_table->join_conditions_.at(i);
      if (OB_FAIL(adjust_expr_with_onetime(expr))) {
        LOG_WARN("failed to try replace onetime subquery", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), K(expr));
      } else if (!expr->has_flag(CNT_ONETIME) || expr->has_flag(CNT_SUB_QUERY)) {
        bool force_push_subq = false;
        bool force_no_push_subq = false;
        if (expr->has_flag(CNT_SUB_QUERY)
            && OB_FAIL(check_push_subq_validity(expr, force_push_subq, force_no_push_subq))) {
          LOG_WARN("failed to check one push subq hint", K(ret), K(i), KPC(expr));
        } else if (force_push_subq && OB_FAIL(push_subq_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(add_subquery_filter(expr))) {
        LOG_WARN("failed to add subquery filter", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_conflict_detectors(const ObIArray<TableItem*> &table_items,
                                          const ObIArray<SemiInfo*> &semi_infos,
                                          ObIArray<ObRawExpr*> &quals,
                                          ObIArray<ObJoinOrder *> &baserels)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  ObSEArray<ConflictDetector*, 8> semi_join_detectors;
  ObSEArray<ConflictDetector*, 8> inner_join_detectors;
  LOG_TRACE("start to generate conflict detector", K(table_items), K(semi_infos), K(quals));
  conflict_detectors_.reuse();
  if (OB_FAIL(get_table_ids(table_items, table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(generate_inner_join_detectors(table_items,
                                                  quals,
                                                  baserels,
                                                  inner_join_detectors))) {
    LOG_WARN("failed to generate inner join detectors", K(ret));
  } else if (OB_FAIL(generate_semi_join_detectors(semi_infos,
                                                  table_ids,
                                                  baserels,
                                                  inner_join_detectors,
                                                  semi_join_detectors))) {
    LOG_WARN("failed to generate semi join detectors", K(ret));
  } else if (OB_FAIL(append(conflict_detectors_, semi_join_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append(conflict_detectors_, inner_join_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else {
    LOG_TRACE("succeed to generate confilct detectors",
                                        K(semi_join_detectors),
                                        K(inner_join_detectors));
  }
  return ret;
}

int ObLogPlan::generate_semi_join_detectors(const ObIArray<SemiInfo*> &semi_infos,
                                            ObRelIds &left_rel_ids,
                                            ObIArray<ObJoinOrder *> &baserels,
                                            const ObIArray<ConflictDetector*> &inner_join_detectors,
                                            ObIArray<ConflictDetector*> &semi_join_detectors)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> right_rel_ids;
  ObSEArray<ObRawExpr *, 4> right_quals;
  ObSEArray<ObRawExpr *, 4> semi_conds;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
    right_rel_ids.reuse();
    SemiInfo *info = semi_infos.at(i);
    ConflictDetector *detector = NULL;
    right_quals.reuse();
    semi_conds.reuse();
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null semi info", K(ret));
      //1. create conflict detector
    } else if (OB_FAIL(ConflictDetector::build_confict(get_allocator(), detector))) {
      LOG_WARN("failed to build conflict detector", K(ret));
    } else if (OB_ISNULL(detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null conflict detector", K(ret));
    } else if (OB_FAIL(detector->L_DS_.add_members(left_rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(stmt->get_table_rel_ids(info->right_table_id_, right_rel_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(detector->R_DS_.add_members(right_rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(semi_join_detectors.push_back(detector))) {
      LOG_WARN("failed to push back detector", K(ret));
    } else if (OB_FAIL(pushdown_semi_conditions(info, right_quals, semi_conds))) {
      LOG_WARN("failed to pushdown semi conditions", K(ret));
    } else if (OB_FAIL(distribute_quals(info->right_table_id_, right_quals, baserels))) {
      LOG_WARN("failed to distribute table filter", K(ret));
    } else {
      detector->join_info_.join_type_ = info->join_type_;
      // 2. add equal join conditions
      ObRawExpr *expr = NULL;
      for (int64_t j = 0; OB_SUCC(ret) && j < semi_conds.count(); ++j) {
        if (OB_ISNULL(expr = semi_conds.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(expr));
        } else if (OB_FAIL(detector->join_info_.table_set_.add_members(expr->get_relation_ids()))) {
          LOG_WARN("failed to add members", K(ret));
        } else if (OB_FAIL(detector->join_info_.on_conditions_.push_back(expr))) {
          LOG_WARN("failed to push back exprs", K(ret));
        } else if (expr->has_flag(IS_JOIN_COND) &&
                   OB_FAIL(detector->join_info_.equal_join_conditions_.push_back(expr))) {
          LOG_WARN("failed to push back qual", K(ret));
        }
      }
      // 3. add other infos to conflict detector
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(detector->L_TES_.intersect(detector->join_info_.table_set_,
                                                    detector->L_DS_))) {
        LOG_WARN("failed to generate L-TES", K(ret));
      } else if (OB_FAIL(detector->R_TES_.intersect(detector->join_info_.table_set_,
                                                    detector->R_DS_))) {
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
    // 4. 生成冲突规则
    for (int64_t j = 0; OB_SUCC(ret) && j < inner_join_detectors.count(); ++j) {
      if (OB_FAIL(generate_conflict_rule(detector,
                                         inner_join_detectors.at(j),
                                         true,
                                         detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_inner_join_detectors(const ObIArray<TableItem*> &table_items,
                                            ObIArray<ObRawExpr*> &quals,
                                            ObIArray<ObJoinOrder *> &baserels,
                                            ObIArray<ConflictDetector*> &inner_join_detectors)
{
  int ret = OB_SUCCESS;
  ObSEArray<ConflictDetector*, 8> outer_join_detectors;
  ObSEArray<ObRawExpr*, 4> all_table_filters;
  ObSEArray<ObRawExpr*, 4> table_filters;
  ObSEArray<ObRawExpr*, 4> redundant_quals;
  ObSEArray<ObRawExpr*, 4> all_quals;
  ObRelIds all_table_ids;
  ObRelIds table_ids;
  ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(query_ctx = get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_ctx));
  } else if (OB_FAIL(split_or_quals(table_items, quals))) {
    LOG_WARN("failed to split or quals", K(ret));
  } else if (OB_FAIL(get_table_ids(table_items, all_table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(deduce_redundant_join_conds(quals,
                                                 table_items,
                                                 redundant_quals))) {
    LOG_WARN("failed to deduce redundancy quals", K(ret));
  } else if (OB_FAIL(all_quals.assign(quals))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(append(all_quals, redundant_quals))) {
    LOG_WARN("failed to append array", K(ret));
  } else {
    OPT_TRACE("deduce redundant qual:", redundant_quals);
  }
  //1. 生成单个table item内部的冲突规则
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    table_filters.reuse();
    table_ids.reuse();
    if (OB_FAIL(get_table_ids(table_items.at(i), table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    }
    //找到table item的过滤谓词
    for (int64_t j = 0; OB_SUCC(ret) && j < quals.count(); ++j) {
      ObRawExpr *expr = quals.at(j);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!expr->is_deterministic() && !expr->has_flag(CNT_ASSIGN_EXPR)) {
        // don't pushdown condition which is not deterministic
        // do nothing
      } else if (!expr->get_relation_ids().is_subset(table_ids) ||
                 (expr->has_flag(CNT_SUB_QUERY) && !ObOptimizerUtil::find_item(push_subq_exprs_, expr))) {
        //do nothing
      } else if (expr->get_relation_ids().is_empty() &&
                 !expr->is_const_expr() &&
                 ObOptimizerUtil::find_item(all_table_filters, expr)) {
        //bug fix:48314988
      } else if (OB_FAIL(table_filters.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(all_table_filters.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_outer_join_detectors(table_items.at(i),
                                                table_filters,
                                                baserels,
                                                outer_join_detectors))) {
        LOG_WARN("failed to generate outer join detectors", K(ret));
      }
    }
  }
  //2. 生成from item之间的inner join的冲突检测器
  ObRawExpr *expr = NULL;
  ConflictDetector *detector = NULL;
  ObSEArray<ObRawExpr*, 4> join_conditions;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_quals.count(); ++i) {
    if (OB_ISNULL(expr = all_quals.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (ObOptimizerUtil::find_item(all_table_filters, expr)) {
      //do nothing
    } else if (query_ctx->check_opt_compat_version(COMPAT_VERSION_4_2_5_BP7)
               && ObOptimizerUtil::find_item(new_or_quals_, expr)) {
      // do nothing, no need to gen detector for new or quals
    } else if (OB_FAIL(join_conditions.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(find_inner_conflict_detector(inner_join_detectors,
                                                    expr->get_relation_ids(),
                                                    detector))) {
      LOG_WARN("failed to find conflict detector", K(ret));
    } else if (NULL == detector) {
      if (OB_FAIL(ConflictDetector::build_confict(get_allocator(), detector))) {
        LOG_WARN("failed to build conflict detector", K(ret));
      } else if (OB_ISNULL(detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detector", K(ret));
      } else if (OB_FAIL(detector->join_info_.where_conditions_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (expr->has_flag(IS_JOIN_COND) &&
                  OB_FAIL(detector->join_info_.equal_join_conditions_.push_back(expr))) {
        LOG_WARN("failed to push back qual", K(ret));
      } else if (OB_FAIL(detector->join_info_.table_set_.add_members(expr->get_relation_ids()))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (expr->has_flag(CNT_SUB_QUERY) && !ObOptimizerUtil::find_item(push_subq_exprs_, expr) &&
                 OB_FAIL(detector->join_info_.table_set_.add_members(all_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(inner_join_detectors.push_back(detector))) {
        LOG_WARN("failed to push back detector", K(ret));
      } else {
        //检查连接谓词是否是退化谓词
        if (detector->join_info_.table_set_.num_members() > 1) {
          detector->is_degenerate_pred_ = false;
        } else {
          detector->is_degenerate_pred_ = true;
          if (OB_FAIL(detector->L_DS_.add_members(all_table_ids))) {
            LOG_WARN("failed to generate R-TES", K(ret));
          } else if (OB_FAIL(detector->R_DS_.add_members(all_table_ids))) {
            LOG_WARN("failed to generate R-TES", K(ret));
          }
        }
        detector->is_commutative_ = COMM_PROPERTY[INNER_JOIN];
        detector->join_info_.join_type_ = INNER_JOIN;
      }
    } else if (OB_FAIL(detector->join_info_.where_conditions_.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (expr->has_flag(IS_JOIN_COND) &&
               OB_FAIL(detector->join_info_.equal_join_conditions_.push_back(expr))) {
        LOG_WARN("failed to push back qual", K(ret));
    } else if (expr->has_flag(CNT_SUB_QUERY) && !ObOptimizerUtil::find_item(push_subq_exprs_, expr) &&
               OB_FAIL(detector->join_info_.table_set_.add_members(all_table_ids))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  //3. 生成inner join的冲突规则
  for (int64_t i = 0; OB_SUCC(ret) && i < inner_join_detectors.count(); ++i) {
    ConflictDetector *inner_detector = inner_join_detectors.at(i);
    const ObRelIds &table_set = inner_detector->join_info_.table_set_;
    //对于inner join来说，满足交换律，所以不需要区分L_TES、R_TES
    //为了方便之后统一applicable算法，L_TES、R_TES都等于SES
    if (OB_ISNULL(inner_detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null detector", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < table_items.count(); ++j) {
      table_ids.reuse();
      if (OB_FAIL(get_table_ids(table_items.at(j), table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (!table_ids.overlap(table_set)) {
        //do nothing
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
      ConflictDetector *outer_detector = outer_join_detectors.at(j);
      if (OB_ISNULL(outer_detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detector", K(ret));
      } else if (IS_INNER_JOIN(outer_detector->join_info_.join_type_)) {
        //inner join与inner join之前没有冲突，do nothing
      } else if (OB_FAIL(generate_conflict_rule(inner_detector,
                                                outer_detector,
                                                true,
                                                inner_detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      } else if (OB_FAIL(generate_conflict_rule(inner_detector,
                                                outer_detector,
                                                false,
                                                inner_detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
  }
  //4. 生成可选的笛卡尔积
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(inner_join_detectors, outer_join_detectors))) {
      LOG_WARN("failed to append detectors", K(ret));
    } else if (OB_FAIL(generate_cross_product_detector(table_items,
                                                       join_conditions,
                                                       inner_join_detectors))) {
      LOG_WARN("failed to generate cross product detector", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::mock_base_rel_detectors(ObJoinOrder *&base_rel)
{
  int ret = OB_SUCCESS;
  ConflictDetector *detector = NULL;
  // mock a conflict detector whose join info's where_condition
  // is base rel's base table pushdown filter, and add it into
  // used_conflict_detector, which will be used for width est.
  // see ObJoinOrder::est_join_width() condition exclusion for detail
  if (OB_ISNULL(base_rel)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (base_rel->get_restrict_infos().empty() ||
             !base_rel->get_conflict_detectors().empty()) {
    // do nothing
  } else if (OB_FAIL(ConflictDetector::build_confict(get_allocator(), detector))) {
    LOG_WARN("failed to build conflict detector", K(ret));
  } else if (OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null detector", K(ret));
  } else if (OB_FAIL(append_array_no_dup(detector->join_info_.where_conditions_,
                                         base_rel->get_restrict_infos()))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(base_rel->get_conflict_detectors(), detector))) {
    LOG_WARN("failed to append into used conflict detectors", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObLogPlan::generate_outer_join_detectors(TableItem *table_item,
                                             ObIArray<ObRawExpr*> &table_filter,
                                             ObIArray<ObJoinOrder *> &baserels,
                                             ObIArray<ConflictDetector*> &outer_join_detectors)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = static_cast<JoinedTable*>(table_item);
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table_item->is_joined_table()) {
    //如果是基表，直接把过程谓词分发到join order里
    if (OB_FAIL(distribute_quals(table_item->table_id_, table_filter, baserels))) {
      LOG_WARN("failed to distribute table filter", K(ret));
    }
  } else if (INNER_JOIN == joined_table->joined_type_) {
    //抚平joined table内部的inner join
    ObSEArray<TableItem *, 4> table_items;
    ObSEArray<ConflictDetector*, 4> detectors;
    if (OB_FAIL(flatten_inner_join(table_item, table_filter, table_items))) {
      LOG_WARN("failed to flatten inner join", K(ret));
    } else if (OB_FAIL(SMART_CALL(generate_inner_join_detectors(table_items,
                                                     table_filter,
                                                     baserels,
                                                     detectors)))) {
      LOG_WARN("failed to generate inner join detectors", K(ret));
    } else if (OB_FAIL(append(outer_join_detectors, detectors))) {
      LOG_WARN("failed to append detectors", K(ret));
    }
  } else if (OB_FAIL(inner_generate_outer_join_detectors(joined_table,
                                                         table_filter,
                                                         baserels,
                                                         outer_join_detectors))) {
    LOG_WARN("failed to generate outer join detectors", K(ret));
  }
  return ret;
}

int ObLogPlan::distribute_quals(uint64_t table_id,
                                const ObIArray<ObRawExpr*> &table_filter,
                                ObIArray<ObJoinOrder *> &baserels)
{
  int ret = OB_SUCCESS;
  ObJoinOrder *cur_rel = NULL;
  int64_t rel_id = OB_INVALID_INDEX;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null stmt", K(ret));
  } else if (OB_FALSE_IT(rel_id = get_stmt()->get_table_bit_index(table_id))) {
  } else if (OB_FAIL(find_base_rel(baserels, rel_id, cur_rel))) {
    LOG_WARN("find_base_rel fails", K(ret));
  } else if (OB_ISNULL(cur_rel)) {
    ret = OB_SQL_OPT_ERROR;
    LOG_WARN("failed to distribute qual to rel", K(baserels), K(rel_id), K(ret));
  } else if (OB_FAIL(append(cur_rel->get_restrict_infos(), table_filter))) {
    LOG_WARN("failed to distribute qual to rel", K(ret));
  }
  return ret;
}

int ObLogPlan::flatten_inner_join(TableItem *table_item,
                                  ObIArray<ObRawExpr*> &table_filter,
                                  ObIArray<TableItem*> &table_items)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = static_cast<JoinedTable*>(table_item);
  ObSEArray<ObRawExpr*, 16> new_conditions;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table_item->is_joined_table() ||
             INNER_JOIN != joined_table->joined_type_) {
    ret = table_items.push_back(table_item);
  } else if (OB_FAIL(SMART_CALL(flatten_inner_join(joined_table->left_table_,
                                                   table_filter,
                                                   table_items)))) {
    LOG_WARN("failed to faltten inner join", K(ret));
  } else if (OB_FAIL(SMART_CALL(flatten_inner_join(joined_table->right_table_,
                                                   table_filter,
                                                   table_items)))) {
    LOG_WARN("failed to faltten inner join", K(ret));
  } else if (OB_FAIL(adjust_exprs_with_onetime(joined_table->join_conditions_,
                                               new_conditions))) {
    LOG_WARN("failed to adjust join conditions with onetime", K(ret));
  } else if (OB_FAIL(append(table_filter, new_conditions))) {
    LOG_WARN("failed to append exprs", K(ret));
  }
  return ret;
}

int ObLogPlan::inner_generate_outer_join_detectors(JoinedTable *joined_table,
                                                  ObIArray<ObRawExpr*> &table_filter,
                                                  ObIArray<ObJoinOrder *> &baserels,
                                                  ObIArray<ConflictDetector*> &outer_join_detectors)
{
  int ret = OB_SUCCESS;
  ObRelIds table_set;
  ObRelIds left_table_ids;
  ObRelIds right_table_ids;
  ConflictDetector *detector = NULL;
  ObSEArray<ObRawExpr *, 4> left_quals;
  ObSEArray<ObRawExpr *, 4> right_quals;
  ObSEArray<ObRawExpr *, 4> join_quals;
  ObSEArray<ConflictDetector*, 4> left_detectors;
  ObSEArray<ConflictDetector*, 4> right_detectors;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_FAIL(get_table_ids(joined_table->join_conditions_, table_set))) {
    LOG_WARN("failed to get table ids", K(ret));
    //1. pushdown where condition
  } else if (OB_FAIL(pushdown_where_filters(joined_table,
                                            table_filter,
                                            left_quals,
                                            right_quals))) {
    LOG_WARN("failed to pushdown where filters", K(ret));
    //2. pushdown on condition
  } else if (OB_FAIL(pushdown_on_conditions(joined_table,
                                            left_quals,
                                            right_quals,
                                            join_quals))) {
    LOG_WARN("failed to pushdown on conditions", K(ret));
    //3. generate left child detectors
  } else if (OB_FAIL(SMART_CALL(generate_outer_join_detectors(joined_table->left_table_,
                                                    left_quals,
                                                    baserels,
                                                    left_detectors)))) {
    LOG_WARN("failed to generate outer join detectors", K(ret));
  } else if (OB_FAIL(append(outer_join_detectors, left_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
    //4. generate right child detectors
  } else if (OB_FAIL(SMART_CALL(generate_outer_join_detectors(joined_table->right_table_,
                                                    right_quals,
                                                    baserels,
                                                    right_detectors)))) {
    LOG_WARN("failed to generate outer join detectors", K(ret));
  } else if (OB_FAIL(append(outer_join_detectors, right_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
    //5. create outer join detector
  } else if (OB_FAIL(ConflictDetector::build_confict(get_allocator(),
                                                      detector))) {
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
  } else if (OB_FAIL(append(detector->join_info_.on_conditions_,
                            join_quals))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(outer_join_detectors.push_back(detector))) {
    LOG_WARN("failed to push back detecotor", K(ret));
  } else {
    //检查连接是否具备交换律
    detector->is_commutative_ = COMM_PROPERTY[joined_table->joined_type_];
    detector->join_info_.join_type_ = joined_table->joined_type_;
    //检查连接谓词是否是退化谓词
    if (table_set.overlap(left_table_ids) &&
        table_set.overlap(right_table_ids)) {
      detector->is_degenerate_pred_ = false;
    } else {
      detector->is_degenerate_pred_ = true;
    }
    //connect by join强制依赖所有的表，与连接谓词无关
    if (CONNECT_BY_JOIN == joined_table->joined_type_) {
      if (OB_FAIL(detector->join_info_.table_set_.add_members(left_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(detector->join_info_.table_set_.add_members(right_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(detector->L_TES_.intersect(detector->join_info_.table_set_,
                                             detector->L_DS_))) {
        LOG_WARN("failed to generate L-TES", K(ret));
      } else if (OB_FAIL(detector->R_TES_.intersect(detector->join_info_.table_set_,
                                                    detector->R_DS_))) {
        LOG_WARN("failed to generate R-TES", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < join_quals.count(); ++i) {
      ObRawExpr *expr = join_quals.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (expr->has_flag(IS_JOIN_COND) &&
                  OB_FAIL(detector->join_info_.equal_join_conditions_.push_back(expr))) {
        LOG_WARN("failed to push back qual", K(ret));
      }
    }
    //6. generate conflict rules
    for (int64_t i = 0; OB_SUCC(ret) && i < left_detectors.count(); ++i) {
      if (OB_FAIL(generate_conflict_rule(detector,
                                          left_detectors.at(i),
                                          true,
                                          detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_detectors.count(); ++i) {
      if (OB_FAIL(generate_conflict_rule(detector,
                                          right_detectors.at(i),
                                          false,
                                          detector->CR_))) {
        LOG_WARN("failed to generate conflict rule", K(ret));
      }
    }
    //7. generate conflict detector for table filter
    if (OB_SUCC(ret) && !table_filter.empty()) {
      if (OB_FAIL(ConflictDetector::build_confict(get_allocator(), detector))) {
        LOG_WARN("failed to build conflict detector", K(ret));
      } else if (OB_ISNULL(detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detector", K(ret));
      } else if (OB_FAIL(append(detector->join_info_.where_conditions_, table_filter))) {
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

int ObLogPlan::pushdown_where_filters(JoinedTable* joined_table,
                                      ObIArray<ObRawExpr*> &table_filter,
                                      ObIArray<ObRawExpr*> &left_quals,
                                      ObIArray<ObRawExpr*> &right_quals)
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
      ObRawExpr *qual =  table_filter.at(i);
      if (OB_ISNULL(qual)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(qual), K(ret));
      } else if (qual->has_flag(CNT_ROWNUM)) {
        if (OB_FAIL(push_back_join_cond_if_needed(new_quals, qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (qual->get_relation_ids().is_empty()) {
        if (OB_FAIL(left_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (qual->is_const_expr() &&
                   OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (LEFT_OUTER_JOIN == join_type &&
                 qual->get_relation_ids().is_subset(left_table_set)) {
        if (OB_FAIL(left_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (RIGHT_OUTER_JOIN == join_type &&
                 qual->get_relation_ids().is_subset(right_table_set)) {
        if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(push_back_join_cond_if_needed(new_quals, qual))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (T_OP_OR == qual->get_expr_type()) {
        ObOpRawExpr *or_qual = static_cast<ObOpRawExpr *>(qual);
        if (LEFT_OUTER_JOIN == join_type
            && OB_FAIL(try_split_or_qual(left_table_set,
                                          *or_qual,
                                          left_quals))) {
          LOG_WARN("failed to split or qual on left table", K(ret));
        } else if (RIGHT_OUTER_JOIN ==join_type
                  && OB_FAIL(try_split_or_qual(right_table_set,
                                              *or_qual,
                                              right_quals))) {
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

int ObLogPlan::pushdown_on_conditions(JoinedTable* joined_table,
                                      ObIArray<ObRawExpr*> &left_quals,
                                      ObIArray<ObRawExpr*> &right_quals,
                                      ObIArray<ObRawExpr*> &join_quals)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else {
    ObRawExpr *qual = NULL;
    ObRelIds left_table_set;
    ObRelIds right_table_set;
    ObJoinType join_type = joined_table->joined_type_;
    int64_t N = joined_table->join_conditions_.count();
    ObSEArray<ObRawExpr*, 16> new_conditions;
    if (OB_FAIL(get_table_ids(joined_table->left_table_, left_table_set))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(get_table_ids(joined_table->right_table_, right_table_set))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(adjust_exprs_with_onetime(joined_table->join_conditions_,
                                                 new_conditions))) {
      LOG_WARN("failed to adjust join conditions with onetime", K(ret));
    } else if (OB_UNLIKELY(new_conditions.count() != N)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr count mismatch", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_ISNULL(qual = new_conditions.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(qual), K(ret));
      } else if (qual->has_flag(CNT_ROWNUM) ||
                 (qual->has_flag(CNT_SUB_QUERY) && !ObOptimizerUtil::find_item(push_subq_exprs_, qual))) {
        if (OB_FAIL(push_back_join_cond_if_needed(join_quals, qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (RIGHT_OUTER_JOIN == join_type &&
                 qual->get_relation_ids().is_subset(left_table_set)) {
        if (OB_FAIL(left_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (LEFT_OUTER_JOIN == join_type &&
                 qual->get_relation_ids().is_subset(right_table_set)) {
        if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (CONNECT_BY_JOIN == join_type &&
                 !qual->get_relation_ids().is_empty() &&
                 qual->get_relation_ids().is_subset(right_table_set)
                 && !qual->has_flag(CNT_LEVEL)
                 && !qual->has_flag(CNT_PRIOR)
                 && !qual->has_flag(CNT_ROWNUM)) {
        if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(push_back_join_cond_if_needed(join_quals, qual))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (!qual->get_relation_ids().is_empty() &&
                  T_OP_OR == qual->get_expr_type()) {
        ObOpRawExpr *or_qual = static_cast<ObOpRawExpr *>(qual);
        if (LEFT_OUTER_JOIN == join_type &&
            OB_FAIL(try_split_or_qual(right_table_set,
                                      *or_qual,
                                      right_quals))) {
          LOG_WARN("failed to split or qual on right table", K(ret));
        } else if (RIGHT_OUTER_JOIN ==join_type &&
                    OB_FAIL(try_split_or_qual(left_table_set,
                                            *or_qual,
                                            left_quals))) {
          LOG_WARN("failed to split or qual on left table", K(ret));
        } else if (CONNECT_BY_JOIN == join_type &&
                    !qual->has_flag(CNT_LEVEL) &&
                    !qual->has_flag(CNT_ROWNUM) &&
                    !qual->has_flag(CNT_PRIOR) &&
                    OB_FAIL(try_split_or_qual(right_table_set,
                                              *or_qual,
                                              right_quals))) {
          LOG_WARN("failed to split or qual on right table", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::pushdown_semi_conditions(SemiInfo *info,
                                        ObIArray<ObRawExpr*> &right_quals,
                                        ObIArray<ObRawExpr*> &semi_conds)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  ObSEArray<ObRawExpr*, 16> new_conditions;
  if (OB_ISNULL(info) || OB_ISNULL(query_ctx = get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null param", K(ret), K(info), K(query_ctx));
  } else if (OB_FAIL(adjust_exprs_with_onetime(info->semi_conditions_,
                                               new_conditions))) {
    LOG_WARN("failed to adjust join conditions with onetime", K(ret));
  } else if (OB_UNLIKELY(new_conditions.count() != info->semi_conditions_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr count mismatch", K(ret), K(new_conditions.count()), K(info->semi_conditions_.count()));
  } else if (!query_ctx->check_opt_compat_version(COMPAT_VERSION_4_2_5_BP7)) {
    if (OB_FAIL(semi_conds.assign(new_conditions))) {
      LOG_WARN("failed to assign semi conditions", K(ret));
    }
  } else {
    ObRawExpr *qual = NULL;
    ObRelIds right_rel_ids;
    if (OB_FAIL(get_table_ids(info->right_table_id_, right_rel_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_conditions.count(); ++i) {
      if (OB_ISNULL(qual = new_conditions.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(qual), K(ret));
      } else if (qual->has_flag(CNT_ROWNUM) ||
                 (qual->has_flag(CNT_SUB_QUERY) && !ObOptimizerUtil::find_item(push_subq_exprs_, qual))) {
        if (OB_FAIL(push_back_join_cond_if_needed(semi_conds, qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (!qual->is_deterministic()) {
        if (OB_FAIL(push_back_join_cond_if_needed(semi_conds, qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (qual->get_relation_ids().is_subset(right_rel_ids)) {
        if (OB_FAIL(right_quals.push_back(qual))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(push_back_join_cond_if_needed(semi_conds, qual))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (!qual->get_relation_ids().is_empty() &&
                  T_OP_OR == qual->get_expr_type()) {
        ObOpRawExpr *or_qual = static_cast<ObOpRawExpr *>(qual);
        if (OB_FAIL(try_split_or_qual(right_rel_ids,
                                      *or_qual,
                                      right_quals))) {
          LOG_WARN("failed to split or qual on right table", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_cross_product_detector(const ObIArray<TableItem*> &table_items,
                                              ObIArray<ObRawExpr*> &quals,
                                              ObIArray<ConflictDetector*> &inner_join_detectors)
{
  int ret = OB_SUCCESS;
  //生成笛卡尔积的冲突检测器
  //在OB中，笛卡尔积也是inner join
  ObRelIds table_ids;
  ConflictDetector *detector = NULL;
  if (table_items.count() < 2) {
    //do nothing
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

int ObLogPlan::check_join_info(const ObRelIds &left,
                               const ObRelIds &right,
                               const ObIArray<ObRelIds> &base_table_ids,
                               bool &is_connected)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_ids;
  is_connected = false;
  if (!left.overlap(right)) {
    //do nothing
  } else if (OB_FAIL(table_ids.except(left, right))) {
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

int ObLogPlan::generate_cross_product_conflict_rule(ConflictDetector *cross_product_detector,
                                                    const ObIArray<TableItem*> &table_items,
                                                    const ObIArray<ObRawExpr*> &join_conditions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cross_product_detector)) {
    ret = OB_SUCCESS;
    LOG_WARN("unexpect null detector", K(ret));
  } else {
    ObRelIds table_ids;
    bool have_new_connect_info = true;
    ObSEArray<ObRelIds, 8> base_table_ids;
    //连通图信息
    ObSEArray<ObRelIds, 8> connect_infos;
    //初始化base table，joined table看做整体
    //笛卡尔积应该在所有的joined table枚举完再枚举
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
    /**
     * 计算连通图信息，核心算法：
     * 初始状态：每个base table为一个独立的图
     * 连通原则：如果某一个join condition引用的表对于某个图而言
     * 只增加了一张表A，那么我们认为这个图通过这个join condition
     * 连通了表A，表A加入这张表。
     * 如果某一个join condition连通了多个图，那么我们认为这些图之间是连通的，
     * 需要合并这些图。
     * 算法思想：由于每个join condition可能相互依赖，所以采用迭代算法，
     * 只要连通图的状态发生变化，就需要遍历未使用的join condition，直到
     * 连通图的状态稳定。
     */
    ObSqlBitSet<> used_join_conditions;
    ObSqlBitSet<> used_infos;
    ObSqlBitSet<> connect_tables;
    while (have_new_connect_info) {
      have_new_connect_info = false;
      //遍历未使用的join condition，检查是否连通某个图
      for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions.count(); ++i) {
        ObRawExpr *expr = join_conditions.at(i);
        if (used_join_conditions.has_member(i)) {
          //do nothing
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (has_depend_table(expr->get_relation_ids())) {
          //do nothing
        } else {
          used_infos.reuse();
          connect_tables.reuse();
          if (OB_FAIL(connect_tables.add_members(expr->get_relation_ids()))) {
            LOG_WARN("failed to add members", K(ret));
          }
          //遍历所有的连通图，检查join condition是否连通某个图
          for (int64_t j = 0; OB_SUCC(ret) && j < connect_infos.count(); ++j) {
            bool is_connected = false;
            if (OB_FAIL(check_join_info(expr->get_relation_ids(),
                                        connect_infos.at(j),
                                        base_table_ids, is_connected))) {
              LOG_WARN("failed to check join info", K(ret));
            } else if (!is_connected) {
              //do nothing
            } else if (OB_FAIL(used_infos.add_member(j))) {
              LOG_WARN("failed to add member", K(ret));
            } else if (OB_FAIL(connect_tables.add_members(connect_infos.at(j)))) {
              LOG_WARN("failed to add members", K(ret));
            }
          }
          /**
           * 合并连通图，实际上并没有删除冗余的图，
           * 这个最后再统一删除，避免容器反复变更size引起数据拷贝
           */
          if (OB_SUCC(ret) && !used_infos.is_empty()) {
            have_new_connect_info = true;
            if (OB_FAIL(used_join_conditions.add_member(i))) {
              LOG_WARN("failed to add member", K(ret));
            }
            for (int64_t j = 0; OB_SUCC(ret) && j < connect_infos.count(); ++j) {
              if (!used_infos.has_member(j)) {
                //do nothing
              } else if (OB_FAIL(connect_infos.at(j).add_members(connect_tables))) {
                LOG_WARN("failed to add members", K(ret));
              }
            }
            LOG_TRACE("succeed to add new connect info", K(*expr), K(connect_infos));
          }
        }
      }
    }
    //去重
    ObSEArray<ObRelIds, 8> new_connect_infos;
    for (int64_t i = 0; OB_SUCC(ret) && i < connect_infos.count(); ++i) {
      bool find = false;
      for (int64_t j =0; !find && j < new_connect_infos.count(); ++j) {
        if (new_connect_infos.at(j).equal(connect_infos.at(i))) {
          find = true;
        }
      }
      if (!find) {
        ret = new_connect_infos.push_back(connect_infos.at(i));
      }
    }
    //使用连通图信息生成冲突规则
    for (int64_t i = 0; OB_SUCC(ret) && i < base_table_ids.count(); ++i) {
      if (base_table_ids.at(i).num_members() < 2) {
        //do nothing
      } else if (OB_FAIL(add_conflict_rule(base_table_ids.at(i),
                                          base_table_ids.at(i),
                                          cross_product_detector->cross_product_rule_))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_connect_infos.count(); ++i) {
      if (new_connect_infos.at(i).num_members() < 2) {
        //do nothing
      } else if (OB_FAIL(add_conflict_rule(new_connect_infos.at(i),
                                            new_connect_infos.at(i),
                                            cross_product_detector->delay_cross_product_rule_))) {
        LOG_WARN("failed to add conflict rule", K(ret));
      }
    }
    //为了能够延迟笛卡尔积，需要生成bushy tree info辅助join order枚举
    //例如(A inner join B on xxx) inner join (C inner join D on xxx)
    //需要生成bushy tree info(A,B,C,D)
    for (int64_t i = 0; OB_SUCC(ret) && i < new_connect_infos.count(); ++i) {
      if (new_connect_infos.at(i).num_members() > 1) {
        for (int64_t j = i+1; OB_SUCC(ret) && j < new_connect_infos.count(); ++j) {
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
        //全连通图，意味着这个笛卡尔是冗余的，leading hint使用
        cross_product_detector->is_redundancy_ = true;
      }
    }
    LOG_TRACE("update bushy tree info", K(bushy_tree_infos_));
  }
  return ret;
}

// 选择location
int ObLogPlan::select_location(ObIArray<ObTablePartitionInfo *> &tbl_part_info_list)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = optimizer_context_.get_exec_ctx();
  ObSEArray<const ObTableLocation*, 1> tbl_loc_list;
  ObSEArray<ObCandiTableLoc*, 1> phy_tbl_loc_info_list;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("exec ctx is NULL", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tbl_part_info_list.count(); ++i) {
    ObTablePartitionInfo *tbl_part_info = tbl_part_info_list.at(i);
    if (OB_ISNULL(tbl_part_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tbl part info is NULL", K(ret), K(i), K(tbl_part_info_list.count()));
    } else if (OB_FAIL(tbl_loc_list.push_back(&tbl_part_info->get_table_location()))) {
      LOG_WARN("fail to push back table location list",
               K(ret), K(tbl_part_info->get_table_location()));
    } else if (OB_FAIL(phy_tbl_loc_info_list.push_back(
                &tbl_part_info->get_phy_tbl_location_info_for_update()))) {
      LOG_WARN("fail to push back phy tble loc info",
               K(ret), K(tbl_part_info->get_phy_tbl_location_info_for_update()));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObLogPlan::select_replicas(*exec_ctx,
                                                tbl_loc_list,
                                                optimizer_context_.get_local_server_addr(),
                                                phy_tbl_loc_info_list))) {
    LOG_WARN("fail to select replicas", K(ret), K(tbl_loc_list.count()),
             K(optimizer_context_.get_local_server_addr()),
             K(phy_tbl_loc_info_list.count()));
  }
  return ret;
}

int ObLogPlan::select_replicas(ObExecContext &exec_ctx,
                               const ObIArray<const ObTableLocation*> &tbl_loc_list,
                               const ObAddr &local_server,
                               ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list)
{
  int ret = OB_SUCCESS;
  bool is_weak = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_weak && i < tbl_loc_list.count(); i++) {
    bool is_weak_read = false;
    const ObTableLocation *table_location = tbl_loc_list.at(i);
    if (OB_ISNULL(table_location)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table location is NULL", K(ret), K(i), K(tbl_loc_list.count()));
    } else if (!table_location->get_loc_meta().is_weak_read_) {
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

int ObLogPlan::select_replicas(ObExecContext &exec_ctx,
                               bool is_weak,
                               const ObAddr &local_server,
                               ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list)
{
  int ret = OB_SUCCESS;
  // 计算是否为weak读
  // 当所有的location都是weak读的时候，总的才是weak读，否则就是strong
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  ObTaskExecutorCtx &task_exec_ctx = exec_ctx.get_task_exec_ctx();
  bool is_hit_partition = false;
  int64_t proxy_stat = 0;
  ObFollowerFirstFeedbackType follower_first_feedback = FFF_HIT_MIN;
  int64_t route_policy_type = 0;
  bool proxy_priority_hit_support = false;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected NULL", K(ret), K(session));
  } else if (OB_FAIL(session->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy_type))) {
    LOG_WARN("fail to get sys variable", K(ret));
  } else {
    proxy_priority_hit_support = session->get_proxy_cap_flags().is_priority_hit_support();
  }

  if (OB_FAIL(ret)) {
  } else if (is_weak) {
    int64_t max_read_stale_time = exec_ctx.get_my_session()->get_ob_max_read_stale_time();
    uint64_t tenant_id = exec_ctx.get_my_session()->get_effective_tenant_id();
    if (OB_FAIL(ObLogPlan::weak_select_replicas(local_server,
                                                static_cast<ObRoutePolicyType>(route_policy_type),
                                                proxy_priority_hit_support,
                                                tenant_id,
                                                max_read_stale_time,
                                                phy_tbl_loc_info_list,
                                                is_hit_partition, follower_first_feedback, proxy_stat))) {
      LOG_WARN("fail to weak select intersect replicas", K(ret), K(local_server), K(phy_tbl_loc_info_list.count()));
    } else {
      session->partition_hit().try_set_bool(is_hit_partition);
      if (FFF_HIT_MIN != follower_first_feedback) {
        if (OB_FAIL(session->set_follower_first_feedback(follower_first_feedback))) {
          LOG_WARN("fail to set_follower_first_feedback", K(follower_first_feedback), K(ret));
        }
      }

      if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
        // standby and restore tenant not feedback
      } else if (OB_SUCC(ret) && proxy_stat != 0) {
        ObObj val;
        val.set_int(proxy_stat);
        if (OB_FAIL(session->update_sys_variable(SYS_VAR__OB_PROXY_WEAKREAD_FEEDBACK, val))) {
          LOG_WARN("replace user val failed", K(ret), K(val));
        }
      }
    }
  } else {
    const bool sess_in_retry = session->get_is_in_retry_for_dup_tbl(); //重试状态下不优化复制表的副本选择
    const bool is_dup_ls_modified = session->is_dup_ls_modified();
    if (OB_FAIL(ObLogPlan::strong_select_replicas(local_server, phy_tbl_loc_info_list, is_hit_partition, sess_in_retry, is_dup_ls_modified))) {
      LOG_WARN("fail to strong select replicas", K(ret), K(local_server), K(phy_tbl_loc_info_list.count()));
    } else {
      session->partition_hit().try_set_bool(is_hit_partition);
    }
  }
  return ret;
}

int ObLogPlan::strong_select_replicas(const ObAddr &local_server,
                                      ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list,
                                      bool &is_hit_partition,
                                      bool sess_in_retry,      //当前session是否在retry中
                                      bool is_dup_ls_modified)
{
  int ret = OB_SUCCESS;
  // 全部选主
  bool all_is_on_same_server = true;
  ObAddr all_same_server = local_server; // 初始化为本机，如果下面所有的表的partition个数都为0的话，same_server就是本机，就会返回给客户端命中
  ObAddr cur_same_server;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_tbl_loc_info_list.count(); ++i) {
    cur_same_server.reset();
    ObCandiTableLoc *phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
    bool is_on_same_server = false;
    if (OB_ISNULL(phy_tbl_loc_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
    } else if (0 == phy_tbl_loc_info->get_partition_cnt()) {
      // 该table的partition个数为0，跳过
    } else {
      if (!sess_in_retry
          && !is_dup_ls_modified
          && phy_tbl_loc_info->is_duplicate_table_not_in_dml()) {
        if (OB_FAIL(phy_tbl_loc_info->all_select_local_replica_or_leader(is_on_same_server, cur_same_server, local_server))) {
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
        //do nothing...
      } else if (all_is_on_same_server) { // 还在同一个server才判断，否则已经不需要判断了
        if (is_on_same_server) { // 该表选择的所有副本在同一个server
          if (0 == i) {
            all_same_server = cur_same_server;
          } else if (all_same_server != cur_same_server) {
            all_is_on_same_server = false;
          }
        } else { // 该表选择的所有副本不在同一个server
          all_is_on_same_server = false;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // 当选择的所有副本在同一个server并且不是本机时，给客户端返回不命中，否则返回命中
    if (all_is_on_same_server && local_server != all_same_server) {
      is_hit_partition = false;
    } else {
      is_hit_partition = true;
    }
  }
  return ret;
}

int ObLogPlan::weak_select_replicas(const ObAddr &local_server,
                                    ObRoutePolicyType route_type,
                                    bool proxy_priority_hit_support,
                                    uint64_t tenant_id,
                                    int64_t max_read_stale_time,
                                    ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list,
                                    bool &is_hit_partition,
                                    ObFollowerFirstFeedbackType &follower_first_feedback,
                                    int64_t &proxy_stat)
{
  int ret = OB_SUCCESS;
  proxy_stat = 0;
  is_hit_partition = true;//当前没有办法来判断是否能选择在一台机器上，所以将该值设置为true
  ObCandiTableLoc * phy_tbl_loc_info = nullptr;
  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
  ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> intersect_server_list(allocator);
  SMART_VAR(ObRoutePolicy, route_policy, local_server) {
    ObRoutePolicyCtx route_policy_ctx;
    route_policy_ctx.policy_type_ = route_type;
    route_policy_ctx.consistency_level_ = WEAK;
    route_policy_ctx.is_proxy_priority_hit_support_ = proxy_priority_hit_support;
    route_policy_ctx.max_read_stale_time_ = max_read_stale_time;
    route_policy_ctx.tenant_id_ = tenant_id;

    if (OB_FAIL(route_policy.init())) {
      LOG_WARN("fail to init route policy", K(ret));
    }
    for (int64_t i = 0 ; OB_SUCC(ret) && i < phy_tbl_loc_info_list.count(); ++i) {
      if (OB_ISNULL(phy_tbl_loc_info = phy_tbl_loc_info_list.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy table loc info is NULL", K(phy_tbl_loc_info), K(i), K(ret));
      } else {
        ObCandiTabletLocIArray &phy_part_loc_info_list = phy_tbl_loc_info->get_phy_part_loc_info_list_for_update();
        for (int64_t j = 0; OB_SUCC(ret) && j < phy_part_loc_info_list.count(); ++j) {
          ObCandiTabletLoc &phy_part_loc_info = phy_part_loc_info_list.at(j);
          if (phy_part_loc_info.has_selected_replica()) {//do nothing
          } else {
            ObIArray<ObRoutePolicy::CandidateReplica> &replica_array = phy_part_loc_info.get_partition_location().get_replica_locations();
            if (OB_FAIL(route_policy.init_candidate_replicas(replica_array))) {
              LOG_WARN("fail to init candidate replicas", K(replica_array), K(ret));
            } else if (OB_FAIL(route_policy.calculate_replica_priority(local_server,
                                                                       phy_part_loc_info.get_ls_id(),
                                                                       replica_array,
                                                                       route_policy_ctx))) {
              LOG_WARN("fail to calculate replica priority", K(replica_array), K(route_policy_ctx), K(ret));
            } else if (OB_FAIL(route_policy.select_replica_with_priority(route_policy_ctx, replica_array, phy_part_loc_info))) {
              LOG_WARN("fail to select replica", K(replica_array), K(ret));
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(route_policy.select_intersect_replica(route_policy_ctx,
                                                              phy_tbl_loc_info_list,
                                                              intersect_server_list,
                                                              is_hit_partition))) {
      LOG_WARN("fail to select intersect replica", K(route_policy_ctx), K(phy_tbl_loc_info_list), K(intersect_server_list), K(ret));
    }

    if (OB_SUCC(ret)) {
      if (proxy_priority_hit_support) {
        // nothing, current doesn't support
      } else {
        ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
        ObAddrList intersect_servers(allocator);
        if (OB_FAIL(calc_rwsplit_partition_feedback(phy_tbl_loc_info_list, local_server, proxy_stat))) {
          LOG_WARN("fail to calc proxy partition feedback", K(ret));
        } else if (OB_FAIL(calc_hit_partition_for_compat(phy_tbl_loc_info_list, local_server, is_hit_partition, intersect_servers))) {
          LOG_WARN("fail to calc hit partition for compat", K(ret));
        } else {
          if (is_hit_partition && route_policy.is_follower_first_route_policy_type(route_policy_ctx)) {
            if (OB_FAIL(calc_follower_first_feedback(phy_tbl_loc_info_list, local_server, intersect_servers, follower_first_feedback))) {
              LOG_WARN("fail to calc follower first feedback", K(ret));
            }
          }
        }
      }
    }
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {//10s打印一次
        LOG_INFO("selected replica ", "intersect_server_list", intersect_server_list,
                "\n phy_tbl_loc_info_list", phy_tbl_loc_info_list,
                "\n route_policy", route_policy,
                "\n route_policy_ctx", route_policy_ctx,
                "\n follower_first_feedback", follower_first_feedback,
                "\n is_hit_partition", is_hit_partition);
    }
  }
  return ret;
}

int ObLogPlan::calc_follower_first_feedback(const ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list,
                                            const ObAddr &local_server,
                                            const ObAddrList &intersect_servers,
                                            ObFollowerFirstFeedbackType &follower_first_feedback)
{
  INIT_SUCC(ret);
  // UNMERGE_FOLLOWER_FIRST反馈策略(在partition_hit为true的情况下生效)
  //
  // 1. 如果所涉及的所有partition的主都在本机，则为FFF_HIT_LEADER(相当于未命中);
  // 2. 其它情况均认为命中, 注意备优先读在非读写分离架构下才生效;
  //
  follower_first_feedback = FFF_HIT_MIN;
  if (intersect_servers.empty()) {
    // nothing, no need feedback
  } else {
    bool is_leader_replica = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_leader_replica && (i < phy_tbl_loc_info_list.count()); ++i) {
      const ObCandiTableLoc *phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
      if (OB_ISNULL(phy_tbl_loc_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
      } else {
        const ObCandiTabletLocIArray &phy_part_loc_info_list = phy_tbl_loc_info->get_phy_part_loc_info_list();
        if (phy_part_loc_info_list.empty()) {
          // juest defense, when partition location list is empty, treat as it's not leader replica
          is_leader_replica = false;
        }
        for (int64_t j = 0; OB_SUCC(ret) && is_leader_replica && (j < phy_part_loc_info_list.count()); ++j) {
          bool found_server = false;
          const ObCandiTabletLoc &phy_part_loc_info = phy_part_loc_info_list.at(j);
          const ObIArray<ObRoutePolicy::CandidateReplica> &replica_loc_list =
            phy_part_loc_info.get_partition_location().get_replica_locations();
          for (int64_t k = 0; !found_server && (k < replica_loc_list.count()); ++k) {
            const ObRoutePolicy::CandidateReplica &tmp_replica = replica_loc_list.at(k);
            if (local_server == tmp_replica.get_server()) {
              found_server = true;
              is_leader_replica = (is_strong_leader(tmp_replica.get_role()));
            }
          }
          if (!found_server) { // if not found, just treat as it's not leader
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
    LOG_TRACE("after calc follower first feedback", K(follower_first_feedback),
              K(is_leader_replica), K(local_server), K(intersect_servers));
  }

  return ret;
}

int ObLogPlan::calc_rwsplit_partition_feedback(const common::ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list,
                                               const common::ObAddr &local_server,
                                               int64_t &proxy_stat)
{
  INIT_SUCC(ret);

  bool all_leader = false;
  bool all_follower = false;
  bool need_break = false;
  for (int64_t i = 0; OB_SUCC(ret) && !need_break  && (i < phy_tbl_loc_info_list.count()); ++i) {
    // table
    const ObCandiTableLoc *phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
    if (OB_ISNULL(phy_tbl_loc_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
    } else {
      const ObCandiTabletLocIArray &phy_part_loc_info_list =
                          phy_tbl_loc_info->get_phy_part_loc_info_list();
      if (phy_part_loc_info_list.empty()) {
        // just defense, when partition location list is empty, treat as it's not leader replica
        need_break = true;
      }

      for (int64_t j = 0; OB_SUCC(ret) && !need_break && (j < phy_part_loc_info_list.count()); ++j) {
        // partition
        bool found_server = false;
        const ObCandiTabletLoc &phy_part_loc_info = phy_part_loc_info_list.at(j);
        const ObIArray<ObRoutePolicy::CandidateReplica> &replica_loc_list =
                                    phy_part_loc_info.get_partition_location().get_replica_locations();
        LOG_TRACE("weak read list", K(replica_loc_list), K(local_server));
        for (int64_t k = 0; !found_server && (k < replica_loc_list.count()); ++k) {
          // replica
          const ObRoutePolicy::CandidateReplica &tmp_replica = replica_loc_list.at(k);
          if (local_server == tmp_replica.get_server()) {
            found_server = true;
            if (is_strong_leader(tmp_replica.get_role())) {
              all_leader = true;
              // part leader, part follower
              need_break = all_follower ? true : false;
            } else {
              all_follower = true;
              // part leader, part follower
              need_break = all_leader ? true : false;
            }
          }
        }
      }
    }
  }

  LOG_TRACE("get feedback policy", K(all_leader), K(all_follower));
  //Design a kv pair (hidden user variable, __ob_proxy_weakread_feedback(bool)):
  //state:
  //1. The current machine does not have any replicas (returns true)
  //2. Some/all copies involved are distributed on the current machine:
  //     a. ALL FOLLOWER (do not return)
  //     b. ALL LEADER (return true)
  //     c. PART LEADER/PART FOLLOWER (return true)
  if (!all_leader && !all_follower) {
    // Current machine has no replica (proxy sent incorrectly, refresh location cache).
    proxy_stat = 1;
  } else if (all_leader && all_follower) {
    // part leader, part follower
    proxy_stat = 3;
  } else if (all_leader) {
    // all leader (proxy sent incorrectly, refresh location cache)
    proxy_stat = 2;
  } else if (all_follower) {
    // proxy not need refresh location cache
  }
  return ret;
}

//该函数是为了兼容老版本proxy的hit策略,当proxy更新后可以去掉该函数
int ObLogPlan::calc_hit_partition_for_compat(const ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list,
                                             const ObAddr &local_server,
                                             bool &is_hit_partition,
                                             ObAddrList &intersect_servers)
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
      const ObAddr &candidate_server = *candidate_server_list_iter;
      if (local_server == candidate_server) {
        is_hit_partition = true;
        can_select_local_server = true;
      }
    }
  }
  return ret;
}

int ObLogPlan::calc_intersect_servers(const ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list,
                                      ObAddrList &candidate_server_list)
{
  int ret = OB_SUCCESS;
  bool can_select_one_server = true;
  ObRoutePolicy::CandidateReplica tmp_replica;
  candidate_server_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && can_select_one_server && i < phy_tbl_loc_info_list.count(); ++i) {
    const ObCandiTableLoc *phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
    if (OB_ISNULL(phy_tbl_loc_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
    } else {
      const ObCandiTabletLocIArray &phy_part_loc_info_list = phy_tbl_loc_info->get_phy_part_loc_info_list();
      for (int64_t j = 0; OB_SUCC(ret) && can_select_one_server && j < phy_part_loc_info_list.count(); ++j) {
        const ObCandiTabletLoc &phy_part_loc_info = phy_part_loc_info_list.at(j);
        const ObIArray<ObRoutePolicy::CandidateReplica> &replica_loc_list = phy_part_loc_info.get_partition_location().get_replica_locations();
        if (0 == i && 0 == j) { // 第一个partition
          for (int64_t k = 0; OB_SUCC(ret) && k < replica_loc_list.count(); ++k) {
            if (OB_FAIL(candidate_server_list.push_back(replica_loc_list.at(k).get_server()))) {
              LOG_WARN("fail to push back candidate server", K(ret), K(k), K(replica_loc_list.at(k)));
            }
          }
        } else { // 不是第一个partition
          ObAddrList::iterator candidate_server_list_iter = candidate_server_list.begin();
          for (; OB_SUCC(ret) && candidate_server_list_iter != candidate_server_list.end(); candidate_server_list_iter++) {
            const ObAddr &candidate_server = *candidate_server_list_iter;
            bool has_replica = false;
            for (int64_t k = 0; OB_SUCC(ret) && !has_replica && k < replica_loc_list.count(); ++k) {
              if (replica_loc_list.at(k).get_server() == candidate_server) {
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

int ObLogPlan::select_one_server(const ObAddr &selected_server,
                                 ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_tbl_loc_info_list.count(); ++i) {
    ObCandiTableLoc *phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
    if (OB_ISNULL(phy_tbl_loc_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
    } else {
      ObCandiTabletLocIArray &phy_part_loc_info_list =
          phy_tbl_loc_info->get_phy_part_loc_info_list_for_update();
      for (int64_t j = 0; OB_SUCC(ret) && j < phy_part_loc_info_list.count(); ++j) {
        ObCandiTabletLoc &phy_part_loc_info = phy_part_loc_info_list.at(j);
        if (phy_part_loc_info.has_selected_replica()) {
          // 已经选好了，跳过
        } else {
          const ObIArray<ObRoutePolicy::CandidateReplica> &replica_loc_list =
              phy_part_loc_info.get_partition_location().get_replica_locations();
          bool replica_is_selected = false;
          for (int64_t k = 0; OB_SUCC(ret) && !replica_is_selected && k < replica_loc_list.count(); ++k) {
            if (selected_server == replica_loc_list.at(k).get_server()) {
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

int ObLogPlan::init_bushy_tree_info(const ObIArray<TableItem*> &table_items)
{
  int ret = OB_SUCCESS;
  ObIArray<LeadingInfo> &leading_infos = log_plan_hint_.join_order_.leading_infos_;
  for (int64_t i = 0; OB_SUCC(ret) && i < leading_infos.count(); ++i) {
    const LeadingInfo &info = leading_infos.at(i);
    if (info.left_table_set_.num_members() > 1 &&
        info.right_table_set_.num_members() > 1) {
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

int ObLogPlan::init_bushy_tree_info_from_joined_tables(TableItem *table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table->is_joined_table()) {
    //do nothing
  } else {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
    if (OB_ISNULL(joined_table->left_table_) ||
        OB_ISNULL(joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (joined_table->left_table_->is_joined_table() &&
               joined_table->right_table_->is_joined_table()) {
      ObRelIds table_ids;
      if (OB_FAIL(get_table_ids(table, table_ids))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (OB_FAIL(bushy_tree_infos_.push_back(table_ids))) {
        LOG_WARN("failed to push back table ids", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(SMART_CALL(init_bushy_tree_info_from_joined_tables(joined_table->left_table_)))) {
      LOG_WARN("failed to init bushy tree infos", K(ret));
    } else if (OB_FAIL(SMART_CALL(init_bushy_tree_info_from_joined_tables(joined_table->right_table_)))) {
      LOG_WARN("failed to init bushy tree infos", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::init_function_table_depend_info(const ObIArray<TableItem*> &table_items)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    TableItem *table = table_items.at(i);
    TableDependInfo info;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_function_table()) {
      //do nothing
    } else if (OB_ISNULL(table->function_table_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null function table expr", K(ret));
    } else if (table->function_table_expr_->get_relation_ids().is_empty()) {
      //do thing
    } else if (OB_FAIL(info.depend_table_set_.add_members(table->function_table_expr_->get_relation_ids()))) {
      LOG_WARN("failed to assign table ids", K(ret));
    } else if (OB_FALSE_IT(info.table_idx_ = stmt->get_table_bit_index(table->table_id_))) {
    } else if (OB_FAIL(table_depend_infos_.push_back(info))) {
      LOG_WARN("failed to push back info", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to init function table depend info", K(table_depend_infos_));
  }
  return ret;
}

int ObLogPlan::init_width_estimation_info(const ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (stmt->is_select_stmt()) {
    const ObSelectStmt *select_stmt = NULL;
    select_stmt = static_cast<const ObSelectStmt*>(stmt);
    // group by/rollup related info
    if (OB_FAIL(append_array_no_dup(groupby_rollup_exprs_, select_stmt->get_group_exprs()))) {
      LOG_WARN("failed to add group exprs into output exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(groupby_rollup_exprs_, select_stmt->get_rollup_exprs()))) {
      LOG_WARN("failed to add rollup exprs into output exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(having_exprs_, select_stmt->get_having_exprs()))) {
      LOG_WARN("failed to add having exprs into output exprs", K(ret));
    }
    // winfunc exprs
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_window_func_exprs().count(); ++i) {
        const ObWinFunRawExpr *winfunc_expr = select_stmt->get_window_func_expr(i);
        if (OB_ISNULL(winfunc_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid expr", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < winfunc_expr->get_partition_exprs().count(); ++j) {
            ObRawExpr *partition_by_expr = winfunc_expr->get_partition_exprs().at(j);
            if (OB_ISNULL(partition_by_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid expr", K(ret));
            } else if (OB_FAIL(add_var_to_array_no_dup(winfunc_exprs_, partition_by_expr))) {
              LOG_WARN("failed to do appending to current array", K(ret));
            }
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < winfunc_expr->get_order_items().count(); ++j) {
            OrderItem orderby_item = winfunc_expr->get_order_items().at(j);
            if (OB_ISNULL(orderby_item.expr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid expr", K(ret));
            } else if (OB_FAIL(add_var_to_array_no_dup(winfunc_exprs_, orderby_item.expr_))) {
              LOG_WARN("failed to do appending to current array", K(ret));
            }
          }
        }
      }
    }
    // select item related info
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
        SelectItem select_item = select_stmt->get_select_item(i);
        if (OB_ISNULL(select_item.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid expr", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(select_item_exprs_, select_item.expr_))) {
          LOG_WARN("failed to do appending to current array", K(ret));
        }
      }
    }
  }
  // condition exprs related info
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->get_where_scope_conditions(condition_exprs_))) {
      LOG_WARN("failed to add condition expr into output exprs", K(ret));
    } else {
      // order by related info
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_order_item_size(); ++i) {
        OrderItem orderby_item = stmt->get_order_item(i);
        if (OB_ISNULL(orderby_item.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid expr", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(orderby_exprs_, orderby_item.expr_))) {
          LOG_WARN("failed to do appending to current array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::init_default_val_json(ObRelIds& depend_table_set,
                                     ObRawExpr*& default_expr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(default_expr)) {
    if (default_expr->get_relation_ids().is_empty()) {
      //do nothing
    } else if (OB_FAIL(depend_table_set.add_members(default_expr->get_relation_ids()))) {
      LOG_WARN("failed to assign table ids", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::init_json_table_column_depend_info(ObRelIds& depend_table_set,
                                                   TableItem* json_table,
                                                   const ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ColumnItem* column_item = NULL;
  common::ObArray<ColumnItem> stmt_column_items;
  if (OB_ISNULL(stmt) || OB_ISNULL(json_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(stmt->get_column_items(json_table->table_id_, stmt_column_items))) {
    LOG_WARN("fail to get column_items", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt_column_items.count(); i++) {
    if (json_table->table_id_ != stmt_column_items.at(i).table_id_) {
    } else if (OB_NOT_NULL(stmt_column_items.at(i).default_value_expr_)
                && OB_FAIL(init_default_val_json(depend_table_set, stmt_column_items.at(i).default_value_expr_))) {
      LOG_WARN("fail to init error default value depend info", K(ret));
    } else if (OB_NOT_NULL(stmt_column_items.at(i).default_empty_expr_)
                && OB_FAIL(init_default_val_json(depend_table_set, stmt_column_items.at(i).default_empty_expr_))) {
      LOG_WARN("fail to init error default value depend info", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::init_json_table_depend_info(const ObIArray<TableItem*> &table_items)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    TableItem *table = table_items.at(i);
    TableDependInfo info;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_json_table()) {
      //do nothing
    } else if (OB_ISNULL(table->json_table_def_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null function table expr", K(ret));
    } else if (table->json_table_def_->doc_expr_->get_relation_ids().is_empty()) {
      //do thing
    } else if (OB_FAIL(info.depend_table_set_.add_members(table->json_table_def_->doc_expr_->get_relation_ids()))) {
      LOG_WARN("failed to assign table ids", K(ret));
    } else if (OB_FAIL(init_json_table_column_depend_info(info.depend_table_set_, table, stmt))) { // deal column items default value
      LOG_WARN("fail to init json table default value depend info", K(ret));
    } else if (OB_FALSE_IT(info.table_idx_ = stmt->get_table_bit_index(table->table_id_))) {
    } else if (OB_FAIL(table_depend_infos_.push_back(info))) {
      LOG_WARN("failed to push back info", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to init function table depend info", K(table_depend_infos_));
  }
  return ret;
}

int ObLogPlan::check_need_bushy_tree(common::ObIArray<JoinOrderArray> &join_rels,
                                    const int64_t level,
                                    bool &need)
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
    const ObRelIds &table_ids = bushy_tree_infos_.at(i);
    if (table_ids.num_members() != level + 1) {
      //do nothing
    } else {
      bool has_generated = false;
      int64_t N = join_rels.at(level).count();
      for (int64_t j = 0; OB_SUCC(ret) && !has_generated && j < N; ++j) {
        ObJoinOrder *join_order = join_rels.at(level).at(j);
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

int ObLogPlan::init_idp(int64_t initial_idp_step,
                        common::ObIArray<JoinOrderArray> &idp_join_rels,
                        common::ObIArray<JoinOrderArray> &full_join_rels)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(idp_join_rels.prepare_allocate(initial_idp_step))) {
    LOG_WARN("failed to prepare allocate join rels", K(ret));
  } else if (relid_joinorder_map_.created() || join_path_set_.created()) {
    relid_joinorder_map_.reuse();
    join_path_set_.reuse();
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < full_join_rels.at(0).count(); ++i) {
      if (OB_FAIL(idp_join_rels.at(0).push_back(full_join_rels.at(0).at(i)))) {
        LOG_WARN("failed to init level 0 rels", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_join_levels_with_IDP(common::ObIArray<JoinOrderArray> &join_rels)
{
  int ret = OB_SUCCESS;
  int64_t join_level = join_rels.count();
  if (has_join_order_hint() &&
      OB_FAIL(inner_generate_join_levels_with_IDP(join_rels,
                                                false))) {
    LOG_WARN("failed to generate join levels with hint", K(ret));
  } else if (1 == join_rels.at(join_level - 1).count()) {
    //根据hint，枚举到了有效join order
    OPT_TRACE("succeed to generate join order with hint");
  } else if (OB_FAIL(inner_generate_join_levels_with_IDP(join_rels,
                                                        true))) {
    LOG_WARN("failed to generate join level in generate_join_orders", K(ret), K(join_level));
  }
  return ret;
}

int ObLogPlan::generate_join_levels_with_orgleading(common::ObIArray<JoinOrderArray> &join_rels)
{
  int ret = OB_SUCCESS;
  ObArray<JoinOrderArray> temp_join_rels;
  int64_t join_level = join_rels.count();
  const ObDMLStmt *stmt = get_stmt();
  ObSEArray<TableItem*, 4> table_items;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(stmt), K(ret));
  } else if (OB_FAIL(get_from_table_items(stmt->get_from_items(), table_items))) {
      LOG_WARN("failed to get table items", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_infos().count(); ++i) {
    SemiInfo *semi_info = stmt->get_semi_infos().at(i);
    if (OB_ISNULL(semi_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null semi info", K(ret));
    } else {
      TableItem *table = stmt->get_table_item_by_id(semi_info->right_table_id_);
      ret = table_items.push_back(table);
    }
  }
  int64_t temp_join_level = table_items.count();
  LOG_TRACE("idp start enum join order with orig leading", K(temp_join_level));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(process_join_level_info(table_items,
                                             join_rels,
                                             temp_join_rels))) {
    LOG_WARN("failed to preprocess for linear", K(ret));
  } else if (OB_FAIL(generate_join_levels_with_IDP(temp_join_rels))) {
    LOG_WARN("failed to generate join level in generate_join_orders", K(ret), K(join_level));
  } else if (OB_FALSE_IT(join_rels.at(join_level - 1).reset())) {
  } else if (OB_FAIL(append(join_rels.at(join_level - 1),
                            temp_join_rels.at(temp_join_level -1)))) {
    LOG_WARN("failed to append join orders", K(ret));
  }
  return ret;
}

int ObLogPlan::inner_generate_join_levels_with_IDP(common::ObIArray<JoinOrderArray> &join_rels,
                                                   bool ignore_hint)
{
  int ret = OB_SUCCESS;
  // stop plan enumeration if
  // a. illegal ordered hint
  // b. idp path num exceeds limitation
  // c. idp enumeration failed, under bushy cases, etc
  ObIDPAbortType abort_type = ObIDPAbortType::IDP_NO_ABORT;
  uint32_t join_level = 0;
  ObArray<JoinOrderArray> temp_join_rels;
  if (ignore_hint) {
    OPT_TRACE("IDP without hint");
  } else {
    OPT_TRACE("IDP with hint");
  }
  if (join_rels.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect empty join rels", K(ret));
  } else {
    join_level = join_rels.at(0).count();
    uint32_t initial_idp_step = join_level;
    if (OB_FAIL(init_idp(initial_idp_step, temp_join_rels, join_rels))) {
      LOG_WARN("failed to init idp", K(ret));
    } else {
      uint32_t curr_idp_step = initial_idp_step;
      uint32_t curr_level = 1;
      for (uint32_t i = 1; OB_SUCC(ret) && i < join_level &&
          ObIDPAbortType::IDP_NO_ABORT == abort_type; i += curr_level) {
        if (curr_idp_step > join_level - i + 1) {
          curr_idp_step = join_level - i + 1;
        }
        LOG_TRACE("start new round of idp", K(i), K(curr_idp_step));
        OPT_TRACE("start new round of idp", KV(i), KV(curr_idp_step));
        if (OB_FAIL(do_one_round_idp(temp_join_rels,
                                    curr_idp_step,
                                    ignore_hint,
                                    curr_level,
                                    abort_type))) {
          LOG_WARN("failed to do idp internal", K(ret));
        } else if (ObIDPAbortType::IDP_INVALID_HINT_ABORT == abort_type) {
          LOG_WARN("failed to do idp internal", K(ret));
        } else if (temp_join_rels.at(curr_level).count() < 1) {
          abort_type = ObIDPAbortType::IDP_ENUM_FAILED_ABORT;
          OPT_TRACE("failed to enum join order at current level", KV(curr_level));
        } else {
          OPT_TRACE("end new round of idp", K(i), K(curr_idp_step));
          ObJoinOrder *best_order = NULL;
          bool is_last_round = (i >= join_level - curr_level);
          if (abort_type == ObIDPAbortType::IDP_STOPENUM_EXPDOWN_ABORT) {
            curr_idp_step /= 2;
            curr_idp_step = max(curr_idp_step, 2U);
          } else if (abort_type == ObIDPAbortType::IDP_STOPENUM_LINEARDOWN_ABORT) {
            curr_idp_step -= 2;
            curr_idp_step = max(curr_idp_step, 2U);
          }
          OPT_TRACE("select best join order");
          if (OB_FAIL(greedy_idp_best_order(curr_level,
                                            temp_join_rels,
                                            best_order))) {
            LOG_WARN("failed to greedy idp best order", K(ret));
          } else if (OB_ISNULL(best_order)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected invalid best order", K(ret));
          } else if (!is_last_round &&
                    OB_FAIL(prepare_next_round_idp(temp_join_rels,
                                                   initial_idp_step,
                                                   best_order))) {
            LOG_WARN("failed to prepare next round of idp", K(curr_level));
          } else {
            if (abort_type == ObIDPAbortType::IDP_STOPENUM_EXPDOWN_ABORT ||
                abort_type == ObIDPAbortType::IDP_STOPENUM_LINEARDOWN_ABORT) {
              abort_type = ObIDPAbortType::IDP_NO_ABORT;
            }
            if (is_last_round) {
              if (OB_FALSE_IT(join_rels.at(join_level - 1).reset())) {
              } else if (OB_FAIL(append(join_rels.at(join_level - 1),
                                        temp_join_rels.at(curr_level)))) {
                LOG_WARN("failed to append join orders", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::do_one_round_idp(common::ObIArray<JoinOrderArray> &temp_join_rels,
                                uint32_t curr_idp_step,
                                bool ignore_hint,
                                uint32_t &real_base_level,
                                ObIDPAbortType &abort_type)
{
  int ret = OB_SUCCESS;
  real_base_level = 1;
  for (uint32_t base_level = 1; OB_SUCC(ret) && base_level < curr_idp_step &&
       ObIDPAbortType::IDP_NO_ABORT == abort_type; ++base_level) {
    LOG_TRACE("idp start base level plan enumeration", K(base_level), K(curr_idp_step));
    OPT_TRACE("idp start base level plan enumeration", K(base_level), K(curr_idp_step));
    for (uint32_t i = 0; OB_SUCC(ret) && i <= base_level/2 &&
         ObIDPAbortType::IDP_NO_ABORT == abort_type; ++i) {
      uint32_t right_level = i;
      uint32_t left_level = base_level - 1 - right_level;
      if (right_level > left_level) {
        //do nothing
      } else if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status fail", K(ret));
      } else if (OB_FAIL(generate_single_join_level_with_DP(temp_join_rels,
                                                            left_level,
                                                            right_level,
                                                            base_level,
                                                            ignore_hint,
                                                            abort_type))) {
        LOG_WARN("failed to generate join order with dynamic program", K(ret));
      }
      bool need_bushy = false;
      if (OB_FAIL(ret) || right_level > left_level) {
      } else if (abort_type < ObIDPAbortType::IDP_NO_ABORT) {
        LOG_TRACE("abort idp join order generation",
                  K(left_level), K(right_level), K(abort_type));
      } else if (temp_join_rels.count() <= base_level) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("Index out of range", K(ret), K(temp_join_rels.count()), K(base_level));
      } else if (OB_FAIL(check_need_bushy_tree(temp_join_rels,
                                              base_level,
                                              need_bushy))) {
        LOG_WARN("failed to check need bushy tree", K(ret));
      } else if (need_bushy) {
        OPT_TRACE("no valid ZigZag tree or leading hint required, we will enumerate bushy tree");
      } else {
        //如果当前level已经枚举到了有效计划，默认关闭bushy tree
        OPT_TRACE("there is valid ZigZag tree, we will not enumerate bushy tree");
        break;
      }
    }
    if (OB_SUCC(ret)) {
      real_base_level = base_level;
      if (abort_type == ObIDPAbortType::IDP_NO_ABORT &&
          OB_FAIL(check_and_abort_curr_round_idp(temp_join_rels,
                                                 base_level,
                                                 abort_type))) {
        LOG_WARN("failed to check and abort current round idp", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::check_and_abort_curr_level_dp(common::ObIArray<JoinOrderArray> &idp_join_rels,
                                             uint32_t curr_level,
                                             ObIDPAbortType &abort_type)
{
  int ret = OB_SUCCESS;
  if (curr_level < 0 || curr_level >= idp_join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(idp_join_rels.count()), K(curr_level));
  } else if (abort_type < ObIDPAbortType::IDP_NO_ABORT) {
    // do nothing
  } else {
    uint64_t total_path_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < idp_join_rels.at(curr_level).count(); ++i) {
      total_path_num += idp_join_rels.at(curr_level).at(i)->get_total_path_num();
    }
    if (OB_SUCC(ret)) {
      uint64_t stop_down_abort_limit = 2 * IDP_PATHNUM_THRESHOLD;
      if (total_path_num >= stop_down_abort_limit) {
        abort_type = ObIDPAbortType::IDP_STOPENUM_EXPDOWN_ABORT;
        OPT_TRACE("there is too much path, we will stop current level idp ",
        KV(total_path_num), KV(stop_down_abort_limit));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObLogPlan::check_and_abort_curr_round_idp(common::ObIArray<JoinOrderArray> &idp_join_rels,
                                              uint32_t curr_level,
                                              ObIDPAbortType &abort_type)
{
  int ret = OB_SUCCESS;
  if (curr_level < 0 || curr_level >= idp_join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(idp_join_rels.count()), K(curr_level));
  } else if (abort_type < ObIDPAbortType::IDP_NO_ABORT) {
    // do nothing
  } else {
    uint64_t total_path_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < idp_join_rels.at(curr_level).count(); ++i) {
      total_path_num += idp_join_rels.at(curr_level).at(i)->get_total_path_num();
    }
    if (OB_SUCC(ret)) {
      uint64_t stop_abort_limit = IDP_PATHNUM_THRESHOLD;
      if (total_path_num >= stop_abort_limit) {
        abort_type = ObIDPAbortType::IDP_STOPENUM_LINEARDOWN_ABORT;
        OPT_TRACE("there is too much path, we will stop current round idp ",
        KV(total_path_num), KV(stop_abort_limit));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObLogPlan::prepare_next_round_idp(common::ObIArray<JoinOrderArray> &idp_join_rels,
                                      uint32_t initial_idp_step,
                                      ObJoinOrder *&best_order)
{
  int ret = OB_SUCCESS;
  JoinOrderArray remained_rels;
  if (OB_ISNULL(best_order)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid best order", K(best_order), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < idp_join_rels.at(0).count(); ++i) {
      if (!idp_join_rels.at(0).at(i)->get_tables().overlap2(best_order->get_tables())) {
        if (OB_FAIL(remained_rels.push_back(idp_join_rels.at(0).at(i)))) {
          LOG_WARN("failed to add level 0 rels", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t j = 0; OB_SUCC(ret) && j < initial_idp_step; ++j) {
        idp_join_rels.at(j).reuse();
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(idp_join_rels.at(0).assign(remained_rels))) {
          LOG_WARN("failed to add remained rels", K(ret));
        } else if (OB_FAIL(idp_join_rels.at(0).push_back(best_order))) {
          LOG_WARN("failed to add selected tree rels", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::greedy_idp_best_order(uint32_t current_level,
                                     common::ObIArray<JoinOrderArray> &idp_join_rels,
                                     ObJoinOrder *&best_order)
{
  // choose best order from join_rels at current_level
  // can be based on cost/min cards/max interesting path num
  // currently based on cost
  int ret = OB_SUCCESS;
  best_order = NULL;
  if (current_level < 0 ||
      current_level >= idp_join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", K(ret), K(idp_join_rels.count()), K(current_level));
  } else if (idp_join_rels.at(current_level).count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("illegal join rels at current level", K(ret));
  }
  if (OB_SUCC(ret)) {
    Path *min_cost_path = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < idp_join_rels.at(current_level).count(); ++i) {
      ObJoinOrder * join_order = idp_join_rels.at(current_level).at(i);
      if (OB_ISNULL(join_order)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid join order found", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < join_order->get_interesting_paths().count(); ++j) {
          Path *path = join_order->get_interesting_paths().at(j);
          if (OB_ISNULL(path)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid join order found", K(ret));
          } else if (NULL == min_cost_path || min_cost_path->cost_ > path->cost_) {
            best_order = join_order;
            min_cost_path = path;
          }
        }
      }
    }
  }
  return ret;
}

/**
 * 进行join order枚举前，需要为所有的joined table生成join order
 * 然后把joined table当前整体进行join reorder
 **/
int ObLogPlan::process_join_level_info(const ObIArray<TableItem*> &table_items,
                                      ObIArray<JoinOrderArray> &join_rels,
                                      ObIArray<JoinOrderArray> &new_join_rels)
{
  int ret = OB_SUCCESS;
  int64_t new_join_level = table_items.count();
  if (join_rels.empty()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("join_rels is empty", K(ret), K(join_rels.count()));
  } else if (OB_FAIL(new_join_rels.prepare_allocate(new_join_level))) {
    LOG_WARN("failed to prepare allocate join rels", K(ret));
  } else if (relid_joinorder_map_.created() || join_path_set_.created()) {
    // clear relid_joinorder map to avoid existing join order misuse
    relid_joinorder_map_.reuse();
    join_path_set_.reuse();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    ObJoinOrder *join_tree = NULL;
    if (OB_FAIL(generate_join_order_with_table_tree(join_rels,
                                                    table_items.at(i),
                                                    join_tree))) {
      LOG_WARN("failed to generate join order", K(ret));
    } else if (OB_ISNULL(join_tree)) {
      ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
      LOG_WARN("no valid join order generated", K(ret));
    } else if (OB_FAIL(new_join_rels.at(0).push_back(join_tree))) {
      LOG_WARN("failed to push back base level join order", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::generate_join_order_with_table_tree(ObIArray<JoinOrderArray> &join_rels,
                                                  TableItem *table,
                                                  ObJoinOrder* &join_tree)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = NULL;
  ObJoinOrder *left_tree = NULL;
  ObJoinOrder *right_tree = NULL;
  join_tree = NULL;
  if (OB_ISNULL(table) || join_rels.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (!table->is_joined_table()) {
    //find base join rels
    ObIArray<ObJoinOrder *> &single_join_rels = join_rels.at(0);
    for (int64_t i = 0; OB_SUCC(ret) && NULL == join_tree && i < single_join_rels.count(); ++i) {
      ObJoinOrder *base_rel = single_join_rels.at(i);
      if (OB_ISNULL(base_rel)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null base rel", K(ret));
      } else if (table->table_id_ == base_rel->get_table_id()) {
        join_tree = base_rel;
      }
    }
  } else if (OB_FALSE_IT(joined_table = static_cast<JoinedTable*>(table))) {
    // do nothing
  } else if (OB_FAIL(SMART_CALL(generate_join_order_with_table_tree(join_rels,
                                                                    joined_table->left_table_,
                                                                    left_tree)))) {
    LOG_WARN("failed to generate join order", K(ret));
  } else if (OB_FAIL(SMART_CALL(generate_join_order_with_table_tree(join_rels,
                                                                    joined_table->right_table_,
                                                                    right_tree)))) {
    LOG_WARN("failed to generate join order", K(ret));
  } else if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_NO_JOIN_ORDER_GENERATED;
    LOG_WARN("no valid join order generated", K(ret));
  } else {
    bool is_valid_join = false;
    int64_t level = left_tree->get_tables().num_members() + right_tree->get_tables().num_members() - 1;
    if (OB_FAIL(inner_generate_join_order(join_rels,
                                          left_tree,
                                          right_tree,
                                          level,
                                          false,
                                          false,
                                          is_valid_join,
                                          join_tree))) {
      LOG_WARN("failed to generated join order", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::generate_single_join_level_with_DP(ObIArray<JoinOrderArray> &join_rels,
                                                  uint32_t left_level,
                                                  uint32_t right_level,
                                                  uint32_t level,
                                                  bool ignore_hint,
                                                  ObIDPAbortType &abort_type)
{
  int ret = OB_SUCCESS;
  abort_type = ObIDPAbortType::IDP_NO_ABORT;
  if (join_rels.empty() ||
      left_level >= join_rels.count() ||
      right_level >= join_rels.count() ||
      level >= join_rels.count()) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(join_rels.count()),
                          K(left_level), K(right_level), K(level));
  } else {
    ObIArray<ObJoinOrder *> &left_rels = join_rels.at(left_level);
    ObIArray<ObJoinOrder *> &right_rels = join_rels.at(right_level);
    ObJoinOrder *left_tree = NULL;
    ObJoinOrder *right_tree = NULL;
    ObJoinOrder *join_tree = NULL;
    //优先枚举有连接条件的join order
    for (int64_t i = 0; OB_SUCC(ret) && i < left_rels.count() &&
         ObIDPAbortType::IDP_NO_ABORT == abort_type; ++i) {
      left_tree = left_rels.at(i);
      if (OB_ISNULL(left_tree)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null join tree", K(ret));
      } else {
        OPT_TRACE("Permutations for Starting Table :", left_tree);
        OPT_TRACE_BEGIN_SECTION;
        for (int64_t j = 0; OB_SUCC(ret) && j < right_rels.count() &&
             ObIDPAbortType::IDP_NO_ABORT == abort_type; ++j) {
          right_tree = right_rels.at(j);
          bool match_hint = false;
          bool is_legal = true;
          bool is_strict_order = true;
          bool is_valid_join = false;
          if (OB_ISNULL(right_tree)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null join tree", K(ret));
          } else if (!ignore_hint &&
                    OB_FAIL(check_join_hint(left_tree->get_tables(),
                                            right_tree->get_tables(),
                                            match_hint,
                                            is_legal,
                                            is_strict_order))) {
            LOG_WARN("failed to check join hint", K(ret));
          } else if (!is_legal) {
            //与hint冲突
            OPT_TRACE("join order conflict with leading hint,", left_tree, right_tree);
            LOG_TRACE("join order conflict with leading hint",
                      K(left_tree->get_tables()), K(right_tree->get_tables()));
          } else if (OB_FAIL(inner_generate_join_order(join_rels,
                                                        is_strict_order ? left_tree : right_tree,
                                                        is_strict_order ? right_tree : left_tree,
                                                        level,
                                                        match_hint,
                                                        !match_hint,
                                                        is_valid_join,
                                                        join_tree))) {
            LOG_WARN("failed to generate join order", K(level), K(ret));
          } else if (match_hint &&
                     !get_leading_tables().is_subset(left_tree->get_tables()) &&
                     !is_valid_join) {
            abort_type = ObIDPAbortType::IDP_INVALID_HINT_ABORT;
            OPT_TRACE("leading hint is invalid, stop idp ", left_tree, right_tree);
          } else if (OB_FAIL(check_and_abort_curr_level_dp(join_rels,
                                                           level,
                                                           abort_type))) {
            LOG_WARN("failed to check abort current dp", K(level), K(abort_type));
          } else {
            LOG_TRACE("succeed to generate join order", K(left_tree->get_tables()),
                       K(right_tree->get_tables()), K(is_valid_join), K(abort_type));
          }
          OPT_TRACE_TIME_USED;
          OPT_TRACE_MEM_USED;
        }
        OPT_TRACE_END_SECTION;
      }
    }
  }
  return ret;
}

/**
 * 使用动态规划算法
 * 通过join_rels[left_level]组合join_rels[right_level]
 * 来枚举join_rels[level]的有效计划
 */
int ObLogPlan::inner_generate_join_order(ObIArray<JoinOrderArray> &join_rels,
                                        ObJoinOrder *left_tree,
                                        ObJoinOrder *right_tree,
                                        uint32_t level,
                                        bool hint_force_order,
                                        bool delay_cross_product,
                                        bool &is_valid_join,
                                        ObJoinOrder *&join_tree)
{
  int ret = OB_SUCCESS;
  is_valid_join = false;
  join_tree = NULL;
  bool need_gen = true;
  if (join_rels.empty() || level >= join_rels.count() ||
      OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Index out of range", K(ret), K(join_rels.count()),
                          K(left_tree), K(right_tree), K(level));
  } else {
    //依次检查每一个join info，是否有合法的连接
    ObSEArray<ConflictDetector*, 4> valid_detectors;
    ObRelIds cur_relids;
    JoinInfo join_info;
    bool is_strict_order = true;
    bool is_detector_valid = true;
    if (left_tree->get_tables().overlap(right_tree->get_tables())) {
      //非法连接，do nothing
    } else if (OB_FAIL(cur_relids.add_members(left_tree->get_tables()))) {
      LOG_WARN("fail to add left tree' table ids", K(ret));
    } else if (OB_FAIL(cur_relids.add_members(right_tree->get_tables()))) {
      LOG_WARN("fail to add right tree' table ids", K(ret));
    } else if (OB_FAIL(find_join_rel(cur_relids, join_tree))) {
      LOG_WARN("fail to find join rel", K(ret), K(level));
    } else if (OB_FAIL(check_need_gen_join_path(left_tree, right_tree, need_gen))) {
      LOG_WARN("failed to find join tree pair", K(ret));
    } else if (!need_gen) {
      // do nothing
      is_valid_join = true;
      OPT_TRACE(left_tree, right_tree,
      " is legal, and has join path cache, no need to generate join path");
    } else if (OB_FAIL(choose_join_info(left_tree,
                                        right_tree,
                                        valid_detectors,
                                        delay_cross_product,
                                        is_strict_order))) {
      LOG_WARN("failed to choose join info", K(ret));
    } else if (valid_detectors.empty()) {
      OPT_TRACE("there is no valid join condition for ", left_tree, "join", right_tree);
      LOG_TRACE("there is no valid join info for ", K(left_tree->get_tables()),
                                                    K(right_tree->get_tables()));
    } else if (NULL != join_tree &&
               OB_FAIL(check_detector_valid(left_tree,
                                            right_tree,
                                            valid_detectors,
                                            join_tree,
                                            is_detector_valid))) {
        LOG_WARN("failed to check detector valid", K(ret));
      } else if (!is_detector_valid) {
        OPT_TRACE("join tree will be remove: ", left_tree, "join", right_tree);
      } else if (OB_FAIL(merge_join_info(left_tree,
                                       right_tree,
                                       valid_detectors,
                                       join_info))) {
      LOG_WARN("failed to merge join info", K(ret));
    } else if (NULL != join_tree && level <= 1 && !hint_force_order) {
      //level==1的时候，左右树都是单表，如果已经生成过AB的话，BA的path也已经生成了，没必要再次生成一遍BA
      is_valid_join = true;
      OPT_TRACE("path has been generated in level one");
    } else {
      if (!is_strict_order) {
        if (!hint_force_order) {
          std::swap(left_tree, right_tree);
        } else {
          //如果leading hint指定了join order，但是合法的join order与
          //leading hint指定的join order相反，那么应该把连接类型取反
          join_info.join_type_ = get_opposite_join_type(join_info.join_type_);
        }
      }
      JoinPathPairInfo pair;
      pair.left_ids_ = left_tree->get_tables();
      pair.right_ids_ = right_tree->get_tables();
      if (NULL == join_tree) {
        if(OB_ISNULL(join_tree = create_join_order(JOIN))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create join tree", K(ret));
        } else if (OB_FAIL(join_tree->init_join_order(left_tree,
                                                      right_tree,
                                                      &join_info,
                                                      valid_detectors))) {
          LOG_WARN("failed to init join order", K(ret));
        } else if (OB_FAIL(join_rels.at(level).push_back(join_tree))) {
          LOG_WARN("failed to push back join order", K(ret));
        } else if (relid_joinorder_map_.created() &&
                   OB_FAIL(relid_joinorder_map_.set_refactored(join_tree->get_tables(), join_tree))) {
          LOG_WARN("failed to add table ids join order to hash map", K(ret));
        }
      }
      OPT_TRACE_TITLE("Now", left_tree, "join", right_tree, join_info);
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (OB_FAIL(join_tree->revise_cardinality(left_tree,
                                                       right_tree,
                                                       join_info))) {
        LOG_WARN("failed to revise ambient card", K(ret));
      } else if (OB_FAIL(join_tree->generate_join_paths(*left_tree,
                                                        *right_tree,
                                                        join_info,
                                                        hint_force_order))) {
        LOG_WARN("failed to generate join paths for left_tree", K(level), K(ret));
      } else if (join_path_set_.created() &&
                 OB_FAIL(join_path_set_.set_refactored(pair))) {
        LOG_WARN("failed to add join path set", K(ret));
      } else {
        is_valid_join = true;
        LOG_TRACE("succeed to generate join order for ", K(left_tree->get_tables()),
                  K(right_tree->get_tables()), K(is_strict_order));
      }
    }
  }
  return ret;
}

int ObLogPlan::is_detector_used(ObJoinOrder *left_tree,
                                ObJoinOrder *right_tree,
                                ConflictDetector *detector,
                                bool &is_used)
{
  int ret = OB_SUCCESS;
  is_used = false;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (INNER_JOIN == detector->join_info_.join_type_ &&
             detector->join_info_.where_conditions_.empty()) {
    //笛卡尔积的冲突检测器可以重复使用
  }else if (ObOptimizerUtil::find_item(left_tree->get_conflict_detectors(), detector)) {
    is_used = true;
  } else if (ObOptimizerUtil::find_item(right_tree->get_conflict_detectors(), detector)) {
    is_used = true;
  }
  return ret;
}

/**
 * 为左右连接树选择合法的连接条件
 * is_strict_order：
 * true：left join right是合法的，right join left是非法的
 * false：left join right是非法的，right join left是合法的
 */
int ObLogPlan::choose_join_info(ObJoinOrder *left_tree,
                                ObJoinOrder *right_tree,
                                ObIArray<ConflictDetector*> &valid_detectors,
                                bool delay_cross_product,
                                bool &is_strict_order)
{
  int ret = OB_SUCCESS;
  bool is_legal = false;
  ObRelIds combined_relids;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect empty conflict detectors", K(ret));
  } else if (OB_FAIL(combined_relids.add_members(left_tree->get_tables()))) {
    LOG_WARN("failed to add left relids into combined relids", K(ret));
  } else if (OB_FAIL(combined_relids.add_members(right_tree->get_tables()))) {
    LOG_WARN("failed to add right relids into combined relids", K(ret));
  }
  is_strict_order = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < conflict_detectors_.count(); ++i) {
    ConflictDetector *detector = conflict_detectors_.at(i);
    bool is_used = false;
    if (OB_ISNULL(detector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("conflict detector is null", K(ret));
    } else if (OB_FAIL(is_detector_used(left_tree,
                                        right_tree,
                                        detector,
                                        is_used))) {
      LOG_WARN("failed to check detector is used", K(ret));
    } else if (is_used) {
      //do nothing
    } else if (OB_FAIL(check_join_legal(left_tree->get_tables(),
                                        right_tree->get_tables(),
                                        combined_relids,
                                        detector,
                                        delay_cross_product,
                                        is_legal))) {
      LOG_WARN("failed to check join legal", K(ret));
    } else if (!is_legal) {
      //对于可交换的join如inner join，既left join right合法
      //也right join left合法，但是我们只需要保存left inner join right
      //因为在generate join path的时候会生成right inner join left的path
      //并且不可能存在一种join，既有left join1 right合法，又有right join2 left合法
      LOG_TRACE("left tree join right tree is not legal", K(left_tree->get_tables()),
                K(right_tree->get_tables()), K(*detector));
      //尝试right tree join left tree
      if (OB_FAIL(check_join_legal(right_tree->get_tables(),
                                   left_tree->get_tables(),
                                   combined_relids,
                                   detector,
                                   delay_cross_product,
                                   is_legal))) {
        LOG_WARN("failed to check join legal", K(ret));
      } else if (!is_legal) {
        LOG_TRACE("right tree join left tree is not legal", K(right_tree->get_tables()),
                K(left_tree->get_tables()), K(*detector));
      } else if (OB_FAIL(valid_detectors.push_back(detector))) {
        LOG_WARN("failed to push back detector", K(ret));
      } else {
        is_strict_order = false;
        LOG_TRACE("succeed to find join info for ", K(left_tree->get_tables()),
                  K(right_tree->get_tables()), K(left_tree->get_conflict_detectors()),
                  K(right_tree->get_conflict_detectors()), K(*detector));
      }
    } else if (OB_FAIL(valid_detectors.push_back(detector))) {
      LOG_WARN("failed to push back detector", K(ret));
    } else {
      LOG_TRACE("succeed to find join info for ", K(left_tree->get_tables()),
                  K(right_tree->get_tables()), K(left_tree->get_conflict_detectors()),
                  K(right_tree->get_conflict_detectors()), K(*detector));
    }
  }
  return ret;
}

/**
 * 只允许多个inner join叠加成一个join info
 * 例如：select * from A, B where A.c1 = B.c1 and A.c2 = B.c2
 * 或者单个OUTER JOIN加上多个inner join info，inner join info作为join qual
 * 例如：select * from A left join B on A.c1 = B.c1 where A.c2 = B.c2
 */
int ObLogPlan::check_join_info(const ObIArray<ConflictDetector*> &valid_detectors,
                               ObJoinType &join_type,
                               bool &is_valid)
{
  int ret = OB_SUCCESS;
  ConflictDetector *detector = NULL;
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
      //do nothing
    } else if (has_non_inner_join) {
      //不允许出现多个非inner join的join info
      is_valid = false;
    } else {
      has_non_inner_join = true;
      join_type = detector->join_info_.join_type_;
    }
  }
  return ret;
}

int ObLogPlan::merge_join_info(ObJoinOrder *left_tree,
                               ObJoinOrder *right_tree,
                               const ObIArray<ConflictDetector*> &valid_detectors,
                               JoinInfo &join_info)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  if (OB_FAIL(check_join_info(valid_detectors,
                              join_info.join_type_,
                              is_valid))) {
    LOG_WARN("failed to check join info", K(ret));
  } else if (!is_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect different join info", K(valid_detectors), K(ret));
  } else {
    ConflictDetector *detector = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_detectors.count(); ++i) {
      detector = valid_detectors.at(i);
      if (OB_ISNULL(detector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null detectors", K(ret));
      } else if (OB_FAIL(join_info.table_set_.add_members(detector->join_info_.table_set_))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(append_array_no_dup(join_info.where_conditions_, detector->join_info_.where_conditions_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (INNER_JOIN == join_info.join_type_ &&
                 OB_FAIL(append_array_no_dup(join_info.where_conditions_, detector->join_info_.on_conditions_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (INNER_JOIN != join_info.join_type_ &&
                 OB_FAIL(append_array_no_dup(join_info.on_conditions_, detector->join_info_.on_conditions_))) {
        LOG_WARN("failed to append exprs", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // TODO: need to optimize this hotspot
      if (OB_FAIL(remove_redundancy_pred(left_tree, right_tree, join_info))) {
        LOG_WARN("failed to remove redundancy pred", K(ret));
      }
    }
    //抽取简单join condition
    if (OB_SUCC(ret)) {
      if (INNER_JOIN == join_info.join_type_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < join_info.where_conditions_.count(); ++i) {
          ObRawExpr *qual = join_info.where_conditions_.at(i);
          if (OB_ISNULL(qual)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null join qual", K(ret));
          } else if (!qual->has_flag(IS_JOIN_COND)) {
            //do nothing
          } else if (OB_FAIL(join_info.equal_join_conditions_.push_back(qual))) {
            LOG_WARN("failed to push back join qual", K(ret));
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < join_info.on_conditions_.count(); ++i) {
          ObRawExpr *qual = join_info.on_conditions_.at(i);
          if (OB_ISNULL(qual)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null join qual", K(ret));
          } else if (!qual->has_flag(IS_JOIN_COND)) {
            //do nothing
          } else if (OB_FAIL(join_info.equal_join_conditions_.push_back(qual))) {
            LOG_WARN("failed to push back join qual", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::check_detector_valid(ObJoinOrder *left_tree,
                                    ObJoinOrder *right_tree,
                                    const ObIArray<ConflictDetector*> &valid_detectors,
                                    ObJoinOrder *cur_tree,
                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<ConflictDetector*, 4> all_detectors;
  ObSEArray<ConflictDetector*, 4> common_detectors;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree) || OB_ISNULL(cur_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_detectors, left_tree->get_conflict_detectors()))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_detectors, right_tree->get_conflict_detectors()))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_detectors, valid_detectors))) {
    LOG_WARN("failed to append detectors", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::intersect(all_detectors, cur_tree->get_conflict_detectors(), common_detectors))) {
    LOG_WARN("failed to calc common detectors", K(ret));
  } else if (common_detectors.count() != all_detectors.count() ||
             common_detectors.count() != cur_tree->get_conflict_detectors().count()) {
    is_valid = false;
  }
  return ret;
}

/**
 * Try keep EQ preds that join same two tables.
 * For example, we will keep the two preds which join t1 and t3 in the below case.
 * (t1 join t2 on t1.c1 = t2.c1)
 *   join
 * (t3 join t4 on t3.c2 = t4.c2)
 *   on t1.c1 = t3.c1 and t2.c1 = t3.c1 and t1.c2 = t3.c2 and t1.c2 = t4.c2
 * =>
 * (t1 join t2 on t1.c1 = t2.c1)
 *   join
 * (t3 join t4 on t3.c2 = t4.c2)
 *   on t1.c1 = t3.c1 and t1.c2 = t3.c2
 *
 * Remove preds which is equation between two exprs in the same equal sets
 * (t1 where c1 = 1) join (t2 where c2 = 1) on t1.c1 = t2.c1
 *  => (t1 where c1 = 1) join (t2 where c2 = 1) on true
 * */
int ObLogPlan::remove_redundancy_pred(ObJoinOrder *left_tree,
                                      ObJoinOrder *right_tree,
                                      JoinInfo &join_info)
{
  int ret = OB_SUCCESS;
  if (INNER_JOIN == join_info.join_type_ ||
      LEFT_SEMI_JOIN == join_info.join_type_ ||
      LEFT_ANTI_JOIN == join_info.join_type_ ||
      RIGHT_SEMI_JOIN == join_info.join_type_ ||
      RIGHT_ANTI_JOIN == join_info.join_type_ ||
      LEFT_OUTER_JOIN == join_info.join_type_ ||
      RIGHT_OUTER_JOIN == join_info.join_type_) {
    ObIArray<ObRawExpr*> &join_pred = IS_NOT_INNER_JOIN(join_info.join_type_) ?
                                      join_info.on_conditions_ :
                                      join_info.where_conditions_;
    EqualSets input_equal_sets;
    if (OB_FAIL(ObEqualAnalysis::merge_equal_set(&allocator_,
                                                        left_tree->get_output_equal_sets(),
                                                        right_tree->get_output_equal_sets(),
                                                        input_equal_sets))) {
      LOG_WARN("failed to compute equal sets for inner join", K(ret));
    } else if (OB_FAIL(inner_remove_redundancy_pred(join_pred,
                                                    input_equal_sets,
                                                    left_tree,
                                                    right_tree))) {
      LOG_WARN("failed to inner remove redundancy pred", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::join_side_from_one_table(ObJoinOrder &child_tree,
                                        ObIArray<ObRawExpr*> &join_pred,
                                        bool &is_valid,
                                        ObRelIds &intersect_rel_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRelIds, 4> eq_sets_rel_ids;
  ObRawExpr *expr_in_tree = NULL;
  intersect_rel_ids.reuse();
  is_valid = true;
  if (OB_FAIL(intersect_rel_ids.add_members(child_tree.get_tables()))){
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::build_rel_ids_by_equal_sets(child_tree.get_output_equal_sets(),
                                                                  eq_sets_rel_ids))) {
    LOG_WARN("failed to build rel ids", K(ret));
  }
  const ObRelIds &tree_rel_ids = child_tree.get_tables();
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < join_pred.count(); i++) {
    ObRawExpr *cur_expr = join_pred.at(i);
    ObRawExpr *left_expr = NULL;
    ObRawExpr *right_expr = NULL;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_expr));
    } else if (T_OP_EQ == cur_expr->get_expr_type()) {
      if (OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
          OB_ISNULL(right_expr = cur_expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), KPC(cur_expr), K(left_expr), K(right_expr));
      } else if (tree_rel_ids.is_superset(left_expr->get_relation_ids())) {
        expr_in_tree = left_expr;
      } else if (tree_rel_ids.is_superset(right_expr->get_relation_ids())) {
        expr_in_tree = right_expr;
      } else {
        is_valid = false;
      }
      if (OB_SUCC(ret) && is_valid) {
        const ObRelIds &expr_rel_ids = expr_in_tree->get_relation_ids();
        int64_t eq_set_idx = OB_INVALID_ID;
        if (expr_rel_ids.num_members() != 1) {
          is_valid = false;
        } else if (OB_FAIL(ObOptimizerUtil::find_expr_in_equal_sets(child_tree.get_output_equal_sets(),
                                                                    expr_in_tree,
                                                                    eq_set_idx))) {
          LOG_WARN("failed to find expr", K(ret));
        } else {
          const ObRelIds &to_intersect = (eq_set_idx == OB_INVALID_ID) ?
                                          expr_rel_ids :
                                          eq_sets_rel_ids.at(eq_set_idx);
          if (OB_FAIL(intersect_rel_ids.intersect_members(to_intersect))) {
            LOG_WARN("failed to intersect", K(ret));
          } else if (intersect_rel_ids.is_empty()) {
            is_valid = false;
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) || !is_valid) {
  } else if (OB_FAIL(intersect_rel_ids.reserve_first())) {
    LOG_WARN("failed to reserve first", K(ret));
  }

  return ret;
}

// Check if there are some predicates that were lost
// for situations where predicate derivation did not happen or predicate derivation was incomplete
int ObLogPlan::re_add_necessary_predicate(ObIArray<ObRawExpr*> &join_pred,
                                          ObIArray<ObRawExpr*> &new_join_pred,
                                          ObIArray<bool> &skip,
                                          EqualSets &equal_sets)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(join_pred.count() != skip.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < join_pred.count(); i++) {
    ObRawExpr *cur_expr= join_pred.at(i);
    ObRawExpr *left_expr = NULL;
    ObRawExpr *right_expr = NULL;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_expr));
    } else if (skip.at(i)) {
      // skip
    } else if (OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
               OB_ISNULL(right_expr = cur_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(left_expr), K(right_expr));
    } else if (ObOptimizerUtil::is_expr_equivalent(left_expr,
                                                   right_expr,
                                                   equal_sets)) {
      // remove preds which is equation between two exprs in the same equal sets
      OPT_TRACE("remove redundancy join condition:", cur_expr);
    } else {
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < new_join_pred.count(); j++) {
        ObRawExpr *cur_new_expr = new_join_pred.at(j);
        ObRawExpr *left_new_expr = NULL;
        ObRawExpr *right_new_expr = NULL;
        if (OB_ISNULL(cur_new_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (cur_expr->get_expr_type() != cur_new_expr->get_expr_type()) {
          // skip
        } else if(OB_ISNULL(left_new_expr = cur_new_expr->get_param_expr(0)) ||
                  OB_ISNULL(right_new_expr = cur_new_expr->get_param_expr(1))) {
          LOG_WARN("get unexpected null", K(ret), K(left_new_expr), K(right_new_expr));
        } else if (ObOptimizerUtil::is_expr_equivalent(left_expr,
                                                       left_new_expr,
                                                       equal_sets) &&
                   ObOptimizerUtil::is_expr_equivalent(right_expr,
                                                       right_new_expr,
                                                       equal_sets)) {
          find = true;
        } else if (ObOptimizerUtil::is_expr_equivalent(left_expr,
                                                       right_new_expr,
                                                       equal_sets) &&
                   ObOptimizerUtil::is_expr_equivalent(right_expr,
                                                       left_new_expr,
                                                       equal_sets)) {
          find = true;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!find && OB_FAIL(new_join_pred.push_back(cur_expr))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (find) {
        // remove preds which do not join the given two tables
        OPT_TRACE("remove redundancy join condition:", cur_expr);
      }
    }
  }
  return ret;
}

int ObLogPlan::inner_remove_redundancy_pred(ObIArray<ObRawExpr*> &join_pred,
                                            EqualSets &equal_sets,
                                            ObJoinOrder *left_tree,
                                            ObJoinOrder *right_tree)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_join_pred;
  ObSEArray<bool, 4> has_checked;
  bool join_two_tables = true;
  ObRelIds left_table, right_table;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(join_side_from_one_table(*left_tree,
                                              join_pred,
                                              join_two_tables,
                                              left_table))) {
    LOG_WARN("failed to check there is only one left table", K(ret));
  } else if (join_two_tables &&
             OB_FAIL(join_side_from_one_table(*right_tree,
                                              join_pred,
                                              join_two_tables,
                                              right_table))) {
    LOG_WARN("failed to check there is only one right table", K(ret));
  } else if (join_two_tables &&
             (OB_UNLIKELY(left_table.num_members() != 1) ||
              OB_UNLIKELY(right_table.num_members() != 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(left_table), K(right_table));
  } else if (OB_FAIL(has_checked.prepare_allocate(join_pred.count(), false))) {
    LOG_WARN("failed to allocate", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < join_pred.count(); i++) {
    ObRawExpr *cur_expr = join_pred.at(i);
    ObRawExpr *left_expr = NULL;
    ObRawExpr *right_expr = NULL;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_expr));
    } else if (T_OP_EQ == cur_expr->get_expr_type() &&
               2 == cur_expr->get_param_count() &&
               cur_expr->get_param_expr(0) != cur_expr->get_param_expr(1)) {
      EqualSets tmp_equal_sets;
      if (OB_ISNULL(left_expr = cur_expr->get_param_expr(0)) ||
          OB_ISNULL(right_expr = cur_expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), KPC(cur_expr), K(left_expr), K(right_expr));
      } else if (!join_two_tables) {
        // do nothing
      } else if (left_tree->get_tables().is_superset(left_expr->get_relation_ids()) &&
          right_tree->get_tables().is_superset(right_expr->get_relation_ids())) {
        // do nothing
      } else if (left_tree->get_tables().is_superset(right_expr->get_relation_ids()) &&
          right_tree->get_tables().is_superset(left_expr->get_relation_ids())) {
        std::swap(left_expr, right_expr);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expression", K(ret), KPC(cur_expr));
      }
      if (OB_FAIL(ret)) {
      } else if (join_two_tables &&
                 !(left_table.is_superset(left_expr->get_relation_ids()) &&
                   right_table.is_superset(right_expr->get_relation_ids()))) {
        // the pred does not join the given two tables,
        // decide whether remove this qual later
      } else if (ObOptimizerUtil::in_same_equalset(left_expr,
                                                   right_expr,
                                                   equal_sets)) {
        // remove preds which is equation between two exprs in the same equal sets
        has_checked.at(i) = true;
        OPT_TRACE("remove redundancy join condition:", cur_expr);
      } else if (OB_FAIL(tmp_equal_sets.assign(equal_sets))) {
        LOG_WARN("failed to append fd equal set", K(ret));
      } else if (FALSE_IT(equal_sets.reuse())) {
      } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(&allocator_,
                                                            cur_expr,
                                                            tmp_equal_sets,
                                                            equal_sets))) {
        LOG_WARN("failed to compute equal sets for inner join", K(ret));
      } else if (OB_FAIL(new_join_pred.push_back(cur_expr))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        has_checked.at(i) = true;
      }
    } else if (OB_FAIL(new_join_pred.push_back(cur_expr))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      has_checked.at(i) = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (join_two_tables &&
             OB_FAIL(re_add_necessary_predicate(join_pred, new_join_pred,
                                                has_checked, equal_sets))) {
    LOG_WARN("failed to re-add preds", K(ret));
  } else if (new_join_pred.count() == join_pred.count()) {
  } else if (OB_UNLIKELY(new_join_pred.count() > join_pred.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pred count", K(ret), K(new_join_pred), K(join_pred));
  } else if (OB_FAIL(join_pred.assign(new_join_pred))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  OPT_TRACE("output join conditions:", join_pred);
  return ret;
}

int ObLogPlan::try_split_or_qual(const ObRelIds &table_ids,
                                 ObOpRawExpr &or_qual,
                                 ObIArray<ObRawExpr*> &table_quals)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *new_expr = NULL;
  if (OB_FAIL(ObOptimizerUtil::split_or_qual_on_table(get_stmt(),
                                                      get_optimizer_context(),
                                                      table_ids,
                                                      or_qual,
                                                      new_expr))) {
    LOG_WARN("failed to split or qual on table", K(ret));
  } else if (NULL == new_expr) {
    /* do nothing */
  } else if (ObOptimizerUtil::find_equal_expr(table_quals, new_expr)) {
    /* do nothing */
  } else if (OB_FAIL(table_quals.push_back(new_expr))) {
    LOG_WARN("failed to push back new expr", K(ret));
  } else if (OB_FAIL(new_or_quals_.push_back(new_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

int ObLogPlan::generate_subplan_for_query_ref(ObQueryRefRawExpr *query_ref,
                                              SubPlanInfo *&subplan_info)
{
  int ret = OB_SUCCESS;
  // check if sub plan has been generated
  subplan_info = NULL;
  const ObSelectStmt *subquery = NULL;
  ObLogPlan *logical_plan = NULL;
  ObOptimizerContext &opt_ctx = get_optimizer_context();
  bool has_ref_assign_user_var = false;
  SubPlanInfo *info = NULL;
  bool is_initplan = false;
  OPT_TRACE_TITLE("start generate subplan for subquery expr");
  OPT_TRACE_BEGIN_SECTION;
  if (OB_ISNULL(subquery = query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery stmt is null", K(ret), K(query_ref));
  } else if (OB_ISNULL(opt_ctx.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx is null", K(ret));
  } else if (OB_ISNULL(logical_plan = opt_ctx.get_log_plan_factory().create(opt_ctx, *subquery))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create plan", K(ret), K(opt_ctx.get_query_ctx()->get_sql_stmt()));
  } else if (FALSE_IT(logical_plan->set_nonrecursive_plan_for_fake_cte(get_nonrecursive_plan_for_fake_cte()))) {
    // never reach
  } else if (OB_FAIL(subquery->has_ref_assign_user_var(has_ref_assign_user_var))) {
    LOG_WARN("faield to check stmt has assignment ref user var", K(ret));
  } else if (OB_FALSE_IT(is_initplan = !query_ref->has_exec_param() && !has_ref_assign_user_var)) {
  } else if (OB_FAIL(logical_plan->init_rescan_info_for_query_ref(*this, !is_initplan))) {
    LOG_WARN("failed to init rescan info", K(ret));
  } else if (OB_FAIL(logical_plan->add_exec_params_meta(query_ref->get_exec_params(),
                                                        get_basic_table_metas(),
                                                        get_selectivity_ctx()))) {
    LOG_WARN("failed to prepare exec param meta", K(ret));
  } else if (OB_FAIL(SMART_CALL(static_cast<ObSelectLogPlan *>(logical_plan)->generate_raw_plan()))) {
    LOG_WARN("failed to optimize sub-select", K(ret));
  } else if (OB_FAIL(add_query_ref_meta(query_ref,
                                        logical_plan->get_update_table_metas(),
                                        logical_plan->get_selectivity_ctx()))) {
    LOG_WARN("failed to add expr meta", K(ret));
  } else if (OB_ISNULL(info = static_cast<SubPlanInfo *>(get_allocator().alloc(sizeof(SubPlanInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc semi info", K(ret));
  } else {
      /**
           * 作为initplan的条件:
           * 1. 不含上层变量，如果含上层变量会在本层当作Const
           * 2. 不含存在赋值操作的用户变量
           */
    info = new(info)SubPlanInfo(query_ref, logical_plan, is_initplan);
    if (OB_FAIL(add_subplan(info))) {
      LOG_WARN("failed to add sp params to rel", K(ret));
    } else {
      logical_plan->set_query_ref(query_ref);
      subplan_info = info;
      LOG_TRACE("succ to generate logical plan of sub-select");
    }

    if (OB_FAIL(ret) && NULL != info) {
      info->subplan_ = NULL; // we leave logical plan to be freed later
      info->~SubPlanInfo();
      info = NULL;
      subplan_info = NULL;
    } else { /* Do nothing */ }
  }
  OPT_TRACE_TITLE("end generate subplan for subquery expr");
  OPT_TRACE_END_SECTION;
  return ret;
}

int ObLogPlan::init_rescan_info_for_query_ref(const ObLogPlan &parent_plan,
                                              const bool is_rescan_subquery)
{
  int ret = OB_SUCCESS;
  is_rescan_subplan_ = parent_plan.is_rescan_subplan_ || is_rescan_subquery;
  disable_child_batch_rescan_ = parent_plan.disable_child_batch_rescan_
                                || (is_rescan_subquery
                                    && !get_optimizer_context().enable_spf_semi_anti_child_batch());
  return ret;
}

int ObLogPlan::init_rescan_info_for_subquery_paths(const ObLogPlan &parent_plan,
                                                   const bool is_inner_path,
                                                   const bool is_semi_anti_join_inner_path)
{
  int ret = OB_SUCCESS;
  is_rescan_subplan_ = parent_plan.is_rescan_subplan_ || is_inner_path;
  disable_child_batch_rescan_ = parent_plan.disable_child_batch_rescan_
                                || (is_semi_anti_join_inner_path
                                    && !get_optimizer_context().enable_spf_semi_anti_child_batch());
  return ret;
}

int ObLogPlan::add_exec_params_meta(ObIArray<ObExecParamRawExpr *> &exec_params,
                                    const OptTableMetas &table_metas,
                                    const OptSelectivityCtx &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exec_params.count(); i ++) {
    ObExecParamRawExpr *exec_param = exec_params.at(i);
    double avg_len = 0;
    OptDynamicExprMeta dynamic_expr_meta;
    if (OB_ISNULL(exec_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param", K(ret), KPC(exec_param));
    } else if (OB_FAIL(ObOptSelectivity::calculate_expr_avg_len(table_metas,
                                                                ctx,
                                                                exec_param,
                                                                avg_len))) {
      LOG_WARN("failed to calc expr avg len", K(ret), KPC(exec_param));
    } else {
      dynamic_expr_meta.set_expr(exec_param);
      dynamic_expr_meta.set_avg_len(avg_len);
    }
    if (FAILEDx(get_basic_table_metas().add_dynamic_expr_meta(dynamic_expr_meta))) {
      LOG_WARN("failed to add expr meta", K(ret));
    } else if (OB_FAIL(get_update_table_metas().add_dynamic_expr_meta(dynamic_expr_meta))) {
      LOG_WARN("failed to add expr meta", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::add_query_ref_meta(ObQueryRefRawExpr *expr,
                                  const OptTableMetas &child_table_metas,
                                  const OptSelectivityCtx &child_ctx)
{
  int ret = OB_SUCCESS;
  ObRawExpr *ref_expr = NULL;
  double avg_len = 0;
  OptDynamicExprMeta dynamic_expr_meta;
  ObSelectStmt *stmt = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt = expr->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), KPC(expr));
  } else if (!expr->is_scalar()) {
    // do nothing
  } else if (OB_UNLIKELY(stmt->get_select_item_size() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected query ref", K(ret), KPC(stmt));
  } else if (OB_FAIL(ObOptSelectivity::calculate_expr_avg_len(
      child_table_metas, child_ctx, stmt->get_select_item(0).expr_, avg_len))) {
    LOG_WARN("failed to calc expr avg len", K(ret), KPC(expr));
  } else {
    dynamic_expr_meta.set_expr(expr);
    dynamic_expr_meta.set_avg_len(avg_len);
    if (OB_FAIL(get_basic_table_metas().add_dynamic_expr_meta(dynamic_expr_meta))) {
      LOG_WARN("failed to add expr meta", K(ret));
    } else if (OB_FAIL(get_update_table_metas().add_dynamic_expr_meta(dynamic_expr_meta))) {
      LOG_WARN("failed to add expr meta", K(ret));
    }
  }
  return ret;
}

//在已有sub_plan_infos中查找expr对应的subplan
int ObLogPlan::get_subplan(const ObRawExpr *expr, SubPlanInfo *&info)
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
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObLogPlan::find_base_rel(ObIArray<ObJoinOrder *> &base_level, int64_t table_idx, ObJoinOrder *&base_rel)
{
  int ret = OB_SUCCESS;
  ObJoinOrder *cur_rel = NULL;
  bool find = false;
  base_rel = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < base_level.count(); ++i) {
    //如果是OJ，这里table_set_可能不止一项，所以不能认为cur_rel->table_set_.num_members() == 1
    if (OB_ISNULL(cur_rel = base_level.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(cur_rel));
    } else if (cur_rel->get_tables().has_member(table_idx)){
      find = true;
      base_rel = cur_rel;
      LOG_TRACE("succeed to find base rel", K(cur_rel->get_tables()), K(table_idx));
    } else { /* do nothing */ }
  }
  return ret;
}

int ObLogPlan::find_join_rel(ObRelIds& relids, ObJoinOrder *&join_rel)
{
  int ret = OB_SUCCESS;
  join_rel = NULL;
  if (relid_joinorder_map_.created() &&
      OB_FAIL(relid_joinorder_map_.get_refactored(relids, join_rel))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to get refactored", K(ret), K(relids));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLogPlan::check_need_gen_join_path(const ObJoinOrder *left_tree,
                                        const ObJoinOrder *right_tree,
                                        bool &need_gen)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  need_gen = true;
  if (OB_ISNULL(left_tree) || OB_ISNULL(right_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input join order", K(left_tree), K(right_tree), K(ret));
  } else if (join_path_set_.created()) {
    JoinPathPairInfo pair;
    pair.left_ids_ = left_tree->get_tables();
    pair.right_ids_ = right_tree->get_tables();
    hash_ret = join_path_set_.exist_refactored(pair);
    if (OB_HASH_EXIST == hash_ret) {
      need_gen = false;
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      // do nothing
    } else {
      ret = hash_ret != OB_SUCCESS ? hash_ret : OB_ERR_UNEXPECTED;
      LOG_WARN("failed to check hash set exsit", K(ret), K(hash_ret), K(pair));
    }
  }
  return ret;
}

int ObLogPlan::allocate_function_table_path(FunctionTablePath *func_table_path,
                                            ObLogicalOperator *&out_access_path_op)
{
  int ret = OB_SUCCESS;
  ObLogFunctionTable *op = NULL;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(func_table_path) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(func_table_path->table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(func_table_path), K(get_stmt()), K(ret));
  } else if (OB_ISNULL(op = static_cast<ObLogFunctionTable*>(get_log_op_factory().
                                        allocate(*this, LOG_FUNCTION_TABLE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate function table", K(ret));
  } else {
    op->set_table_id(func_table_path->table_id_);
    op->add_values_expr(func_table_path->value_expr_);
    op->set_table_name(table_item->get_table_name());
    if (OB_FAIL(append(op->get_filter_exprs(), func_table_path->filter_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(op->compute_property(func_table_path))) {
      LOG_WARN("failed to compute property", K(ret));
    } else if (OB_FAIL(op->pick_out_startup_filters())) {
      LOG_WARN("failed to pick out startup filters", K(ret));
    } else {
      out_access_path_op = op;
    }
  }
  return ret;
}

int ObLogPlan::allocate_json_table_path(JsonTablePath *json_table_path,
                                        ObLogicalOperator *&out_access_path_op)
{
  int ret = OB_SUCCESS;
  ObLogJsonTable *op = NULL;
  TableItem *table_item = NULL;
  if (OB_ISNULL(json_table_path) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(json_table_path->table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(json_table_path), K(get_stmt()), K(ret));
  } else if (OB_ISNULL(op = static_cast<ObLogJsonTable*>(get_log_op_factory().
                                        allocate(*this, LOG_JSON_TABLE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate json table path", K(ret));
  } else {
    op->set_table_id(json_table_path->table_id_);
    op->add_values_expr(json_table_path->value_expr_);
    op->set_table_name(table_item->get_table_name());
    ObJsonTableDef* tbl_def = table_item->get_json_table_def();

    if (OB_ISNULL(tbl_def)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param, table define can't be null", K(ret));
    } else if (OB_FAIL(append(op->get_origin_cols_def(), tbl_def->all_cols_))) {
      LOG_WARN("failed to append orgin defs", K(ret));
    } else if (OB_FAIL(append(op->get_filter_exprs(), json_table_path->filter_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(op->compute_property(json_table_path))) {
      LOG_WARN("failed to compute property", K(ret));
    } else if (OB_FAIL(op->pick_out_startup_filters())) {
      LOG_WARN("failed to pick out startup filters", K(ret));
    } else if (OB_FAIL(op->set_namespace_arr(tbl_def->namespace_arr_))) {
      LOG_WARN("fail to get ns array from table def", K(ret));
    } else if (OB_FAIL(op->set_column_param_default_arr(json_table_path->column_param_default_exprs_))) {
      LOG_WARN("fail to get default array from table def", K(ret));
    } else {
      op->set_table_type(tbl_def->table_type_);
      out_access_path_op = op;
    }
  }
  return ret;
}

int ObLogPlan::allocate_temp_table_path(TempTablePath *temp_table_path,
                                        ObLogicalOperator *&out_access_path_op)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  ObLogTempTableAccess *op = NULL;
  if (OB_ISNULL(temp_table_path) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(temp_table_path->table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(temp_table_path), K(get_stmt()), K(table_item), K(ret));
  } else if (OB_ISNULL(op = static_cast<ObLogTempTableAccess*>
               (log_op_factory_.allocate(*this, LOG_TEMP_TABLE_ACCESS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for ObLogFunctionTableScan failed", K(ret));
  } else {
    op->set_table_id(temp_table_path->table_id_);
    op->set_temp_table_id(temp_table_path->temp_table_id_);
    op->get_table_name().assign_ptr(table_item->table_name_.ptr(),
                                    table_item->table_name_.length());
    op->get_access_name().assign_ptr(table_item->alias_name_.ptr(),
                                     table_item->alias_name_.length());
    if (OB_FAIL(op->get_filter_exprs().assign(temp_table_path->filter_))) {
      LOG_WARN("failed to assign filter exprs", K(ret));
    } else if (OB_FAIL(op->compute_property(temp_table_path))) {
      LOG_WARN("failed to compute property", K(ret));
    } else if (OB_FAIL(op->pick_out_startup_filters())) {
      LOG_WARN("failed to pick out startup filters", K(ret));
    } else {
      out_access_path_op = op;
    }
  }
  return ret;
}

int ObLogPlan::allocate_cte_table_path(CteTablePath *cte_table_path,
                                       ObLogicalOperator *&out_access_path_op)
{
  int ret = OB_SUCCESS;
  ObLogTableScan *scan = NULL;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(cte_table_path) ||
      OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(cte_table_path->table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(table_item), K(ret));
  } else if (OB_UNLIKELY(NULL == (scan = static_cast<ObLogTableScan *>
                 (get_log_op_factory().allocate(*this, LOG_TABLE_SCAN))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate table/index operator", K(ret));
  } else {
    scan->set_table_id(cte_table_path->table_id_);
    scan->set_ref_table_id(cte_table_path->ref_table_id_);
    scan->set_index_table_id(cte_table_path->ref_table_id_);
    scan->set_table_name(table_item->get_table_name());
    if (OB_FAIL(scan->get_filter_exprs().assign(cte_table_path->filter_))) {
      LOG_WARN("failed to set filters", K(ret));
    } else if (OB_FAIL(scan->compute_property(cte_table_path))) {
      LOG_WARN("failed to compute property", K(ret));
    } else if (OB_FAIL(scan->pick_out_startup_filters())) {
      LOG_WARN("failed to pick out startup filters", K(ret));
    } else {
      out_access_path_op = scan;
    }
  }
  return ret;
}

int ObLogPlan::allocate_access_path(AccessPath *ap,
                                    ObLogicalOperator *&out_access_path_op)
{
  int ret = OB_SUCCESS;
  ObLogTableScan *scan = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(ap) || OB_ISNULL(get_stmt()) || OB_ISNULL(ap->parent_)
      || OB_ISNULL(ap->get_strong_sharding()) || OB_ISNULL(ap->table_partition_info_)
      || OB_ISNULL(schema_guard = get_optimizer_context().get_sql_schema_guard())
      || OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(ap->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ap), K(get_stmt()),
        K(schema_guard), K(table_item), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ap->table_id_, ap->ref_table_id_, get_stmt(), table_schema))) {
    LOG_WARN("fail to get_table_schema", K(ret));
  } else if (OB_ISNULL(scan = static_cast<ObLogTableScan *>
                 (get_log_op_factory().allocate(*this, ObLogOpType::LOG_TABLE_SCAN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate table/index operator", K(ret));
  } else if (OB_FAIL(scan->set_est_row_count_record(ap->est_records_))) {
    LOG_WARN("failed to set estimation info", K(ret));
  } else {
    LOG_TRACE("allocate access path", KPC(ap), K(ap->nl_params_), K(scan), K(ap->table_id_), K(ap->ref_table_id_),
             "index_table_id: ", ap->index_id_, KPC(table_item), K(ap->table_partition_info_),
             KPC(ap->table_partition_info_));
    scan->set_est_cost_info(&ap->get_cost_table_scan_info());
    scan->set_flashback_query_expr(table_item->flashback_query_expr_);
    scan->set_flashback_query_type(table_item->flashback_query_type_);
    scan->set_fq_read_tx_uncommitted(get_optimizer_context().get_global_hint().get_flashback_read_tx_uncommitted());
    scan->set_table_id(ap->get_table_id());
    scan->set_ref_table_id(ap->get_ref_table_id());
    scan->set_index_table_id(ap->get_index_table_id());
    scan->set_scan_direction(ap->order_direction_);
    scan->set_is_index_global(ap->is_global_index_);
    scan->set_index_back(ap->est_cost_info_.index_meta_info_.is_index_back_);
    scan->set_is_spatial_index(ap->est_cost_info_.index_meta_info_.is_geo_index_);
    scan->set_use_das(ap->use_das_);
    scan->set_table_partition_info(ap->table_partition_info_);
    scan->set_table_opt_info(ap->table_opt_info_);
    scan->set_access_path(ap);
    scan->set_dblink_id(table_item->dblink_id_); // will be delete after implement log_link_table_scan
    scan->set_sample_info(ap->sample_info_);
    if (NULL != table_schema && table_schema->is_tmp_table()) {
      scan->set_session_id(table_schema->get_session_id());
    }
    scan->set_table_row_count(ap->table_row_count_);
    scan->set_output_row_count(ap->output_row_count_);
    scan->set_phy_query_range_row_count(ap->phy_query_range_row_count_);
    scan->set_query_range_row_count(ap->query_range_row_count_);
    scan->set_index_back_row_count(ap->index_back_row_count_);
    scan->set_pre_query_range(ap->pre_query_range_);
    scan->set_pre_range_graph(ap->pre_range_graph_);
    scan->set_skip_scan(OptSkipScanState::SS_DISABLE != ap->use_skip_scan_);
    scan->set_table_type(table_schema->get_table_type());
    scan->set_index_prefix(ap->index_prefix_);
    if (!ap->is_inner_path_ &&
        OB_FAIL(scan->set_query_ranges(ap->get_cost_table_scan_info().ranges_,
                                       ap->get_cost_table_scan_info().ss_ranges_))) {
      LOG_WARN("failed to set query ranges", K(ret));
    } else if (OB_FAIL(scan->set_range_columns(ap->get_cost_table_scan_info().range_columns_))) {
      LOG_WARN("failed to set range column", K(ret));
    } else if (OB_FAIL(append(scan->get_server_list(), ap->get_server_list()))) {
      LOG_WARN("failed to assign server list", K(ret));
    } else { // set table name and index name
      scan->set_table_name(table_item->get_table_name());
      scan->set_diverse_path_count(ap->parent_->get_diverse_path_count());
      if (ap->get_index_table_id() != ap->get_ref_table_id()) {
        if (OB_FAIL(store_index_column_ids(*schema_guard, *scan, ap->get_table_id(), ap->get_index_table_id()))) {
          LOG_WARN("Failed to store index column id", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(scan->set_table_scan_filters(ap->filter_))) {
        LOG_WARN("failed to set filters", K(ret));
      } else if (OB_FAIL(append(scan->get_pushdown_filter_exprs(), ap->pushdown_filters_))) {
        LOG_WARN("failed to append pushdown filters", K(ret));
      }
    }

    //init part/subpart expr for query range prune
    if (OB_SUCC(ret)) {
      ObRawExpr *part_expr = NULL;
      ObRawExpr *subpart_expr = NULL;
      uint64_t table_id = scan->get_table_id();
      uint64_t ref_table_id = scan->get_location_table_id();
      share::schema::ObPartitionLevel part_level = share::schema::PARTITION_LEVEL_MAX;
      if (is_virtual_table(ref_table_id)
          || is_inner_table(ref_table_id)
          || is_cte_table(ref_table_id)) {
        // do nothing
      } else if (OB_FAIL(get_part_exprs(table_id,
                                        ref_table_id,
                                        part_level,
                                        part_expr,
                                        subpart_expr))) {
        LOG_WARN("failed to get part exprs", K(ret));
      } else {
        scan->set_part_expr(part_expr);
        scan->set_subpart_expr(subpart_expr);
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(scan->compute_property(ap))) {
      LOG_WARN("failed to compute property", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (ap->is_global_index_ && scan->get_index_back()) {
        if (OB_FAIL(scan->init_calc_part_id_expr())) {
          LOG_WARN("failed to init calc part id expr", K(ret));
        } else {
          scan->set_global_index_back_table_partition_info(ap->parent_->get_table_partition_info());
          if (ap->est_cost_info_.table_filters_.count() > 0) {
            bool has_index_scan_filter = false;
            bool has_index_lookup_filter = false;
            if (OB_FAIL(ObOptimizerUtil::get_has_global_index_filters(scan->get_filter_exprs(),
                                                                      scan->get_idx_columns(),
                                                                      has_index_scan_filter,
                                                                      has_index_lookup_filter))) {
              LOG_WARN("failed to get has global index filters", K(ret));
            } else {
              scan->set_has_index_scan_filter(has_index_scan_filter);
              scan->set_has_index_lookup_filter(has_index_lookup_filter);
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      out_access_path_op = scan;
    }
  }
  return ret;
}

int ObLogPlan::store_index_column_ids(
    ObSqlSchemaGuard &schema_guard,
    ObLogTableScan &scan,
    const int64_t table_id,
    const int64_t index_id)
{
  int ret = OB_SUCCESS;
  ObString index_name;
  const ObTableSchema *index_schema = NULL;
  if (OB_FAIL(schema_guard.get_table_schema(table_id, index_id, get_stmt(), index_schema))) {
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
        OB_SUCC(ret) && iter != index_schema->column_end(); ++iter) {
      if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is null", K(ret), K(iter));
      } else {
        const ObColumnSchemaV2 *column_schema = *iter;
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_schema is null", K(ret));
        } else if (OB_FAIL(scan.add_idx_column_id(column_schema->get_column_id()))) {
          LOG_WARN("Fail to add column id to scan", K(ret));
        } else { }//do nothing
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_join_path(JoinPath *join_path,
                                  ObLogicalOperator *&out_join_path_op)
{
  int ret = OB_SUCCESS;
  ObJoinOrder *join_order = NULL;
  if (OB_ISNULL(join_path) || OB_ISNULL(join_order = join_path->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(join_path), K(join_order));
  } else {
    Path *left_path = const_cast<Path*>(join_path->left_path_);
    Path *right_path = const_cast<Path*>(join_path->right_path_);
    ObLogicalOperator *left_child = NULL;
    ObLogicalOperator *right_child = NULL;
    ObExchangeInfo left_exch_info;
    bool left_exch_is_non_preserve_side = (join_path->is_naaj_
                                            && RIGHT_ANTI_JOIN == join_path->join_type_);
    ObExchangeInfo right_exch_info;
    bool right_exch_is_non_preserve_side = (join_path->is_naaj_
                                             && LEFT_ANTI_JOIN == join_path->join_type_);
    ObLogJoin *join_op = NULL;
    if (OB_ISNULL(left_path) || OB_ISNULL(right_path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(left_path), K(right_path));
    } else if (OB_FAIL(SMART_CALL(create_plan_tree_from_path(left_path, left_child))) ||
               OB_FAIL(SMART_CALL(create_plan_tree_from_path(right_path, right_child)))) {
      LOG_WARN("failed to create plan tree from path", K(ret));
    } else if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get expected null", K(left_child), K(right_child), K(ret));
    } else if (OB_FAIL(compute_join_exchange_info(*join_path, left_exch_info, right_exch_info,
                        left_exch_is_non_preserve_side, right_exch_is_non_preserve_side))) {
      LOG_WARN("failed to compute join exchange info", K(ret));
    } else if (OB_FAIL(allocate_sort_and_exchange_as_top(left_child,
                                                         left_exch_info,
                                                         join_path->left_sort_keys_,
                                                         join_path->left_need_sort_,
                                                         join_path->left_prefix_pos_,
                                                         join_path->is_left_local_order()))) {
      LOG_WARN("failed to allocate operator for child", K(ret));
    } else if (OB_FAIL(allocate_sort_and_exchange_as_top(right_child,
                                                         right_exch_info,
                                                         join_path->right_sort_keys_,
                                                         join_path->right_need_sort_,
                                                         join_path->right_prefix_pos_,
                                                         join_path->is_right_local_order()))) {
      LOG_WARN("failed to allocate operator for child", K(ret));
    } else if (join_path->need_mat_ && LOG_MATERIAL != right_child->get_type()
               && OB_FAIL(allocate_material_as_top(right_child))) {
      LOG_WARN("failed to allocate material as top", K(ret));
    } else if (join_path->is_adaptive_ && NESTED_LOOP_JOIN == join_path->join_algo_
               && OB_FAIL(allocate_statistics_collector_as_top(left_child, join_path->adaptive_threshold_))) {
      LOG_WARN("failed to allocate statistics collector as top", K(ret));
    } else if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(left_child), K(right_child), K(ret));
    } else if (OB_ISNULL(join_op = static_cast<ObLogJoin *>(get_log_op_factory().allocate(*this, LOG_JOIN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate join_op operator", K(ret));
    } else if (OB_FAIL(join_op->set_join_filter_infos(join_path->join_filter_infos_))) {
      LOG_WARN("failed to set join filter infos", K(ret));
    } else {
      join_op->set_left_child(left_child);
      join_op->set_right_child(right_child);
      join_op->set_join_type(join_path->join_type_);
      join_op->set_join_algo(join_path->join_algo_);
      join_op->set_join_distributed_method(join_path->join_dist_algo_);
      join_op->set_slave_mapping_type(join_path->get_slave_mapping_type());
      join_op->set_is_partition_wise(join_path->is_partition_wise());
      join_op->set_can_use_batch_nlj(join_path->can_use_batch_nlj_);
      join_op->set_inherit_sharding_index(join_path->inherit_sharding_index_);
      join_op->set_join_path(join_path);
      join_op->set_is_adaptive(join_path->is_adaptive_);
      join_op->set_adaptive_dist_algo(join_path->adaptive_dist_algo_);
      if (OB_FAIL(join_op->set_merge_directions(join_path->merge_directions_))) {
        LOG_WARN("failed to set merge directions", K(ret));
      } else if (OB_FAIL(join_op->set_nl_params(static_cast<AccessPath*>(right_path)->nl_params_))) {
        LOG_WARN("failed to set nl params", K(ret));
      } else if (OB_FAIL(join_op->set_join_conditions(join_path->equal_join_conditions_))) {
        LOG_WARN("append error in allocate_join_path", K(ret));
      } else if (IS_NOT_INNER_JOIN(join_path->join_type_)) {
        if (OB_FAIL(append(join_op->get_join_filters(), join_path->other_join_conditions_))) {
          LOG_WARN("failed to allocate filter", K(ret));
        } else if (OB_FAIL(append(join_op->get_filter_exprs(), join_path->filter_))) {
          LOG_WARN("failed to allocate filter", K(ret));
        } else {
          LOG_TRACE("connect by join exec params", K(join_path->other_join_conditions_), K(ret));
        }
      } else if (OB_FAIL(append(join_op->get_join_filters(), join_path->other_join_conditions_))) {
        LOG_WARN("failed to allocate filter", K(ret));
      } else if (OB_FAIL(append(join_op->get_join_filters(), join_path->filter_))) {
        LOG_WARN("failed to allocate filter", K(ret));
      } else { /* do nothing */}

      if (OB_SUCC(ret) && CONNECT_BY_JOIN == join_path->join_type_) {
        if (OB_FAIL(set_connect_by_property(join_path, *join_op))) {
          LOG_WARN("failed to set connect by property", K(ret));
        } else { /*do nothing*/ }
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

int ObLogPlan::compute_join_exchange_info(JoinPath &join_path,
                                          ObExchangeInfo &left_exch_info,
                                          ObExchangeInfo &right_exch_info,
                                          bool left_is_non_preserve_side,
                                          bool right_is_non_preserve_side)
{
  int ret = OB_SUCCESS;
  EqualSets equal_sets;
  ObSEArray<ObRawExpr*, 8> left_keys;
  ObSEArray<ObRawExpr*, 8> right_keys;
  ObSEArray<bool, 8> null_safe_info;
  SlaveMappingType sm_type = join_path.get_slave_mapping_type();
  left_exch_info.dist_method_ = ObPQDistributeMethod::NONE;
  left_exch_info.parallel_ = join_path.parallel_;
  left_exch_info.server_cnt_ = join_path.server_cnt_;
  right_exch_info.dist_method_ = ObPQDistributeMethod::NONE;
  right_exch_info.parallel_ = join_path.parallel_;
  right_exch_info.server_cnt_ = join_path.server_cnt_;
  if (OB_ISNULL(join_path.left_path_) || OB_ISNULL(join_path.left_path_->parent_) ||
      OB_ISNULL(join_path.right_path_) || OB_ISNULL(join_path.right_path_->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(join_path.left_path_), K(join_path.right_path_), K(ret));
  } else if (OB_FAIL(left_exch_info.server_list_.assign(join_path.server_list_))
             || OB_FAIL(right_exch_info.server_list_.assign(join_path.server_list_))) {
    LOG_WARN("failed to assign server list", K(ret));
  } else if (OB_FAIL(append(equal_sets, join_path.left_path_->parent_->get_output_equal_sets())) ||
             OB_FAIL(append(equal_sets, join_path.right_path_->parent_->get_output_equal_sets()))) {
    LOG_WARN("failed to append equal sets", K(ret));
  } else if (OB_FAIL(get_join_path_keys(join_path, left_keys, right_keys, null_safe_info))) {
    LOG_WARN("failed to get join path keys", K(ret));
  } else if (DistAlgo::DIST_PARTITION_WISE == join_path.join_dist_algo_ ||
             DistAlgo::DIST_EXT_PARTITION_WISE == join_path.join_dist_algo_) {
    if (join_path.is_slave_mapping_) {
      if (OB_FAIL(compute_hash_distribution_info(join_path.join_type_,
                                                 join_path.use_hybrid_hash_dm_,
                                                 join_path.equal_join_conditions_,
                                                 join_path.left_path_->parent_->get_output_tables(),
                                                 left_exch_info,
                                                 right_exch_info))) {
        LOG_WARN("failed to compute exchange info for hash distribution", K(ret));
      } else {
        left_exch_info.slave_mapping_type_ = sm_type;
        right_exch_info.slave_mapping_type_ = sm_type;
      }
    } else { /*do nothing*/ }
  } else if (DistAlgo::DIST_PARTITION_NONE == join_path.join_dist_algo_) {
    ObPQDistributeMethod::Type unmatch_method = ObPQDistributeMethod::DROP;
    if (LEFT_ANTI_JOIN == join_path.join_type_ ||
        LEFT_OUTER_JOIN == join_path.join_type_ ||
        FULL_OUTER_JOIN == join_path.join_type_) {
      unmatch_method = ObPQDistributeMethod::RANDOM;
    }
    if (OB_FAIL(compute_repartition_distribution_info(equal_sets,
                                                      left_keys,
                                                      right_keys,
                                                      *join_path.right_path_,
                                                       left_exch_info))) {
      LOG_WARN("failed to compute repartition distribution info", K(ret));
    } else {
      left_exch_info.unmatch_row_dist_method_ = unmatch_method;
      if (join_path.is_slave_mapping_) {
        if (OB_FAIL(compute_hash_distribution_info(join_path.join_type_,
                                                   join_path.use_hybrid_hash_dm_,
                                                   join_path.equal_join_conditions_,
                                                   join_path.left_path_->parent_->get_output_tables(),
                                                   left_exch_info,
                                                   right_exch_info))) {
          LOG_WARN("failed to compute hash distribution info", K(ret));
        } else {
          left_exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_HASH;
          right_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
          left_exch_info.slave_mapping_type_ = sm_type;
          right_exch_info.slave_mapping_type_ = sm_type;
        }
      }
    }
  } else if (DistAlgo::DIST_NONE_PARTITION == join_path.join_dist_algo_) {
    ObPQDistributeMethod::Type unmatch_method = ObPQDistributeMethod::DROP;
    if (RIGHT_ANTI_JOIN == join_path.join_type_ ||
        RIGHT_OUTER_JOIN == join_path.join_type_ ||
        FULL_OUTER_JOIN == join_path.join_type_) {
      unmatch_method = ObPQDistributeMethod::RANDOM;
    }
    if (OB_FAIL(compute_repartition_distribution_info(equal_sets,
                                                      right_keys,
                                                      left_keys,
                                                      *join_path.left_path_,
                                                       right_exch_info))) {
      LOG_WARN("failed to compute repartition distribution_info", K(ret));
    } else {
      right_exch_info.unmatch_row_dist_method_ = unmatch_method;
      if (join_path.is_slave_mapping_) {
        if (OB_FAIL(compute_hash_distribution_info(join_path.join_type_,
                                                   join_path.use_hybrid_hash_dm_,
                                                   join_path.equal_join_conditions_,
                                                   join_path.left_path_->parent_->get_output_tables(),
                                                   left_exch_info,
                                                   right_exch_info))) {
          LOG_WARN("failed to compute hash distribution info", K(ret));
        } else {
          left_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
          right_exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_HASH;
          left_exch_info.slave_mapping_type_ = sm_type;
          right_exch_info.slave_mapping_type_ = sm_type;
        }
      }
    }
  } else if (DistAlgo::DIST_BC2HOST_NONE == join_path.join_dist_algo_) {
     left_exch_info.dist_method_ = ObPQDistributeMethod::BC2HOST;
  } else if (DistAlgo::DIST_BROADCAST_NONE == join_path.join_dist_algo_) {
    if (!join_path.is_slave_mapping_) {
       left_exch_info.dist_method_ = ObPQDistributeMethod::BROADCAST;
    } else {
      if (OB_FAIL(compute_repartition_distribution_info(equal_sets,
                                                        left_keys,
                                                        right_keys,
                                                        *join_path.right_path_,
                                                        left_exch_info))) {
        LOG_WARN("failed to compute repartition distrubution info", K(ret));
      } else {
        left_exch_info.repartition_type_ = OB_REPARTITION_NO_REPARTITION;
        left_exch_info.dist_method_ = ObPQDistributeMethod::SM_BROADCAST;
        left_exch_info.slave_mapping_type_ = sm_type;
      }
    }
  } else if (DistAlgo::DIST_NONE_BROADCAST == join_path.join_dist_algo_) {
    if (!join_path.is_slave_mapping_) {
      right_exch_info.dist_method_ = ObPQDistributeMethod::BROADCAST;
    } else {
      if (OB_FAIL(compute_repartition_distribution_info(equal_sets,
                                                        right_keys,
                                                        left_keys,
                                                        *join_path.left_path_,
                                                        right_exch_info))) {
        LOG_WARN("failed to compute repartition distribution_info", K(ret));
      } else {
        right_exch_info.repartition_type_ = OB_REPARTITION_NO_REPARTITION;
        right_exch_info.dist_method_ = ObPQDistributeMethod::SM_BROADCAST;
        right_exch_info.slave_mapping_type_ = sm_type;
      }
    }
  } else if (DistAlgo::DIST_HASH_NONE == join_path.join_dist_algo_) {
    ObPQDistributeMethod::Type unmatch_method = ObPQDistributeMethod::DROP;
    if (LEFT_ANTI_JOIN == join_path.join_type_ ||
        LEFT_OUTER_JOIN == join_path.join_type_ ||
        FULL_OUTER_JOIN == join_path.join_type_) {
      unmatch_method = ObPQDistributeMethod::RANDOM;
    }
    if (OB_FAIL(compute_single_side_hash_distribution_info(equal_sets,
                                                           left_keys,
                                                           right_keys,
                                                           *join_path.right_path_,
                                                           left_exch_info))) {
      LOG_WARN("failed to compute left hash distribution info", K(ret));
    } else {
      left_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
      right_exch_info.dist_method_ = ObPQDistributeMethod::NONE;
      right_exch_info.unmatch_row_dist_method_ = unmatch_method;
    }
  } else if (DistAlgo::DIST_NONE_HASH == join_path.join_dist_algo_) {
    ObPQDistributeMethod::Type unmatch_method = ObPQDistributeMethod::DROP;
    if (RIGHT_ANTI_JOIN == join_path.join_type_ ||
        RIGHT_OUTER_JOIN == join_path.join_type_ ||
        FULL_OUTER_JOIN == join_path.join_type_) {
      unmatch_method = ObPQDistributeMethod::RANDOM;
    }
    if (OB_FAIL(compute_single_side_hash_distribution_info(equal_sets,
                                                           right_keys,
                                                           left_keys,
                                                           *join_path.left_path_,
                                                           right_exch_info))) {
      LOG_WARN("failed to compute left hash distribution info", K(ret));
    } else {
      left_exch_info.dist_method_ = ObPQDistributeMethod::NONE;
      right_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
      left_exch_info.unmatch_row_dist_method_ = unmatch_method;
    }
  } else if (DistAlgo::DIST_HASH_HASH == join_path.join_dist_algo_) {
    if (OB_FAIL(compute_hash_distribution_info(join_path.join_type_,
                                               join_path.use_hybrid_hash_dm_,
                                               join_path.equal_join_conditions_,
                                               join_path.left_path_->parent_->get_output_tables(),
                                               left_exch_info,
                                               right_exch_info))) {
      LOG_WARN("failed to compute hash distribution info", K(ret));
    } else { /* do nothing*/ }
  } else if (DistAlgo::DIST_PULL_TO_LOCAL == join_path.join_dist_algo_) {
    if (join_path.left_path_->is_sharding() && !join_path.left_path_->contain_fake_cte()) {
      left_exch_info.dist_method_ = ObPQDistributeMethod::LOCAL;
    }
    if (join_path.right_path_->is_sharding() && !join_path.right_path_->contain_fake_cte()) {
      right_exch_info.dist_method_ = ObPQDistributeMethod::LOCAL;
    }
  } else if (DistAlgo::DIST_NONE_ALL == join_path.join_dist_algo_
            || DistAlgo::DIST_ALL_NONE == join_path.join_dist_algo_) {
    // do nothing
  } else if (DistAlgo::DIST_RANDOM_ALL == join_path.join_dist_algo_) {
    left_exch_info.dist_method_ = ObPQDistributeMethod::RANDOM;
  } else { /*do nothing*/
  }

  if (OB_SUCC(ret)) {
    // support null skew handling
    compute_null_distribution_info(join_path.join_type_, left_exch_info, right_exch_info, null_safe_info);
  }

  if (OB_SUCC(ret)) {
    if (left_exch_info.need_exchange()) {
      if (OB_FAIL(left_exch_info.weak_sharding_.assign(join_path.get_weak_sharding()))) {
        LOG_WARN("failed to assign weak sharding", K(ret));
      } else {
        left_exch_info.strong_sharding_ = join_path.get_strong_sharding();
        left_exch_info.need_null_aware_shuffle_ = (left_is_non_preserve_side
                        && (ObPQDistributeMethod::HASH == left_exch_info.dist_method_
                            || ObPQDistributeMethod::PARTITION == left_exch_info.dist_method_));
      }
    }
    if (right_exch_info.need_exchange()) {
      if (OB_FAIL(right_exch_info.weak_sharding_.assign(join_path.get_weak_sharding()))) {
        LOG_WARN("failed to assign weak sharding", K(ret));
      } else {
        right_exch_info.strong_sharding_ = join_path.get_strong_sharding();
        right_exch_info.need_null_aware_shuffle_ = (right_is_non_preserve_side
                              && (ObPQDistributeMethod::HASH == right_exch_info.dist_method_
                              || ObPQDistributeMethod::PARTITION == right_exch_info.dist_method_));
      }
    }
  }
  return ret;
}

int ObLogPlan::get_join_path_keys(const JoinPath &join_path,
                                  ObIArray<ObRawExpr*> &left_keys,
                                  ObIArray<ObRawExpr*> &right_keys,
                                  ObIArray<bool> &null_safe_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> conditions;
  if (OB_ISNULL(join_path.left_path_) || OB_ISNULL(join_path.left_path_->parent_) ||
      OB_ISNULL(join_path.right_path_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(join_path.left_path_), K(join_path.right_path_), K(ret));
  } else if (JoinAlgo::HASH_JOIN == join_path.join_algo_ ||
             JoinAlgo::MERGE_JOIN == join_path.join_algo_) {
    if (OB_FAIL(append(conditions, join_path.equal_join_conditions_))) {
      LOG_WARN("failed to append conditions", K(ret));
    } else { /*do nothing*/ }
  } else {
    if (OB_FAIL(append(conditions, join_path.other_join_conditions_))) {
      LOG_WARN("failed to append conditions", K(ret));
    } else if (OB_FAIL(append_array_no_dup(conditions, join_path.right_path_->pushdown_filters_))) {
      LOG_WARN("failed to append keys", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(ObOptimizerUtil::get_equal_keys(conditions,
                                                     join_path.left_path_->parent_->get_tables(),
                                                     left_keys,
                                                     right_keys,
                                                     null_safe_info))) {
    LOG_WARN("failed to get equal keys", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

// to support null skew handling
void ObLogPlan::compute_null_distribution_info(const ObJoinType &join_type,
                                              ObExchangeInfo &left_exch_info,
                                              ObExchangeInfo &right_exch_info,
                                              ObIArray<bool> &null_safe_info)
{
  bool contain_ns_cond = false;
  for (int64_t i = 0; i < null_safe_info.count(); ++i) {
    // as null safe is rarely used, don't need to early break the loop
    contain_ns_cond = contain_ns_cond || null_safe_info.at(i);
  }
  if (contain_ns_cond) {
    left_exch_info.null_row_dist_method_ = ObNullDistributeMethod::NONE;
    right_exch_info.null_row_dist_method_ = ObNullDistributeMethod::NONE;
  } else {
    switch (join_type) {
      case ObJoinType::INNER_JOIN: {
        left_exch_info.null_row_dist_method_ = ObNullDistributeMethod::DROP;
        right_exch_info.null_row_dist_method_ = ObNullDistributeMethod::DROP;
        break;
      }
      case ObJoinType::LEFT_SEMI_JOIN:
      case ObJoinType::RIGHT_SEMI_JOIN:
        left_exch_info.null_row_dist_method_ = ObNullDistributeMethod::DROP;
        right_exch_info.null_row_dist_method_ = ObNullDistributeMethod::DROP;
        break;
      case ObJoinType::LEFT_OUTER_JOIN:
        left_exch_info.null_row_dist_method_ = ObNullDistributeMethod::NONE;
        right_exch_info.null_row_dist_method_ = ObNullDistributeMethod::DROP;
        break;
      case ObJoinType::RIGHT_OUTER_JOIN:
        left_exch_info.null_row_dist_method_ = ObNullDistributeMethod::DROP;
        right_exch_info.null_row_dist_method_ = ObNullDistributeMethod::NONE;
        break;
      case ObJoinType::FULL_OUTER_JOIN:
        left_exch_info.null_row_dist_method_ = ObNullDistributeMethod::NONE;
        right_exch_info.null_row_dist_method_ = ObNullDistributeMethod::NONE;
        break;
      default:
        left_exch_info.null_row_dist_method_ = ObNullDistributeMethod::NONE;
        right_exch_info.null_row_dist_method_ = ObNullDistributeMethod::NONE;
        break;
    }
  }
}

int ObLogPlan::get_histogram_by_join_exprs(ObOptimizerContext &optimizer_ctx,
                                           const ObDMLStmt *stmt,
                                           const ObRawExpr &expr,
                                           ObOptColumnStatHandle &handle) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = optimizer_ctx.get_session_info();
  ObSchemaGetterGuard *schema_guard = optimizer_ctx.get_schema_guard();
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info get unexpected null", K(ret));
  } else {
    uint64_t table_id = static_cast<const ObColumnRefRawExpr&>(expr).get_table_id();
    uint64_t column_id = static_cast<const ObColumnRefRawExpr&>(expr).get_column_id();
    const TableItem *table_item = NULL;
    const ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument passed in", K(table_id), K(ret));
    } else if (!table_item->is_basic_table()) {
      // nop, don't skip none base table (such as view) for now.
    } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                      table_item->ref_id_, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(table_id), K(column_id), K(table_item->ref_id_), K(*table_item));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(optimizer_ctx.get_opt_stat_manager()->get_column_stat(
                session_info->get_effective_tenant_id(),
                table_item->ref_id_,
                table_schema->is_partitioned_table() ? -1 : table_item->ref_id_, /* use -1 for part table */
                column_id, handle))) {
      LOG_WARN("fail get full table column stat", K(ret), K(table_id), K(column_id));
    } else if (OB_ISNULL(handle.stat_)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObLogPlan::check_if_use_hybrid_hash_distribution(ObOptimizerContext &optimizer_ctx,
                                                     const ObDMLStmt *stmt,
                                                     ObJoinType join_type,
                                                     ObRawExpr  &expr,
                                                     ObIArray<ObObj> &popular_values) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = optimizer_ctx.get_session_info();
  bool enable_skew_handling = optimizer_ctx.get_session_info()->get_px_join_skew_handling();
  if (OB_SUCC(ret)
      && enable_skew_handling
      && expr.is_column_ref_expr()
      && (ObJoinType::INNER_JOIN == join_type
          || ObJoinType::RIGHT_OUTER_JOIN == join_type
          || ObJoinType::RIGHT_SEMI_JOIN == join_type
          || RIGHT_ANTI_JOIN == join_type)
     ) {
    ObOptColumnStatHandle handle;
    if (OB_FAIL(get_histogram_by_join_exprs(optimizer_ctx,
                                            stmt,
                                            expr,
                                            handle))) {
      LOG_WARN("fail get hisstogram by join exprs", K(ret));
    } else if (OB_FAIL(get_popular_values_hash(get_allocator(), handle, popular_values))) {
      LOG_WARN("fail get popular values hash", K(ret));
    }
  }
  return ret;
}
int ObLogPlan::get_popular_values_hash(ObIAllocator &allocator,
                                       ObOptColumnStatHandle &handle,
                                       common::ObIArray<ObObj> &popular_values) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(handle.stat_)
      || 0 >= handle.stat_->get_last_analyzed()
      || handle.stat_->get_histogram().get_bucket_size() <= 0) {
    // no histogram info, don't use hybrid hash
    LOG_DEBUG("table not analyzed. disable hybrid hash DM", K(ret));
  } else {
    const ObHistogram &histogram = handle.stat_->get_histogram();
    // get total value count via last bucket by it's cumulative endpoint num
    const ObHistBucket &last_bucket = histogram.get(histogram.get_bucket_size() - 1);
    int64_t total_cnt = std::max(1L, last_bucket.endpoint_num_); // avoid zero div
    int64_t min_freq = optimizer_context_.get_session_info()->get_px_join_skew_minfreq();
    for (int64_t i = 0; OB_SUCC(ret) && i < histogram.get_bucket_size(); ++i) {
      const ObHistBucket &bucket = histogram.get(i);
      int64_t freq = bucket.endpoint_repeat_count_ * 100 / total_cnt;
      if (freq >= min_freq) {
        ObObj value;
        if (OB_FAIL(ob_write_obj(allocator, bucket.endpoint_value_, value))) {
          LOG_WARN("fail write object", K(ret));
        } else if (OB_FAIL(popular_values.push_back(value))) {
          LOG_WARN("fail add value to exchange info", K(ret));
        }
      }
      LOG_DEBUG("add a popular value to array",
                K(freq), K(bucket.endpoint_repeat_count_), K(total_cnt), K(min_freq));
    }
  }
  return ret;
}

int ObLogPlan::assign_right_popular_value_to_left(ObExchangeInfo &left_exch_info,
                                                  ObExchangeInfo &right_exch_info)
{
  int ret = OB_SUCCESS;
  // for join char with text, popular value from char col histogram should add lob locator
  for (int i = 0; OB_SUCC(ret) && i < right_exch_info.popular_values_.count(); i++) {
    const ObObj &pv = right_exch_info.popular_values_.at(i);
    if (left_exch_info.hash_dist_exprs_.count() == 0 ||
        OB_ISNULL(left_exch_info.hash_dist_exprs_.at(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left_exch_info hash_dist_exprs_ is empty or null.", K(ret), K(left_exch_info));
    } else {
      ObObjType expect_type = left_exch_info.hash_dist_exprs_.at(0).expr_->get_result_meta().get_type();
      bool need_cast = (!is_lob_storage(pv.get_type()) && is_lob_storage(expect_type)) ||
                       (is_lob_storage(pv.get_type()) && !is_lob_storage(expect_type));
      ObObj new_pv;
      if (need_cast) {
        ObCastCtx cast_ctx(&get_allocator(), NULL, CM_NONE, pv.get_meta().get_collation_type());
        if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, pv, new_pv))) {
          LOG_WARN("failed to do cast obj", K(ret), K(pv), K(expect_type));;
        }
      } else {
        new_pv = pv;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(left_exch_info.popular_values_.push_back(new_pv))) {
        LOG_WARN("failed to push obj to left_exch_info popular_values", K(ret), K(new_pv), K(left_exch_info));
      }
    }
  }
  return ret;
}

int ObLogPlan::compute_hash_distribution_info(const ObJoinType &join_type,
                                              const bool enable_hybrid_hash_dm,
                                              const ObIArray<ObRawExpr*> &join_exprs,
                                              const ObRelIds &left_table_set,
                                              ObExchangeInfo &left_exch_info,
                                              ObExchangeInfo &right_exch_info)
{
  int ret = OB_SUCCESS;
  bool use_hybrid_hash = false;
  if (OB_UNLIKELY(join_exprs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join expr is empty", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < join_exprs.count(); i++) {
      ObRawExpr *expr = NULL;
      ObRawExpr *left_expr = NULL;
      ObRawExpr *right_expr = NULL;
      if (OB_ISNULL(expr = join_exprs.at(i)) ||
          OB_ISNULL(left_expr = expr->get_param_expr(0)) ||
          OB_ISNULL(right_expr = expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(expr), K(left_expr), K(right_expr), K(ret));
      } else {
        if (!left_expr->get_relation_ids().is_subset(left_table_set)) {
          std::swap(left_expr, right_expr);
        }
        if (OB_FAIL(left_exch_info.hash_dist_exprs_.push_back(
                    ObExchangeInfo::HashExpr(left_expr,
                                             expr->get_result_type().get_calc_meta())))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(right_exch_info.hash_dist_exprs_.push_back(
                    ObExchangeInfo::HashExpr(right_expr,
                                             expr->get_result_type().get_calc_meta())))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // for now, only support hybrid hash DM with only 1 base column join condition
      // after DSQ supported, we can do better with more scenarios.
      if (enable_hybrid_hash_dm && right_exch_info.hash_dist_exprs_.count() > 0) {
        if (OB_FAIL(check_if_use_hybrid_hash_distribution(
                    optimizer_context_,
                    get_stmt(),
                    join_type,
                    *right_exch_info.hash_dist_exprs_.at(0).expr_,
                    right_exch_info.popular_values_))) {
          LOG_WARN("fail check use hybrid hash dist", K(ret));
        } else if (OB_FAIL(assign_right_popular_value_to_left(left_exch_info, right_exch_info))) {
          LOG_WARN("fail to assign right exch info popular value to left", K(ret), K(left_exch_info), K(right_exch_info));
        } else {
          left_exch_info.dist_method_ = ObPQDistributeMethod::HYBRID_HASH_BROADCAST;
          right_exch_info.dist_method_ = ObPQDistributeMethod::HYBRID_HASH_RANDOM;
        }
      } else {
        left_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
        right_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
      }
    }
  }
  return ret;
}

int ObLogPlan::compute_single_side_hash_distribution_info(const EqualSets &equal_sets,
                                                          const ObIArray<ObRawExpr*> &src_keys,
                                                          const ObIArray<ObRawExpr*> &target_keys,
                                                          const Path &target_path,
                                                          ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(target_path.log_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(compute_single_side_hash_distribution_info(equal_sets,
                                                                src_keys,
                                                                target_keys,
                                                                *target_path.log_op_,
                                                                exch_info))) {
    LOG_WARN("failed to compute single side hash distribution info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::compute_single_side_hash_distribution_info(const EqualSets &equal_sets,
                                                          const ObIArray<ObRawExpr*> &src_keys,
                                                          const ObIArray<ObRawExpr*> &target_keys,
                                                          const ObLogicalOperator &target_op,
                                                          ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> hash_dist_exprs;
  if (OB_ISNULL(target_op.get_strong_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_repartition_keys(equal_sets,
                                          src_keys,
                                          target_keys,
                                          target_op.get_strong_sharding()->get_partition_keys(),
                                          hash_dist_exprs))) {
    LOG_WARN("failed to get hash dist keys", K(ret));
  } else if (OB_FAIL(exch_info.append_hash_dist_expr(hash_dist_exprs))) {
    LOG_WARN("failed to append hash dist expr", K(ret));
  } else {
    exch_info.dist_method_ = ObPQDistributeMethod::HASH;
  }
  return ret;
}

int ObLogPlan::compute_repartition_distribution_info(const EqualSets &equal_sets,
                                                     const ObIArray<ObRawExpr*> &src_keys,
                                                     const ObIArray<ObRawExpr*> &target_keys,
                                                     const Path &target_path,
                                                     ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(target_path.log_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(compute_repartition_distribution_info(equal_sets,
                                                           src_keys,
                                                           target_keys,
                                                           *target_path.log_op_,
                                                           exch_info))) {
    LOG_WARN("failed to compute repartition distribution info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::compute_repartition_distribution_info(const EqualSets &equal_sets,
                                                     const ObIArray<ObRawExpr*> &src_keys,
                                                     const ObIArray<ObRawExpr*> &target_keys,
                                                     const ObLogicalOperator &target_op,
                                                     ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  ObString table_name;
  uint64_t ref_table_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;
  if (OB_ISNULL(target_op.get_strong_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_repartition_table_info(target_op,
                                                table_name,
                                                ref_table_id,
                                                table_id))) {
    LOG_WARN("failed to get repartition table info", K(ret));
  } else if (OB_FAIL(compute_repartition_distribution_info(equal_sets,
                                                           src_keys,
                                                           target_keys,
                                                           ref_table_id,
                                                           table_id,
                                                           table_name,
                                                           *target_op.get_strong_sharding(),
                                                           exch_info))) {
    LOG_WARN("failed to compute repartition distribution info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::find_base_sharding_table_scan(const ObLogicalOperator &op,
                                             const ObLogTableScan *&tsc)
{
  int ret = OB_SUCCESS;
  if (LOG_TABLE_SCAN == op.get_type()) {
    tsc = static_cast<const ObLogTableScan*>(&op);
  } else if (-1 == op.get_inherit_sharding_index()) {
    // return null tsc.
  } else if (OB_UNLIKELY(op.get_inherit_sharding_index() >= op.get_child_list().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected sharding src index", K(op.get_inherit_sharding_index()), K(ret));
  } else if (OB_ISNULL(op.get_child(op.get_inherit_sharding_index()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(SMART_CALL(find_base_sharding_table_scan(*op.get_child(op.get_inherit_sharding_index()),
                                                              tsc)))) {
    LOG_WARN("failed to find base sharding table scan", K(ret));
  }
  return ret;
}

int ObLogPlan::get_repartition_table_info(const ObLogicalOperator &op,
                                          ObString &table_name,
                                          uint64_t &ref_table_id,
                                          uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator *cur_op = &op;
  const ObLogTableScan *table_scan = NULL;
  if (OB_FAIL(find_base_sharding_table_scan(op, table_scan))) {
    LOG_WARN("find table scan failed", K(ret));
  } else if (OB_ISNULL(table_scan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    table_name = table_scan->get_table_name();
    table_id = table_scan->get_table_id();
    ref_table_id = table_scan->get_index_table_id();
  }
  return ret;
}

int ObLogPlan::compute_repartition_distribution_info(const EqualSets &equal_sets,
                                                     const ObIArray<ObRawExpr*> &src_keys,
                                                     const ObIArray<ObRawExpr*> &target_keys,
                                                     const uint64_t ref_table_id,
                                                     const uint64_t table_id,
                                                     const ObString &table_name,
                                                     const ObShardingInfo &target_sharding,
                                                     ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session), K(ret));
  } else if (OB_FAIL(compute_repartition_func_info(equal_sets,
                                                   src_keys,
                                                   target_keys,
                                                   target_sharding,
                                                   get_optimizer_context().get_expr_factory(),
                                                   exch_info))) {
    LOG_WARN("failed to compute repartition func info", K(ret));
  } else {
    exch_info.dist_method_ = ObPQDistributeMethod::PARTITION;
    exch_info.repartition_ref_table_id_ = ref_table_id;
    exch_info.repartition_table_id_ = table_id;
    exch_info.repartition_table_name_ = table_name;
    exch_info.slice_count_ = target_sharding.get_part_cnt();
    if (share::schema::PARTITION_LEVEL_ONE == target_sharding.get_part_level()) {
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
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(exch_info.init_calc_part_id_expr(get_optimizer_context()))) {
      LOG_WARN("failed to init calc part id expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::compute_repartition_func_info(const EqualSets &equal_sets,
                                             const ObIArray<ObRawExpr *> &src_keys,
                                             const ObIArray<ObRawExpr *> &target_keys,
                                             const ObShardingInfo &target_sharding,
                                             ObRawExprFactory &expr_factory,
                                             ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObRawExpr*, 4> repart_exprs;
  ObSEArray<ObRawExpr*, 4> repart_sub_exprs;
  ObSEArray<ObRawExpr*, 4> repart_func_exprs;
  ObRawExprCopier copier(expr_factory);
  // get repart exprs
  bool skip_part = target_sharding.is_partition_single();
  bool skip_subpart = target_sharding.is_subpartition_single();
  if (OB_ISNULL(session_info = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(ret));
  } else if (!skip_part && OB_FAIL(get_repartition_keys(equal_sets,
                                                        src_keys,
                                                        target_keys,
                                                        target_sharding.get_partition_keys(),
                                                        repart_exprs))) {
    LOG_WARN("failed to get repartition keys", K(ret));
  } else if (!skip_subpart && OB_FAIL(get_repartition_keys(equal_sets,
                                                           src_keys,
                                                           target_keys,
                                                           target_sharding.get_sub_partition_keys(),
                                                           repart_sub_exprs))) {
    LOG_WARN("failed to get repartition keys", K(ret));
  } else if (!skip_part &&
             OB_FAIL(copier.add_replaced_expr(target_sharding.get_partition_keys(),
                                              repart_exprs))) {
    LOG_WARN("failed to add replace pair", K(ret));
  } else if (!skip_subpart &&
             OB_FAIL(copier.add_replaced_expr(target_sharding.get_sub_partition_keys(),
                                              repart_sub_exprs))) {
    LOG_WARN("failed to add replace pair", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < target_sharding.get_partition_func().count(); i++) {
      ObRawExpr *repart_func_expr = NULL;
      ObRawExpr *target_func_expr = target_sharding.get_partition_func().at(i);
      if ((0 == i && skip_part) || (1 == i && skip_subpart)) {
        // 对于只涉及到一个一级（二级）分区的二级分区表，做repart重分区并不需要生成一级（二级）分区
        // 的repart function。但对于二级分区表要求repart_func_exprs的数量必须为两个，因此在对应
        // 的位置放一个常量作为dummy repart function
        ObConstRawExpr *const_expr = NULL;
        ObRawExpr *dummy_expr = NULL;
        int64_t const_value = 1;
        if (OB_FAIL(ObRawExprUtils::build_const_int_expr(get_optimizer_context().get_expr_factory(),
                                                         ObIntType,
                                                         const_value,
                                                         const_expr))) {
          LOG_WARN("Failed to build const expr", K(ret));
        } else if (OB_ISNULL(dummy_expr = const_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(dummy_expr->formalize(session_info))) {
          LOG_WARN("Failed to formalize a new expr", K(ret));
        } else if (OB_FAIL(repart_func_exprs.push_back(dummy_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else if (OB_FAIL(copier.copy_on_replace(target_func_expr,
                                                repart_func_expr))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      } else if (OB_ISNULL(repart_func_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(repart_func_exprs.push_back(repart_func_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(exch_info.repartition_keys_.assign(repart_exprs)) ||
          OB_FAIL(exch_info.repartition_sub_keys_.assign(repart_sub_exprs)) ||
          OB_FAIL(exch_info.repartition_func_exprs_.assign(repart_func_exprs))) {
        LOG_WARN("failed to set repartition keys", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::get_repartition_keys(const EqualSets &equal_sets,
                                    const ObIArray<ObRawExpr*> &src_keys,
                                    const ObIArray<ObRawExpr*> &target_keys,
                                    const ObIArray<ObRawExpr*> &target_part_keys,
                                    ObIArray<ObRawExpr *> &src_part_keys,
                                    const bool ignore_no_match /* default false */ )
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src_keys.count() != target_keys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count",
        K(src_keys.count()), K(target_keys.count()), K(ret));
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
          } else if (ObOptimizerUtil::is_expr_equivalent(target_part_keys.at(i),
              target_keys.at(j), equal_sets)
              && target_part_keys.at(i)->get_result_type().get_type_class()
                  == src_keys.at(j)->get_result_type().get_type_class()
              && target_part_keys.at(i)->get_result_type().get_collation_type()
                  == src_keys.at(j)->get_result_type().get_collation_type()
              && !ObObjCmpFuncs::is_otimestamp_cmp(target_part_keys.at(i)->get_result_type().get_type(),
                  src_keys.at(j)->get_result_type().get_type())
              && !ObObjCmpFuncs::is_datetime_timestamp_cmp(
                  target_part_keys.at(i)->get_result_type().get_type(),
                  src_keys.at(j)->get_result_type().get_type())) {
            if (OB_FAIL(src_part_keys.push_back(src_keys.at(j)))) {
              LOG_WARN("failed to push back keys", K(ret));
            } else {
              is_find = true;
            }
          } else { /*do nothing*/ }
        }
        if (OB_SUCC(ret) && !is_find && !ignore_no_match) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not find part expr", K(target_part_keys.at(i)),
              K(src_keys), K(target_keys), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::set_connect_by_property(JoinPath *join_path,
                                       ObLogJoin &join_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_path) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(join_path->left_path_) || OB_ISNULL(join_path->left_path_->parent_) ||
      OB_ISNULL(join_path->right_path_) || OB_ISNULL(join_path->right_path_->parent_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(join_path), K(ret));
  } else if (OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stmt type", K(get_stmt()->get_stmt_type()), K(ret));
  } else {
    ObSEArray<ObRawExpr*, 8> connect_by_extra_exprs;
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt *>(get_stmt());
    if (OB_FAIL(select_stmt->get_column_exprs(join_path->left_path_->parent_->get_table_id(),
                                              connect_by_extra_exprs))) {
      LOG_WARN("failed to get left table columns", K(ret));
    } else if (OB_FAIL(select_stmt->get_column_exprs(join_path->right_path_->parent_->get_table_id(),
                                                     connect_by_extra_exprs))) {
      LOG_WARN("failed to get right table columns", K(ret));
    } else if (OB_FAIL(join_op.set_connect_by_extra_exprs(connect_by_extra_exprs))) {
      LOG_WARN("failed to set connect by extra exprs", K(ret));
    } else if (OB_FAIL(join_op.set_connect_by_prior_exprs(select_stmt->get_connect_by_prior_exprs()))) {
      LOG_WARN("fail to set connect by priro expr", K(ret));
    } else if (OB_FAIL(select_stmt->get_connect_by_pseudo_exprs(join_op.get_connect_by_pseudo_columns()))) {
      LOG_WARN("failed to get connect by pseudo exprs", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::allocate_subquery_path(SubQueryPath *subpath,
                                      ObLogicalOperator *&out_subquery_path_op)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *root = NULL;
  ObLogSubPlanScan *subplan_scan = NULL;
  const TableItem *table_item = NULL;
  if (OB_ISNULL(subpath) || OB_ISNULL(root = subpath->root_) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(subpath->subquery_id_)) ||
      OB_ISNULL(table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(subpath), K(root), K(get_stmt()), K(table_item), K(ret));
  } else if (OB_ISNULL(subplan_scan = static_cast<ObLogSubPlanScan*>
                      (get_log_op_factory().allocate(*this, LOG_SUBPLAN_SCAN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate subquery operator", K(ret));
  } else {
    subplan_scan->set_subquery_id(subpath->subquery_id_);
    subplan_scan->set_child(ObLogicalOperator::first_child, root);
    subplan_scan->set_inherit_sharding_index(subpath->inherit_sharding_index_);
    subplan_scan->get_subquery_name().assign_ptr(table_item->table_name_.ptr(),
                                                 table_item->table_name_.length());
    if (OB_FAIL(append(subplan_scan->get_filter_exprs(), subpath->filter_))) {
      LOG_WARN("failed to allocate_filter", K(ret));
    } else if (OB_FAIL(append(subplan_scan->get_pushdown_filter_exprs(), subpath->pushdown_filters_))) {
      LOG_WARN("failed to append pushdown filters", K(ret));
    } else if (OB_FAIL(subplan_scan->compute_property(subpath))) {
      LOG_WARN("failed to compute property", K(ret));
    } else if (OB_FAIL(subplan_scan->pick_out_startup_filters())) {
      LOG_WARN("failed to pick out startup filters", K(ret));
    } else {
      out_subquery_path_op = subplan_scan;
    }
    if (OB_SUCC(ret)) {
      bool contains_assignment = false;
      if (OB_FAIL(ObOptimizerUtil::check_contains_assignment(table_item->ref_query_,
                                                             contains_assignment))) {
        LOG_WARN("failed to check contains assignment", K(ret));
      } else if (contains_assignment &&
                 OB_FAIL(allocate_material_as_top(out_subquery_path_op))) {
        LOG_WARN("failed to allocate meterail as top", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_material_as_top(ObLogicalOperator *&old_top)
{
  int ret = OB_SUCCESS;
  ObLogMaterial *material_op = NULL;
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

int ObLogPlan::create_plan_tree_from_path(Path *path,
                                          ObLogicalOperator *&out_plan_tree)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *op = NULL;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(path));
  } else if (NULL != path->log_op_) {
    out_plan_tree = path->log_op_;
  } else {
    if (path->is_access_path()) {
      AccessPath *access_path = static_cast<AccessPath *>(path);
      if (OB_FAIL(allocate_access_path(access_path, op))) {
        LOG_WARN("failed to allocate access path", K(ret));
      } else { /*do nothing*/ }
    } else if (path->is_cte_path()) {
      CteTablePath *cte_table_path = static_cast<CteTablePath*>(path);
      if (OB_FAIL(allocate_cte_table_path(cte_table_path, op))) {
        LOG_WARN("failed to allocate cte table path", K(ret));
      } else { /*do nothing*/ }
    } else if (path->is_function_table_path()) {
      FunctionTablePath *func_table_path = static_cast<FunctionTablePath *>(path);
      if (OB_FAIL(allocate_function_table_path(func_table_path, op))) {
        LOG_WARN("failed to allocate function table path", K(ret));
      } else { /* Do nothing */ }
    } else if (path->is_json_table_path()) {
      JsonTablePath *json_table_path = static_cast<JsonTablePath *>(path);
      if (OB_FAIL(allocate_json_table_path(json_table_path, op))) {
        LOG_WARN("failed to allocate json table path", K(ret));
      } else { /* Do nothing */ }
    } else if (path->is_temp_table_path()) {
      TempTablePath *temp_table_path = static_cast<TempTablePath *>(path);
      if (OB_FAIL(allocate_temp_table_path(temp_table_path, op))) {
        LOG_WARN("failed to allocate access path", K(ret));
      } else { /* Do nothing */ }
    } else if (path->is_join_path()) {
      JoinPath *join_path = static_cast<JoinPath *>(path);
      if (OB_FAIL(allocate_join_path(join_path, op))) {
        LOG_WARN("failed to allocate join path", K(ret));
      } else {/* do nothing */ }
    } else if (path->is_subquery_path()) {
      SubQueryPath *subquery_path = static_cast<SubQueryPath *>(path);
      if (OB_FAIL(allocate_subquery_path(subquery_path, op))) {
        LOG_WARN("failed to allocate subquery path", K(ret));
      } else { /* Do nothing */ }
    } else if (path->is_values_table_path()) {
      ValuesTablePath *values_table_path = static_cast<ValuesTablePath *>(path);
      if (OB_FAIL(allocate_values_table_path(values_table_path, op))) {
        LOG_WARN("failed to allocate values table path", K(ret));
      } else { /* Do nothing */ }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected path type");
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(allocate_subplan_filter_for_on_condition(path->subquery_exprs_, op))) {
        LOG_WARN("failed to allocate subplan filter", K(ret));
      } else if (OB_FAIL(append(op->equal_param_constraints_,
                                path->equal_param_constraints_))) {
        LOG_WARN("failed to append param constraints", K(ret));
      } else if (OB_FAIL(append(op->const_param_constraints_,
                                path->const_param_constraints_))) {
        LOG_WARN("failed to append param constraints", K(ret));
      } else if (OB_FAIL(append(op->expr_constraints_,
                                path->expr_constraints_))) {
        LOG_WARN("failed to append expr constraints", K(ret));
      } else {
        path->log_op_ = op;
        out_plan_tree = op;
      }
    }
  }
  return ret;
}

int ObLogPlan::init_candidate_plans()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_order_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(join_order_));
  } else {
    int64_t total_usage = allocator_.total();
    LOG_TRACE("memory usage after generating join order", K(total_usage));
    ObSEArray<CandidatePlan, 8> candi_plans;
    for (int64_t i = 0; OB_SUCC(ret) && i < join_order_->get_interesting_paths().count(); i++) {
      ObLogicalOperator *root = NULL;
      if (OB_ISNULL(join_order_->get_interesting_paths().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (OB_FAIL(create_plan_tree_from_path(join_order_->get_interesting_paths().at(i),
                                                    root))) {
        LOG_WARN("failed to create a path", K(ret));
      } else if (OB_ISNULL(root)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(root));
      } else if (OB_FAIL(append(root->get_filter_exprs(),
                                get_special_exprs()))) {
        LOG_WARN("failed to append special to filter", K(ret));
      } else if (OB_FAIL(append_array_no_dup(root->get_startup_exprs(),
                                             get_startup_filters()))) {
        LOG_WARN("failed to append startup filters", K(ret));
      } else if (OB_FAIL(candi_plans.push_back(CandidatePlan(root)))) {
        LOG_WARN("failed to push back candidate plan", K(ret));
      } else {
        int64_t plan_usage = allocator_.total() - total_usage;
        total_usage = allocator_.total();
        LOG_TRACE("memory usage after generate a candidate", K(total_usage), K(plan_usage));
        total_usage = allocator_.total();
      }
    } // for join orders end

    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_candidate_plans(candi_plans))) {
        LOG_WARN("failed to init candidates", K(ret));
      } else {
        LOG_TRACE("succeed to init candidate plans", K(candidates_.candidate_plans_.count()));
      }
    }
  }
  return ret;
}

int ObLogPlan::init_candidate_plans(ObIArray<CandidatePlan> &candi_plans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(candidates_.candidate_plans_.assign(candi_plans))) {
    LOG_WARN("failed to push back candi plans", K(ret));
  } else if (OB_UNLIKELY(get_optimizer_context().generate_random_plan())) {
    candidates_.is_final_sort_ = false;
    int64_t idx = ObRandom::rand(0, candi_plans.count() - 1);
    if (OB_UNLIKELY(0 > idx || candi_plans.count() <= idx )
        || OB_ISNULL(candi_plans.at(idx).plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected candi_plans", K(ret), K(idx), K(candi_plans.count()));
    } else {
      candidates_.plain_plan_.second = idx;
      candidates_.plain_plan_.first = candi_plans.at(idx).plan_tree_->get_cost();
    }
  } else {
    candidates_.is_final_sort_ = false;
    candidates_.plain_plan_.first = 0;
    candidates_.plain_plan_.second = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_plans.count(); i++) {
      if (OB_ISNULL(candi_plans.at(i).plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (candidates_.plain_plan_.second == -1 ||
                 candi_plans.at(i).plan_tree_->get_cost() < candidates_.plain_plan_.first) {
        candidates_.plain_plan_.second = i;
        candidates_.plain_plan_.first = candi_plans.at(i).plan_tree_->get_cost();
      } else { /* do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_err_log(const ObDelUpdStmt *del_upd_stmt)
{
  int ret = OB_SUCCESS;
  CandidatePlan candidate_plan;
  ObSEArray<CandidatePlan, 4> error_log_plans;
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
    candidate_plan = candidates_.candidate_plans_.at(i);
    if (OB_ISNULL(candidate_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(allocate_err_log_as_top(del_upd_stmt, candidate_plan.plan_tree_))) {
      LOG_WARN("failed to allocate select into", K(ret));
    } else if (OB_FAIL(error_log_plans.push_back(candidate_plan))) {
      LOG_WARN("failed to push back candidate plan", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_and_keep_best_plans(error_log_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::allocate_err_log_as_top(const ObDelUpdStmt *del_upd_stmt, ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObLogErrLog *err_log_op = NULL;
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(top)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ret), K(top), K(del_upd_stmt));
  } else if (OB_ISNULL(err_log_op = static_cast<ObLogErrLog *>(get_log_op_factory().
                       allocate(*this, LOG_ERR_LOG)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate sequence operator", K(ret));
  } else {
    err_log_op->set_del_upd_stmt(del_upd_stmt);
    err_log_op->set_child(ObLogicalOperator::first_child, top);
    if (OB_FAIL(err_log_op->extract_err_log_info())) {
      LOG_WARN("failed to extract err log info", K(ret));
    } else if (OB_FAIL(err_log_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = err_log_op;
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_sequence()
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  CandidatePlan candidate_plan;
  ObSEArray<CandidatePlan, 4> sequence_plans;
  ObDMLStmt *root_stmt = NULL;
  bool will_use_parallel_sequence = false;
  bool has_dblink_sequence = false;
  OPT_TRACE_TITLE("start to generate sequence plan");
  if (OB_ISNULL(root_stmt = get_optimizer_context().get_root_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(root_stmt), K(ret));
  } else if (OB_FAIL(check_has_dblink_sequence(has_dblink_sequence))) {
    LOG_WARN("failed to check has dblink sequence", K(ret));
  } else if (root_stmt->is_explain_stmt() &&
             OB_ISNULL(root_stmt=static_cast<ObExplainStmt*>(root_stmt)->get_explain_query_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(root_stmt), K(ret));
  } else {
    will_use_parallel_sequence = (get_optimizer_context().is_online_ddl() || root_stmt->is_insert_stmt())
                                  && !has_dblink_sequence;
    OPT_TRACE("check will use parallel sequence:", will_use_parallel_sequence);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
    candidate_plan = candidates_.candidate_plans_.at(i);
    OPT_TRACE("generate sequence for plan:", candidate_plan);
    if (OB_ISNULL(candidate_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!will_use_parallel_sequence &&
               candidate_plan.plan_tree_->is_sharding() &&
               allocate_exchange_as_top(candidate_plan.plan_tree_, exch_info)) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_FAIL(allocate_sequence_as_top(candidate_plan.plan_tree_))) {
      LOG_WARN("failed to allocate sequence as top", K(ret));
    } else if (OB_FAIL(sequence_plans.push_back(candidate_plan))) {
      LOG_WARN("failed to push back candidate plan", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_and_keep_best_plans(sequence_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::check_has_dblink_sequence(bool &has)
{
  int ret = OB_SUCCESS;
  has = false;
  ObSQLSessionInfo *session = get_optimizer_context().get_session_info();
  if (OB_ISNULL(session) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is invalid", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < get_stmt()->get_nextval_sequence_ids().count(); ++i) {
    const ObSequenceSchema *seq_schema = NULL;
    if (OB_FAIL(session->get_dblink_sequence_schema(get_stmt()->get_nextval_sequence_ids().at(i),
                                                    seq_schema))) {
      LOG_WARN("failed to get dblink sequence schema", K(ret));
    } else if (NULL != seq_schema) {
      has = true;
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
int ObLogPlan::allocate_sequence_as_top(ObLogicalOperator *&old_top)
{
  int ret = OB_SUCCESS;
  ObLogSequence *sequence = NULL;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Get unexpected null", K(ret), K(old_top), K(get_stmt()));
  } else if (OB_ISNULL(sequence = static_cast<ObLogSequence *>(get_log_op_factory().
                                  allocate(*this, LOG_SEQUENCE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate sequence operator", K(ret));
  } else if (OB_FAIL(append_array_no_dup(sequence->get_sequence_ids(),
                                         get_stmt()->get_nextval_sequence_ids()))) {
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

/*
 * for expr values, old top may be null
 */
int ObLogPlan::allocate_expr_values_as_top(ObLogicalOperator *&top,
                                           const ObIArray<ObRawExpr*> *filter_exprs)
{
  int ret = OB_SUCCESS;
  ObLogExprValues *expr_values = NULL;
  if (OB_ISNULL(expr_values = static_cast<ObLogExprValues *>(get_log_op_factory().
                                  allocate(*this, LOG_EXPR_VALUES)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate expr-values operator", K(ret));
  } else if (NULL != filter_exprs &&
             OB_FAIL(expr_values->get_filter_exprs().assign(*filter_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(expr_values->compute_property())) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    top = expr_values;
  }
  return ret;
}

int ObLogPlan::allocate_values_as_top(ObLogicalOperator *&old_top)
{
  int ret = OB_SUCCESS;
  ObLogValues *values = NULL;
  if (OB_ISNULL(values = static_cast<ObLogValues *>(get_log_op_factory().
                                  allocate(*this, LOG_VALUES)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate expr-values operator", K(ret));
  } else {
    if (NULL != old_top) {
      values->set_child(ObLogicalOperator::first_child, old_top);
    }
    if (OB_FAIL(values->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      old_top = values;
    }
  }
  return ret;
}

int ObLogPlan::allocate_temp_table_insert_as_top(ObLogicalOperator *&top,
                                                 const ObSqlTempTableInfo *temp_table_info)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *op = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(temp_table_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(temp_table_info), K(get_stmt()), K(ret));
  } else if (OB_ISNULL(op = log_op_factory_.allocate(*this, LOG_TEMP_TABLE_INSERT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate temp table operator", K(ret));
  } else {
    top->mark_is_plan_root();
    ObLogTempTableInsert *temp_table_insert = static_cast<ObLogTempTableInsert*>(op);
    temp_table_insert->set_temp_table_id(temp_table_info->temp_table_id_);
    temp_table_insert->get_table_name().assign_ptr(temp_table_info->table_name_.ptr(),
                                                   temp_table_info->table_name_.length());
    if (OB_FAIL(temp_table_insert->add_child(top))) {
      LOG_WARN("failed to add one children", K(ret));
    } else if (OB_FAIL(temp_table_insert->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = temp_table_insert;
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_temp_table_transformation()
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  CandidatePlan candidate_plan;
  ObSEArray<CandidatePlan, 8> temp_table_trans_plans;
  ObSEArray<ObLogicalOperator*, 8> temp_table_insert;
  ObIArray<ObSqlTempTableInfo*> &temp_table_infos = get_optimizer_context().get_temp_table_infos();
  OPT_TRACE_TITLE("start generate temp table transformation");
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); i++) {
    ObLogicalOperator *temp_table_plan = NULL;
    if (OB_ISNULL(temp_table_plan = temp_table_infos.at(i)->table_plan_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(temp_table_plan));
    } else if (OB_FAIL(allocate_temp_table_insert_as_top(temp_table_plan, temp_table_infos.at(i)))) {
      LOG_WARN("failed to allocate temp table insert", K(ret));
    } else if (OB_FAIL(temp_table_insert.push_back(temp_table_plan))) {
      LOG_WARN("failed to push back temp table plan", K(ret));
    } else { /*do nothing*/ }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
    candidate_plan = candidates_.candidate_plans_.at(i);
    OPT_TRACE("generate temp table transformation for plan:", candidate_plan);
    if (OB_FAIL(create_temp_table_transformation_plan(candidate_plan.plan_tree_,
                                                      temp_table_insert))) {
      LOG_WARN("failed to allocate temp table transformation", K(ret));
    } else if (OB_FAIL(temp_table_trans_plans.push_back(candidate_plan))) {
      LOG_WARN("failed to push back temp table transformation", K(ret));
    } else { /*do nothing*/ }
  }
  // choose the best plan
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_and_keep_best_plans(temp_table_trans_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/ }
  }

  return ret;
}

int ObLogPlan::create_temp_table_transformation_plan(ObLogicalOperator *&top,
                                                     const ObIArray<ObLogicalOperator*> &temp_table_insert)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  ObIArray<ObSqlTempTableInfo*> &temp_table_infos = get_optimizer_context().get_temp_table_infos();
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(ret));
  } else if (OB_FAIL(check_basic_sharding_for_temp_table(top,
                                                        temp_table_insert,
                                                        is_basic))) {
    LOG_WARN("failed to check basic temp table transform plan", K(ret));
  } else if (is_basic) {
    if (OB_FAIL(allocate_temp_table_transformation_as_top(top, temp_table_insert))) {
      LOG_WARN("failed to allocate temp-table transformation", K(ret));
    } else { /*do nothing*/ }
  } else if (temp_table_infos.count() != temp_table_insert.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect temp table info count", K(ret));
  } else {
    ObExchangeInfo exch_info;
    ObSEArray<ObLogicalOperator*, 16> child_ops;
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_insert.count(); i++) {
      ObLogicalOperator *temp = temp_table_insert.at(i);
      ObSqlTempTableInfo* info = temp_table_infos.at(i);
      if (OB_ISNULL(temp) || OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (temp->is_sharding() && OB_FAIL(allocate_exchange_as_top(temp, exch_info))) {
        LOG_WARN("failed to allocate exchange as top", K(ret));
      } else if (OB_FAIL(child_ops.push_back(temp))) {
        LOG_WARN("failed to push back child ops", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (top->is_sharding() && OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
      LOG_WARN("failed to allocate exchange info", K(ret));
    } else if (OB_FAIL(allocate_temp_table_transformation_as_top(top, child_ops))) {
      LOG_WARN("failed to allocate temp-table transformation as top", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::check_basic_sharding_for_temp_table(ObLogicalOperator *&top,
                                                  const ObIArray<ObLogicalOperator*> &temp_table_insert,
                                                  bool &is_basic)
{
  int ret = OB_SUCCESS;
  is_basic = false;
  ObSEArray<ObLogicalOperator*, 8> child_ops;
  ObAddr &local_addr = get_optimizer_context().get_local_server_addr();
  if (OB_FAIL(append(child_ops, temp_table_insert))) {
    LOG_WARN("failed to append array", K(ret));
  } else if (OB_FAIL(child_ops.push_back(top))) {
    LOG_WARN("failed to push back array", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_basic_sharding_info(local_addr,
                                                                child_ops,
                                                                is_basic))) {
    LOG_WARN("failed to check basic sharding info", K(ret));
  } else if (!is_basic) {
    //do nothing
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_basic && i < child_ops.count(); ++i) {
    ObLogicalOperator *op = child_ops.at(i);
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null operator", K(ret));
    } else if (op->is_exchange_allocated()) {
      is_basic = false;
    } else {
      is_basic = true;
    }
  }
  return ret;
}

int ObLogPlan::allocate_temp_table_transformation_as_top(ObLogicalOperator *&top,
                                                         const ObIArray<ObLogicalOperator*> &temp_table_insert)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *temp_table_transformation = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top));
  } else if (OB_ISNULL(temp_table_transformation =
             log_op_factory_.allocate(*this, LOG_TEMP_TABLE_TRANSFORMATION))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate temp table operator", K(ret));
  } else if (OB_FAIL(temp_table_transformation->add_child(temp_table_insert))) {
    LOG_WARN("failed to add child ops", K(ret));
  } else if (OB_FAIL(temp_table_transformation->add_child(top))) {
    LOG_WARN("failed to add children", K(ret));
  } else if (OB_FAIL(temp_table_transformation->compute_property())) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    top = temp_table_transformation;
  }
  return ret;
}

int ObLogPlan::candi_allocate_root_exchange()
{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 8> best_candidates;
  if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_, best_candidates))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < best_candidates.count(); i++) {
      ObExchangeInfo exch_info;
      if (OB_ISNULL(best_candidates.at(i).plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED == best_candidates.at(i).plan_tree_->get_phy_plan_type() &&
                 OB_FAIL(compute_duplicate_table_replicas(best_candidates.at(i).plan_tree_))) {
        LOG_WARN("failed to compute duplicate table plan type", K(ret));
      } else if (best_candidates.at(i).plan_tree_->get_phy_plan_type() == ObPhyPlanType::OB_PHY_PLAN_REMOTE) {
        exch_info.is_remote_ = true;
        if (OB_FAIL(allocate_exchange_as_top(best_candidates.at(i).plan_tree_, exch_info))) {
          LOG_WARN("failed to allocate exchange as top", K(ret));
        } else { /*do nothing*/ }
      } else if (best_candidates.at(i).plan_tree_->is_sharding() &&
                 OB_FAIL(allocate_exchange_as_top(best_candidates.at(i).plan_tree_, exch_info))) {
        LOG_WARN("failed to allocate exchange as top", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      ObLogicalOperator *best_plan = NULL;
      if (OB_FAIL(init_candidate_plans(best_candidates))) {
        LOG_WARN("failed to do candi into", K(ret));
      } else if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
        LOG_WARN("failed to get best plan", K(ret));
      } else {
        set_plan_root(best_plan);
        best_plan->mark_is_plan_root();
        get_optimizer_context().set_plan_type(best_plan->get_phy_plan_type(),
                                            best_plan->get_location_type(),
                                            best_plan->is_exchange_allocated());
      }
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_scala_group_by(const ObIArray<ObAggFunRawExpr*> &agg_items)
{
  int ret = OB_SUCCESS;
  bool is_from_povit = false;
  ObSEArray<ObRawExpr*, 1> dummy_having_exprs;
  if (OB_FAIL(candi_allocate_scala_group_by(agg_items, dummy_having_exprs, is_from_povit))) {
    LOG_WARN("failed to allocate scala group by", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::prepare_three_stage_info(const ObIArray<ObRawExpr *> &group_by_exprs,
                                        const ObIArray<ObRawExpr *> &rollup_exprs,
                                        GroupingOpHelper &helper)
{
  int ret = OB_SUCCESS;
  ObIArray<ObAggFunRawExpr *> &aggr_items = helper.distinct_aggr_items_;
  UNUSED(group_by_exprs);
  if (OB_ISNULL(get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is invalid", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_inner_aggr_code_expr(
                       get_optimizer_context().get_expr_factory(),
                       *get_optimizer_context().get_session_info(),
                       helper.aggr_code_expr_))) {
    LOG_WARN("failed to build inner aggr code expr", K(ret));
  } else if (!rollup_exprs.empty() &&
             OB_FAIL(ObRawExprUtils::build_pseudo_rollup_id(
                       get_optimizer_context().get_expr_factory(),
                       *get_optimizer_context().get_session_info(),
                       helper.rollup_id_expr_))) {
    LOG_WARN("failed to build inner aggr code expr", K(ret));
  }


  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_items.count(); ++i) {
    ObAggFunRawExpr *aggr = aggr_items.at(i);
    if (OB_ISNULL(aggr) || OB_UNLIKELY(!aggr->is_param_distinct())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr item is null", K(ret), K(aggr));
    } else if (OB_FAIL(generate_three_stage_aggr_expr(get_optimizer_context().get_expr_factory(),
                                                      *get_optimizer_context().get_session_info(),
                                                      !rollup_exprs.empty(),
                                                      aggr,
                                                      helper.distinct_aggr_batch_,
                                                      helper.distinct_params_))) {
      LOG_WARN("failed generate three stage aggr expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    helper.distinct_aggr_items_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < helper.distinct_aggr_batch_.count(); ++i) {
      const ObDistinctAggrBatch &batch = helper.distinct_aggr_batch_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < batch.mocked_aggrs_.count(); ++j) {
        if (OB_FAIL(helper.distinct_aggr_items_.push_back(batch.mocked_aggrs_.at(j).first))) {
          LOG_WARN("failed to push back mocked aggr exprs", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr *, 4> group_rollup_exprs;
    if (OB_FAIL(append(group_rollup_exprs, group_by_exprs))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(append(group_rollup_exprs, rollup_exprs))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(calculate_group_distinct_ndv(group_rollup_exprs, helper))) {
      LOG_WARN("failed to calculate group distinct ndv", K(ret), K(helper));
    }
  }
  return ret;
}

int ObLogPlan::generate_three_stage_aggr_expr(ObRawExprFactory &expr_factory,
                                              ObSQLSessionInfo &session_info,
                                              const bool is_rollup,
                                              ObAggFunRawExpr *aggr,
                                              ObIArray<ObDistinctAggrBatch> &batch_distinct_aggrs,
                                              ObIArray<ObRawExpr *> &distinct_params)
{
  int ret = OB_SUCCESS;
  bool find_same = false;
  ObAggFunRawExpr *new_aggr = NULL;
  std::pair<ObAggFunRawExpr *, ObAggFunRawExpr *> mocked_aggr;
  // 1. create a mock aggr expr
  if (OB_ISNULL(aggr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregation expr is null", K(ret), K(aggr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(aggr->get_expr_type(), new_aggr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(new_aggr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new aggr is null", K(ret));
  } else if (OB_FAIL(new_aggr->assign(*aggr))) {
    LOG_WARN("failed to assign aggr expr", K(ret));
  } else {
    mocked_aggr.first = aggr;
    mocked_aggr.second = new_aggr;
    if (!is_rollup) {
      new_aggr->set_param_distinct(false);
    }
  }
  // 2. check whether the aggr share the same distinct with others.
  for (int64_t i = 0; OB_SUCC(ret) && !find_same && i < batch_distinct_aggrs.count(); ++i) {
    ObDistinctAggrBatch &batch = batch_distinct_aggrs.at(i);
    find_same = batch.mocked_params_.count() == new_aggr->get_real_param_count();
    for (int64_t j = 0; find_same && j < new_aggr->get_real_param_count(); ++j) {
      find_same = (batch.mocked_params_.at(j).first == new_aggr->get_real_param_exprs().at(j));
    }
    if (find_same) {
      for (int64_t j = 0; j < new_aggr->get_real_param_count(); ++j) {
        new_aggr->get_real_param_exprs_for_update().at(j) = batch.mocked_params_.at(j).second;
      }
      if (OB_FAIL(batch.mocked_aggrs_.push_back(mocked_aggr))) {
        LOG_WARN("failed to push back mocked aggr", K(ret));
      }
    }
  }
  // 3. the aggr does not share distinct exprs with others, create a new batch here
  if (OB_SUCC(ret) && !find_same) {
    ObDistinctAggrBatch batch;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_aggr->get_real_param_exprs().count(); ++i) {
      ObRawExpr *real_param = new_aggr->get_real_param_exprs().at(i);
      ObRawExpr *new_real_param = NULL;
      if (OB_FAIL(ObRawExprUtils::build_dup_data_expr(expr_factory,
                                                      real_param,
                                                      new_real_param))) {
        LOG_WARN("failed to create dup data expr", K(ret));
      } else if (OB_FAIL(batch.mocked_params_.push_back(
                           std::pair<ObRawExpr *, ObRawExpr *>(real_param, new_real_param)))) {
        LOG_WARN("failed to push back the mocked param pair", K(ret));
      } else if (OB_FAIL(distinct_params.push_back(new_real_param))) {
        LOG_WARN("failed to push new real param", K(ret));
      } else {
        new_aggr->get_real_param_exprs_for_update().at(i) = new_real_param;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(batch.mocked_aggrs_.push_back(mocked_aggr))) {
        LOG_WARN("failed to push back mocked aggr", K(ret));
      } else if (OB_FAIL(batch_distinct_aggrs.push_back(batch))) {
        LOG_WARN("failed to push back batch distinct funcs", K(ret));
      }
    }
  }
  return ret;
}

bool ObLogPlan::disable_hash_groupby_in_second_stage()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  bool disable_hash_groupby_in_second = false;
  if (OB_ISNULL(session_info = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info get unexpected null", K(ret), K(lbt()));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(session_info->get_effective_tenant_id()));
    if (tenant_config.is_valid()) {
      disable_hash_groupby_in_second = tenant_config->_sqlexec_disable_hash_based_distagg_tiv;
      LOG_TRACE("trace disable hash groupby in second stage for three-stage",
        K(disable_hash_groupby_in_second));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to init tenant config", K(lbt()));
    }
  }
  return disable_hash_groupby_in_second;
}

int ObLogPlan::create_three_stage_group_plan(const ObIArray<ObRawExpr*> &group_by_exprs,
                                             const ObIArray<ObRawExpr*> &rollup_exprs,
                                             const ObIArray<ObRawExpr*> &having_exprs,
                                             GroupingOpHelper &helper,
                                             ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObLogGroupBy *first_group_by = NULL;
  ObLogGroupBy *second_group_by = NULL;
  ObLogGroupBy *third_group_by = NULL;
  ObArray<ObRawExpr *> dummy_exprs;

  ObSEArray<OrderItem, 4> second_sort_keys;
  ObSEArray<ObRawExpr *, 8> rd_second_sort_exprs;
  ObSEArray<OrderItem, 4> rd_second_sort_keys;
  ObSEArray<OrderItem, 4> rd_second_ecd_sort_keys;
  OrderItem encode_sort_key;
  bool enable_encode_sort = false;
  ObExchangeInfo second_exch_info;

  ObSEArray<OrderItem, 4> third_sort_keys;
  ObExchangeInfo third_exch_info;

  ObSEArray<ObRawExpr *, 8> first_group_by_exprs;
  ObSEArray<ObRawExpr *, 8> second_group_by_exprs;
  ObSEArray<ObRawExpr *, 8> third_group_by_exprs;
  ObSEArray<ObRawExpr *, 8> second_exch_exprs;
  ObSEArray<ObRawExpr *, 8> third_exch_exprs;
  ObSEArray<ObAggFunRawExpr *, 8> second_aggr_items;
  ObSEArray<ObAggFunRawExpr *, 8> third_aggr_items;
  ObSEArray<ObRawExpr *, 8> third_sort_exprs;

  ObRollupStatus second_rollup_status;
  ObRollupStatus third_rollup_status;
  AggregateAlgo second_aggr_algo;
  AggregateAlgo third_aggr_algo;
  bool can_sort_opt = true;
  ObLogicalOperator *child = NULL;
  ObThreeStageAggrInfo three_stage_info;
  double aggr_code_ndv = helper.non_distinct_aggr_items_.empty() ?
                         helper.distinct_aggr_batch_.count() :
                         helper.distinct_aggr_batch_.count() + 1;

  // 1. prepare to allocate the first group by
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(top));
  } else if (OB_FAIL(append(first_group_by_exprs, group_by_exprs)) ||
             OB_FAIL(append(first_group_by_exprs, rollup_exprs)) ||
             OB_FAIL(first_group_by_exprs.push_back(helper.aggr_code_expr_)) ||
             OB_FAIL(append(first_group_by_exprs, helper.distinct_params_))) {
    LOG_WARN("failed to construct first group by exprs", K(ret));
  } else if (OB_FAIL(three_stage_info.set_first_stage_info(helper.aggr_code_expr_,
                                                           helper.distinct_aggr_batch_,
                                                           aggr_code_ndv))) {
    LOG_WARN("failed to set first stage info");
  } else if (OB_FAIL(allocate_group_by_as_top(top,
                                              HASH_AGGREGATE,
                                              first_group_by_exprs,
                                              dummy_exprs,
                                              helper.non_distinct_aggr_items_,
                                              dummy_exprs,
                                              false,
                                              helper.group_distinct_ndv_,
                                              top->get_card(),
                                              false,
                                              true,
                                              false,
                                              ObRollupStatus::NONE_ROLLUP,
                                              &three_stage_info))) {
    LOG_WARN("failed to allocate group by as top", K(ret));
  } else if (OB_UNLIKELY(LOG_GROUP_BY != top->get_type()) ||
             OB_ISNULL(first_group_by = static_cast<ObLogGroupBy *>(top))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first group by is invalid", K(ret), KP(top));
  }

  // 2. prepare to allocate the second group by
  if (OB_SUCC(ret)) {
    if (!rollup_exprs.empty()) {
      second_aggr_algo = MERGE_AGGREGATE;
    } else if (helper.force_use_hash_) {
      second_aggr_algo = HASH_AGGREGATE;
    } else if (helper.force_use_merge_ || disable_hash_groupby_in_second_stage()) {
      second_aggr_algo = MERGE_AGGREGATE;
    } else {
      second_aggr_algo = HASH_AGGREGATE;
    }
    second_rollup_status = !rollup_exprs.empty() ? ROLLUP_DISTRIBUTOR : NONE_ROLLUP;

    if (OB_FAIL(append(second_group_by_exprs, group_by_exprs)) ||
        OB_FAIL(second_group_by_exprs.push_back(helper.aggr_code_expr_))) {
      LOG_WARN("failed to construct second group by exprs", K(ret));
    } else if (OB_FAIL(append(second_aggr_items, helper.distinct_aggr_items_)) ||
               OB_FAIL(append(second_aggr_items, helper.non_distinct_aggr_items_))) {
      LOG_WARN("failed to construct second aggr items", K(ret));
    } else if (OB_FAIL(append(second_exch_exprs, group_by_exprs)) ||
                // Ensure that the rows of the same distinct columns are in the same thread
               OB_FAIL(second_exch_exprs.push_back(helper.aggr_code_expr_)) ||
               OB_FAIL(append(second_exch_exprs, helper.distinct_params_))) {
      LOG_WARN("failed to construct second exchange exprs", K(ret));
    } else if (OB_FAIL(get_grouping_style_exchange_info(second_exch_exprs,
                                                        top->get_output_equal_sets(),
                                                        second_exch_info))) {
      LOG_WARN("failed to get grouping style exchange info", K(ret));
    } else if (second_aggr_algo != MERGE_AGGREGATE ||
               second_rollup_status == ROLLUP_DISTRIBUTOR) {
      // ROLLUP_DISTRIBUTOR has inner sort
      if (second_rollup_status != ROLLUP_DISTRIBUTOR || second_aggr_algo != MERGE_AGGREGATE) {
      } else if (OB_FAIL(append(rd_second_sort_exprs, second_group_by_exprs)) ||
                OB_FAIL(append(rd_second_sort_exprs, rollup_exprs))) {
        LOG_WARN("failed to append rollup distributor sort keys", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(rd_second_sort_exprs,
                                                        default_asc_direction(),
                                                        rd_second_sort_keys))) {
        LOG_WARN("failed to make sort keys", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::check_can_encode_sortkey(rd_second_sort_keys,
                                                                    can_sort_opt, *this, top->get_card()))) {
        LOG_WARN("failed to check encode sortkey expr", K(ret));
      } else if (can_sort_opt
          && (OB_FAIL(ObSQLUtils::create_encode_sortkey_expr(get_optimizer_context().get_expr_factory(),
                                                            get_optimizer_context().get_exec_ctx(),
                                                            rd_second_sort_keys,
                                                            0,
                                                            encode_sort_key)
          // just append
          || FALSE_IT(enable_encode_sort = true)
          || OB_FAIL(rd_second_ecd_sort_keys.push_back(encode_sort_key))))) {
        LOG_WARN("failed to create encode sortkey expr", K(ret));
      }
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(first_group_by_exprs,
                                                       default_asc_direction(),
                                                       second_sort_keys))) {
      LOG_WARN("failed to make sort keys", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(allocate_sort_and_exchange_as_top(top,
                                                  second_exch_info,
                                                  second_sort_keys,
                                                  true,
                                                  0,
                                                  top->get_is_local_order()))) {
      LOG_WARN("failed to allocate sort and exchange as top", K(ret));
    } else if (OB_FAIL(three_stage_info.set_second_stage_info(helper.aggr_code_expr_,
                                                              helper.distinct_aggr_batch_,
                                                              helper.distinct_params_))) {
      LOG_WARN("failed to set second stage info");
    } else if (OB_FAIL(allocate_group_by_as_top(top,
                                                second_aggr_algo,
                                                second_group_by_exprs,
                                                rollup_exprs,
                                                second_aggr_items,
                                                dummy_exprs,
                                                false,
                                                helper.group_ndv_ * aggr_code_ndv,
                                                top->get_card(),
                                                false,
                                                true,
                                                false,
                                                second_rollup_status,
                                                &three_stage_info))) {
      LOG_WARN("failed to allocate group by as top", K(ret));
    } else if (OB_UNLIKELY(LOG_GROUP_BY != top->get_type()) ||
               OB_ISNULL(second_group_by = static_cast<ObLogGroupBy *>(top))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("second group by is invalid", K(ret), KP(top));
    } else if (OB_FAIL(second_group_by->set_rollup_info(second_rollup_status,
                                                        helper.rollup_id_expr_,
                                                        rd_second_sort_keys,
                                                        rd_second_ecd_sort_keys,
                                                        enable_encode_sort))) {
      LOG_WARN("failed to set rollup parallel info", K(ret));
    }
  }

  // 3. prepare to allocate the third group by
  if (OB_SUCC(ret)) {
    if (helper.is_scalar_group_by_) {
      third_aggr_algo = SCALAR_AGGREGATE;
    } else if (group_by_exprs.empty()) {
      third_aggr_algo = MERGE_AGGREGATE;
    } else {
      third_aggr_algo = second_aggr_algo;
    }
    third_rollup_status = !rollup_exprs.empty() ? ROLLUP_COLLECTOR : NONE_ROLLUP;
    third_exch_info.is_rollup_hybrid_ = !rollup_exprs.empty();

    if (OB_FAIL(append(third_group_by_exprs, group_by_exprs))) {
      LOG_WARN("failed to append third group by expr", K(ret));
    } else if (!rollup_exprs.empty() && OB_FAIL(third_group_by_exprs.push_back(helper.rollup_id_expr_))) {
      LOG_WARN("failed to append rollup id expr", K(ret));
    } else if (OB_FAIL(append(third_aggr_items, helper.distinct_aggr_items_))) {
      LOG_WARN("failed to construct third aggregate function exprs", K(ret));
    } else if (!rollup_exprs.empty() && OB_FAIL(append(third_aggr_items, helper.non_distinct_aggr_items_))) {
      LOG_WARN("failed to construct third aggregate function exprs", K(ret));
    } else if (OB_FAIL(append(third_exch_exprs, third_group_by_exprs)) ||
               OB_FAIL(append(third_exch_exprs, rollup_exprs))) {
      LOG_WARN("failed to append to third sort exprs", K(ret));
    } else if (OB_FAIL(get_grouping_style_exchange_info(third_exch_exprs,
                                                        top->get_output_equal_sets(),
                                                        third_exch_info))) {
      LOG_WARN("failed to get grouping style exchange info", K(ret));
    } else if (third_aggr_algo != MERGE_AGGREGATE) {
      // do nothing
    } else if (OB_FAIL(append(third_sort_exprs, third_group_by_exprs)) ||
               OB_FAIL(append(third_sort_exprs, rollup_exprs)) ||
               OB_FAIL(third_sort_exprs.push_back(helper.aggr_code_expr_))) {
      LOG_WARN("failed to create third sort keys", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(third_sort_exprs,
                                                       default_asc_direction(),
                                                       third_sort_keys))) {
      LOG_WARN("failed to make sort keys", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(allocate_sort_and_exchange_as_top(top,
                                                  third_exch_info,
                                                  third_sort_keys,
                                                  0 < rollup_exprs.count() ? true: false,
                                                  true,
                                                  false,
                                                  0,
                                                  top->get_is_local_order()))) {
      LOG_WARN("failed to allocate sort and exchange as top", K(ret));
    } else if (OB_FAIL(three_stage_info.set_third_stage_info(helper.aggr_code_expr_,
                                                             helper.distinct_aggr_batch_))) {
      LOG_WARN("failed to set third stage info");
    } else if (OB_FAIL(allocate_group_by_as_top(top,
                                                third_aggr_algo,
                                                third_group_by_exprs,
                                                rollup_exprs,
                                                third_aggr_items,
                                                having_exprs,
                                                false,
                                                helper.group_ndv_,
                                                top->get_card(),
                                                false,
                                                false,
                                                false,
                                                third_rollup_status,
                                                &three_stage_info))) {
      LOG_WARN("failed to allocate group by as top", K(ret));
    } else if (OB_UNLIKELY(LOG_GROUP_BY != top->get_type()) ||
               OB_ISNULL(third_group_by = static_cast<ObLogGroupBy *>(top))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("second group by is invalid", K(ret), KP(top));
    } else if (OB_FAIL(third_group_by->set_rollup_info(third_rollup_status,
                                                       helper.rollup_id_expr_))) {
      LOG_WARN("failed to set rollup parallel info", K(ret));
    } else {
      third_group_by->set_group_by_outline_info(false, false, HASH_AGGREGATE == second_aggr_algo, true);
    }
  }
  return ret;
}

// old : old wf expr with aggr_expr(param of aggr_expr is orig expr)
// new : new wf expr with aggr_expr(param of aggr_expr is the res of old wf expr)
int ObLogPlan::perform_window_function_pushdown(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  ObLogWindowFunction *window_function = NULL;
  if (NULL != (window_function = dynamic_cast<ObLogWindowFunction *>(op))) {
    ObIArray<ObWinFunRawExpr *> &window_exprs = window_function->get_window_exprs();
    if (window_function->is_consolidator()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < window_exprs.count(); ++i) {
        ObRawExpr *new_wf_expr = NULL;
        ObAggFunRawExpr *new_aggr_expr = NULL;
        if (OB_FAIL(get_optimizer_context().get_expr_factory().create_raw_expr(
            window_exprs.at(i)->get_expr_class(),
            window_exprs.at(i)->get_expr_type(),
            new_wf_expr))) {
          LOG_WARN("failed to create new_wf_expr", K(ret));
        } else if (OB_FAIL(new_wf_expr->assign(*window_exprs.at(i)))) {
          LOG_WARN("failed to assign to new_wf_expr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_common_aggr_expr(
            get_optimizer_context().get_expr_factory(), get_optimizer_context().get_session_info(),
            T_FUN_COUNT == static_cast<ObWinFunRawExpr*>(new_wf_expr)->get_agg_expr()->get_expr_type()
              ? T_FUN_COUNT_SUM
              : static_cast<ObWinFunRawExpr*>(new_wf_expr)->get_agg_expr()->get_expr_type(),
            window_exprs.at(i),
            new_aggr_expr))) {
          LOG_WARN("failed to build_common_aggr_expr", K(ret));
        } else if (FALSE_IT(static_cast<ObWinFunRawExpr*>(new_wf_expr)->set_agg_expr(
                            new_aggr_expr))) {
        } else if (OB_FAIL(window_function_replacer_.add_replace_expr(window_exprs.at(i),
                                                                      new_wf_expr))) {
          LOG_WARN("failed to push back pushdown aggr", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(op->replace_op_exprs(window_function_replacer_))) {
    LOG_WARN("failed to replace generated aggr expr", K(ret));
  }
  return ret;
}

int ObLogPlan::perform_group_by_pushdown(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  ObLogTableScan *table_scan = NULL;
  ObLogGroupBy *group_by = NULL;
  if (NULL != (table_scan = dynamic_cast<ObLogTableScan *>(op))) {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_scan->get_pushdown_aggr_exprs().count(); ++i) {
      ObAggFunRawExpr *old_aggr = table_scan->get_pushdown_aggr_exprs().at(i);
      if (OB_FAIL(group_replaced_exprs_.push_back(
                    std::pair<ObRawExpr *, ObRawExpr *>(old_aggr, old_aggr)))) {
        LOG_WARN("failed to push back scalar aggr replace pair", K(ret));
      }
    }
  } else {
    if (NULL != (group_by = dynamic_cast<ObLogGroupBy *>(op))) {
      for (int64_t i = 0; OB_SUCC(ret) && i < group_by->get_aggr_funcs().count(); ++i) {
        ObRawExpr *expr = group_by->get_aggr_funcs().at(i);
        ObAggFunRawExpr *old_aggr = static_cast<ObAggFunRawExpr *>(expr);
        ObAggFunRawExpr *new_aggr = NULL;
        if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->is_aggr_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid aggr expr", K(ret));
        } else if (OB_FAIL(try_to_generate_pullup_aggr(old_aggr, new_aggr))) {
          LOG_WARN("failed to generate pullup aggr", K(ret));
        } else if (!group_by->is_push_down() || new_aggr != NULL) {
          // do nothing if
          // 1. the group by is not a pushdown operator or
          // 2. the group by is responsible for merging partial aggregations.
        } else if (OB_FAIL(group_replaced_exprs_.push_back(
                             std::pair<ObRawExpr *, ObRawExpr *>(old_aggr, old_aggr)))) {
          LOG_WARN("failed to push back pushdown aggr", K(ret));
        }
      }
      if (group_by->is_second_stage()) {
        for (int64_t i = 0; i < group_by->get_distinct_aggr_batch().count(); ++i) {
          const ObDistinctAggrBatch &batch = group_by->get_distinct_aggr_batch().at(i);
          for (int64_t j = 0; j < batch.mocked_aggrs_.count(); ++j) {
            ObRawExpr *from = batch.mocked_aggrs_.at(j).first;
            ObRawExpr *to = batch.mocked_aggrs_.at(j).second;

            for (int64_t k = 0; k < group_replaced_exprs_.count(); ++k) {
              if (group_replaced_exprs_.at(k).first == from) {
                group_replaced_exprs_.at(k).second = to;
                if (OB_FAIL(group_replacer_.add_replace_expr(from, to))) {
                  LOG_WARN("failed to add replace expr", K(ret));
                }
                break;
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(op->replace_op_exprs(group_replacer_))) {
      LOG_WARN("failed to replace generated aggr expr", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObLogPlan::try_to_generate_pullup_aggr
 * 1. If the old_aggr exists in the group_replaced_exprs_,
 * it means that the aggr is pre-aggregated by a child group-by operator.
 * therefore, the current group-by is responsible for merging partial aggregation results.
 *
 * 2. If the old_aggr does not exists in the group_replaced_exprs_,
 * the current group-by is the first one to generate the aggregation.
 *
 * @return
 */
int ObLogPlan::try_to_generate_pullup_aggr(ObAggFunRawExpr *old_aggr,
                                           ObAggFunRawExpr *&new_aggr)
{
  int ret = OB_SUCCESS;
  new_aggr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < group_replaced_exprs_.count(); ++i) {
    if (group_replaced_exprs_.at(i).first != old_aggr) {
      // do nothing
    } else if (OB_ISNULL(group_replaced_exprs_.at(i).second) ||
               OB_UNLIKELY(!group_replaced_exprs_.at(i).second->is_aggr_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the group replaced expr is expected to be a aggregation", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::generate_pullup_aggr_expr(
                         get_optimizer_context().get_expr_factory(),
                         get_optimizer_context().get_session_info(),
                         static_cast<ObAggFunRawExpr *>(group_replaced_exprs_.at(i).second),
                         new_aggr))) {
      LOG_WARN("failed to generate pullup aggr expr", K(ret));
    } else if (OB_FAIL(group_replacer_.add_replace_expr(old_aggr, new_aggr, true))) {
      LOG_WARN("failed to add replace expr" ,K(ret));
    } else {
      group_replaced_exprs_.at(i).second = new_aggr;
      break;
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_scala_group_by(const ObIArray<ObAggFunRawExpr*> &agg_items,
                                             const ObIArray<ObRawExpr*> &having_exprs,
                                             const bool is_from_povit)
{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 4> groupby_plans;
  ObSEArray<ObRawExpr*, 1> dummy_exprs;
  SMART_VAR(GroupingOpHelper, groupby_helper) {
    if (OB_FAIL(init_groupby_helper(dummy_exprs, dummy_exprs, agg_items,
                                    is_from_povit, groupby_helper))) {
      LOG_WARN("failed to init group by helper", K(ret));
    } else if (OB_FAIL(inner_candi_allocate_scala_group_by(agg_items,
                                                           having_exprs,
                                                           groupby_helper,
                                                           groupby_plans))) {
      LOG_WARN("failed to inner candi allocate scala group by", K(ret));
    } else if (!groupby_plans.empty()) {
      LOG_TRACE("succeed to allocate scala group by using hint", K(groupby_plans.count()), K(groupby_helper));
      OPT_TRACE("success to generate scala group plan with hint");
    } else if (OB_FAIL(get_log_plan_hint().check_status())) {
      LOG_WARN("failed to generate plans with hint", K(ret));
    } else if (OB_FALSE_IT(groupby_helper.set_ignore_hint())) {
    } else if (OB_FAIL(inner_candi_allocate_scala_group_by(agg_items,
                                                           having_exprs,
                                                           groupby_helper,
                                                           groupby_plans))) {
      LOG_WARN("failed to inner candi allocate scala group by", K(ret));
    } else {
      LOG_TRACE("succeed to allocate scala group by ignore hint", K(groupby_plans.count()), K(groupby_helper));
      OPT_TRACE("success to generate scala group plan without hint");
    }

    if (OB_SUCC(ret)) {
      int64_t check_scope = OrderingCheckScope::CHECK_WINFUNC |
                            OrderingCheckScope::CHECK_DISTINCT |
                            OrderingCheckScope::CHECK_SET |
                            OrderingCheckScope::CHECK_ORDERBY;
      if (OB_FAIL(update_plans_interesting_order_info(groupby_plans, check_scope))) {
        LOG_WARN("failed to update plans interesting order info", K(ret));
      } else if (OB_FAIL(prune_and_keep_best_plans(groupby_plans))) {
        LOG_WARN("failed to add plan", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::inner_candi_allocate_scala_group_by(const ObIArray<ObAggFunRawExpr*> &agg_items,
                                                   const ObIArray<ObRawExpr*> &having_exprs,
                                                   GroupingOpHelper &groupby_helper,
                                                   ObIArray<CandidatePlan> &groupby_plans)

{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 4> best_plans;
  if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_, best_plans))) {
    LOG_WARN("failed to get minimal cost candidate", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < best_plans.count(); i++) {
      OPT_TRACE("generate scala group by for plan:", best_plans.at(i));
      if (OB_FAIL(create_scala_group_plan(agg_items,
                                          having_exprs,
                                          groupby_helper,
                                          best_plans.at(i).plan_tree_))) {
        LOG_WARN("failed to create scala group by plan", K(ret));
      } else if (NULL != best_plans.at(i).plan_tree_ &&
                 OB_FAIL(groupby_plans.push_back(best_plans.at(i)))) {
        LOG_WARN("failed to push merge group by", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::create_scala_group_plan(const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                       const ObIArray<ObRawExpr*> &having_exprs,
                                       GroupingOpHelper &groupby_helper,
                                       ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  bool is_partition_wise = false;
  ObSEArray<ObRawExpr*, 1> dummy_exprs;
  ObSEArray<ObAggFunRawExpr*, 1> dummy_aggrs;
  double origin_child_card = 0.0;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(origin_child_card = top->get_card())) {
  } else if (groupby_helper.can_scalar_pushdown_ &&
             OB_FAIL(try_push_aggr_into_table_scan(top, aggr_items))) {
    LOG_WARN("failed to push group by into table scan", K(ret));
  } else if (!top->is_distributed()) {
    OPT_TRACE("generate scala group plan without pushdown");
    if (!groupby_helper.allow_basic()) {
      top = NULL;
      OPT_TRACE("ignore basic scala group by hint");
    } else if (OB_FAIL(allocate_scala_group_by_as_top(top,
                                                      aggr_items,
                                                      having_exprs,
                                                      groupby_helper.is_from_povit_,
                                                      origin_child_card))) {
      LOG_WARN("failed to allocate scala group by as top", K(ret));
    } else {
      static_cast<ObLogGroupBy*>(top)->set_group_by_outline_info(true, false, false, false);
    }
  } else if (!groupby_helper.distinct_exprs_.empty() &&
             OB_FAIL(top->check_sharding_compatible_with_reduce_expr(groupby_helper.distinct_exprs_,
                                                                     is_partition_wise))) {
    LOG_WARN("failed to check if sharding compatible with distinct expr", K(ret));
  } else if (groupby_helper.can_three_stage_pushdown_ &&
             !(is_partition_wise && groupby_helper.allow_partition_wise(top->is_parallel_more_than_part_cnt()))) {
    OPT_TRACE("generate three stage group plan");
    if (NULL == groupby_helper.aggr_code_expr_ &&
        OB_FAIL(prepare_three_stage_info(dummy_exprs, dummy_exprs, groupby_helper))) {
      LOG_WARN("failed to prepare three stage info", K(ret));
    } else if (OB_FAIL(create_three_stage_group_plan(dummy_exprs,
                                                    dummy_exprs,
                                                    having_exprs,
                                                    groupby_helper,
                                                    top))) {
      LOG_WARN("failed to create three stage group plan", K(ret));
    }
  } else {
    if ((groupby_helper.can_basic_pushdown_ || is_partition_wise) &&
        OB_FAIL(allocate_group_by_as_top(top,
                                         AggregateAlgo::MERGE_AGGREGATE,
                                         dummy_exprs,
                                         dummy_exprs,
                                         aggr_items,
                                         dummy_exprs,
                                         groupby_helper.is_from_povit_,
                                         groupby_helper.group_ndv_,
                                         origin_child_card,
                                         is_partition_wise,
                                         true,
                                         is_partition_wise))) {
      LOG_WARN("failed to allocate scala group by as top", K(ret));
    } else if (OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_FAIL(allocate_scala_group_by_as_top(top,
                                                      aggr_items,
                                                      having_exprs,
                                                      groupby_helper.is_from_povit_,
                                                      origin_child_card))) {
      LOG_WARN("failed to allocate scala group by as top", K(ret));
    } else {
      static_cast<ObLogGroupBy*>(top)->set_group_by_outline_info(false, is_partition_wise, false, groupby_helper.can_basic_pushdown_ || is_partition_wise);
    }
  }

  return ret;
}

int ObLogPlan::try_push_aggr_into_table_scan(ObLogicalOperator *top,
                                             const ObIArray<ObAggFunRawExpr *> &aggr_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(top));
  } else if (log_op_def::LOG_TABLE_SCAN == top->get_type()) {
    ObLogTableScan *scan_op = static_cast<ObLogTableScan*>(top);
    bool has_npd_filter = false; //has non-pushdown filter
    if (OB_FAIL(scan_op->has_nonpushdown_filter(has_npd_filter))) {
      LOG_WARN("check whether hash non-pushdown filter failed", K(ret));
    } else if (scan_op->get_index_back() ||
               scan_op->is_sample_scan() ||
               has_npd_filter) {
      //aggr func cannot be pushed down to the storage layer in these scenarios:
      //1. TSC has index lookup
      //2. TSC is sample scan operator
      //3. TSC contains filters that cannot be pushed down to the storage
    } else if (OB_FAIL(scan_op->get_pushdown_aggr_exprs().assign(aggr_items))) {
      LOG_WARN("failed to assign group exprs", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::get_grouping_style_exchange_info(const ObIArray<ObRawExpr*> &partition_exprs,
                                                const EqualSets &equal_sets,
                                                ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  if (!partition_exprs.empty()) {
    ObShardingInfo *sharding_info = NULL;
    if (OB_FAIL(get_cached_hash_sharding_info(partition_exprs, equal_sets, sharding_info))) {
      LOG_WARN("failed to get cached sharding info", K(ret));
    } else if (NULL != sharding_info) {
      if (OB_FAIL(exch_info.append_hash_dist_expr(partition_exprs))) {
        LOG_WARN("failed to append hash dist exprs", K(ret));
      } else {
        exch_info.dist_method_ = ObPQDistributeMethod::HASH;
        exch_info.strong_sharding_ = sharding_info;
      }
    } else if (OB_ISNULL(sharding_info = reinterpret_cast<ObShardingInfo*>(
                         allocator_.alloc(sizeof(ObShardingInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      sharding_info = new(sharding_info) ObShardingInfo();
      if (OB_FAIL(exch_info.append_hash_dist_expr(partition_exprs))) {
        LOG_WARN("append hash dist expr failed", K(ret));
      } else if (OB_FAIL(sharding_info->get_partition_keys().assign(partition_exprs))) {
        LOG_WARN("failed to assign expr", K(ret));
      } else {
        sharding_info->set_distributed();
        exch_info.dist_method_ = ObPQDistributeMethod::HASH;
        exch_info.strong_sharding_ = sharding_info;
        if (OB_FAIL(get_hash_dist_info().push_back(sharding_info))) {
          LOG_WARN("failed to push back sharding info", K(ret));
        } else { /*do nothing*/ }
      }
    }
  } else {
    exch_info.dist_method_ = ObPQDistributeMethod::LOCAL;
  }
  return ret;
}

int ObLogPlan::init_groupby_helper(const ObIArray<ObRawExpr*> &group_exprs,
                                   const ObIArray<ObRawExpr*> &rollup_exprs,
                                   const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                   const bool is_from_povit,
                                   GroupingOpHelper &groupby_helper)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObLogicalOperator *best_plan = NULL;
  const ObDMLStmt* stmt = NULL;
  ObSEArray<ObRawExpr*, 4> group_rollup_exprs;
  bool push_group = false;
  groupby_helper.is_scalar_group_by_ = true;
  groupby_helper.is_from_povit_ = is_from_povit;
  groupby_helper.clear_ignore_hint();
  groupby_helper.optimizer_features_enable_version_ = get_log_plan_hint().optimizer_features_enable_version_;
  if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan) ||
             OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (stmt->is_select_stmt() &&
             OB_FALSE_IT(groupby_helper.is_scalar_group_by_ =
                         static_cast<const ObSelectStmt*>(stmt)->is_scala_group_by())) {
  } else if (OB_FAIL(append(group_rollup_exprs, group_exprs)) ||
             OB_FAIL(append(group_rollup_exprs, rollup_exprs))) {
    LOG_WARN("failed to append group rollup exprs", K(ret));
  } else if (OB_FAIL(get_log_plan_hint().get_aggregation_info(groupby_helper.force_use_hash_,
                                                              groupby_helper.force_use_merge_,
                                                              groupby_helper.force_part_sort_,
                                                              groupby_helper.force_normal_sort_,
                                                              groupby_helper.force_basic_,
                                                              groupby_helper.force_partition_wise_,
                                                              groupby_helper.force_dist_hash_))) {
    LOG_WARN("failed to get aggregation info from hint", K(ret));
  } else if (OB_FAIL(check_scalar_groupby_pushdown(aggr_items,
                                                   groupby_helper.can_scalar_pushdown_))) {
    LOG_WARN("failed to check scalar group by pushdown", K(ret));
  } else if (get_log_plan_hint().no_pushdown_group_by()) {
    OPT_TRACE("hint disable pushdown group by");
  } else if (OB_ISNULL(session_info = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(ret));
  } else if (OB_FAIL(session_info->if_aggr_pushdown_allowed(push_group))) {
    LOG_WARN("fail to get aggr_pushdown_allowed", K(ret));
  } else if (!push_group && !get_log_plan_hint().pushdown_group_by()) {
    OPT_TRACE("session info disable pushdown group by");
  } else if (OB_FAIL(check_rollup_pushdown(session_info,
                                           aggr_items,
                                           groupby_helper.can_rollup_pushdown_))) {
    LOG_WARN("failed to check rollup pushdown", K(ret));
  } else if (OB_FAIL(check_basic_groupby_pushdown(
                       aggr_items,
                       best_plan->get_output_equal_sets(),
                       groupby_helper.can_basic_pushdown_))) {
    LOG_WARN("failed to check whether aggr can be pushed", K(ret));
  } else if (groupby_helper.can_basic_pushdown_ || is_from_povit) {
    // do nothing
  } else if (OB_FAIL(check_three_stage_groupby_pushdown(
                       rollup_exprs,
                       aggr_items,
                       groupby_helper.non_distinct_aggr_items_,
                       groupby_helper.distinct_aggr_items_,
                       best_plan->get_output_equal_sets(),
                       groupby_helper.distinct_exprs_,
                       groupby_helper.can_three_stage_pushdown_))) {
    LOG_WARN("failed to check use three stage push down", K(ret));
  }

  if (OB_SUCC(ret)) {
    get_selectivity_ctx().init_op_ctx(best_plan);
    if (group_rollup_exprs.empty()) {
      groupby_helper.group_ndv_ = 1.0;
    } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(get_update_table_metas(),
                                                            get_selectivity_ctx(),
                                                            group_rollup_exprs,
                                                            best_plan->get_card(),
                                                            groupby_helper.group_ndv_))) {
      LOG_WARN("failed to calculate distinct", K(ret));
    } else { /* do nothing */ }
  }
  LOG_TRACE("succeed to check whether aggr can be pushed", K(groupby_helper));
  return ret;
}

int ObLogPlan::calculate_group_distinct_ndv(const ObIArray<ObRawExpr*> &groupby_rollup_exprs, GroupingOpHelper &groupby_helper)
{
  int ret = OB_SUCCESS;
  double total_ndv = 0;
  ObLogicalOperator *best_plan = NULL;
  if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    get_selectivity_ctx().init_op_ctx(best_plan);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < groupby_helper.distinct_aggr_batch_.count(); ++i) {
    ObSEArray<ObRawExpr*, 8> group_distinct_exprs;
    ObDistinctAggrBatch &distinct_aggr_batch = groupby_helper.distinct_aggr_batch_.at(i);
    double ndv = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < distinct_aggr_batch.mocked_params_.count(); j ++) {
      if (OB_FAIL(group_distinct_exprs.push_back(distinct_aggr_batch.mocked_params_.at(j).first))) {
        LOG_WARN("Failed to push back exprs", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append(group_distinct_exprs, groupby_rollup_exprs))) {
      LOG_WARN("failed to append group distinct exprs", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(get_update_table_metas(),
                                                            get_selectivity_ctx(),
                                                            group_distinct_exprs,
                                                            get_selectivity_ctx().get_current_rows(),
                                                            ndv))) {
      LOG_WARN("failed to calculate distinct", K(ret));
    } else {
      total_ndv += ndv;
    }
  }
  if (OB_SUCC(ret) && !groupby_helper.non_distinct_aggr_items_.empty()) {
    total_ndv += groupby_helper.group_ndv_;
  }
  groupby_helper.group_distinct_ndv_ = total_ndv;
  LOG_TRACE("succeed to calculate group distinct ndv for three stage", K(groupby_helper));
  return ret;
}

int ObLogPlan::init_distinct_helper(const ObIArray<ObRawExpr*> &distinct_exprs,
                                    GroupingOpHelper &distinct_helper)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *best_plan = NULL;
  ObSQLSessionInfo *session_info = NULL;
  bool push_distinct = false;
  distinct_helper.can_basic_pushdown_ = false;
  distinct_helper.clear_ignore_hint();
  distinct_helper.optimizer_features_enable_version_ = get_log_plan_hint().optimizer_features_enable_version_;
  if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_log_plan_hint().get_distinct_info(distinct_helper.force_use_hash_,
                                                           distinct_helper.force_use_merge_,
                                                           distinct_helper.force_basic_,
                                                           distinct_helper.force_partition_wise_,
                                                           distinct_helper.force_dist_hash_))) {
    LOG_WARN("failed to get distinct info from hint", K(ret));
  } else if (get_log_plan_hint().no_pushdown_distinct()) {
    OPT_TRACE("hint disable pushdown distinct");
  } else if (OB_ISNULL(session_info = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(ret));
  } else if (OB_FAIL(session_info->if_aggr_pushdown_allowed(push_distinct))) {
    LOG_WARN("fail to get aggr_pushdown_allowed", K(ret));
  } else if (!push_distinct && !get_log_plan_hint().pushdown_distinct()) {
    OPT_TRACE("session info disable pushdown distinct");
  } else {
    distinct_helper.can_basic_pushdown_ = true;
    OPT_TRACE("try pushdown distinct");
  }

  if (OB_SUCC(ret)) {
    get_selectivity_ctx().init_op_ctx(best_plan);
    if (distinct_exprs.empty()) {
      distinct_helper.group_ndv_ = 1.0;
    } else if (get_stmt()->is_set_stmt()) {
      // union distinct
      const ObSelectStmt *sel_stmt = static_cast<const ObSelectStmt *>(get_stmt());
      distinct_helper.group_ndv_ = 0.0;
      for (int64_t i = 0; i < sel_stmt->get_set_query().count(); i ++) {
        const OptTableMeta *table_meta = get_update_table_metas().get_table_meta_by_table_id(i);
        double child_ndv = 0;
        if (OB_NOT_NULL(table_meta)) {
          child_ndv = table_meta->get_distinct_rows();
        }
        distinct_helper.group_ndv_ += child_ndv;
      }
    } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(get_update_table_metas(),
                                                            get_selectivity_ctx(),
                                                            distinct_exprs,
                                                            best_plan->get_card(),
                                                            distinct_helper.group_ndv_))) {
      LOG_WARN("failed to calculate distinct", K(ret));
    } else { /* do nothing */ }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to init distinct helper", K(distinct_helper));
    OPT_TRACE("hint force use hash:", distinct_helper.force_use_hash_);
    OPT_TRACE("hint force use merge:", distinct_helper.force_use_merge_);
  }
  return ret;
}

int ObLogPlan::check_three_stage_groupby_pushdown(const ObIArray<ObRawExpr *> &rollup_exprs,
                                                  const ObIArray<ObAggFunRawExpr *> &aggr_items,
                                                  ObIArray<ObAggFunRawExpr *> &non_distinct_aggrs,
                                                  ObIArray<ObAggFunRawExpr *> &distinct_aggrs,
                                                  const EqualSets &equal_sets,
                                                  ObIArray<ObRawExpr *> &distinct_exprs,
                                                  bool &can_push)
{
  int ret = OB_SUCCESS;
  bool is_rollup = !rollup_exprs.empty();
  can_push = true;
  bool has_one_distinct = true;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && can_push && i < aggr_items.count(); ++i) {
    ObAggFunRawExpr *aggr_expr = aggr_items.at(i);
    if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr expr is null", K(ret), K(aggr_expr));
    } else if (aggr_expr->get_expr_type() != T_FUN_MIN &&
               aggr_expr->get_expr_type() != T_FUN_MAX &&
               aggr_expr->get_expr_type() != T_FUN_SUM &&
               aggr_expr->get_expr_type() != T_FUN_COUNT &&
               aggr_expr->get_expr_type() != T_FUN_GROUPING &&
               aggr_expr->get_expr_type() != T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS &&
               aggr_expr->get_expr_type() != T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE &&
               aggr_expr->get_expr_type() != T_FUN_SYS_BIT_AND &&
               aggr_expr->get_expr_type() != T_FUN_SYS_BIT_OR &&
               aggr_expr->get_expr_type() != T_FUN_SYS_BIT_XOR) {
      can_push = false;
    } else if (is_rollup && aggr_expr->get_expr_type() == T_FUN_GROUPING) {
      can_push = false;
    } else if (aggr_expr->is_param_distinct()) {
      if (OB_FAIL(distinct_aggrs.push_back(aggr_expr))) {
        LOG_WARN("failed to push back distinct aggr", K(ret));
      } else if (!has_one_distinct) {
        /* do nothing */
      } else if (distinct_exprs.empty()) {
        if (OB_FAIL(append(distinct_exprs, aggr_expr->get_real_param_exprs()))) {
          LOG_WARN("failed to append expr", K(ret));
        }
      } else {
        has_one_distinct = ObOptimizerUtil::same_exprs(distinct_exprs,
                                                       aggr_expr->get_real_param_exprs(),
                                                       equal_sets);
      }
    } else if (OB_FAIL(non_distinct_aggrs.push_back(aggr_expr))) {
      LOG_WARN("failed to push back non distinct aggr", K(ret));
    }
  }
  if (OB_SUCC(ret) && can_push) {
    // if aggregate function has distinct arguments, then use 3 stage aggregate algorithm
    can_push = 0 < distinct_aggrs.count();
    if (can_push) {
      // only for test
      ret = OB_E(EventTable::EN_ENABLE_THREE_STAGE_AGGREGATE) ret;
      if (OB_FAIL(ret)) {
        // by default disable three stage aggregate
        int64_t xx = -ret;
        if (xx % 2 == 0) {
          can_push = false;
        } else {
          can_push = true;
        }
      }
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) && can_push && is_rollup) {
    int64_t partial_rollup_pushdown = 0;
    if (OB_FAIL(session->get_distinct_agg_partial_rollup_pushdown(
                  partial_rollup_pushdown))) {
      LOG_WARN("get force parallel ddl dop failed", K(ret));
    } else {
      can_push = (0 < partial_rollup_pushdown) && !aggr_items.empty();
    }
  }
  if (OB_SUCC(ret) && (!has_one_distinct || !can_push)) {
    distinct_exprs.reuse();
  }
  return ret;
}

int ObLogPlan::check_basic_groupby_pushdown(const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                            const EqualSets &equal_sets,
                                            bool &can_push)
{
  int ret = OB_SUCCESS;
  can_push = true;
  // check whether contain agg expr can not be pushed down
  for (int64_t i = 0; OB_SUCC(ret) && can_push && i < aggr_items.count(); ++i) {
    ObAggFunRawExpr *aggr_expr = aggr_items.at(i);
    if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_FUN_MAX != aggr_expr->get_expr_type() &&
               T_FUN_MIN != aggr_expr->get_expr_type() &&
               T_FUN_SUM != aggr_expr->get_expr_type() &&
               T_FUN_COUNT != aggr_expr->get_expr_type() &&
               T_FUN_COUNT_SUM != aggr_expr->get_expr_type() &&
               T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS != aggr_expr->get_expr_type() &&
               T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE != aggr_expr->get_expr_type() &&
               !(T_FUN_GROUPING == aggr_expr->get_expr_type() &&
                 aggr_expr->get_real_param_count() == 1) &&
               T_FUN_TOP_FRE_HIST != aggr_expr->get_expr_type() &&
               T_FUN_SYS_BIT_AND != aggr_expr->get_expr_type() &&
               T_FUN_SYS_BIT_OR != aggr_expr->get_expr_type() &&
               T_FUN_SYS_BIT_XOR != aggr_expr->get_expr_type()) {
      can_push = false;
    } else if (aggr_expr->is_param_distinct()) {
      can_push = false;
    }
  }

  return ret;
}

int ObLogPlan::check_rollup_pushdown(const ObSQLSessionInfo *info,
                                     const ObIArray<ObAggFunRawExpr *> &aggr_items,
                                     bool &can_push)
{
  int ret = OB_SUCCESS;
  int64_t enable_rollup_pushdown = 0;
  can_push = false;
  if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret), K(info));
  } else if (OB_FAIL(info->get_partial_rollup_pushdown(enable_rollup_pushdown))) {
    LOG_WARN("failed to get partial rollup pushdown", K(ret));
  } else {
    can_push = (enable_rollup_pushdown > 0) && !aggr_items.empty();
  }
  for (int64_t i = 0; OB_SUCC(ret) && can_push && i < aggr_items.count(); ++i) {
    if (aggr_items.at(i)->get_expr_type() != T_FUN_MIN &&
        aggr_items.at(i)->get_expr_type() != T_FUN_MAX &&
        aggr_items.at(i)->get_expr_type() != T_FUN_SUM &&
        aggr_items.at(i)->get_expr_type() != T_FUN_COUNT &&
        aggr_items.at(i)->get_expr_type() != T_FUN_SYS_BIT_AND &&
        aggr_items.at(i)->get_expr_type() != T_FUN_SYS_BIT_OR &&
        aggr_items.at(i)->get_expr_type() != T_FUN_SYS_BIT_XOR) {
      can_push = false;
    } else if (aggr_items.at(i)->is_param_distinct()) {
      can_push = false;
    }
  }
  return ret;
}

bool ObLogPlan::is_tenant_enable_aggr_push_down(ObSQLSessionInfo &session_info)
{
  bool enabled = false;
  uint64_t tenant_id = session_info.get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    enabled = ObPushdownFilterUtils::is_aggregate_pushdown_enabled(tenant_config->_pushdown_storage_level);
  }
  return enabled;
}

int ObLogPlan::check_scalar_groupby_pushdown(const ObIArray<ObAggFunRawExpr *> &aggrs,
                                             bool &can_push)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObAggFunRawExpr *cur_aggr = NULL;
  ObRawExpr *first_param = NULL;
  bool has_virtual_col = false;
  can_push = false;
  if (OB_ISNULL(stmt = get_stmt()) ||
      OB_ISNULL(session_info = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!is_tenant_enable_aggr_push_down(*session_info) ||
             !stmt->is_select_stmt()) {
   OPT_TRACE("tenant disable aggregation push down");
  } else if (!static_cast<const ObSelectStmt*>(stmt)->is_scala_group_by() ||
             stmt->has_for_update() ||
             !stmt->is_single_table_stmt()) {
    /*do nothing*/
  } else if (OB_ISNULL(table_item = stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_item));
  } else if (!table_item->is_basic_table() ||
             table_item->is_link_table() ||
             is_sys_table(table_item->ref_id_) ||
             is_virtual_table(table_item->ref_id_) ||
             EXTERNAL_TABLE == table_item->table_type_) {
    /*do nothing*/
  } else if (OB_FAIL(stmt->has_virtual_generated_column(table_item->table_id_, has_virtual_col))) {
    LOG_WARN("failed to check has virtual generated column", K(ret), K(*table_item));
  } else if (has_virtual_col) {
    /* do not push down when exists virtual generated column */
  } else {
    const ObIArray<ObRawExpr *> &filters = stmt->get_condition_exprs();
    can_push = true;
    /*do not push down when filters contain pl udf*/
    for (int64_t i = 0; OB_SUCC(ret) && can_push && i < filters.count(); i++) {
      if (OB_ISNULL(filters.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (filters.at(i)->has_flag(ObExprInfoFlag::CNT_PL_UDF)) {
        can_push = false;
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && can_push && i < aggrs.count(); ++i) {
    if (OB_ISNULL(cur_aggr = aggrs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_FUN_COUNT != cur_aggr->get_expr_type()
               && T_FUN_MIN != cur_aggr->get_expr_type()
               && T_FUN_MAX != cur_aggr->get_expr_type()) {
      can_push = false;
    } else if (cur_aggr->is_param_distinct() || 1 < cur_aggr->get_real_param_count()) {
      /* mysql mode, support count(distinct c1, c2). if this distinct can be eliminated,
           the count(c1, c2) can not push down*/
      can_push = false;
    } else if (cur_aggr->get_real_param_exprs().empty()) {
      /* do nothing */
    } else if (OB_ISNULL(first_param = cur_aggr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!first_param->is_column_ref_expr() ||
               table_item->table_id_ != static_cast<ObColumnRefRawExpr*>(first_param)->get_table_id()) {
      can_push = false;
    }
  }
  return ret;
}

int ObLogPlan::check_can_pullup_gi(ObLogicalOperator &top,
                                   bool is_partition_wise,
                                   bool need_sort,
                                   bool &can_pullup)
{
  int ret = OB_SUCCESS;
  can_pullup = false;
  bool has_win_func = false;
  if (is_partition_wise) {
    can_pullup = true;
  } else if (need_sort || !top.get_is_local_order() || top.is_exchange_allocated()) {
    /* do nothing */
  } else if (OB_FAIL(top.check_has_op_below(LOG_WINDOW_FUNCTION, has_win_func))) {
    LOG_WARN("failed to check has window function below", K(ret));
  } else {
    can_pullup = !has_win_func;
  }
  return ret;
}

/**
 *  @brief  adjust_sort_expr_ordering
 *  调整需要排序的expr的顺序。像group by a, b 既可以按a, b排序，也可以按照b, a排序。
 *  先看能不能利用下层算子的序，如果不能再根据窗口函数或stmt order by调整顺序。
 */
int ObLogPlan::adjust_sort_expr_ordering(ObIArray<ObRawExpr*> &sort_exprs,
                                         ObIArray<ObOrderDirection> &sort_directions,
                                         ObLogicalOperator &child_op,
                                         bool check_win_func)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const EqualSets &equal_sets = child_op.get_output_equal_sets();
  const ObIArray<ObRawExpr *> &const_exprs = child_op.get_output_const_exprs();
  int64_t prefix_count = -1;
  bool input_ordering_all_used = false;
  if (OB_ISNULL(stmt = child_op.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(stmt));
  } else if (!child_op.get_op_ordering().empty() &&
             OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(sort_exprs,
                                                               child_op.get_op_ordering(),
                                                               equal_sets,
                                                               const_exprs,
                                                               onetime_query_refs_,
                                                               prefix_count,
                                                               input_ordering_all_used,
                                                               sort_directions))) {
    LOG_WARN("failed to adjust exprs by ordering", K(ret));
  } else if (input_ordering_all_used) {
    /* sort_exprs use input ordering, need not sort */
  } else {
    bool adjusted = false;
    if (stmt->is_select_stmt() && check_win_func) {
      const ObSelectStmt *sel_stmt = static_cast<const ObSelectStmt *>(stmt);
      for (int64_t i = 0; OB_SUCC(ret) && !adjusted && i < sel_stmt->get_window_func_count(); ++i) {
        const ObWinFunRawExpr *cur_expr = sel_stmt->get_window_func_expr(i);
        if (OB_ISNULL(cur_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null window function expr", K(ret));
        } else if (cur_expr->get_partition_exprs().count() == 0 &&
                   cur_expr->get_order_items().count() == 0) {
          // win_func over(), do nothing
        } else if (prefix_count > 0) {
          /* used part of input ordering, do not adjust now*/
          adjusted = true;
        } else if (OB_FAIL(adjust_exprs_by_win_func(sort_exprs,
                                                    *cur_expr,
                                                    equal_sets,
                                                    const_exprs,
                                                    sort_directions))) {
            LOG_WARN("failed to adjust exprs by win func", K(ret));
        } else {
          /* use no input ordering, adjusted by win func*/
          adjusted = true;
        }
      }
    }
    if (OB_SUCC(ret) && !adjusted && stmt->get_order_item_size() > 0) {
      if (prefix_count > 0) {
        /* used part of input ordering, try adjust sort_exprs after prefix_count by order item */
        if (OB_FAIL(adjust_postfix_sort_expr_ordering(stmt->get_order_items(),
                                                      child_op.get_fd_item_set(),
                                                      equal_sets,
                                                      const_exprs,
                                                      prefix_count,
                                                      sort_exprs,
                                                      sort_directions))) {
          LOG_WARN("failed to adjust exprs by ordering", K(ret));
        }
      } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(sort_exprs,
                                                                   stmt->get_order_items(),
                                                                   equal_sets,
                                                                   const_exprs,
                                                                   onetime_query_refs_,
                                                                   prefix_count,
                                                                   input_ordering_all_used,
                                                                   sort_directions))) {
        LOG_WARN("failed to adjust exprs by ordering", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::adjust_postfix_sort_expr_ordering(const ObIArray<OrderItem> &ordering,
                                                  const ObFdItemSet &fd_item_set,
                                                  const EqualSets &equal_sets,
                                                  const ObIArray<ObRawExpr*> &const_exprs,
                                                  const int64_t prefix_count,
                                                  ObIArray<ObRawExpr*> &sort_exprs,
                                                  ObIArray<ObOrderDirection> &sort_directions)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("begin adjust postfix sort expr ordering", K(prefix_count), K(fd_item_set), K(equal_sets),
                                              K(sort_exprs), K(sort_directions), K(ordering));
  if (OB_UNLIKELY(prefix_count < 0 || prefix_count >= sort_exprs.count())
      || OB_UNLIKELY(sort_directions.count() != sort_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(prefix_count), K(sort_exprs.count()),
                                          K(sort_directions.count()));
  } else if (ordering.count() < prefix_count) {
    /* do nothing */
  } else {
    ObSEArray<ObRawExpr*, 5> new_sort_exprs;
    ObSEArray<ObOrderDirection, 5> new_sort_directions;
    bool check_next = false;
    bool can_adjust = true;
    int64_t idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && can_adjust && i < prefix_count; ++i) {
      check_next = true;
      while (OB_SUCC(ret) && check_next && idx < ordering.count()) {
        // after ObOptimizerUtil::adjust_exprs_by_ordering, there is not const exprs in sort_exprs.
        if (sort_directions.at(i) == ordering.at(idx).order_type_
            && ObOptimizerUtil::is_expr_equivalent(sort_exprs.at(i), ordering.at(idx).expr_, equal_sets)) {
          check_next = false;
        } else if (OB_FAIL(ObOptimizerUtil::is_const_or_equivalent_expr(ordering, equal_sets,
                                                                  const_exprs, onetime_query_refs_,
                                                                  idx, check_next))) {
          LOG_WARN("failed to check is const or equivalent exprs", K(ret));
        } else if (!check_next &&
                   OB_FAIL(ObOptimizerUtil::is_expr_is_determined(new_sort_exprs, fd_item_set,
                                                                  equal_sets, const_exprs,
                                                                  ordering.at(idx).expr_,
                                                                  check_next))) {
          LOG_WARN("failed to check is expr is determined", K(ret));
        } else if (check_next) {
          ++idx;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (check_next) {
        can_adjust = false;
      } else if (OB_FAIL(new_sort_exprs.push_back(sort_exprs.at(i)))
                 || OB_FAIL(new_sort_directions.push_back(sort_directions.at(i)))) {
        LOG_WARN("failed to add prefix expr/direction", K(ret));
      } else {
        ++idx;
      }
    }
    if (OB_SUCC(ret) && idx < ordering.count() && can_adjust) {
      ObSqlBitSet<> added_sort_exprs;
      for (int64_t i = idx; OB_SUCC(ret) && can_adjust && i < ordering.count(); ++i) {
        can_adjust = false;
        for (int64_t j = prefix_count; OB_SUCC(ret) && !can_adjust && j <  sort_exprs.count(); ++j) {
          if (ObOptimizerUtil::is_expr_equivalent(sort_exprs.at(j), ordering.at(i).expr_, equal_sets)) {
            can_adjust = true;
            if (added_sort_exprs.has_member(j)) {
              /* do nothing */
            } else if (OB_FAIL(added_sort_exprs.add_member(j))) {
              LOG_WARN("failed to add bit set", K(ret));
            } else if (OB_FAIL(new_sort_exprs.push_back(sort_exprs.at(j)))
                       || OB_FAIL(new_sort_directions.push_back(ordering.at(i).order_type_))) {
              LOG_WARN("Failed to add prefix expr/direction", K(ret));
            }
          }
        }
        if (OB_FAIL(ret) || can_adjust) {
        } else if (OB_FAIL(ObOptimizerUtil::is_const_or_equivalent_expr(ordering, equal_sets,
                                                                        const_exprs,
                                                                        onetime_query_refs_,
                                                                        i, can_adjust))) {
          LOG_WARN("failed to check is const or equivalent exprs", K(ret));
        } else if (!can_adjust && OB_FAIL(ObOptimizerUtil::is_expr_is_determined(new_sort_exprs,
                                                                                fd_item_set,
                                                                                equal_sets,
                                                                                const_exprs,
                                                                                ordering.at(i).expr_,
                                                                                can_adjust))) {
          LOG_WARN("failed to check is expr is determined", K(ret));
        }
      }
      if (OB_SUCC(ret) && can_adjust) {
        for (int64_t i = prefix_count; OB_SUCC(ret) && i < sort_exprs.count(); ++i) {
          if (added_sort_exprs.has_member(i)) {
            /* do nothing */
          } else if (OB_FAIL(new_sort_exprs.push_back(sort_exprs.at(i)))
                     || OB_FAIL(new_sort_directions.push_back(sort_directions.at(i)))) {
            LOG_WARN("failed to add prefix expr / direction", K(ret));
          }
        }
        LOG_DEBUG("adjusted postfix sort expr ordering", K(new_sort_exprs), K(new_sort_directions));
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sort_exprs.assign(new_sort_exprs))) {
            LOG_WARN("assign adjusted exprs failed", K(ret));
          } else if (OB_FAIL(sort_directions.assign(new_sort_directions))) {
            LOG_WARN("failed to assign order types", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

/**
 * @brief  adjust_exprs_by_win_func
 * 根据 window function 调整 exprs 的顺序。先匹配 window function 的
 * partition by exprs, 如果 partition by exprs 能够完全匹配, 再匹配
 * window function 的 order by exprs。
 * 其中 partition by exprs 不要求严格的前缀匹配, order by exprs 要求严
 * 格的前缀匹配, 因为 partition by exprs 也是可以调整顺序的。
 */
int ObLogPlan::adjust_exprs_by_win_func(ObIArray<ObRawExpr *> &exprs,
                                        const ObWinFunRawExpr &win_expr,
                                        const EqualSets &equal_sets,
                                        const ObIArray<ObRawExpr*> &const_exprs,
                                        ObIArray<ObOrderDirection> &directions)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> adjusted_exprs;
  ObSEArray<ObOrderDirection, 8> order_types;
  ObSEArray<ObRawExpr *, 8> rest_exprs;
  ObSEArray<ObOrderDirection, 8> rest_order_types;
  ObBitSet<64> expr_idxs;
  bool all_part_used = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < win_expr.get_partition_exprs().count(); ++i) {
    bool find = false;
    const ObRawExpr *cur_expr = win_expr.get_partition_exprs().at(i);
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < exprs.count(); ++j) {
      if (expr_idxs.has_member(j)) {
        // already add into adjusted_exprs
      } else if (ObOptimizerUtil::is_expr_equivalent(cur_expr, exprs.at(j), equal_sets)) {
        find = true;
        if (OB_FAIL(adjusted_exprs.push_back(exprs.at(j)))) {
          LOG_WARN("store ordered expr failed", K(ret), K(i), K(j));
        } else if (OB_FAIL(order_types.push_back(directions.at(j)))) {
          LOG_WARN("failed to push back order type");
        } else if (OB_FAIL(expr_idxs.add_member(j))) {
          LOG_WARN("add expr idxs member failed", K(ret), K(j));
        }
      }
    }
    if (!find) {
      all_part_used = false;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (expr_idxs.has_member(i)) {
      // already add into adjusted_exprs
    } else if (OB_FAIL(rest_exprs.push_back(exprs.at(i)))) {
      LOG_WARN("store ordered expr failed", K(ret), K(i));
    } else if (OB_FAIL(rest_order_types.push_back(directions.at(i)))) {
      LOG_WARN("failed to push back order type", K(ret));
    }
  }
  if (OB_SUCC(ret) && all_part_used &&
      win_expr.get_order_items().count() > 0 &&
      rest_exprs.count() > 0) {
    int64_t prefix_count = -1;
    bool input_ordering_all_used = false;
    if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(rest_exprs,
                                                          win_expr.get_order_items(),
                                                          equal_sets,
                                                          const_exprs,
                                                          onetime_query_refs_,
                                                          prefix_count,
                                                          input_ordering_all_used,
                                                          rest_order_types))) {
      LOG_WARN("failed to adjust exprs by ordering", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(adjusted_exprs, rest_exprs))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(append(order_types, rest_order_types))) {
      LOG_WARN("failed to append order direction", K(ret));
    } else if (adjusted_exprs.count() != exprs.count() ||
               order_types.count() != exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exprs don't covered completely",
               K(adjusted_exprs.count()), K(exprs.count()), K(order_types.count()));
    } else {
      exprs.reuse();
      if (OB_FAIL(exprs.assign(adjusted_exprs))) {
        LOG_WARN("assign adjusted exprs failed", K(ret));
      } else if (OB_FAIL(directions.assign(order_types))) {
        LOG_WARN("failed to assign order types", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_plan_tree()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_stmt()));
  } else {
    // 1.1 generate access paths
    /* random exprs should be split from condition exprs to avoid being pushed down
     * random exprs will be added back in function candi_init*/
    if (OB_FAIL(generate_join_orders())) {
      LOG_WARN("failed to generate the access path for the single-table query",
               K(ret), K(get_optimizer_context().get_query_ctx()->get_sql_stmt()));
    } else if (OB_FAIL(init_candidate_plans())) {
      LOG_WARN("failed to initialized the plan candidates from the join order", K(ret));
    } else {
      LOG_TRACE("plan candidates is initialized from the join order",
                  "# of candidates", candidates_.candidate_plans_.count());
    }
  }
  return ret;
}

int ObLogPlan::get_minimal_cost_candidates(const ObIArray<CandidatePlan> &candidates,
                                           ObIArray<CandidatePlan> &best_candidates)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSEArray<CandidatePlan, 16>, 8> candidate_list;
  if (OB_FAIL(classify_candidates_based_on_sharding(candidates,
                                                    candidate_list))) {
    LOG_WARN("failed to classify candidates based on sharding", K(ret));
  } else if (OB_FAIL(get_minimal_cost_candidates(candidate_list,
                                                 best_candidates))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::check_join_legal(const ObRelIds &left_set,
                                const ObRelIds &right_set,
                                const ObRelIds &combined_set,
                                ConflictDetector *detector,
                                bool delay_cross_product,
                                bool &legal)
{
  int ret = OB_SUCCESS;
  legal = true;
  if (OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null detector", K(ret));
  } else if (INNER_JOIN == detector->join_info_.join_type_) {
    if (!detector->join_info_.table_set_.is_subset(combined_set)) {
      //对于inner join来说只需要检查SES是否包含于left_set u right_set
      legal = false;
    }
  } else {
    if (detector->L_TES_.is_subset(left_set) &&
        detector->R_TES_.is_subset(right_set)) {
      //do nothing
    } else if (!detector->is_commutative_) {
      legal = false;
    } else if (detector->R_TES_.is_subset(left_set) &&
               detector->L_TES_.is_subset(right_set)) {
      //do nothing
    } else {
      legal = false;
    }
    if (legal && !detector->is_commutative_) {
      if (left_set.overlap(detector->R_DS_) ||
          right_set.overlap(detector->L_DS_)) {
        legal = false;
      }
    }
  }
  //如果连接谓词是退化谓词，例如t1 left join t2 on 1=t2.c1，需要额外的检查
  if (OB_FAIL(ret) || !legal) {
    //do nothing
  } else if (detector->is_degenerate_pred_) {
    //check T(left(o)) n S1 != empty ^ T(right(o)) n S2 != empty
    if (detector->L_DS_.overlap(left_set) &&
        detector->R_DS_.overlap(right_set)) {
      //do nothing
    } else if (!detector->is_commutative_) {
      legal = false;
    } else if (detector->R_DS_.overlap(left_set) &&
               detector->L_DS_.overlap(right_set)) {
      //do nothing
    } else {
      legal = false;
    }
  }
  //冲突规则检查
  if (OB_FAIL(ret) || !legal) {
    //do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && legal && i < detector->CR_.count(); ++i) {
      const ObRelIds& T1 = detector->CR_.at(i).first;
      const ObRelIds& T2 = detector->CR_.at(i).second;
      if (T1.overlap(combined_set) && !T2.is_subset(combined_set)) {
        legal = false;
      }
    }
    //检查笛卡尔积的约束
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
    //如果需要延迟笛卡尔积，需要检查是否满足延迟条件
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
    //检查dependent function table的约束
    for (int64_t i = 0; OB_SUCC(ret) && legal && i < table_depend_infos_.count(); ++i) {
      TableDependInfo &info = table_depend_infos_.at(i);
      if (left_set.has_member(info.table_idx_)) {
        legal = info.depend_table_set_.is_subset(left_set);
      } else if (right_set.has_member(info.table_idx_)) {
        legal = info.depend_table_set_.is_subset(left_set) || info.depend_table_set_.is_subset(right_set);
      }
    }
  }
  return ret;
}

int ObLogPlan::get_minimal_cost_candidates(
    const ObIArray<ObSEArray<CandidatePlan, 16>> &candidate_list,
    ObIArray<CandidatePlan> &best_candidates)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < candidate_list.count(); i++) {
    CandidatePlan best_candidate;
    if (OB_FAIL(get_minimal_cost_candidate(candidate_list.at(i),
                                           best_candidate))) {
      LOG_WARN("failed to get minimal cost candidate", K(ret));
    } else if (OB_FAIL(best_candidates.push_back(best_candidate))) {
      LOG_WARN("failed to push back candidate", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::get_minimal_cost_candidate(const ObIArray<CandidatePlan> &candidates,
                                          CandidatePlan &candidate)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_optimizer_context().generate_random_plan())) {
    ObQueryCtx* query_ctx;
    if (OB_ISNULL(query_ctx = get_optimizer_context().get_query_ctx())) {
      LOG_WARN("unexpected null value", K(query_ctx));
      candidate = candidates.at(0);
    } else {
      candidate = candidates.at(query_ctx->rand_gen_.get(0, candidates.count() - 1));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); i++) {
      if (OB_ISNULL(candidates.at(i).plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (NULL == candidate.plan_tree_ ||
                candidates.at(i).plan_tree_->get_cost() < candidate.plan_tree_->get_cost()) {
        candidate = candidates.at(i);
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::classify_candidates_based_on_sharding(
    const ObIArray<CandidatePlan> &candidates,
    ObIArray<ObSEArray<CandidatePlan, 16>> &candidate_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); i++) {
    if (OB_ISNULL(candidates.at(i).plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      bool is_find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < candidate_list.count(); j++) {
        bool is_equal = false;
        ObIArray<CandidatePlan> &temp_candidate = candidate_list.at(j);
        if (OB_UNLIKELY(temp_candidate.empty()) ||
            OB_ISNULL(temp_candidate.at(0).plan_tree_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret));
        } else if (candidates.at(i).plan_tree_->get_parallel() != temp_candidate.at(0).plan_tree_->get_parallel()) {
          /*do nothing*/
        } else if (candidates.at(i).plan_tree_->is_exchange_allocated() != temp_candidate.at(0).plan_tree_->is_exchange_allocated()) {
          /*do nothing*/
        } else if (candidates.at(i).plan_tree_->get_contains_pw_merge_op() != temp_candidate.at(0).plan_tree_->get_contains_pw_merge_op()) {
          /*do nothing*/
        } else if (OB_FAIL(ObShardingInfo::is_sharding_equal(
                            candidates.at(i).plan_tree_->get_strong_sharding(),
                            candidates.at(i).plan_tree_->get_weak_sharding(),
                            candidate_list.at(j).at(0).plan_tree_->get_strong_sharding(),
                            candidate_list.at(j).at(0).plan_tree_->get_weak_sharding(),
                            candidates.at(i).plan_tree_->get_output_equal_sets(),
                            is_equal))) {
          LOG_WARN("failed to check whether sharding is equal", K(ret));
        } else if (!is_equal) {
          /*do nothing*/
        } else if (OB_FAIL(temp_candidate.push_back(candidates.at(i).plan_tree_))) {
          LOG_WARN("failed to push back candidate plan", K(ret));
        } else {
          is_find = true;
        }
      }
      if (OB_SUCC(ret) && !is_find) {
        ObSEArray<CandidatePlan, 16> temp_candidate;
        if (OB_FAIL(temp_candidate.push_back(candidates.at(i)))) {
          LOG_WARN("failed to push back candidate plan", K(ret));
        } else if (OB_FAIL(candidate_list.push_back(temp_candidate))) {
          LOG_WARN("failed to push back candidate plan", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_order_by(bool &need_limit,
                                       ObIArray<OrderItem> &order_items)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *best_plan = NULL;
  ObSEArray<ObRawExpr*, 4> order_by_exprs;
  ObSEArray<ObOrderDirection, 4> directions;
  ObSEArray<CandidatePlan, 8> limit_plans;
  ObSEArray<CandidatePlan, 8> order_by_plans;
  ObSEArray<OrderItem, 8> candi_order_items;
  ObSEArray<ObRawExpr*, 4> candi_subquery_exprs;
  ObRawExpr *topn_expr = NULL;
  bool is_fetch_with_ties = false;
  need_limit = false;
  OPT_TRACE_TITLE("start generate order by");
  if (OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (FALSE_IT(need_limit = get_stmt()->has_limit())) {
    /*do nothing*/
  } else if (OB_FAIL(candi_allocate_subplan_filter_for_order_by())) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_order_by_exprs(best_plan, order_by_exprs, &directions))) {
    LOG_WARN("failed to get order by columns", K(ret));
  } else if (order_by_exprs.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(make_order_items(order_by_exprs, directions, candi_order_items))) {
    LOG_WARN("Failed to make order items", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(best_plan->get_fd_item_set(),
                                                             best_plan->get_output_equal_sets(),
                                                             best_plan->get_output_const_exprs(),
                                                             onetime_query_refs_,
                                                             candi_order_items,
                                                             order_items))) {
    LOG_WARN("failed to simplify exprs", K(ret));
  } else if (order_items.empty()) {
    OPT_TRACE("this plan has interesting order, no need allocate order by");
  } else if (OB_FAIL(get_order_by_topn_expr(best_plan->get_card(),
                                            topn_expr,
                                            is_fetch_with_ties,
                                            need_limit))) {
    LOG_WARN("failed to get order by top-n expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
      bool is_reliable = false;
      CandidatePlan candidate_plan = candidates_.candidate_plans_.at(i);
      OPT_TRACE("generate order by for plan:", candidate_plan);
      if (OB_FAIL(create_order_by_plan(candidate_plan.plan_tree_,
                                       order_items,
                                       topn_expr,
                                       is_fetch_with_ties))) {
        LOG_WARN("failed to create order by plan", K(ret));
      } else if (NULL != topn_expr && OB_FAIL(is_plan_reliable(candidate_plan.plan_tree_,
                                                               is_reliable))) {
        LOG_WARN("failed to check if plan is reliable", K(ret));
      } else if (is_reliable) {
        ret = limit_plans.push_back(candidate_plan);
      } else {
        ret = order_by_plans.push_back(candidate_plan);
      }
    }
    // keep minimal cost plan or interesting plan
    if (OB_SUCC(ret)) {
      int64_t check_scope = OrderingCheckScope::CHECK_SET;
      if (limit_plans.empty() && OB_FAIL(limit_plans.assign(order_by_plans))) {
        LOG_WARN("failed to assign candidate plans", K(ret));
      } else if (OB_FAIL(update_plans_interesting_order_info(limit_plans, check_scope))) {
        LOG_WARN("failed to update plans interesting order info", K(ret));
      } else if (OB_FAIL(prune_and_keep_best_plans(limit_plans))) {
        LOG_WARN("failed to prune and keep best plans", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

/* allocate subplan filter for subquerys in order by
 * 1. Query has no limit, if select_item also has subquery, then allocate subquery together.
 * 2. Query has limit, don't allocate subqery in select_item.
*/
int ObLogPlan::candi_allocate_subplan_filter_for_order_by()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> candi_exprs;
  ObSEArray<ObQueryRefRawExpr *, 4> candi_subquery_exprs;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(stmt->get_order_exprs(candi_exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else if (!get_stmt()->has_limit() && stmt->is_select_stmt()) {
    for (int64_t i = 0; OB_SUCC(ret) && candi_subquery_exprs.empty() && i < candi_exprs.count(); ++i) {
      if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(candi_exprs.at(i),
                                                           candi_subquery_exprs,
                                                           false))) {
        LOG_WARN("failed to extract query ref exprs", K(ret));
      }
    }
    if (OB_SUCC(ret) && !candi_subquery_exprs.empty() &&
        OB_FAIL(static_cast<const ObSelectStmt *>(stmt)->get_select_exprs(candi_exprs))) {
      LOG_WARN("failed to get select exprts", K(ret));
    }
  }
  if (OB_SUCC(ret) &&
      OB_FAIL(candi_allocate_subplan_filter(candi_exprs))) {
    LOG_WARN("failed to allocate subplan filter for exprs", K(ret));
  } else { /* do nothing */ }
  return ret;
}

int ObLogPlan::create_order_by_plan(ObLogicalOperator *&top,
                                    const ObIArray<OrderItem> &order_items,
                                    ObRawExpr *topn_expr,
                                    bool is_fetch_with_ties)
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  int64_t prefix_pos = 0;
  ObExchangeInfo exch_info;
  exch_info.dist_method_ = (NULL != top && top->is_single()) ?
                           ObPQDistributeMethod::NONE : ObPQDistributeMethod::LOCAL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(order_items,
                                                      top->get_op_ordering(),
                                                      top->get_fd_item_set(),
                                                      top->get_output_equal_sets(),
                                                      top->get_output_const_exprs(),
                                                      onetime_query_refs_,
                                                      top->get_is_at_most_one_row(),
                                                      need_sort,
                                                      prefix_pos))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (OB_FAIL(allocate_sort_and_exchange_as_top(top,
                                                       exch_info,
                                                       order_items,
                                                       need_sort,
                                                       prefix_pos,
                                                       top->get_is_local_order(),
                                                       topn_expr,
                                                       is_fetch_with_ties))) {
    LOG_WARN("failed to allocate sort as top", K(ret));
  } else if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    top->set_is_order_by_plan_top(true);
  }
  return ret;
}

int ObLogPlan::get_order_by_exprs(const ObLogicalOperator *top,
                                  ObIArray<ObRawExpr *> &order_by_exprs,
                                  ObIArray<ObOrderDirection> *directions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_stmt()), K(top));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_stmt()->get_order_item_size(); i++) {
      const OrderItem &order_item = get_stmt()->get_order_item(i);
      bool is_const = false;
      bool has_null_reject = false;
      if (OB_ISNULL(order_item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(order_item.expr_,
                                                        top->get_output_equal_sets(),
                                                        top->get_output_const_exprs(),
                                                        onetime_query_refs_,
                                                        is_const))) {
        LOG_WARN("check is const expr failed", K(ret));
      } else if (is_const) {
        /**
         * orderby后面的const都已经被替换成了SelectItem里的expr，所以一般这里是不会出现const的。
         * 不过如果SelectItem里的expr本身是个const，那么这里会出现const，如：SELECT 1 FROM t1 ORDER BY 1;
         * 遇到const，跳过即可。
         */
      } else if (OB_FAIL(order_by_exprs.push_back(order_item.expr_))) {
        LOG_WARN("failed to add order by expr", K(ret));
      } else if (NULL != directions) {
        if (OB_FAIL(ObTransformUtils::has_null_reject_condition(get_stmt()->get_condition_exprs(),
                                                                order_item.expr_,
                                                                has_null_reject))) {
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

int ObLogPlan::make_order_items(const common::ObIArray<ObRawExpr*> &exprs,
                                const ObIArray<ObOrderDirection> *dirs,
                                ObIArray<OrderItem> &items)
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

int ObLogPlan::make_order_items(const common::ObIArray<ObRawExpr *> &exprs,
                                common::ObIArray<OrderItem> &items)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_stmt()));
  } else {
    ObOrderDirection direction = default_asc_direction();
    if (get_stmt()->get_order_item_size() > 0) {
      direction = get_stmt()->get_order_item(0).order_type_;
    } else { /* Do nothing */ }
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

int ObLogPlan::make_order_items(const ObIArray<ObRawExpr *> &exprs,
                                const ObIArray<ObOrderDirection> &dirs,
                                ObIArray<OrderItem> &items)
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
      } else if (exprs.at(i)->is_const_expr()) {
      //do nothing
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

int ObLogPlan::get_order_by_topn_expr(int64_t input_card,
                                      ObRawExpr *&topn_expr,
                                      bool &is_fetch_with_ties,
                                      bool &need_limit)
{
  int ret = OB_SUCCESS;
  int64_t limit_count = 0;
  int64_t limit_offset = 0;
  const ObDMLStmt *stmt = NULL;
  bool is_null_value = false;
  need_limit = true;
  topn_expr = NULL;
  is_fetch_with_ties = false;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (!get_stmt()->has_limit()) {
    need_limit = false;
  } else if (get_stmt()->is_calc_found_rows() ||
             NULL == get_stmt()->get_limit_expr() ||
             NULL != get_stmt()->get_limit_percent_expr()) {
    need_limit = true;
  } else if (OB_FAIL(ObTransformUtils::get_limit_value(stmt->get_limit_expr(),
                                                       get_optimizer_context().get_params(),
                                                       get_optimizer_context().get_exec_ctx(),
                                                       &get_optimizer_context().get_allocator(),
                                                       limit_count,
                                                       is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else if (!is_null_value &&
             OB_FAIL(ObTransformUtils::get_limit_value(stmt->get_offset_expr(),
                                                       get_optimizer_context().get_params(),
                                                       get_optimizer_context().get_exec_ctx(),
                                                       &get_optimizer_context().get_allocator(),
                                                       limit_offset,
                                                       is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else {
    if (NULL != stmt->get_offset_expr()) {
      if (OB_FAIL(ObTransformUtils::make_pushdown_limit_count(
                                            get_optimizer_context().get_expr_factory(),
                                            *get_optimizer_context().get_session_info(),
                                            stmt->get_limit_expr(),
                                            stmt->get_offset_expr(),
                                            topn_expr))) {
        LOG_WARN("failed to make push down limit count", K(ret));
      } else if (OB_ISNULL(topn_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        need_limit = true;
        is_fetch_with_ties = stmt->is_fetch_with_ties();
      }
    } else {
      topn_expr = stmt->get_limit_expr();
      is_fetch_with_ties = stmt->is_fetch_with_ties();
      need_limit = false;
    }
  }
  return ret;
}

int ObLogPlan::allocate_exchange_as_top(ObLogicalOperator *&top,
                                        const ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  ObLogExchange *producer = NULL;
  ObLogExchange *consumer = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(producer = static_cast<ObLogExchange*>(
                       get_log_op_factory().allocate(*this, LOG_EXCHANGE))) ||
             OB_ISNULL(consumer = static_cast<ObLogExchange*>(
                       get_log_op_factory().allocate(*this, LOG_EXCHANGE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate sort for order by", K(producer), K(consumer), K(ret));
  } else {
    producer->set_child(ObLogicalOperator::first_child, top);
    consumer->set_child(ObLogicalOperator::first_child, producer);
    producer->set_to_producer();
    consumer->set_to_consumer();
    producer->set_sample_type(exch_info.sample_type_);
    if (OB_FAIL(producer->set_exchange_info(exch_info))) {
      LOG_WARN("failed to set exchange info", K(ret));
    } else if (OB_FAIL(producer->compute_property())) {
      LOG_WARN("failed to compute propery", K(ret));
    } else if (OB_FAIL(consumer->set_exchange_info(exch_info))) {
      LOG_WARN("failed to set exchange info", K(ret));
    } else if (OB_FAIL(consumer->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = consumer;
    }
  }
  return ret;
}

int ObLogPlan::allocate_px_object_sample_as_top(ObLogicalOperator *&top,
                                              ObPxObjectSampleType obj_sample_type,
                                              const ObIArray<OrderItem> &sort_keys,
                                              share::schema::ObPartitionLevel part_level)
{
  int ret = OB_SUCCESS;
  if (obj_sample_type == SAMPLE_SORT) {
    ObLogPxObjectSample *object_sample = NULL;
    if (OB_ISNULL(top)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(top), K(ret));
    } else if (OB_ISNULL(object_sample =
        static_cast<ObLogPxObjectSample*>(get_log_op_factory().allocate(*this, LOG_PX_OBJECT_SAMPLE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate sort for order by", K(ret));
    } else {
      object_sample->set_child(ObLogicalOperator::first_child, top);
      object_sample->set_is_none_partition(PARTITION_LEVEL_ZERO == part_level);
      object_sample->set_px_object_sample_type(obj_sample_type);
      if (OB_FAIL(object_sample->set_sort_keys(sort_keys))) {
        LOG_WARN("failed to set sort keys", K(ret));
      } else if (OB_FAIL(object_sample->compute_property())) {
        LOG_WARN("failed to compute property", K(ret));
      } else {
        top = object_sample;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported stat type", K(ret));
  }
  return ret;
}

/**
 * 检查当前枚举的join order是否满足leading hint要求
 * match hint是否受leading hint控制
 * is_legal是否与leading hint冲突
 */
int ObLogPlan::check_join_hint(const ObRelIds &left_set,
                              const ObRelIds &right_set,
                              bool &match_hint,
                              bool &is_legal,
                              bool &is_strict_order)
{
  int ret = OB_SUCCESS;
  const ObRelIds &leading_tables = get_leading_tables();
  if (!left_set.overlap(leading_tables) && !right_set.overlap(leading_tables)) {
    //没有涉及leading hint的表，不需要额外的检查
    match_hint = false;
    is_legal = true;
    is_strict_order = true;
  } else if (left_set.is_subset(leading_tables) && right_set.is_subset(leading_tables)) {
    //正在枚举leading hint内部表
    bool found = false;
    //查找是否有满足的hint
    ObIArray<LeadingInfo> &leading_infos = log_plan_hint_.join_order_.leading_infos_;
    for (int64_t i = 0; !found && i < leading_infos.count(); ++i) {
      const LeadingInfo &info = leading_infos.at(i);
      if (left_set.equal(info.left_table_set_) && right_set.equal(info.right_table_set_)) {
        is_strict_order = true;
        found = true;
      } else if (right_set.equal(info.left_table_set_) && left_set.equal(info.right_table_set_)) {
        is_strict_order = false;
        found = true;
      }
    }
    if (!found) {
      //枚举的join order尝试打乱leading hint
      is_legal = false;
    } else {
      match_hint = true;
      is_legal = true;
    }
  } else if (leading_tables.is_subset(left_set)) {
    //处理完所有leading hint表之后的枚举过程
    match_hint = true;
    is_legal = true;
    is_strict_order = true;
  } else {
    //使用部分leading hint的表，非法枚举
    is_legal = false;
  }
  return ret;
}

int ObLogPlan::allocate_scala_group_by_as_top(ObLogicalOperator *&top,
                                              const ObIArray<ObAggFunRawExpr*> &agg_items,
                                              const ObIArray<ObRawExpr*> &having_exprs,
                                              const bool from_pivot,
                                              const double origin_child_card)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 1> dummy_group_by_exprs;
  ObSEArray<ObRawExpr*, 1> dummy_rollup_exprs;
  if (OB_FAIL(allocate_group_by_as_top(top,
                                       AggregateAlgo::SCALAR_AGGREGATE,
                                       dummy_group_by_exprs,
                                       dummy_rollup_exprs,
                                       agg_items,
                                       having_exprs,
                                       from_pivot,
                                       1.0,
                                       origin_child_card))) {
    LOG_WARN("failed to allocate group by as top", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::allocate_group_by_as_top(ObLogicalOperator *&top,
                                        const AggregateAlgo algo,
                                        const ObIArray<ObRawExpr*> &group_by_exprs,
                                        const ObIArray<ObRawExpr*> &rollup_exprs,
                                        const ObIArray<ObAggFunRawExpr*> &agg_items,
                                        const ObIArray<ObRawExpr*> &having_exprs,
                                        const bool from_pivot,
                                        const double total_ndv,
                                        const double origin_child_card,
                                        const bool is_partition_wise,
                                        const bool is_push_down,
                                        const bool is_partition_gi,
                                        const ObRollupStatus rollup_status,
                                        const ObThreeStageAggrInfo *three_stage_info)
{
  int ret = OB_SUCCESS;
  ObLogGroupBy *group_by = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(top));
  } else if (OB_ISNULL(group_by = static_cast<ObLogGroupBy*>(
                       get_log_op_factory().allocate(*this, LOG_GROUP_BY)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate group by operator", K(ret));
  } else {
    const ObGlobalHint &global_hint = get_optimizer_context().get_global_hint();
    bool has_dbms_stats = global_hint.has_dbms_stats_hint();
    bool is_first_stage = NULL != three_stage_info && three_stage_info->aggr_stage_ == ObThreeStageAggrStage::FIRST_STAGE;
    group_by->set_child(ObLogicalOperator::first_child, top);
    group_by->set_algo_type(algo);
    group_by->set_from_pivot(from_pivot);
    group_by->set_push_down(is_push_down);
    group_by->set_partition_gi(is_partition_gi);
    group_by->set_total_ndv(total_ndv);
    group_by->set_origin_child_card(origin_child_card);
    group_by->set_rollup_status(rollup_status);
    group_by->set_is_partition_wise(is_partition_wise);
    group_by->set_force_push_down((FORCE_GPD & get_optimizer_context().get_aggregation_optimization_settings()) ||
                                  (!is_first_stage && has_dbms_stats));
    if (OB_FAIL(group_by->set_group_by_exprs(group_by_exprs))) {
      LOG_WARN("failed to set group by columns", K(ret));
    } else if (OB_FAIL(group_by->set_rollup_exprs(rollup_exprs))) {
      LOG_WARN("failed to set rollup columns", K(ret));
    } else if (OB_FAIL(group_by->set_aggr_exprs(agg_items))) {
      LOG_WARN("failed to set aggregation exprs", K(ret));
    } else if (OB_FAIL(group_by->get_filter_exprs().assign(having_exprs))) {
      LOG_WARN("failed to set filter exprs", K(ret));
    } else if (NULL != three_stage_info &&
               OB_FAIL(group_by->set_three_stage_info(*three_stage_info))) {
      LOG_WARN("failed to set three stage info", K(ret));
    } else if (OB_FAIL(group_by->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = group_by;
    }
  }
  return ret;
}

int ObLogPlan::allocate_sort_and_exchange_as_top(ObLogicalOperator *&top,
                                                 const ObExchangeInfo &exch_info,
                                                 const ObIArray<OrderItem> &sort_keys,
                                                 const bool need_sort,
                                                 const int64_t prefix_pos,
                                                 const bool is_local_order,
                                                 ObRawExpr *topn_expr,
                                                 bool is_fetch_with_ties,
                                                 const OrderItem *hash_sortkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (exch_info.is_pq_local() && NULL == topn_expr && GCONF._enable_px_ordered_coord) {
    if (OB_FAIL(allocate_dist_range_sort_as_top(top, sort_keys, need_sort, is_local_order))) {
      LOG_WARN("failed to allocate dist range sort as top", K(ret));
    } else { /*do nothing*/ }
  } else {
    // allocate push down limit if necessary
    if (NULL != topn_expr && !need_sort) {
      bool is_pushed = false;
      if (!is_fetch_with_ties &&
          OB_FAIL(try_push_limit_into_table_scan(top, topn_expr, topn_expr, NULL, is_pushed))) {
        LOG_WARN("failed to push limit into table scan", K(ret));
      } else if (!is_local_order && (!is_pushed || top->is_distributed()) &&
                 OB_FAIL(allocate_limit_as_top(top,
                                               topn_expr,
                                               NULL,
                                               NULL,
                                               false,
                                               false,
                                               is_fetch_with_ties,
                                               &sort_keys))) {
        LOG_WARN("failed to allocate limit as top", K(ret));
      } else { /*do nothing*/ }
    }

    // allocate push down sort if necessary
    if (OB_SUCC(ret) &&
        (exch_info.is_pq_local() || !exch_info.need_exchange()) && !sort_keys.empty() &&
        (need_sort || is_local_order)) {
      int64_t real_prefix_pos = need_sort && !is_local_order ? prefix_pos : 0;
      bool real_local_order = need_sort ? false : is_local_order;
      if (OB_FAIL(allocate_sort_as_top(top,
                                       sort_keys,
                                       real_prefix_pos,
                                       real_local_order,
                                       topn_expr,
                                       is_fetch_with_ties,
                                       hash_sortkey))) {
        LOG_WARN("failed to allocate sort as top", K(ret));
      } else if (OB_ISNULL(top)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else { /*do nothing*/ }
    }

    // allocate exchange if necessary
    if (OB_SUCC(ret) && exch_info.need_exchange()) {
      if (!sort_keys.empty() &&
          (top->is_distributed() || is_local_order) &&
          (!need_sort || exch_info.is_pq_local())) {
        ObExchangeInfo cur_exch_info;
        if (OB_FAIL(cur_exch_info.assign(exch_info))) {
          LOG_WARN("failed to assign exchange info", K(ret));
        } else {
          cur_exch_info.is_merge_sort_ = true;
          cur_exch_info.is_sort_local_order_ = exch_info.is_pq_local() ? false : is_local_order;
          cur_exch_info.sort_keys_.reuse();
          if (hash_sortkey != NULL && OB_FAIL(cur_exch_info.sort_keys_.push_back(*hash_sortkey))) {
            LOG_WARN("failed to add hash sort key", K(ret));
          } else if (OB_FAIL(append(cur_exch_info.sort_keys_, sort_keys))) {
            LOG_WARN("failed to append sort keys", K(ret));
          } else if (OB_FAIL(allocate_exchange_as_top(top, cur_exch_info))) {
            LOG_WARN("failed to allocate exchange as top", K(ret));
          }
        }
      } else if (OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
        LOG_WARN("failed to allocate exchange as top", K(ret));
      } else { /*do nothing*/ }
    }

    // allocate final sort if necessary
    if (OB_SUCC(ret) && need_sort && !sort_keys.empty() &&
        exch_info.need_exchange() && !exch_info.is_pq_local()) {
      int64_t real_prefix_pos = 0;
      bool real_local_order = false;
      if (OB_FAIL(allocate_sort_as_top(top,
                                       sort_keys,
                                       real_prefix_pos,
                                       real_local_order,
                                       NULL,
                                       false,
                                       hash_sortkey))) {
        LOG_WARN("failed to allocate sort as top", K(ret));
      } else { /*do nothing*/ }
    }

    // allocate final limit if necessary
    if (OB_SUCC(ret) && NULL != topn_expr && exch_info.is_pq_local()) {
      if (OB_FAIL(allocate_limit_as_top(top,
                                        topn_expr,
                                        NULL,
                                        NULL,
                                        false,
                                        false,
                                        is_fetch_with_ties,
                                        &sort_keys))) {
        LOG_WARN("failed to allocate limit as top", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::allocate_dist_range_sort_as_top(ObLogicalOperator *&top,
                                               const ObIArray<OrderItem> &sort_keys,
                                               const bool need_sort,
                                               const bool is_local_order)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    // allocate range exchange info
    ObExchangeInfo range_exch_info;
    range_exch_info.dist_method_ = ObPQDistributeMethod::RANGE;
    range_exch_info.sample_type_ = HEADER_INPUT_SAMPLE;
    if (OB_FAIL(range_exch_info.sort_keys_.assign(sort_keys))) {
      LOG_WARN("failed to assign sort keys", K(ret));
    } else if (OB_FAIL(allocate_exchange_as_top(top, range_exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    }

    // allocate sort
    if (OB_SUCC(ret)) {
      bool prefix_pos = 0;
      bool is_local_merge_sort = !need_sort && is_local_order;
      if (OB_FAIL(allocate_sort_as_top(top, sort_keys, prefix_pos, is_local_merge_sort))) {
        LOG_WARN("failed to allocate sort as top", K(ret));
      } else { /*do nothing*/ }
    }
    // allocate final exchange
    if (OB_SUCC(ret)) {
      ObExchangeInfo temp_exch_info;
      temp_exch_info.is_task_order_ = true;
      if (OB_FAIL(temp_exch_info.sort_keys_.assign(sort_keys))) {
        LOG_WARN("failed to assign sort keys", K(ret));
      } else if (OB_FAIL(allocate_exchange_as_top(top, temp_exch_info))) {
        LOG_WARN("failed to allocate exchange as top", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::try_allocate_sort_as_top(ObLogicalOperator *&top,
                                        const ObIArray<OrderItem> &sort_keys,
                                        const bool need_sort,
                                        const int64_t prefix_pos,
                                        const int64_t part_cnt)
{
  int ret = OB_SUCCESS;
  OrderItem hash_sortkey;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (need_sort && part_cnt > 0 &&
            OB_FAIL(create_hash_sortkey(part_cnt, sort_keys, hash_sortkey))) {
    LOG_WARN("failed to create hash sort key", K(ret), K(part_cnt), K(sort_keys));
  } else {
    bool is_local_order = top->get_is_local_order()
        && (top->is_single() || (top->is_distributed() && top->is_exchange_allocated()));
    ObExchangeInfo exch_info;
    exch_info.dist_method_ = ObPQDistributeMethod::NONE;
    if (OB_FAIL(allocate_sort_and_exchange_as_top(top,
                                                  exch_info,
                                                  sort_keys,
                                                  need_sort,
                                                  prefix_pos,
                                                  is_local_order,
                                                  NULL,
                                                  false,
                                                  (need_sort && part_cnt > 0) ? &hash_sortkey : NULL))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::allocate_sort_as_top(ObLogicalOperator *&top,
                                    const ObIArray<OrderItem> &sort_keys,
                                    const int64_t prefix_pos,
                                    const bool is_local_merge_sort,
                                    ObRawExpr *topn_expr,
                                    bool is_fetch_with_ties,
                                    const OrderItem *hash_sortkey)
{
  int ret = OB_SUCCESS;
  ObLogSort *sort = NULL;
  int64_t part_cnt = 0;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(ret));
  } else if (OB_ISNULL(sort = static_cast<ObLogSort*>(get_log_op_factory().allocate(*this, LOG_SORT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate sort for order by", K(ret));
  } else {
    sort->set_child(ObLogicalOperator::first_child, top);
    sort->set_prefix_pos(prefix_pos);
    sort->set_local_merge_sort(is_local_merge_sort);
    sort->set_topn_expr(topn_expr);
    sort->set_fetch_with_ties(is_fetch_with_ties);
    if (hash_sortkey != NULL &&
        hash_sortkey->expr_ != NULL &&
        hash_sortkey->expr_->get_expr_type() == T_FUN_SYS_HASH) {
      part_cnt = hash_sortkey->expr_->get_children_count();
    }
    sort->set_part_cnt(part_cnt);

    if (OB_FAIL(sort->set_sort_keys(sort_keys))) {
      LOG_WARN("failed to set sort keys", K(ret));
    } else if (part_cnt > 0 && FALSE_IT(sort->set_hash_sortkey(*hash_sortkey))) {
    } else if (OB_FAIL(sort->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = sort;
    }
    if (OB_SUCC(ret) && NULL != topn_expr &&
        OB_FAIL(construct_startup_filter_for_limit(topn_expr, sort))) {
      LOG_WARN("failed to construct startup filter", KPC(topn_expr));
    }
  }
  return ret;
}

/*
 * Limit clause will trigger a cost re-estimation phase based on a uniform distribution assumption.
 * For certain plans, this assumption may result in bad plans (
 * instead of choosing minimal-cost plans, we prefer more reliable plans.
 */
int ObLogPlan::candi_allocate_limit(const ObIArray<OrderItem> &order_items)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  OPT_TRACE_TITLE("start generate limit");
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  } else if (OB_FAIL(candi_allocate_limit(stmt->get_limit_expr(),
                                          stmt->get_offset_expr(),
                                          stmt->get_limit_percent_expr(),
                                          stmt->is_calc_found_rows(),
                                          stmt->has_top_limit(),
                                          stmt->is_fetch_with_ties(),
                                          &order_items))) {
    LOG_WARN("failed to allocate limit operator", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::candi_allocate_limit(ObRawExpr *limit_expr,
                                    ObRawExpr *offset_expr,
                                    ObRawExpr *percent_expr,
                                    const bool is_calc_found_rows,
                                    const bool is_top_limit,
                                    const bool is_fetch_with_ties,
                                    const ObIArray<OrderItem> *ties_ordering)
{
  int ret = OB_SUCCESS;
  ObRawExpr *pushed_expr = NULL;
  if (NULL != limit_expr &&
      OB_FAIL(ObTransformUtils::make_pushdown_limit_count(
                                       get_optimizer_context().get_expr_factory(),
                                       *get_optimizer_context().get_session_info(),
                                       limit_expr,
                                       offset_expr,
                                       pushed_expr))) {
    LOG_WARN("failed to make push down limit count", K(ret));
  } else {
    ObSEArray<CandidatePlan, 8> non_reliable_plans;
    ObSEArray<CandidatePlan, 8> reliable_plans;
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
      bool is_reliable = false;
      CandidatePlan &plain_plan = candidates_.candidate_plans_.at(i);
      OPT_TRACE("generate limit for plan:", plain_plan);
      if (OB_ISNULL(plain_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(create_limit_plan(plain_plan.plan_tree_,
                                           limit_expr,
                                           pushed_expr,
                                           offset_expr,
                                           percent_expr,
                                           is_calc_found_rows,
                                           is_top_limit,
                                           is_fetch_with_ties,
                                           ties_ordering))) {
        LOG_WARN("failed to create limit plan", K(ret));
      } else if (NULL == percent_expr &&
                 OB_FAIL(is_plan_reliable(plain_plan.plan_tree_, is_reliable))) {
        LOG_WARN("failed to check plan is reliable", K(ret));
      } else if (is_reliable) {
        ret = reliable_plans.push_back(plain_plan);
      } else {
        ret = non_reliable_plans.push_back(plain_plan);
      }
    }
    if (OB_SUCC(ret)) {
      int64_t check_scope = OrderingCheckScope::NOT_CHECK;
      if (reliable_plans.empty() && OB_FAIL(reliable_plans.assign(non_reliable_plans))) {
        LOG_WARN("failed to assign plans", K(ret));
      } else if (OB_FAIL(update_plans_interesting_order_info(reliable_plans, check_scope))) {
        LOG_WARN("failed to update plans interesting order info", K(ret));
      } else if (OB_FAIL(prune_and_keep_best_plans(reliable_plans))) {
        LOG_WARN("failed to prune and keep best plans", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::create_limit_plan(ObLogicalOperator *&top,
                                 ObRawExpr *limit_expr,
                                 ObRawExpr *pushed_expr,
                                 ObRawExpr *offset_expr,
                                 ObRawExpr *percent_expr,
                                 const bool is_calc_found_rows,
                                 const bool is_top_limit,
                                 const bool is_fetch_with_ties,
                                 const ObIArray<OrderItem> *ties_ordering)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (NULL != percent_expr) {
    // for percent case
    if (top->is_distributed() && OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_ISNULL(top)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (LOG_MATERIAL != top->get_type() &&
              (LOG_SORT != top->get_type() || !top->is_block_op()) &&
               OB_FAIL(allocate_material_as_top(top))) {
      LOG_WARN("failed to allocate material as top", K(ret));
    } else if (OB_FAIL(allocate_limit_as_top(top,
                                             limit_expr,
                                             offset_expr,
                                             percent_expr,
                                             is_calc_found_rows,
                                             is_top_limit,
                                             is_fetch_with_ties,
                                             ties_ordering)) ) {
      LOG_WARN("failed to allocate limit as top", K(ret));
    } else { /*do nothing*/ }
  } else {
    bool is_pushed = false;
    // for normal limit-offset case
    if (NULL != limit_expr && !is_calc_found_rows && !is_fetch_with_ties &&
        OB_FAIL(try_push_limit_into_table_scan(top,
                                               limit_expr,
                                               pushed_expr,
                                               offset_expr,
                                               is_pushed))) {
      LOG_WARN("failed to push limit into table scan", K(ret));
    } else if (top->is_single() && is_pushed) {
      // pushed into table-scan
    } else if (top->is_distributed() && !is_calc_found_rows && NULL != pushed_expr &&
               OB_FAIL(allocate_limit_as_top(top,
                                             pushed_expr,
                                             NULL,
                                             NULL,
                                             false,
                                             false,
                                             false,
                                             NULL))) {
      LOG_WARN("failed to allocate limit as top", K(ret));
    } else if (top->is_distributed() &&
               OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_FAIL(allocate_limit_as_top(top,
                                             limit_expr,
                                             offset_expr,
                                             percent_expr,
                                             is_calc_found_rows,
                                             is_top_limit,
                                             is_fetch_with_ties,
                                             ties_ordering))) {
      LOG_WARN("failed to allocate limit as top", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::try_push_limit_into_table_scan(ObLogicalOperator *top,
                                              ObRawExpr *limit_expr,
                                              ObRawExpr *pushed_expr,
                                              ObRawExpr *offset_expr,
                                              bool &is_pushed)
{
  int ret = OB_SUCCESS;
  is_pushed = false;
  if (OB_ISNULL(top) || OB_ISNULL(limit_expr) || OB_ISNULL(pushed_expr) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(limit_expr), K(get_stmt()), K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN == top->get_type()) {
    ObLogTableScan *table_scan = static_cast<ObLogTableScan *>(top);
    ObRawExpr *new_limit_expr = NULL;
    ObRawExpr *new_offset_expr = NULL;

    bool has_npd_filter = false; //has non-pushdown filter
    //if TSC contains filters that cannot be pushdown to the storage
    //the limit clause cannot be pushed down either.
    if (OB_FAIL(table_scan->has_nonpushdown_filter(has_npd_filter))) {
      LOG_WARN("check whether has non-pushdown filter failed", K(ret));
    } else if (!has_npd_filter && !is_virtual_table(table_scan->get_ref_table_id()) &&
        table_scan->get_table_type() != schema::EXTERNAL_TABLE &&
        !(OB_INVALID_ID != table_scan->get_dblink_id() && NULL != offset_expr) &&
        !get_stmt()->is_calc_found_rows() && !table_scan->is_sample_scan() &&
        !(table_scan->get_is_index_global() && table_scan->get_index_back() && table_scan->has_index_lookup_filter()) &&
        (NULL == table_scan->get_limit_expr() ||
         ObOptimizerUtil::is_point_based_sub_expr(limit_expr, table_scan->get_limit_expr()))) {
      if (!top->is_distributed()) {
        new_limit_expr = limit_expr;
        new_offset_expr = offset_expr;
      } else {
        new_limit_expr = pushed_expr;
      }
      if (OB_FAIL(table_scan->set_limit_offset(new_limit_expr, new_offset_expr))) {
        LOG_WARN("failed to set limit-offset", K(ret));
      } else if (NULL != new_limit_expr && NULL == new_offset_expr &&
                 OB_FAIL(construct_startup_filter_for_limit(new_limit_expr, table_scan))) {
        LOG_WARN("failed to construct startup filter", KPC(limit_expr));
      } else {
        is_pushed = true;
      }
    }
  } else { /*do nothing*/ }
  return ret;
}

/*
 * A plan is reliable if it does not make any uniform assumption during the cost re-estimation phase.
 * In other words, it should satisfy the following two requirements:
 * 1 no operator in the plan has more than 1 children
 * 2 all operators in the plan is pipelinable and does not have any filters.
 */
int ObLogPlan::is_plan_reliable(const ObLogicalOperator *root,
                                bool &is_reliable)
{
  int ret = OB_SUCCESS;
  is_reliable = false;
  if (OB_ISNULL(root)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN == root->get_type()) {
    const ObCostTableScanInfo *cost_info = static_cast<const ObLogTableScan*>(root)->get_est_cost_info();
    if (OB_ISNULL(cost_info)) {
      /* cost_info could be null if limit has been pushed down into cte table scan */
      is_reliable = false;
    } else {
      is_reliable = cost_info->table_filters_.empty() && cost_info->postfix_filters_.empty();
    }
  } else if (log_op_def::LOG_GROUP_BY == root->get_type() ||
             log_op_def::LOG_SORT == root->get_type() ||
             log_op_def::LOG_WINDOW_FUNCTION == root->get_type() ||
             log_op_def::LOG_DISTINCT == root->get_type()) {
    is_reliable = false;
  } else if (root->get_filter_exprs().count() == 0 && !root->is_block_op()) {
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
    } else if (OB_FAIL(is_plan_reliable(root->get_child(0),
                                        is_child_reliable))) {
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

int ObLogPlan::allocate_limit_as_top(ObLogicalOperator *&old_top,
                                     ObRawExpr *limit_expr,
                                     ObRawExpr *offset_expr,
                                     ObRawExpr *percent_expr,
                                     const bool is_calc_found_rows,
                                     const bool is_top_limit,
                                     const bool is_fetch_with_ties,
                                     const ObIArray<OrderItem> *ties_ordering)
{
  int ret = OB_SUCCESS;
  ObLogLimit *limit = NULL;
  if (OB_ISNULL(old_top) || OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(old_top), K(get_stmt()), K(ret));
  } else if (log_op_def::LOG_LIMIT == old_top->get_type() &&
             ObOptimizerUtil::is_point_based_sub_expr(limit_expr,
                   static_cast<ObLogLimit*>(old_top)->get_limit_expr())) {
    limit = static_cast<ObLogLimit*>(old_top);
    limit->set_limit_expr(limit_expr);
    limit->set_offset_expr(offset_expr);
    limit->set_percent_expr(percent_expr);
    limit->set_is_calc_found_rows(is_calc_found_rows);
    limit->set_top_limit(is_top_limit);
    limit->set_fetch_with_ties(is_fetch_with_ties);
    if (OB_FAIL(limit->est_cost())) {
      LOG_WARN("failed to estimate cost", K(ret));
    } else { /*do nothing*/ }
  } else if (OB_ISNULL(limit = static_cast<ObLogLimit *>
                               (get_log_op_factory().allocate(*this, LOG_LIMIT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for limit op", K(ret));
  } else {
    limit->set_limit_expr(limit_expr);
    limit->set_offset_expr(offset_expr);
    limit->set_percent_expr(percent_expr);
    limit->set_child(ObLogicalOperator::first_child, old_top);
    limit->set_is_calc_found_rows(is_calc_found_rows);
    limit->set_top_limit(is_top_limit);
    limit->set_fetch_with_ties(is_fetch_with_ties);
    //支持with ties功能,需要保存对应的order items,由于存在order by会保存在expected_ordering中，所以直接共用
    //但是直接将get_order_items()放入到expected ordering是不对的,可能会导致在分布式计划中多生成一个sort算子,
    //因此需要按照设置order by item方式设置, 这里主要是防止后续消除order by语义. order by的SORT可能不需要分配
    if (NULL != ties_ordering && is_fetch_with_ties &&
        OB_FAIL(limit->set_ties_ordering(*ties_ordering))) {
      LOG_WARN("failed to set ties ordering", K(ret));
    } else if (OB_FAIL(limit->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      old_top = limit;
    }
  }
  if (OB_SUCC(ret) && NULL != limit_expr && NULL == offset_expr
      && NULL == percent_expr && !is_calc_found_rows &&
      OB_FAIL(construct_startup_filter_for_limit(limit_expr, limit))) {
    LOG_WARN("failed to construct startup filter", KPC(limit_expr));
  }
  return ret;
}

int ObLogPlan::candi_allocate_select_into()
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  CandidatePlan candidate_plan;
  ObSEArray<CandidatePlan, 4> select_into_plans;
  for (int64_t i = 0 ; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
    candidate_plan = candidates_.candidate_plans_.at(i);
    if (OB_ISNULL(candidate_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (candidate_plan.plan_tree_->is_sharding() &&
               OB_FAIL((allocate_exchange_as_top(candidate_plan.plan_tree_, exch_info)))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_FAIL(allocate_select_into_as_top(candidate_plan.plan_tree_))) {
      LOG_WARN("failed to allocate select into", K(ret));
    } else if (OB_FAIL(select_into_plans.push_back(candidate_plan))) {
      LOG_WARN("failed to push back candidate plan", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_and_keep_best_plans(select_into_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::allocate_select_into_as_top(ObLogicalOperator *&old_top)
{
  int ret = OB_SUCCESS;
  ObLogSelectInto *select_into = NULL;
  const ObSelectStmt *stmt = static_cast<const ObSelectStmt *>(get_stmt());
  if (OB_ISNULL(old_top) || OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Get unexpected null", K(ret), K(old_top), K(get_stmt()));
  } else if (OB_ISNULL(select_into = static_cast<ObLogSelectInto *>(
                       get_log_op_factory().allocate(*this, LOG_SELECT_INTO)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for ObLogSelectInto failed", K(ret));
  } else {
    ObSelectIntoItem *into_item = stmt->get_select_into();
    ObSEArray<ObRawExpr*, 4> select_exprs;
    ObRawExpr *to_outfile_expr = NULL;
    if (OB_ISNULL(into_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("into item is null", K(ret));
    } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (T_INTO_OUTFILE == into_item->into_type_ &&
               OB_FAIL(ObRawExprUtils::build_to_outfile_expr(
                                    get_optimizer_context().get_expr_factory(),
                                    get_optimizer_context().get_session_info(),
                                    into_item,
                                    select_exprs,
                                    to_outfile_expr))) {
      LOG_WARN("failed to build_to_outfile_expr", K(*into_item), K(ret));
    } else if (T_INTO_OUTFILE == into_item->into_type_ &&
               OB_FAIL(select_into->get_select_exprs().push_back(to_outfile_expr))) {
      LOG_WARN("failed to add into outfile expr", K(ret));
    } else if (T_INTO_OUTFILE != into_item->into_type_ &&
               OB_FAIL(select_into->get_select_exprs().assign(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else {
      select_into->set_into_type(into_item->into_type_);
      select_into->set_outfile_name(into_item->outfile_name_);
      select_into->set_filed_str(into_item->filed_str_);
      select_into->set_line_str(into_item->line_str_);
      select_into->set_user_vars(into_item->user_vars_);
      select_into->set_is_optional(into_item->is_optional_);
      select_into->set_closed_cht(into_item->closed_cht_);
      select_into->set_child(ObLogicalOperator::first_child, old_top);
      // compute property
      if (OB_FAIL(select_into->compute_property())) {
        LOG_WARN("failed to compute equal set", K(ret));
      } else {
        old_top = select_into;
      }
    }
  }
  return ret;
}

int ObLogPlan::candi_allocate_subplan_filter_for_where()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> filters;
  if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(get_subquery_filters(),
                                                  filters,
                                                  false))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter(get_subquery_filters(),
                                                   filters.empty() ? NULL : &filters))) {
    LOG_WARN("failed to candi allocate subplan filter", K(ret));
  }
  return ret;
}

int ObLogPlan::candi_allocate_subplan_filter(const ObIArray<ObRawExpr*> &subquery_exprs,
                                             const ObIArray<ObRawExpr *> *filters,
                                             const bool is_update_set,
                                             const bool for_on_condition)
{
  int ret = OB_SUCCESS;
  ObBitSet<> initplan_idxs;
  ObBitSet<> onetime_idxs;
  bool for_cursor_expr = false;
  ObSEArray<ObLogPlan*, 4> subplans;
  ObSEArray<ObQueryRefRawExpr *, 4> query_refs;
  ObSEArray<ObExecParamRawExpr *, 4> params;
  ObSEArray<ObExecParamRawExpr *, 4> onetime_exprs;
  ObSEArray<ObRawExpr *, 4> new_filters;
  OPT_TRACE_TITLE("start generate subplan filter");
  ObSEArray<ObQueryRefRawExpr*, 4> subqueries;
  ObSEArray<ObRawExpr*, 4> nested_subquery_exprs;
  if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(subquery_exprs, subqueries, false))) {
    LOG_WARN("failed to extract query ref expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_nested_exprs(subqueries, nested_subquery_exprs))) {
    LOG_WARN("failed to get nested subquery exprs", K(ret));
  } else if (!nested_subquery_exprs.empty() &&
             OB_FAIL(SMART_CALL(candi_allocate_subplan_filter(nested_subquery_exprs)))) {
    LOG_WARN("failed to allocate subplan filter for order by exprs", K(ret));
  } else if (OB_FAIL(generate_subplan_filter_info(subquery_exprs,
                                                  subplans,
                                                  query_refs,
                                                  params,
                                                  onetime_exprs,
                                                  initplan_idxs,
                                                  onetime_idxs,
                                                  for_cursor_expr,
                                                  for_on_condition))) {
    LOG_WARN("failed to generated subplan filter info", K(ret));
  } else if (NULL != filters && OB_FAIL(adjust_exprs_with_onetime(*filters, new_filters))) {
    LOG_WARN("failed to transform filters with onetime", K(ret));
  } else if (subplans.empty()) {
    if (NULL != filters) {
      if (OB_FAIL(candi_allocate_filter(new_filters))) {
        LOG_WARN("failed to allocate filter as top", K(ret));
      }
    }
  } else {
    if (OB_FAIL(inner_candi_allocate_subplan_filter(subplans,
                                                    query_refs,
                                                    params,
                                                    onetime_exprs,
                                                    initplan_idxs,
                                                    onetime_idxs,
                                                    new_filters,
                                                    for_cursor_expr,
                                                    is_update_set))) {
      LOG_WARN("failed to allocate subplan filter", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::inner_candi_allocate_subplan_filter(ObIArray<ObLogPlan*> &subplans,
                                                   ObIArray<ObQueryRefRawExpr *> &query_refs,
                                                   ObIArray<ObExecParamRawExpr *> &params,
                                                   ObIArray<ObExecParamRawExpr *> &onetime_exprs,
                                                   ObBitSet<> &initplan_idxs,
                                                   ObBitSet<> &onetime_idxs,
                                                   const ObIArray<ObRawExpr *> &filters,
                                                   const bool for_cursor_expr,
                                                   const bool is_update_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSEArray<CandidatePlan, 4>, 8> best_subplan_list;
  ObSEArray<ObSEArray<CandidatePlan, 4>, 8> best_dist_subplan_list;
  ObSEArray<CandidatePlan, 4> subquery_plans;
  const bool has_onetime = !onetime_idxs.is_empty();
  int64_t dist_methods = DIST_INVALID_METHOD;
  if (OB_FAIL(prepare_subplan_candidate_list(subplans, params, best_subplan_list,
                                             best_dist_subplan_list))) {
    LOG_WARN("failed to prepare subplan candidate list", K(ret));
  } else if (OB_FAIL(get_valid_subplan_filter_dist_method(subplans,
                                                          for_cursor_expr,
                                                          has_onetime,
                                                          false,
                                                          dist_methods))) {
    LOG_WARN("failed to get valid subplan filter dist method", K(ret));
  } else if (DIST_INVALID_METHOD != dist_methods &&
             OB_FAIL(inner_candi_allocate_subplan_filter(best_subplan_list,
                                                          best_dist_subplan_list,
                                                          query_refs,
                                                          params,
                                                          onetime_exprs,
                                                          initplan_idxs,
                                                          onetime_idxs,
                                                          filters,
                                                          for_cursor_expr,
                                                          is_update_set,
                                                          dist_methods,
                                                          subquery_plans))) {
    LOG_WARN("failed to allocate subplan filter", K(ret), K(subquery_plans.count()));
  } else if (!subquery_plans.empty()) {
    LOG_TRACE("succeed to allocate subplan filter using hint", K(subquery_plans.count()), K(dist_methods));
    OPT_TRACE("success to generate subplan filter plan with hint");
  } else if (OB_FAIL(get_log_plan_hint().check_status())) {
    LOG_WARN("failed to generate plans with hint", K(ret));
  } else if (OB_FAIL(get_valid_subplan_filter_dist_method(subplans,
                                                          for_cursor_expr,
                                                          has_onetime,
                                                          true,
                                                          dist_methods))) {
    LOG_WARN("failed to get valid subplan filter dist method", K(ret));
  } else if (OB_FAIL(inner_candi_allocate_subplan_filter(best_subplan_list,
                                                          best_dist_subplan_list,
                                                          query_refs,
                                                          params,
                                                          onetime_exprs,
                                                          initplan_idxs,
                                                          onetime_idxs,
                                                          filters,
                                                          for_cursor_expr,
                                                          is_update_set,
                                                          dist_methods,
                                                          subquery_plans))) {
    LOG_WARN("failed to allocate subplan filter", K(ret), K(subquery_plans.count()));
  } else {
    LOG_TRACE("succeed to allocate subplan filter ignore hint", K(subquery_plans.count()), K(dist_methods));
    OPT_TRACE("success to generate subplan filter plan ignore hint");
  }

  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(prune_and_keep_best_plans(subquery_plans))) {
    LOG_WARN("failed to prune and keep best plans", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

// get best candidate list
int ObLogPlan::prepare_subplan_candidate_list(ObIArray<ObLogPlan*> &subplans,
                                              ObIArray<ObExecParamRawExpr *> &params,
                                              ObIArray<ObSEArray<CandidatePlan, 4>> &best_list,
                                              ObIArray<ObSEArray<CandidatePlan, 4>> &dist_best_list)
{
  int ret = OB_SUCCESS;
  best_list.reuse();
  dist_best_list.reuse();
  ObLogPlan *log_plan = NULL;
  const ObDMLStmt *stmt = NULL;
  CandidatePlan candidate_plan;
  ObSEArray<CandidatePlan, 4> temp_plans;
  ObSEArray<CandidatePlan, 4> dist_temp_plans;
  ObExchangeInfo exch_info;
  for (int64_t i = 0; OB_SUCC(ret) && i < subplans.count(); i++) {
    temp_plans.reuse();
    if (OB_ISNULL(subplans.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(get_minimal_cost_candidates(subplans.at(i)->get_candidate_plans().candidate_plans_,
                                                   temp_plans))) {
      LOG_WARN("failed to get minimal cost candidates", K(ret));
    } else if (OB_UNLIKELY(temp_plans.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret));
    } else if (OB_FAIL(best_list.push_back(temp_plans))) {
      LOG_WARN("failed to push back temp plans", K(ret));
    } else {
      dist_temp_plans.reuse();
      for (int64_t j = 0; OB_SUCC(ret) && j < temp_plans.count(); j++) {
        candidate_plan = temp_plans.at(j);
        if (OB_ISNULL(candidate_plan.plan_tree_) ||
            OB_ISNULL(log_plan = candidate_plan.plan_tree_->get_plan()) ||
            OB_ISNULL(stmt = log_plan->get_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(log_plan), K(stmt), K(ret));
        } else if (!candidate_plan.plan_tree_->is_sharding() ||
                   candidate_plan.plan_tree_->get_contains_fake_cte()) {
          /*do nothing*/
        } else if (OB_FAIL(log_plan->allocate_exchange_as_top(candidate_plan.plan_tree_, exch_info))) {
          LOG_WARN("failed to allocate exchange as top", K(ret));
        } else if (params.empty() && stmt->is_contains_assignment() &&
                   OB_FAIL(log_plan->allocate_material_as_top(candidate_plan.plan_tree_))) {
          LOG_WARN("failed to allocate material as top", K(ret));
        } else { /*do nothing*/ }

        if (OB_FAIL(ret)) {
          /*do nothing*/
        } else if (OB_FAIL(dist_temp_plans.push_back(candidate_plan))) {
          LOG_WARN("failed to push back temp plans", K(ret));
        } else { /*do nothing*/ }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(dist_best_list.push_back(dist_temp_plans))) {
          LOG_WARN("failed to push back plan list", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObLogPlan::get_valid_subplan_filter_dist_method(ObIArray<ObLogPlan*> &subplans,
                                                    const bool for_cursor_expr,
                                                    const bool has_onetime,
                                                    const bool ignore_hint,
                                                    int64_t &dist_methods)
{
  int ret = OB_SUCCESS;
  dist_methods = DIST_BASIC_METHOD | DIST_PULL_TO_LOCAL
                 | DIST_PARTITION_WISE | DIST_PARTITION_NONE
                 | DIST_NONE_ALL | DIST_HASH_ALL | DIST_RANDOM_ALL;
  const ObLogicalOperator *op = NULL;
  bool contain_recursive_cte = false;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(candidates_.candidate_plans_.empty()
      || OB_UNLIKELY(subplans.empty()))
      || OB_ISNULL(op = candidates_.candidate_plans_.at(0).plan_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected list", K(ret), K(subplans.count()), K(op));
  } else {
    contain_recursive_cte |= op->get_contains_fake_cte();
    ObSEArray<ObString, 4> sub_qb_names;
    ObString qb_name;
    ObLogPlan *subplan = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < subplans.count(); i++) {
      if (OB_ISNULL(subplan = subplans.at(i)) || OB_ISNULL(subplan->get_stmt())
          || OB_UNLIKELY(subplan->candidates_.candidate_plans_.empty())
          || OB_ISNULL(op = subplan->candidates_.candidate_plans_.at(0).plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected list", K(ret), K(subplan), K(op));
      } else if (OB_FAIL(subplan->get_stmt()->get_qb_name(qb_name))) {
        LOG_WARN("failed to get qb name", K(ret));
      } else if (OB_FAIL(sub_qb_names.push_back(qb_name))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        contain_recursive_cte |= op->get_contains_fake_cte();
      }
    }

    if (OB_SUCC(ret) && !ignore_hint) {
      const bool implicit_hint_allowed = (subplans.count() == get_stmt()->get_subquery_expr_size());
      dist_methods &= get_log_plan_hint().get_valid_pq_subquery_dist_algo(sub_qb_names, implicit_hint_allowed);
    }

    if (OB_SUCC(ret) && has_onetime) {
      dist_methods &= ~DIST_NONE_ALL;
      dist_methods &= ~DIST_HASH_ALL;
      dist_methods &= ~DIST_RANDOM_ALL;
      OPT_TRACE("SPF will not use DIST_NONE_ALL/DIST_HASH_ALL/DIST_RANDOM_ALL method due to onetime subquery");
    }

    if (OB_FAIL(ret)) {
    } else if (for_cursor_expr || contain_recursive_cte) {
      dist_methods &= (DIST_BASIC_METHOD | DIST_PULL_TO_LOCAL);
      OPT_TRACE("SPF will use basic method");
    } else if (!get_optimizer_context().is_var_assign_only_in_root_stmt()
               && get_optimizer_context().has_var_assign()) {
      dist_methods &= (DIST_BASIC_METHOD | DIST_PULL_TO_LOCAL);
      OPT_TRACE("SPF will use pull to local method for var assign");
    }
  }
  return ret;
}

int ObLogPlan::inner_candi_allocate_subplan_filter(ObIArray<ObSEArray<CandidatePlan,4>> &best_list,
                                                   ObIArray<ObSEArray<CandidatePlan,4>> &dist_best_list,
                                                   ObIArray<ObQueryRefRawExpr *> &query_refs,
                                                   ObIArray<ObExecParamRawExpr *> &params,
                                                   ObIArray<ObExecParamRawExpr *> &onetime_exprs,
                                                   ObBitSet<> &initplan_idxs,
                                                   ObBitSet<> &onetime_idxs,
                                                   const ObIArray<ObRawExpr *> &filters,
                                                   const bool for_cursor_expr,
                                                   const bool is_update_set,
                                                   const int64_t dist_methods,
                                                   ObIArray<CandidatePlan> &subquery_plans)
{
  int ret = OB_SUCCESS;
  if (query_refs.count() > 3) {
    if (OB_FAIL(inner_candi_allocate_massive_subplan_filter(best_list,
                                                            dist_best_list,
                                                            query_refs,
                                                            params,
                                                            onetime_exprs,
                                                            initplan_idxs,
                                                            onetime_idxs,
                                                            filters,
                                                            for_cursor_expr,
                                                            is_update_set,
                                                            dist_methods,
                                                            subquery_plans))) {
      LOG_WARN("failed to candi allocate massive subplan filter", K(ret));
    }
  } else {
    CandidatePlan candidate_plan;
    ObSEArray<int64_t, 4> move_pos;
    ObSEArray<ObLogicalOperator*, 4> child_ops;
    ObSEArray<ObLogicalOperator*, 4> dist_child_ops;
    // generate subplan filter
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
      candidate_plan = candidates_.candidate_plans_.at(i);
      OPT_TRACE("generate subplan filter for plan:", candidate_plan);
      if (OB_ISNULL(candidate_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(candidate_plan.plan_tree_), K(ret));
      } else {
        bool has_next = true;
        move_pos.reuse();
        for (int64_t j = 0; OB_SUCC(ret) && j < best_list.count(); j++) {
          ret = move_pos.push_back(0);
        }
        // get child ops to generate plan
        while (OB_SUCC(ret) && has_next) {
          child_ops.reuse();
          dist_child_ops.reuse();
          // get child ops to generate plan
          for (int64_t j = 0; OB_SUCC(ret) && j < move_pos.count(); j++) {
            int64_t size = best_list.at(j).count();
            if (OB_UNLIKELY(move_pos.at(j) < 0 || move_pos.at(j) >= size)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected array count", K(size), K(move_pos.at(i)), K(ret));
            } else if (OB_FAIL(child_ops.push_back(best_list.at(j).at(move_pos.at(j)).plan_tree_))) {
              LOG_WARN("failed to push back child ops", K(ret));
            } else if (OB_FAIL(dist_child_ops.push_back(dist_best_list.at(j).at(move_pos.at(j)).plan_tree_))) {
              LOG_WARN("failed to push back child ops", K(ret));
            } else { /*do nothing*/ }
          }
          // create subplan filter plan
          if (OB_SUCC(ret)) {
            CandidatePlan curr_candidate_plan(candidate_plan.plan_tree_);
            if (OB_FAIL(create_subplan_filter_plan(curr_candidate_plan.plan_tree_,
                                                    child_ops,
                                                    dist_child_ops,
                                                    query_refs,
                                                    params,
                                                    onetime_exprs,
                                                    initplan_idxs,
                                                    onetime_idxs,
                                                    dist_methods,
                                                    filters,
                                                    is_update_set,
                                                    for_cursor_expr))) {
              LOG_WARN("failed to create subplan filter plan", K(ret));
            } else if (NULL != curr_candidate_plan.plan_tree_
                      && OB_FAIL(subquery_plans.push_back(curr_candidate_plan))) {
              LOG_WARN("failed to push back subquery plans", K(ret));
            } else { /*do nothing*/ }
          }
          // reset pos for next generation
          if (OB_SUCC(ret)) {
            has_next = false;
            for (int64_t j = move_pos.count() - 1; !has_next && OB_SUCC(ret) && j >= 0; j--) {
              if (move_pos.at(j) < best_list.at(j).count() - 1) {
                ++move_pos.at(j);
                has_next = true;
                for (int64_t k = j + 1; k < move_pos.count(); k++) {
                  move_pos.at(k) = 0;
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::inner_candi_allocate_massive_subplan_filter(ObIArray<ObSEArray<CandidatePlan,4>> &best_list,
                                                            ObIArray<ObSEArray<CandidatePlan,4>> &dist_best_list,
                                                            ObIArray<ObQueryRefRawExpr *> &query_refs,
                                                            ObIArray<ObExecParamRawExpr *> &params,
                                                            ObIArray<ObExecParamRawExpr *> &onetime_exprs,
                                                            ObBitSet<> &initplan_idxs,
                                                            ObBitSet<> &onetime_idxs,
                                                            const ObIArray<ObRawExpr *> &filters,
                                                            const bool for_cursor_expr,
                                                            const bool is_update_set,
                                                            const int64_t dist_methods,
                                                            ObIArray<CandidatePlan> &subquery_plans)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSEArray<ObLogicalOperator*, 4>, 3> child_ops;
  ObSEArray<ObSEArray<ObLogicalOperator*, 4>, 3> dist_child_ops;
  if (OB_UNLIKELY(best_list.count() != dist_best_list.count() || best_list.count() != query_refs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(best_list.count()), K(dist_best_list.count()), K(query_refs.count()), K(ret));
  } else if (OB_FAIL(child_ops.prepare_allocate(3))
             || OB_FAIL(dist_child_ops.prepare_allocate(3))) {
    LOG_WARN("fail to prepare allocate", K(ret));
  } else {
    bool exist_das_plan = true;
    bool exist_px_plan = true;
    ObLogicalOperator *cur_child = NULL;
    int64_t das_best_pos = OB_INVALID_INDEX;
    int64_t px_best_pos = OB_INVALID_INDEX;
    int64_t best_pos = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < best_list.count(); ++i) {
      das_best_pos = OB_INVALID_INDEX;
      px_best_pos = OB_INVALID_INDEX;
      best_pos = OB_INVALID_INDEX;
      if (OB_UNLIKELY(best_list.at(i).count() != dist_best_list.at(i).count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array count", K(i), K(best_list.at(i).count()), K(dist_best_list.at(i).count()), K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < best_list.at(i).count(); ++j) {
        if (OB_ISNULL(cur_child = best_list.at(i).at(j).plan_tree_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(i), K(j), K(ret), K(best_list));
        } else {
          if (cur_child->is_match_all()
              && (OB_INVALID_INDEX == das_best_pos
                  || best_list.at(i).at(das_best_pos).plan_tree_->get_cost() > cur_child->get_cost())) {
            das_best_pos = j;
          }
          if ((!cur_child->is_match_all() || !cur_child->get_contains_das_op())
              && (OB_INVALID_INDEX == px_best_pos
                  || best_list.at(i).at(px_best_pos).plan_tree_->get_cost() > cur_child->get_cost())) {
            px_best_pos = j;
          }
          if (OB_INVALID_INDEX == best_pos
              || best_list.at(i).at(best_pos).plan_tree_->get_cost() > cur_child->get_cost()) {
            best_pos = j;
          }
        }
      }

      if (OB_FAIL(ret) || !exist_das_plan) {
      } else if (OB_INVALID_INDEX == das_best_pos) {
        exist_das_plan = false;
        child_ops.at(0).reuse();
        dist_child_ops.at(0).reuse();
      } else if (OB_FAIL(child_ops.at(0).push_back(best_list.at(i).at(das_best_pos).plan_tree_))
                 || OB_FAIL(dist_child_ops.at(0).push_back(dist_best_list.at(i).at(das_best_pos).plan_tree_))) {
        LOG_WARN("failed to push back", K(ret));
      }

      if (OB_FAIL(ret) || !exist_px_plan) {
      } else if (OB_INVALID_INDEX == px_best_pos) {
        exist_px_plan = false;
        child_ops.at(1).reuse();
        dist_child_ops.at(1).reuse();
      } else if (OB_FAIL(child_ops.at(1).push_back(best_list.at(i).at(px_best_pos).plan_tree_))
                 || OB_FAIL(dist_child_ops.at(1).push_back(dist_best_list.at(i).at(px_best_pos).plan_tree_))) {
        LOG_WARN("failed to push back", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == best_pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected best pos", K(ret), K(best_pos));
      } else if (OB_FAIL(child_ops.at(2).push_back(best_list.at(i).at(best_pos).plan_tree_))
                 || OB_FAIL(dist_child_ops.at(2).push_back(dist_best_list.at(i).at(best_pos).plan_tree_))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
      OPT_TRACE("generate subplan filter for plan:", candidates_.candidate_plans_.at(i));
      for (int64_t j = 0; OB_SUCC(ret) && j <= 2; ++j) {
        CandidatePlan curr_candidate_plan(candidates_.candidate_plans_.at(i).plan_tree_);
        if ((0 == j && !exist_das_plan)
            || (1 == j && !exist_px_plan)
            || (2 == j && (exist_das_plan || exist_px_plan))) {
          /* do nothing */
        } else if (OB_FAIL(create_subplan_filter_plan(curr_candidate_plan.plan_tree_,
                                                      child_ops.at(j),
                                                      dist_child_ops.at(j),
                                                      query_refs,
                                                      params,
                                                      onetime_exprs,
                                                      initplan_idxs,
                                                      onetime_idxs,
                                                      dist_methods,
                                                      filters,
                                                      is_update_set,
                                                      for_cursor_expr))) {
          LOG_WARN("failed to create subplan filter plan", K(ret));
        } else if (NULL != curr_candidate_plan.plan_tree_
                    && OB_FAIL(subquery_plans.push_back(curr_candidate_plan))) {
          LOG_WARN("failed to push back subquery plans", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

/*
 * @param for_on_condition
 *   The subquery on the on condition will try to generate the subplan filter multiple times
 *   when generating the plan, and the subquery on the on condition does not have a shared subquery,
 *   so when generating the subplan filter, the subplan only needs to be generated once,
 *   and when generating the subplan filter operator generated subplans do not need to be ignored.
 */
int ObLogPlan::generate_subplan_filter_info(const ObIArray<ObRawExpr *> &subquery_exprs,
                                            ObIArray<ObLogPlan *> &subplans,
                                            ObIArray<ObQueryRefRawExpr *> &query_refs,
                                            ObIArray<ObExecParamRawExpr *> &exec_params,
                                            ObIArray<ObExecParamRawExpr *> &onetime_exprs,
                                            ObBitSet<> &initplan_idxs,
                                            ObBitSet<> &onetime_idxs,
                                            bool &for_cursor_expr,
                                            bool for_on_condition)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObQueryRefRawExpr *, 4> candi_query_refs;
  ObSEArray<ObQueryRefRawExpr *, 4> onetime_query_refs;
  ObSEArray<ObQueryRefRawExpr *, 4> tmp;
  int64_t idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); ++i) {
    tmp.reuse();
    if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(subquery_exprs.at(i),
                                                         candi_query_refs,
                                                         false))) {
      LOG_WARN("failed to extract query ref exprs", K(ret));
    } else if (OB_FAIL(extract_onetime_exprs(subquery_exprs.at(i),
                                             onetime_exprs,
                                             tmp,
                                             for_on_condition))) {
      LOG_WARN("failed to extract onetime exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(onetime_query_refs, tmp))) {
      LOG_WARN("failed to append onetime query refs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(candi_query_refs, tmp))) {
      LOG_WARN("failed to append query refs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_query_refs.count(); ++i) {
    SubPlanInfo *info = NULL;
    if (OB_FAIL(get_subplan(candi_query_refs.at(i), info))) {
      LOG_WARN("failed to get subplan", K(ret));
    } else if (NULL != info && !for_on_condition && info->allocated_) {
      // do nothing
    } else if (OB_FAIL(append(exec_params, candi_query_refs.at(i)->get_exec_params()))) {
      LOG_WARN("failed to append exec params", K(ret));
    } else if (NULL == info &&
               OB_FAIL(generate_subplan_for_query_ref(candi_query_refs.at(i), info))) {
      LOG_WARN("failed to generate subplan for query ref", K(ret));
    } else if (OB_FAIL(subplans.push_back(info->subplan_))) {
      LOG_WARN("failed to push back subplan", K(ret));
    } else if (OB_FAIL(query_refs.push_back(candi_query_refs.at(i)))) {
      LOG_WARN("failed to push back query ref expr", K(ret));
    } else {
      ++ idx;
      info->allocated_ = true;
      for_cursor_expr = for_cursor_expr || candi_query_refs.at(i)->is_cursor();
      if (info->init_plan_) {
        if (ObOptimizerUtil::find_item(onetime_query_refs, candi_query_refs.at(i))) {
          if (OB_FAIL(onetime_idxs.add_member(idx))) {
            LOG_WARN("failed to add onetime query ref index", K(ret));
          }
        } else {
          if (OB_FAIL(initplan_idxs.add_member(idx))) {
            LOG_WARN("failed to add onetime query ref index", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::adjust_expr_with_onetime(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *new_expr = NULL;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = get_optimizer_context().get_session_info()) ||
      OB_ISNULL(onetime_copier_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(ret));
  } else if (OB_FAIL(onetime_copier_->copy_on_replace(expr, new_expr))) {
    LOG_WARN("failed to copy expr");
  } else if (OB_ISNULL(new_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (new_expr == expr) {
    // do nothing
  } else if (OB_FAIL(new_expr->formalize(session_info))) {
    LOG_WARN("failed to formalize expr", K(ret));
  } else {
    expr = new_expr;
  }
  return ret;
}

int ObLogPlan::adjust_exprs_with_onetime(const ObIArray<ObRawExpr *> &exprs,
                                         ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  if (0 == exprs.count()) {
    // do nothing
  } else if (OB_ISNULL(session_info = get_optimizer_context().get_session_info()) ||
             OB_ISNULL(onetime_copier_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(ret));
  } else if (OB_FAIL(onetime_copier_->copy_on_replace(exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_UNLIKELY(exprs.count() != new_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr number mismatch", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (exprs.at(i) == new_exprs.at(i)) {
      // do nothing
    } else if (OB_FAIL(new_exprs.at(i)->formalize(session_info))) {
      LOG_WARN("failed to formalize expr", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::adjust_exprs_with_onetime(ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> tmp_exprs;
  if (0 == exprs.count()) {
    // do nothing
  } else if (OB_FAIL(adjust_exprs_with_onetime(exprs, tmp_exprs))) {
    LOG_WARN("failed to adjust exprss with onetime", K(ret));
  } else if (OB_FAIL(exprs.assign(tmp_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  return ret;
}

int ObLogPlan::get_subplan_filter_distributed_method(ObLogicalOperator *&top,
                                                     const ObIArray<ObLogicalOperator*> &subquery_ops,
                                                     const ObIArray<ObExecParamRawExpr *> &params,
                                                     const bool for_cursor_expr,
                                                     int64_t &distributed_methods)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  bool is_remote = false;
  bool is_recursive_cte = false;
  bool is_partition_wise = false;
  bool is_none_all = false;
  bool is_all_none = false;
  bool is_partition_none = false;
  ObSEArray<ObLogicalOperator*, 8> sf_childs;

  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(sf_childs.push_back(top)) ||
             OB_FAIL(append(sf_childs, subquery_ops))) {
    LOG_WARN("failed to append child ops", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_basic_sharding_info(get_optimizer_context().get_local_server_addr(),
                                                                sf_childs,
                                                                is_basic,
                                                                is_remote))) {
    LOG_WARN("failed to check if match basic sharding info", K(ret));
  } else if (is_basic && !is_remote) {
    distributed_methods &= DistAlgo::DIST_BASIC_METHOD;
  } else if (is_remote) {
    distributed_methods &= (DIST_BASIC_METHOD | DIST_PULL_TO_LOCAL);
  }

  if (OB_SUCC(ret) && top->is_table_scan()
      && (distributed_methods & DistAlgo::DIST_HASH_ALL ||
          distributed_methods & DistAlgo::DIST_RANDOM_ALL)) {
    if (OB_FAIL(check_if_match_none_all(top, subquery_ops, is_none_all))) {
      LOG_WARN("failed to check if match repart", K(ret));
    } else if (is_none_all) {
      // if it's hint control
      if (distributed_methods == DistAlgo::DIST_HASH_ALL) {
        distributed_methods = DistAlgo::DIST_HASH_ALL;
        OPT_TRACE("SPF will use hash all method by hint");
      } else if (distributed_methods == DistAlgo::DIST_RANDOM_ALL) {
        distributed_methods = DistAlgo::DIST_RANDOM_ALL;
        OPT_TRACE("SPF will use random all method by hint");
      } else {
        int enable_px_random_shuffle_only_statistic_exist = (OB_E(EventTable::EN_PX_RANDOM_SHUFFLE_WITHOUT_STATISTIC_INFORMATION) OB_SUCCESS);
        int64_t compute_parallel = top->get_parallel();
        int64_t px_expected_work_count = 0;
        ObLogTableScan *log_table_scan = static_cast<ObLogTableScan *>(top);
        const AccessPath *ap = NULL;
        const ObTableMetaInfo *table_meta_info = NULL;
        if (OB_ISNULL(ap = log_table_scan->get_access_path())
            || OB_ISNULL(table_meta_info = ap->est_cost_info_.table_meta_info_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("get unexpected null", KPC(ap), KPC(table_meta_info), K(ret));
        } else if (OB_SUCC(enable_px_random_shuffle_only_statistic_exist) &&
         (!table_meta_info->has_opt_stat_ || table_meta_info->micro_block_count_ == 0)) {
          // Whether to use PX Random Shuffle is based on statistic infomation, so if we don't have
          // it, we just use normal NONE_ALL
          distributed_methods &= ~DIST_HASH_ALL;
          distributed_methods &= ~DIST_RANDOM_ALL;
          OPT_TRACE("plan will not use random all because lack of statistic information");
        } else if (OB_FAIL(ObOptimizerUtil::compute_nlj_spf_storage_compute_parallel_skew(
                     &get_optimizer_context(), log_table_scan->get_ref_table_id(), table_meta_info,
                     compute_parallel, px_expected_work_count))) {
          LOG_WARN("Fail to compute none_all spf storage compute parallel skew", K(ret));
        } else if (px_expected_work_count < compute_parallel) {
          // we have more compute resources, so we should add a hash shuffle
          // by default we use hash_all if we have exec_param, otherwise random_all in subplan filter only show up by hint
          if (params.empty()) {
            distributed_methods = DIST_RANDOM_ALL;
            OPT_TRACE("SPF will use random all method");
          } else {
            distributed_methods = DIST_HASH_ALL;
            OPT_TRACE("SPF will use hash all method");
          }
        } else {
          distributed_methods &= ~DistAlgo::DIST_HASH_ALL;
          distributed_methods &= ~DistAlgo::DIST_RANDOM_ALL;
        }
      }
    } else {
      distributed_methods &= ~DIST_HASH_ALL;
      distributed_methods &= ~DIST_RANDOM_ALL;
    }
  }

  if (OB_SUCC(ret) && (distributed_methods & DistAlgo::DIST_NONE_ALL)) {
    if (OB_FAIL(check_if_match_none_all(top, subquery_ops, is_none_all))) {
      LOG_WARN("failed to check if match repart", K(ret));
    } else if (is_none_all) {
      distributed_methods = DistAlgo::DIST_NONE_ALL;
      OPT_TRACE("SPF will use none all method");
    } else {
      distributed_methods &= ~DIST_NONE_ALL;
    }
  }

  if (OB_SUCC(ret) && (distributed_methods & DistAlgo::DIST_BASIC_METHOD)) {
    if (is_basic && (!for_cursor_expr || !is_remote)) {
      distributed_methods = DistAlgo::DIST_BASIC_METHOD;
      OPT_TRACE("SPF will use basic method");
    } else {
      distributed_methods &= ~DIST_BASIC_METHOD;
    }
  }

  if (OB_SUCC(ret) && (distributed_methods & DistAlgo::DIST_PARTITION_WISE)) {
    if (OB_FAIL(check_if_subplan_filter_match_partition_wise(top, subquery_ops, params, is_partition_wise))) {
      LOG_WARN("failed to check if match partition wise", K(ret));
    } else if (is_partition_wise) {
      distributed_methods = DistAlgo::DIST_PARTITION_WISE;
      OPT_TRACE("SPF will use partition wise method");
    } else {
      distributed_methods &= ~DIST_PARTITION_WISE;
    }
  }

  if (OB_SUCC(ret) && (distributed_methods & DistAlgo::DIST_PARTITION_NONE)) {
    if (OB_FAIL(check_if_subplan_filter_match_repart(top, subquery_ops, params, is_partition_none))) {
      LOG_WARN("failed to check if match repart", K(ret));
    } else if (is_partition_none) {
      distributed_methods = DistAlgo::DIST_PARTITION_NONE;
      OPT_TRACE("SPF will use repartition method");
    } else {
      distributed_methods &= ~DIST_PARTITION_NONE;
    }
  }

  if (OB_SUCC(ret) && (distributed_methods & DistAlgo::DIST_PULL_TO_LOCAL)) {
    distributed_methods = DistAlgo::DIST_PULL_TO_LOCAL;
    OPT_TRACE("SPF will use pull to local method");
  }
  return ret;
}

int ObLogPlan::create_subplan_filter_plan(ObLogicalOperator *&top,
                                          const ObIArray<ObLogicalOperator*> &subquery_ops,
                                          const ObIArray<ObLogicalOperator*> &dist_subquery_ops,
                                          const ObIArray<ObQueryRefRawExpr *> &query_ref_exprs,
                                          const ObIArray<ObExecParamRawExpr *> &params,
                                          const ObIArray<ObExecParamRawExpr *> &onetime_exprs,
                                          const ObBitSet<> &initplan_idxs,
                                          const ObBitSet<> &onetime_idxs,
                                          const int64_t dist_methods,
                                          const ObIArray<ObRawExpr*> &filters,
                                          const bool is_update_set,
                                          const bool for_cursor_expr)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  int64_t cur_dist_methods = dist_methods;
  DistAlgo dist_algo = DIST_INVALID_METHOD;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_subplan_filter_distributed_method(top,
                                                           subquery_ops,
                                                           params,
                                                           for_cursor_expr,
                                                           cur_dist_methods))) {
    LOG_WARN("failed to get subplan filter distributed method", K(ret));
  } else if (DIST_INVALID_METHOD == (dist_algo = get_dist_algo(cur_dist_methods))) {
    top = NULL;
  } else if (DistAlgo::DIST_BASIC_METHOD == dist_algo ||
             DistAlgo::DIST_PARTITION_WISE == dist_algo ||
             DistAlgo::DIST_NONE_ALL == dist_algo) {
    // is basic or is_partition_wise
    if (OB_FAIL(allocate_subplan_filter_as_top(top,
                                                subquery_ops,
                                                query_ref_exprs,
                                                params,
                                                onetime_exprs,
                                                initplan_idxs,
                                                onetime_idxs,
                                                filters,
                                                dist_algo,
                                                is_update_set))) {
      LOG_WARN("failed to allocate subplan filter as top", K(ret));
    } else { /*do nothing*/ }
  } else if (DistAlgo::DIST_HASH_ALL == dist_algo || DistAlgo::DIST_RANDOM_ALL == dist_algo) {
    if(OB_FAIL(compute_subplan_filter_random_shuffle_info(top, params, dist_algo, exch_info))) {
      LOG_WARN("failed to compute subplan filter random shuffle exchange info", K(ret));
    } else if (OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_FAIL(allocate_subplan_filter_as_top(top,
                                                      subquery_ops,
                                                      query_ref_exprs,
                                                      params,
                                                      onetime_exprs,
                                                      initplan_idxs,
                                                      onetime_idxs,
                                                      filters,
                                                      dist_algo,
                                                      is_update_set))) {
      LOG_WARN("failed to allocate subplan filter as top", K(ret));
    } else { /*do nothing*/ }
  } else if (DistAlgo::DIST_PARTITION_NONE == dist_algo) {
    if (OB_FAIL(compute_subplan_filter_repartition_distribution_info(top,
                                                                      subquery_ops,
                                                                      params,
                                                                      exch_info))) {
      LOG_WARN("failed to compute subplan filter distribution info", K(ret));
    } else if (OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
      LOG_WARN("failed to allocate exchange as top");
    } else if (OB_FAIL(allocate_subplan_filter_as_top(top,
                                                      subquery_ops,
                                                      query_ref_exprs,
                                                      params,
                                                      onetime_exprs,
                                                      initplan_idxs,
                                                      onetime_idxs,
                                                      filters,
                                                      dist_algo,
                                                      is_update_set))) {
      LOG_WARN("failed to allocate subplan filter as top", K(ret));
    } else { /*do nothing*/ }
  } else if (OB_UNLIKELY(DistAlgo::DIST_PULL_TO_LOCAL != dist_algo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected subplan filter distributed method", K(ret), K(dist_algo));
  } else if (top->is_sharding() && OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
    LOG_WARN("failed to allocate exchange as top", K(ret));
  } else if (OB_FAIL(allocate_subplan_filter_as_top(top,
                                                    dist_subquery_ops,
                                                    query_ref_exprs,
                                                    params,
                                                    onetime_exprs,
                                                    initplan_idxs,
                                                    onetime_idxs,
                                                    filters,
                                                    dist_algo,
                                                    is_update_set))) {
    LOG_WARN("failed to allocate subplan filter as top", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogPlan::compute_subplan_filter_random_shuffle_info(ObLogicalOperator* top,
                                                          const ObIArray<ObExecParamRawExpr *> &params,
                                                          const DistAlgo dist_algo,
                                                          ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  if (dist_algo == DistAlgo::DIST_RANDOM_ALL) {
    exch_info.dist_method_ = ObPQDistributeMethod::RANDOM;
  } else if (dist_algo == DistAlgo::DIST_HASH_ALL) {
    ObSEArray<ObRawExpr *, 4> exec_raw_params;
    for (int64_t i = 0; i < params.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(add_var_to_array_no_dup(exec_raw_params, params.at(i)->get_ref_expr()))) {
        LOG_WARN("fail to push_back expr to exec_raw_params", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!exec_raw_params.empty()) {
      if (OB_FAIL(get_grouping_style_exchange_info(exec_raw_params, top->get_output_equal_sets(),
                                                   exch_info))) {
        LOG_WARN("fail get spf hash shuffle exchange info", K(ret));
      }
    } else {
      // Subplan filter must should have exec params when use hash shuffle
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Subplan filter must should have exec params when use hash shuffle!", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::init_subplan_filter_child_ops(const ObIArray<ObLogicalOperator*> &subquery_ops,
                                             const ObIArray<std::pair<int64_t, ObRawExpr*>> &params,
                                             ObIArray<ObLogicalOperator*> &dist_subquery_ops)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  for (int64 i = 0; OB_SUCC(ret) && i < subquery_ops.count(); i++) {
    bool need_rescan = false;
    ObLogicalOperator *child = NULL;
    if (OB_ISNULL(child = subquery_ops.at(i)) || OB_ISNULL(child->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (child->is_distributed() &&
               OB_FAIL(allocate_exchange_as_top(child, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (params.empty() && child->get_stmt()->is_contains_assignment() &&
               OB_FAIL(child->check_exchange_rescan(need_rescan))) {
      LOG_WARN("failed to check exchange rescan", K(ret));
    } else if (need_rescan && OB_FAIL(allocate_material_as_top(child))) {
      LOG_WARN("failed to allocate material as top", K(ret));
    } else if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(dist_subquery_ops.push_back(child))) {
      LOG_WARN("failed to push back op", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::check_if_match_none_all(ObLogicalOperator *top,
                                       const ObIArray<ObLogicalOperator*> &child_ops,
                                       bool &is_none_all)
{
  int ret = OB_SUCCESS;
  is_none_all = true;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid top", K(ret));
  } else if (!top->is_distributed()) {
    is_none_all = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_none_all && i < child_ops.count(); i ++) {
      ObLogicalOperator *child = child_ops.at(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child", K(ret));
      } else if (!child->is_match_all()) {
        is_none_all = false;
      }
    }
  }
  return ret;
}

int ObLogPlan::check_if_subplan_filter_match_partition_wise(ObLogicalOperator *top,
                                                            const ObIArray<ObLogicalOperator*> &subquery_ops,
                                                            const ObIArray<ObExecParamRawExpr *> &params,
                                                            bool &is_partition_wise)
{
  int ret = OB_SUCCESS;
  EqualSets input_esets;
  is_partition_wise = false;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!top->is_distributed()) {
    is_partition_wise = false;
  } else if (OB_FAIL(append(input_esets, top->get_output_equal_sets()))) {
    LOG_WARN("failed to append equal sets", K(ret));
  } else {
    ObLogicalOperator *child = NULL;
    ObSEArray<ObRawExpr*, 4> left_keys;
    ObSEArray<ObRawExpr*, 4> right_keys;
    ObSEArray<bool, 4> null_safe_info;
    is_partition_wise = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_ops.count(); i++) {
      if (OB_ISNULL(child = subquery_ops.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(append(input_esets, child->get_output_equal_sets()))) {
        LOG_WARN("failed to append input equal sets", K(ret));
      } else { /*do nothing*/ }
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_partition_wise && i < subquery_ops.count(); i++) {
      left_keys.reuse();
      right_keys.reuse();
      null_safe_info.reuse();
      if (OB_ISNULL(child = subquery_ops.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!child->is_distributed() || child->is_exchange_allocated()) {
        is_partition_wise = false;
      } else if (OB_FAIL(get_subplan_filter_equal_keys(child,
                                                       params,
                                                       left_keys,
                                                       right_keys,
                                                       null_safe_info))) {
        LOG_WARN("failed to get equal join key", K(ret));
      } else if (OB_FAIL(ObShardingInfo::check_if_match_partition_wise(
                                        input_esets,
                                        left_keys,
                                        right_keys,
                                        null_safe_info,
                                        top->get_strong_sharding(),
                                        top->get_weak_sharding(),
                                        child->get_strong_sharding(),
                                        child->get_weak_sharding(),
                                        is_partition_wise))) {
        LOG_WARN("failed to check match partition wise join", K(ret));
      } else { /*do nothing*/}
    }
  }
  return ret;
}

int ObLogPlan::check_if_subplan_filter_match_repart(ObLogicalOperator *top,
                                                   const ObIArray<ObLogicalOperator*> &subquery_ops,
                                                   const ObIArray<ObExecParamRawExpr *> &params,
                                                   bool &is_match_repart)
{
  int ret = OB_SUCCESS;
  EqualSets input_esets;
  bool is_partition_wise = false;
  is_match_repart = false;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (top->is_match_all()) {
    is_match_repart = false;
  } else if (OB_FAIL(append(input_esets, top->get_output_equal_sets()))) {
    LOG_WARN("failed to append equal sets", K(ret));
  } else {
    ObLogicalOperator *child = NULL;
    ObLogicalOperator *pre_child = NULL;
    ObSEArray<ObRawExpr *, 4> left_keys;
    ObSEArray<ObRawExpr *, 4> right_keys;
    ObSEArray<ObRawExpr *, 4> pre_left_keys;
    ObSEArray<ObRawExpr *, 4> pre_right_keys;
    ObSEArray<ObRawExpr*, 4> target_part_keys;
    ObSEArray<bool, 4> null_safe_info;
    is_match_repart = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_ops.count(); i++) {
      if (OB_ISNULL(child = subquery_ops.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(append(input_esets, child->get_output_equal_sets()))) {
        LOG_WARN("failed to append input equal sets", K(ret));
      } else { /*do nothing*/ }
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_match_repart && i < subquery_ops.count(); ++i) {
      left_keys.reuse();
      right_keys.reuse();
      null_safe_info.reuse();
      target_part_keys.reuse();
      if (OB_ISNULL(child = subquery_ops.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!child->is_distributed() || child->is_exchange_allocated() || OB_ISNULL(child->get_strong_sharding())) {
        is_match_repart = false;
      } else if (OB_FAIL(get_subplan_filter_equal_keys(child,
                                                       params,
                                                       left_keys,
                                                       right_keys,
                                                       null_safe_info))) {
        LOG_WARN("failed to get equal join key", K(ret));
      } else if (OB_FAIL(child->get_strong_sharding()->get_all_partition_keys(target_part_keys, true))) {
        LOG_WARN("failed to get partition keys", K(ret));
      } else if (OB_FAIL(ObShardingInfo::check_if_match_repart_or_rehash(input_esets,
                                                                          left_keys,
                                                                          right_keys,
                                                                          target_part_keys,
                                                                          is_match_repart))) {
        LOG_WARN("failed to check if match repartition", K(ret));
      } else if (!is_match_repart) {
        //do nothing
      } else if (i < 1) {
        if (OB_FAIL(pre_left_keys.assign(left_keys))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else if (OB_FAIL(pre_right_keys.assign(right_keys))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else {
          pre_child = child;
        }
      } else if (!ObOptimizerUtil::is_exprs_equivalent(left_keys,
                                                       pre_left_keys,
                                                       input_esets)) {
        is_match_repart = false;
      } else if(OB_ISNULL(pre_child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObShardingInfo::check_if_match_partition_wise(
                                        input_esets,
                                        pre_right_keys,
                                        right_keys,
                                        pre_child->get_strong_sharding(),
                                        child->get_strong_sharding(),
                                        is_partition_wise))) {
        LOG_WARN("failed to check match partition wise join", K(ret));
      } else {
        is_match_repart = is_partition_wise;
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("succeed to check subplan filter matchs repart", K(is_match_repart));
    }
  }
  return ret;
}

int ObLogPlan::get_subplan_filter_equal_keys(ObLogicalOperator *child,
                                             const ObIArray<ObExecParamRawExpr *> &params,
                                             ObIArray<ObRawExpr *> &left_keys,
                                             ObIArray<ObRawExpr *> &right_keys,
                                             ObIArray<bool> &null_safe_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_subplan_filter_normal_equal_keys(child,
                                                          left_keys,
                                                          right_keys,
                                                          null_safe_info))) {
    LOG_WARN("failed to get normal equal key", K(ret));
  } else if (!params.empty() &&
             OB_FAIL(get_subplan_filter_correlated_equal_keys(child, params,
                                                              left_keys, right_keys,
                                                              null_safe_info))) {
    LOG_WARN("failed to get correlated equal keys", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::get_subplan_filter_normal_equal_keys(const ObLogicalOperator *child,
                                                    ObIArray<ObRawExpr *> &left_keys,
                                                    ObIArray<ObRawExpr *> &right_keys,
                                                    ObIArray<bool> &null_safe_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child) || OB_ISNULL(child->get_stmt()) ||
      OB_UNLIKELY(!child->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else {
    //首先在filter里寻找
    for (int64_t i = 0; OB_SUCC(ret) && i < child->get_filter_exprs().count(); ++i) {
      ObRawExpr *expr = child->get_filter_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (T_OP_SQ_EQ == expr->get_expr_type()
                 || T_OP_SQ_NSEQ == expr->get_expr_type()
                 || T_OP_EQ == expr->get_expr_type()
                 || T_OP_NSEQ == expr->get_expr_type()) {
        ObRawExpr *left_hand = NULL;
        ObRawExpr *right_hand = NULL;
        bool is_null_safe = (T_OP_SQ_NSEQ == expr->get_expr_type() ||
                             T_OP_NSEQ == expr->get_expr_type());
        ObSelectStmt *right_stmt = NULL;
        if (OB_ISNULL(left_hand = expr->get_param_expr(0))
            || OB_ISNULL(right_hand = expr->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is invalid", K(ret), K(left_hand), K(right_hand));
        } else if (!right_hand->is_query_ref_expr()) {
          // do nothing
        } else if (OB_FALSE_IT(right_stmt = static_cast<ObQueryRefRawExpr *>(
                                            right_hand)->get_ref_stmt())) {
        } else if (child->get_plan()->get_stmt() == right_stmt) {
          // do nothing
        } else if (T_OP_ROW == left_hand->get_expr_type()) { //向量
          ObOpRawExpr *row_expr = static_cast<ObOpRawExpr *>(left_hand);
          if (row_expr->get_param_count() != right_stmt->get_select_item_size()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr size does not match",
                     K(ret), K(*row_expr), K(right_stmt->get_select_items()));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < row_expr->get_param_count(); ++j) {
              if (OB_FAIL(left_keys.push_back(row_expr->get_param_expr(j)))
                  || OB_FAIL(right_keys.push_back(right_stmt->get_select_item(j).expr_))
                  || OB_FAIL(null_safe_info.push_back(is_null_safe))) {
                LOG_WARN("push back error", K(ret));
              } else { /* Do nothing */ }
            }
          }
        } else { //单expr
          if (1 != right_stmt->get_select_item_size()) {
            LOG_WARN("select item size should be 1",
                     K(ret), K(right_stmt->get_select_item_size()));
          } else if (OB_FAIL(left_keys.push_back(left_hand))
                     || OB_FAIL(right_keys.push_back(right_stmt->get_select_item(0).expr_))
                     || OB_FAIL(null_safe_info.push_back(is_null_safe))) {
            LOG_WARN("push back error", K(ret));
          } else { /* Do nothing */ }
        }
      } else { /* Do nothing */ }
    }
  }
  return ret;
}

int ObLogPlan::get_subplan_filter_correlated_equal_keys(const ObLogicalOperator *top,
                                                        const ObIArray<ObExecParamRawExpr *> &params,
                                                        ObIArray<ObRawExpr *> &left_keys,
                                                        ObIArray<ObRawExpr *> &right_keys,
                                                        ObIArray<bool> &null_safe_info)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  ObSEArray<ObRawExpr *, 8> filters;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_FAIL(append(filters, top->get_filter_exprs()))) {
    LOG_WARN("failed to append filter exprs", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN == top->get_type()) {
    const ObLogTableScan *table_scan = static_cast<const ObLogTableScan *>(top);
    if (NULL != table_scan->get_pre_graph() &&
        OB_FAIL(append(filters, table_scan->get_pre_graph()->get_range_exprs()))) {
      LOG_WARN("failed to append conditions", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::extract_equal_exec_params(filters,
                                                                params,
                                                                left_keys,
                                                                right_keys,
                                                                null_safe_info))) {
    LOG_WARN("failed to extract equal exec params", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < top->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(get_subplan_filter_correlated_equal_keys(top->get_child(i),
                                                                      params,
                                                                      left_keys,
                                                                      right_keys,
                                                                      null_safe_info)))) {
        LOG_WARN("extract_correlated_keys error", K(ret));
      } else { /* do nothing */ }
    }
  }
  return ret;
}

/*
 * 在当前算子上层分配subplan filter算子，输入的表达式都是包含子查询的表达式，
 * 其中is_filter 表示当前输入的子查询表达式是不是过滤条件，因为子查询可能出现在select语句的各个子句中，
 * 其中只有出现在where子句和having子句中的子查询是filter。
 * 分配where子句，having子句和select子句中的子查询会直接调用该函数进行算子分配
 */
int ObLogPlan::allocate_subplan_filter_as_top(ObLogicalOperator *&top_node,
                                              const ObIArray<ObRawExpr*> &subquery_exprs,
                                              const bool is_filter,
                                              const bool for_on_condition)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(top_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(make_candidate_plans(top_node))) {
    LOG_WARN("failed to make candidate plans", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter(subquery_exprs,
                                                   is_filter ? &subquery_exprs : NULL,
                                                   false,
                                                   for_on_condition))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (OB_FAIL(candidates_.get_best_plan(top_node))) { // only get best plan, not optimizer
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(top_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::allocate_subplan_filter_as_top(ObLogicalOperator *&top,
                                              const ObIArray<ObLogicalOperator*> &subquery_ops,
                                              const ObIArray<ObQueryRefRawExpr *> &query_ref_exprs,
                                              const ObIArray<ObExecParamRawExpr *> &params,
                                              const ObIArray<ObExecParamRawExpr *> &onetime_exprs,
                                              const ObBitSet<> &initplan_idxs,
                                              const ObBitSet<> &onetime_idxs,
                                              const ObIArray<ObRawExpr*> &filters,
                                              const DistAlgo dist_algo,
                                              const bool is_update_set)
{
  int ret = OB_SUCCESS;
  ObLogSubPlanFilter *spf_node = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(ret));
  } else if (OB_ISNULL(spf_node = static_cast<ObLogSubPlanFilter*>(
                       get_log_op_factory().allocate(*this, LOG_SUBPLAN_FILTER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(spf_node->add_child(top))) {
    LOG_WARN("failed to add child", K(ret));
  } else if (OB_FAIL(spf_node->add_child(subquery_ops))) {
    LOG_WARN("failed to add child", K(ret));
  } else {
    spf_node->set_distributed_algo(dist_algo);
    spf_node->set_update_set(is_update_set);
    if (OB_FAIL(append(spf_node->get_filter_exprs(), filters))) {
      LOG_WARN("failed to append filter exprs", K(ret));
    } else if (OB_FAIL(spf_node->add_subquery_exprs(query_ref_exprs))) {
      LOG_WARN("failed to add subquery exprs", K(ret));
    } else if (OB_FAIL(spf_node->add_exec_params(params))) {
      LOG_WARN("failed to add exec params", K(ret));
    } else if (OB_FAIL(spf_node->add_onetime_exprs(onetime_exprs))) {
      LOG_WARN("failed to add onetime exprs", K(ret));
    } else if (OB_FAIL(spf_node->add_initplan_idxs(initplan_idxs))) {
      LOG_WARN("failed to add init plan idxs", K(ret));
    } else if (OB_FAIL(spf_node->add_onetime_idxs(onetime_idxs))) {
      LOG_WARN("failed to add onetime idxs", K(ret));
    } else if (OB_FAIL(spf_node->compute_spf_batch_rescan())) {
      LOG_WARN("failed to compute spf batch rescan", K(ret));
    } else if (OB_FAIL(spf_node->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = spf_node;
    }
  }
  return ret;
}

int ObLogPlan::allocate_subplan_filter_for_on_condition(ObIArray<ObRawExpr*> &subquery_exprs, ObLogicalOperator* &top)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> pushdown_subquery;
  ObSEArray<ObRawExpr*, 4> none_pushdown_subquery;
  //下推的subplan filter不需要重新计算选择率
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); ++i) {
    ObRawExpr* expr = subquery_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(none_pushdown_subquery.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (OB_FAIL(pushdown_subquery.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (!pushdown_subquery.empty() &&
              OB_FAIL(allocate_subplan_filter_as_top(top,
                                                    pushdown_subquery,
                                                    false,
                                                    true))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (!none_pushdown_subquery.empty() &&
              OB_FAIL(allocate_subplan_filter_as_top(top,
                                                    none_pushdown_subquery,
                                                    true,
                                                    true))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::candi_allocate_filter(const ObIArray<ObRawExpr*> &filter_exprs)
{
  int ret = OB_SUCCESS;
  double sel = 1.0;
  ObLogicalOperator *best_plan = NULL;
  EqualSets equal_sets;
  if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(best_plan->get_input_equal_sets(equal_sets))) {
    LOG_WARN("failed to get input equal sets", K(ret));
  } else if (OB_FALSE_IT(get_selectivity_ctx().init_op_ctx(best_plan))) {
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_update_table_metas(),
                                                             get_selectivity_ctx(),
                                                             filter_exprs,
                                                             sel,
                                                             get_predicate_selectivities()))) {
    LOG_WARN("failed to calc selectivity", K(ret));
  } else if (OB_FALSE_IT(get_selectivity_ctx().clear())) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
      ObLogicalOperator *top = NULL;
      if (OB_ISNULL(top = candidates_.candidate_plans_.at(i).plan_tree_)) {
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


int ObLogPlan::plan_tree_traverse(const TraverseOp &operation, void *ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(get_plan_root()) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_stmt()), K(get_plan_root()));
  } else {
    NumberingCtx numbering_ctx;                      // operator numbering context
    NumberingExchangeCtx numbering_exchange_ctx;     // operator numbering context
    ObAllocExprContext alloc_expr_ctx;               // expr allocation context
    uint64_t hash_seed =  0;                         // seed for plan signature
    ObBitSet<256> output_deps;                       // output expr dependencies
    AllocGIContext gi_ctx;
    ObPxPipeBlockingCtx pipe_block_ctx(get_allocator());
    ObLocationConstraintContext location_constraints;
    AllocMDContext md_ctx;
    AllocBloomFilterContext bf_ctx;
    SMART_VAR(ObBatchExecParamCtx, batch_exec_param_ctx) {
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
        if (get_stmt()->is_insert_stmt() &&
            !static_cast<const ObInsertStmt*>(get_stmt())->value_from_select()) {
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
      case RUNTIME_FILTER: {
        ctx = &bf_ctx;
        break;
      }
      case PROJECT_PRUNING: {
        ctx = &output_deps;
        break;
      }
      case ALLOC_EXPR: {
        if (OB_FAIL(set_use_batch_for_table_scan(get_plan_root(), true, false))) {
          LOG_WARN("failed to set use batch for table scan", K(ret));
        } else if (OB_FAIL(alloc_expr_ctx.flattern_expr_map_.create(128, "ExprAlloc"))) {
          LOG_WARN("failed to init hash map", K(ret));
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
      case COLLECT_BATCH_EXEC_PARAM: {
        ctx = &batch_exec_param_ctx;
        break;
      }
      case ALLOC_STARTUP_EXPR:
      default:
        break;
      }
      if (OB_SUCC(ret)) {
        if (((PX_ESTIMATE_SIZE == operation) ||
            (PX_PIPE_BLOCKING == operation) ||
            (PX_RESCAN == operation)) &&
            (get_optimizer_context().is_local_or_remote_plan())) {
          /*do nothing*/
        } else if (ALLOC_GI == operation &&
                  get_optimizer_context().is_local_or_remote_plan() &&
                  !(gi_ctx.is_valid_for_gi_ &&
                    get_optimizer_context().enable_batch_rpc())) {
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
              hash_value_ = *static_cast<uint64_t *>(ctx);
              LOG_TRACE("succ to generate plan hash value", "hash_value", hash_value_);
            }
          } else if (GEN_LOCATION_CONSTRAINT == operation) {
            ObSqlCtx *sql_ctx = NULL;
            if (OB_ISNULL(optimizer_context_.get_exec_ctx())
                || OB_ISNULL(sql_ctx = optimizer_context_.get_exec_ctx()->get_sql_ctx())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret), K(optimizer_context_.get_exec_ctx()), K(sql_ctx));
            } else if (OB_FAIL(remove_duplicate_constraint(location_constraints,
                                                          *sql_ctx))) {
              LOG_WARN("fail to remove duplicate constraint", K(ret));
            } else if (OB_FAIL(calc_and_set_exec_pwj_map(location_constraints))) {
              LOG_WARN("failed to calc and set exec pwj map", K(ret));
            }
          } else if (OPERATOR_NUMBERING == operation) {
            NumberingCtx *num_ctx = static_cast<NumberingCtx *>(ctx);
            max_op_id_ = num_ctx->op_id_;
            LOG_TRACE("trace max operator id", K(max_op_id_), K(this));
          } else { /* Do nothing */ }
          LOG_TRACE("succ to apply operaion to operator", K(operation), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::init_onetime_subquery_info()
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> func_table_exprs;
  ObArray<ObRawExpr *> json_table_exprs;
  void *ptr = NULL;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_ISNULL(ptr = get_allocator().alloc(sizeof(ObRawExprCopier)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    onetime_copier_ = new (ptr) ObRawExprCopier(get_optimizer_context().get_expr_factory());
  }

  if (OB_SUCC(ret) && stmt_->get_subquery_expr_size() > 0) {
    ObSEArray<ObRawExpr *, 4> exprs;
    if (OB_FAIL(stmt_->get_relation_exprs(exprs))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    } else if (OB_FAIL(stmt_->get_table_function_exprs(func_table_exprs))) {
      LOG_WARN("failed to get table function exprs", K(ret));
    } else if (OB_FAIL(stmt_->get_json_table_exprs(json_table_exprs))) {
      LOG_WARN("failed to get json table exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      bool dummy = false;
      bool dummy_shared = false;
      ObRawExpr *expr = exprs.at(i);
      ObSEArray<ObRawExpr *, 4> onetime_list;
      ObSEArray<ObQueryRefRawExpr *, 4> queryref_list;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (ObOptimizerUtil::find_item(func_table_exprs, expr)) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(json_table_exprs, expr)) {
        // do nothing
      } else if (OB_FAIL(extract_onetime_subquery(expr, onetime_list, dummy, dummy_shared))) {
        LOG_WARN("failed to extract onetime subquery", K(ret));
      } else if (onetime_list.empty()) {
        // do nothing
      } else if (OB_FAIL(create_onetime_param(expr, onetime_list))) {
        LOG_WARN("failed to create onetime param expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(onetime_list,
                                                                  queryref_list,
                                                                  false))) {
        LOG_WARN("failed to extract query ref exprs", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < queryref_list.count(); ++j) {
        SubPlanInfo *info = NULL;
        ObQueryRefRawExpr *onetime_queryref_expr = queryref_list.at(j);
        if (OB_ISNULL(onetime_queryref_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected onetime expr", K(ret), KPC(onetime_queryref_expr));
        } else if (OB_FAIL(get_subplan(onetime_queryref_expr, info))) {
          LOG_WARN("failed to get subplan", K(ret));
        } else if (NULL != info) {
          // do nothing
        } else if (OB_FAIL(generate_subplan_for_query_ref(onetime_queryref_expr, info))) {
          LOG_WARN("failed to generate subplan for query ref", K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief ObLogPlan::extract_onetime_subquery
 * @param expr
 * @param onetime_list
 * @param is_valid: if a expr is invalid,
 *                  its parent is also invalid while its children can be valid
 * @return
 */
int ObLogPlan::extract_onetime_subquery(ObRawExpr *expr,
                                        ObIArray<ObRawExpr *> &onetime_list,
                                        bool &is_valid,
                                        bool &has_shared_subquery)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    // if a expr contain psedu column, hierachical expr, any column
    is_valid = !ObOptimizerUtil::has_psedu_column(*expr)
              && !ObOptimizerUtil::has_hierarchical_expr(*expr)
              && !expr->has_flag(CNT_COLUMN)
              && !expr->has_flag(CNT_AGG)
              && !expr->has_flag(CNT_WINDOW_FUNC)
              && !expr->has_flag(CNT_ALIAS)
              && !expr->has_flag(CNT_SET_OP);
  }

  if (OB_SUCC(ret) && expr->is_query_ref_expr()) {
    bool has_ref_assign_user_var = false;
    if (!is_valid) {
      // do nothing
    } else if (expr->get_param_count() > 0) {
      is_valid = false;
    } else if (OB_FAIL(ObOptimizerUtil::check_subquery_has_ref_assign_user_var(
                         expr, has_ref_assign_user_var))) {
      LOG_WARN("failed to check subquery has ref assign user var", K(ret));
    } else if (has_ref_assign_user_var) {
      is_valid = false;
    } else if (static_cast<ObQueryRefRawExpr *>(expr)->is_scalar()) {
      if (OB_FAIL(onetime_list.push_back(expr))) {
        LOG_WARN("failed to push back candi onetime expr", K(ret));
      } else if (expr->is_explicited_reference() && expr->get_ref_count() > 1) {
        has_shared_subquery = true;
      }
    }
  }
  // if query_ref has an exec_param, then will also has a CNT_SUB_QUERY flag
  if (OB_SUCC(ret) && expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      bool is_param_valid = false;
      bool has_child_shared_subquery = false;
      if (OB_FAIL(extract_onetime_subquery(expr->get_param_expr(i),
                                           onetime_list,
                                           is_param_valid,
                                           has_child_shared_subquery))) {
        LOG_WARN("failed to extract onetime subquery", K(ret));
      } else if (!is_param_valid) {
        is_valid = false;
      }
      has_shared_subquery |= has_child_shared_subquery;
    }
    if (OB_SUCC(ret) && is_valid && !has_shared_subquery &&
                        (T_OP_EXISTS == expr->get_expr_type()
                         || T_OP_NOT_EXISTS == expr->get_expr_type()
                         || expr->has_flag(IS_WITH_ALL)
                         || expr->has_flag(IS_WITH_ANY))) {
      if (OB_FAIL(onetime_list.push_back(expr))) {
        LOG_WARN("failed to push back candi onetime exprs", K(ret));
      } else if (expr->is_explicited_reference() && expr->get_ref_count() > 1) {
        has_shared_subquery = true;
      }
    }
  }
  return ret;
}

int ObLogPlan::create_onetime_param(ObRawExpr *expr,
                                    const ObIArray<ObRawExpr *> &onetime_list)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(stmt_) || OB_ISNULL(expr) ||
      OB_ISNULL(query_ctx = get_optimizer_context().get_query_ctx()) ||
      OB_ISNULL(onetime_copier_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (ObOptimizerUtil::find_item(onetime_query_refs_, expr)) {
    // do nothing
  } else if (ObOptimizerUtil::find_item(onetime_list, expr)) {
    ObRawExpr *new_expr = expr;
    ObExecParamRawExpr *exec_param = NULL;
    if (OB_FAIL(ObRawExprUtils::create_new_exec_param(query_ctx,
                                                      get_optimizer_context().get_expr_factory(),
                                                      new_expr,
                                                      true))) {
      LOG_WARN("failed to create new exec param", K(ret));
    } else if (OB_ISNULL(exec_param = static_cast<ObExecParamRawExpr*>(new_expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new exec param is null", KPC(new_expr), K(ret));
    } else if (OB_FAIL(exec_param->formalize(get_optimizer_context().get_session_info()))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(onetime_query_refs_.push_back(expr))) {
      LOG_WARN("failed to add old expr", K(ret));
    } else if (OB_FAIL(onetime_params_.push_back(exec_param))) {
      LOG_WARN("failed to push back new expr", K(ret));
    } else if (OB_FAIL(onetime_copier_->add_replaced_expr(expr, exec_param))) {
      LOG_WARN("failed to add replace expr", K(ret));
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(create_onetime_param(expr->get_param_expr(i), onetime_list)))) {
        LOG_WARN("failed to create onetime param", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::extract_onetime_exprs(ObRawExpr *expr,
                                     ObIArray<ObExecParamRawExpr *> &onetime_exprs,
                                     ObIArray<ObQueryRefRawExpr *> &onetime_query_refs,
                                     const bool for_on_condition)
{
  int ret = OB_SUCCESS;
  ObExecParamRawExpr *exec_param = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret), K(expr));
  } else if (expr->is_exec_param_expr() && expr->has_flag(IS_ONETIME)) {
    exec_param = static_cast<ObExecParamRawExpr*>(expr);
  } else {
    int64_t idx = -1;
    if (!ObOptimizerUtil::find_item(onetime_query_refs_, expr, &idx)) {
      // do nothing
    } else if (OB_UNLIKELY(idx < 0 || idx > onetime_params_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("onetime expr count mismatch", K(ret));
    } else if (OB_ISNULL(exec_param = onetime_params_.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("onetime expr is null", K(exec_param));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (NULL != exec_param) {
    bool has_exists = ObOptimizerUtil::find_item(get_onetime_exprs(), exec_param);
    if (!for_on_condition && has_exists) {
      // another one has created the onetime
    } else if (!has_exists &&
               OB_FAIL(get_onetime_exprs().push_back(exec_param))) {
      LOG_WARN("failed to append onetime expr", K(ret));
    } else if (OB_FAIL(onetime_exprs.push_back(exec_param))) {
      LOG_WARN("failed to append array no dup", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(exec_param->get_ref_expr(),
                                                                onetime_query_refs,
                                                                false))) {
      LOG_WARN("failed to extract query ref expr", K(ret));
    }
  } else if (expr->has_flag(CNT_ONETIME) || expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_onetime_exprs(expr->get_param_expr(i),
                                                   onetime_exprs,
                                                   onetime_query_refs,
                                                   for_on_condition)))) {
        LOG_WARN("failed to extract onetime exprs", K(ret));
      }
    }
  }
  return ret;
}

double ObLogPlan::get_expr_selectivity(const ObRawExpr *expr, bool &found)
{
  double sel = 1.0;
  found = false;
  if (OB_ISNULL(expr)) {
  } else {
    for (int64_t i = 0; !found && i < pred_sels_.count(); ++i) {
      if (expr == pred_sels_.at(i).expr_) {
        found = true;
        sel = pred_sels_.at(i).sel_;
      }
    }
  }
  return sel;
}

int ObLogPlan::candi_allocate_count()
{
  int ret = OB_SUCCESS;
  ObRawExpr *limit_expr = NULL;
  ObRawExpr *rownum_expr = NULL;
  ObSEArray<ObRawExpr*, 4> filter_exprs;
  ObSEArray<ObRawExpr*, 4> start_exprs;
  ObSEArray<ObRawExpr*, 4> subquery_exprs;
  OPT_TRACE_TITLE("start generate count operator");
  if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(get_rownum_exprs(),
                                                  subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (!subquery_exprs.empty() &&
             OB_FAIL(candi_allocate_subplan_filter(subquery_exprs))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (OB_FAIL(classify_rownum_exprs(get_rownum_exprs(),
                                           filter_exprs,
                                           start_exprs,
                                           limit_expr))) {
    LOG_WARN("failed to classify rownum exprs", K(ret));
  } else if (OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected null stmt", K(ret), K(get_stmt()));
  } else if (OB_FAIL(get_stmt()->get_rownum_expr(rownum_expr))) {
    LOG_WARN("get rownum expr failed", K(ret));
  } else {
    CandidatePlan candidate_plan;
    ObSEArray<CandidatePlan, 4> rownum_plans;
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
      candidate_plan = candidates_.candidate_plans_.at(i);
      OPT_TRACE("generate count for plan:", candidate_plan);
      if (OB_FAIL(create_rownum_plan(candidate_plan.plan_tree_,
                                     filter_exprs,
                                     start_exprs,
                                     limit_expr,
                                     rownum_expr))) {
        LOG_WARN("failed to allocate count operator", K(ret));
      } else if (OB_FAIL(rownum_plans.push_back(candidate_plan))) {
        LOG_WARN("failed to push back plan", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prune_and_keep_best_plans(rownum_plans))) {
        LOG_WARN("failed to prune and keep best plans", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::create_rownum_plan(ObLogicalOperator *&top,
                                  const ObIArray<ObRawExpr*> &filter_exprs,
                                  const ObIArray<ObRawExpr*> &start_exprs,
                                  ObRawExpr *limit_expr,
                                  ObRawExpr *rownum_expr)
{
  int ret = OB_SUCCESS;
  bool is_pushed = false;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (NULL != limit_expr && filter_exprs.empty() && !limit_expr->has_flag(CNT_DYNAMIC_PARAM) &&
             OB_FAIL(try_push_limit_into_table_scan(top, limit_expr, limit_expr, NULL, is_pushed))) {
    LOG_WARN("failed to push limit into table scan", K(ret));
  } else if (top->is_distributed() && NULL != limit_expr &&
             OB_FAIL(allocate_limit_as_top(top, limit_expr, NULL, NULL, false, false, false))) {
    LOG_WARN("failed to allocate limit as top", K(ret));
  } else if (top->is_distributed() &&
             OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
    LOG_WARN("failed to allocate exchange as top", K(ret));
  } else if (OB_FAIL(allocate_count_as_top(top,
                                           filter_exprs,
                                           start_exprs,
                                           limit_expr,
                                           rownum_expr))) {
    LOG_WARN("failed to allocate count as top", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::allocate_count_as_top(ObLogicalOperator *&top,
                                     const ObIArray<ObRawExpr*> &filter_exprs,
                                     const ObIArray<ObRawExpr*> &start_exprs,
                                     ObRawExpr *limit_expr,
                                     ObRawExpr *rownum_expr)
{
  int ret = OB_SUCCESS;
  ObLogCount *count = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected null stmt", K(ret), K(top), K(get_stmt()));
  } else if (OB_ISNULL(count = static_cast<ObLogCount *>(get_log_op_factory().allocate(*this, LOG_COUNT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory for ObLogCount Failed", K(ret));
  } else if (OB_FAIL(append(count->get_filter_exprs(), filter_exprs))) {
    LOG_WARN("failed to append filter exprs", K(ret));
  } else if (OB_FAIL(append(count->get_startup_exprs(), start_exprs))) {
    LOG_WARN("failed to append start up exprs", K(ret));
  } else {
    count->set_rownum_limit_expr(limit_expr);
    count->set_rownum_expr(rownum_expr);
    count->set_child(ObLogicalOperator::first_child, top);
    if (OB_FAIL(count->compute_property())) {
      LOG_WARN("failed to compute property");
    } else {
      top = count;
    }
  }
  return ret;
}

int ObLogPlan::classify_rownum_exprs(const ObIArray<ObRawExpr*> &rownum_exprs,
                                     ObIArray<ObRawExpr*> &filter_exprs,
                                     ObIArray<ObRawExpr*> &start_exprs,
                                     ObRawExpr *&limit_expr)
{
  int ret = OB_SUCCESS;
  ObItemType limit_rownum_type = T_INVALID;
  limit_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < rownum_exprs.count(); i++) {
    ObRawExpr *rownum_expr = rownum_exprs.at(i);
    ObRawExpr *const_expr = NULL;
    ObItemType expr_type = T_INVALID;
    bool dummy_flag = false;
    if (OB_FAIL(ObOptimizerUtil::get_rownum_filter_info(
                  rownum_expr, expr_type, const_expr, dummy_flag))) {
      LOG_WARN("failed to check is rownum expr used as filter", K(ret));
    } else if (OB_FAIL(classify_rownum_expr(expr_type, rownum_expr,
                                            const_expr, filter_exprs,
                                            start_exprs, limit_expr))) {
      LOG_WARN("failed to classify rownum expr", K(ret));
    } else if (const_expr == limit_expr && T_INVALID == limit_rownum_type) {
      limit_rownum_type = expr_type;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(limit_expr)) {
    ObRawExpr *limit_int_expr = NULL;
    if (OB_FAIL(ObOptimizerUtil::convert_rownum_filter_as_limit(
                  get_optimizer_context().get_expr_factory(),
                  get_optimizer_context().get_session_info(),
                  limit_rownum_type, limit_expr, limit_int_expr))) {
      LOG_WARN("failed to transform rownum filter as limit", K(ret));
    } else {
      limit_expr = limit_int_expr;
    }
  }
  return ret;
}

int ObLogPlan::classify_rownum_expr(const ObItemType expr_type,
                                    ObRawExpr *rownum_expr,
                                    ObRawExpr *left_const_expr,
                                    common::ObIArray<ObRawExpr*> &filter_exprs,
                                    common::ObIArray<ObRawExpr*> &start_exprs,
                                    ObRawExpr *&limit_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rownum_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(rownum_expr), K(ret));
  } else if ((expr_type == T_OP_LE || expr_type == T_OP_LT)
             && NULL == limit_expr && NULL != left_const_expr) {
    limit_expr = left_const_expr;
  } else if (expr_type == T_OP_GE || expr_type == T_OP_GT) {
    if (OB_FAIL(start_exprs.push_back(rownum_expr))) {
      LOG_WARN("failed to push back rownum expr", K(ret));
    } else { /*do nothing*/ }
  } else {
    if (OB_FAIL(filter_exprs.push_back(rownum_expr))) {
      LOG_WARN("failed to push back rownum expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::update_plans_interesting_order_info(ObIArray<CandidatePlan> &candidate_plans,
                                                   const int64_t check_scope)
{
  int ret = OB_SUCCESS;
  int64_t match_info = OrderingFlag::NOT_MATCH;
  if (check_scope == OrderingCheckScope::NOT_CHECK) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < candidate_plans.count(); i++) {
      CandidatePlan &candidate_plan = candidate_plans.at(i);
      if (OB_ISNULL(candidate_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::compute_stmt_interesting_order(
          candidate_plan.plan_tree_->get_op_ordering(),
          get_stmt(),
          get_is_subplan_scan(),
          get_equal_sets(),
          get_const_exprs(),
          get_is_parent_set_distinct(),
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

int ObLogPlan::prune_and_keep_best_plans(ObIArray<CandidatePlan> &all_candidate_plans)
{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 8> candidate_plans;
  ObSEArray<CandidatePlan, 8> best_plans;
  OPT_TRACE_TITLE("prune and keep best plans");
  if (OB_FAIL(remove_match_all_fake_cte_plan(all_candidate_plans, candidate_plans))) {
    LOG_WARN("failed to remove match all fake cte plan", K(ret));
  } else if (OB_UNLIKELY(get_optimizer_context().generate_random_plan())) {
    ObQueryCtx* query_ctx = get_optimizer_context().get_query_ctx();
    for (int64_t i = 0; OB_SUCC(ret) && i < candidate_plans.count(); i++) {
      bool random_flag = !OB_ISNULL(query_ctx) && query_ctx->rand_gen_.get(0, 1) == 1;
      if (random_flag && OB_FAIL(best_plans.push_back(candidate_plans.at(i)))) {
        LOG_WARN("failed to push back random candi plan", K(ret));
      }
    }
    if (OB_SUCC(ret) && best_plans.empty() && !candidate_plans.empty()) {
      if (OB_FAIL(best_plans.push_back(candidate_plans.at(0)))) {
        LOG_WARN("failed to push back random candi plan", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < candidate_plans.count(); i++) {
      CandidatePlan &candidate_plan = candidate_plans.at(i);
      if (OB_FAIL(add_candidate_plan(best_plans, candidate_plan))) {
        LOG_WARN("failed to add candidate plan", K(ret));
      } else { /*do nothing*/ }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_candidate_plans(best_plans))) {
      LOG_WARN("failed to do candi init", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::remove_match_all_fake_cte_plan(ObIArray<CandidatePlan> &all_candidate_plans,
                                              ObIArray<CandidatePlan> &candidate_plans)
{
  int ret = OB_SUCCESS;
  candidate_plans.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < all_candidate_plans.count(); i++) {
    CandidatePlan &candi_plan = all_candidate_plans.at(i);
    if (OB_ISNULL(candi_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (candi_plan.plan_tree_->get_type() == LOG_SET &&
               static_cast<ObLogSet*>(candi_plan.plan_tree_)->is_recursive_union()) {
      ObLogicalOperator* right_child = candi_plan.plan_tree_->get_child(ObLogicalOperator::second_child);
      if (OB_ISNULL(right_child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (right_child->get_contains_match_all_fake_cte() &&
                 !candi_plan.plan_tree_->is_remote()) {
        OPT_TRACE("containt match all fake cte, but not remote plan, will not add plan");
      } else if (OB_FAIL(candidate_plans.push_back(candi_plan))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } else if (candi_plan.plan_tree_->get_contains_match_all_fake_cte() &&
               !candi_plan.plan_tree_->is_remote()) {
      OPT_TRACE("containt match all fake cte, but not remote plan, will not add plan");
    } else if (OB_FAIL(candidate_plans.push_back(candi_plan))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::add_candidate_plan(ObIArray<CandidatePlan> &current_plans,
                                  const CandidatePlan &new_plan)
{
  int ret = OB_SUCCESS;
  bool should_add = true;
  DominateRelation plan_rel = DominateRelation::OBJ_UNCOMPARABLE;
  OPT_TRACE("new candidate plan:", new_plan);
  for (int64_t i = current_plans.count() - 1;
       OB_SUCC(ret) && should_add && i >= 0; --i) {
    OPT_TRACE("compare with current plan:", current_plans.at(i));
    if (OB_FAIL(compute_plan_relationship(current_plans.at(i),
                                          new_plan,
                                          plan_rel))) {
      LOG_WARN("failed to compute plan relationship",
               K(current_plans.at(i)), K(new_plan), K(ret));
    } else if (DominateRelation::OBJ_LEFT_DOMINATE == plan_rel ||
               DominateRelation::OBJ_EQUAL == plan_rel) {
      should_add = false;
      OPT_TRACE("current plan has be dominated");
    } else if (DominateRelation::OBJ_RIGHT_DOMINATE == plan_rel) {
      if (OB_FAIL(current_plans.remove(i))) {
        LOG_WARN("failed to remove dominated plans", K(i), K(ret));
      } else { /* do nothing*/ }
      OPT_TRACE("new plan dominate current plan");
    } else {
      OPT_TRACE("plan can not compare");
    }
  }
  if (OB_SUCC(ret) && should_add) {
    if (OB_FAIL(current_plans.push_back(new_plan))) {
      LOG_WARN("failed to add plan", K(ret));
    } else { /*do nothing*/ }
    OPT_TRACE("new candidate plan added, candidate plan count:", current_plans.count());
  } else {
    OPT_TRACE("new candidate plan is dominated, candidate plan count:", current_plans.count());
  }
  return ret;
}

int ObLogPlan::compute_plan_relationship(const CandidatePlan &first_candi_plan,
                                         const CandidatePlan &second_candi_plan,
                                         DominateRelation &plan_rel)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const ObLogicalOperator *first_plan = NULL;
  const ObLogicalOperator *second_plan = NULL;
  plan_rel = DominateRelation::OBJ_UNCOMPARABLE;
  if (OB_ISNULL(stmt = get_stmt()) ||
      OB_ISNULL(first_plan = first_candi_plan.plan_tree_) ||
      OB_ISNULL(second_plan = second_candi_plan.plan_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(first_plan), K(second_plan), K(ret));
  } else if (OB_FAIL(compute_rescan_plan_relationship(*first_plan, *second_plan, plan_rel))) {
    LOG_WARN("failed to compute rescan plan relationship", K(ret));
  } else if (DominateRelation::OBJ_UNCOMPARABLE != plan_rel) {

  } else {
    DominateRelation temp_relation;
    int64_t left_dominated_count = 0;
    int64_t right_dominated_count = 0;
    int64_t uncompareable_count = 0;
    // compare cost
    if (fabs(first_plan->get_cost() - second_plan->get_cost()) < OB_DOUBLE_EPSINON) {
      // do nothing
      OPT_TRACE("the cost of two plan is equal");
    } else if (first_plan->get_cost() < second_plan->get_cost()) {
      left_dominated_count++;
      OPT_TRACE("left plan is cheaper");
    } else {
      right_dominated_count++;
      OPT_TRACE("right plan is cheaper");
    }
    // compare parallel degree
    if (first_plan->get_parallel() == second_plan->get_parallel()) {
      // do nothing
      OPT_TRACE("the parallel of two plans is equal");
    } else if (first_plan->get_parallel() < second_plan->get_parallel()) {
      left_dominated_count++;
      OPT_TRACE("left plan use less parallel");
    } else {
      right_dominated_count++;
      OPT_TRACE("right plan use less parallel");
    }
    // compare interesting order
    if (OB_FAIL(ObOptimizerUtil::compute_ordering_relationship(
                                 first_plan->has_any_interesting_order_info_flag(),
                                 second_plan->has_any_interesting_order_info_flag(),
                                 first_plan->get_op_ordering(),
                                 second_plan->get_op_ordering(),
                                 first_plan->get_output_equal_sets(),
                                 first_plan->get_output_const_exprs(),
                                 temp_relation))) {
      LOG_WARN("failed to compute ordering relationship", K(ret));
    } else if (temp_relation == DominateRelation::OBJ_EQUAL) {
      /*do nothing*/
      OPT_TRACE("the interesting order of two plans is equal");
    } else if (temp_relation == DominateRelation::OBJ_LEFT_DOMINATE) {
      left_dominated_count++;
      OPT_TRACE("left plan dominate right plan beacuse of interesting order");
    } else if (temp_relation == DominateRelation::OBJ_RIGHT_DOMINATE) {
      OPT_TRACE("right plan dominate left plan beacuse of interesting order");
      right_dominated_count++;
    } else {
      uncompareable_count++;
    }
    // check dominate relationship for sharding info
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOptimizerUtil::compute_sharding_relationship(
                                   first_plan->get_strong_sharding(),
                                   first_plan->get_weak_sharding(),
                                   second_plan->get_strong_sharding(),
                                   second_plan->get_weak_sharding(),
                                   first_plan->get_output_equal_sets(),
                                   temp_relation))) {
        LOG_WARN("failed to compute sharding relationship", K(ret));
      } else if (temp_relation == DominateRelation::OBJ_EQUAL) {
        /*do nothing*/
        OPT_TRACE("this sharding of two plans is equal");
      } else if (temp_relation == DominateRelation::OBJ_LEFT_DOMINATE) {
        left_dominated_count++;
        OPT_TRACE("left plan dominate right plan beacuse of sharding");
      } else if (temp_relation == DominateRelation::OBJ_RIGHT_DOMINATE) {
        right_dominated_count++;
        OPT_TRACE("right plan dominate left plan beacuse of sharding");
      } else {
        uncompareable_count++;
        OPT_TRACE("sharding can not compare");
      }
    }

    // compare pipeline operator
    if (OB_SUCC(ret) && stmt->has_limit()) {
      if (OB_FAIL(compute_pipeline_relationship(*first_plan,
                                                *second_plan,
                                                temp_relation))) {
        LOG_WARN("failed to compute pipeline relationship", K(ret));
      } else if (temp_relation == DominateRelation::OBJ_EQUAL) {
        /*do nothing*/
        OPT_TRACE("both plan is pipeline");
      } else if (temp_relation == DominateRelation::OBJ_LEFT_DOMINATE) {
        left_dominated_count++;
        OPT_TRACE("left plan dominate right plan beacuse of pipeline");
      } else if (temp_relation == DominateRelation::OBJ_RIGHT_DOMINATE) {
        right_dominated_count++;
        OPT_TRACE("right plan dominate left plan beacuse of pipeline");
      } else {
        uncompareable_count++;
        OPT_TRACE("pipeline path can not compare");
      }
    }
    // compute final result
    if (OB_SUCC(ret)) {
      if (left_dominated_count > 0 && right_dominated_count == 0
          && uncompareable_count == 0) {
        plan_rel = DominateRelation::OBJ_LEFT_DOMINATE;
        LOG_TRACE("first dominated second",
                  K(first_plan->get_cost()), K(first_plan->get_op_ordering()),
                  K(second_plan->get_cost()), K(second_plan->get_op_ordering()));
      } else if (right_dominated_count > 0 && left_dominated_count == 0
                 && uncompareable_count == 0) {
        plan_rel = DominateRelation::OBJ_RIGHT_DOMINATE;
        LOG_TRACE("second dominated first",
                  K(second_plan->get_cost()), K(second_plan->get_op_ordering()),
                  K(first_plan->get_cost()), K(first_plan->get_op_ordering()));
      } else if (left_dominated_count == 0 && right_dominated_count == 0
                 && uncompareable_count == 0) {
        plan_rel = DominateRelation::OBJ_EQUAL;
      } else {
        plan_rel = DominateRelation::OBJ_UNCOMPARABLE;
      }
    }
  }
  return ret;
}

int ObLogPlan::compute_rescan_plan_relationship(const ObLogicalOperator &first_plan,
                                                const ObLogicalOperator &second_plan,
                                                DominateRelation &relation)
{
  int ret = OB_SUCCESS;
  relation = DominateRelation::OBJ_UNCOMPARABLE;
  if (get_is_rescan_subplan()) {
    bool first_is_px_with_das = !first_plan.is_match_all() && first_plan.get_contains_das_op();
    bool second_is_px_with_das = !second_plan.is_match_all() && second_plan.get_contains_das_op();
    if (first_is_px_with_das && !second_is_px_with_das) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      OPT_TRACE("right plan dominate left plan because of px rescan plan use das op");
    } else if (!first_is_px_with_das && second_is_px_with_das) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
      OPT_TRACE("left plan dominate right plan because of px rescan plan use das op");
    }
  }
  if (log_op_def::LOG_SUBPLAN_FILTER == first_plan.get_type()
      && log_op_def::LOG_SUBPLAN_FILTER == second_plan.get_type()) {
    const ObLogSubPlanFilter *first_spf = static_cast<const ObLogSubPlanFilter*>(&first_plan);
    const ObLogSubPlanFilter *second_spf = static_cast<const ObLogSubPlanFilter*>(&second_plan);
    int64_t first_right_local_rescan = 0;
    int64_t second_right_local_rescan = 0;
    bool first_can_px_batch_rescan = false;
    bool second_can_px_batch_rescan = false;
    bool first_rescan_contain_match_all = false;
    bool second_rescan_contain_match_all = false;
    bool need_compare = false;
    if (OB_FAIL(ObLogSubPlanFilter::need_compare_batch_rescan(*first_spf, *second_spf, need_compare))) {
      LOG_WARN("failed to check need compare batch rescan", K(ret));
    } else if (!need_compare) {
      /* do nothing */
    } else if (!first_spf->enable_das_group_rescan() && second_spf->enable_das_group_rescan()) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      OPT_TRACE("right plan dominate left plan because of group rescan subplan filter");
    } else if (first_spf->enable_das_group_rescan() && !second_spf->enable_das_group_rescan()) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
      OPT_TRACE("left plan dominate right plan because of group rescan subplan filter");
    } else if (OB_FAIL(first_spf->check_right_is_local_scan(first_right_local_rescan))
               || OB_FAIL(second_spf->check_right_is_local_scan(second_right_local_rescan))) {
      LOG_WARN("failed to check right is local rescan", K(ret));
    } else if (first_spf->enable_das_group_rescan() && second_spf->enable_das_group_rescan()) {
      if (first_spf->get_parallel() != second_spf->get_parallel()) {
        /* do nothing */
      } else if (first_right_local_rescan < second_right_local_rescan) {
        relation = DominateRelation::OBJ_RIGHT_DOMINATE;
        OPT_TRACE("right path dominate left path because of local group rescan");
      } else if (first_right_local_rescan > second_right_local_rescan) {
        relation = DominateRelation::OBJ_LEFT_DOMINATE;
        OPT_TRACE("left path dominate right path because of local group rescan");
      }
    } else if (first_right_local_rescan < second_right_local_rescan) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      OPT_TRACE("right plan dominate left plan because of local rescan subplan filter");
    } else if (first_right_local_rescan > second_right_local_rescan) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
      OPT_TRACE("left plan dominate right plan because of local rescan subplan filter");
    } else if (0 < first_right_local_rescan && 0 < second_right_local_rescan) {
      /* do nothing */
    } else if (OB_FAIL(first_spf->pre_check_spf_can_px_batch_rescan(first_can_px_batch_rescan, first_rescan_contain_match_all))
               || OB_FAIL(second_spf->pre_check_spf_can_px_batch_rescan(second_can_px_batch_rescan, second_rescan_contain_match_all))) {
      LOG_WARN("failed to pre check spf can px batch rescan", K(ret));
    } else if ((!first_can_px_batch_rescan || (first_rescan_contain_match_all && !second_rescan_contain_match_all))
               && second_can_px_batch_rescan) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
      OPT_TRACE("right plan dominate left plan because of px batch rescan subplan filter");
    } else if (first_can_px_batch_rescan
               && (!second_can_px_batch_rescan || (!first_rescan_contain_match_all && second_rescan_contain_match_all))) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
      OPT_TRACE("left plan dominate right plan because of px batch rescan subplan filter");
    } else {
      /* do nothing */
    }
    LOG_TRACE("finish compute spf rescan plan relationship", K(need_compare),
                    K(first_spf->enable_das_group_rescan()), K(second_spf->enable_das_group_rescan()),
                    K(first_right_local_rescan), K(second_right_local_rescan),
                    K(first_can_px_batch_rescan), K(second_can_px_batch_rescan));
  }
  return ret;
}

int ObLogPlan::compute_pipeline_relationship(const ObLogicalOperator &first_plan,
                                             const ObLogicalOperator &second_plan,
                                             DominateRelation &relation)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  relation = DominateRelation::OBJ_UNCOMPARABLE;
  bool check_pipeline = true;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (!stmt->get_order_items().empty()) {
    bool is_first_left_prefix = false;
    bool is_first_right_prefix = false;
    bool is_second_left_prefix = false;
    bool is_second_right_prefix = false;
    check_pipeline = false;
    if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(stmt->get_order_items(),
                                                    first_plan.get_op_ordering(),
                                                    first_plan.get_output_equal_sets(),
                                                    first_plan.get_output_const_exprs(),
                                                    is_first_left_prefix,
                                                    is_first_right_prefix))) {
      LOG_WARN("failed to compute prefix ordering relationship", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::is_prefix_ordering(stmt->get_order_items(),
                                                           second_plan.get_op_ordering(),
                                                           second_plan.get_output_equal_sets(),
                                                           second_plan.get_output_const_exprs(),
                                                           is_second_left_prefix,
                                                           is_second_right_prefix))) {
      LOG_WARN("failed to compute prefix ordering relationship", K(ret));
    } else if (!is_first_left_prefix && !is_second_left_prefix) {
      relation = DominateRelation::OBJ_EQUAL;
    } else if (is_first_left_prefix && !is_second_left_prefix) {
      relation = DominateRelation::OBJ_LEFT_DOMINATE;
    } else if (!is_first_left_prefix && is_second_left_prefix) {
      relation = DominateRelation::OBJ_RIGHT_DOMINATE;
    } else {
      check_pipeline = true;
    }
  } else { /*do nothing*/ }
  bool is_first_pipeline = first_plan.is_pipelined_plan();
  bool is_second_pipeline = second_plan.is_pipelined_plan();
  if (OB_FAIL(ret) || !check_pipeline) {
    //do nothing
  } else if (!is_first_pipeline && is_second_pipeline) {
    relation = DominateRelation::OBJ_RIGHT_DOMINATE;
  } else if (is_first_pipeline && !is_second_pipeline) {
    relation = DominateRelation::OBJ_LEFT_DOMINATE;
  } else if (!is_first_pipeline && !is_second_pipeline) {
    relation = DominateRelation::OBJ_EQUAL;
  } else if (is_first_pipeline && is_second_pipeline) {
    relation = DominateRelation::OBJ_UNCOMPARABLE;
  }
  return ret;
}

int ObLogPlan::add_global_table_partition_info(ObTablePartitionInfo *addr_table_partition_info)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  if (OB_NOT_NULL(addr_table_partition_info) &&
      addr_table_partition_info->get_table_location().use_das() &&
      addr_table_partition_info->get_table_location().get_has_dynamic_exec_param()) {
    // table locations maintained in physical plan will include those for px/das static partition pruning
    // don't add those for das dynamic partition pruning which maintained independently
  } else {
    ObIArray<ObTablePartitionInfo *> & table_partition_infos = optimizer_context_.get_table_partition_info();
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < table_partition_infos.count(); ++i) {
      const ObTablePartitionInfo *tmp_info = table_partition_infos.at(i);
      if (tmp_info == addr_table_partition_info
          || (tmp_info->get_table_id() == addr_table_partition_info->get_table_id() &&
              tmp_info->get_ref_table_id() == addr_table_partition_info->get_ref_table_id())) {
        is_found = true;
      }
    }
    if (OB_SUCC(ret) && !is_found) {
      if (OB_FAIL(table_partition_infos.push_back(addr_table_partition_info))) {
        LOG_WARN("store table partition info failed", K(ret));
      }
    }
  }
  return ret;
}

/**
 * 分析location_constraint中的基表约束、严格partition wise join约束、非严格partition wise join约束
 * 对于如下计划：
 *           HJ4
 *          /   \
 *         EX   UNION_ALL
 *         |      /     \
 *         EX   TS5     TS6
 *         |
 *         HJ3
 *        /    \
 *      HJ1    HJ2
 *    /   \    /   \
 *  TS1   TS2 TS3  TS4
 *
 *    基表约束: {t1, dist}, {t2, dist}, {t3, dist}, {t4, dist}, {t5, local}, {t6, local}
 *    严格pwj约束:  [0,1], [2,3], [0,1,2,3]
 *    非严格pwj约束: [4,5]
 * 去除有重复的约束条件后
 *    基表约束: {t1, dist}, {t2, dist}, {t3, dist}, {t4, dist}, {t5, local}, {t6, local}
 *    严格pwj约束: [0,1,2,3]
 *    非严格pwj约束: [4,5]
 */
int ObLogPlan::remove_duplicate_constraint(ObLocationConstraintContext &location_constraint,
                                           ObSqlCtx &sql_ctx) const
{
  int ret = OB_SUCCESS;
  // 约束去重
  if (OB_FAIL(remove_duplicate_base_table_constraint(location_constraint))) {
    LOG_WARN("failed to remove duplicate base table constraint", K(ret));
  } else if (OB_FAIL(remove_duplicate_pwj_constraint(location_constraint.strict_constraints_))) {
    LOG_WARN("failed to remove duplicate strict pwj constraint", K(ret));
  } else if (OB_FAIL(remove_duplicate_pwj_constraint(location_constraint.non_strict_constraints_))){
    LOG_WARN("failed to remove duplicate strict pwj constraint", K(ret));
  } else if (OB_FAIL(sort_pwj_constraint(location_constraint))) {
    LOG_WARN("failed to sort pwj constraint", K(ret));
  } else if (OB_FAIL(resolve_dup_tab_constraint(location_constraint))) {
    LOG_WARN("failed to resolve duplicatet table constraint");
  // 将约束设置给sql_ctx
  } else if (OB_FAIL(sql_ctx.set_location_constraints(location_constraint, get_allocator()))) {
    LOG_WARN("failed to set location constraints", K(ret));
  } else {
    LOG_TRACE("duplicated constraints removed", K(location_constraint));
  }
  return ret;
}

/**
 * 移除重复的基表约束
 * TODO yibo 理论上基表location约束不应该存在重复，先留一个检查。
 * 以下场景是一个例外：
 * 目前domain index的实现会在计划生成时mock一些log_table_scan出来，mock出来的log_table_scan直接使用了原
 * log_table_scan的table_id，会导致出现重复的基表约束。
 */
int ObLogPlan::remove_duplicate_base_table_constraint(ObLocationConstraintContext &location_constraint) const
{
  int ret = OB_SUCCESS;
  ObLocationConstraint &base_constraints = location_constraint.base_table_constraints_;
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

// 发现重复的基表约束时, 替换pwj约束中重复的基表约束
// TODO yibo 理论上基表location约束不应该存在重复，这个函数应该也不需要
int ObLogPlan::replace_pwj_constraints(ObIArray<ObPwjConstraint *> &constraints,
                                       const int64_t from,
                                       const int64_t to) const
{
  int ret = OB_SUCCESS;
  ObPwjConstraint *cur_cons = NULL;
  ObSEArray<ObPwjConstraint *, 4> new_constraints;
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
      // 消除pwj constraint中重复的基表约束后，可能会导致新的pwj constraint中只有一个基表，此时这个约束就无效了
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

// 利用包含关系移除重复的pwj约束
// e.g. 存在约束[[0,1], [2,3], [0,1,2,3], [4,5]] 可以移除[0,1]和[2,3]
int ObLogPlan::remove_duplicate_pwj_constraint(ObIArray<ObPwjConstraint *> &pwj_constraints) const
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
        } else if (OB_FAIL(ObLocationConstraintContext::calc_constraints_inclusion(
            l_cons, r_cons, inclusion_result))) {
          LOG_WARN("failed to calculate inclusion relation between constraints",
                   K(ret), K(*l_cons), K(*r_cons));
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
    ObSEArray<ObPwjConstraint *, 8> tmp_constraints;
    if (OB_FAIL(tmp_constraints.assign(pwj_constraints))) {
      LOG_WARN("failed to assign strict constraints", K(ret));
    } else {
      pwj_constraints.reuse();
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_constraints.count(); ++i) {
        if (!removed_idx.has_member(i) &&
            OB_FAIL(pwj_constraints.push_back(tmp_constraints.at(i)))) {
          LOG_WARN("failed to push back pwj constraint", K(ret));
        }
      }
    }
  }
  return ret;
}

EqualSets* ObLogPlan::create_equal_sets()
{
  EqualSets *esets = NULL;
  void *ptr = NULL;
  if (OB_LIKELY(NULL != (ptr = get_allocator().alloc(sizeof(EqualSets))))) {
    esets = new (ptr) EqualSets();
  }
  return esets;
}

ObJoinOrder* ObLogPlan::create_join_order(PathType type)
{
  void *ptr = NULL;
  ObJoinOrder *join_order = NULL;
  if (OB_LIKELY(NULL != (ptr = get_allocator().alloc(sizeof(ObJoinOrder))))) {
    join_order = new (ptr) ObJoinOrder(&get_allocator(), this, type);
  }
  return join_order;
}

// 将pwj约束每一个分区中的值按照从小到大的顺序排列
int ObLogPlan::sort_pwj_constraint(ObLocationConstraintContext &location_constraint) const
{
  int ret = OB_SUCCESS;
  ObIArray<ObPwjConstraint *> &strict_pwj_cons = location_constraint.strict_constraints_;
  ObIArray<ObPwjConstraint *> &non_strict_pwj_cons = location_constraint.non_strict_constraints_;
  ObPwjConstraint *cur_cons = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < strict_pwj_cons.count(); ++i) {
    if (OB_ISNULL(cur_cons = strict_pwj_cons.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else {
      lib::ob_sort(&cur_cons->at(0), &cur_cons->at(0) + cur_cons->count());
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < non_strict_pwj_cons.count(); ++i) {
    if (OB_ISNULL(cur_cons = non_strict_pwj_cons.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else {
      lib::ob_sort(&cur_cons->at(0), &cur_cons->at(0) + cur_cons->count());
    }
  }
  return ret;
}

int ObLogPlan::check_enable_plan_expiration(bool &enable) const
{
  int ret = OB_SUCCESS;
  enable = false;
  ObOptimizerContext &opt_ctx = get_optimizer_context();
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(opt_ctx.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_stmt()), K(opt_ctx.get_query_ctx()));
  } else if (!get_stmt()->is_select_stmt()) {
    // do nothing
  } else if (opt_ctx.get_phy_plan_type() != OB_PHY_PLAN_LOCAL &&
             opt_ctx.get_phy_plan_type() != OB_PHY_PLAN_DISTRIBUTED) {
    // do nothing
  } else if (opt_ctx.get_query_ctx()->get_query_hint().has_outline_data()
#ifdef OB_BUILD_SPM
             && !opt_ctx.get_query_ctx()->is_spm_evolution_
#endif
            ) {
    // do nothing
  } else {
    enable = true;
  }
  return ret;
}

bool ObLogPlan::need_consistent_read() const
{
  bool bret = true;
  if (OB_NOT_NULL(root_) && OB_NOT_NULL(get_stmt()) && OB_NOT_NULL(get_optimizer_context().get_query_ctx())) {
    //保守起见，这里只放开对insert/replace语句的限制
    //即insert/replace中table set为空的时候代表不依赖consistent read
    if (stmt::T_INSERT == get_stmt()->get_stmt_type()) {
      const ObInsertStmt *insert_stmt = static_cast<const ObInsertStmt*>(get_stmt());
      if (!insert_stmt->is_replace() && !insert_stmt->is_insert_up()) {
        uint64_t insert_table_id = insert_stmt->get_insert_table_info().table_id_;
        bool found_other_table = false;
        for (int64_t i = 0;
            !found_other_table && i < get_optimizer_context().get_table_partition_info().count(); ++i) {
          const ObTablePartitionInfo *part_info = get_optimizer_context().get_table_partition_info().at(i);
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

/*
 * for update/delete/select-for-update stmt
 */
int ObLogPlan::check_need_multi_partition_dml(const ObDMLStmt &stmt,
                                              ObLogicalOperator &top,
                                              const ObIArray<IndexDMLInfo *> &index_dml_infos,
                                              bool use_parallel_das,
                                              bool &is_multi_part_dml,
                                              bool &is_result_local)
{
  int ret = OB_SUCCESS;
  is_multi_part_dml = false;
  is_result_local = false;
  ObShardingInfo *source_sharding = NULL;
  if (OB_UNLIKELY(index_dml_infos.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index dml info is empty", K(ret));
  } else if (stmt.has_instead_of_trigger()) {
    is_multi_part_dml = true;
    is_result_local = true;
  } else if (use_parallel_das) {
    is_multi_part_dml = true;
    is_result_local = true;
  } else if (OB_FAIL(check_stmt_need_multi_partition_dml(stmt,
                                                         index_dml_infos,
                                                         is_multi_part_dml))) {
    LOG_WARN("failed to check stmt need multi partition dml", K(ret));
  } else if (is_multi_part_dml) {
    /*do nothing*/
  } else if (OB_UNLIKELY(index_dml_infos.empty()) || OB_ISNULL(index_dml_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index dml info", K(ret));
  } else if (OB_FAIL(check_location_need_multi_partition_dml(top,
                                                             index_dml_infos.at(0)->loc_table_id_,
                                                             is_multi_part_dml,
                                                             is_result_local,
                                                             source_sharding))) {
    LOG_WARN("failed to check whether location need multi-partition dml", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

/*
 * this function is used for select-for-update/update/delete
 */
int ObLogPlan::check_stmt_need_multi_partition_dml(const ObDMLStmt &stmt,
                                                   const ObIArray<IndexDMLInfo *> &index_dml_infos,
                                                   bool &is_multi_part_dml)
{
  int ret = OB_SUCCESS;
  is_multi_part_dml = index_dml_infos.count() > 1
      || optimizer_context_.is_batched_multi_stmt()
      //ddl sql can produce a PDML plan with PL UDF,
      //some PL UDF that cannot be executed in a PDML plan
      //will be forbidden during the execution phase
      || optimizer_context_.contain_user_nested_sql();
  if (!is_multi_part_dml && stmt.is_update_stmt()) {
    const ObUpdateStmt &update_stmt = static_cast<const ObUpdateStmt&>(stmt);
    bool part_key_update = false;
    TableItem *table_item = NULL;
    ObSchemaGetterGuard *schema_guard = get_optimizer_context().get_schema_guard();
    ObSQLSessionInfo* session_info = get_optimizer_context().get_session_info();
    const ObTableSchema *table_schema = NULL;
    ObUpdateTableInfo* table_info = nullptr;
    if (OB_FAIL(update_stmt.part_key_is_updated(part_key_update))) {
      LOG_WARN("failed to check part key is updated", K(ret));
    } else if (!part_key_update) {
      // do nothing
    } else if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(schema_guard), K(session_info), K(ret));
    } else if (OB_UNLIKELY(update_stmt.get_update_table_info().count() != 1) ||
               OB_ISNULL(table_info = update_stmt.get_update_table_info().at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(update_stmt.get_update_table_info()));
    } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                      table_info->ref_table_id_, table_schema))) {
      LOG_WARN("get table schema failed", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      is_multi_part_dml = !ObSQLUtils::is_one_part_table_can_skip_part_calc(*table_schema);
    }
  } else if (!is_multi_part_dml && stmt.is_select_stmt() && stmt.has_for_update()) {
    ObSchemaGetterGuard *schema_guard = get_optimizer_context().get_schema_guard();
    ObSQLSessionInfo* session_info = get_optimizer_context().get_session_info();
    const ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(index_dml_infos.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index dml info", K(ret));
    } else if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(schema_guard), K(session_info), K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                      index_dml_infos.at(0)->ref_table_id_,
                                                      table_schema))) {
      LOG_WARN("get table schema failed", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      is_multi_part_dml = table_schema->is_partitioned_table();
    }
  }
  return ret;
}

/*
 * this function is used for select-for-update/update/delete
 */
int ObLogPlan::check_location_need_multi_partition_dml(ObLogicalOperator &top,
                                                       uint64_t table_id,
                                                       bool &is_multi_part_dml,
                                                       bool &is_result_local,
                                                       ObShardingInfo *&source_sharding)
{
  int ret = OB_SUCCESS;
  source_sharding = NULL;
  ObTablePartitionInfo *source_table_part = NULL;
  ObTableLocationType source_loc_type = OB_TBL_LOCATION_UNINITIALIZED;
  is_multi_part_dml = false;
  is_result_local = false;
  if (OB_FAIL(get_source_table_info(top,
                                    table_id,
                                    source_sharding,
                                    source_table_part))) {
    LOG_WARN("failed to get dml table sharding info", K(ret), K(table_id), K(source_sharding));
  } else if (OB_ISNULL(source_sharding) || OB_ISNULL(source_table_part)) {
    is_multi_part_dml = true;
    is_result_local = true;
  } else if (OB_FAIL(source_table_part->get_location_type(
                                          get_optimizer_context().get_local_server_addr(),
                                          source_loc_type))) {
    LOG_WARN("failed get location type", K(ret));
  } else if (source_sharding->is_match_all() || OB_TBL_LOCATION_ALL == source_loc_type) {
    is_multi_part_dml = true;
    is_result_local = true;
  } else if (source_sharding->is_local()) {
    is_multi_part_dml = false;
    is_result_local = true;
  } else if (source_sharding->is_distributed() && OB_TBL_LOCATION_REMOTE == source_loc_type) {
    is_multi_part_dml = true;
    is_result_local = true;
  } else {
    // dml 开启 PX模式下决定是否使用 multi part 计划
    ObShardingInfo *top_sharding = top.get_strong_sharding();
    if (top.is_exchange_allocated() ||
        (NULL != top_sharding && top_sharding->is_distributed_without_table_location())) {
      is_multi_part_dml = true;
      is_result_local = true;
    } else {
      is_multi_part_dml = false;
      is_result_local = source_sharding->is_local();
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to check location need multi-partition update", K(table_id), K(is_multi_part_dml),
        K(is_result_local));
  }
  return ret;
}

int ObLogPlan::get_source_table_info(ObLogicalOperator &top,
                                     uint64_t source_table_id,
                                     ObShardingInfo *&source_sharding,
                                     ObTablePartitionInfo *&source_table_part)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  source_sharding = NULL;
  source_table_part = NULL;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (top.is_table_scan()) {
    ObLogTableScan &table_scan = static_cast<ObLogTableScan&>(top);
    bool is_adaptive_nlj = false;
    if (OB_NOT_NULL(top.get_parent()) && top.get_parent()->get_type() == LOG_JOIN) {
      ObLogJoin *join_op = static_cast<ObLogJoin *>(top.get_parent());
      if (join_op->get_join_algo() == NESTED_LOOP_JOIN &&
          join_op->is_adaptive()) {
        is_adaptive_nlj = true;
      }
    }
    if (is_adaptive_nlj) {
    } else if (table_scan.get_table_id() == source_table_id && !table_scan.get_is_index_global()) {
      source_sharding = table_scan.get_strong_sharding();
      source_table_part = table_scan.get_table_partition_info();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && NULL == source_sharding
                      && i < top.get_num_of_child(); ++i) {
    if (OB_ISNULL(top.get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(SMART_CALL(get_source_table_info(*top.get_child(i),
                                                        source_table_id,
                                                        source_sharding,
                                                        source_table_part)))) {
      LOG_WARN("get source sharding info recursive failed", K(ret), K(source_table_id));
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(log_op_def::ObLogOpType::LOG_SET == top.get_type()
                                  && NULL != source_sharding)) {
    int64_t total_part_cnt = 0;
    if (!source_sharding->is_distributed() && OB_FAIL(source_sharding->get_total_part_cnt(total_part_cnt))) {
      LOG_WARN("failed to get total part cnt", K(ret), K(*source_sharding));
    } else if (source_sharding->is_distributed() || total_part_cnt > 1) {
      /*  create table t3(c1 int, c2 int, c3 int, index idx(c2)) partition by hash(c1) partitions 5;
      *  update t3 set c3 = 3  where (c1 = 1 or c2 =1);
      *  If this DML happend or expansion transform, we need multi table dml,
      *  here set source_sharding to null.
      */
      source_sharding = NULL;
      LOG_TRACE("partition table happend or expansion, need multi-table dml");
    }
  }
  return ret;
}

int ObLogPlan::init_plan_info()
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObQueryCtx* query_ctx = NULL;
  if (OB_ISNULL(schema_guard = get_optimizer_context().get_sql_schema_guard())
      || OB_ISNULL(get_stmt()) || OB_ISNULL(query_ctx = get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  } else if (OB_FAIL(get_stmt()->get_stmt_equal_sets(equal_sets_,
                                                     allocator_,
                                                     false))) {
    LOG_WARN("failed to get stmt equal sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(get_stmt()->get_condition_exprs(),
                                                          get_const_exprs()))) {
    LOG_WARN("failed to compute const equivalent exprs", K(ret));
#ifndef OB_BUILD_SPM
  } else if (OB_FAIL(log_plan_hint_.init_log_plan_hint(*schema_guard, *get_stmt(),
                                                       query_ctx->get_query_hint()))) {
    LOG_WARN("failed to init log plan hint", K(ret));
#else
  } else if (OB_FAIL(log_plan_hint_.init_log_plan_hint(*schema_guard, *get_stmt(),
                                                       query_ctx->get_query_hint(),
                                                       query_ctx->is_spm_evolution_))) {
    LOG_WARN("failed to init log plan hint", K(ret));
#endif
  } else if (OB_FAIL(init_onetime_subquery_info())) {
    LOG_WARN("failed to extract onetime_exprs", K(ret));
  }
  return ret;
}

int ObLogPlan::generate_plan()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(generate_raw_plan())) {
    LOG_WARN("fail to generate raw plan", K(ret));
  } else if (stmt->is_explain_stmt() || stmt->is_help_stmt()) {
    /*do nothing*/
  } else if (OB_FAIL(do_post_plan_processing())) {
    LOG_WARN("failed to post plan processing", K(ret));
  } else if (OB_FAIL(plan_traverse_loop(PX_RESCAN,
                                        RUNTIME_FILTER,
                                        ALLOC_GI,
                                        PX_PIPE_BLOCKING,
                                        ALLOC_MONITORING_DUMP,
                                        OPERATOR_NUMBERING,
                                        EXCHANGE_NUMBERING,
                                        ALLOC_EXPR,
                                        PROJECT_PRUNING,
                                        GEN_SIGNATURE,
                                        GEN_LOCATION_CONSTRAINT,
                                        PX_ESTIMATE_SIZE,
                                        ALLOC_STARTUP_EXPR,
                                        COLLECT_BATCH_EXEC_PARAM))) {
    LOG_WARN("failed to do plan traverse", K(ret));
  } else if (OB_FAIL(do_post_traverse_processing())) {
    LOG_WARN("failed to post traverse processing", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::generate_raw_plan()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  uint64_t dblink_id = OB_INVALID_ID;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(dblink_id = stmt->get_dblink_id())) {
  } else if (OB_INVALID_ID != dblink_id) {
    if (OB_FAIL(generate_dblink_raw_plan())) {
      LOG_WARN("fail to generate dblink raw plan", K(ret));
    }
  } else if (OB_FAIL(init_plan_info())) {
    LOG_WARN("failed to init equal_sets");
  } else if (OB_FAIL(generate_normal_raw_plan())) {
    LOG_WARN("fail to generate normal raw plan", K(ret));
  }
  return ret;
}

int ObLogPlan::generate_dblink_raw_plan()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObLogPlan::do_post_traverse_processing()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *root = NULL;
  if (OB_ISNULL(root = get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(replace_generate_column_exprs(root))) {
    LOG_WARN("failed to replace generate column exprs", K(ret));
  } else if (OB_FAIL(calc_plan_resource())) {
    LOG_WARN("fail calc plan resource", K(ret));
  } else if (OB_FAIL(add_explain_note())) {
    LOG_WARN("fail to add plan node", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

// replace generated column exprs.
int ObLogPlan::replace_generate_column_exprs(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid op", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
    ObLogicalOperator *child = op->get_child(i);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid child", K(ret));
    } else if (OB_FAIL(SMART_CALL(replace_generate_column_exprs(child)))) {
      LOG_WARN("failed to replace generate column expr", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_FAIL(ret)) {
  } else if (op->get_type() == log_op_def::LOG_TABLE_SCAN) {
    // In the table_scan scenario, it is necessary to distinguish the three scenarios
    // of the main table, the index table and the index back table to determine whether
    // to replace the expression of the virtual generated column
    ObLogTableScan *scan_op = static_cast<ObLogTableScan*>(op);
    if (OB_FAIL(generate_tsc_replace_exprs_pair(scan_op))) {
      LOG_WARN("failed to generate replace generated tsc expr", K(ret));
    } else if (OB_FAIL(scan_op->generate_ddl_output_column_ids())) {
      LOG_WARN("fail to generate ddl output column ids");
    } else if (OB_FAIL(scan_op->copy_gen_col_range_exprs())) {
      LOG_WARN("fail to copy gen col range exprs", K(ret));
    } else if (OB_FAIL(scan_op->replace_gen_col_op_exprs(gen_col_replacer_))) {
      LOG_WARN("failed to replace generated tsc expr", K(ret));
    }
    if (OB_SUCC(ret) && EXTERNAL_TABLE == scan_op->get_table_type()) {
      for (int i = 0; OB_SUCC(ret) && i < scan_op->get_access_exprs().count(); ++i) {
        ObColumnRefRawExpr *col_expr = NULL;
        if (scan_op->get_access_exprs().at(i)->is_column_ref_expr()
            && (col_expr = static_cast<ObColumnRefRawExpr*>(
                  scan_op->get_access_exprs().at(i)))->is_stored_generated_column()) {
          ObRawExpr *dep_expr = col_expr->get_dependant_expr();
          if (OB_FAIL(scan_op->extract_file_column_exprs_recursively(dep_expr))) {
            LOG_WARN("fail to extract file column expr", K(ret));
          } else if (OB_FAIL(scan_op->get_ext_column_convert_exprs().push_back(dep_expr))) {
            LOG_WARN("fail to push back expr", K(ret));
          }
        }
      }
    }
  } else if ((op->get_type() == log_op_def::LOG_INSERT) ||
            ((op->get_type() == log_op_def::LOG_INSERT_ALL))) {
    ObLogDelUpd *insert_op = static_cast<ObLogDelUpd*>(op);
    if (OB_FAIL(generate_ins_replace_exprs_pair(insert_op))) {
      LOG_WARN("fail to generate insert replace exprs pair");
    } else if (OB_FAIL(generate_old_column_values_exprs(insert_op))) {
      LOG_WARN("fail to generate index dml info column old values exprs");
    } else if (OB_FAIL(insert_op->replace_op_exprs(gen_col_replacer_))) {
      LOG_WARN("failed to replace generated exprs", K(ret));
    }
  } else {
    if (OB_FAIL(generate_old_column_values_exprs(op))) {
      LOG_WARN("fail to generate index dml info column old values exprs");
    } else if (OB_FAIL(op->replace_op_exprs(gen_col_replacer_))) {
      LOG_WARN("failed to replace generated exprs", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::generate_old_column_exprs(ObIArray<IndexDMLInfo*> &index_dml_infos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
    if (OB_ISNULL(index_dml_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index_dml_info", K(ret));
    } else if (OB_FAIL(index_dml_infos.at(i)->generate_column_old_values_exprs())) {
      LOG_WARN("failed to generate column old values exprs", K(ret));
    }
  }
  return ret;
}

// construct index_dml_info_column_old_values_exprs.
int ObLogPlan::generate_old_column_values_exprs(ObLogicalOperator *root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid op", K(ret));
  } else if (root->get_type() == log_op_def::LOG_UPDATE ||
      root->get_type() == log_op_def::LOG_DELETE) {
    ObLogDelUpd *del_upd = static_cast<ObLogDelUpd*>(root);
    if (OB_FAIL(generate_old_column_exprs(del_upd->get_index_dml_infos()))) {
      LOG_WARN("failed to generate column old values exprs", K(ret));
    }
  } else if (root->get_type() == log_op_def::LOG_INSERT) {
    ObLogInsert *insert_op = static_cast<ObLogInsert*>(root);
    if (OB_FAIL(generate_old_column_exprs(insert_op->get_insert_up_index_dml_infos()))) {
      LOG_WARN("failed to generate column old values exprs", K(ret));
    } else if (OB_FAIL(generate_old_column_exprs(insert_op->get_replace_index_dml_infos()))) {
      LOG_WARN("failed to generate column old values exprs", K(ret));
    }
  } else if (root->get_type() == log_op_def::LOG_MERGE) {
    ObLogMerge *merge_op = static_cast<ObLogMerge*>(root);
    if (OB_FAIL(generate_old_column_exprs(merge_op->get_update_infos()))) {
      LOG_WARN("failed to generate column old values exprs", K(ret));
    } else if (OB_FAIL(generate_old_column_exprs(merge_op->get_delete_infos()))) {
      LOG_WARN("failed to generate column old values exprs", K(ret));
    }
  } else if (root->get_type() == log_op_def::LOG_FOR_UPD) {
    ObLogForUpdate *for_upd_op = static_cast<ObLogForUpdate*>(root);
    if (OB_FAIL(generate_old_column_exprs(for_upd_op->get_index_dml_infos()))) {
      LOG_WARN("failed to generate column old values exprs", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::adjust_expr_properties_for_external_table(ObRawExpr *col_expr, ObRawExpr *&expr) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_expr) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", K(ret));
  } else if (expr->get_expr_type() == T_PSEUDO_EXTERNAL_FILE_COL
            || expr->get_expr_type() == T_PSEUDO_EXTERNAL_FILE_URL) {
    // The file column PSEUDO expr does not have relation_ids.
    // Using relation_ids in column expr to act as a column from the table.
    // Relation_ids are required when cg join conditions.
    if (OB_FAIL(expr->add_relation_ids(col_expr->get_relation_ids()))) {
      LOG_WARN("fail to add relation ids", K(ret));
    } else {
      static_cast<ObPseudoColumnRawExpr *>(expr)->set_table_name(
            static_cast<ObColumnRefRawExpr *>(col_expr)->get_table_name());
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < expr->get_children_count(); i++) {
    if (OB_FAIL(adjust_expr_properties_for_external_table(col_expr, expr->get_param_expr(i)))) {
      LOG_WARN("fail to adjust expr properties for external table", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::generate_tsc_replace_exprs_pair(ObLogTableScan *op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid op", K(ret));
  } else if (!op->need_replace_gen_column()) {
    //no need replace in index table non-return table scenario.
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_access_exprs().count(); ++i) {
      ObRawExpr *expr = op->get_access_exprs().at(i);
      if (expr->is_column_ref_expr() &&
          static_cast<ObColumnRefRawExpr *>(expr)->is_virtual_generated_column() &&
          !static_cast<ObColumnRefRawExpr *>(expr)->is_xml_column()) {
        ObRawExpr *&dependant_expr = static_cast<ObColumnRefRawExpr *>(
                                    expr)->get_dependant_expr();
        if (dependant_expr->is_const_expr()) {
          ObRawExpr *new_expr = NULL;
          ObSQLSessionInfo* session_info = get_optimizer_context().get_session_info();
          if (OB_ISNULL(session_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(session_info));
          } else if (OB_FAIL(ObRawExprUtils::build_remove_const_expr(
                                    get_optimizer_context().get_expr_factory(),
                                    *session_info,
                                    dependant_expr,
                                    new_expr))) {
            LOG_WARN("failed to build remove const expr", K(ret));
          } else {
            if (NULL != new_expr) {
              dependant_expr = new_expr;
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(gen_col_replacer_.add_replace_expr(expr, dependant_expr))) {
          LOG_WARN("failed to push back generate replace pair", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_ins_replace_exprs_pair(ObLogDelUpd *op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid op", K(ret));
  } else if (NULL != op->get_table_columns()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_table_columns()->count(); ++i) {
      ObColumnRefRawExpr *expr = op->get_table_columns()->at(i);
      if (expr->is_virtual_generated_column()) {
        ObRawExpr *dependant_expr = static_cast<ObColumnRefRawExpr *>(
                                    expr)->get_dependant_expr();
        if (OB_FAIL(gen_col_replacer_.add_replace_expr(expr, dependant_expr))) {
          LOG_WARN("failed to push back generate replace pair", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::do_post_plan_processing()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *root = NULL;
  uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  if (OB_ISNULL(root = get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(set_use_batch_for_table_scan(root, false, false))) {
    LOG_WARN("failed to set use batch for table scan", K(ret));
  } else if (OB_FAIL(adjust_final_plan_info(root))) {
    LOG_WARN("failed to adjust parent-child relationship", K(ret));
  } else if (OB_FAIL(remove_duplicate_constraints())) {
    LOG_WARN("failed to remove duplicate constraints", K(ret));
  } else if (min_cluster_version >= CLUSTER_VERSION_4_2_2_0 && is_oracle_mode() &&
             OB_FAIL(set_identify_seq_expr_for_recursive_union_all(root))) {
    LOG_WARN("failed to set identify seq expr", K(ret));
  } else if (OB_FAIL(update_re_est_cost(root))) {
    LOG_WARN("failed to re est cost", K(ret));
  } else if (OB_FAIL(choose_duplicate_table_replica(root,
                                                    get_optimizer_context().get_local_server_addr(),
                                                    true))) {
    LOG_WARN("failed to set duplicated table location", K(ret));
  } else if (OB_FAIL(set_advisor_table_id(root))) {
    LOG_WARN("failed to set advise table id from duplicate table", K(ret));
  } else if (OB_FAIL(collect_table_location(root))) {
    LOG_WARN("failed to collect table location", K(ret));
  } else if (OB_FAIL(build_location_related_tablet_ids())) {
    LOG_WARN("build location related tablet ids failed", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::adjust_final_plan_info(ObLogicalOperator *&op)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(op) || OB_ISNULL(op->get_plan()) || OB_ISNULL(op->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
      ObLogicalOperator *child = NULL;
      if (OB_ISNULL(child = op->get_child(i)) || OB_ISNULL(child->get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(child), K(ret));
      } else {
        child->set_parent(op);
        op->set_child(i, child);
        if (op->get_type() == log_op_def::LOG_SET ||
            op->get_type() == log_op_def::LOG_SUBPLAN_SCAN ||
            op->get_type() == log_op_def::LOG_UNPIVOT ||
            (op->get_type() == log_op_def::LOG_SUBPLAN_FILTER && i > 0)) {
          child->mark_is_plan_root();
          child->get_plan()->set_plan_root(child);
        }
        if (OB_FAIL(SMART_CALL(adjust_final_plan_info(child)))) {
          LOG_WARN("failed to adjust parent-child relationship", K(ret));
        } else { /*do nothing*/ }
      }
    }
    if (OB_SUCC(ret) && op->get_type() == LOG_SUBPLAN_FILTER) {
      ObLogSubPlanFilter *subplan_filter = static_cast<ObLogSubPlanFilter *>(op);
      if (OB_FAIL(subplan_filter->allocate_subquery_id())) {
        LOG_WARN("failed to allocate subquery id", K(ret));
      } else if (!subplan_filter->is_update_set()) {
        // do nothing
      } else if (OB_UNLIKELY(!subplan_filter->get_stmt()->is_insert_stmt() &&
                              !subplan_filter->get_stmt()->is_merge_stmt() &&
                              !subplan_filter->get_stmt()->is_update_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update stmt is expected", K(ret));
      } else {
        ObDelUpdLogPlan *plan = static_cast<ObDelUpdLogPlan *>(op->get_plan());
        // stmt is only allowed to be modified in the function;
        ObDelUpdStmt *stmt = const_cast<ObDelUpdStmt* >(plan->get_stmt());
        if (OB_FAIL(plan->perform_vector_assign_expr_replacement(stmt))) {
          LOG_WARN("failed to perform vector assgin expr replace", K(ret));
        }
        for (int64_t i = 1; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
          ObLogPlan *child_plan = NULL;
          if (OB_ISNULL(op->get_child(i)) ||
              OB_ISNULL(child_plan = op->get_child(i)->get_plan())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child expr is null", K(ret));
          } else if (OB_FAIL(plan->group_replacer_.append_replace_exprs(
                                    child_plan->group_replacer_))) {
            LOG_WARN("failed to append group replaced exprs", K(ret));
          } else if (OB_FAIL(plan->window_function_replacer_.append_replace_exprs(
                                    child_plan->window_function_replacer_))) {
            LOG_WARN("failed to append window_function_replaced_exprs", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && op->get_type() == LOG_INSERT && optimizer_context_.is_online_ddl()) {
      ObLogInsert *insert = static_cast<ObLogInsert *>(op);
      ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
      ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
      IndexDMLInfo* index_dml_info = insert->get_index_dml_infos().at(0);
      TableItem* table_item = nullptr;
      const ObTableSchema *index_schema = nullptr;
      if (OB_ISNULL(get_stmt()) || OB_ISNULL(schema_guard) ||
          OB_ISNULL(session_info) || OB_ISNULL(index_dml_info) ||
          OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(index_dml_info->table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(schema_guard),
                    K(session_info), K(index_dml_info), K(table_item));
      } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                        table_item->ddl_table_id_, index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table schema", KPC(index_dml_info), K(ret));
      } else if (index_schema->is_index_table() && !index_schema->is_global_index_table()) {
        for (int64_t i = index_dml_info->column_exprs_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
          ObColumnRefRawExpr* column_expr = index_dml_info->column_exprs_.at(i);
          bool has_column = false;
          if (OB_ISNULL(column_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(index_schema->has_column(column_expr->get_column_id(), has_column))) {
            LOG_WARN("failed to check table has column", K(ret));
          } else if (has_column) {
            // do nothing
          } else if (OB_FAIL(index_dml_info->column_exprs_.remove(i))) {
            LOG_WARN("failed to remove column exprs", K(ret));
          } else if (OB_FAIL(index_dml_info->column_convert_exprs_.remove(i))) {
            LOG_WARN("failed to remove column convert exprs", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && op->get_type() == LOG_JOIN) {
      ObLogJoin *join_op = static_cast<ObLogJoin *>(op);
      if (join_op->get_join_algo() == NESTED_LOOP_JOIN && join_op->is_adaptive()) {
        if (OB_FAIL(perform_adaptive_join_tsc_replace(*join_op))) {
          LOG_WARN("perform adaptive join tsc replace failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && NULL != (dynamic_cast<ObSelectLogPlan*>(op->get_plan()))) {
      ObSelectLogPlan *plan = static_cast<ObSelectLogPlan *>(op->get_plan());
      // stmt is only allowed to be modified in the function;
      ObSelectStmt *stmt = const_cast<ObSelectStmt* >(plan->get_stmt());
      if (!op->need_late_materialization()) {
        // do nothing
      } else if (OB_FAIL(plan->perform_late_materialization(stmt, op))) {
        LOG_WARN("failed to perform late materilization", K(ret), K(*stmt), K(*op));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(get_optimizer_context().get_query_ctx())) {
      ObQueryCtx *query_ctx = get_optimizer_context().get_query_ctx();
      if (OB_FAIL(append(query_ctx->all_equal_param_constraints_, op->equal_param_constraints_))) {
        LOG_WARN("failed to push back equal param constraints", K(ret));
      } else if (OB_FAIL(append(query_ctx->all_plan_const_param_constraints_,
                                op->const_param_constraints_))) {
        LOG_WARN("failed to push back equal param constraints", K(ret));
      } else if (OB_FAIL(append_array_no_dup(query_ctx->all_expr_constraints_,
                                             op->expr_constraints_))) {
        LOG_WARN("failed to append expr constraints", K(ret));
      }
    }

    if (OB_SUCC(ret) && op->get_type() == LOG_SET &&
        static_cast<ObLogSet*>(op)->is_recursive_union()) {
      ObLogicalOperator* right_child = NULL;
      if (OB_UNLIKELY(2 != op->get_num_of_child()) ||
          OB_ISNULL(right_child = op->get_child(ObLogicalOperator::second_child))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(op->get_name()));
      } else if (OB_FAIL(allocate_material_for_recursive_cte_plan(*right_child))) {
        LOG_WARN("faile to allocate material for recursive cte plan", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (op->is_plan_root() && OB_FAIL(op->set_plan_root_output_exprs())) {
        LOG_WARN("failed to add plan root exprs", K(ret));
      } else if (OB_FAIL(op->get_plan()->perform_group_by_pushdown(op))) {
        LOG_WARN("failed to perform group by push down", K(ret));
      } else if (OB_FAIL(op->get_plan()->perform_simplify_win_expr(op)))  {
        LOG_WARN("failed to perform simplify win expr", K(ret));
      } else if (OB_FAIL(op->get_plan()->perform_window_function_pushdown(op))) {
        LOG_WARN("failed to perform window function push down", K(ret));
      } else if (OB_FAIL(op->get_plan()->perform_adjust_onetime_expr(op))) {
        LOG_WARN("failed to perform adjust onetime expr", K(ret));
      } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0 &&
                 get_optimizer_context().get_query_ctx()->get_global_hint().has_dbms_stats_hint() &&
                 OB_FAIL(op->get_plan()->perform_gather_stat_replace(op))) {
        LOG_WARN("failed to perform gather stat replace");
      } else if (OB_FAIL(op->reorder_filter_exprs())) {
        LOG_WARN("failed to reorder filter exprs", K(ret));
      } else if (log_op_def::LOG_JOIN == op->get_type() &&
                 OB_FAIL(static_cast<ObLogJoin*>(op)->adjust_join_conds(static_cast<ObLogJoin *>(op)->get_join_conditions()))) {
        LOG_WARN("failed to adjust join conds", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

//  1. set use batch for table scan
//  2. clear function storage pushdown aggr for batch rescan table scan
int ObLogPlan::set_use_batch_for_table_scan(ObLogicalOperator *op, bool check_gi, bool in_batch_rescan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (op->is_table_scan()) {
    ObLogTableScan *scan_op = static_cast<ObLogTableScan*>(op);
    scan_op->set_use_batch(in_batch_rescan);
    if (in_batch_rescan) {
      scan_op->get_pushdown_aggr_exprs().reuse();
    }
  } else if (check_gi && OB_FAIL(reset_use_batch_due_to_gi_allocated_below(op))) {
    LOG_WARN("failed to reset use batch due to gi allocated below", K(ret));
  } else {
    bool is_batch_rescan_op = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
      is_batch_rescan_op = in_batch_rescan;
      if (0 == i) {
      } else if (log_op_def::LOG_JOIN == op->get_type()) {
        is_batch_rescan_op |= static_cast<ObLogJoin*>(op)->can_use_batch_nlj();
      } else if (log_op_def::LOG_SUBPLAN_FILTER == op->get_type()) {
        ObLogSubPlanFilter *spf = static_cast<ObLogSubPlanFilter*>(op);
        is_batch_rescan_op |= spf->enable_das_group_rescan()
                              && !spf->get_onetime_idxs().has_member(i)
                              && !spf->get_initplan_idxs().has_member(i);
      }
      if (OB_FAIL(SMART_CALL(set_use_batch_for_table_scan(op->get_child(i), check_gi, is_batch_rescan_op)))) {
        LOG_WARN("failed to set use batch for table scan", K(ret), K(i), K(op->get_type()),
                                                    K(op->get_name()), K(op->get_op_id()));
      }
    }
  }
  return ret;
}

int ObLogPlan::reset_use_batch_due_to_gi_allocated_below(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(op));
  } else if (log_op_def::LOG_JOIN == op->get_type()
             && static_cast<ObLogJoin*>(op)->can_use_batch_nlj()) {
    bool has_gi_below = false;
    if (OB_ISNULL(op->get_child(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null right child", K(ret));
    } else if (OB_FAIL(op->get_child(1)->check_has_op_below(LOG_GRANULE_ITERATOR, has_gi_below))) {
      LOG_WARN("failed to check has gi below", K(ret));
    } else if (!has_gi_below) {
      /* do nothing */
    } else {
      static_cast<ObLogJoin*>(op)->set_can_use_batch_nlj(false);
      LOG_TRACE("reset batch nlj due to gi allocated", K(op->get_type()), K(op->get_name()), K(op->get_op_id()));
    }
  } else if (log_op_def::LOG_SUBPLAN_FILTER == op->get_type()
             && static_cast<ObLogSubPlanFilter*>(op)->enable_das_group_rescan()) {
    ObLogSubPlanFilter *spf = static_cast<ObLogSubPlanFilter*>(op);
    bool has_gi_below = false;
    for (int64_t i = 1; !has_gi_below && OB_SUCC(ret) && i < spf->get_num_of_child(); i++) {
      if (OB_ISNULL(spf->get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null child", K(ret), K(i));
      } else if (spf->get_onetime_idxs().has_member(i) || spf->get_initplan_idxs().has_member(i)) {
        /* do nothing */
      } else if (OB_FAIL(spf->get_child(i)->check_has_op_below(LOG_GRANULE_ITERATOR, has_gi_below))) {
        LOG_WARN("failed to check has gi below", K(ret));
      }
    }
    if (OB_SUCC(ret) && has_gi_below) {
      spf->set_enable_das_group_rescan(false);
      LOG_TRACE("reset spf group rescan due to gi allocated", K(op->get_type()), K(op->get_name()), K(op->get_op_id()));
    }
  }
  return ret;
}

int ObLogPlan::set_identify_seq_expr_for_recursive_union_all(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (LOG_SET == op->get_type() && static_cast<ObLogSet *>(op)->is_recursive_union() &&
             static_cast<ObLogSet *>(op)->is_breadth_search()) {
    ObRawExpr *identify_seq_expr = NULL;
    bool is_valid = true;
    if (OB_FAIL(ObOptimizerUtil::allocate_identify_seq_expr(get_optimizer_context(), identify_seq_expr))) {
        LOG_WARN("allocate identify seq expr failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
      if (OB_FAIL(SMART_CALL(set_identify_seq_expr_for_fake_cte(op->get_child(i),
                                                                identify_seq_expr,
                                                                is_valid)))) {
        LOG_WARN("failed to set identify seq expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      static_cast<ObLogSet *>(op)->set_identify_seq_expr(identify_seq_expr);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
    if (OB_FAIL(SMART_CALL(set_identify_seq_expr_for_recursive_union_all(op->get_child(i))))) {
      LOG_WARN("failed to set identify seq expr", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::set_identify_seq_expr_for_fake_cte(ObLogicalOperator *op,
                                                  ObRawExpr *identify_seq_expr,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(identify_seq_expr) || OB_ISNULL(op->get_parent())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (op->get_contains_fake_cte()) {
    if (op->is_plan_root()) {
      // check whether the identify seq expr can be set to the output exprs of the plan root
      if (LOG_SET == op->get_parent()->get_type() &&
          static_cast<ObLogSet *>(op->get_parent())->is_recursive_union()) {
        // do nothing
      } else {
        // fake cte is in an inline view, not supported
        is_valid = false;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
      if (OB_FAIL(SMART_CALL(set_identify_seq_expr_for_fake_cte(op->get_child(i),
                                                                identify_seq_expr,
                                                                is_valid)))) {
        LOG_WARN("failed to set identify seq expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      if (LOG_TABLE_SCAN == op->get_type()) {
        static_cast<ObLogTableScan *>(op)->set_identify_seq_expr(identify_seq_expr);
      }
      if (op->is_plan_root()) {
        if (OB_FAIL(op->get_output_exprs().push_back(identify_seq_expr))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

/*
 * re-estimate cost for limit/join filter/parallel
 */
int ObLogPlan::update_re_est_cost(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  EstimateCostInfo info;
  info.override_ = true;
  double cost = 0.0;
  double card = 0.0;
  LOG_TRACE("Begin final update re est cost");
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(op), K(ret));
  } else if (OB_FAIL(op->re_est_cost(info, card, cost))) {
    LOG_WARN("failed to re-estimate cost", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::choose_duplicate_table_replica(ObLogicalOperator *op,
                                              const ObAddr &addr,
                                              bool is_root)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  bool is_stack_overflow = false;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (log_op_def::LOG_TEMP_TABLE_INSERT == op->get_type()) {
    // do nothing
  } else if (log_op_def::LOG_TABLE_SCAN == op->get_type() &&
             NULL != op->get_strong_sharding() &&
             op->get_strong_sharding()->get_can_reselect_replica() &&
             !is_root) {
    ObLogTableScan *table_scan = static_cast<ObLogTableScan*>(op);
    ObCandiTableLoc &phy_loc =
        table_scan->get_table_partition_info()->get_phy_tbl_location_info_for_update();
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_loc.get_partition_cnt(); ++i) {
      int64_t dup_table_pos = OB_INVALID_INDEX;
      ObCandiTabletLoc &phy_part_loc =
           phy_loc.get_phy_part_loc_info_list_for_update().at(i);
      if (!phy_part_loc.is_server_in_replica(addr, dup_table_pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no server in replica", K(addr), K(table_scan->get_table_id()), K(ret));
      } else {
        phy_part_loc.set_selected_replica_idx(dup_table_pos);
      }
    }
  } else if (log_op_def::LOG_EXCHANGE == op->get_type()) {
    if (OB_ISNULL(child = op->get_child(ObLogicalOperator::first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(SMART_CALL(choose_duplicate_table_replica(child,
                                                                 addr,
                                                                 true)))) {
      LOG_WARN("failed to set duplicated table location", K(ret));
    } else { /*do nothing*/ }
  } else {
    ObShardingInfo *sharding = op->get_strong_sharding();
    ObAddr adjust_addr;
    bool can_reselect_replica = NULL != sharding && sharding->get_can_reselect_replica();
    if (is_root && can_reselect_replica) {
      if (OB_ISNULL(sharding->get_phy_table_location_info()) ||
          OB_UNLIKELY(1 != sharding->get_phy_table_location_info()->get_partition_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(sharding->get_phy_table_location_info()));
      } else {
        share::ObLSReplicaLocation replica_loc;
        const ObCandiTabletLocIArray &phy_partition_loc =
            sharding->get_phy_table_location_info()->get_phy_part_loc_info_list();
        if (OB_FAIL(phy_partition_loc.at(0).get_selected_replica(replica_loc))) {
          LOG_WARN("fail to get selected replica", K(ret), K(phy_partition_loc.at(0)));
        } else if (!replica_loc.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replica location is invalid", K(ret));
        } else {
          adjust_addr = replica_loc.get_server();
        }
      }
    } else if (can_reselect_replica) {
      adjust_addr = addr;
    } else if (NULL != sharding && sharding->is_remote()) {
      if (OB_FAIL(sharding->get_remote_addr(adjust_addr))) {
        LOG_WARN("failed to get remote addr", K(ret));
      }
    } else {
      adjust_addr = get_optimizer_context().get_local_server_addr();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
      if (OB_ISNULL(child = op->get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(choose_duplicate_table_replica(child,
                                                                   adjust_addr,
                                                                   false)))) {
        LOG_WARN("failed to set duplicated table location", K(ret),
                  K(can_reselect_replica), K(adjust_addr));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::gen_das_table_location_info(ObLogTableScan *table_scan,
                                           ObTablePartitionInfo *&table_partition_info)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *sql_schema_guard = NULL;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table_item = NULL;
  ObSEArray<ObRawExpr *, 8> all_filters;
  bool has_dppr = false;
  ObOptimizerContext *opt_ctx = &get_optimizer_context();
  if (OB_ISNULL(table_scan) ||
      OB_ISNULL(table_partition_info) ||
      OB_ISNULL(sql_schema_guard = opt_ctx->get_sql_schema_guard()) ||
      OB_ISNULL(stmt = table_scan->get_stmt()) ||
      OB_ISNULL(table_item = stmt->get_table_item_by_id(table_scan->get_table_id())) ||
      OB_ISNULL(table_scan->get_strong_sharding())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(sql_schema_guard), K(stmt), K(table_scan),
             K(table_partition_info), K(ret));
  } else if (!table_scan->use_das() || !table_scan->is_match_all()) {
    // do nothing
  } else if (OB_FAIL(append_array_no_dup(all_filters, table_scan->get_range_conditions()))) {
    LOG_WARN("failed to add into all filters", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_filters, table_scan->get_filter_exprs()))) {
    LOG_WARN("failed to add into all filters", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_exec_param_filter_exprs(all_filters,
                                                                    has_dppr))) {
    LOG_WARN("failed to find das dppr filter exprs", K(ret));
  } else if (!has_dppr) {
    // do nothing
  } else {
    SMART_VAR(ObTableLocation, das_location) {
      const ObDataTypeCastParams dtc_params =
            ObBasicSessionInfo::create_dtc_params(opt_ctx->get_session_info());
      int64_t ref_table_id = table_scan->get_is_index_global() ?
                              table_scan->get_index_table_id() :
                              table_scan->get_ref_table_id();
      if (OB_FAIL(das_location.init(*sql_schema_guard,
                                    *stmt,
                                    opt_ctx->get_exec_ctx(),
                                    all_filters,
                                    table_scan->get_table_id(),
                                    ref_table_id,
                                    table_scan->get_is_index_global() ? NULL : &table_item->part_ids_,
                                    dtc_params,
                                    false))) {
        LOG_WARN("fail to init table location", K(ret), K(all_filters));
      } else if (das_location.is_all_partition()) {
        // do nothing
      } else {
        das_location.set_has_dynamic_exec_param(true);
        das_location.set_use_das(true);
        if (OB_FAIL(table_partition_info->set_table_location(das_location))) {
          LOG_WARN("failed to set table location");
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::collect_table_location(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (LOG_LINK_SCAN == op->get_type()) {
    /*do nothing*/
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
      if (OB_FAIL(SMART_CALL(collect_table_location(op->get_child(i))))) {
        LOG_WARN("failed to collect table location", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (log_op_def::LOG_TABLE_SCAN == op->get_type()) {
      ObTablePartitionInfo *table_partition_info = NULL;
      ObLogTableScan *table_scan = static_cast<ObLogTableScan*>(op);
      if (table_scan->get_contains_fake_cte() || OB_INVALID_ID != table_scan->get_dblink_id()) {
        /*do nothing*/
      } else if (OB_ISNULL(table_partition_info = table_scan->get_table_partition_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(table_partition_info), K(ret));
      } else if (OB_FALSE_IT(table_partition_info->get_table_location().set_use_das(table_scan->use_das()))) {
      } else if (OB_FAIL(gen_das_table_location_info(table_scan,
                                                     table_partition_info))) {
        LOG_WARN("failed to gen das table location info", K(ret));
      } else if (OB_FAIL(table_partition_info->replace_final_location_key(
          *optimizer_context_.get_exec_ctx(),
          table_scan->get_real_index_table_id(),
          table_scan->is_index_scan() && !table_scan->get_is_index_global()))) {
        LOG_WARN("failed to set table partition info", K(ret));
      } else if (OB_FAIL(add_global_table_partition_info(table_partition_info))) {
        LOG_WARN("failed to add table partition info", K(ret));
      } else { /*do nothing*/ }
    } else if ((log_op_def::LOG_DELETE == op->get_type() ||
                log_op_def::LOG_UPDATE == op->get_type() ||
                log_op_def::LOG_INSERT == op->get_type()) &&
                static_cast<ObLogDelUpd*>(op)->is_pdml()) {
      ObTablePartitionInfo *table_partition_info =
          static_cast<ObLogDelUpd*>(op)->get_table_partition_info();
      if (OB_ISNULL(table_partition_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(add_global_table_partition_info(table_partition_info))) {
        LOG_WARN("failed to add table partition info", K(ret));
      } else { /*do nothing*/ }
    } else if (log_op_def::LOG_INSERT == op->get_type()
               && static_cast<ObLogInsert*>(op)->is_insert_select()
               && !static_cast<ObLogDelUpd*>(op)->has_instead_of_trigger()) {
      ObLogInsert *insert_op = static_cast<ObLogInsert*>(op);
      ObTablePartitionInfo *table_partition_info = insert_op->get_table_partition_info();
      if (OB_ISNULL(table_partition_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(table_partition_info), K(ret));
      } else if (!insert_op->is_multi_part_dml() ||
                 ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED == insert_op->get_phy_plan_type()) {
        if (OB_FAIL(add_global_table_partition_info(table_partition_info))) {
          LOG_WARN("failed to add table partition info", K(ret));
        } else { /*do nothing*/ }
      } else { /*do nothing*/ }
    } else if (log_op_def::LOG_MERGE == op->get_type()
               && !static_cast<ObLogDelUpd*>(op)->has_instead_of_trigger()) {
      ObLogMerge *dml_op = static_cast<ObLogMerge*>(op);
      ObTablePartitionInfo *table_partition_info = dml_op->get_table_partition_info();
      const ObOptimizerContext &opt_ctx = get_optimizer_context();
      if (NULL == table_partition_info) {
        // merge into has no insert, do nothing
      } else if (OB_ISNULL(opt_ctx.get_query_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(table_partition_info), K(opt_ctx.get_query_ctx()), K(ret));
      } else if (!dml_op->is_multi_part_dml()) {
        /* for merge into view, additional table location for insert sharding need add
          create table tp1(c1 int, c2 int, c3 int) partition by hash(c1) partitions 2;
          create table tp2(c1 int, c2 int, c3 int) partition by hash(c1) partitions 2;
          create or replace view v as select * from tp1 where c1 = 3;
          merge into v v1 using (select * from tp2 where c1 = 2) t2 on (v1.c1 = t2.c1)
          when matched then update set v1.c2 = t2.c1 + 100
          when not matched then insert values (t2.c1, t2.c2, t2.c3);
        */
        table_partition_info->get_table_location().set_table_id(opt_ctx.get_query_ctx()->available_tb_id_ - 1);
        if (OB_FAIL(add_global_table_partition_info(table_partition_info))) {
          LOG_WARN("failed to add table partition info", K(ret));
        }
      } else { /*do nothing*/ }
    } else { /*do nothing*/ }
    if (OB_SUCC(ret) && OB_FAIL(collect_location_related_info(*op))) {
      LOG_WARN("collect location related info failed", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::collect_location_related_info(ObLogicalOperator &op)
{
  int ret = OB_SUCCESS;
  ObIArray<TableLocRelInfo> &loc_rel_infos = optimizer_context_.get_loc_rel_infos();
  if (op.is_table_scan()) {
    ObLogTableScan &tsc_op = static_cast<ObLogTableScan&>(op);
    ObTablePartitionInfo *table_part_info = tsc_op.get_table_partition_info();
    ObTableID table_loc_id = tsc_op.get_table_id();
    ObTableID ref_table_id = tsc_op.get_ref_table_id();
    if (OB_NOT_NULL(optimizer_context_.get_loc_rel_info_by_id(table_loc_id, ref_table_id))) {
      //ret = OB_ERR_UNEXPECTED;
      //do nothing.
      LOG_WARN("table location related info already exists", K(ret), K(&op),
               K(table_loc_id), K(ref_table_id), K(loc_rel_infos));
    } else if (!tsc_op.get_is_index_global()) {
      //global index with data table has no related tablet info
      TableLocRelInfo rel_info;
      rel_info.table_loc_id_ = tsc_op.get_table_id();
      rel_info.ref_table_id_ = tsc_op.get_real_ref_table_id();
      if (OB_FAIL(rel_info.related_ids_.push_back(tsc_op.get_real_index_table_id()))) {
        LOG_WARN("store the source table id failed", K(ret));
      } else if (table_part_info != nullptr &&
          OB_FAIL(rel_info.table_part_infos_.push_back(table_part_info))) {
        LOG_WARN("collect table partition info to relation info failed", K(ret));
      } else if (tsc_op.get_index_back()) {
        if (OB_FAIL(rel_info.related_ids_.push_back(tsc_op.get_real_ref_table_id()))) {
          LOG_WARN("store the related table id failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(optimizer_context_.get_loc_rel_infos().push_back(rel_info))) {
        LOG_WARN("store location related info failed", K(ret));
      }
    } else if (tsc_op.get_is_index_global() && tsc_op.get_index_back()) {
      //for global index lookup
      TableLocRelInfo rel_info;
      rel_info.table_loc_id_ = tsc_op.get_table_id();
      rel_info.ref_table_id_ = tsc_op.get_ref_table_id();
      if (OB_FAIL(rel_info.related_ids_.push_back(tsc_op.get_ref_table_id()))) {
        LOG_WARN("store the source table id failed", K(ret));
      } else if (nullptr != tsc_op.get_global_index_back_table_partition_info() && OB_FAIL(rel_info.table_part_infos_.push_back(tsc_op.get_global_index_back_table_partition_info()))) {
        LOG_WARN("collect table partition info to relation info failed", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(optimizer_context_.get_loc_rel_infos().push_back(rel_info))) {
        LOG_WARN("store location related info failed", K(ret));
      }
    }
  } else if (op.is_dml_operator()) {
    ObLogDelUpd &dml_op = static_cast<ObLogDelUpd&>(op);
    ObTablePartitionInfo *table_part_info = dml_op.get_table_partition_info();
    const ObIArray<IndexDMLInfo *> &index_dml_infos = dml_op.get_index_dml_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
      const IndexDMLInfo &index_info = *index_dml_infos.at(i);
      ObTableID table_loc_id = index_info.loc_table_id_;
      ObTableID ref_table_id = index_info.ref_table_id_;
      TableLocRelInfo *loc_rel_info = nullptr;
      if (index_info.is_primary_index_) {
        if (OB_ISNULL(loc_rel_info = optimizer_context_.get_loc_rel_info_by_id(
                                       table_loc_id, ref_table_id))) {
          //init table location related info with the main table
          TableLocRelInfo rel_info;
          rel_info.table_loc_id_ = table_loc_id;
          rel_info.ref_table_id_ = ref_table_id;
          if (OB_FAIL(rel_info.related_ids_.push_back(ref_table_id))) {
            LOG_WARN("store the source table id failed", K(ret));
          } else if (OB_FAIL(loc_rel_infos.push_back(rel_info))) {
            LOG_WARN("store rel info failed", K(ret));
          } else {
            loc_rel_info = &loc_rel_infos.at(loc_rel_infos.count() - 1);
          }
        } else if (OB_FAIL(add_var_to_array_no_dup(loc_rel_info->related_ids_, ref_table_id))) {
          LOG_WARN("add ref_table_id to the related ids failed", K(ret));
        }
        if (OB_SUCC(ret) && table_part_info != nullptr
            && table_part_info->get_table_id() == table_loc_id
            && table_part_info->get_ref_table_id() == ref_table_id) {
          if (OB_FAIL(add_var_to_array_no_dup(loc_rel_info->table_part_infos_, table_part_info))) {
            LOG_WARN("store table part info failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(append_array_no_dup(loc_rel_info->related_ids_, index_info.related_index_ids_))) {
            LOG_WARN("add the ref table id to the related ids failed", K(ret));
          } else {
            LOG_DEBUG("collect dml op related table id", KPC(loc_rel_info), K(table_loc_id), K(ref_table_id));
          }
        }
      }
    }
  } else if (log_op_def::LOG_FOR_UPD == op.get_type()) {
    ObLogForUpdate &for_upd_op = static_cast<ObLogForUpdate&>(op);
    const ObIArray<IndexDMLInfo *> &index_dml_infos = for_upd_op.get_index_dml_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
      const IndexDMLInfo &index_info = *index_dml_infos.at(i);
      ObTableID table_loc_id = index_info.loc_table_id_;
      ObTableID ref_table_id = index_info.ref_table_id_;
      TableLocRelInfo *loc_rel_info = nullptr;
      if (!index_info.is_primary_index_) {
        //do nothing
      } else if (OB_ISNULL(loc_rel_info = optimizer_context_.get_loc_rel_info_by_id(
          table_loc_id, ref_table_id))) {
        //location related info is empty, means the TSC is global index scan, so ignore it
      } else if (loc_rel_info->related_ids_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("related table id array is empty", K(ret), KPC(loc_rel_info));
      } else if (loc_rel_info->related_ids_.at(0) == ref_table_id) {
        //the depend table id is same with the source location table id
        //does not need to add the related table id to related ids
      } else if (OB_FAIL(add_var_to_array_no_dup(loc_rel_info->related_ids_, ref_table_id))) {
        LOG_WARN("add the ref table id to the related ids failed", K(ret));
      }
    }
  }
  return ret;
}

//restore the related table id to the loc_meta in source table location
int ObLogPlan::build_location_related_tablet_ids()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < optimizer_context_.get_loc_rel_infos().count(); ++i) {
    TableLocRelInfo &rel_info = optimizer_context_.get_loc_rel_infos().at(i);
    if (rel_info.related_ids_.count() <= 1) {
      //the first table id is the source table, <=1 mean no dependency table
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < rel_info.table_part_infos_.count(); ++j) {
        ObTablePartitionInfo *source_part_info = rel_info.table_part_infos_.at(j);
        ObDASTableLocMeta &source_loc_meta = source_part_info->get_table_location().get_loc_meta();
        source_loc_meta.related_table_ids_.set_capacity(rel_info.related_ids_.count() - 1);
        for (int64_t k = 0; OB_SUCC(ret) && k < rel_info.related_ids_.count(); ++k) {
          //set related table ids to loc meta
          if (rel_info.related_ids_.at(k) == source_part_info->get_ref_table_id()) {
            //ignore itself, do nothing
          } else if (OB_FAIL(source_loc_meta.related_table_ids_.push_back(rel_info.related_ids_.at(k)))) {
            LOG_WARN("store related table ids failed", K(ret));
          }
        }
      }
    }
  }
  optimizer_context_.get_exec_ctx()->get_das_ctx().clear_all_location_info();
  for (int64_t i = 0; OB_SUCC(ret) && i < optimizer_context_.get_table_partition_info().count(); ++i) {
    ObTablePartitionInfo *table_part_info = optimizer_context_.get_table_partition_info().at(i);
    //need to call ObTableLocation::calculate_table_partition_ids() to
    //reload the related index tablet ids in DASCtx
    //because in the generate_plan stage, only the tablet_id of the data table is calculated,
    //and the tablet_id of the related index table is not calculated
    //In the execution phase,
    //DASCtx relies on the tablet mapping relationship //between the data table and the local index
    //to build the table location of the local index
    DASRelatedTabletMap *map = nullptr;
    if (OB_ISNULL(table_part_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition info", K(ret));
    } else if (0 == table_part_info->get_phy_tbl_location_info().get_partition_cnt()) {
      // partition count is 0 means no matching partition for data table, no need to calculate
      // related tablet ids for it.
    } else if (!table_part_info->get_table_location().use_das() &&
               OB_FAIL(ObPhyLocationGetter::build_related_tablet_info(
                       table_part_info->get_table_location(), *optimizer_context_.get_exec_ctx(), map))) {
      LOG_WARN("rebuild related tablet info failed", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::calc_plan_resource()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = nullptr;
  ObLogicalOperator *plan_root = nullptr;
  int64_t max_parallel_thread_count = 0;
  int64_t max_parallel_group_count = 0;
  if (OB_ISNULL(stmt = get_stmt()) ||
      OB_ISNULL(plan_root = get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObPxResourceAnalyzer analyzer;
    if (OB_FAIL(analyzer.analyze(*plan_root,
                                 max_parallel_thread_count,
                                 max_parallel_group_count,
                                 get_optimizer_context().get_expected_worker_map(),
                                 get_optimizer_context().get_minimal_worker_map()))) {
      LOG_WARN("fail analyze px stmt thread group reservation count", K(ret));
    } else {
      LOG_TRACE("[PxResAnaly]max parallel thread group count",
               K(max_parallel_thread_count), K(max_parallel_group_count));
      get_optimizer_context().set_expected_worker_count(max_parallel_thread_count);
      get_optimizer_context().set_minimal_worker_count(max_parallel_group_count);
    }
  }
  return ret;
}

int ObLogPlan::add_explain_note()
{
  int ret = OB_SUCCESS;
  ObOptimizerContext &opt_ctx = get_optimizer_context();
  ObInsertLogPlan *insert_plan = NULL;
  if (OB_ISNULL(opt_ctx.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(opt_ctx.get_query_ctx()));
#ifdef OB_BUILD_SPM
  } else if (opt_ctx.get_query_ctx()->is_spm_evolution_
             && OB_FALSE_IT(opt_ctx.add_plan_note(PLAN_GENERATED_BY_SPM_BASELINE))) {
#endif
  } else if (OB_FAIL(add_parallel_explain_note())) {
    LOG_WARN("fail to add parallel explain note", K(ret));
  } else if (NULL != (insert_plan = dynamic_cast<ObInsertLogPlan*>(this))
             && insert_plan->is_direct_insert()
             && OB_FALSE_IT(opt_ctx.add_plan_note(DIRECT_MODE_INSERT_INTO_SELECT))) {
  }
  return ret;
}

int ObLogPlan::add_parallel_explain_note()
{
  int ret = OB_SUCCESS;
  int64_t parallel = ObGlobalHint::UNSET_PARALLEL;
  const char *parallel_str = NULL;
  ObOptimizerContext &opt_ctx = get_optimizer_context();
  bool has_valid_table_parallel_hint = false;
  switch (opt_ctx.get_parallel_rule()) {
    case PXParallelRule::LICENSE_NOT_ALLOW_OLAP:
      parallel_str = PARALLEL_DISABLED_BY_LICENSE;
      break;
    case PXParallelRule::PL_UDF_DAS_FORCE_SERIALIZE:
      parallel_str = PARALLEL_DISABLED_BY_PL_UDF_DAS;
      break;
    case PXParallelRule::DBLINK_FORCE_SERIALIZE:
      parallel_str = PARALLEL_DISABLED_BY_DBLINK;
      break;
    case PXParallelRule::MANUAL_HINT:
      has_valid_table_parallel_hint = opt_ctx.get_max_parallel() > opt_ctx.get_parallel();
      parallel_str = PARALLEL_ENABLED_BY_GLOBAL_HINT;
      break;
    case PXParallelRule::SESSION_FORCE_PARALLEL:
      parallel_str = PARALLEL_ENABLED_BY_SESSION;
      has_valid_table_parallel_hint = opt_ctx.get_max_parallel() > opt_ctx.get_parallel();
      break;
    case PXParallelRule::MANUAL_TABLE_DOP:
      parallel_str = PARALLEL_ENABLED_BY_TABLE_PROPERTY;
      break;
    case PXParallelRule::AUTO_DOP:
      parallel_str = PARALLEL_ENABLED_BY_AUTO_DOP;
      break;
    case PXParallelRule::USE_PX_DEFAULT:
      has_valid_table_parallel_hint = opt_ctx.get_max_parallel() > opt_ctx.get_parallel();
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected parallel rule", K(ret), K(opt_ctx.get_parallel_rule()));
  }
  if (OB_SUCC(ret)) {
    parallel_str = has_valid_table_parallel_hint ? PARALLEL_ENABLED_BY_TABLE_HINT : parallel_str;
    if (OB_NOT_NULL(parallel_str)) {
      opt_ctx.add_plan_note(parallel_str, opt_ctx.get_max_parallel());
    }
  }
  return ret;
}

/**
 * 按照去重后的location约束计算出执行依赖的partition wise join map并设置到exec ctx中
 * 计算逻辑与ObDistPlans::check_inner_constraints()类似，只是省略了一些计划生成过程中已经做过的检查
 */
int ObLogPlan::calc_and_set_exec_pwj_map(ObLocationConstraintContext &location_constraint) const
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = get_optimizer_context().get_exec_ctx();
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (location_constraint.strict_constraints_.count() > 0) {
    ObIArray<LocationConstraint> &base_location_cons = location_constraint.base_table_constraints_;
    ObIArray<ObPwjConstraint *> &strict_cons = location_constraint.strict_constraints_;
    const int64_t tbl_count = base_location_cons.count();
    SMART_VAR(ObStrictPwjComparer, strict_pwj_comparer) {
      PWJTabletIdMap pwj_map;
      if (OB_FAIL(pwj_map.create(8, ObModIds::OB_PLAN_EXECUTE))) {
        LOG_WARN("create pwj map failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < strict_cons.count(); ++i) {
        const ObPwjConstraint *pwj_cons = strict_cons.at(i);
        if (OB_ISNULL(pwj_cons) || OB_UNLIKELY(pwj_cons->count() <= 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected pwj constraint", K(ret), K(pwj_cons));
        } else if (OB_FAIL(check_pwj_cons(*pwj_cons, base_location_cons,
                                          strict_pwj_comparer, pwj_map))) {
          LOG_WARN("failed to check pwj cons", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        GroupPWJTabletIdMap *group_pwj_map = nullptr;
        if (OB_FAIL(exec_ctx->get_group_pwj_map(group_pwj_map))) {
          LOG_WARN("failed to get exec group pwj map", K(ret));
        } else if (OB_FAIL(group_pwj_map->reuse())) {
          LOG_WARN("failed to reuse group pwj map", K(ret));
        }
        GroupPWJTabletIdInfo group_pwj_tablet_id_info;
        TabletIdArray &tablet_id_array = group_pwj_tablet_id_info.tablet_id_array_;
        for (int64_t group_id = 0; OB_SUCC(ret) && group_id < strict_cons.count(); ++group_id) {
          group_pwj_tablet_id_info.group_id_ = group_id;
          const ObPwjConstraint *pwj_cons = strict_cons.at(group_id);
          for (int64_t i = 0; OB_SUCC(ret) && i < pwj_cons->count(); ++i) {
            const int64_t table_idx = pwj_cons->at(i);
            uint64_t table_id = base_location_cons.at(table_idx).key_.table_id_;
            tablet_id_array.reset();
            if (!base_location_cons.at(table_idx).is_multi_part_insert()) {
              if (OB_FAIL(pwj_map.get_refactored(table_idx, tablet_id_array))) {
                if (OB_HASH_NOT_EXIST == ret) {
                  // means this is not a partition wise join table
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("failed to get refactored", K(ret));
                }
              } else if (OB_FAIL(group_pwj_map->set_refactored(table_id, group_pwj_tablet_id_info))) {
                LOG_WARN("failed to set refactored", K(ret));
                if (OB_HASH_EXIST == ret) {
                  ret = OB_SUCCESS;
                }
              }
            }
          }
        }
      }

      // 释放pwj_map的内存
      if (pwj_map.created()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = pwj_map.destroy()))) {
          LOG_WARN("failed to destroy pwj map", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::check_pwj_cons(const ObPwjConstraint &pwj_cons,
                              const ObIArray<LocationConstraint> &base_location_cons,
                              ObStrictPwjComparer &pwj_comparer,
                              PWJTabletIdMap &pwj_map) const
{
  int ret = OB_SUCCESS;
  bool is_same = true;
  ObTablePartitionInfo *first_table_partition_info = base_location_cons.at(pwj_cons.at(0)).table_partition_info_;
  if (OB_ISNULL(first_table_partition_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(first_table_partition_info));
  } else if (1 == first_table_partition_info->get_phy_tbl_location_info().get_partition_cnt()) {
    // all tables in pwj constraint are local or remote
    // alreay checked, do nothing
  } else {
    // distribute partition wise join
    LOG_DEBUG("check pwj constraint", K(pwj_cons), K(base_location_cons));
    pwj_comparer.reset();
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < pwj_cons.count(); ++i) {
      const int64_t table_idx = pwj_cons.at(i);
      ObTablePartitionInfo *table_part_info = NULL;
      ObSchemaGetterGuard *schema_guard = get_optimizer_context().get_schema_guard();
      ObSQLSessionInfo *session = get_optimizer_context().get_session_info();
      const ObTableSchema *table_schema = NULL;
      PwjTable table;
      if (table_idx < 0 || table_idx >= base_location_cons.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table index is invalid", K(ret), K(table_idx), K(base_location_cons.count()));
      } else if (OB_ISNULL(schema_guard) || OB_ISNULL(session) ||
                 OB_ISNULL(table_part_info = base_location_cons.at(table_idx).table_partition_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table part info", K(ret), K(table_part_info), K(schema_guard), K(session));
      } else if (OB_FAIL(schema_guard->get_table_schema(session->get_effective_tenant_id(),
                                                        base_location_cons.at(table_idx).key_.ref_table_id_,
                                                        table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(table_schema),
                                             K(base_location_cons.at(table_idx).key_.ref_table_id_));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema should not be null", K(table_schema), K(ret));
      } else if (OB_FAIL(table.init(*table_schema, table_part_info->get_phy_tbl_location_info()))) {
        LOG_WARN("failed to init owj table with sharding info", K(ret));
      } else if (OB_FAIL(pwj_comparer.add_table(table, is_same))) {
        LOG_WARN("failed to add table", K(ret));
      } else if (!is_same) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get not same table", KPC(first_table_partition_info), K(table));
      } else if (OB_FAIL(pwj_map.set_refactored(table_idx,
                                                pwj_comparer.get_tablet_id_group().at(i)))) {
        LOG_WARN("failed to set refactored", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::get_cached_hash_sharding_info(const ObIArray<ObRawExpr*> &hash_exprs,
                                             const EqualSets &equal_sets,
                                             ObShardingInfo *&cached_sharding)
{
  int ret = OB_SUCCESS;
  cached_sharding = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == cached_sharding && i < hash_dist_info_.count(); i++) {
    ObShardingInfo *temp_sharding = NULL;
    if (OB_ISNULL(temp_sharding = hash_dist_info_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (ObOptimizerUtil::is_exprs_equivalent(hash_exprs,
                                                    temp_sharding->get_partition_keys(),
                                                    equal_sets)) {
      cached_sharding = temp_sharding;
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::push_back_join_cond_if_needed(ObIArray<ObRawExpr*> &join_conds,
                                             ObRawExpr *cond)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(query_ctx = get_optimizer_context().get_query_ctx()) || OB_ISNULL(cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_ctx), K(cond));
  } else if (query_ctx->check_opt_compat_version(COMPAT_VERSION_4_2_5_BP7)
             && ObOptimizerUtil::find_item(new_or_quals_, cond)) {
    // do nothing, no need to keep new or quals on join conds or filters
  } else if (OB_FAIL(join_conds.push_back(cond))) {
    LOG_WARN("failed to push back join cond", K(ret));
  }
  return ret;
}

bool ObLogPlan::has_depend_table(const ObRelIds& table_ids)
{
  bool b_ret = false;
  for (int64_t i = 0; !b_ret && i < table_depend_infos_.count(); ++i) {
    TableDependInfo &info = table_depend_infos_.at(i);
    if (table_ids.has_member(info.table_idx_)) {
      b_ret = true;
    }
  }
  return b_ret;
}


int ObLogPlan::allocate_output_expr_for_values_op(ObLogicalOperator &values_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(optimizer_context_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    // Static typing engine need expr to output rows. We generate a const output expr
    // for values_op operator.
    ObConstRawExpr *output = NULL;
    if (OB_FAIL(optimizer_context_.get_expr_factory().create_raw_expr(T_VARCHAR, output))) {
      LOG_WARN("create const expr failed", K(ret));
    } else {
      ObObj v;
      v.set_varchar(" ");
      v.set_collation_type(ObCharset::get_system_collation());
      output->set_param(v);
      output->set_value(v);
      if (OB_FAIL(output->formalize(optimizer_context_.get_session_info()))) {
        LOG_WARN("const expr formalize failed", K(ret));
      } else if (OB_FAIL(values_op.get_output_exprs().push_back(output))) {
        LOG_WARN("add output expr failed", K(ret));
      } else {
        values_op.set_branch_id(0);
        values_op.set_id(0);
        values_op.set_op_id(0);
        if (OB_FAIL(get_optimizer_context().get_all_exprs().append(output))) {
          LOG_WARN("failed to append output", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObLogPlan::add_subquery_filter(ObRawExpr *qual)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExecParamRawExpr*, 4> onetime_exprs;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_onetime_exprs(qual, onetime_exprs))) {
    LOG_WARN("failed to get onetime exprs", K(ret));
  } else if (!ObOptimizerUtil::is_subset(onetime_exprs, onetime_params_)) {
    //属于当前stmt的onetime才需要分配subplan filter
  } else if (OB_FAIL(subquery_filters_.push_back(qual))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

int ObLogPlan::get_rowkey_exprs(const uint64_t table_id,
                                const uint64_t ref_table_id,
                                ObIArray<ObRawExpr*> &keys)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  if (OB_ISNULL(schema_guard = get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, ref_table_id, get_stmt(), table_schema))) {
    LOG_WARN("fail to get table schema", K(ref_table_id), K(table_schema), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema should not be null", K(table_schema), K(ret));
  } else if (OB_FAIL(get_rowkey_exprs(table_id, *table_schema, keys))) {
    LOG_WARN("failed to get rowkey exprs", K(ret));
  }
  return ret;
}

int ObLogPlan::get_rowkey_exprs(const uint64_t table_id,
                                const ObTableSchema &table_schema,
                                ObIArray<ObRawExpr*> &keys)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
  const ObColumnSchemaV2 *column_schema = NULL;
  const ColumnItem *column_item = NULL;
  ColumnItem column_item2;
  for (int i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
    uint64_t  column_id = OB_INVALID_ID;
    if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
      LOG_WARN("Failed to get column_id from rowkey_info", K(ret));
    } else if (NULL != (column_item = get_column_item_by_id(table_id, column_id))) {
      if (OB_FAIL(keys.push_back(column_item->expr_))) {
        LOG_WARN("failed to push column item", K(ret));
      }
    } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(column_id), K(ret));
    } else if (OB_FAIL(generate_column_expr(get_optimizer_context().get_expr_factory(), table_id,
                                            *column_schema, column_item2))) {
      LOG_WARN("failed to get rowkey exprs", K(ret));
    } else if (OB_FAIL(keys.push_back(column_item2.expr_))) {
      LOG_WARN("failed to push column item", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::get_index_column_items(ObRawExprFactory &expr_factory,
                                      uint64_t table_id,
                                      const share::schema::ObTableSchema &index_table_schema,
                                      common::ObIArray<ColumnItem> &index_columns)
{
  int ret = OB_SUCCESS;
  // get all the index keys
  const ObRowkeyInfo* rowkey_info = NULL;
  const ObColumnSchemaV2 *column_schema = NULL;
  uint64_t column_id = OB_INVALID_ID;
  if (index_table_schema.is_index_table()
      && is_virtual_table(index_table_schema.get_data_table_id())
      && !index_table_schema.is_ordered()) {
    // for virtual table and its hash index
    rowkey_info = &index_table_schema.get_index_info();
  } else {
    rowkey_info = &index_table_schema.get_rowkey_info();
  }
  const ColumnItem *column_item = NULL;
  ColumnItem column_item2;
  for (int col_idx = 0; OB_SUCC(ret) && col_idx < rowkey_info->get_size(); ++col_idx) {
    if (OB_FAIL(rowkey_info->get_column_id(col_idx, column_id))) {
      LOG_WARN("Failed to get column id", K(ret));
    } else if (OB_ISNULL(column_schema = index_table_schema.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(column_id), K(ret));
    } else if (NULL != (column_item = get_column_item_by_id(table_id, column_id))) {
      if (OB_FAIL(index_columns.push_back(*column_item))) {
        LOG_WARN("failed to push column item", K(ret));
      }
    } else if (OB_FAIL(generate_column_expr(expr_factory, table_id,
                                            *column_schema, column_item2))) {
      LOG_WARN("failed to get rowkey exprs", K(ret));
    } else if (OB_FAIL(index_columns.push_back(column_item2))) {
      LOG_WARN("failed to push column item", K(ret));
    }
  } // for end
  if (OB_SUCC(ret)) {
    LOG_TRACE("get range columns", K(index_columns));
  }
  return ret;
}

ObColumnRefRawExpr *ObLogPlan::get_column_expr_by_id(uint64_t table_id, uint64_t column_id) const
{
  const ColumnItem *column_item = get_column_item_by_id(table_id, column_id);
  return NULL == column_item ? NULL : column_item->expr_;
}

const ColumnItem *ObLogPlan::get_column_item_by_id(uint64_t table_id, uint64_t column_id) const
{
  const ColumnItem *column_item = NULL;
  if (OB_ISNULL(get_stmt())) {
    // do nothing
  } else {
    const common::ObIArray<ColumnItem> &stmt_column_items = get_stmt()->get_column_items();
    for (int64_t i = 0; NULL == column_item && i < stmt_column_items.count(); i++) {
      if (table_id == stmt_column_items.at(i).table_id_ &&
          column_id == stmt_column_items.at(i).column_id_) {
        column_item = &stmt_column_items.at(i);
      }
    }
    for (int64_t i = 0; NULL == column_item && i < column_items_.count(); i++) {
      if (table_id == column_items_.at(i).table_id_ &&
          column_id == column_items_.at(i).column_id_) {
        column_item = &column_items_.at(i);
      }
    }
  }
  return column_item;
}


int ObLogPlan::get_column_exprs(uint64_t table_id, ObIArray<ObColumnRefRawExpr*> &column_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_stmt()));
  } else if (OB_FAIL(get_stmt()->get_column_exprs(table_id, column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); ++i) {
      if (table_id == column_items_.at(i).table_id_
          && OB_FAIL(column_exprs.push_back(column_items_.at(i).expr_))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::generate_column_expr(ObRawExprFactory &expr_factory,
                                    const uint64_t &table_id,
                                    const ObColumnSchemaV2 &column_schema,
                                    ColumnItem &column_item)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  ObColumnRefRawExpr *rowkey;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument passed in", K(stmt), K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, column_schema, rowkey))) {
    LOG_WARN("build column expr failed", K(ret));
  } else if (OB_ISNULL(rowkey)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create raw expr for dummy output", K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table item by id failed", K(table_id));
  } else {
    rowkey->set_ref_id(table_id, column_schema.get_column_id());
    rowkey->get_relation_ids().reuse();
    rowkey->set_column_attr(table_item->get_table_name(), column_schema.get_column_name_str());
    rowkey->set_database_name(table_item->database_name_);
    if (!table_item->alias_name_.empty()) {
      rowkey->set_table_alias_name();
    }
    column_item.table_id_ = rowkey->get_table_id();
    column_item.column_id_ = rowkey->get_column_id();
    column_item.base_tid_ = table_item->ref_id_;
    column_item.base_cid_ = rowkey->get_column_id();
    column_item.column_name_ = rowkey->get_column_name();
    column_item.set_default_value(column_schema.get_cur_default_value());
    column_item.expr_ = rowkey;
    if (OB_FAIL(rowkey->add_relation_id(stmt->get_table_bit_index(table_id)))) {
      LOG_WARN("add relation id to expr failed", K(ret));
    } else if (OB_FAIL(rowkey->formalize(optimizer_context_.get_session_info()))) {
      LOG_WARN("formalize rowkey failed", K(ret));
    } else if (OB_FAIL(rowkey->pull_relation_id())) {
      LOG_WARN("failed to pullup relation ids", K(ret));
    } else if (OB_FAIL(column_items_.push_back(column_item))) {
      LOG_WARN("failed to push column item", K(ret));
    }
  }
  return ret;
}

//mysql mode need distinguish different of for update, eg:
/*
 * create table t1(c1 int primary key, c2 int);
 * create table t2(c1 int primary key, c2 int);
 * create table t3(c1 int primary key, c2 int);
 * select * from t1 where c2 in (select t2.c1 from t2,t3 for update wait 1) for update
 * ==> for update: t1
 *     for update wait 1: t2,t3;
*/
int ObLogPlan::candi_allocate_for_update()
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObSEArray<CandidatePlan, 8> best_plans;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (lib::is_oracle_mode()) {
    const ObSelectStmt *sel_stmt = static_cast<const ObSelectStmt *>(stmt);
    if (OB_UNLIKELY(!stmt->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is expected to be select", K(ret));
    } else if (sel_stmt->has_distinct() ||
               sel_stmt->has_group_by() ||
               sel_stmt->is_set_stmt()) {
      ret = OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT;
      LOG_WARN("for update can not exists in stmt with distint, groupby", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSEArray<uint64_t, 4> sfu_table_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
      const TableItem *table = NULL;
      if (OB_ISNULL(table = stmt->get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret), K(i), K(table));
      } else if (!table->for_update_ || table_is_allocated_for_update(table->table_id_)) {
        // do nothing
      } else if (OB_FAIL(sfu_table_list.push_back(table->table_id_))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_SUCC(ret) && !sfu_table_list.empty()) {
      if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_, best_plans))) {
        LOG_WARN("failed to get minimal cost candidates", K(ret));
      } else {
        OPT_TRACE_TITLE("start generate for update plan");
        for (int64_t i = 0; OB_SUCC(ret) && i < best_plans.count(); i++) {
          ObSEArray<int64_t, 4> origin_alloc_sfu_list;
          OPT_TRACE("generate for update for plan:", best_plans.at(i));
          if (i != best_plans.count() - 1 &&
              OB_FAIL(origin_alloc_sfu_list.assign(get_alloc_sfu_list()))) {
            LOG_WARN("failed to assign", K(ret));
          } else if (OB_FAIL(allocate_for_update_as_top(best_plans.at(i).plan_tree_,
                                                                   sfu_table_list))) {
            LOG_WARN("allocate for update as top", K(ret));
          } else if (i != best_plans.count() - 1 &&
                     OB_FAIL(get_alloc_sfu_list().assign(origin_alloc_sfu_list))) {
            LOG_WARN("failed to assign", K(ret));
          } else {/*do nothing*/}
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(prune_and_keep_best_plans(best_plans))) {
            LOG_WARN("failed to prune and keep best plans", K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObDMLStmt *root_stmt = get_root_stmt();
    if (OB_ISNULL(root_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(root_stmt), K(ret));
    } else if (root_stmt->is_insert_stmt() || root_stmt->is_update_stmt() || root_stmt->is_delete_stmt()) {
      if (OB_FAIL(candi_allocate_material_for_dml())) {
        LOG_WARN("failed to candi allocate for update materialization", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::allocate_for_update_as_top(ObLogicalOperator *&top,
                                          ObIArray<uint64_t> &sfu_table_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sfu_table_list.count(); ++i) {
    ObSEArray<uint64_t, 1> need_alloc_list;
    if (table_is_allocated_for_update(sfu_table_list.at(i))) {
      //do nothing
    } else if (OB_FAIL(need_alloc_list.push_back(sfu_table_list.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(merge_same_sfu_table_list(sfu_table_list.at(i),
                                                 i + 1,
                                                 sfu_table_list,
                                                 need_alloc_list))) {
      LOG_WARN("failed to merge same sfu table list", K(ret));
    } else {
      int64_t wait_ts = 0;
      bool skip_locked = false;
      ObRawExpr *lock_rownum = NULL;
      ObSEArray<IndexDMLInfo*, 1> index_dml_infos;
      for (int64_t j = 0; OB_SUCC(ret) && j < need_alloc_list.count(); ++j) {
        IndexDMLInfo *index_dml_info = NULL;
        bool is_hierarchical = false;
        if (OB_FAIL(is_hierarchical_for_update(is_hierarchical))) {
          LOG_WARN("failed to check hierarchical query", K(ret));
        } else if (is_hierarchical) {
          if (OB_FAIL(get_table_for_update_info_for_hierarchical(need_alloc_list.at(j),
                                                                 index_dml_infos,
                                                                 wait_ts,
                                                                 skip_locked))) {
            LOG_WARN("failed to get table for update info for hierarchical", K(ret), KPC(get_stmt()));
          }
        } else if (OB_FAIL(get_table_for_update_info(need_alloc_list.at(j),
                                                     index_dml_info,
                                                     wait_ts,
                                                     skip_locked))) {
          LOG_WARN("failed to get for update info", K(ret), KPC(get_stmt()));
        } else if (OB_ISNULL(index_dml_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(index_dml_info));
        } else if (OB_FAIL(index_dml_infos.push_back(index_dml_info))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      }
      if (OB_SUCC(ret) && skip_locked) {
        ObPseudoColumnRawExpr* pseudo_expr = NULL;
        if (OB_FAIL(get_optimizer_context().get_expr_factory().create_raw_expr(
                      T_MULTI_LOCK_ROWNUM, pseudo_expr))) {
          LOG_WARN("fail to allocate rownum_expr_", K(ret));
        } else if (OB_ISNULL(pseudo_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rownum_expr_ is null");
        } else {
          lock_rownum = pseudo_expr;
          lock_rownum->set_data_type(ObIntType),
          lock_rownum->set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(create_for_update_plan(top,
                                                index_dml_infos,
                                                wait_ts,
                                                skip_locked,
                                                lock_rownum))) {
        LOG_WARN("failed to create update plan", K(ret));
      } else if (OB_FAIL(append(alloc_sfu_list_, need_alloc_list))) {
        LOG_WARN("failed to append", K(ret));
      } else {
        LOG_TRACE("succced to allocate for update as top", K(sfu_table_list), K(alloc_sfu_list_));
      }
    }
  }
  return ret;
}

int ObLogPlan::is_hierarchical_for_update(bool &is_hierarchical)
{
  int ret = OB_SUCCESS;
  is_hierarchical = false;
  const TableItem *table = NULL;
  const ObSelectStmt *ref_view = NULL;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (get_stmt()->is_hierarchical_query()) {
    is_hierarchical = true;
  } else if (get_stmt()->get_table_size() != 1) {
    is_hierarchical = false;
  } else if (OB_ISNULL(table = get_stmt()->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is NULL", K(ret));
  } else if (!table->for_update_ || !table->is_generated_table()) {
    is_hierarchical = false;
  } else if (OB_UNLIKELY(static_cast<const ObSelectStmt*>(get_stmt())->get_for_update_dml_infos().count() == 0)) {
    // For "select * from (select c1 from t1 connect by xxx) view1 where xxx for update;"
    ret = OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT;
    LOG_WARN("for update dml info is null", K(ret), KPC(static_cast<const ObSelectStmt*>(get_stmt())));
  } else if (OB_ISNULL(ref_view = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref query is NULL", K(ret));
  } else if (ref_view->is_hierarchical_query()) {
    is_hierarchical = true;
  }
  return ret;
}

int ObLogPlan::merge_same_sfu_table_list(uint64_t target_id,
                                         int64_t begin_idx,
                                         ObIArray<uint64_t> &src_table_list,
                                         ObIArray<uint64_t> &res_table_list)
{
  int ret = OB_SUCCESS;
  const TableItem *target_table = NULL;
  if (OB_ISNULL(get_stmt()) ||
      OB_ISNULL(target_table = get_stmt()->get_table_item_by_id(target_id)) ||
      OB_UNLIKELY(!target_table->for_update_ || begin_idx < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), KPC(target_table), K(target_id),
                                     K(begin_idx), K(ret));
  } else {
    for (int64_t i = begin_idx; OB_SUCC(ret) && i < src_table_list.count(); ++i) {
      const TableItem *tmp_table = NULL;
      if (OB_ISNULL(tmp_table = get_stmt()->get_table_item_by_id(src_table_list.at(i))) ||
          OB_UNLIKELY(!tmp_table->for_update_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(tmp_table));
      } else if (target_table->for_update_wait_us_ == tmp_table->for_update_wait_us_ &&
                 target_table->skip_locked_ == tmp_table->skip_locked_) {
        if (OB_FAIL(res_table_list.push_back(src_table_list.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObLogPlan::get_part_column_exprs(const uint64_t table_id,
                                     const uint64_t ref_table_id,
                                     ObIArray<ObRawExpr*> &part_exprs) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (NULL == (expr = get_stmt()->get_part_expr(table_id, ref_table_id))) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, temp_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_FAIL(part_exprs.assign(temp_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (NULL == (expr = get_stmt()->get_subpart_expr(table_id, ref_table_id))) {
    // do nothing
  } else if (FALSE_IT(temp_exprs.reset())) {
    /*do nothing*/
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, temp_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(part_exprs, temp_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogPlan::get_table_for_update_info(const uint64_t table_id,
                                         IndexDMLInfo *&index_dml_info,
                                         int64_t &wait_ts,
                                         bool &skip_locked)
{
  int ret = OB_SUCCESS;
  const TableItem *table = NULL;
  skip_locked = false;
  wait_ts = 0;
  index_dml_info = NULL;
  if (OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table = get_stmt()->get_table_item_by_id(table_id)) ||
      OB_UNLIKELY(!table->for_update_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), KPC(table), K(ret));
  } else {
    bool is_nullable = false;
    ObSEArray<ObRawExpr*, 4> temp_rowkeys;
    if (OB_UNLIKELY(!table->is_basic_table()) || OB_UNLIKELY(is_virtual_table(table->ref_id_))) {
      // invalid usage
      // bad case: select * from (select /*+no_merge*/ * from t1) for update
      ret = OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT;
      LOG_USER_ERROR(OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT);
    } else if (OB_FAIL(get_rowkey_exprs(table->table_id_, table->ref_id_, temp_rowkeys))) {
      LOG_WARN("failed to generate rowkey exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(get_stmt(),
                                                              table->table_id_,
                                                              is_nullable))) {
      LOG_WARN("failed to check is table on null side", K(ret));
    } else if (OB_ISNULL(index_dml_info = static_cast<IndexDMLInfo *>(
                       get_optimizer_context().get_allocator().alloc(sizeof(IndexDMLInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for index dml info", K(ret));
    } else {
      index_dml_info = new (index_dml_info) IndexDMLInfo();
      index_dml_info->table_id_ = table->table_id_;
      index_dml_info->loc_table_id_ = table->table_id_;
      index_dml_info->ref_table_id_ = table->ref_id_;
      index_dml_info->distinct_algo_ = T_DISTINCT_NONE;
      index_dml_info->rowkey_cnt_ = temp_rowkeys.count();
      index_dml_info->need_filter_null_ = is_nullable;
      index_dml_info->is_primary_index_ = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < temp_rowkeys.count(); ++i) {
        if (OB_ISNULL(temp_rowkeys.at(i)) ||
            OB_UNLIKELY(!temp_rowkeys.at(i)->is_column_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid rowkey expr", K(ret), K(temp_rowkeys.at(i)));
        } else if (OB_FAIL(index_dml_info->column_exprs_.push_back(
                              static_cast<ObColumnRefRawExpr*>(temp_rowkeys.at(i))))) {
          LOG_WARN("failed to push back column expr", K(ret));
        } else {
          temp_rowkeys.at(i)->set_explicited_reference();
        }
      }
      ObArray<ObRawExpr*> tmp_partkey_exprs;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(get_part_column_exprs(table->table_id_, table->ref_id_, tmp_partkey_exprs))) {
          LOG_WARN("get part column exprs failed", K(ret));
        }
      }
      for (int i = 0; OB_SUCC(ret) && i < tmp_partkey_exprs.count(); ++i) {
        if (OB_ISNULL(tmp_partkey_exprs.at(i)) ||
            OB_UNLIKELY(!tmp_partkey_exprs.at(i)->is_column_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid rowkey expr", K(ret), K(temp_rowkeys.at(i)));
        } else if (OB_FAIL(add_var_to_array_no_dup(index_dml_info->column_exprs_,
                                                   static_cast<ObColumnRefRawExpr*>(tmp_partkey_exprs.at(i))))) {
          LOG_WARN("failed to push back column expr", K(ret));
        } else {
          tmp_partkey_exprs.at(i)->set_explicited_reference();
        }
      }
      if (OB_SUCC(ret)) {
        wait_ts = table->for_update_wait_us_;
        skip_locked = table->skip_locked_;
        LOG_TRACE("Succeed to get table for update info", K(table_id), K(*index_dml_info),
                                                          K(wait_ts), K(skip_locked));
      }
    }
  }
  return ret;
}

int ObLogPlan::get_table_for_update_info_for_hierarchical(const uint64_t table_id,
                                                          ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                          int64_t &wait_ts,
                                                          bool &skip_locked)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = NULL;
  const TableItem *table = NULL;
  skip_locked = false;
  wait_ts = 0;
  if (OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table = get_stmt()->get_table_item_by_id(table_id)) ||
      OB_UNLIKELY(!table->for_update_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(get_stmt()), KPC(table));
  } else if (OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is not a select stmt", K(ret), KPC(get_stmt()));
  } else if (OB_FALSE_IT(select_stmt = static_cast<const ObSelectStmt*>(get_stmt()))) {
  } else if (table->is_generated_table()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_for_update_dml_infos().count(); ++i) {
      const ForUpdateDMLInfo *for_update_info = NULL;
      IndexDMLInfo *index_dml_info = NULL;
      const TableItem *base_table = NULL;
      if (OB_ISNULL(for_update_info = select_stmt->get_for_update_dml_infos().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("for update info is null", K(ret), KPC(select_stmt));
      } else if (table_id != for_update_info->table_id_) {
        // do nothing
      } else if (OB_ISNULL(index_dml_info = static_cast<IndexDMLInfo *>(
                        get_optimizer_context().get_allocator().alloc(sizeof(IndexDMLInfo))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for index dml info", K(ret));
      } else if (OB_ISNULL(table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref query is null", K(ret), KPC(table));
      } else if (OB_FAIL(check_hierarchical_for_update(table, for_update_info->base_table_id_))) {
        LOG_WARN("failed to check hierarchical for update", K(ret), KPC(for_update_info));
      } else {
        index_dml_info = new (index_dml_info) IndexDMLInfo();
        index_dml_info->table_id_ = for_update_info->table_id_;
        index_dml_info->loc_table_id_ = for_update_info->base_table_id_;
        index_dml_info->ref_table_id_ = for_update_info->ref_table_id_;
        index_dml_info->distinct_algo_ = T_DISTINCT_NONE;
        index_dml_info->rowkey_cnt_ = for_update_info->rowkey_cnt_;
        index_dml_info->need_filter_null_ = for_update_info->is_nullable_;
        index_dml_info->is_primary_index_ = true;
        skip_locked = for_update_info->skip_locked_;
        wait_ts = for_update_info->for_update_wait_us_;
        for (int64_t j = 0; OB_SUCC(ret) && j < for_update_info->unique_column_ids_.count(); ++j) {
          ObColumnRefRawExpr *col_expr = select_stmt->get_column_expr_by_id(for_update_info->table_id_,
                                                              for_update_info->unique_column_ids_.at(j));
          if (OB_ISNULL(col_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column expr is null", K(ret));
          } else if (OB_FAIL(index_dml_info->column_exprs_.push_back(col_expr))) {
            LOG_WARN("failed to push back column expr", K(ret));
          } else {
            col_expr->set_explicited_reference();
          }
        }

        if (OB_FAIL(ret)){
          // do nothing
        } else if (OB_SUCC(ret) && OB_FAIL(index_dml_infos.push_back(index_dml_info))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          LOG_TRACE("Succeed to get table for update info for hierarchical", K(table_id), K(for_update_info),
                    KPC(index_dml_info), K(wait_ts), K(skip_locked));
        }
      }
    }
  } else if (table->is_basic_table()) {
    IndexDMLInfo *index_dml_info = NULL;
    if (OB_FAIL(get_table_for_update_info(table_id,
                                          index_dml_info,
                                          wait_ts,
                                          skip_locked))) {
      LOG_WARN("failed to get for update info", K(ret));
    } else if (OB_ISNULL(index_dml_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(index_dml_info));
    } else if (OB_FAIL(index_dml_infos.push_back(index_dml_info))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table table type", K(ret), KPC(table));
  }
  return ret;
}

int ObLogPlan::check_hierarchical_for_update(const TableItem *table,
                                             uint64_t base_tid)
{
  int ret = OB_SUCCESS;
  TableItem *base_table = NULL;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_stmt()), K(table));
  } else if (OB_UNLIKELY(!table->is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is not a generated table", K(ret), KPC(table));
  } else if (OB_ISNULL(table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref query is NULL", K(ret));
  } else if (get_stmt()->is_hierarchical_query()) {
    base_table = table->ref_query_->get_table_item_by_id(base_tid);
  } else if (table->ref_query_->is_hierarchical_query()) {
    for (int i = 0; OB_SUCC(ret) && i < table->ref_query_->get_table_size(); ++i) {
      TableItem *mocked_join_table = table->ref_query_->get_table_item(i);
      if(OB_ISNULL(mocked_join_table)
          || OB_UNLIKELY(!mocked_join_table->is_generated_table())
          || OB_ISNULL(mocked_join_table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected mocked join table", K(ret), KPC(mocked_join_table));
      } else {
        base_table = mocked_join_table->ref_query_->get_table_item_by_id(base_tid);
        if (base_table != NULL) {
          break;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a hierarchical query", K(ret), KPC(get_stmt()));
  }
  // check base table
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(base_table)) {
    // base table is not in hierarchical query mocked view, maybe in the
    // inner generated table which not be merged.
    ret = OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT;
    LOG_WARN("base table is not in hierarchical query mocked view", K(ret), KPC(table->ref_query_), K(base_tid));
  } else if (OB_UNLIKELY(!base_table->is_basic_table() || is_virtual_table(base_table->ref_id_))) {
    // invalid usage
    ret = OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT;
    LOG_WARN("base table is not basic table", K(ret), KPC(base_table));
  }
  return ret;
}

int ObLogPlan::create_for_update_plan(ObLogicalOperator *&top,
                                      const ObIArray<IndexDMLInfo *> &index_dml_infos,
                                      int64_t wait_ts,
                                      bool skip_locked,
                                      ObRawExpr *lock_rownum)
{
  int ret = OB_SUCCESS;
  bool is_multi_part_dml = false;
  bool is_result_local = false;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(top) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_need_multi_partition_dml(*get_stmt(),
                                                    *top,
                                                    index_dml_infos,
                                                    false,
                                                    is_multi_part_dml,
                                                    is_result_local))) {
    LOG_WARN("failed to check need multi-partition dml", K(ret));
  } else if (((skip_locked && top->is_distributed())
              || (!is_multi_part_dml && is_result_local && top->is_sharding()))
              && OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
    LOG_WARN("fail to allocate exchange op", K(ret), K(skip_locked));
  } else if (OB_FAIL(allocate_for_update_as_top(top,
                                                is_multi_part_dml,
                                                index_dml_infos,
                                                wait_ts,
                                                skip_locked,
                                                lock_rownum))) {
    LOG_WARN("failed to allocate delete as top", K(ret));
  } else if (!skip_locked) {
    optimizer_context_.set_no_skip_for_update();
  }
  return ret;
}

int ObLogPlan::allocate_for_update_as_top(ObLogicalOperator *&top,
                                          const bool is_multi_part_dml,
                                          const ObIArray<IndexDMLInfo *> &index_dml_infos,
                                          int64_t wait_ts,
                                          bool skip_locked,
                                          ObRawExpr *lock_rownum)
{
  int ret = OB_SUCCESS;
  ObLogForUpdate *for_update_op = NULL;
  if (OB_ISNULL(for_update_op = static_cast<ObLogForUpdate *>(
                get_log_op_factory().allocate(*this, LOG_FOR_UPD)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate for update operator", K(ret));
  } else if (OB_FAIL(for_update_op->add_child(top))) {
    LOG_WARN("failed to add child for for_update operator", K(ret));
  } else if (OB_FAIL(for_update_op->get_index_dml_infos().assign(index_dml_infos))) {
    LOG_WARN("failed to assign index dml info", K(ret));
  } else {
    for_update_op->set_wait_ts(wait_ts);
    for_update_op->set_skip_locked(skip_locked);
    for_update_op->set_is_multi_part_dml(is_multi_part_dml);
    for_update_op->set_lock_rownum(lock_rownum);
    if (OB_FAIL(for_update_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = for_update_op;
      LOG_TRACE("Succeed to allocate for update as top", K(is_multi_part_dml), K(index_dml_infos),
                                                      K(wait_ts), K(skip_locked), KPC(lock_rownum));
    }
  }
  return ret;
}

bool ObLogPlan::table_is_allocated_for_update(const int64_t table_id)
{
  bool is_allocated = false;
  for (int64_t i = 0; !is_allocated && i < alloc_sfu_list_.count(); ++i) {
    is_allocated = table_id == alloc_sfu_list_.at(i);
  }
  return is_allocated;
}

int ObLogPlan::get_cache_calc_part_id_expr(int64_t table_id, int64_t ref_table_id,
    CalcPartIdType calc_type, ObRawExpr* &expr)
{
  int ret = OB_SUCCESS;
  bool find = false;
  expr = NULL;
  for (int64_t i = 0; !find && i < cache_part_id_exprs_.count(); ++i) {
    PartIdExpr &part_id_expr = cache_part_id_exprs_.at(i);
    if (part_id_expr.table_id_ == table_id &&
        part_id_expr.ref_table_id_ == ref_table_id &&
        calc_type == part_id_expr.calc_type_) {
      expr = part_id_expr.calc_part_id_expr_;
      find = true;
      LOG_TRACE("succeed to get chache calc_part_id_expr",
                K(table_id), K(ref_table_id), K(calc_type), K(*expr));
    }
  }
  return ret;
}

int ObLogPlan::get_part_exprs(uint64_t table_id,
                              uint64_t ref_table_id,
                              share::schema::ObPartitionLevel &part_level,
                              ObRawExpr *&part_expr,
                              ObRawExpr *&subpart_expr)
{
  int ret = OB_SUCCESS;
  part_expr = NULL;
  subpart_expr = NULL;
  const ObDMLStmt *stmt = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  const share::schema::ObTableSchema *table_schema = NULL;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(stmt = get_stmt())
      || OB_INVALID_ID == ref_table_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id", K(ref_table_id), K(ret));
  } else if (OB_ISNULL(schema_guard = get_optimizer_context().get_schema_guard()) ||
             OB_ISNULL(session = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(session->get_effective_tenant_id(),
                                                    ref_table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ref_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_schema));
  } else {
    part_level = table_schema->get_part_level();
    part_expr = stmt->get_part_expr(table_id, ref_table_id);
    subpart_expr = stmt->get_subpart_expr(table_id, ref_table_id);
    if (NULL != part_expr) {
      part_expr->set_part_key_reference();
    }
    if (NULL != subpart_expr) {
      subpart_expr->set_part_key_reference();
    }
  }

  return ret;
}

int ObLogPlan::create_hash_sortkey(const int64_t part_cnt,
                                   const common::ObIArray<OrderItem> &order_keys,
                                   OrderItem &hash_sortkey)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *hash_expr = NULL;
  ObRawExprFactory &expr_factory = get_optimizer_context().get_expr_factory();
  ObExecContext *exec_ctx = get_optimizer_context().get_exec_ctx();
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_HASH, hash_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_UNLIKELY(part_cnt > order_keys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected order_keys count", K(ret), K(part_cnt), K(order_keys));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; ++i) {
      if (OB_FAIL(hash_expr->add_param_expr(order_keys.at(i).expr_))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(hash_expr->formalize(exec_ctx->get_my_session()))) {
    LOG_WARN("failed to formalize expr", K(ret));
  } else {
    hash_sortkey.expr_ = hash_expr;
    hash_sortkey.order_type_ = default_asc_direction();
  }
  return ret;
}

int ObLogPlan::gen_calc_part_id_expr(uint64_t table_id,
                                     uint64_t ref_table_id,
                                     CalcPartIdType calc_id_type,
                                     ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  expr = NULL;
  ObSQLSessionInfo *session = NULL;
  share::schema::ObPartitionLevel part_level = share::schema::PARTITION_LEVEL_MAX;
  ObRawExpr *part_expr = NULL;
  ObRawExpr *subpart_expr = NULL;
  if (OB_FAIL(get_cache_calc_part_id_expr(table_id, ref_table_id, calc_id_type, expr))) {
    LOG_WARN("failed to get cache calc part id expr", K(ret));
  } else if (NULL != expr) {
    //do nothing
  } else if (OB_INVALID_ID == ref_table_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect table if", K(ret));
  } else if (OB_ISNULL(session = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null");
  } else if (OB_FAIL(get_part_exprs(table_id,
                                    ref_table_id,
                                    part_level,
                                    part_expr,
                                    subpart_expr))) {
    LOG_WARN("fail to get part exprs", K(ret));
  } else {
    ObRawExprFactory &expr_factory = get_optimizer_context().get_expr_factory();
    if (CALC_TABLET_ID == calc_id_type) {
      if (OB_FAIL(ObRawExprUtils::build_calc_tablet_id_expr(expr_factory,
                                                            *session,
                                                            ref_table_id,
                                                            part_level,
                                                            part_expr,
                                                            subpart_expr,
                                                            expr))) {
        LOG_WARN("fil to build table location expr", K(ret));
      }
    } else if (CALC_PARTITION_ID == calc_id_type) {
      if (OB_FAIL(ObRawExprUtils::build_calc_part_id_expr(expr_factory,
                                                          *session,
                                                          ref_table_id,
                                                          part_level,
                                                          part_expr,
                                                          subpart_expr,
                                                          expr))) {
        LOG_WARN("fail to build table location expr", K(ret));
      }
    } else if (OB_FAIL(ObRawExprUtils::build_calc_partition_tablet_id_expr(expr_factory,
                                                                           *session,
                                                                           ref_table_id,
                                                                           part_level,
                                                                           part_expr,
                                                                           subpart_expr,
                                                                           expr))) {
      LOG_WARN("fail to build table location expr", K(ret));
    }
    if (OB_SUCC(ret)) {
      PartIdExpr part_id_expr;
      part_id_expr.table_id_ = table_id;
      part_id_expr.ref_table_id_ = ref_table_id;
      part_id_expr.calc_part_id_expr_ = expr;
      part_id_expr.calc_type_ = calc_id_type;
      if (OB_FAIL(cache_part_id_exprs_.push_back(part_id_expr))) {
        LOG_WARN("failed to push back part id expr", K(ret));
      }
    }
  }
  return ret;
}

// this function is used to allocate the material operator for dml plan
// to prevent data from being returned during the DML execution process,
// which would cause the -6005 error to be non-retryable
int ObLogPlan::candi_allocate_material_for_dml()
{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 8> best_plans;
  ObExchangeInfo exch_info;
  if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_, best_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < best_plans.count(); i++) {
    if (OB_ISNULL(best_plans.at(i).plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pointer", K(ret));
    } else if (best_plans.at(i).plan_tree_->is_distributed() &&
              OB_FAIL(allocate_exchange_as_top(best_plans.at(i).plan_tree_, exch_info))) {
      LOG_WARN("failed to allocate exchange op", K(ret));
    } else if (OB_FAIL(allocate_material_as_top(best_plans.at(i).plan_tree_))) {
      LOG_WARN("fail to allocate material as top", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_and_keep_best_plans(best_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogPlan::add_extra_dependency_table() const
{
  return OB_SUCCESS;
}

int ObLogPlan::simplify_win_expr(ObLogicalOperator* child_op, ObWinFunRawExpr &win_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(simplify_win_partition_exprs(child_op, win_expr))) {
    LOG_WARN("failed to simplify win partition exprs", K(ret));
  } else if(OB_FAIL(simplify_win_order_items(child_op, win_expr))) {
    LOG_WARN("failed to simplify win order items", K(ret));
  }
  return ret;
}

int ObLogPlan::perform_simplify_win_expr(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  ObLogWindowFunction *win_func = NULL;
  if (OB_NOT_NULL(win_func = dynamic_cast<ObLogWindowFunction *>(op))) {
    for (int64_t i = 0; OB_SUCC(ret) && i < win_func->get_window_exprs().count(); ++i) {
      ObWinFunRawExpr *win_expr = win_func->get_window_exprs().at(i);
      if (OB_UNLIKELY(op->get_num_of_child() == 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("window function op should have child", K(ret));
      } else if (OB_FAIL(simplify_win_expr(op->get_child(ObLogicalOperator::first_child), *win_expr))) {
        LOG_WARN("transform win func failed", K(ret));
      } else if (OB_FAIL(win_expr->formalize(get_optimizer_context().get_session_info()))) {
        LOG_WARN("formalize win func expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::simplify_win_partition_exprs(ObLogicalOperator* child_op,
                                               ObWinFunRawExpr& win_expr)
{
  int ret = OB_SUCCESS;
  bool is_const = false;
  ObIArray<ObRawExpr *>& partition_exprs = win_expr.get_partition_exprs();
  ObSEArray<ObRawExpr *, 4> new_partition_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_exprs.count(); ++i) {
    ObRawExpr *part_expr = partition_exprs.at(i);
    if (OB_ISNULL(part_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part expr is null", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(part_expr,
                                                      child_op->get_output_equal_sets(),
                                                      child_op->get_output_const_exprs(),
                                                      onetime_query_refs_,
                                                      is_const))) {
      LOG_WARN("check is const expr failed", K(ret));
    } else if (is_const) {
    } else if (OB_FAIL(new_partition_exprs.push_back(part_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(partition_exprs.reuse())) {
  } else if (OB_UNLIKELY(new_partition_exprs.empty())) {
    // do nothing, item is empty, no need simplify
  } else if (OB_FAIL(ObOptimizerUtil::simplify_exprs(child_op->get_fd_item_set(),
                                                     child_op->get_output_equal_sets(),
                                                     child_op->get_output_const_exprs(),
                                                     new_partition_exprs,
                                                     partition_exprs))) {
    LOG_WARN("failed to simplify exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObLogPlan::simplify_win_order_items(ObLogicalOperator* child_op,
                                          ObWinFunRawExpr& win_expr
                                           )
{
  int ret = OB_SUCCESS;
  ObRawExpr* first_order_expr = NULL;
  bool is_const = false;
  ObIArray<OrderItem> &order_items = win_expr.get_order_items();
  ObSEArray<OrderItem, 4> new_order_items;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
    ObRawExpr *order_expr = order_items.at(i).expr_;
    if (OB_ISNULL(order_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("order expr is null", K(ret));
    } else if (i == 0 && OB_FALSE_IT(first_order_expr = order_expr))  {
    } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(order_expr,
                                                      child_op->get_output_equal_sets(),
                                                      child_op->get_output_const_exprs(),
                                                      onetime_query_refs_,
                                                      is_const))) {
      LOG_WARN("check is const expr failed", K(ret));
    } else if (is_const) {
    } else if (OB_FAIL(new_order_items.push_back(order_items.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(order_items.reuse())) {
  } else if (OB_UNLIKELY(new_order_items.empty())) {
    // do nothing, item is empty, no need simplify
  } else if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(child_op->get_fd_item_set(),
                                                             child_op->get_output_equal_sets(),
                                                             child_op->get_output_const_exprs(),
                                                             onetime_query_refs_,
                                                             new_order_items,
                                                             order_items))) {
    LOG_WARN("failed to simplify ordered exprs", K(ret));
  }

  if (OB_SUCC(ret)
      && order_items.count() == 0
      && OB_NOT_NULL(first_order_expr)) {
    // for computing range frame
    // at least one order item when executing
    if (win_expr.win_type_ == WINDOW_RANGE &&
        OB_FAIL(ObTransformUtils::rebuild_win_compare_range_expr(&get_optimizer_context().get_expr_factory(),
                                                                 win_expr, first_order_expr))) {
      LOG_WARN("failed to rebuild win compare range expr", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::compute_subplan_filter_repartition_distribution_info(ObLogicalOperator *top,
                                                                    const ObIArray<ObLogicalOperator*> &subquery_ops,
                                                                    const ObIArray<ObExecParamRawExpr *> &params,
                                                                    ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  EqualSets input_esets;
  if (OB_ISNULL(top) || OB_UNLIKELY(subquery_ops.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(top), K(subquery_ops.empty()));
  } else if (OB_FAIL(append(input_esets, top->get_output_equal_sets()))) {
    LOG_WARN("failed to append equal sets", K(ret));
  } else {
    ObLogicalOperator *child = NULL;
    ObLogicalOperator *right_child = subquery_ops.at(0);
    ObLogicalOperator *max_parallel_child = NULL;
    ObSEArray<ObRawExpr*, 4> left_keys;
    ObSEArray<ObRawExpr*, 4> right_keys;
    ObSEArray<bool, 4> null_safe_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_ops.count(); i++) {
      if (OB_ISNULL(child = subquery_ops.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(append(input_esets, child->get_output_equal_sets()))) {
        LOG_WARN("failed to append input equal sets", K(ret));
      } else {
        max_parallel_child = (NULL == max_parallel_child
                              || max_parallel_child->get_parallel() < child->get_parallel())
                             ? child : max_parallel_child;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_subplan_filter_equal_keys(right_child,
                                                     params,
                                                     left_keys,
                                                     right_keys,
                                                     null_safe_info))) {
      LOG_WARN("failed to get subplan filter equal key", K(ret));
    } else if (OB_FAIL(compute_repartition_distribution_info(input_esets,
                                                     left_keys,
                                                     right_keys,
                                                     *right_child,
                                                     exch_info))) {
      LOG_WARN("failed to compute repartition distribution info", K(ret));
    } else if (OB_FAIL(exch_info.server_list_.assign(max_parallel_child->get_server_list()))) {
      LOG_WARN("failed to assign server list", K(ret));
    } else {
      exch_info.parallel_ = max_parallel_child->get_parallel();
      exch_info.server_cnt_ = max_parallel_child->get_server_cnt();
      exch_info.unmatch_row_dist_method_ = ObPQDistributeMethod::DROP;
      LOG_TRACE("succeed to compute repartition distribution info", K(exch_info));
    }
  }
  return ret;
}

int ObLogPlan::perform_adjust_onetime_expr(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  ObLogSubPlanFilter *subplan_filter = NULL;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null op", K(ret));
  } else if (OB_FAIL(init_onetime_replaced_exprs_if_needed())) {
    LOG_WARN("faile to init onetime replaced exprs", K(ret));
  } else if (NULL != (subplan_filter = dynamic_cast<ObLogSubPlanFilter *>(op))) {
    if (OB_FAIL(subplan_filter->replace_nested_subquery_exprs(onetime_replacer_))) {
      LOG_WARN("failed to replace nested subquery exprs", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(op->replace_op_exprs(onetime_replacer_))) {
    LOG_WARN("failed to replace onetime subquery", K(ret));
  }
  return ret;
}

int ObLogPlan::init_onetime_replaced_exprs_if_needed()
{
  int ret = OB_SUCCESS;
  if (0 == onetime_params_.count() || onetime_replaced_exprs_.count() > 0) {
    // do nothing
  } else if (OB_ISNULL(onetime_copier_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("onetime expr copier is null", K(ret));
  } else if (OB_FAIL(onetime_copier_->get_copied_exprs(onetime_replaced_exprs_))) {
    LOG_WARN("failed to get copied exprs", K(ret));
  } else if (OB_FAIL(onetime_replacer_.add_replace_exprs(onetime_replaced_exprs_))) {
    LOG_WARN("failed to add replace exprs", K(ret));
  }
  return ret;
}

int ObLogPlan::allocate_material_for_recursive_cte_plan(ObLogicalOperator &op)
{
  int ret = OB_SUCCESS;
  ObLogPlan *log_plan = NULL;
  int64_t fake_cte_pos = -1;
  ObIArray<ObLogicalOperator*> &child_ops = op.get_child_list();
  for (int64_t i = 0; OB_SUCC(ret) && fake_cte_pos == -1 && i < child_ops.count(); i++) {
    if (OB_ISNULL(child_ops.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (child_ops.at(i)->get_contains_fake_cte()) {
      fake_cte_pos = i;
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret) && fake_cte_pos != -1) {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_ops.count(); i++) {
      if (OB_ISNULL(child_ops.at(i)) || OB_ISNULL(log_plan = child_ops.at(i)->get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (op.get_type() == log_op_def::LOG_JOIN &&
                 static_cast<ObLogJoin&>(op).is_nlj_with_param_down() &&
                 1 == i) {
        // do nothing
      } else if (op.get_type() == log_op_def::LOG_SUBPLAN_FILTER &&
                 static_cast<ObLogSubPlanFilter&>(op).has_exec_params() &&
                 i > 0) {
        // do nothing
      } else if (i == fake_cte_pos) {
        if (OB_FAIL(SMART_CALL(allocate_material_for_recursive_cte_plan(*child_ops.at(i))))) {
          LOG_WARN("failed to adjust recursive cte plan", K(ret));
        } else { /*do nothing*/ }
      } else if (log_op_def::LOG_MATERIAL != child_ops.at(i)->get_type() &&
                 log_op_def::LOG_TABLE_SCAN != child_ops.at(i)->get_type() &&
                 log_op_def::LOG_EXPR_VALUES != child_ops.at(i)->get_type()) {
        bool is_plan_root = child_ops.at(i)->is_plan_root();
        ObLogicalOperator *orig_op = child_ops.at(i);
        ObLogicalOperator *parent_op = orig_op->get_parent();
        child_ops.at(i)->set_is_plan_root(false);
        if (OB_FAIL(log_plan->allocate_material_as_top(child_ops.at(i)))) {
          LOG_WARN("failed to allocate materialize as top", K(ret));
        } else if (OB_FALSE_IT(child_ops.at(i)->set_parent(parent_op))) {
          // do nothing
        } else if (is_plan_root) {
          child_ops.at(i)->mark_is_plan_root();
          child_ops.at(i)->get_plan()->set_plan_root(child_ops.at(i));
          if (OB_FAIL(child_ops.at(i)->get_output_exprs().assign(orig_op->get_output_exprs()))) {
            LOG_WARN("failed to set plan root output", K(ret));
          }
        }
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogPlan::set_advisor_table_id(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is null", K(ret), K(op));
  } else if (OB_NOT_NULL(op->get_sharding()) &&
             OB_NOT_NULL(op->get_sharding()->get_phy_table_location_info()) &&
             (op->get_sharding()->is_local() || op->get_sharding()->is_remote())) {
    if (OB_FAIL(negotiate_advisor_table_id(op))) {
      LOG_WARN("failed to negotiate advise table id", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(set_advisor_table_id(op->get_child(i))))) {
        LOG_WARN("failed to update advise table id", K(ret));
      }
    }
  }
  return ret;
}
int ObLogPlan::negotiate_advisor_table_id(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  uint64_t base_table_id = OB_INVALID_ID;
  uint64_t dup_table_id = OB_INVALID_ID;
  ObArray<ObLogicalOperator *> all_ops;
  ObArray<ObLogTableScan *> all_dup_tables;
  for (int64_t i = -1; OB_SUCC(ret) && i < all_ops.count(); ++i) {
    ObLogicalOperator *cur_op = (i == -1 ? op : all_ops.at(i));
    if (OB_ISNULL(cur_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current operator is null", K(ret));
    } else if (cur_op->is_table_scan()) {
      ObLogTableScan *table_scan = static_cast<ObLogTableScan *>(cur_op);
      if (table_scan->is_duplicate_table()) {
        if (OB_FAIL(all_dup_tables.push_back(table_scan))) {
          LOG_WARN("failed to push back duplicate table scan", K(ret));
        } else if (OB_INVALID_ID == dup_table_id) {
          dup_table_id = table_scan->get_table_id();
        }
      } else {
        if (OB_INVALID_ID == base_table_id) {
          base_table_id = table_scan->get_table_id();
        }
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < cur_op->get_num_of_child(); ++j) {
      ObLogicalOperator * child = cur_op->get_child(j);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (log_op_def::LOG_EXCHANGE == child->get_type()) {
        if (OB_FAIL(SMART_CALL(set_advisor_table_id(child)))) {
          LOG_WARN("failed to update advise table id", K(ret));
        }
      } else if (OB_FAIL(all_ops.push_back(child))) {
        LOG_WARN("failed to push back child operator", K(ret));
      }
    }
  }

  if (base_table_id != OB_INVALID_ID || dup_table_id != OB_INVALID_ID) {
    uint64_t final_table_id =  (base_table_id == OB_INVALID_ID ? dup_table_id : base_table_id);
    for (int64_t i = 0; OB_SUCC(ret) && i < all_dup_tables.count(); ++i) {
      if (final_table_id != all_dup_tables.at(i)->get_table_id()) {
        all_dup_tables.at(i)->set_advisor_table_id(final_table_id);
      }
      // LOG_INFO("link debug", K(all_dup_tables.at(i)->get_table_id()), K(final_table_id));
    }
  }
  return ret;
}

int ObLogPlan::find_possible_join_filter_tables(ObLogicalOperator *op,
                                                const JoinFilterPushdownHintInfo &hint_info,
                                                ObRelIds &right_tables,
                                                bool is_current_dfo,
                                                bool is_fully_partition_wise,
                                                int64_t current_dfo_level,
                                                const ObIArray<ObRawExpr*> &left_join_conditions,
                                                const ObIArray<ObRawExpr*> &right_join_conditions,
                                                ObIArray<JoinFilterInfo> &join_filter_infos)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt;
  if (OB_ISNULL(op)
      || OB_ISNULL(stmt = get_stmt())
      || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (op->get_type() == log_op_def::LOG_SET) {
    ObLogSet *log_set = static_cast<ObLogSet *>(op);
    bool is_ext_pw = (log_set->get_distributed_algo() == DistAlgo::DIST_SET_PARTITION_WISE);
    is_fully_partition_wise |= (op->is_fully_partition_wise() || is_ext_pw);
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
      ObLogicalOperator* child_op;
      ObLogPlan* child_plan;
      if (OB_ISNULL(child_op = op->get_child(i)) ||
          OB_ISNULL(child_plan = child_op->get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(child_plan->pushdown_join_filter_into_subquery(stmt,
                                                                        child_op,
                                                                        ObTabletID::INVALID_TABLET_ID,
                                                                        hint_info,
                                                                        is_current_dfo,
                                                                        is_fully_partition_wise,
                                                                        current_dfo_level,
                                                                        left_join_conditions,
                                                                        right_join_conditions,
                                                                        join_filter_infos))) {
        LOG_WARN("failed to find pushdown join filter table", K(ret));
      }
    }
  } else if (!op->get_table_set().overlap(right_tables)) {
    /* do nothing */
  } else if (op->is_table_scan()) {
    ObLogTableScan* scan = static_cast<ObLogTableScan*>(op);
    bool can_join_filter = false;
    const ObJoinFilterHint *force_hint = NULL;
    bool can_part_join_filter = false;
    const ObJoinFilterHint *force_part_hint = NULL;
    if (OB_FAIL(hint_info.check_use_join_filter(*stmt,
                                                stmt->get_query_ctx()->get_query_hint(),
                                                scan->get_table_id(),
                                                false,
                                                can_join_filter,
                                                force_hint))) {
      LOG_WARN("failed to check use join filter", K(ret));
    } else if (!is_fully_partition_wise &&
               OB_FAIL(hint_info.check_use_join_filter(*stmt,
                                                       stmt->get_query_ctx()->get_query_hint(),
                                                       scan->get_table_id(),
                                                       true,
                                                       can_part_join_filter,
                                                       force_part_hint))) {
      LOG_WARN("faile to check use join filter", K(ret));
    } else if ((!can_join_filter && !can_part_join_filter)
               || (scan->use_das() && NULL == force_hint)) {
      // do nothing, hint disable the join filter or das scan without force hint
    } else {
      JoinFilterInfo info;
      info.table_id_ = scan->get_table_id();
      info.filter_table_id_ = hint_info.filter_table_id_;
      info.ref_table_id_ = scan->get_ref_table_id();
      info.index_id_ = scan->get_index_table_id();
      info.sharding_ = scan->get_strong_sharding();
      info.row_count_ = scan->get_output_row_count();
      info.can_use_join_filter_ = can_join_filter;
      info.force_filter_ = force_hint;
      info.need_partition_join_filter_ = can_part_join_filter;
      info.force_part_filter_ = force_part_hint;
      info.in_current_dfo_ = is_current_dfo;
      if (info.can_use_join_filter_ || info.need_partition_join_filter_) {
        if (OB_FAIL(get_join_filter_exprs(left_join_conditions,
                                          right_join_conditions,
                                          info))) {
          LOG_WARN("failed to get join filter exprs", K(ret));
        } else if (OB_FAIL(fill_join_filter_info(info))) {
          LOG_WARN("failed to fill join filter info");
        } else if(OB_FAIL(join_filter_infos.push_back(info))) {
          LOG_WARN("failed to push back info", K(ret));
        }
      }
    }
  } else if (log_op_def::LOG_TEMP_TABLE_ACCESS == op->get_type()) {
    const ObLogTempTableAccess* temp_table = static_cast<const ObLogTempTableAccess*>(op);
    bool can_join_filter = false;
    const ObJoinFilterHint *force_hint = NULL;
    if (OB_FAIL(hint_info.check_use_join_filter(*stmt,
                                                stmt->get_query_ctx()->get_query_hint(),
                                                temp_table->get_table_id(),
                                                false,
                                                can_join_filter,
                                                force_hint))) {
      LOG_WARN("failed to check use join filter", K(ret));
    } else if (can_join_filter) {
      JoinFilterInfo info;
      info.table_id_ = temp_table->get_table_id();
      info.filter_table_id_ = hint_info.filter_table_id_;
      info.row_count_ = temp_table->get_card();
      info.can_use_join_filter_ = true;
      info.force_filter_ = force_hint;
      info.need_partition_join_filter_ = false;
      info.force_part_filter_ = NULL;
      info.in_current_dfo_ = is_current_dfo;
      if (OB_FAIL(get_join_filter_exprs(left_join_conditions,
                                        right_join_conditions,
                                        info))) {
        LOG_WARN("failed to get join filter exprs", K(ret));
      } else if (OB_FAIL(fill_join_filter_info(info))) {
        LOG_WARN("failed to fill join filter info");
      } else if (OB_FAIL(join_filter_infos.push_back(info))) {
      LOG_WARN("failed to push back info", K(ret));
      }
    }
  } else if (op->get_type() == log_op_def::LOG_SUBPLAN_SCAN) {
    ObLogPlan* child_plan;
    ObLogicalOperator* child_op;
    ObSEArray<ObRawExpr*, 4> pushdown_left_quals;
    ObSEArray<ObRawExpr*, 4> pushdown_right_quals;
    ObSqlBitSet<> table_set;
    uint64_t subquery_id = static_cast<ObLogSubPlanScan*>(op)->get_subquery_id();
    if (OB_UNLIKELY(op->get_num_of_child() == 0) ||
        OB_ISNULL(child_op = op->get_child(ObLogicalOperator::first_child)) ||
        OB_ISNULL(child_plan = child_op->get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected subplan scan", K(ret));
    } else if (OB_FAIL(table_set.add_member(stmt->get_table_bit_index(subquery_id)))) {
      LOG_WARN("failed to add member into table set", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::extract_pushdown_join_filter_quals(left_join_conditions,
                                                                           right_join_conditions,
                                                                           table_set,
                                                                           pushdown_left_quals,
                                                                           pushdown_right_quals))) {
      LOG_WARN("failed to extract pushdown quals", K(ret));
    } else if (OB_FAIL(child_plan->pushdown_join_filter_into_subquery(
                                   stmt,
                                   child_op,
                                   subquery_id,
                                   hint_info,
                                   is_current_dfo,
                                   is_fully_partition_wise,
                                   current_dfo_level,
                                   pushdown_left_quals,
                                   pushdown_right_quals,
                                   join_filter_infos))) {
      LOG_WARN("failed to find pushdown join filter table", K(ret));
    }
  } else if (log_op_def::LOG_JOIN == op->get_type()) {
    ObLogJoin* join_op = static_cast<ObLogJoin*>(op);
    ObLogicalOperator* left_op;
    ObLogicalOperator* right_op;
    is_fully_partition_wise |= join_op->is_fully_partition_wise();
    if (OB_UNLIKELY(2 != op->get_num_of_child()) ||
        OB_ISNULL(left_op = op->get_child(ObLogicalOperator::first_child)) ||
        OB_ISNULL(right_op = op->get_child(ObLogicalOperator::second_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(SMART_CALL(find_possible_join_filter_tables(left_op,
                                                                    hint_info,
                                                                    right_tables,
                                                                    is_current_dfo,
                                                                    is_fully_partition_wise,
                                                                    current_dfo_level,
                                                                    left_join_conditions,
                                                                    right_join_conditions,
                                                                    join_filter_infos)))) {
      LOG_WARN("failed to find shuffle table scan", K(ret));
    } else if (OB_FAIL(SMART_CALL(find_possible_join_filter_tables(right_op,
                                                                    hint_info,
                                                                    right_tables,
                                                                    is_current_dfo,
                                                                    is_fully_partition_wise,
                                                                    current_dfo_level,
                                                                    left_join_conditions,
                                                                    right_join_conditions,
                                                                    join_filter_infos)))) {
      LOG_WARN("failed to find shuffle table scan", K(ret));
    }
  } else if (log_op_def::LOG_EXCHANGE == op->get_type() &&
             static_cast<ObLogExchange*>(op)->is_consumer() &&
             static_cast<ObLogExchange*>(op)->is_local()) {
    /* do nothing */
  } else if (log_op_def::LOG_EXCHANGE == op->get_type() &&
             static_cast<ObLogExchange*>(op)->is_consumer() &&
             (OB_FALSE_IT(is_current_dfo = false) ||
              OB_FALSE_IT(current_dfo_level = (current_dfo_level == -1) ? -1 : current_dfo_level + 1) ||
              current_dfo_level >= 2)) {
    /* do nothing */
  } else if (log_op_def::LOG_SUBPLAN_FILTER == op->get_type()) {
    is_fully_partition_wise |= op->is_fully_partition_wise();
    if (OB_FAIL(SMART_CALL(find_possible_join_filter_tables(op->get_child(ObLogicalOperator::first_child),
                                                            hint_info,
                                                            right_tables,
                                                            is_current_dfo,
                                                            is_fully_partition_wise,
                                                            current_dfo_level,
                                                            left_join_conditions,
                                                            right_join_conditions,
                                                            join_filter_infos)))) {
      LOG_WARN("failed to find shuffle table scan", K(ret));
    }
  } else {
    is_fully_partition_wise |= op->is_fully_partition_wise();
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(find_possible_join_filter_tables(op->get_child(i),
                                                              hint_info,
                                                              right_tables,
                                                              is_current_dfo,
                                                              is_fully_partition_wise,
                                                              current_dfo_level,
                                                              left_join_conditions,
                                                              right_join_conditions,
                                                              join_filter_infos)))) {
        LOG_WARN("failed to find shuffle table scan", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::pushdown_join_filter_into_subquery(const ObDMLStmt *parent_stmt,
                                                  ObLogicalOperator* child_op,
                                                  uint64_t subquery_id,
                                                  const JoinFilterPushdownHintInfo &hint_info,
                                                  bool is_current_dfo,
                                                  bool is_fully_partition_wise,
                                                  int64_t current_dfo_level,
                                                  const ObIArray<ObRawExpr*> &left_join_conditions,
                                                  const ObIArray<ObRawExpr*> &right_join_conditions,
                                                  ObIArray<JoinFilterInfo> &join_filter_infos)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *child_stmt = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSEArray<ObRawExpr*, 4> candi_left_filters;
  ObSEArray<ObRawExpr*, 4> candi_right_quals;
  ObSEArray<ObRawExpr*, 4> candi_right_filters;
  ObRelIds right_tables;
  bool can_pushdown = false;
  if (OB_ISNULL(child_op) ||
      OB_ISNULL(session_info = get_optimizer_context().get_session_info()) ||
      OB_ISNULL(expr_factory = &get_optimizer_context().get_expr_factory()) ||
      OB_ISNULL(child_stmt = static_cast<const ObSelectStmt*>(get_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::pushdown_join_filter_into_subquery(*parent_stmt,
                                                                         *child_stmt,
                                                                         left_join_conditions,
                                                                         right_join_conditions,
                                                                         candi_left_filters,
                                                                         candi_right_quals,
                                                                         can_pushdown))) {
    LOG_WARN("failed to pushdown join filter into subquery", K(ret));
  } else if (!can_pushdown) {
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::rename_pushdown_filter(*parent_stmt,
                                                             *child_stmt,
                                                             subquery_id,
                                                             session_info,
                                                             *expr_factory,
                                                             candi_right_quals,
                                                             candi_right_filters))) {
    LOG_WARN("failed to rename pushdown filter", K(ret));
  } else if (OB_FAIL(get_table_ids(candi_right_filters, right_tables))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(find_possible_join_filter_tables(child_op,
                                                      hint_info,
                                                      right_tables,
                                                      is_current_dfo,
                                                      is_fully_partition_wise,
                                                      current_dfo_level,
                                                      candi_left_filters,
                                                      candi_right_filters,
                                                      join_filter_infos))) {
    LOG_WARN("failed to find possible join filter table", K(ret));
  }
  return ret;
}

int ObLogPlan::get_join_filter_exprs(const ObIArray<ObRawExpr*> &left_join_conditions,
                                     const ObIArray<ObRawExpr*> &right_join_conditions,
                                     JoinFilterInfo &join_filter_info)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_set;
  const ObDMLStmt* stmt;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret));
  } else if (OB_UNLIKELY(left_join_conditions.count() != right_join_conditions.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join condition length error", K(ret));
  } else if (OB_FAIL(table_set.add_member(stmt->get_table_bit_index(join_filter_info.table_id_)))) {
      LOG_WARN("failed to add member", K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < right_join_conditions.count(); ++j) {
    ObRawExpr *lexpr = left_join_conditions.at(j);
    ObRawExpr *rexpr = right_join_conditions.at(j);
    if (OB_ISNULL(lexpr) || OB_ISNULL(rexpr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (rexpr->get_relation_ids().is_subset(table_set)) {
      if (OB_FAIL(join_filter_info.lexprs_.push_back(lexpr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(join_filter_info.rexprs_.push_back(rexpr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::fill_join_filter_info(JoinFilterInfo &join_filter_info)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt;
  const TableItem* table_item;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_stmt()));
  } else if (FALSE_IT(get_selectivity_ctx().clear())) {
  } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(get_update_table_metas(),
                                                          get_selectivity_ctx(),
                                                          join_filter_info.rexprs_,
                                                          join_filter_info.row_count_,
                                                          join_filter_info.right_distinct_card_,
                                                          true/*todo*/))) {
    LOG_WARN("failed to calc distinct", K(ret));
  } else if (join_filter_info.table_id_ == join_filter_info.filter_table_id_) {
    /* do nothing */
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(join_filter_info.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    join_filter_info.pushdown_filter_table_.set_table(*table_item);
  }
  return ret;
}

int ObLogPlan::resolve_dup_tab_constraint(ObLocationConstraintContext &location_constraint) const
{
  int ret = OB_SUCCESS;
  ObIArray<ObDupTabConstraint> &dup_cons = location_constraint.dup_table_replica_cons_;
  ObIArray<LocationConstraint> &base_cons = location_constraint.base_table_constraints_;

  for (int64_t i=0; i<dup_cons.count(); ++i) {
    uint64_t dup_tab_id = dup_cons.at(i).first_;
    uint64_t advisor_table_id = dup_cons.at(i).second_;
    bool found_dup = false;
    bool found_advisor = false;
    for (int64_t j=0; j<base_cons.count(); ++j) {
      if (dup_tab_id == base_cons.at(j).key_.table_id_) {
        found_dup = true;
        dup_cons.at(i).first_ = j;
      } else if (advisor_table_id == base_cons.at(j).key_.table_id_) {
        found_advisor = true;
        dup_cons.at(i).second_ = j;
      } else {
        // do nothing
      }
    }

    if (found_dup && found_advisor) {
      // do nothing
    } else {
      dup_cons.at(i).first_ = OB_INVALID_ID;
      dup_cons.at(i).second_ = OB_INVALID_ID;
    }
  }

  return ret;
}

int ObLogPlan::perform_gather_stat_replace(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  ObLogTableScan *table_scan = NULL;
  ObLogGroupBy *group_by = NULL;
  if (NULL != (table_scan = dynamic_cast<ObLogTableScan *>(op))) {
    ObOpPseudoColumnRawExpr *partition_id_expr = nullptr;
    if (OB_FAIL(table_scan->generate_pseudo_partition_id_expr(partition_id_expr))) {
      LOG_WARN("fail allocate part id expr", K(ret));
    } else {
      stat_partition_id_expr_ = partition_id_expr;
      stat_table_scan_ = table_scan;
      table_scan->set_tablet_id_expr(partition_id_expr);
    }
  } else {
    if (NULL != (group_by = dynamic_cast<ObLogGroupBy *>(op))) {
      if (group_by->get_rollup_exprs().empty() && group_by->get_group_by_exprs().count() > 0) {
        //bug:
        bool found_it = false;//expected only one T_FUN_SYS_CALC_PARTITION_ID in gather stats.
        for (int64_t i = 0; OB_SUCC(ret) && !found_it && i < group_by->get_group_by_exprs().count(); ++i) {
          ObRawExpr* group_by_expr = group_by->get_group_by_exprs().at(i);
          if (OB_ISNULL(group_by_expr) || OB_ISNULL(stat_partition_id_expr_) || OB_ISNULL(stat_table_scan_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(group_by_expr), K(stat_partition_id_expr_), K(stat_table_scan_));
          } else if (T_FUN_SYS_CALC_PARTITION_ID != group_by_expr->get_expr_type()) {
            // do nothing
          } else if (OB_FAIL(stat_gather_replacer_.add_replace_expr(group_by_expr,
                                                                    stat_partition_id_expr_))) {
            LOG_WARN("failed to push back replaced expr", K(ret));
          } else if (group_by_expr->get_partition_id_calc_type() == CALC_IGNORE_SUB_PART) {
            stat_table_scan_->set_tablet_id_type(1);
            found_it = true;
          } else {
            stat_table_scan_->set_tablet_id_type(2);
            found_it = true;
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(op->replace_op_exprs(stat_gather_replacer_))) {
      LOG_WARN("failed to replace generated aggr expr", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::allocate_values_table_path(ValuesTablePath *values_table_path,
                                          ObLogicalOperator *&out_access_path_op)
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_2_0) {
    ObLogExprValues *values_op = NULL;
    if (OB_FAIL(do_alloc_values_table_path(values_table_path, values_op))) {
      LOG_WARN("failed to allocate values table access op", K(ret));
    } else {
      out_access_path_op = values_op;
    }
  } else {
    ObLogValuesTableAccess *values_op = NULL;
    if (OB_FAIL(do_alloc_values_table_path(values_table_path, values_op))) {
      LOG_WARN("failed to allocate values table access op", K(ret));
    } else {
      out_access_path_op = values_op;
    }
  }
  return ret;
}

int ObLogPlan::do_alloc_values_table_path(ValuesTablePath *values_table_path,
                                          ObLogExprValues *&values_op)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  ObValuesTableDef *table_def = NULL;
  if (OB_ISNULL(values_table_path) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(values_table_path->table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(values_table_path), K(get_stmt()), K(ret));
  } else if (OB_UNLIKELY(!table_item->is_values_table()) ||
             OB_ISNULL(table_def = values_table_path->table_def_) ||
             OB_UNLIKELY(0 == table_def->column_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. get unexpect param", K(ret), K(*table_item), KP(table_def));
  } else if (OB_ISNULL(values_op = static_cast<ObLogExprValues*>(get_log_op_factory().
                                   allocate(*this, LOG_EXPR_VALUES)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate values op", K(ret));
  } else {
    values_op->set_table_name(table_item->get_table_name());
    values_op->set_is_values_table(true);
    values_op->set_table_id(values_table_path->table_id_);
    values_op->set_values_table_def(table_def);
    ObSEArray<ObColumnRefRawExpr *, 4> values_desc;
    if (OB_FAIL(values_op->add_values_expr(table_def->access_exprs_))) {
      LOG_WARN("failed to add values expr", K(ret));
    } else if (OB_FAIL(get_stmt()->get_column_exprs(values_table_path->table_id_, values_desc))) {
      LOG_WARN("failed to get column exprs");
    } else if (OB_FAIL(values_op->add_values_desc(values_desc))) {
      LOG_WARN("failed to add values desc", K(ret));
    } else if (OB_FAIL(append(values_op->get_filter_exprs(), values_table_path->filter_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(values_op->compute_property(values_table_path))) {
      LOG_WARN("failed to compute propery", K(ret));
    } else if (OB_FAIL(values_op->pick_out_startup_filters())) {
      LOG_WARN("failed to pick out startup filters", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::do_alloc_values_table_path(ValuesTablePath *values_table_path,
                                          ObLogValuesTableAccess *&values_op)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  ObValuesTableDef *table_def = NULL;
  if (OB_ISNULL(values_table_path) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(values_table_path->table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(values_table_path), K(get_stmt()), K(ret));
  } else if (OB_UNLIKELY(!table_item->is_values_table()) ||
             OB_ISNULL(table_def = values_table_path->table_def_) ||
             OB_UNLIKELY(0 == table_def->column_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. get unexpect param", K(ret), K(*table_item), KP(table_def));
  } else if (OB_ISNULL(values_op = static_cast<ObLogValuesTableAccess*>(get_log_op_factory().
                                   allocate(*this, LOG_VALUES_TABLE_ACCESS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate values op", K(ret));
  } else {
    values_op->set_table_name(table_item->get_table_name());
    values_op->set_table_id(values_table_path->table_id_);
    values_op->set_values_table_def(table_def);
    values_op->set_values_path(values_table_path);
    ObSEArray<ObColumnRefRawExpr *, 4> column_exprs;
    if (OB_FAIL(get_stmt()->get_column_exprs(values_table_path->table_id_, column_exprs))) {
      LOG_WARN("failed to get column exprs");
    } else if (OB_UNLIKELY(column_exprs.count() != table_def->column_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not allow to do project pruning now", K(ret));
    } else if (OB_FAIL(values_op->get_column_exprs().assign(column_exprs))) {
      LOG_WARN("failed to add values desc", K(ret));
    } else if (OB_FAIL(append(values_op->get_filter_exprs(), values_table_path->filter_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(values_op->compute_property(values_table_path))) {
      LOG_WARN("failed to compute propery", K(ret));
    } else if (OB_FAIL(values_op->pick_out_startup_filters())) {
      LOG_WARN("failed to pick out startup filters", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::deduce_redundant_join_conds(const ObIArray<ObRawExpr*> &quals,
                                           const ObIArray<TableItem*> &table_items,
                                           ObIArray<ObRawExpr*> &redundant_quals)
{
  int ret = OB_SUCCESS;
  EqualSets all_equal_sets;
  ObSEArray<ObRawExpr*, 8> normal_quals;
  ObSEArray<ObRawExpr*, 8> subquery_quals;
  ObSEArray<ObRelIds, 8> connect_infos;
  ObSEArray<ObRelIds, 8> single_table_ids;
  ObRelIds table_ids;
  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_EQUAL_SETS, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_FAIL(ObOptimizerUtil::classify_subquery_exprs(quals,
                                                       subquery_quals,
                                                       normal_quals,
                                                       false /* with_onetime */ ))) {
    LOG_WARN("failed to classify subquery exprs", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(&allocator,
                                                        normal_quals,
                                                        all_equal_sets))) {
    LOG_WARN("failed to compute equal set", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < normal_quals.count(); ++i) {
    if (OB_ISNULL(normal_quals.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(connect_infos,
                                               normal_quals.at(i)->get_relation_ids()))) {
      LOG_WARN("failed to add var to array no dup", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    table_ids.reuse();
    if (OB_FAIL(get_table_ids(table_items.at(i), table_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(single_table_ids.push_back(table_ids))) {
      LOG_WARN("failed to push back table ids", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_equal_sets.count(); ++i) {
    ObIArray<ObRawExpr*> *esets = all_equal_sets.at(i);
    if (OB_ISNULL(esets)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(deduce_redundant_join_conds_with_equal_set(*esets,
                                                                  connect_infos,
                                                                  single_table_ids,
                                                                  redundant_quals))) {
      LOG_WARN("failed to deduce redundancy quals with equal set", K(ret));
    }
  }
  return ret;
}

int ObLogPlan::construct_startup_filter_for_limit(ObRawExpr *limit_expr, ObLogicalOperator *log_op)
{
  int ret = OB_SUCCESS;
  int64_t limit_value = 0;
  ObRawExpr *limit_is_zero = NULL;
  ObConstRawExpr *zero_expr = NULL;
  ObRawExpr *startup_filter = NULL;
  bool is_null_value = false;
  if (OB_ISNULL(limit_expr) || OB_ISNULL(log_op)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObTransformUtils::get_expr_int_value(limit_expr,
                                                          get_optimizer_context().get_params(),
                                                          get_optimizer_context().get_exec_ctx(),
                                                          &get_allocator(),
                                                          limit_value,
                                                          is_null_value))) {
    LOG_WARN("failed to get limit int value", K(ret));
  } else if (limit_value > 0 || is_null_value) {
    // do not construct startup filter which is always true
  } else if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(&get_optimizer_context().get_expr_factory(),
                                                           startup_filter, false))) {
    LOG_WARN("failed to build bool expr", K(ret));
  } else if (OB_FAIL(log_op->get_startup_exprs().push_back(startup_filter))) {
    LOG_WARN("failed to allocate select into", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(get_optimizer_context().get_expr_factory(),
                                                          ObIntType,
                                                          0,
                                                          zero_expr))) {
    LOG_WARN("failed to build int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(get_optimizer_context().get_expr_factory(),
                                                           get_optimizer_context().get_session_info(),
                                                           T_OP_EQ,
                                                           limit_is_zero,
                                                           limit_expr,
                                                           zero_expr))) {
    LOG_WARN("failed to build cmp expr", K(ret));
  } else if (OB_FAIL(log_op->expr_constraints_.push_back(
      ObExprConstraint(limit_is_zero, PreCalcExprExpectResult::PRE_CALC_RESULT_TRUE)))) {
    LOG_WARN("failed to add expr constraint", K(ret));
  }
  return ret;
}

int ObLogPlan::adjust_dup_table_replica_by_cons(
  const ObIArray<ObDupTabConstraint> &dup_table_replica_cons,
  common::ObIArray<ObCandiTableLoc> &phy_tbl_info_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_tbl_info_list.count(); ++i) {
    const ObCandiTableLoc &phy_tbl_info = phy_tbl_info_list.at(i);
    if (phy_tbl_info.get_partition_cnt() == 1) {
      const ObCandiTabletLoc &dup_tbl_loc = phy_tbl_info.get_phy_part_loc_info_list().at(0);
      int64_t replica_idx = 0;
      int64_t left_tbl_pos = -1;
      // find first constraint
      for (int64_t j = 0; OB_SUCC(ret) && j < dup_table_replica_cons.count(); ++j) {
        ObDupTabConstraint con = dup_table_replica_cons.at(j);
        if (con.first_ == common::OB_INVALID_ID) {
          // do nothing
        } else if (con.first_ == i) {
          left_tbl_pos = con.second_;
        }
      }
      if (left_tbl_pos != -1) {
        const ObCandiTabletLoc &left_tbl_part_loc_info =
          phy_tbl_info_list.at(left_tbl_pos).get_phy_part_loc_info_list().at(0);
        share::ObLSReplicaLocation replica_loc;
        if (OB_FAIL(left_tbl_part_loc_info.get_selected_replica(replica_loc))) {
          LOG_WARN("failed to set selected replica idx", K(ret), K(left_tbl_part_loc_info));
        } else if (dup_tbl_loc.is_server_in_replica(replica_loc.get_server(), replica_idx)) {
          LOG_DEBUG("reselect replica index according to pwj constraints will happen",
                    K(dup_tbl_loc), K(replica_idx), K(replica_loc.get_server()), K(replica_loc));
          ObRoutePolicy::CandidateReplica replica;
          if (OB_FAIL(dup_tbl_loc.get_priority_replica(replica_idx, replica))) {
            LOG_WARN("failed to get priority replica", K(ret));
          } else if (OB_FAIL(const_cast<ObCandiTabletLoc &>(dup_tbl_loc)
                               .set_selected_replica_idx(replica_idx))) {
            LOG_WARN("failed to set selected replica idx", K(ret), K(replica_idx));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::deduce_redundant_join_conds_with_equal_set(
               const ObIArray<ObRawExpr*> &equal_set,
               ObIArray<ObRelIds> &connect_infos,
               ObIArray<ObRelIds> &single_table_ids,
               ObIArray<ObRawExpr*> &redundant_quals)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  ObRawExpr *new_expr = NULL;
  bool contain_const = false;
  if (OB_ISNULL(get_optimizer_context().get_session_info())
      || OB_ISNULL(get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_optimizer_context().get_session_info()), K(get_optimizer_context().get_query_ctx()));
  } else if (get_optimizer_context().get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_5_BP3)) {
    for (int64_t i = 0; OB_SUCC(ret) && !contain_const && i < equal_set.count(); ++i) {
      if (OB_ISNULL(equal_set.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (equal_set.at(i)->is_const_expr()) {
        contain_const = true;
      }
    }
  }
  if (OB_SUCC(ret) && !contain_const) {
    for (int64_t m = 0; OB_SUCC(ret) && m < equal_set.count() - 1; ++m) {
      for (int64_t n = m + 1; OB_SUCC(ret) && n < equal_set.count(); ++n) {
        table_ids.reuse();
        new_expr = NULL;
        if (OB_ISNULL(equal_set.at(m)) ||
            OB_ISNULL(equal_set.at(n))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (equal_set.at(m)->get_result_meta() !=
                    equal_set.at(n)->get_result_meta()) {
          // do nothing
        } else if (!equal_set.at(m)->has_flag(CNT_COLUMN) ||
                  !equal_set.at(n)->has_flag(CNT_COLUMN) ||
                  equal_set.at(m)->get_relation_ids().overlap(equal_set.at(n)->get_relation_ids())) {
          // do nothing
        } else if (OB_FAIL(table_ids.add_members(equal_set.at(m)->get_relation_ids())) ||
                  OB_FAIL(table_ids.add_members(equal_set.at(n)->get_relation_ids()))) {
          LOG_WARN("failed to add members", K(ret));
        } else if (ObOptimizerUtil::find_item(connect_infos, table_ids)) {
          // do nothing
        } else if (ObOptimizerUtil::find_superset(table_ids, single_table_ids)) {
          // do nothing
        } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                          get_optimizer_context().get_expr_factory(),
                          get_optimizer_context().get_session_info(),
                          T_OP_EQ,
                          new_expr,
                          equal_set.at(m),
                          equal_set.at(n)))) {
            LOG_WARN("failed to create double op expr", K(ret));
        } else if (OB_ISNULL(new_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(new_expr->pull_relation_id())) {
          LOG_WARN("failed to pull releation id");
        } else if (OB_FAIL(connect_infos.push_back(table_ids))) {
            LOG_WARN("failed to push back array", K(ret));
        } else if (OB_FAIL(redundant_quals.push_back(new_expr))) {
          LOG_WARN("failed to push back array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogPlan::compute_duplicate_table_replicas(ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  ObShardingInfo *sharding = NULL;
  ObSEArray<ObAddr, 4> valid_addrs;
  ObAddr basic_addr;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (NULL == (sharding = op->get_strong_sharding()) ||
             !sharding->get_can_reselect_replica()) {
    // do nothing
  } else {
    if (OB_ISNULL(sharding->get_phy_table_location_info()) ||
        OB_UNLIKELY(1 != sharding->get_phy_table_location_info()->get_partition_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_duplicate_table_replica(*sharding->get_phy_table_location_info(),
                                                                    valid_addrs))) {
      LOG_WARN("failed to get duplicated table replica", K(ret));
    } else if (OB_UNLIKELY(valid_addrs.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected valid addrs", K(ret));
    } else if (ObOptimizerUtil::find_item(valid_addrs,
                                          get_optimizer_context().get_local_server_addr())) {
      sharding->set_local();
      basic_addr = get_optimizer_context().get_local_server_addr();
    } else {
      sharding->set_remote();
      basic_addr = valid_addrs.at(0);
    }
    if (OB_SUCC(ret)) {
      int64_t dup_table_pos = OB_INVALID_INDEX;
      ObCandiTableLoc *phy_loc =sharding->get_phy_table_location_info();
      ObCandiTabletLoc &phy_part_loc =
           phy_loc->get_phy_part_loc_info_list_for_update().at(0);
      if (!phy_part_loc.is_server_in_replica(basic_addr, dup_table_pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no server in replica", K(basic_addr), K(ret));
      } else {
        phy_part_loc.set_selected_replica_idx(dup_table_pos);
      }
    }
  }
  return ret;
}

int ObLogPlan::init_lateral_table_depend_info(const ObIArray<TableItem*> &table_items)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    TableItem *table = table_items.at(i);
    TableDependInfo info;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_lateral_table() ||
                table->exec_params_.empty()) {
      //do nothing
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < table->exec_params_.count(); ++j) {
        if (OB_ISNULL(table->exec_params_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(info.depend_table_set_.add_members(
                  table->exec_params_.at(j)->get_ref_expr()->get_relation_ids()))) {
          LOG_WARN("failed to add table ids", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FALSE_IT(info.table_idx_ = stmt->get_table_bit_index(table->table_id_))) {
      } else if (OB_FAIL(table_depend_infos_.push_back(info))) {
        LOG_WARN("failed to push back info", K(ret));
      }
    }

  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to init function table depend info", K(table_depend_infos_));
  }
  return ret;
}

int ObLogPlan::allocate_statistics_collector_as_top(ObLogicalOperator *&old_top, int64_t adaptive_threshold)
{
  int ret = OB_SUCCESS;
  ObLogStatisticsCollector *stat_coll_op = NULL;
  if (OB_ISNULL(old_top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(old_top));
  } else if (OB_ISNULL(stat_coll_op = static_cast<ObLogStatisticsCollector*>(
                        get_log_op_factory().allocate(*this, LOG_STATISTICS_COLLECTOR)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate statistics collector operator", K(ret));
  } else {
    stat_coll_op->set_adaptive_threshold(adaptive_threshold);
    stat_coll_op->set_child(ObLogicalOperator::first_child, old_top);
    if (OB_FAIL(stat_coll_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      old_top = stat_coll_op;
    }
  }
  return ret;
}

int ObLogPlan::generate_alias_table_item(ObDMLStmt *stmt,
                                         uint64_t old_table_id,
                                         uint64_t new_table_id,
                                         TableItem *&new_table_item)
{
  int ret = OB_SUCCESS;
  TableItem *temp_item = NULL;
  TableItem *old_table_item = NULL;
  new_table_item = NULL;
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(old_table_item = stmt->get_table_item_by_id(old_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(old_table_id), K(new_table_id), KPC(stmt));
  } else if (OB_ISNULL(temp_item = stmt->create_table_item(get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create temp table item", K(ret));
  } else {
    temp_item->type_ = TableItem::ALIAS_TABLE;
    temp_item->database_name_ = old_table_item->database_name_;
    temp_item->table_name_ = old_table_item->table_name_;
    temp_item->is_system_table_ = old_table_item->is_system_table_;
    temp_item->is_view_table_ = old_table_item->is_view_table_;
    temp_item->table_id_ = new_table_id;
    temp_item->ref_id_ = old_table_item->ref_id_;
    temp_item->table_type_ = old_table_item->table_type_;

    const char *str = "_alias";
    char *buf = static_cast<char*>(get_allocator().alloc(
        old_table_item->table_name_.length() + strlen(str)));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate string buffer", K(ret));
    } else {
      MEMCPY(buf, old_table_item->table_name_.ptr(), old_table_item->table_name_.length());
      MEMCPY(buf + old_table_item->table_name_.length(), str, strlen(str));
      temp_item->alias_name_.assign_ptr(buf, old_table_item->table_name_.length() + strlen(str));
    }

    if (OB_SUCC(ret)) {
      new_table_item = temp_item;
    }
  }
  return ret;
}

int ObLogPlan::perform_adaptive_join_tsc_replace(ObLogJoin &op)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child_op = NULL;
  ObLogTableScan *old_scan = NULL;
  TableItem *new_table_item = NULL;
  ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(child_op = op.get_child(1)) ||
      OB_UNLIKELY(child_op->get_type() != LOG_TABLE_SCAN)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child", K(ret), KPC(child_op));
  } else if (OB_ISNULL(child_op->get_plan()) ||
             OB_ISNULL(stmt = const_cast<ObDMLStmt *>(child_op->get_plan()->get_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(child_op->get_plan()));
  } else if (FALSE_IT(old_scan = static_cast<ObLogTableScan *>(child_op))) {
  } else if (OB_FAIL(generate_adaptive_join_info(*stmt, *old_scan, new_table_item))) {
    LOG_WARN("generate adaptive join info failed", K(ret), KPC(stmt), KPC(old_scan));
  } else if (OB_ISNULL(new_table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_FAIL(adjust_adaptive_join_structure(*stmt, *old_scan, *new_table_item, op))) {
    LOG_WARN("adjust adaptive join structure failed", K(ret));
  }
  return ret;
}

int ObLogPlan::generate_adaptive_join_info(ObDMLStmt &stmt,
                                           ObLogTableScan &table_scan,
                                           TableItem *&new_table_item)
{
  int ret = OB_SUCCESS;
  new_table_item = NULL;
  if (OB_ISNULL(optimizer_context_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else {
    uint64_t new_table_id = optimizer_context_.get_query_ctx()->available_tb_id_--;
    if (OB_FAIL(generate_alias_table_item(&stmt,
                                          table_scan.get_table_id(),
                                          new_table_id,
                                          new_table_item))) {
      LOG_WARN("failed to generate adaptive join table item", K(ret), K(table_scan),
               K(table_scan.get_ref_table_id()), K(table_scan.get_table_name()));
    } else if (OB_ISNULL(new_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(stmt.get_table_items().push_back(new_table_item))) {
      LOG_WARN("push back failed", K(ret));
    } else if (OB_FAIL(stmt.set_table_bit_index(new_table_item->table_id_))) {
      LOG_WARN("failed to set table bit index", K(ret));
    } else {}
  }
  return ret;
}

int ObLogPlan::adjust_adaptive_join_structure(ObDMLStmt &stmt,
                                              ObLogTableScan &table_scan,
                                              TableItem &table_item,
                                              ObLogJoin &op)
{
  int ret = OB_SUCCESS;
  uint64_t new_table_id = table_item.table_id_;
  uint64_t old_table_id = table_scan.get_table_id();
  ObSEArray<ObColumnRefRawExpr *, 16> orig_col_exprs;
  ObSEArray<ObColumnRefRawExpr *, 16> orig_access_col_exprs;
  ObSEArray<ObRawExpr *, 16> orig_pseudo_exprs;
  ObSEArray<ObRawExpr *, 16> new_exprs;
  ObRawExprCopier expr_copier(get_optimizer_context().get_expr_factory());
  ObLogTableScan *hj_tsc_op = nullptr;
  ObLogicalOperator *parent = op.get_parent();
  if (OB_FAIL(stmt.get_column_exprs(old_table_id, orig_col_exprs))) {
    LOG_WARN("get column exprs failed", K(ret));
  } else {
    table_scan.set_table_id(new_table_id);
    table_scan.get_table_name() = table_item.alias_name_.length() > 0 ?
                                   table_item.alias_name_ : table_item.table_name_;
    table_scan.set_is_adaptive_inner_scan(true);
    if (OB_ISNULL(parent) || OB_UNLIKELY(parent->get_type() != LOG_JOIN)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected parent", K(ret), K(parent));
    } else if (OB_ISNULL(parent->get_child(1)) ||
               OB_UNLIKELY(parent->get_child(1)->get_type() != LOG_TABLE_SCAN)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected child", K(ret));
    } else {
      hj_tsc_op = static_cast<ObLogTableScan *>(parent->get_child(1));
    }
  }
  // create raw expr replacer.
  for (int64_t i = 0; i < orig_col_exprs.count() && OB_SUCC(ret); i++) {
    ColumnItem *col_item = NULL;
    ObColumnRefRawExpr *col_expr = orig_col_exprs.at(i);
    ObRawExpr *new_col_expr = NULL;
    if (OB_FAIL(expr_copier.copy(col_expr, new_col_expr))) {
      LOG_WARN("copy expr failed", K(ret));
    } else if (OB_ISNULL(new_col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("copy expr failed", K(ret));
    } else if (!new_col_expr->is_column_ref_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect column ref expr", K(ret));
    } else if (OB_ISNULL(col_item = stmt.get_column_item(col_expr->get_table_id(),
               col_expr->get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get column item failed", K(ret), KPC(col_expr), K(stmt));
    } else if (OB_FAIL(stmt.get_column_items().push_back(*col_item))) {
      LOG_WARN("push back failed", K(ret));
    } else {
      static_cast<ObColumnRefRawExpr *>(new_col_expr)->set_table_id(new_table_id);
      static_cast<ObColumnRefRawExpr *>(new_col_expr)->set_table_name(table_scan.get_table_name());
      static_cast<ObColumnRefRawExpr *>(new_col_expr)->set_table_alias_name();

      ColumnItem &new_col_item = stmt.get_column_items().at(stmt.get_column_items().count() - 1);
      new_col_item.table_id_ = new_table_id;
      new_col_item.expr_ = static_cast<ObColumnRefRawExpr *>(new_col_expr);

      new_col_expr->get_relation_ids().reset();
      if (OB_FAIL(new_col_expr->add_relation_id(stmt.get_table_bit_index(new_table_id)))) {
        LOG_WARN("add relation id failed", K(ret));
      } else if (!table_scan.check_expr_will_be_used(*col_expr) || !hj_tsc_op->check_expr_will_be_used(*col_expr)) {
        // do nothing.
      } else if (OB_FAIL(new_exprs.push_back(new_col_expr))) {
        LOG_WARN("new_exprs push back failed", K(ret));
      } else if (OB_FAIL(orig_access_col_exprs.push_back(col_expr))) {
        LOG_WARN("orig_access_col_exprs push back failed", K(ret));
      }
    }
  }
  // replace op exprs of table scan which is right child of nlj.
  int32_t old_ref_id = stmt.get_table_bit_index(old_table_id);
  int32_t new_ref_id = stmt.get_table_bit_index(new_table_id);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(recursive_add_ambient_card(&op, old_ref_id, new_ref_id))) {
    LOG_WARN("Fail to recursive add ambient card", K(ret));
  } else if (OB_FAIL(expr_copier.copy_on_replace(op.get_filter_exprs(), op.get_filter_exprs()))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy_on_replace(op.get_join_filters(), op.get_join_filters()))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(table_scan.deep_copy_structures(expr_copier, get_allocator()))) {
    LOG_WARN("replace op exprs failed", K(ret));
  } else {
    for (int64_t i = 0; i < stmt.get_part_exprs().count() && OB_SUCC(ret); i++) {
      if (old_table_id != stmt.get_part_exprs().at(i).table_id_) {
        // do nothing
      } else {
        ObDMLStmt::PartExprItem new_part_item = stmt.get_part_exprs().at(i);
        new_part_item.table_id_ = new_table_id;
        if (OB_FAIL(expr_copier.copy_on_replace(new_part_item.part_expr_,
                                                new_part_item.part_expr_))) {
          LOG_WARN("copy on replace failed", K(ret));
        } else if (NULL != new_part_item.subpart_expr_ &&
                   OB_FAIL(expr_copier.copy_on_replace(new_part_item.subpart_expr_,
                                                       new_part_item.subpart_expr_))) {
          LOG_WARN("copy on replace failed", K(ret));
        } else if (OB_FAIL(stmt.get_part_exprs().push_back(new_part_item))) {
          LOG_WARN("set part expr item failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && op.get_dist_method() == DIST_PARTITION_NONE &&
      OB_FAIL(recursive_replace_exchange_repart_id(op.get_child(0), old_table_id, new_table_id))) {
    LOG_WARN("recursive replace exchange repart id failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    // copy pseudo columns in stmt.
    if (OB_FAIL(stmt.get_table_pseudo_column_like_exprs(old_table_id, orig_pseudo_exprs))) {
      LOG_WARN("get table pseudo column like exprs failed", K(ret));
    } else {
      LOG_TRACE("get table pseudo column like exprs", K(old_table_id), K(orig_pseudo_exprs));
      for (int64_t i = 0; i < orig_pseudo_exprs.count() && OB_SUCC(ret); i++) {
        ObRawExpr *new_pseudo_expr = NULL;
        if (OB_FAIL(expr_copier.copy(orig_pseudo_exprs.at(i), new_pseudo_expr))) {
          LOG_WARN("copy expr failed", K(ret));
        } else if (OB_ISNULL(new_pseudo_expr) || OB_UNLIKELY(!new_pseudo_expr->is_pseudo_column_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected expr", K(ret), KPC(new_pseudo_expr));
        } else if (OB_FAIL(stmt.get_pseudo_column_like_exprs().push_back(new_pseudo_expr))) {
          LOG_WARN("push back failed", K(ret));
        } else if (OB_FAIL(new_exprs.push_back(new_pseudo_expr))) {
          LOG_WARN("push back failed", K(ret));
        } else {
          static_cast<ObPseudoColumnRawExpr *>(new_pseudo_expr)->set_table_id(new_table_id);
          static_cast<ObPseudoColumnRawExpr *>(new_pseudo_expr)->set_table_name(table_scan.get_table_name());
        }
      }
    }
  }
  // generate map between output of right child of hash join and nlj.
  if (OB_SUCC(ret)) {
    ObLogJoin *hj = static_cast<ObLogJoin *>(parent);
    if (OB_FAIL(hj->get_adaptive_nlj_scan_cols().assign(new_exprs))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(hj->get_adaptive_hj_scan_cols().reserve(
                orig_access_col_exprs.count() + orig_pseudo_exprs.count()))) {
      LOG_WARN("reserve failed", K(ret));
    } else {
      for (int64_t i = 0; i < orig_access_col_exprs.count() && OB_SUCC(ret); i++) {
        ObColumnRefRawExpr *col_expr = orig_access_col_exprs.at(i);
        if (OB_FAIL(hj->get_adaptive_hj_scan_cols().push_back(static_cast<ObRawExpr *>(col_expr)))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
      for (int64_t i = 0; i < orig_pseudo_exprs.count() && OB_SUCC(ret); i++) {
        if (OB_FAIL(hj->get_adaptive_hj_scan_cols().push_back(orig_pseudo_exprs.at(i)))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    hash::ObHashSet<uint64_t> &new_exprs = expr_copier.get_new_exprs();
    hash::ObHashSet<uint64_t>::const_iterator iter;
    for(iter = new_exprs.begin(); iter != new_exprs.end() && OB_SUCC(ret); iter++) {
      ObRawExpr *new_expr = NULL;
      if (OB_ISNULL(new_expr = reinterpret_cast<ObRawExpr *>(iter->first))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(new_expr->pull_relation_id())) {
        LOG_WARN("pull relation id failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::recursive_replace_exchange_repart_id(ObLogicalOperator *op,
                                                          uint64_t old_table_id,
                                                          uint64_t new_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (LOG_EXCHANGE == op->get_type()) {
    ObLogExchange *ex_op = static_cast<ObLogExchange *>(op);
    if (old_table_id == ex_op->get_repartition_table_id()) {
      ex_op->set_repartition_table_id(new_table_id);
    }
    if (ex_op->is_consumer() && OB_FAIL(SMART_CALL(recursive_replace_exchange_repart_id(
                                        op->get_child(0), old_table_id, new_table_id)))) {
      LOG_WARN("recursive replace exchange repart id", K(ret));
    }
  } else {
    for (int64_t i = 0; i < op->get_num_of_child() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(recursive_replace_exchange_repart_id(op->get_child(i), old_table_id,
                                                                  new_table_id)))) {
        LOG_WARN("recursive replace exchange repart id", K(ret));
      }
    }
  }
  return ret;
}

int ObLogPlan::recursive_add_ambient_card(ObLogicalOperator *op,
                                          const int32_t old_ref_id,
                                          const int32_t new_ref_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is null", K(ret));
  } else {
    int64_t before_cnt = op->get_ambient_card().count();
    if (new_ref_id >= op->get_ambient_card().count()) {
      op->get_ambient_card().reserve(new_ref_id + 1);
      for (int64_t idx = before_cnt; idx < new_ref_id; ++idx) {
        op->get_ambient_card().push_back(-1.0);
      }
      op->get_ambient_card().push_back(op->get_ambient_card().at(old_ref_id));
    } else if (op->get_ambient_card().at(new_ref_id) == -1.0) {
      op->get_ambient_card().at(new_ref_id) =
          op->get_ambient_card().at(old_ref_id);
    }
    if (op->get_type() == log_op_def::ObLogOpType::LOG_JOIN) {
      JoinPath *join_path = nullptr;
      ObJoinOrder *join_order = nullptr;
      if (OB_ISNULL(join_path = ((ObLogJoin *)op)->get_join_path())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join_path is null", K(ret));
      } else if (OB_ISNULL(join_order = join_path->parent_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join_order is null", K(ret));
      } else {
        before_cnt = join_order->get_ambient_card().count();
        if (new_ref_id >= join_order->get_ambient_card().count()) {
          join_order->get_ambient_card().reserve(new_ref_id + 1);
          for (int64_t idx = before_cnt; idx < new_ref_id; ++idx) {
            join_order->get_ambient_card().push_back(-1.0);
          }
          join_order->get_ambient_card().push_back(join_order->get_ambient_card().at(old_ref_id));
        } else if (join_order->get_ambient_card().at(new_ref_id) == -1.0) {
          join_order->get_ambient_card().at(new_ref_id) =
              join_order->get_ambient_card().at(old_ref_id);
        }
      }
    }
    if (OB_SUCC(ret) && !op->is_plan_root()) {
      if (OB_FAIL(SMART_CALL(recursive_add_ambient_card(op->get_parent(), old_ref_id, new_ref_id)))) {
        LOG_WARN("Fail to recursive add ambient card", K(ret));
      }
    }
  }

  return ret;
}

int ObLogPlan::remove_duplicate_constraints()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && OB_NOT_NULL(get_optimizer_context().get_query_ctx())) {
    ObQueryCtx *query_ctx = get_optimizer_context().get_query_ctx();
    for (int64_t i = query_ctx->all_equal_param_constraints_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      bool find_duplicate = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find_duplicate && j < i; j++) {
        if (query_ctx->all_equal_param_constraints_.at(i) == query_ctx->all_equal_param_constraints_.at(j)) {
          find_duplicate = true;
        }
      }
      if (OB_SUCC(ret) && find_duplicate &&
          OB_FAIL(query_ctx->all_equal_param_constraints_.remove(i))) {
        LOG_WARN("failed to remove a element from array", K(ret));
      }
    }
    for (int64_t i = query_ctx->all_plan_const_param_constraints_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      bool find_duplicate = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find_duplicate && j < i; j++) {
        if (query_ctx->all_plan_const_param_constraints_.at(i) == query_ctx->all_plan_const_param_constraints_.at(j)) {
          find_duplicate = true;
        }
      }
      if (OB_SUCC(ret) && find_duplicate &&
          OB_FAIL(query_ctx->all_plan_const_param_constraints_.remove(i))) {
        LOG_WARN("failed to remove a element from array", K(ret));
      }
    }
    for (int64_t i = query_ctx->all_expr_constraints_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      bool find_duplicate = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find_duplicate && j < i; j++) {
        if (query_ctx->all_expr_constraints_.at(i) == query_ctx->all_expr_constraints_.at(j)) {
          find_duplicate = true;
        }
      }
      if (OB_SUCC(ret) && find_duplicate &&
          OB_FAIL(query_ctx->all_expr_constraints_.remove(i))) {
        LOG_WARN("failed to remove a element from array", K(ret));
      }
    }
  }
  return ret;
}
