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
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/optimizer/ob_merge_log_plan.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_link_dml.h"
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_del_upd_resolver.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObMergeLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  const ObMergeStmt *merge_stmt = get_stmt();
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else {
    LOG_TRACE("start to allocate operators for ", "sql", get_optimizer_context().get_query_ctx()->get_sql_stmt());
    OPT_TRACE("generate plan for ", get_stmt());
    if (OB_FAIL(generate_plan_tree())) {
      LOG_WARN("failed to generate plan tree for plain select", K(ret));
    } else {
      LOG_TRACE("succeed to generate plan tree", K(candidates_.candidate_plans_.count()));
    }

    // allocate subqueries in merge stmt
    if (OB_SUCC(ret)) {
      if (OB_FAIL(candi_allocate_subplan_filter_for_merge())) {
        LOG_WARN("failed to allocate merge subquery", K(ret));
      } else {
        LOG_TRACE("succeed to allocate subquery for merge op",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // step. allocate 'sequence' if needed
    if (OB_SUCC(ret) && get_stmt()->has_sequence()) {
      if (OB_FAIL(candi_allocate_sequence())) {
        LOG_WARN("failed to allocate sequence operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate sequence operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // allocate merge operator
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compute_dml_parallel())) {  // compute parallel before call prepare_dml_infos
        LOG_WARN("failed to compute dml parallel", K(ret));
      } else if (OB_FAIL(prepare_dml_infos())) {
        LOG_WARN("failed to prepare dml infos", K(ret));
      } else if (use_pdml()) {
        if (OB_FAIL(candi_allocate_pdml_merge())) {
          LOG_WARN("failed to allocate pdml merge", K(ret));
        }
      } else if (OB_FAIL(candi_allocate_merge())) {
        LOG_WARN("failed to allocate merge operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate merge operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    //allocate temp-table transformation if needed.
    if (OB_SUCC(ret) && !get_optimizer_context().get_temp_table_infos().empty() && is_final_root_plan()) {
      if (OB_FAIL(candi_allocate_temp_table_transformation())) {
        LOG_WARN("failed to allocate transformation operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate temp-table transformation",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // allocate root exchange
    if (OB_SUCC(ret)) {
      if (OB_FAIL(candi_allocate_root_exchange())) {
        LOG_WARN("failed to allocate root exchange", K(ret));
      } else {
        LOG_TRACE("succeed to allocate root exchange",
            K(candidates_.candidate_plans_.count()));
      }
    }
  }
  return ret;
}

int ObMergeLogPlan::prepare_dml_infos()
{
  int ret = OB_SUCCESS;
  const ObMergeStmt *stmt = NULL;
  IndexDMLInfo* table_dml_info = nullptr;
  ObSEArray<IndexDMLInfo*, 8> index_dml_infos;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(update_condition_exprs_.assign(stmt->get_update_condition_exprs()))) {
    LOG_WARN("failed to assign update condition expr", K(ret));
  } else if (OB_FAIL(delete_condition_exprs_.assign(stmt->get_delete_condition_exprs()))) {
    LOG_WARN("failed to assign delete condition expr", K(ret));
  } else if (OB_FAIL(insert_condition_exprs_.assign(stmt->get_insert_condition_exprs()))) {
    LOG_WARN("failed to assign insert condition expr", K(ret));
  } else {
    const ObMergeTableInfo& table_info = stmt->get_merge_table_info();
    bool has_tg = stmt->has_instead_of_trigger();
    if (OB_FAIL(prepare_table_dml_info_basic(table_info, table_dml_info, index_dml_infos, has_tg))) {
      LOG_WARN("failed to prepare table dml info", K(ret));
    } else if (OB_FAIL(prepare_table_dml_info_insert(table_info, table_dml_info, index_dml_infos, index_dml_infos_))) {
      LOG_WARN("failed to prepare table dml info special", K(ret));
    }
    if (OB_SUCC(ret) && stmt->has_update_clause()) {
      table_dml_info = nullptr;
      index_dml_infos.reset();
      if (OB_FAIL(prepare_table_dml_info_basic(table_info, table_dml_info, index_dml_infos, has_tg))) {
        LOG_WARN("failed to prepare table dml info", K(ret));
      } else if (OB_FAIL(prepare_table_dml_info_update(table_info, table_dml_info, index_dml_infos, index_update_infos_))) {
        LOG_WARN("failed to prepare table dml info special", K(ret));
      }
    }
    if (OB_SUCC(ret) && stmt->has_delete_clause()) {
      table_dml_info = nullptr;
      index_dml_infos.reset();
      if (OB_FAIL(prepare_table_dml_info_basic(table_info, table_dml_info, index_dml_infos, has_tg))) {
        LOG_WARN("failed to prepare table dml info", K(ret));
      } else if (OB_FAIL(prepare_table_dml_info_delete(table_info, table_dml_info, index_dml_infos, index_delete_infos_))) {
        LOG_WARN("failed to prepare table dml info special", K(ret));
      }
    }
  }
  return ret;
}

int ObMergeLogPlan::candi_allocate_merge()
{
  int ret = OB_SUCCESS;
  ObTablePartitionInfo *insert_table_part = NULL;
  ObShardingInfo *insert_sharding = NULL;
  ObSEArray<CandidatePlan, 8> candi_plans;
  ObSEArray<CandidatePlan, 8> merge_plans;
  const bool force_no_multi_part = get_log_plan_hint().no_use_distributed_dml();
  const bool force_multi_part = get_log_plan_hint().use_distributed_dml();
  OPT_TRACE("start generate normal merge plan");
  OPT_TRACE("force no multi part:", force_no_multi_part);
  OPT_TRACE("force multi part:", force_multi_part);
  if (OB_FAIL(calculate_insert_table_location_and_sharding(insert_table_part,
                                                           insert_sharding))) {
    LOG_WARN("failed to calculate insert table location and sharding", K(ret));
  } else if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_,
                                                 candi_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else if (OB_FAIL(create_merge_plans(candi_plans, insert_table_part, insert_sharding,
                                        force_no_multi_part, force_multi_part,
                                        merge_plans))) {
    LOG_WARN("failed to create merge plans", K(ret));
  } else if (!merge_plans.empty()) {
    LOG_TRACE("succeed to create merge plan using hint", K(merge_plans.count()));
  } else if (OB_FAIL(get_log_plan_hint().check_status())) {
    LOG_WARN("failed to generate plans with hint", K(ret));
  } else if (OB_FAIL(create_merge_plans(candi_plans, insert_table_part, insert_sharding,
                                        false, false, merge_plans))) {
    LOG_WARN("failed to create merge plans", K(ret));
  } else {
    LOG_TRACE("succeed to create merge plan ignore hint", K(merge_plans.count()));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_and_keep_best_plans(merge_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObMergeLogPlan::create_merge_plans(ObIArray<CandidatePlan> &candi_plans,
                                       ObTablePartitionInfo *insert_table_part,
                                       ObShardingInfo *insert_sharding,
                                       const bool force_no_multi_part,
                                       const bool force_multi_part,
                                       ObIArray<CandidatePlan> &merge_plans)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  CandidatePlan candi_plan;
  ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 1> equal_pairs;
  bool is_multi_part_dml = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_plans.count(); i++) {
    equal_pairs.reuse();
    candi_plan = candi_plans.at(i);
    is_multi_part_dml = force_multi_part;
    if (OB_ISNULL(candi_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!force_multi_part &&
               OB_FAIL(check_merge_need_multi_partition_dml(*candi_plan.plan_tree_,
                                                            insert_table_part,
                                                            insert_sharding,
                                                            is_multi_part_dml,
                                                            equal_pairs))) {
      LOG_WARN("failed to check need multi-partition dml", K(ret));
    } else if (is_multi_part_dml && force_no_multi_part) {
      /*do nothing*/
    } else if (candi_plan.plan_tree_->is_sharding()
               && (is_multi_part_dml || insert_sharding->is_local())
               && OB_FAIL(allocate_exchange_as_top(candi_plan.plan_tree_, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_FAIL(allocate_merge_as_top(candi_plan.plan_tree_, insert_table_part,
                                             is_multi_part_dml, &equal_pairs))) {
      LOG_WARN("failed to allocate merge as top", K(ret));
    } else if (OB_FAIL(merge_plans.push_back(candi_plan))) {
      LOG_WARN("failed to push back", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObMergeLogPlan::candi_allocate_pdml_merge()
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  ObShardingInfo *target_sharding = NULL;
  ObTablePartitionInfo *target_table_partition = NULL;
  ObSEArray<CandidatePlan, 8> best_plans;
  const IndexDMLInfo *index_dml_info = NULL;
  OPT_TRACE("start generate pdml merge plan");
  if (NULL != get_stmt() && get_stmt()->has_insert_clause()) {
    index_dml_info = index_dml_infos_.empty() ? NULL : index_dml_infos_.at(0);
  } else {
    index_dml_info = index_update_infos_.empty() ? NULL : index_update_infos_.at(0);
  }

  if (OB_ISNULL(get_stmt()) || OB_ISNULL(index_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index dml infos", K(ret), K(get_stmt()), K(index_dml_info),
                                          K(index_dml_infos_), K(index_update_infos_));
  } else if (OB_FAIL(calculate_table_location_and_sharding(*get_stmt(),
                                                          get_stmt()->get_sharding_conditions(),
                                                          index_dml_info->loc_table_id_,
                                                          index_dml_info->ref_table_id_,
                                                          &index_dml_info->part_ids_,
                                                          target_sharding,
                                                          target_table_partition))) {
    LOG_WARN("failed to calculate table location and sharding", K(ret));
  } else if (OB_ISNULL(target_sharding) || OB_ISNULL(target_table_partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(target_sharding), K(target_table_partition), K(ret));
  } else if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_,
                                                 best_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else if (get_stmt()->has_insert_clause() &&
             OB_FAIL(compute_exchange_info_for_pdml_insert(*target_sharding,
                                                          *target_table_partition,
                                                          *index_dml_info,
                                                          false,/*is_index_maintenance*/
                                                          exch_info))) {
    LOG_WARN("failed to compute exchange info for insert", K(ret));
  } else if (!get_stmt()->has_insert_clause() &&
             OB_FAIL(compute_exchange_info_for_pdml_del_upd(*target_sharding,
                                                            *target_table_partition,
                                                            *index_dml_info,
                                                            false,
                                                            exch_info))) {
    LOG_WARN("fail to compute exchange info for pdml merge", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < best_plans.count(); i++) {
      if (OB_FAIL(create_pdml_merge_plan(best_plans.at(i).plan_tree_,
                                         target_table_partition,
                                         exch_info))) {
        LOG_WARN("failed to create pdml merge plan", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prune_and_keep_best_plans(best_plans))) {
        LOG_WARN("failed to prune and keep best plans", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObMergeLogPlan::create_pdml_merge_plan(ObLogicalOperator *&top,
                                           ObTablePartitionInfo *insert_table_part,
                                           ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(top) || OB_ISNULL(insert_table_part)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(insert_table_part), K(ret));
  } else if (OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
    LOG_WARN("failed to allocate exchange as top", K(ret));
  } else if (OB_FAIL(allocate_merge_as_top(top, insert_table_part, true/*MULTI DML*/, NULL))) {
    LOG_WARN("failed to allocate delete as top", K(ret));
  } else {
    static_cast<ObLogMerge *>(top)->set_table_location_uncertain(true);
  }
  return ret;
}

int ObMergeLogPlan::candi_allocate_subplan_filter_for_merge()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> assign_exprs;
  ObSEArray<ObRawExpr*, 8> condition_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> target_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> delete_subquery_exprs;
  const ObMergeStmt *merge_stmt = get_stmt();
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(merge_stmt), K(ret));
  } else if (merge_stmt->get_subquery_exprs().empty()) {
    /*do nothing*/
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(merge_stmt->get_update_condition_exprs(),
                                                         condition_subquery_exprs))) {
    LOG_WARN("failed to get update conditions", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(merge_stmt->get_insert_condition_exprs(),
                                                         condition_subquery_exprs))) {
    LOG_WARN("failed to get insert conditions", K(ret));
  } else if (OB_FAIL(merge_stmt->get_assignments_exprs(assign_exprs))) {
    LOG_WARN("failed to get table assignment", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(assign_exprs,
                                                         target_subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(merge_stmt->get_values_vector(),
                                                         target_subquery_exprs))) {
    LOG_WARN("failed to get target subquery exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(merge_stmt->get_delete_condition_exprs(),
                                                         delete_subquery_exprs))) {
    LOG_WARN("failed to get delete subquery exprs", K(ret));
  } else if (!get_subquery_filters().empty() &&
             candi_allocate_subplan_filter_for_where()) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (condition_subquery_exprs.empty() && target_subquery_exprs.empty() &&
             delete_subquery_exprs.empty()) {
    // do nothing
  } else if (!condition_subquery_exprs.empty() &&
             OB_FAIL(candi_allocate_subplan_filter(condition_subquery_exprs))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (!target_subquery_exprs.empty() &&
             OB_FAIL(candi_allocate_subplan_filter(target_subquery_exprs))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else if (!delete_subquery_exprs.empty() &&
             OB_FAIL(candi_allocate_subplan_filter(delete_subquery_exprs))) {
    LOG_WARN("failed to allocate subplan filter", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObMergeLogPlan::allocate_merge_as_top(ObLogicalOperator *&top,
                                          ObTablePartitionInfo *table_partition_info,
                                          bool is_multi_part_dml,
                                          const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> *equal_pairs)
{
  int ret = OB_SUCCESS;
  ObLogMerge *merge_op = NULL;
  const ObMergeStmt *merge_stmt = get_stmt();
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(top), K(merge_stmt), K(ret));
  } else if (OB_ISNULL(merge_op = static_cast<ObLogMerge *>(
                       get_log_op_factory().allocate(*this, LOG_MERGE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate MERGE operator", K(ret));
  } else if (OB_FAIL(merge_op->assign_dml_infos(index_dml_infos_,
                                                index_update_infos_,
                                                index_delete_infos_))) {
    LOG_WARN("failed to assign dml infos", K(ret));
  } else {
    merge_op->set_child(ObLogicalOperator::first_child, top);
    merge_op->set_is_multi_part_dml(is_multi_part_dml);
    merge_op->set_table_partition_info(table_partition_info);
    if (NULL != equal_pairs && OB_FAIL(merge_op->set_equal_pairs(*equal_pairs))) {
      LOG_WARN("failed to set equal pairs", K(ret));
    } else if (OB_FAIL(merge_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = merge_op;
    }
  }
  return ret;
}

/*
 * for insert stmt to check whether need multi-partition dml
 *  1. check basic need multi part dml
 *  2. check insert need multi part dml basic
 *  3. get insert sharding and check update/insert sharding matched
 */
int ObMergeLogPlan::check_merge_need_multi_partition_dml(ObLogicalOperator &top,
                                                         ObTablePartitionInfo *insert_table_partition,
                                                         ObShardingInfo *insert_sharding,
                                                         bool &is_multi_part_dml,
                                                         ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &equal_pairs)
{
  int ret = OB_SUCCESS;
  is_multi_part_dml = false;
  equal_pairs.reset();
  bool is_one_part_table = false;
  bool is_partition_wise = false;
  bool is_basic = false;
  const ObMergeStmt *merge_stmt = NULL;
  bool is_result_local = false;
  ObShardingInfo *update_sharding = NULL;
  if (OB_ISNULL(merge_stmt = get_stmt()) ||
      OB_UNLIKELY(index_dml_infos_.empty()) || OB_ISNULL(index_dml_infos_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(merge_stmt), K(index_dml_infos_));
  } else if (OB_FAIL(check_stmt_need_multi_partition_dml(*merge_stmt, index_dml_infos_, is_multi_part_dml))) {
    LOG_WARN("failed to check stmt need multi partition dml", K(ret));
  } else if (is_multi_part_dml) {
    /*do nothing*/
  } else if (OB_FAIL(check_merge_stmt_need_multi_partition_dml(is_multi_part_dml, is_one_part_table))) {
    LOG_WARN("failed to check need multi-partition dml", K(ret));
  } else if (is_multi_part_dml) {
    /*do nothing*/
  } else if (OB_FAIL(check_location_need_multi_partition_dml(top,
                                                             index_dml_infos_.at(0)->loc_table_id_,
                                                             is_multi_part_dml,
                                                             is_result_local,
                                                             update_sharding))) {
    LOG_WARN("failed to check whether location need multi-partition dml", K(ret));
  } else if (is_multi_part_dml) {
    /*do nothing*/
  } else if (!merge_stmt->has_insert_clause() || is_one_part_table) {
    is_multi_part_dml = false;
  } else if (OB_FAIL(check_update_insert_sharding_basic(top,
                                                        insert_sharding,
                                                        update_sharding,
                                                        is_basic,
                                                        equal_pairs))) {
    LOG_WARN("failed to check update insert sharding basic", K(ret));
  } else if (is_basic) {
    is_multi_part_dml = false;
  } else if (OB_FAIL(check_update_insert_sharding_partition_wise(top, insert_sharding,
                                                                 is_partition_wise))) {
    LOG_WARN("failed to check update insert match partition wise", K(ret));
  } else if (is_partition_wise) {
    is_multi_part_dml = false;
  } else {
    is_multi_part_dml = true;
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to check merge_stmt need multi-partition-dml", K(is_multi_part_dml),
                                                          K(is_partition_wise), K(equal_pairs));
  }
  return ret;
}

int ObMergeLogPlan::check_update_insert_sharding_basic(ObLogicalOperator &top,
                                                       ObShardingInfo *insert_sharding,
                                                       ObShardingInfo *update_sharding,
                                                       bool &is_basic,
                                                       ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &equal_pairs)
{
  int ret = OB_SUCCESS;
  is_basic = false;
  bool can_gen_cons = false;
  ObSEArray<ObShardingInfo*, 2> input_sharding;
  bool is_sharding_basic = false;
  bool is_match_same_partition = false;
  if (NULL == insert_sharding || NULL == update_sharding) {
    /* do nothing */
  } else if (OB_FAIL(input_sharding.push_back(insert_sharding))
             || OB_FAIL(input_sharding.push_back(update_sharding))) {
    LOG_WARN("failed to push back sharding info", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_basic_sharding_info(get_optimizer_context().get_local_server_addr(),
                                                                input_sharding,
                                                                is_sharding_basic))) {
    LOG_WARN("failed to check basic sharding info", K(ret));
  } else if (!is_sharding_basic) {
    /* do nothing */
  } else if (false == (is_match_same_partition = match_same_partition(*insert_sharding, *update_sharding))) {
    /* insert_sharding and update_sharding match basic,
       but the partition is different, need multi part merge */
    is_basic = false;
  } else if (OB_FAIL(generate_equal_constraint(top, *insert_sharding, can_gen_cons, equal_pairs))) {
    LOG_WARN("failed to generate equal constraint", K(ret));
  } else if (can_gen_cons) {
    is_basic = true;
  } else {
    is_basic = false;
  }
  LOG_TRACE("finish check update insert sharding basic", K(is_basic), K(is_sharding_basic),
          K(is_match_same_partition), K(can_gen_cons), KPC(insert_sharding), KPC(update_sharding));
  return ret;
}

bool ObMergeLogPlan::match_same_partition(const ObShardingInfo &l_sharding_info,
                                          const ObShardingInfo &r_sharding_info)
{
  bool bret = false;
  const ObCandiTableLoc *l_phy_table_loc = NULL;
  const ObCandiTableLoc *r_phy_table_loc = NULL;
  if (OB_NOT_NULL(l_phy_table_loc = l_sharding_info.get_phy_table_location_info()) &&
      OB_NOT_NULL(r_phy_table_loc = r_sharding_info.get_phy_table_location_info()) &&
      OB_LIKELY(1 == l_phy_table_loc->get_phy_part_loc_info_list().count()) &&
      OB_LIKELY(1 == r_phy_table_loc->get_phy_part_loc_info_list().count())) {
    const ObCandiTabletLoc &l_phy_part_loc = l_phy_table_loc->get_phy_part_loc_info_list().at(0);
    const ObCandiTabletLoc &r_phy_part_loc = r_phy_table_loc->get_phy_part_loc_info_list().at(0);
    bret = l_phy_part_loc.get_partition_location().get_partition_id()
           == r_phy_part_loc.get_partition_location().get_partition_id();
  }
  return bret;
}

// When insert_sharding and update_sharding match same partition,
// need add equal constraints for const params in sharding conditions which equal to part key.
int ObMergeLogPlan::generate_equal_constraint(ObLogicalOperator &top,
                                              ObShardingInfo &insert_sharding,
                                              bool &can_gen_cons,
                                              ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &equal_pairs)
{
  int ret = OB_SUCCESS;
  equal_pairs.reset();
  can_gen_cons = false;
  ObLogTableScan *target_table_scan = NULL;
  ObSEArray<ObRawExpr*, 4> left_part_keys;
  ObSEArray<ObRawExpr*, 4> right_part_keys;
  ObSEArray<ObRawExpr*, 4> right_conds;
  const ObMergeStmt *stmt = NULL;
  const TableItem *target_table = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_ISNULL(target_table = stmt->get_table_item_by_id(stmt->get_target_table_id()))) {
    LOG_WARN("failed to get target table", K(ret));
  } else if (OB_FAIL(get_target_table_scan(target_table->get_base_table_item().table_id_,
                                           &top, target_table_scan))) {
    LOG_WARN("get target table scan failed", K(ret), K(*target_table));
  } else if (OB_ISNULL(target_table_scan) || OB_ISNULL(target_table_scan->get_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(target_table_scan));
  } else if (OB_FAIL(append(right_conds, target_table_scan->get_range_conditions()))
              ||OB_FAIL(append(right_conds, target_table_scan->get_filter_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(insert_sharding.get_all_partition_keys(left_part_keys))) {
    LOG_WARN("failed to get partition keys", K(ret));
  } else if (OB_FAIL(target_table_scan->get_sharding()->get_all_partition_keys(right_part_keys))) {
    LOG_WARN("failed to get partition keys", K(ret));
  } else if (left_part_keys.count() != right_part_keys.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected part key size", K(ret), K(left_part_keys), K(right_part_keys));
  } else {
    ObSEArray<ObRawExpr*, 4> l_const_exprs;
    ObSEArray<ObRawExpr*, 4> r_const_exprs;
    const ObIArray<ObRawExpr*> &left_conds = stmt->get_sharding_conditions();
    bool has_equal = true;
    can_gen_cons = true;
    for (int64_t i = 0; OB_SUCC(ret) && has_equal && i < left_part_keys.count(); ++i) {
      if (OB_FAIL(get_const_expr_values(left_part_keys.at(i),
                                        left_conds,
                                        l_const_exprs))) {
        LOG_WARN("failed to get const expr values", K(ret));
      } else if (OB_FAIL(get_const_expr_values(right_part_keys.at(i),
                                               right_conds,
                                               r_const_exprs))) {
        LOG_WARN("failed to get const expr values", K(ret));
      } else if (ObOptimizerUtil::overlap(l_const_exprs, r_const_exprs)) {
        /* exists shared exprs in l_const_exprs and r_const_exprs, need not add constrain */
      } else if (OB_FAIL(has_equal_values(l_const_exprs,
                                          r_const_exprs,
                                          has_equal,
                                          equal_pairs))) {
        LOG_WARN("failed to check has equal values", K(ret));
      }
    }
    if (OB_SUCC(ret) && !has_equal) {
      equal_pairs.reset();
      can_gen_cons = false;
    }
    LOG_TRACE("finish generate equal constraint", K(can_gen_cons), K(equal_pairs),
                    K(left_part_keys), K(left_conds), K(right_part_keys), K(right_conds));
  }
  return ret;
}

int ObMergeLogPlan::has_equal_values(const ObIArray<ObRawExpr*> &l_const_exprs,
                                     const ObIArray<ObRawExpr*> &r_const_exprs,
                                     bool &has_equal,
                                     ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &equal_pairs)
{
  int ret = OB_SUCCESS;
  has_equal = false;
  ObObj l_value;
  ObObj r_value;
  bool is_valid = false;
  ObOptimizerContext &opt_ctx = get_optimizer_context();
  int eq_cmp = 0;
  for (int64_t i = 0; OB_SUCC(ret) && !has_equal && i < l_const_exprs.count(); ++i) {
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(opt_ctx.get_exec_ctx(),
                                                          l_const_exprs.at(i),
                                                          l_value,
                                                          is_valid,
                                                          opt_ctx.get_allocator()))) {
      LOG_WARN("failed to calc const or calculable expr", K(ret));
    } else if (!is_valid) {
      /* do nothing */
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && !has_equal && j < r_const_exprs.count(); ++j) {
        if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(opt_ctx.get_exec_ctx(),
                                                              r_const_exprs.at(j),
                                                              r_value,
                                                              is_valid,
                                                              opt_ctx.get_allocator()))) {
          // Since calculated expr is cached in exec_ctx when call calc_const_or_calculable_expr,
          // we can call calc_const_or_calculable_expr repeatedly for the same expr.
          LOG_WARN("failed to calc const or calculable expr", K(ret));
        } else if (!is_valid) {
          /* do nothing */
        } else if (OB_UNLIKELY(OB_SUCCESS != l_value.compare(r_value, eq_cmp))
                   || 0 != eq_cmp) {
          /* do nothing */
        } else if (OB_FAIL(equal_pairs.push_back(std::pair<ObRawExpr*, ObRawExpr*>(l_const_exprs.at(i), r_const_exprs.at(j))))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          has_equal = true;
        }
      }
    }
  }
  return ret;
}

int ObMergeLogPlan::get_const_expr_values(const ObRawExpr *part_expr,
                                          const ObIArray<ObRawExpr*> &conds,
                                          ObIArray<ObRawExpr*> &const_exprs)
{
  int ret = OB_SUCCESS;
  const_exprs.reuse();
  if (OB_ISNULL(part_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(part_expr));
  } else if (!ob_is_valid_obj_tc(part_expr->get_type_class())) {
    /* do nothing */
  } else {
    ObRawExpr *cur_expr = NULL;
    ObRawExpr *const_expr = NULL;
    ObRawExpr *param_1 = NULL;
    ObRawExpr *param_2 = NULL;
    bool is_const = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
      const_expr = NULL;
      if (OB_ISNULL(cur_expr = conds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(cur_expr));
      } else if (T_OP_EQ != cur_expr->get_expr_type()) {
        /* do nothing */
      } else if (OB_ISNULL(param_1 = cur_expr->get_param_expr(0))
                || OB_ISNULL(param_2 = cur_expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(param_1), K(param_2), K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(param_1, param_1))) {
        LOG_WARN("failed to get expr without lossless cast", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(param_2, param_2))) {
        LOG_WARN("failed to get expr without lossless cast", K(ret));
      } else if (part_expr == param_1 && param_2->is_const_expr()) {
        const_expr = param_2;
      } else if (part_expr == param_2 && param_1->is_const_expr()) {
        const_expr = param_1;
      }

      if (NULL != const_expr && ob_is_valid_obj_tc(const_expr->get_type_class())) {
        if (OB_FAIL(ObObjCaster::is_const_consistent(const_expr->get_result_type().get_obj_meta(),
                                                      part_expr->get_result_type().get_obj_meta(),
                                                      cur_expr->get_result_type().get_calc_type(),
                                                      cur_expr->get_result_type().get_calc_meta().get_collation_type(),
                                                      is_const))) {
          LOG_WARN("check expr type is strict monotonic failed", K(ret));
        } else if (!is_const) {
          /* do nothing */
        } else if (OB_FAIL(const_exprs.push_back(const_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMergeLogPlan::get_target_table_scan(const uint64_t target_table_id,
                                          ObLogicalOperator *cur_op,
                                          ObLogTableScan *&target_table_scan)
{
  int ret = OB_SUCCESS;
  target_table_scan = NULL;
  if (OB_ISNULL(cur_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(cur_op));
  } else if (cur_op->is_table_scan()) {
    ObLogTableScan *table_scan = static_cast<ObLogTableScan*>(cur_op);
    if (table_scan->get_table_id() == target_table_id && !table_scan->get_is_index_global()) {
      target_table_scan = table_scan;
    }
  } else {
    int64_t N = cur_op->get_num_of_child();
    for (int64_t i = 0; OB_SUCC(ret) && NULL == target_table_scan && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(get_target_table_scan(target_table_id,
                                                   cur_op->get_child(i),
                                                   target_table_scan)))) {
        LOG_WARN("get target table scan recursive failed", K(ret), K(target_table_id));
      }
    }
  }
  return ret;
}

int ObMergeLogPlan::check_update_insert_sharding_partition_wise(ObLogicalOperator &top,
                                                                ObShardingInfo *insert_sharding,
                                                                bool &is_partition_wise)
{
  int ret = OB_SUCCESS;
  is_partition_wise = false;
  ObSEArray<ObRawExpr*, 4> target_exprs;
  ObSEArray<ObRawExpr*, 4> source_exprs;
  ObArray<ObShardingInfo*> dummy_weak_sharding;
  ObArray<bool> dummy_nullsafe_info;
  const ObMergeStmt *stmt = NULL;
  if (top.is_exchange_allocated()) {
    /* do nothing */
  } else if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(append(target_exprs, stmt->get_merge_table_info().column_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(stmt->get_value_exprs(source_exprs))) {
    LOG_WARN("failed to get value exprs", K(ret));
  } else if (OB_UNLIKELY(target_exprs.count() != source_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exprs count", K(ret), K(target_exprs), K(source_exprs));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < source_exprs.count(); ++i) {
      if (OB_FAIL(dummy_nullsafe_info.push_back(true))) {
        LOG_WARN("failed to create dummy nullsafe info", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObShardingInfo::check_if_match_partition_wise(top.get_output_equal_sets(),
                                                                target_exprs,
                                                                source_exprs,
                                                                dummy_nullsafe_info,
                                                                insert_sharding,
                                                                dummy_weak_sharding,
                                                                top.get_strong_sharding(),
                                                                top.get_weak_sharding(),
                                                                is_partition_wise))) {
        LOG_WARN("failed to check if match partition wise", K(ret));
      }
    }
  }
  return ret;
}

int ObMergeLogPlan::prepare_table_dml_info_insert(const ObMergeTableInfo& merge_info,
                                                  IndexDMLInfo* table_dml_info,
                                                  ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                  ObIArray<IndexDMLInfo*> &all_index_dml_infos)
{
  int ret = OB_SUCCESS;
  IndexDMLInfo* index_dml_info = NULL;
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  const ObTableSchema* index_schema = NULL;
  const ObMergeStmt* stmt = get_stmt();
  ObRawExprCopier copier(optimizer_context_.get_expr_factory());
  if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info) ||
      OB_ISNULL(stmt) || OB_ISNULL(table_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(schema_guard), 
              K(session_info), K(stmt), K(table_dml_info));
  } else if (OB_FAIL(table_dml_info->column_convert_exprs_.assign(merge_info.column_conv_exprs_))) {
    LOG_WARN("failed to assign column convert exprs", K(ret));
  } else if (stmt->has_insert_clause()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_info.column_exprs_.count(); ++i) {
      if (OB_FAIL(copier.add_replaced_expr(merge_info.column_exprs_.at(i),
                                           merge_info.column_conv_exprs_.at(i)))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_dml_info->ck_cst_exprs_.count(); ++i) {
      if (OB_FAIL(copier.copy_on_replace(table_dml_info->ck_cst_exprs_.at(i),
                                         table_dml_info->ck_cst_exprs_.at(i)))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
    IndexDMLInfo* index_dml_info = index_dml_infos.at(i);
    if (OB_ISNULL(index_dml_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(i), K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                      index_dml_info->ref_table_id_,
                                                      index_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table schema", KPC(index_dml_info), K(ret));
    } else if (OB_FAIL(generate_index_column_exprs(merge_info.table_id_,
                                                   *index_schema,
                                                   index_dml_info->column_exprs_))) {
      LOG_WARN("resolve index related column exprs failed", K(ret));
    } else if (stmt->has_insert_clause() &&
               OB_FAIL(fill_index_column_convert_exprs(copier,
                                                       index_dml_info->column_exprs_,
                                                       index_dml_info->column_convert_exprs_))) {
      LOG_WARN("failed to fill index column convert exprs", K(ret));
    }
  }
  
  if (FAILEDx(ObDelUpdLogPlan::prepare_table_dml_info_special(merge_info,
                                                              table_dml_info,
                                                              index_dml_infos,
                                                              all_index_dml_infos))) {
    LOG_WARN("failed to prepare table dml info special", K(ret));
  }
  return ret;
}

int ObMergeLogPlan::prepare_table_dml_info_update(const ObMergeTableInfo& merge_info,
                                                  IndexDMLInfo* table_dml_info,
                                                  ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                  ObIArray<IndexDMLInfo*> &all_index_dml_infos)
{
  int ret = OB_SUCCESS;
  IndexDMLInfo* index_dml_info = NULL;
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  const ObTableSchema* index_schema = NULL;
  ObRawExprCopier copier(optimizer_context_.get_expr_factory());
  if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info) || OB_ISNULL(table_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(schema_guard), K(session_info), K(table_dml_info));
  } else if (OB_FAIL(table_dml_info->init_assignment_info(merge_info.assignments_,
                                                          optimizer_context_.get_expr_factory()))) {
    LOG_WARN("failed to init assignemt info", K(ret));
  } else if (OB_FAIL(ObResolverUtils::prune_check_constraints(merge_info.assignments_,
                                                              table_dml_info->ck_cst_exprs_))) {
    LOG_WARN("failed to prune check constraints", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                    merge_info.ref_table_id_, index_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table schema", K(merge_info), K(ret));
  } else if (!merge_info.is_link_table_ &&
             OB_FAIL(check_update_part_key(index_schema, table_dml_info))) {
    LOG_WARN("failed to check update part key", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_dml_info->ck_cst_exprs_.count(); ++i) {
      if (OB_FAIL(ObDMLResolver::copy_schema_expr(optimizer_context_.get_expr_factory(),
                                                  table_dml_info->ck_cst_exprs_.at(i),
                                                  table_dml_info->ck_cst_exprs_.at(i)))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      } else if (OB_FAIL(ObTableAssignment::expand_expr(optimizer_context_.get_expr_factory(),
                                                        merge_info.assignments_,
                                                        table_dml_info->ck_cst_exprs_.at(i)))) {
        LOG_WARN("failed to expand expr", K(ret));
      }
    }

    ObSEArray<IndexDMLInfo*, 8> udpate_indexes;
    if (OB_FAIL(udpate_indexes.assign(index_dml_infos))) {
      LOG_WARN("failed to assign index dml infos", K(ret));
    } else {
      index_dml_infos.reset();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < udpate_indexes.count(); ++i) {
      IndexDMLInfo* index_dml_info = udpate_indexes.at(i);
      bool index_update = false;
      if (OB_ISNULL(index_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(i), K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                        index_dml_info->ref_table_id_,
                                                        index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table schema", KPC(index_dml_info), K(ret));
      } else if (OB_FAIL(check_index_update(merge_info.assignments_, *index_schema, 
                                            merge_info.table_id_ != merge_info.loc_table_id_,
                                            index_update))) {
          LOG_WARN("failed to check index update", K(ret), K(merge_info));
      } else if (!index_update) {
          // do nothing
      } else if (OB_FAIL(generate_index_column_exprs(merge_info.table_id_,
                                                     *index_schema,
                                                     merge_info.assignments_,
                                                     index_dml_info->column_exprs_))) {
        LOG_WARN("resolve index related column exprs failed", K(ret));
      } else if (OB_FAIL(index_dml_info->init_assignment_info(merge_info.assignments_,
                                                              optimizer_context_.get_expr_factory()))) {
        LOG_WARN("failed to init assignment info", K(ret));
      } else if (!merge_info.is_link_table_ &&
                 OB_FAIL(check_update_part_key(index_schema, index_dml_info))) {
        LOG_WARN("faield to check update part key", K(ret));
      } else if (OB_FAIL(index_dml_infos.push_back(index_dml_info))) {
        LOG_WARN("failed to push back index dml info", K(ret));
      }
    }
  }
  if (FAILEDx(ObDelUpdLogPlan::prepare_table_dml_info_special(merge_info,
                                                              table_dml_info,
                                                              index_dml_infos,
                                                              all_index_dml_infos))) {
    LOG_WARN("failed to prepare table dml info special", K(ret));
  }
  return ret;
}

int ObMergeLogPlan::prepare_table_dml_info_delete(const ObMergeTableInfo& merge_info,
                                                  IndexDMLInfo* table_dml_info,
                                                  ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                  ObIArray<IndexDMLInfo*> &all_index_dml_infos)
{
  int ret = OB_SUCCESS;
  ObAssignments empty_assignments;
  IndexDMLInfo* index_dml_info = NULL;
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  const ObTableSchema* index_schema = NULL;
  const ObMergeStmt* stmt = get_stmt();
  ObRawExprCopier copier(optimizer_context_.get_expr_factory());
  if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info) || OB_ISNULL(table_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(schema_guard), K(session_info), K(table_dml_info));
  } else {
    // delete doesn't need check constraint expr
    table_dml_info->ck_cst_exprs_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
      IndexDMLInfo* index_dml_info = index_dml_infos.at(i);
      if (OB_ISNULL(index_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(i), K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                        index_dml_info->ref_table_id_,
                                                        index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table schema", KPC(index_dml_info), K(ret));
      } else if (OB_FAIL(generate_index_column_exprs(merge_info.table_id_,
                                                    *index_schema,
                                                    empty_assignments,
                                                    index_dml_info->column_exprs_))) {
        LOG_WARN("resolve index related column exprs failed", K(ret));
      }
    }
  }
  
  if (FAILEDx(ObDelUpdLogPlan::prepare_table_dml_info_special(merge_info,
                                                              table_dml_info,
                                                              index_dml_infos,
                                                              all_index_dml_infos))) {
    LOG_WARN("failed to prepare table dml info special", K(ret));
  }
  return ret;
}

int ObMergeLogPlan::check_merge_stmt_need_multi_partition_dml(bool &is_multi_part_dml,
                                                              bool &is_one_part_table)
{
  int ret = OB_SUCCESS;
  bool has_rand_part_key = false;
  bool has_subquery_part_key = false;
  bool has_auto_inc_part_key = false;
  bool part_key_update = false;
  ObSchemaGetterGuard *schema_guard = get_optimizer_context().get_schema_guard();
  ObSQLSessionInfo* session_info = get_optimizer_context().get_session_info();
  const ObTableSchema *table_schema = NULL;
  const ObMergeStmt *merge_stmt = get_stmt();
  ObSEArray<ObObjectID, 4> part_ids;
  is_multi_part_dml = false;
  is_one_part_table = false;
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(merge_stmt), K(schema_guard), K(session_info), K(ret));
  } else if (merge_stmt->has_instead_of_trigger() ||
             index_dml_infos_.count() > 1 ||
             get_optimizer_context().is_batched_multi_stmt() ||
             //ddl sql can produce a PDML plan with PL UDF,
             //some PL UDF that cannot be executed in a PDML plan
             //will be forbidden during the execution phase
             optimizer_context_.contain_user_nested_sql()) {
    is_multi_part_dml = true;
  } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                    merge_stmt->get_merge_table_info().ref_table_id_,
                                                    table_schema))) {
    LOG_WARN("get table schema from schema guard failed", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(is_one_part_table = ObSQLUtils::is_one_part_table_can_skip_part_calc(*table_schema))) {
  } else if (!merge_stmt->get_merge_table_info().part_ids_.empty() &&
             merge_stmt->value_from_select()) {
    is_multi_part_dml = true;
  } else if (OB_FAIL(merge_stmt->part_key_is_updated(part_key_update))) {
    LOG_WARN("failed to check part key is updated", K(ret));
  } else if (part_key_update && !is_one_part_table) {
    is_multi_part_dml = true;
  } else if (merge_stmt->has_part_key_sequence() &&
              share::schema::PARTITION_LEVEL_ZERO != table_schema->get_part_level()) {
    // sequence 作为分区键的值插入分区表或者是insert...update更新了分区键，都需要使用multi table dml
    is_multi_part_dml = true;
  } else if (OB_FAIL(merge_stmt->part_key_has_rand_value(has_rand_part_key))) {
    LOG_WARN("check part key has rand value failed", K(ret));
  } else if (OB_FAIL(merge_stmt->part_key_has_subquery(has_subquery_part_key))) {
    LOG_WARN("failed to check part key has subquery", K(ret));
  } else if (OB_FAIL(merge_stmt->part_key_has_auto_inc(has_auto_inc_part_key))) {
    LOG_WARN("check to check whether part key contains auto inc column", K(ret));
  } else if (has_rand_part_key || has_subquery_part_key || has_auto_inc_part_key) {
    is_multi_part_dml = true;
  } else { /*do nothing*/ }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to check insert_stmt need multi-partition-dml", K(is_multi_part_dml));
  }
  return ret;
}
