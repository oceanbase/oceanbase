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
#include "sql/optimizer/ob_delete_log_plan.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_link_dml.h"
#include "ob_log_operator_factory.h"
#include "ob_log_table_scan.h"
#include "ob_log_sort.h"
#include "ob_log_limit.h"
#include "ob_log_table_scan.h"
#include "ob_join_order.h"
#include "ob_opt_est_cost.h"
/**
 * DELETE syntax from MySQL 5.7
 *
 * Single-Table Syntax:
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
 *   [PARTITION (partition_name,...)]
 *   [WHERE where_condition]
 *   [ORDER BY ...]
 *   [LIMIT row_count]
 *
 * Multiple-Table Syntax
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
 *   tbl_name[.*] [, tbl_name[.*]] ...
 *   FROM table_references
 *   [WHERE where_condition]
 *  Or:
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
 *   FROM tbl_name[.*] [, tbl_name[.*]] ...
 *   USING table_references
 *   [WHERE where_condition]
 */
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql::log_op_def;

int ObDeleteLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  const ObDeleteStmt *delete_stmt = get_stmt();
  if (OB_ISNULL(delete_stmt) || OB_ISNULL(get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(delete_stmt), K(ret));
  } else {
    bool need_limit = true;
    ObSEArray<OrderItem, 4> order_items;
    LOG_TRACE("start to allocate operators for ", "sql", get_optimizer_context().get_query_ctx()->get_sql_stmt());
    OPT_TRACE("generate plan for ", get_stmt());
    if (OB_FAIL(generate_plan_tree())) {
      LOG_WARN("failed to generate plan tree for plain select", K(ret));
    } else {
      LOG_TRACE("succeed to generate plan tree", K(candidates_.candidate_plans_.count()));
    }

    if (OB_SUCC(ret) && get_subquery_filters().count() > 0) {
      if (OB_FAIL(candi_allocate_subplan_filter_for_where())) {
        LOG_WARN("failed to allocate subplan filter for where statement", K(ret));
      } else {
        LOG_TRACE("succeed to allocate subplan filter for where statement",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // 1. allocate 'count' for rownum if needed
    if(OB_SUCC(ret)) {
      bool has_rownum = false;
      if (OB_FAIL(delete_stmt->has_rownum(has_rownum))) {
        LOG_WARN("failed to check rownum info", K(ret));
      } else if (!has_rownum) {
        // do nothing
      } else if (OB_FAIL(candi_allocate_count())) {
        LOG_WARN("failed to allocate count operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate count operator", K(candidates_.candidate_plans_.count()));
      }
    }

    // 2 allocate 'order-by' if needed
    if (OB_SUCC(ret) && delete_stmt->has_order_by()) {
      if (OB_FAIL(candi_allocate_order_by(need_limit, order_items))) {
        LOG_WARN("failed to allocate order by operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate order by operator", K(candidates_.candidate_plans_.count()));
      }
    }

    // 3. allocate 'limit' if needed
    if (OB_SUCC(ret) && delete_stmt->has_limit() && need_limit) {
      if (OB_FAIL(candi_allocate_limit(order_items))) {
        LOG_WARN("failed to allocate limit operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate limit operator", K(candidates_.candidate_plans_.count()));
      }
    }
    
    if (OB_SUCC(ret) && lib::is_mysql_mode() && delete_stmt->has_for_update()) {
      if (OB_FAIL(candi_allocate_for_update())) {
        LOG_WARN("failed to allocate for update operator", K(ret));
      }
    }
    
    // 4. allocate delete operator
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compute_dml_parallel())) {  // compute parallel before call prepare_dml_infos
        LOG_WARN("failed to compute dml parallel", K(ret));
      } else if (OB_FAIL(prepare_dml_infos())) {
        LOG_WARN("failed to prepare dml infos", K(ret));
      } else if (use_pdml()) {
        if (OB_FAIL(candi_allocate_pdml_delete())) {
          LOG_WARN("failed to allocate pdml as top", K(ret));
        } else {
          LOG_TRACE("succeed to allocate pdml operator", K(candidates_.candidate_plans_.count()));
        }
      } else {
        if (OB_FAIL(candi_allocate_delete())) {
          LOG_WARN("failed to allocate delete operator", K(ret));
        } else {
          LOG_TRACE("succeed to allocate delete operator",
              K(candidates_.candidate_plans_.count()));
        }
      }
    }

    // 5. allocate scalar operator, just for agg func returning
    if (OB_SUCC(ret) && delete_stmt->get_returning_aggr_item_size() > 0) {
      if (OB_FAIL(candi_allocate_scala_group_by(delete_stmt->get_returning_aggr_items()))) {
        LOG_WARN("failed to allocate group by operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate group by operator",
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
      } else if (lib::is_mysql_mode() && !delete_stmt->has_limit() &&
                 OB_FAIL(check_fullfill_safe_update_mode(get_plan_root()))) {
        LOG_WARN("failed to check fullfill safe update mode", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

// (2) normal delete plan:
//      ....
//         MULTI PART DELETE
//                 EX IN
//                  EX OUT
//                    TSC
int ObDeleteLogPlan::candi_allocate_delete()
{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 8> candi_plans;
  ObSEArray<CandidatePlan, 8> delete_plans;
  const bool force_no_multi_part = get_log_plan_hint().no_use_distributed_dml();
  const bool force_multi_part = get_log_plan_hint().use_distributed_dml();
  OPT_TRACE("start generate normal insert plan");
  OPT_TRACE("force no multi part:", force_no_multi_part);
  OPT_TRACE("force multi part:", force_multi_part);
  if (OB_FAIL(check_table_rowkey_distinct(index_dml_infos_))) {
    LOG_WARN("failed to check table rowkey distinct", K(ret));
  } else if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_, candi_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else if (OB_FAIL(create_delete_plans(candi_plans,
                                         force_no_multi_part,
                                         force_multi_part,
                                         delete_plans))) {
    LOG_WARN("failed to create delete plans", K(ret));
  } else if (!delete_plans.empty()) {
    LOG_TRACE("succeed to create delete plan using hint", K(delete_plans.count()));
  } else if (OB_FAIL(get_log_plan_hint().check_status())) {
    LOG_WARN("failed to generate plans with hint", K(ret));
  } else if (OB_FAIL(create_delete_plans(candi_plans, false, false, delete_plans))) {
    LOG_WARN("failed to create delete plans", K(ret));
  } else {
    LOG_TRACE("succeed to create delete plan ignore hint", K(delete_plans.count()));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_and_keep_best_plans(delete_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObDeleteLogPlan::create_delete_plans(ObIArray<CandidatePlan> &candi_plans,
                                         const bool force_no_multi_part,
                                         const bool force_multi_part,
                                         ObIArray<CandidatePlan> &delete_plans)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  CandidatePlan candi_plan;
  bool is_multi_part_dml = false;
  bool is_result_local = false;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_plans.count(); i++) {
    candi_plan = candi_plans.at(i);
    is_multi_part_dml = force_multi_part;
    if (OB_ISNULL(candi_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!force_multi_part &&
               OB_FAIL(check_need_multi_partition_dml(*get_stmt(),
                                                      *candi_plan.plan_tree_,
                                                      index_dml_infos_,
                                                      is_multi_part_dml,
                                                      is_result_local))) {
      LOG_WARN("failed to check need multi-partition dml", K(ret));
    } else if (is_multi_part_dml && force_no_multi_part) {
      /*do nothing*/
    } else if (candi_plan.plan_tree_->is_sharding() && (is_multi_part_dml || is_result_local) &&
               OB_FAIL(allocate_exchange_as_top(candi_plan.plan_tree_, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_FAIL(allocate_delete_as_top(candi_plan.plan_tree_, is_multi_part_dml))) {
      LOG_WARN("failed to allocate delete as top", K(ret));
    } else if (OB_FAIL(delete_plans.push_back(candi_plan))) {
      LOG_WARN("failed to push back", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObDeleteLogPlan::allocate_delete_as_top(ObLogicalOperator *&top,
                                            bool is_multi_part_dml)
{
  int ret = OB_SUCCESS;
  ObLogDelete *delete_op = NULL;
  const ObDeleteStmt *delete_stmt = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(delete_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(top), K(delete_stmt), K(ret));
  } else if (OB_ISNULL(delete_op = static_cast<ObLogDelete*>(
                         get_log_op_factory().allocate(*this, LOG_DELETE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate delete operator", K(ret));
  } else {
    delete_op->set_child(ObLogicalOperator::first_child, top);
    delete_op->set_is_returning(delete_stmt->is_returning());
    delete_op->set_is_multi_part_dml(is_multi_part_dml);
    delete_op->set_has_instead_of_trigger(delete_stmt->has_instead_of_trigger());
    if (OB_FAIL(delete_op->assign_dml_infos(index_dml_infos_))) {
      LOG_WARN("failed to assign dml infos", K(ret));
    } else if (delete_stmt->is_error_logging() && OB_FAIL(delete_op->extract_err_log_info())) {
      LOG_WARN("failed to extract error log info", K(ret));
    } else if (OB_FAIL(delete_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = delete_op;
    }
  }
  return ret;
}

int ObDeleteLogPlan::candi_allocate_pdml_delete()
{
  int ret = OB_SUCCESS;
  int64_t gidx_cnt = index_dml_infos_.count();
  OPT_TRACE("start generate pdml delete plan");
  const bool is_pdml_update_split = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos_.count(); i++) {
    if (OB_FAIL(candi_allocate_one_pdml_delete(i > 0,
                                               (i == gidx_cnt - 1),
                                               is_pdml_update_split,
                                               index_dml_infos_.at(i)))) {
      LOG_WARN("failed to allocate one pdml delete", K(ret));
    } else {
      LOG_TRACE("succeed to allocate one pdml delete");
    }
  }
  return ret;
}

int ObDeleteLogPlan::prepare_dml_infos()
{
  int ret = OB_SUCCESS;
  const ObDeleteStmt *delete_stmt = get_stmt();
  if (OB_ISNULL(delete_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    const ObIArray<ObDeleteTableInfo*>& table_infos = delete_stmt->get_delete_table_info();
    bool has_tg = delete_stmt->has_instead_of_trigger();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      const ObDeleteTableInfo* table_info = table_infos.at(i);
      IndexDMLInfo* table_dml_info = nullptr;
      ObSEArray<IndexDMLInfo*, 8> index_dml_infos;
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (OB_FAIL(prepare_table_dml_info_basic(*table_info,
                                                      table_dml_info,
                                                      index_dml_infos,
                                                      has_tg))) {
        LOG_WARN("failed to prepare table dml info basic", K(ret));
      } else if (OB_FAIL(prepare_table_dml_info_special(*table_info,
                                                        table_dml_info,
                                                        index_dml_infos,
                                                        index_dml_infos_))) {
        LOG_WARN("failed to prepare table dml info special", K(ret));
      }
    }
  }
  return ret;
}

int ObDeleteLogPlan::prepare_table_dml_info_special(const ObDmlTableInfo& table_info,
                                                    IndexDMLInfo* table_dml_info,
                                                    ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                    ObIArray<IndexDMLInfo*> &all_index_dml_infos)
{
  int ret = OB_SUCCESS;
  ObAssignments empty_assignments;
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  const ObTableSchema* index_schema = NULL;
  if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",K(ret), K(schema_guard), K(session_info));
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
    } else if (OB_FAIL(generate_index_column_exprs(table_info.table_id_,
                                                   *index_schema,
                                                   empty_assignments,
                                                   index_dml_info->column_exprs_))) {
      LOG_WARN("resolve index related column exprs failed", K(ret), K(table_info));
    }
  }
  
  if (FAILEDx(ObDelUpdLogPlan::prepare_table_dml_info_special(table_info,
                                                              table_dml_info,
                                                              index_dml_infos,
                                                              all_index_dml_infos))) {
    LOG_WARN("failed to prepare table dml info special", K(ret));
  }
  return ret;
}
