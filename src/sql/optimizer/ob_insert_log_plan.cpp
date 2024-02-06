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
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_insert_log_plan.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_insert_all.h"
#include "sql/optimizer/ob_log_link_dml.h"
#include "sql/ob_optimizer_trace_impl.h"
#include "common/ob_smart_call.h"
#include "sql/resolver/dml/ob_del_upd_resolver.h"
#include "share/system_variable/ob_sys_var_class_type.h"
#include "share/stat/ob_stat_define.h"
#include "sql/rewrite/ob_transform_utils.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql::log_op_def;

int ObInsertLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  const ObInsertStmt *insert_stmt = get_stmt();
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(get_optimizer_context().get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(insert_stmt), K(ret));
  } else {
    LOG_TRACE("start to allocate operators for ", "sql", get_optimizer_context().get_query_ctx()->get_sql_stmt());
    OPT_TRACE("generate plan for ", get_stmt());
    if (!insert_stmt->value_from_select()) {
      // insert into values xxxx
      ObLogicalOperator *top = NULL;
      if (insert_stmt->has_sequence() &&
          OB_FAIL(allocate_sequence_as_top(top))) {
        LOG_WARN("failed to allocate sequence as top", K(ret));
      } else if (OB_FAIL(allocate_insert_values_as_top(top))) {
        LOG_WARN("failed to allocate expr values as top", K(ret));
      } else if (OB_FAIL(make_candidate_plans(top))) {
        LOG_WARN("failed to make candidate plans", K(ret));
      } else { /*do nothing*/ }
    } else {
      // insert into select xxx
      if (OB_FAIL(generate_plan_tree())) {
        LOG_WARN("failed to generate plan tree", K(ret));
      } else if (insert_stmt->has_sequence() &&
                 OB_FAIL(candi_allocate_sequence())) {
        LOG_WARN("failed to allocate sequence", K(ret));
      } else { /*do nothing*/}
    }

    // allocate subplan filter for "INSERT .. ON DUPLICATE KEY UPDATE c1 = (select...)"
    if (OB_SUCC(ret) && insert_stmt->is_insert_up()) {
      ObSEArray<ObRawExpr*, 4> subquery;
      ObSEArray<ObRawExpr*, common::OB_PREALLOCATED_NUM> assign_exprs;
      bool contain = false;
      if (OB_FAIL(insert_stmt->get_assignments_exprs(assign_exprs))) {
        LOG_WARN("failed to get assignment exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(assign_exprs, subquery))) {
        LOG_WARN("failed to get subqueries", K(ret)) ;
      } else if (OB_FAIL(check_contain_non_onetime_expr(subquery, contain))) {
        LOG_WARN("check contain non onetime expr", K(ret));
      } else if (contain) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "update values contain non onetime subquery");
        LOG_WARN("update values contain non onetime subquery", K(ret));
      } else if (!subquery.empty() && OB_FAIL(candi_allocate_subplan_filter(subquery))) {
        LOG_WARN("failed to allocate subplan", K(ret));
      } else { /*do nothing*/ }
    }

    bool need_osg = false;
    OSGShareInfo *osg_info = NULL;
    if (OB_SUCC(ret)) {
      // compute parallel before check allocate stats gather
      if (OB_FAIL(compute_dml_parallel())) {
        LOG_WARN("failed to compute dml parallel", K(ret));
      } else if (use_pdml() && OB_FAIL(set_is_direct_insert())) {
        LOG_WARN("failed to set is direct insert", K(ret));
      } else if (OB_FAIL(check_need_online_stats_gather(need_osg))) {
        LOG_WARN("fail to check wether we need optimizer stats gathering operator", K(ret));
      } else if (need_osg && OB_FAIL(generate_osg_share_info(osg_info))) {
        LOG_WARN("failed to generate osg share info");
      } else if (need_osg && OB_ISNULL(osg_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null");
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(prepare_dml_infos())) {
        LOG_WARN("failed to prepare dml infos", K(ret));
      } else if (use_pdml()) {
        if (OB_FAIL(candi_allocate_pdml_insert(osg_info))) {
          LOG_WARN("failed to allocate pdml insert", K(ret));
        } else {
          LOG_TRACE("succeed to allocate pdml insert operator",
              K(candidates_.candidate_plans_.count()));
        }
      } else if (OB_FAIL(candi_allocate_insert(osg_info))) {
        LOG_WARN("failed to allocate insert operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate insert operator", K(candidates_.candidate_plans_.count()));
      }
    }
    if (OB_SUCC(ret) && insert_stmt->get_returning_aggr_item_size() > 0) {
      if (OB_FAIL(candi_allocate_scala_group_by(insert_stmt->get_returning_aggr_items()))) {
        LOG_WARN("failed to allocate scalar group by", K(ret));
      } else {
        LOG_TRACE("succeed to allocate group by operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    /* if the plan is pdml or parallel select, should allocate a OSG above insert. This OSG is used to merge
     * all information collection by other OSG.
    */
    if (OB_SUCC(ret) && need_osg) {
      if (OB_FAIL(candi_allocate_optimizer_stats_merge(osg_info))) {
        LOG_WARN("fail to allcate osg on top", K(ret));
      } else {
        LOG_TRACE("succeed to allocate optimizer stat merge",
            K(candidates_.candidate_plans_.count()));
      }
    }

    //allocate temp-table transformation if needed.
    if (OB_SUCC(ret) && !get_optimizer_context().get_temp_table_infos().empty() && is_final_root_plan()) {
      if (OB_FAIL(candi_allocate_temp_table_transformation())) {
        LOG_WARN("failed to allocate transformation operator", K(ret));
      } else {
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(candi_allocate_root_exchange())) {
        LOG_WARN("failed to allocate root exchange", K(ret));
      } else {
        LOG_TRACE("succeed to allocate root operator",
                K(candidates_.candidate_plans_.count()));
      }
    }
  }
  return ret;
}

int ObInsertLogPlan::generate_osg_share_info(OSGShareInfo *&info)
{
  int ret = OB_SUCCESS;
  const ObInsertStmt *stmt = NULL;
  const ObTableSchema *tab_schema = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  if (OB_ISNULL(schema_guard = get_optimizer_context().get_sql_schema_guard()) ||
      OB_UNLIKELY(!get_stmt()->is_insert_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(info = OB_NEWx(OSGShareInfo, (&get_allocator())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory");
  } else {
    stmt = static_cast<const ObInsertStmt *>(get_stmt());
    const ObInsertTableInfo& table_info = stmt->get_insert_table_info();
    uint64_t table_id = table_info.table_id_;
    uint64_t ref_table_id = table_info.ref_table_id_;
    if (OB_FAIL(schema_guard->get_table_schema(table_id, ref_table_id, stmt, tab_schema))) {
      LOG_WARN("fail to get table schema", K(ref_table_id), K(tab_schema), K(ret));
    } else if (OB_ISNULL(tab_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("get unexpected null pointer", K(ret));
    } else {
      const ObColumnSchemaV2 *col_schema = NULL;
      ObSEArray<uint64_t, 4> generated_column_ids;
      info->table_id_ = ref_table_id;
      if (tab_schema->is_partitioned_table()) {
        info->part_level_ = tab_schema->get_part_level();
        if (OB_FAIL(gen_calc_part_id_expr(table_info.loc_table_id_,
                                          ref_table_id,
                                          CALC_PARTITION_TABLET_ID,
                                          info->calc_part_id_expr_))) {
          LOG_WARN("failed to init calc part id", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::replace_column_with_select_for_partid(stmt,
                                                                                  get_optimizer_context(),
                                                                                  info->calc_part_id_expr_))) {
          // using the select column/values expr to calc partid.
          LOG_WARN("fail to replace column item with select item", K(ret));
        } else {
          LOG_TRACE("success to generate calc_part expr", K(ret), K(info->calc_part_id_expr_));
        }
      }
      // column conv;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_info.column_exprs_.count(); i++) {
        ObColumnRefRawExpr *tmp_col = table_info.column_exprs_.at(i);
        ObRawExpr *col_conv_expr = table_info.column_conv_exprs_.at(i);
        if (OB_ISNULL(tmp_col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null pointer", K(ret));
        } else if (OB_FAIL(ObTransformUtils::get_base_column(stmt, tmp_col))) {
          LOG_WARN("fail to get base column", K(ret));
        } else {
          // since the column_exprs may be a generated_table's column. e.g., insert into (select c2, c1 from t1) values (1,1);
          // for t1, the column_ids of c1 and c2 are 16 and 17. However, in the insert stmt, the column c2 in subquery is 16.
          // we need to get the real column id.
          col_schema = tab_schema->get_column_schema(tmp_col->get_column_id());
          if (OB_ISNULL(col_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("can't get column schema", K(ret));
          } else if (!pl::ObDbmsStats::check_column_validity(*tab_schema, *col_schema)) {
            // do not gather stats for auto inc column.
            // continue, shouldn't add these to osg's output.
          } else if (!col_schema->is_generated_column()) {
            if (OB_FAIL(info->col_conv_exprs_.push_back(col_conv_expr))) {
              LOG_WARN("fail to push back column convert expr", K(ret));
            } else if (OB_FAIL(info->column_ids_.push_back(tmp_col->get_column_id()))) {
              LOG_WARN("fail to push back column ids", K(ret));
            }
          } else {
            // generated column: no need to replace column expr with select_item in select clause. since this work has already done.
            if (OB_FAIL(info->generated_column_exprs_.push_back(col_conv_expr))) {
              LOG_WARN("fail to add generated column expr", K(ret));
            } else if (OB_FAIL(generated_column_ids.push_back(tmp_col->get_column_id()))) {
              LOG_WARN("fail to push back column ids", K(ret));
            }
          }
        }
      } // end for
      if (OB_SUCC(ret)) {
        // column ids of generated column should be add at the tail.
        if (OB_FAIL(append(info->column_ids_, generated_column_ids))) {
          LOG_WARN("fail to append column ids", K(ret));
        }
      }
    }
  }
  return ret;
}

// Direct-insert is enabled only:
// 1. pdml insert
// 2. _ob_enable_direct_load
// 3. insert into select clause
// 4. append hint
// 5. auto_commit, not in a transaction
int ObInsertLogPlan::set_is_direct_insert() {
  int ret = OB_SUCCESS;
  is_direct_insert_ = false;
  bool auto_commit = false;
  const ObSQLSessionInfo* session_info = get_optimizer_context().get_session_info();
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_stmt()), K(session_info));
  } else if (!get_stmt()->value_from_select()
             || !get_optimizer_context().get_global_hint().has_append()
             || !GCONF._ob_enable_direct_load
             || session_info->is_in_transaction()) {
    /* is not direct insert */
  } else if (OB_FAIL(session_info->get_autocommit(auto_commit))) {
    LOG_WARN("failed to get auto commit", KR(ret));
  } else {
    is_direct_insert_ = auto_commit;
  }
  return ret;
}

int ObInsertLogPlan::check_contain_non_onetime_expr(const ObRawExpr *expr, bool &contain)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->has_flag(IS_SUB_QUERY)) {
    if (!ObOptimizerUtil::find_item(get_onetime_query_refs(), expr)) {
      contain = true;
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    if (ObOptimizerUtil::find_item(get_onetime_query_refs(), expr)) {
      //do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !contain && i < expr->get_param_count(); i++) {
        if (OB_FAIL(SMART_CALL(check_contain_non_onetime_expr(expr->get_param_expr(i), contain)))) {
          LOG_WARN("check contain non one time expr failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObInsertLogPlan::check_contain_non_onetime_expr(const ObIArray<ObRawExpr *> &exprs, bool &contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  for (int64_t i = 0; OB_SUCC(ret) && !contain && i < exprs.count(); i++) {
    if (OB_FAIL(check_contain_non_onetime_expr(exprs.at(i), contain))) {
      LOG_WARN("check contain non one time expr failed", K(ret));
    }
  }
  return ret;
}


int ObInsertLogPlan::check_need_online_stats_gather(bool &need_osg)
{
  int ret = OB_SUCCESS;
  need_osg = false;
  bool online_sys_var = false;
  bool need_gathering = true;
  ObObj online_sys_var_obj;
  const ObInsertStmt *insert_stmt = NULL;
  TableItem *ins_table = NULL;
  if (OB_ISNULL(insert_stmt = get_stmt()) ||
      OB_ISNULL(ins_table = insert_stmt->get_table_item_by_id(insert_stmt->get_insert_table_info().table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret), K(insert_stmt->get_insert_table_info()));
  } else if (OB_UNLIKELY(ins_table->is_system_table_ || ins_table->is_index_table_)
             || insert_stmt->is_insert_up()
             || !insert_stmt->value_from_select()
             || (!get_optimizer_context().get_session_info()->is_user_session())) {
    need_gathering = false;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_optimizer_context().get_session_info()->get_sys_variable(share::SYS_VAR__OPTIMIZER_GATHER_STATS_ON_LOAD,
                                                                                  online_sys_var_obj))) {
    LOG_WARN("fail to get sys var", K(ret));
  } else {
    online_sys_var = online_sys_var_obj.get_bool();
    // shouldn't gather stats if the stmt is insert update.
    // if the online_opt_stat_gather is enable, should gather opt_stats even there is no hint.
    // if the online_opt_stat_gather is disable, only gather opt_stats when there is hint.
    need_osg = need_gathering
               && !get_optimizer_context().get_query_ctx()->get_global_hint().has_no_gather_opt_stat_hint()
               && online_sys_var
               && ((get_optimizer_context().get_query_ctx()->get_global_hint().should_generate_osg_operator())
                   || use_pdml());
    LOG_TRACE("online insert stat", K(online_sys_var), K(need_osg), K(need_gathering));
  }
  return ret;
}

int ObInsertLogPlan::allocate_insert_values_as_top(ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObLogExprValues *values_op = NULL;
  const ObInsertStmt *insert_stmt = get_stmt();
  ObSQLSessionInfo *session_info = get_optimizer_context().get_session_info();
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(session_info)) {
    LOG_WARN("get unexpected null", K(insert_stmt), K(session_info), K(ret));
  } else if (OB_ISNULL(values_op = static_cast<ObLogExprValues*>(get_log_op_factory().
                                   allocate(*this, LOG_EXPR_VALUES)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate values op", K(ret));
  } else if (insert_stmt->is_error_logging() && OB_FAIL(values_op->extract_err_log_info())) {
    LOG_WARN("failed to extract error log exprs", K(ret));
  } else if (OB_FAIL(values_op->compute_property())) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    if (NULL != top) {
      ret = values_op->add_child(top);
    }
    top = values_op;
    bool contain = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_contain_non_onetime_expr(insert_stmt->get_values_vector(), contain))) {
      LOG_WARN("check contain non onetime expr", K(ret));
    } else if (contain) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert values contain non onetime subquery");
      LOG_WARN("insert values contain non onetime subquery", K(ret));
    } else if (!insert_stmt->get_subquery_exprs().empty() &&
        OB_FAIL(allocate_subplan_filter_as_top(top,
                                               insert_stmt->get_values_vector()))) {
      LOG_WARN("failed to allocate subplan filter as top", K(ret));
    } else if (OB_FAIL(values_op->add_values_expr(insert_stmt->get_values_vector()))) {
      LOG_WARN("failed to add values expr", K(ret));
    } else if (OB_FAIL(values_op->add_values_desc(insert_stmt->get_values_desc()))) {
      LOG_WARN("failed to add values desc", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObInsertLogPlan::candi_allocate_insert(OSGShareInfo *osg_info)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *lock_row_flag_expr = NULL;
  ObTablePartitionInfo *insert_table_part = NULL;
  ObShardingInfo *insert_sharding = NULL;
  ObSEArray<CandidatePlan, 8> candi_plans;
  ObSEArray<CandidatePlan, 8> insert_plans;
  const bool force_no_multi_part = get_log_plan_hint().no_use_distributed_dml();
  const bool force_multi_part = get_log_plan_hint().use_distributed_dml();
  OPT_TRACE("start generate normal insert plan");
  OPT_TRACE("force no multi part:", force_no_multi_part);
  OPT_TRACE("force multi part:", force_multi_part);
  if (OB_FAIL(build_lock_row_flag_expr(lock_row_flag_expr))) {
    LOG_WARN("failed to build lock row flag expr", K(ret));
  } else if (OB_FAIL(calculate_insert_table_location_and_sharding(insert_table_part,
                                                                  insert_sharding))) {
    LOG_WARN("failed to calculate insert table location and sharding", K(ret));
  } else if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_,
                                                 candi_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else if (OB_FAIL(create_insert_plans(candi_plans,
                                         insert_table_part,
                                         insert_sharding,
                                         lock_row_flag_expr,
                                         force_no_multi_part,
                                         force_multi_part,
                                         insert_plans,
                                         osg_info))) {
    LOG_WARN("failed to create insert plans", K(ret));
  } else if (!insert_plans.empty()) {
    LOG_TRACE("succeed to create insert plan using hint", K(insert_plans.count()));
  } else if (OB_FAIL(get_log_plan_hint().check_status())) {
    LOG_WARN("failed to generate plans with hint", K(ret));
  } else if (OB_FAIL(create_insert_plans(candi_plans, insert_table_part,
                                         insert_sharding,
                                         lock_row_flag_expr,
                                         false, false,
                                         insert_plans,
                                         osg_info))) {
    LOG_WARN("failed to create insert plans", K(ret));
  } else {
    LOG_TRACE("succeed to create insert plan ignore hint", K(insert_plans.count()));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_and_keep_best_plans(insert_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObInsertLogPlan::build_lock_row_flag_expr(ObConstRawExpr *&lock_row_flag_expr)
{
  int ret = OB_SUCCESS;
  lock_row_flag_expr = NULL;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(ret));
  } else {
    const ObInsertStmt *insert_stmt = get_stmt();
    const ObIArray<ObAssignment>& assignments = insert_stmt->get_table_assignments();
    if (!assignments.empty()) {
      // table_assigns非空, 说明可能会有update子计划
      if (OB_FAIL(ObRawExprUtils::build_var_int_expr(optimizer_context_.get_expr_factory(),
                                                     lock_row_flag_expr))) {
        LOG_WARN("fail to create expr", K(ret));
      } else if (OB_FAIL(lock_row_flag_expr->formalize(optimizer_context_.get_session_info()))) {
        LOG_WARN("fail to formalize", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObInsertLogPlan::create_insert_plans(ObIArray<CandidatePlan> &candi_plans,
                                         ObTablePartitionInfo *insert_table_part,
                                         ObShardingInfo *insert_sharding,
                                         ObConstRawExpr *lock_row_flag_expr,
                                         const bool force_no_multi_part,
                                         const bool force_multi_part,
                                         ObIArray<CandidatePlan> &insert_plans,
                                         OSGShareInfo *osg_info)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  bool is_multi_part_dml = false;
  CandidatePlan candi_plan;
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_plans.count(); i++) {
    candi_plan = candi_plans.at(i);
    is_multi_part_dml = force_multi_part;
    if (OB_ISNULL(candi_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!force_multi_part &&
               OB_FAIL(check_insert_need_multi_partition_dml(*candi_plan.plan_tree_,
                                                             insert_table_part,
                                                             insert_sharding,
                                                             is_multi_part_dml))) {
      LOG_WARN("failed to check need multi-partition dml", K(ret));
    } else if (is_multi_part_dml && force_no_multi_part) {
      /*do nothing*/
    } else if (osg_info != NULL &&
               OB_FAIL(allocate_optimizer_stats_gathering_as_top(candi_plan.plan_tree_,
                                                                 *osg_info))) {
      LOG_WARN("failed to allocate sequence as top", K(ret));
    } else if (candi_plan.plan_tree_->is_sharding() &&
               (is_multi_part_dml || insert_sharding->is_local()) &&
               OB_FAIL(allocate_exchange_as_top(candi_plan.plan_tree_, exch_info))) {
      LOG_WARN("failed to allocate exchange as top", K(ret));
    } else if (OB_FAIL(allocate_insert_as_top(candi_plan.plan_tree_,
                                              lock_row_flag_expr,
                                              insert_table_part,
                                              insert_sharding,
                                              is_multi_part_dml))) {
      LOG_WARN("failed to allocate insert as top", K(ret));
    } else if (OB_FAIL(insert_plans.push_back(candi_plan))) {
      LOG_WARN("failed to push back", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObInsertLogPlan::allocate_insert_as_top(ObLogicalOperator *&top,
                                            ObRawExpr *lock_row_flag_expr,
                                            ObTablePartitionInfo *table_partition_info,
                                            ObShardingInfo *insert_sharding,
                                            bool is_multi_part_dml)
{
  int ret = OB_SUCCESS;
  ObLogInsert *insert_op = NULL;
  const ObInsertStmt *insert_stmt = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(insert_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(insert_stmt), K(ret));
  } else if (OB_ISNULL(insert_op = static_cast<ObLogInsert*>(get_log_op_factory().allocate(*this, LOG_INSERT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate insert operator failed", K(ret));
  } else if (OB_FAIL(insert_op->assign_dml_infos(index_dml_infos_))) {
    LOG_WARN("failed to assign index dml infos", K(ret));
  } else if (insert_stmt->is_replace() &&
             OB_FAIL(insert_op->get_replace_index_dml_infos().assign(replace_del_index_del_infos_))) {
    LOG_WARN("failed to assign replace index dml infos", K(ret));
  } else if (insert_stmt->is_insert_up() &&
             OB_FAIL(insert_op->get_insert_up_index_dml_infos().assign(insert_up_index_upd_infos_))) {
    LOG_WARN("failed to assign insert_up upd index dml infos", K(ret));
  } else {
    insert_op->set_child(ObLogicalOperator::first_child, top);
    insert_op->set_is_insert_select(insert_stmt->value_from_select());
    insert_op->set_is_returning(insert_stmt->is_returning());
    insert_op->set_replace(insert_stmt->is_replace());
    insert_op->set_ignore(insert_stmt->is_ignore());
    insert_op->set_is_multi_part_dml(is_multi_part_dml);
    insert_op->set_table_partition_info(table_partition_info);
    insert_op->set_lock_row_flag_expr(lock_row_flag_expr);
    insert_op->set_has_instead_of_trigger(insert_stmt->has_instead_of_trigger());
    if (OB_NOT_NULL(insert_stmt->get_table_item(0))) {
      insert_op->set_append_table_id(insert_stmt->get_table_item(0)->ref_id_);
    }
    if (top->is_match_all() && !is_multi_part_dml && !insert_stmt->has_instead_of_trigger()) {
      insert_op->set_strong_sharding(insert_sharding);
    }
    if (insert_stmt->is_insert_up()) {
      insert_op->set_insert_up(true);
    }
    if (insert_stmt->is_insert_up() || insert_stmt->is_replace()) {
      insert_op->set_constraint_infos(&uk_constraint_infos_);
    }
    if (insert_stmt->is_error_logging() && OB_FAIL(insert_op->extract_err_log_info())) {
      LOG_WARN("failed to extract error log info", K(ret));
    } else if (OB_FAIL(insert_stmt->get_view_check_exprs(insert_op->get_view_check_exprs()))) {
      LOG_WARN("failed to get view check exprs", K(ret));
    } else if (OB_FAIL(insert_op->compute_property())) {
      LOG_WARN("failed to compute equal set", K(ret));
    } else {
      top = insert_op;
    }
  }
  return ret;
}

int ObInsertLogPlan::candi_allocate_pdml_insert(OSGShareInfo *osg_info)
{
  int ret = OB_SUCCESS;
  int64_t gidx_cnt = index_dml_infos_.count();
  OPT_TRACE("start generate pdml insert plan");
  const bool is_pdml_update_split = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < gidx_cnt; i++) {
    if (OB_FAIL(candi_allocate_one_pdml_insert(i > 0,
                                               i == gidx_cnt - 1,
                                               is_pdml_update_split,
                                               index_dml_infos_.at(i),
                                               i == 0 ? osg_info : NULL))) {
      LOG_WARN("failed to allocate one pdml insert", K(ret), K(i), K(index_dml_infos_));
    } else {
      LOG_TRACE("succeed to allocate one pdml insert");
    }
  }
  return ret;
}

/*
 * for insert stmt to check whether need multi-partition dml
 */
int ObInsertLogPlan::check_insert_need_multi_partition_dml(ObLogicalOperator &top,
                                                           ObTablePartitionInfo *insert_table_partition,
                                                           ObShardingInfo *insert_sharding,
                                                           bool &is_multi_part_dml)
{
  int ret = OB_SUCCESS;
  is_multi_part_dml = false;
  bool is_one_part_table = false;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  } else if (OB_FAIL(check_insert_stmt_need_multi_partition_dml(is_multi_part_dml,
                                                                is_one_part_table))) {
    LOG_WARN("failed to check if insert stmt need multi-partition dml", K(ret));
  } else if (is_multi_part_dml) {
    /*do nothing*/
  } else if (OB_FAIL(check_insert_location_need_multi_partition_dml(top,
                                                                    insert_table_partition,
                                                                    insert_sharding,
                                                                    is_one_part_table,
                                                                    is_multi_part_dml))) {
    LOG_WARN("failed to check if insert stmt need multi-partition dml", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObInsertLogPlan::check_insert_stmt_need_multi_partition_dml(bool &is_multi_part_dml,
                                                                bool &is_one_part_table)
{
  int ret = OB_SUCCESS;
  bool has_rand_part_key = false;
  bool has_subquery_part_key = false;
  bool has_auto_inc_part_key = false;
  bool part_key_update = false;
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  const ObInsertStmt *insert_stmt = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObObjectID, 4> part_ids;
  is_multi_part_dml = false;
  is_one_part_table = false;
  if (OB_ISNULL(insert_stmt = get_stmt()) ||
      OB_ISNULL(schema_guard = get_optimizer_context().get_schema_guard()) ||
      OB_ISNULL(session_info = get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(insert_stmt), K(schema_guard), K(session_info), K(ret));
  } else if (insert_stmt->has_instead_of_trigger() || index_dml_infos_.count() > 1 ||
             get_optimizer_context().is_batched_multi_stmt() ||
             //ddl sql can produce a PDML plan with PL UDF,
             //some PL UDF that cannot be executed in a PDML plan
             //will be forbidden during the execution phase
             optimizer_context_.contain_user_nested_sql()) {
    is_multi_part_dml = true;
  } else if (!insert_stmt->value_from_select()) {
    is_multi_part_dml = true;
  } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                    insert_stmt->get_insert_table_info().ref_table_id_,
                                                    table_schema))) {
    LOG_WARN("get table schema from schema guard failed", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(is_one_part_table = ObSQLUtils::is_one_part_table_can_skip_part_calc(*table_schema))) {
  } else if ((insert_stmt->is_ignore() && !is_one_part_table) ||
             (lib::is_mysql_mode() && !is_strict_mode(session_info->get_sql_mode()))) {
    // insert ignore，并且是分区表插入时，不能优化
    // mysql non strict mode can not optimize as multi part dml
    is_multi_part_dml = true;
  } else if (!insert_stmt->get_insert_table_info().part_ids_.empty() &&
             insert_stmt->value_from_select()) {
    is_multi_part_dml = true;
  } else if (OB_FAIL(insert_stmt->part_key_is_updated(part_key_update))) {
    LOG_WARN("failed to check part key is updated", K(ret));
  } else if (part_key_update && !is_one_part_table) {
    is_multi_part_dml = true;
  } else if (insert_stmt->has_part_key_sequence() &&
              share::schema::PARTITION_LEVEL_ZERO != table_schema->get_part_level()) {
    // sequence 作为分区键的值插入分区表或者是insert...update更新了分区键，都需要使用multi table dml
    is_multi_part_dml = true;
  } else if (OB_FAIL(insert_stmt->part_key_has_rand_value(has_rand_part_key))) {
    LOG_WARN("check part key has rand value failed", K(ret));
  } else if (OB_FAIL(insert_stmt->part_key_has_subquery(has_subquery_part_key))) {
    LOG_WARN("failed to check part key has subquery", K(ret));
  } else if (OB_FAIL(insert_stmt->part_key_has_auto_inc(has_auto_inc_part_key))) {
    LOG_WARN("check to check whether part key contains auto inc column", K(ret));
  } else if (has_rand_part_key || has_subquery_part_key || has_auto_inc_part_key) {
    is_multi_part_dml = true;
  } else { /*do nothing*/ }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to check insert_stmt need multi-partition-dml", K(is_multi_part_dml));
  }
  return ret;
}

int ObInsertLogPlan::check_insert_location_need_multi_partition_dml(ObLogicalOperator &top,
                                                                    ObTablePartitionInfo *insert_table_part,
                                                                    ObShardingInfo *insert_sharding,
                                                                    bool is_one_part_table,
                                                                    bool &is_multi_part_dml)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  bool is_partition_wise = false;
  const ObInsertStmt *insert_stmt = NULL;
  is_multi_part_dml = false;
  if (OB_ISNULL(insert_table_part) || OB_ISNULL(insert_sharding) ||
      OB_ISNULL(insert_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_table_part), K(insert_sharding),
                                    K(insert_stmt), K(ret));
  } else if (0 == insert_table_part->get_phy_tbl_location_info().get_partition_cnt()) {
    is_multi_part_dml = true;
  } else if (OB_FAIL(check_basic_sharding_for_insert_stmt(*insert_sharding,
                                                           top,
                                                           is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (insert_sharding->is_local() || is_basic) {
    if (is_one_part_table || !insert_table_part->get_table_location().is_part_or_subpart_all_partition()) {
      is_multi_part_dml = false;
    } else {
      is_multi_part_dml = true;
    }
  } else if (insert_stmt->is_insert_up() || insert_stmt->is_replace()) {
    // #issue/44052024
    // force insert_up & replace use distribute op to avoid 4.0 branch rollback bug.
    // should remove this condition in 4.1
    is_multi_part_dml = true;
  } else if (OB_FAIL(check_if_match_partition_wise_insert(*insert_sharding,
                                                          top,
                                                          is_partition_wise))) {
    LOG_WARN("failed to check if match partition wise insert", K(ret));
  } else if (is_partition_wise) {
    is_multi_part_dml = false;
  } else {
    is_multi_part_dml = true;
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to check insert location need multi_partition_dml", K(is_multi_part_dml),
        K(is_partition_wise));
  }
  return ret;
}

int ObInsertLogPlan::check_basic_sharding_for_insert_stmt(ObShardingInfo &target_sharding,
                                                          ObLogicalOperator &child,
                                                          bool &is_basic)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObShardingInfo*, 4> input_sharding;
  ObAddr &local_addr = get_optimizer_context().get_local_server_addr();
  is_basic = false;
  if (OB_ISNULL(child.get_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(input_sharding.push_back(&target_sharding)) ||
             OB_FAIL(input_sharding.push_back(child.get_sharding()))) {
    LOG_WARN("failed to push back sharding info", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_basic_sharding_info(local_addr,
                                                                input_sharding,
                                                                is_basic))) {
    LOG_WARN("failed to check if it is basic sharding info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObInsertLogPlan::check_if_match_partition_wise_insert(ObShardingInfo &target_sharding,
                                                          ObLogicalOperator &top,
                                                          bool &is_partition_wise)
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  ObSEArray<ObRawExpr*, 4> target_exprs;
  ObSEArray<ObRawExpr*, 4> source_exprs;
  const ObInsertStmt *insert_stmt = NULL;
  is_partition_wise = false;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  } else if (!get_stmt()->is_insert_stmt()) {
    // do nothing for merge stmt
  } else if (FALSE_IT(insert_stmt = get_stmt())) {
    /*do nothing*/
  } else if (OB_FAIL(append(target_exprs, insert_stmt->get_insert_table_info().column_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(insert_stmt->get_value_exprs(source_exprs))) {
    LOG_WARN("failed to get value exprs", K(ret));
  } else if (OB_FAIL(ObShardingInfo::check_if_match_partition_wise(top.get_output_equal_sets(),
                                                                   target_exprs,
                                                                   source_exprs,
                                                                   &target_sharding,
                                                                   top.get_strong_sharding(),
                                                                   is_match))) {
    LOG_WARN("failed to check if match partition wise join", K(ret));
  } else {
    is_partition_wise = is_match && !top.is_exchange_allocated();
  }
  return ret;
}

int ObInsertLogPlan::prepare_dml_infos()
{
  int ret = OB_SUCCESS;
  const ObInsertStmt *stmt = NULL;
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(session_info));
  } else if (optimizer_context_.is_online_ddl() &&
             !ObSQLUtils::is_nested_sql(session_info->get_cur_exec_ctx())) {
    //@todo: (cangdi), special operation of DDL should not include nested sql triggered in DDL session
    if (OB_FAIL(prepare_table_dml_info_for_ddl(stmt->get_insert_table_info(), index_dml_infos_))) {
      LOG_WARN("failed to prepare table dml info for ddl", K(ret));
    }
  } else {
    const ObInsertTableInfo& table_info = stmt->get_insert_table_info();
    IndexDMLInfo* table_dml_info = nullptr;
    ObSEArray<IndexDMLInfo*, 8> index_dml_infos;
    bool has_tg = stmt->has_instead_of_trigger();
    if (OB_FAIL(prepare_table_dml_info_basic(table_info, table_dml_info, index_dml_infos, has_tg))) {
      LOG_WARN("failed to prepare table dml info basic", K(ret));
    } else if (OB_FAIL(prepare_table_dml_info_special(table_info, table_dml_info, index_dml_infos, index_dml_infos_))) {
      LOG_WARN("failed to prepare table dml info special", K(ret));
    } else if ((stmt->is_insert_up() || stmt->is_replace()) &&
               OB_FAIL(prepare_unique_constraint_infos(table_info))) {
      LOG_WARN("failed to prepare unique constraint infos", K(ret));
    } else if (stmt->is_replace()) {
      // prepare_table_dml_info_special will prune virtual column and collect related local
      // index ids. Since replace delete index dml info has same pruned column and related 
      // local index, we can prepare_table_dml_info_special first and copy next.
      if (OB_FAIL(ObDelUpdLogPlan::prepare_table_dml_info_special(table_info,
                                                                  table_dml_info,
                                                                  index_dml_infos,
                                                                  index_dml_infos_))) {
        LOG_WARN("failed to prepare table dml info special", K(ret));
      } else if (OB_FAIL(copy_index_dml_infos_for_replace(index_dml_infos_,
                                                          replace_del_index_del_infos_))) {
        LOG_WARN("failed to copy log index dml infos", K(ret));
      }
    } else if (stmt->is_insert_up()) {
      // prepare_table_dml_info_special will prune virtual column and collect related local
      // index ids. Since insert update index dml info has different pruned column and related 
      // local index, we need copy dml info fisrt and then prepare_table_dml_info_special.
      if (OB_FAIL(copy_index_dml_infos_for_insert_up(table_info, table_dml_info,
                                                     index_dml_infos, insert_up_index_upd_infos_))) {
        LOG_WARN("failed to copy log index dml infos", K(ret));
      } else if (OB_FAIL(ObDelUpdLogPlan::prepare_table_dml_info_special(table_info,
                                                                         table_dml_info,
                                                                         index_dml_infos,
                                                                         index_dml_infos_))) {
        LOG_WARN("failed to prepare table dml info special", K(ret));
      } else if (OB_ISNULL(insert_up_index_upd_infos_.at(0)) ||
                 OB_UNLIKELY(!insert_up_index_upd_infos_.at(0)->is_primary_index_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected index dml info", K(ret), KPC(insert_up_index_upd_infos_.at(0)));
      } else {
        //@TODO:zimiao 目前由于执行层insert up的基于表达式的实现
        //要求update子句和insert子句的local index id保持一致
        //这里的处理比较费解，理论上insert子句和update子句应该各自有自己独立的local index关系
        //后续insert up会基于DAS Cache来做批处理，跟表达式的关系解耦后，这里不需要对insert up做特殊处理
        if (OB_FAIL(insert_up_index_upd_infos_.at(0)->related_index_ids_.assign(
                    table_dml_info->related_index_ids_))) {
          LOG_WARN("assing related index id failed", K(ret));
        }
      }
    } else if (OB_FAIL(ObDelUpdLogPlan::prepare_table_dml_info_special(table_info,
                                                                         table_dml_info,
                                                                         index_dml_infos,
                                                                         index_dml_infos_))) {
      LOG_WARN("failed to prepare table dml info special", K(ret));
    }
  }
  return ret;
}

int ObInsertLogPlan::prepare_table_dml_info_special(const ObDmlTableInfo& table_info,
                                                    IndexDMLInfo* table_dml_info,
                                                    ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                    ObIArray<IndexDMLInfo*> &all_index_dml_infos)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  const ObTableSchema* index_schema = NULL;
  const ObInsertTableInfo& insert_info = static_cast<const ObInsertTableInfo&>(table_info);
  if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",K(ret), K(schema_guard), K(session_info));
  } else if (OB_FAIL(table_dml_info->column_convert_exprs_.assign(insert_info.column_conv_exprs_))) {
    LOG_WARN("failed to assign column convert exprs", K(ret));
  } else {
    ObRawExprCopier copier(optimizer_context_.get_expr_factory());
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_info.column_exprs_.count(); ++i) {
      if (OB_FAIL(copier.add_replaced_expr(insert_info.column_exprs_.at(i),
                                           insert_info.column_conv_exprs_.at(i)))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_dml_info->ck_cst_exprs_.count(); ++i) {
      if (OB_FAIL(copier.copy_on_replace(table_dml_info->ck_cst_exprs_.at(i),
                                         table_dml_info->ck_cst_exprs_.at(i)))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      }
    }
    
    if (OB_SUCC(ret) && !index_dml_infos.empty()) {
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
        } else if (OB_FAIL(generate_index_column_exprs(insert_info.table_id_,
                                                      *index_schema,
                                                      index_dml_info->column_exprs_))) {
          LOG_WARN("resolve index related column exprs failed", K(ret));
        } else if (OB_FAIL(fill_index_column_convert_exprs(copier,
                                                          index_dml_info->column_exprs_,
                                                          index_dml_info->column_convert_exprs_))) {
          LOG_WARN("failed to fill index column convert exprs", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObInsertLogPlan::copy_index_dml_infos_for_replace(ObIArray<IndexDMLInfo*> &src_dml_infos,
                                                      ObIArray<IndexDMLInfo*> &dst_dml_infos)
{
  int ret = OB_SUCCESS;
  IndexDMLInfo* index_dml_info = nullptr;
  void* ptr = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < src_dml_infos.count(); ++i) {
    ptr = nullptr;
    if (OB_ISNULL(src_dml_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null",K(ret), K(i), K(src_dml_infos));
    } else if (OB_ISNULL(ptr = get_optimizer_context().get_allocator().alloc(sizeof(IndexDMLInfo)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      index_dml_info = new (ptr) IndexDMLInfo();
      if (OB_FAIL(index_dml_info->assign_basic(*src_dml_infos.at(i)))) {
        LOG_WARN("failed to assign table dml info", K(ret));
      } else if (index_dml_info->is_primary_index_ &&
                 !optimizer_context_.get_session_info()->get_ddl_info().is_ddl() &&
                 OB_FAIL(collect_related_local_index_ids(*index_dml_info))) {
        LOG_WARN("collect related local index ids failed", K(ret));
      } else {
        // these index dml infos is used for delete in replace stmt,
        // so column convert expr is useless
        index_dml_info->column_convert_exprs_.reset();
      }
      if (FAILEDx(dst_dml_infos.push_back(index_dml_info))) {
        LOG_WARN("failed to push back index dml info", K(ret));
      }
    }
  }
  return ret;
}

int ObInsertLogPlan::copy_index_dml_infos_for_insert_up(const ObInsertTableInfo& table_info,
                                                        IndexDMLInfo* table_dml_info,
                                                        ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                        ObIArray<IndexDMLInfo*> &dst_dml_infos)
{
  int ret = OB_SUCCESS;
  const ObInsertStmt* insert_stmt = get_stmt();
  ObSEArray<ObRawExpr*, 8> update_cst_exprs;
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  const ObTableSchema* index_schema = NULL;
  IndexDMLInfo* index_dml_info = nullptr;
  void* ptr = nullptr;
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret));
  } else if (OB_FAIL(update_cst_exprs.assign(table_info.check_constraint_exprs_))) {
    LOG_WARN("failed to get check constraint exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < update_cst_exprs.count(); ++i) {
      if (OB_FAIL(ObDMLResolver::copy_schema_expr(optimizer_context_.get_expr_factory(),
                                                  update_cst_exprs.at(i),
                                                  update_cst_exprs.at(i)))) {
        LOG_WARN("failed to copy schema expr", K(ret));
      } else if (OB_FAIL(ObTableAssignment::expand_expr(optimizer_context_.get_expr_factory(),
                                                        table_info.assignments_,
                                                        update_cst_exprs.at(i)))) {
        LOG_WARN("failed to create expanded expr", K(ret));
      }
    }
    int64_t dml_info_count = index_dml_infos.count() + 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < dml_info_count; ++i) {
      ptr = nullptr;
      IndexDMLInfo* src_info = 0 == i ? table_dml_info : index_dml_infos.at(i - 1);
      if (OB_ISNULL(src_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null",K(ret), K(i), KPC(table_dml_info), K(index_dml_infos));
      } else if (OB_ISNULL(ptr = get_optimizer_context().get_allocator().alloc(sizeof(IndexDMLInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (OB_FALSE_IT(index_dml_info = new (ptr) IndexDMLInfo())) {
      } else if (OB_FAIL(index_dml_info->assign_basic(*src_info))) {
        LOG_WARN("failed to assign table dml info", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                        index_dml_info->ref_table_id_,
                                                        index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table schema", KPC(index_dml_info), K(ret));
      } else {
        index_dml_info->column_convert_exprs_.reset();
        if (OB_FAIL(index_dml_info->init_assignment_info(table_info.assignments_,
                                                         optimizer_context_.get_expr_factory()))) {
          LOG_WARN("init index assignment info failed", K(ret));
        } else if (!table_info.is_link_table_ &&
                  OB_FAIL(check_update_part_key(index_schema, index_dml_info))) {
          LOG_WARN("failed to check update part key", K(ret));
        } else if (0 == i) {
          if (OB_FAIL(index_dml_info->ck_cst_exprs_.assign(update_cst_exprs))) {
            LOG_WARN("failed to assign update check exprs", K(ret));
          } else if (OB_FAIL(prune_virtual_column(*index_dml_info))) {
            LOG_WARN("prune virtual column failed", K(ret));
          } else if (!optimizer_context_.get_session_info()->get_ddl_info().is_ddl() &&
                    OB_FAIL(collect_related_local_index_ids(*index_dml_info))) {
            LOG_WARN("collect related local index ids failed", K(ret));
          }
        }
      }
      if (FAILEDx(dst_dml_infos.push_back(index_dml_info))) {
        LOG_WARN("failed to push back index dml info", K(ret));
      } 
    }
  }
  return ret;
}

int ObInsertLogPlan::prepare_unique_constraint_infos(const ObDmlTableInfo& table_info)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  const ObTableSchema *index_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 16> index_infos;
  ObUniqueConstraintInfo constraint_info;
  if (OB_ISNULL(session_info) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                    table_info.ref_table_id_, index_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(index_schema->get_simple_index_infos(index_infos))) {
    LOG_WARN("failed to get index index", K(ret));
  } else if (OB_FAIL(prepare_unique_constraint_info(*index_schema,
                                                    table_info.table_id_,
                                                    constraint_info))) {
    LOG_WARN("failed to resolver unique constraint info", K(ret));
  } else if (OB_FAIL(uk_constraint_infos_.push_back(constraint_info))) {
    LOG_WARN("failed to push back constraint info", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); i++) {
      constraint_info.reset();
      if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                 index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!index_schema->is_final_invalid_index() && index_schema->is_unique_index()) {
        if (OB_FAIL(prepare_unique_constraint_info(*index_schema,
                                                   table_info.table_id_,
                                                   constraint_info))) {
          LOG_WARN("failed to resolver unique constraint info", K(ret));
        } else if (OB_FAIL(uk_constraint_infos_.push_back(constraint_info))) {
          LOG_WARN("failed to push back constraint info", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  
  return ret;
}

int ObInsertLogPlan::prepare_unique_constraint_info(const ObTableSchema &index_schema,
                                                    const uint64_t table_id,
                                                    ObUniqueConstraintInfo &constraint_info)
{
  int ret = OB_SUCCESS;
  const ObInsertStmt* insert_stmt = get_stmt();
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret));
  } else {
    constraint_info.table_id_ = table_id;
    constraint_info.index_tid_ = index_schema.get_table_id();
    if (!index_schema.is_index_table()) {
      constraint_info.constraint_name_ = "PRIMARY";
    } else if (OB_FAIL(index_schema.get_index_name(constraint_info.constraint_name_))) {
      LOG_WARN("failed to get index name", K(ret));
    }
  }
  /*
   * create table t1(pk int, a int, b int, primary key (a), unique key uk_a_b (a, b)) partition by hash(a) partitions 3;
   * OceanBase(root@oceanbase)>explain replace into t1 values(80, 81, 82), (100, 101,102)\G;
   * *************************** 1. row ***************************
   * Query Plan: ============================================
   * |ID|OPERATOR           |NAME|EST. ROWS|COST|
   * --------------------------------------------
   *  |0 |MULTI TABLE REPLACE|    |2        |1   |
   *  |1 | EXPRESSION        |    |2        |1   |
   *  ============================================
   *
   *  Outputs & filters:
   *  -------------------------------------
   *    0 - output([column_conv(INT,PS:(11,0),NOT NULL,__values.a)], [column_conv(INT,PS:(11,0),NULL,__values.pk)], [column_conv(INT,PS:(11,0),NULL,__values.b)]), filter(nil),
   *          columns([{t1: ({t1: (t1.a, t1.pk, t1.b)}, {uk_a_b: (t1.a, t1.b, uk_a_b.shadow_pk_0)})}]), partitions(p0, p2)
   *    1 - output([__values.pk], [__values.a], [__values.b]), filter(nil)
   *          values({80, 81, 82}, {100, 101, 102})
   *
   *  对于上面这种计划, a, b, shadow_pk_0会作为constraint_columns expr,
   *  其中shadow_pk_0依赖的column会被替换为column_conv(..., __values.*)的表达式,
   *  而constraint_columns是用于duplicate_checker, 为了实现更清晰, 所有duplicate_checker
   *  中涉及的表达式计算最终都依赖于table column值, 所以用于duplicate_checker的shadow_pk_0依赖的
   *  column不希望被替换为column_conv(..., __value.*)表达式, 因此这里解析index_rowkey_exprs时,
   *  不会使用前面生成的index_column_exprs中shadow_pk_0, 而是重新在生成一个shadow_pk_0表达式,
   *  该表达式最终依赖于table column值;
   * */
  if (FAILEDx(generate_index_rowkey_exprs(constraint_info.table_id_,
                                          index_schema,
                                          constraint_info.constraint_columns_,
                                          true))) {
    LOG_WARN("failed to generate index rowkey exprs", K(ret));
  } else if (!index_schema.is_index_table() && index_schema.is_heap_table()) {
    // 如果是堆表，那么这里还需要在 constraint_info.constraint_columns_中追加分区建
    // 因为4.0版本堆表 分区建 + hidden_pk 才能保证唯一性
    uint64_t rowkey_column_id = 0;
    const ObRowkeyInfo &rowkey_info = index_schema.get_partition_key_info();
    const ColumnItem *col_item = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("get rowkey column id failed", K(ret));
      } else if (OB_ISNULL(col_item = ObResolverUtils::find_col_by_base_col_id(*insert_stmt,
                                                                               constraint_info.table_id_,
                                                                               rowkey_column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column expr by id failed", K(ret), K(rowkey_column_id));
      } else if (OB_FAIL(constraint_info.constraint_columns_.push_back(col_item->expr_))) {
        LOG_WARN("store column expr to column exprs failed", K(ret));
      }
    }
  }
  return ret;
}

int ObInsertLogPlan::prepare_table_dml_info_for_ddl(const ObInsertTableInfo& table_info,
                                                    ObIArray<IndexDMLInfo*> &all_index_dml_infos)
{
  int ret = OB_SUCCESS;
  const ObInsertStmt* stmt = get_stmt();
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  void *ptr= nullptr;
  IndexDMLInfo* index_dml_info = nullptr;
  const ObTableSchema* index_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  TableItem *table_item = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_guard) || OB_ISNULL(session_info) ||
      OB_ISNULL(table_item = stmt->get_table_item_by_id(table_info.table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",K(ret), K(stmt), K(schema_guard), K(session_info), K(table_item));
  } else if (OB_ISNULL(ptr = optimizer_context_.get_allocator().alloc(sizeof(IndexDMLInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    index_dml_info = new (ptr) IndexDMLInfo();
    if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                               table_item->ddl_table_id_, index_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table schema", K(table_info), K(ret));
    } else if (index_schema->is_index_table() && !index_schema->is_global_index_table()) {
      // local index
      if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                 index_schema->get_data_table_id(),
                                                 data_table_schema))) {
        LOG_WARN("get table schema failed", K(ret));
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table schema", K(index_schema->get_data_table_id()), K(ret));
      } else {
        index_dml_info->table_id_ = table_info.table_id_;
        index_dml_info->loc_table_id_ = table_info.loc_table_id_;
        index_dml_info->ref_table_id_ = index_schema->get_data_table_id();
        index_dml_info->rowkey_cnt_ = index_schema->get_rowkey_column_num();
        index_dml_info->spk_cnt_ = index_schema->get_shadow_rowkey_column_num();
        index_dml_info->index_name_ = data_table_schema->get_table_name_str();
      }
    } else {
      // global index or primary table
      index_dml_info->table_id_ = table_info.table_id_;
      index_dml_info->loc_table_id_ = table_info.loc_table_id_;
      index_dml_info->ref_table_id_ = index_schema->get_table_id();
      index_dml_info->rowkey_cnt_ = index_schema->get_rowkey_column_num();
      index_dml_info->spk_cnt_ = index_schema->get_shadow_rowkey_column_num();
      index_dml_info->index_name_ = index_schema->get_table_name_str();
    }
    if (optimizer_context_.is_online_ddl()) {
      //由于现在local index和主表的tablet_id和partition_id都是独立编码的，从编码的角度不存在依赖关系
      //因此一定要让IndexDMLInfo里的table_id是local index自己的table_id，而不能是主表的
      //后面执行层需要使用local index自己的table_id去计算自己分区的tablet_id
      //为什么要在这里特殊处理？
      //因为在resolver阶段，由于解析上有一些地方依赖主表的table_id，
      //因此resolver阶段IndexDMLInfo上的index_tid是主表table_id,
      //在优化器阶段替换成索引表自己的table_id是方便优化器使用这些信息去生成local index自己的执行信息
      //这里先这样临时处理一下，保证create local index路径能够正常通过
      //@TODO: 后续@yibo, @cangdi会重构create local index的处理
      index_dml_info->ref_table_id_ = table_item->ddl_table_id_;
    }
    
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_all_rowkey_columns_for_ddl(table_info, index_schema, index_dml_info->column_exprs_))) {
        LOG_WARN("failed to get all rowkey columns for ddl" , K(ret));
      } else if (OB_FAIL(get_all_columns_for_ddl(table_info, index_schema, index_dml_info->column_exprs_))) {
        LOG_WARN("failed to get all columns for ddl" , K(ret));
      } else if (index_schema->is_index_table() && !index_schema->is_global_index_table() && data_table_schema->is_heap_table() && 
                 OB_FAIL(get_all_part_columns_for_ddl(table_info, data_table_schema, index_dml_info->column_exprs_))) {
        LOG_WARN("failed to get all part columns for ddl" , K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info->column_exprs_.count(); ++i) {
          bool find = false;
          int64_t idx = 0;
          ObColumnRefRawExpr* column = index_dml_info->column_exprs_.at(i);
          for (int64_t j = 0; !find && j < table_info.column_exprs_.count(); ++j) {
            if (table_info.column_exprs_.at(j) == column) {
              idx = j;
              find = true;
            }
          }
          if (OB_UNLIKELY(!find)) {
            ObRawExpr* column_conv = nullptr;
            if (OB_ISNULL(column) || OB_UNLIKELY(!is_shadow_column(column->get_column_id()))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("not find column expr in table info", K(ret), KPC(index_dml_info->column_exprs_.at(i)));
            } else if (OB_FAIL(build_column_conv_for_shadow_pk(table_info, column, column_conv))) { 
              LOG_WARN("failed to build column conv for shadow pk", K(ret));
            } else if (OB_FAIL(index_dml_info->column_convert_exprs_.push_back(column_conv))) {
              LOG_WARN("failed to push back column convert expr", K(ret));
            }
          } else if (OB_FAIL(index_dml_info->column_convert_exprs_.push_back(table_info.column_conv_exprs_.at(idx)))) {
            LOG_WARN("failed to push back column convert expr", K(ret));
          }
        }
        if (index_schema->is_unique_index()) {
          optimizer_context_.set_ddl_sample_column_count(index_schema->get_index_column_number());
          LOG_INFO("set ddl sample column count", K(optimizer_context_.get_ddl_sample_column_count()),
                    K(table_info));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(all_index_dml_infos.push_back(index_dml_info))) {
        LOG_WARN("failed to push back index dml info", K(ret));
      }
    }
  }
  return ret;
}

int ObInsertLogPlan::get_all_rowkey_columns_for_ddl(const ObInsertTableInfo& table_info,
                                                    const ObTableSchema* ddl_table_schema,
                                                    ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = NULL;
  
  // uint64_t ddl_table_id = table_item.ddl_table_id_;
  const ColumnItem* column_item = nullptr;
  const ObInsertStmt* stmt = get_stmt();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  if (OB_ISNULL(stmt) || OB_ISNULL(ddl_table_schema) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ddl_table_schema));
  } else {
    const ObRowkeyInfo &rowkey_info = ddl_table_schema->get_rowkey_info();
    uint64_t rowkey_column_id = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("get rowkey info failed", K(ret), K(i), K(rowkey_info));
      } else if (!is_shadow_column(rowkey_column_id)) {
        if (OB_ISNULL(column_item = stmt->get_column_item_by_id(table_info.table_id_,
                                                                rowkey_column_id))) {
        
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null column item", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(column_exprs, column_item->expr_))) {
          LOG_WARN("add column item to stmt failed", K(ret));
        }
      } else {
        // add shadow column
        ObColumnRefRawExpr *spk_expr = nullptr;
        if (OB_FAIL(ObRawExprUtils::build_shadow_pk_expr(table_info.table_id_,
                                                         rowkey_column_id, *stmt,
                                                         optimizer_context_.get_expr_factory(),
                                                         *session_info,
                                                         *ddl_table_schema,
                                                         spk_expr))) {
          LOG_WARN("failed to build shadow pk expr", K(ret), K(table_info), K(rowkey_column_id));
        } else {
          // ColumnItem column_item;
          // column_item.expr_ = spk_expr;
          // column_item.table_id_ = spk_expr->get_table_id();
          // column_item.column_id_ = spk_expr->get_column_id();
          // column_item.column_name_ = spk_expr->get_column_name();
          // column_item.base_tid_ = table_item.ddl_table_id_;
          // column_item.base_cid_ = column_item.column_id_;
          // if (OB_FAIL(stmt->add_column_item(column_item))) {
          //   LOG_WARN("add column item to stmt failed", K(ret));
          spk_expr->set_table_id(table_info.table_id_);
          spk_expr->set_explicited_reference();
          spk_expr->get_relation_ids().reuse();
          if (OB_FAIL(spk_expr->add_relation_id(stmt->get_table_bit_index(spk_expr->get_table_id())))) {
            LOG_WARN("add relation id to expr failed", K(ret));
          } else if (OB_FAIL(spk_expr->pull_relation_id())) {
            LOG_WARN("failed to pullup relation ids", K(ret));
          } else if (OB_FAIL(add_var_to_array_no_dup(column_exprs, spk_expr))) {
            LOG_WARN("fail to add column item to array", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObInsertLogPlan::get_all_columns_for_ddl(const ObInsertTableInfo& table_info,
                                             const ObTableSchema* ddl_table_schema,
                                             ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  const ColumnItem* column_item = nullptr;
  const ObInsertStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt) || OB_ISNULL(ddl_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ddl_table_schema));
  } else {
    ObTableSchema::const_column_iterator iter = ddl_table_schema->column_begin();
    ObTableSchema::const_column_iterator end = ddl_table_schema->column_end();
    for (; OB_SUCC(ret) && iter != end; ++iter) {
      const ObColumnSchemaV2 *column = *iter;
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", K(column));
      } else if (column->is_rowkey_column()) {
        // do nothing
      } else if (column->is_virtual_generated_column()) {
        // do not add virtual generated column since DDL operations directly insert into sstable.
      } else if (OB_ISNULL(column_item = stmt->get_column_item_by_id(table_info.table_id_,
                                                                     column->get_column_id()))) {
        
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column item", K(ret), KPC(column), KPC(stmt));
      } else if (OB_FAIL(add_var_to_array_no_dup(column_exprs, column_item->expr_))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      }
    } //end for
  }
  return ret;
}

int ObInsertLogPlan::get_all_part_columns_for_ddl(const ObInsertTableInfo& table_info,
                                                  const ObTableSchema* data_table_schema,
                                                  ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  const ColumnItem* column_item = nullptr;
  const ObInsertStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt) || OB_ISNULL(data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(data_table_schema));
  } else {
    ObTableSchema::const_column_iterator iter = data_table_schema->column_begin();
    ObTableSchema::const_column_iterator end = data_table_schema->column_end();
    for (; OB_SUCC(ret) && iter != end; ++iter) {
      const ObColumnSchemaV2 *column = *iter;
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", K(column));
      } else if (!column->is_tbl_part_key_column()) {
      } else if (OB_ISNULL(column_item = stmt->get_column_item_by_id(table_info.table_id_,
                                                                     column->get_column_id()))) {
        
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column item", K(ret), KPC(column), KPC(stmt));
      } else if (OB_FAIL(add_var_to_array_no_dup(column_exprs, column_item->expr_))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      } else if (column->is_generated_column()) {
        // need to add all cascaded columns for generate column
        ObSEArray<uint64_t, 5> cascaded_columns;
        if (OB_FAIL(column->get_cascaded_column_ids(cascaded_columns))) {
          LOG_WARN("failed to get cascaded_column_ids", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < cascaded_columns.count(); ++i) {
            if (OB_ISNULL(column_item = stmt->get_column_item_by_id(table_info.table_id_,
                                                                    cascaded_columns.at(i)))) {
              
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null column item", K(ret), KPC(column), KPC(stmt));
            } else if (OB_FAIL(add_var_to_array_no_dup(column_exprs, column_item->expr_))) {
              LOG_WARN("add column item to stmt failed", K(ret));
            }
          } // end cascased_columns for
        }
      }
    } //end for
  }
  return ret;
}

int ObInsertLogPlan::build_column_conv_for_shadow_pk(const ObInsertTableInfo& table_info,
                                                     ObColumnRefRawExpr *column_expr,
                                                     ObRawExpr *&column_conv_expr)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = table_info.table_id_;
  ObRawExpr *expr = NULL;
  ObRawExprCopier copier(optimizer_context_.get_expr_factory());
  if (OB_ISNULL(column_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_UNLIKELY(!is_shadow_column(column_expr->get_column_id()) || !column_expr->is_generated_column())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column", K(ret), KPC(column_expr));
  } else if (OB_UNLIKELY(table_info.column_exprs_.count() !=  table_info.column_conv_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column count", K(ret), K(table_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info.column_exprs_.count(); ++i) {
      if (OB_FAIL(copier.add_replaced_expr(table_info.column_exprs_.at(i),
                                           table_info.column_conv_exprs_.at(i)))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
    if (FAILEDx(copier.copy_on_replace(column_expr->get_dependant_expr(), expr))) {
      LOG_WARN("failed to copy expr expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should not be null", K(ret));
    } else if (ob_is_enum_or_set_type(expr->get_data_type())) {
      column_conv_expr = expr;
    } else {
      // For char type, compare and hash ignore space
      // For binary type, compare and hash not ignore '\0', so need to padding
      // '\0' for optimizer calculating partition location. As storage do right
      // trim of '\0', so don't worry extra space usage.
      //
      // shadow pk can not be partition key, so padding is not need?
      // if (ObObjMeta::is_binary(column_expr->get_data_type(), column_expr->get_collation_type())) {
      //   const ObColumnSchemaV2 *column_schema = NULL;
      //   if (OB_FAIL(get_column_schema(column_expr->get_table_id(), column_expr->get_column_id(), column_schema, true))) {
      //     LOG_WARN("fail to get column schema", K(ret), K(*column_expr));
      //   } else if (NULL == column_schema) {
      //     ret = OB_ERR_UNEXPECTED;
      //     LOG_WARN("get column schema fail", K(column_schema));
      //   } else if (OB_FAIL(build_padding_expr(optimizer_context_.get_session_info(), column_schema, expr))) {
      //     LOG_WARN("fail to build padding expr", K(ret));
      //   }
      // }
      if (FAILEDx(ObRawExprUtils::build_column_conv_expr(optimizer_context_.get_expr_factory(),
                                                        optimizer_context_.get_allocator(),
                                                        *column_expr,
                                                        expr,
                                                        optimizer_context_.get_session_info()))) {
        LOG_WARN("fail to build column conv expr", K(ret));
      } else {
        column_conv_expr = expr;
      }
    }
  }
  return ret;
}

int ObInsertLogPlan::candi_allocate_optimizer_stats_merge(OSGShareInfo *osg_info)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  CandidatePlan candidate_plan;
  ObSEArray<CandidatePlan, 4> stats_gathering_plan;
  ObSEArray<CandidatePlan, 8> best_candidates;
  if (OB_ISNULL(osg_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_, best_candidates))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < best_candidates.count(); i++) {
      ObExchangeInfo exch_info;
      if (OB_ISNULL(best_candidates.at(i).plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (best_candidates.at(i).plan_tree_->need_osg_merge()) {
        if (best_candidates.at(i).plan_tree_->is_sharding() &&
            OB_FAIL(allocate_exchange_as_top(best_candidates.at(i).plan_tree_, exch_info))) {
          LOG_WARN("failed to allocate exchange as top", K(ret));
        } else if (OB_FAIL(allocate_optimizer_stats_gathering_as_top(
                                                                best_candidates.at(i).plan_tree_,
                                                                *osg_info))) {
          LOG_WARN("failed to allocate sequence as top", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(stats_gathering_plan.push_back(best_candidates.at(i)))) {
        LOG_WARN("failed to push back candidate plan", K(ret));
      }
    } // end for
  }
  if (OB_SUCC(ret) && OB_FAIL(prune_and_keep_best_plans(stats_gathering_plan))) {
    LOG_WARN("failed to prune and keep best plans", K(ret));
  }
  return ret;
}
