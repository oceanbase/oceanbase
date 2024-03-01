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

#include "sql/optimizer/ob_log_group_by.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_update_log_plan.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_link_dml.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"
#include "sql/resolver/dml/ob_del_upd_resolver.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql::log_op_def;

/*
为了理解 ObUpdateLogPlan 代码，
需要知道：一个较为复杂的 update 语句，
可以包含 LIMIT、SORT、SubPlanFilter 等算子。

create table t1 (i int, j int);
create table t2 (i int, j int);
create table t3 (i int, j int);
Mysql:
explain update t2 set j = (select i from t1 limit 1) where i in (select j from t3) order by j limit 3\G
===============================================
|ID|OPERATOR           |NAME |EST. ROWS|COST  |
-----------------------------------------------
|0 |UPDATE             |     |3        |339326|
|1 | SUBPLAN FILTER    |     |3        |339323|
|2 |  LIMIT            |     |3        |339287|
|3 |   TOP-N SORT      |     |3        |339286|
|4 |    HASH JOIN      |     |98011    |258722|
|5 |     SUBPLAN SCAN  |VIEW1|101      |99182 |
|6 |      HASH DISTINCT|     |101      |99169 |
|7 |       TABLE SCAN  |t3   |100000   |66272 |
|8 |     TABLE SCAN    |t2   |100000   |68478 |
|9 |  TABLE SCAN       |t1   |1        |36    |
===============================================
Oracle:
explain update t2 set j = (select i from t1 where rownum < 2) where i in (select j from t3) and rownum < j\G
==============================================
|ID|OPERATOR          |NAME |EST. ROWS|COST  |
----------------------------------------------
|0 |UPDATE            |     |98011    |394701|
|1 | SUBPLAN FILTER   |     |98011    |296691|
|2 |  COUNT           |     |98011    |283128|
|3 |   HASH JOIN      |     |98011    |269600|
|4 |    SUBPLAN SCAN  |VIEW1|101      |99182 |
|5 |     HASH DISTINCT|     |101      |99169 |
|6 |      TABLE SCAN  |T3   |100000   |66272 |
|7 |    TABLE SCAN    |T2   |100000   |68478 |
|8 |  TABLE SCAN      |T1   |1        |36    |
==============================================
*/


int ObUpdateLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  /**
   *  Currently we only support update statement with just one table. Having more
   *  than one table would require rewriting the logic in this function.
   */
  const ObUpdateStmt *update_stmt = get_stmt();
  ObSQLSessionInfo *session = optimizer_context_.get_session_info();
  if (OB_ISNULL(update_stmt) || OB_ISNULL(session) || OB_ISNULL(optimizer_context_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    bool need_limit = true;
    ObSEArray<OrderItem, 4> order_items;
    LOG_TRACE("start to allocate operators for ", "sql", optimizer_context_.get_query_ctx()->get_sql_stmt());
    OPT_TRACE("generate plan for ", get_stmt());
    // step. generate access paths
    if (OB_FAIL(generate_plan_tree())) {
      LOG_WARN("failed to generate plan tree for plain select", K(ret));
    } else {
      LOG_TRACE("succ to generate plan tree", K(candidates_.candidate_plans_.count()));
    }
    // allocate subplan filter if needed, mainly for the subquery in where statement
    if (OB_SUCC(ret) && !get_subquery_filters().empty()) {
      if (OB_FAIL(candi_allocate_subplan_filter_for_where())) {
        LOG_WARN("failed to allocate subplan filter for where statement", K(ret));
      } else {
        LOG_TRACE("succeed to allocate subplan filter for where statement",
            K(candidates_.candidate_plans_.count()));
      }
    }
    // step. allocate 'count' for rownum if needed, Oracle mode only
    if(OB_SUCC(ret)) {
      bool has_rownum = false;
      if (OB_FAIL(update_stmt->has_rownum(has_rownum))) {
        LOG_WARN("failed to get rownum info", K(ret));
      } else if (has_rownum) {
        if (OB_FAIL(candi_allocate_count())) {
          LOG_WARN("failed to allocate count opeartor", K(ret));
        } else {
          LOG_TRACE("succeed to allocate count operator",
              K(candidates_.candidate_plans_.count()));
        }
      }
    }

    // step. allocate 'order-by' if needed, MySQL mode only
    if (OB_SUCC(ret) && update_stmt->has_order_by()) {
      if (OB_FAIL(candi_allocate_order_by(need_limit, order_items))) {
        LOG_WARN("failed to allocate order by operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate order by operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // step. allocate 'limit' if needed
    if (OB_SUCC(ret) && update_stmt->has_limit() && need_limit) {
      // 说明：MySQL 模式下使用 limit， Oracle 模式下使用 rownum < constant 时，会生成 limit 算子
      if (OB_FAIL(candi_allocate_limit(order_items))) {
        LOG_WARN("failed to allocate limit operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate limit operator",
            K(candidates_.candidate_plans_.count()));
      }
    }
    
    if (OB_SUCC(ret) && lib::is_mysql_mode() && update_stmt->has_for_update()) {
      if (OB_FAIL(candi_allocate_for_update())) {
        LOG_WARN("failed to allocate for update operator", K(ret));
      }
    }

    // step. allocate 'sequence' if needed
    if (OB_SUCC(ret) && update_stmt->has_sequence()) {
      if (OB_FAIL(candi_allocate_sequence())) {
        LOG_WARN("failed to allocate sequence operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate sequence operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    //
    // 以上是针对被 update 表相关的查询部分，其输出是符合条件的行
    // 下面针对 assign 部分涉及的查询生成计划，其输出是更新后的值
    //
    if (OB_SUCC(ret)) {
      ObSEArray<ObRawExpr*, 4> normal_query_refs;
      ObSEArray<ObRawExpr*, 4> alias_query_refs;
      if (OB_FAIL(extract_assignment_subqueries(normal_query_refs, alias_query_refs))) {
        LOG_WARN("failed to get assignment subquery exprs", K(ret));
      } else if (!normal_query_refs.empty() &&
                 OB_FAIL(candi_allocate_subplan_filter(normal_query_refs, NULL, false))) {
        // step. allocate subplan filter for "update .. set c1 = (select)"
        LOG_WARN("failed to allocate subplan", K(ret));
      } else if (!alias_query_refs.empty() &&
                 OB_FAIL(candi_allocate_subplan_filter(alias_query_refs, NULL, true))) {
        // step. allocate subplan filter for "update .. set (a,b)=(select..), (c,d)=(select..)"
        LOG_WARN("failed to allocate subplan filter", K(ret));
      } else {
        LOG_TRACE("succeed to allocate subplan filter for assignment", K(ret),
                              K(normal_query_refs.count()), K(alias_query_refs.count()));
      }
    }

    if (OB_SUCC(ret) && update_stmt->is_error_logging()) {
      if (OB_FAIL(candi_allocate_err_log(update_stmt))) {
        LOG_WARN("fail to allocate err_log", K(ret));
      } else {
        LOG_TRACE("succeed to allocate err log", K(candidates_.candidate_plans_.count()));
      }
    }

    // step. allocate update operator
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compute_dml_parallel())) {  // compute parallel before call prepare_dml_infos
        LOG_WARN("failed to compute dml parallel", K(ret));
      } else if (OB_FAIL(prepare_dml_infos())) {
        LOG_WARN("failed to prepare dml infos", K(ret));
      } else if (use_pdml()) {
        // PDML计划
        if (OB_FAIL(candi_allocate_pdml_update())) {
          LOG_WARN("failed to allocate pdml update operator", K(ret));
        } else {
          LOG_TRACE("succeed to allocate pdml update operator",
              K(candidates_.candidate_plans_.count()));
        }
        // normal update 计划
      } else {
        if (OB_FAIL(candi_allocate_update())) {
          LOG_WARN("failed to allocate update operator", K(ret));
        } else {
          LOG_TRACE("succeed to allocate normal update operator",
              K(candidates_.candidate_plans_.count()));
        }
      }
    }

    // step. allocate scalar operator
    if (OB_SUCC(ret) && update_stmt->get_returning_aggr_item_size() > 0) {
      // returning 逻辑用于存储过程中将最终结果做聚集后填入一个变量，例如：
      //
      // DECLARE
      //   l_max_id NUMBER;
      // BEGIN
      //   UPDATE t1
      //   SET    description = description
      //   RETURNING MAX(id) INTO l_max_id;
      //
      //   DBMS_OUTPUT.put_line('l_max_id=' || l_max_id);
      //
      //   COMMIT;
      // END;
      //
      // 参考：https://oracle-base.com/articles/misc/dml-returning-into-clause#aggregation
      if (OB_FAIL(candi_allocate_scala_group_by(update_stmt->get_returning_aggr_items()))) {
        LOG_WARN("failed to allocate group by opeartor", K(ret));
      } else {
        LOG_TRACE("succeed to allocate group by operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    //allocate temp-table transformation if needed.
    if (OB_SUCC(ret) && !get_optimizer_context().get_temp_table_infos().empty()) {
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
      } else if (lib::is_mysql_mode() && !update_stmt->has_limit() &&
                 OB_FAIL(check_fullfill_safe_update_mode(get_plan_root()))) {
        LOG_WARN("failed to check fullfill safe update mode", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObUpdateLogPlan::candi_allocate_update()
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *lock_row_flag_expr = NULL;
  ObSEArray<CandidatePlan, 8> candi_plans;
  ObSEArray<CandidatePlan, 8> update_plans;
  const bool force_no_multi_part = get_log_plan_hint().no_use_distributed_dml();
  const bool force_multi_part = get_log_plan_hint().use_distributed_dml();
  OPT_TRACE("start generate normal update plan");
  OPT_TRACE("force no multi part:", force_no_multi_part);
  OPT_TRACE("force multi part:", force_multi_part);
  if (OB_FAIL(check_table_rowkey_distinct(index_dml_infos_))) {
    LOG_WARN("failed to check table rowkey distinct", K(ret));
  } else if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_,
                                                 candi_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_var_int_expr(optimizer_context_.get_expr_factory(),
                                                        lock_row_flag_expr))) {
    LOG_WARN("fail to create expr", K(ret));
  } else if (OB_FAIL(lock_row_flag_expr->formalize(optimizer_context_.get_session_info()))) {
    LOG_WARN("fail to formalize", K(ret));
  } else if (OB_FAIL(create_update_plans(candi_plans, lock_row_flag_expr,
                                         force_no_multi_part, force_multi_part,
                                         update_plans))) {
    LOG_WARN("failed to create update plans", K(ret));
  } else if (!update_plans.empty()) {
    LOG_TRACE("succeed to create update plan using hint", K(update_plans.count()));
  } else if (OB_FAIL(get_log_plan_hint().check_status())) {
    LOG_WARN("failed to generate plans with hint", K(ret));
  } else if (OB_FAIL(create_update_plans(candi_plans, lock_row_flag_expr,
                                         false, false,
                                         update_plans))) {
    LOG_WARN("failed to create update plans", K(ret));
  } else {
    LOG_TRACE("succeed to create update plan ignore hint", K(update_plans.count()));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_and_keep_best_plans(update_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObUpdateLogPlan::create_update_plans(ObIArray<CandidatePlan> &candi_plans,
                                         ObConstRawExpr *lock_row_flag_expr,
                                         const bool force_no_multi_part,
                                         const bool force_multi_part,
                                         ObIArray<CandidatePlan> &update_plans)
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
    } else if (OB_FAIL(allocate_update_as_top(candi_plan.plan_tree_,
                                              lock_row_flag_expr,
                                              is_multi_part_dml))) {
      LOG_WARN("failed to allocate update as top", K(ret));
    } else if (OB_FAIL(update_plans.push_back(candi_plan))) {
      LOG_WARN("failed to push back", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObUpdateLogPlan::allocate_update_as_top(ObLogicalOperator *&top,
                                            ObConstRawExpr *lock_row_flag_expr,
                                            bool is_multi_part_dml)
{
  int ret = OB_SUCCESS;
  ObLogUpdate *update_op = nullptr;
  const ObUpdateStmt *update_stmt = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(update_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(top), K(update_stmt));
  } else if (OB_ISNULL(update_op = static_cast<ObLogUpdate*>(
                         get_log_op_factory().allocate(*this, LOG_UPDATE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate update operator", K(ret));
  } else if (OB_FAIL(update_op->assign_dml_infos(index_dml_infos_))) {
    LOG_WARN("failed to assign dml infos", K(ret));
  } else {
    update_op->set_child(ObLogicalOperator::first_child, top);
    update_op->set_ignore(update_stmt->is_ignore());
    update_op->set_is_returning(update_stmt->is_returning());
    update_op->set_has_instead_of_trigger(update_stmt->has_instead_of_trigger());
    update_op->set_is_multi_part_dml(is_multi_part_dml);
    update_op->set_lock_row_flag_expr(lock_row_flag_expr);
    if (update_stmt->is_error_logging() && OB_FAIL(update_op->extract_err_log_info())) {
      LOG_WARN("failed to extract error log info", K(ret));
    } else if (OB_FAIL(update_stmt->get_view_check_exprs(update_op->get_view_check_exprs()))) {
      LOG_WARN("failed to get view check exprs", K(ret));
    } else if (OB_FAIL(update_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = update_op;
    }
  }
  return ret;
}

int ObUpdateLogPlan::candi_allocate_pdml_update()
{
  int ret = OB_SUCCESS;
  const ObUpdateStmt *update_stmt = NULL;
  OPT_TRACE("start generate pdml update plan");
  if (OB_ISNULL(update_stmt = get_stmt()) ||
      OB_UNLIKELY(1 != update_stmt->get_update_table_info().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(ret));
  } else {
    int64_t gidx_cnt = index_dml_infos_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < gidx_cnt; i++) {
      IndexDMLInfo *index_dml_info = index_dml_infos_.at(i);
      if (OB_ISNULL(index_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret));
      } else if (index_dml_info->is_update_part_key_ ||
                 index_dml_info->is_update_unique_key_ ||
                 index_dml_info->is_update_primary_key_) {
        IndexDMLInfo *index_delete_info = nullptr;
        IndexDMLInfo *index_insert_info = nullptr;
        // 更新了当前索引的分区键，需要做 row-movement
        // 需要为每一个global index以及primary index分配各自的 update operator，形成如下的计划:
        //  ....
        //    INSERT INDEX (i3)
        //      DELETE INDEX (i3)
        //        INSERT INDEX (i2)
        //          DELETE INDEX (i2)
        //              ....
        if (OB_FAIL(split_update_index_dml_info(*index_dml_info,
                                                       index_delete_info,
                                                       index_insert_info))) {
          LOG_WARN("failed to create index delete info", K(ret));
        } else {
          const bool is_pdml_update_split = true;
          if (OB_FAIL(candi_allocate_one_pdml_delete(i > 0,
                                                     i == gidx_cnt - 1,
                                                     is_pdml_update_split,
                                                     index_delete_info))) {
            LOG_WARN("failed to allocate one pdml delete operator", K(ret));
          } else if (OB_FAIL(candi_allocate_one_pdml_insert(i > 0,
                                                            i == gidx_cnt - 1,
                                                            is_pdml_update_split,
                                                            index_insert_info))) {
            LOG_WARN("failed to allocate one pdml insert operator", K(ret));
          } else {
            LOG_TRACE("succeed to allocate pdml row-movement update");
          }
        }
      } else {
        // 在PDML update中，可能包含有多个global index，
        // 需要为每一个global index以及primary index分配各自的 update operator，形成如下的计划:
        //  ....
        //    UPDATE INDEX (i3)
        //       UPDATE INDEX (i2)
        //         UPDATE INDEX (i1)
        //           UPDATE
        //
        if (OB_FAIL(candi_allocate_one_pdml_update(i > 0,
                                                   i == gidx_cnt - 1,
                                                   index_dml_info))) {
          LOG_WARN("failed to allocate one pdml update operator", K(ret));
        } else {
          LOG_TRACE("succeed to allocate pdml update operator", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUpdateLogPlan::perform_vector_assign_expr_replacement(ObUpdateStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObUpdateTableInfo* table_info = nullptr;
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  if (OB_ISNULL(stmt) || OB_ISNULL(table_info = stmt->get_update_table_info().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(table_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info->assignments_.count(); ++i) {
      ObRawExpr *value = table_info->assignments_.at(i).expr_;
      bool replace_happened = false;
      if (OB_FAIL(replace_alias_ref_expr(value, replace_happened))) {
        LOG_WARN("failed to replace alias ref expr", K(ret));
      } else if (replace_happened && OB_FAIL(value->formalize(session_info))) {
        LOG_WARN("failed to formalize expr", K(ret));
      }
    }
  }
  return ret;
}

int ObUpdateLogPlan::replace_alias_ref_expr(ObRawExpr *&expr, bool &replace_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret));
  } else if (expr->is_alias_ref_expr()) {
    ObAliasRefRawExpr *alias = static_cast<ObAliasRefRawExpr *>(expr);
    if (OB_UNLIKELY(!alias->is_ref_query_output())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid alias expr", K(ret), K(*alias));
    } else {
      expr = alias->get_ref_expr();
      replace_happened = true;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(replace_alias_ref_expr(expr->get_param_expr(i), replace_happened))) {
        LOG_WARN("failed to replace alias ref expr", K(ret));
      }
    }
  }
  return ret;
}

int ObUpdateLogPlan::extract_assignment_subqueries(ObIArray<ObRawExpr*> &normal_query_refs,
                                                   ObIArray<ObRawExpr*> &alias_query_refs)
{
  int ret = OB_SUCCESS;
  const ObUpdateStmt *update_stmt = get_stmt();
  ObSEArray<ObRawExpr*, 8> assign_exprs;
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(update_stmt));
  } else if (OB_FAIL(update_stmt->get_assignments_exprs(assign_exprs))) {
    LOG_WARN("failed to get assign exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < assign_exprs.count(); ++i) {
      if (OB_FAIL(extract_assignment_subqueries(assign_exprs.at(i),
                                                normal_query_refs,
                                                alias_query_refs))) {
        LOG_WARN("failed to replace alias ref expr", K(ret));
      }
    }
  }
  return ret;
}

int ObUpdateLogPlan::extract_assignment_subqueries(ObRawExpr *expr,
                                                   ObIArray<ObRawExpr*> &normal_query_refs,
                                                   ObIArray<ObRawExpr*> &alias_query_refs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->has_flag(CNT_ONETIME)
             || expr->is_query_ref_expr()
             || T_OP_EXISTS == expr->get_expr_type()
             || T_OP_NOT_EXISTS == expr->get_expr_type()
             || expr->has_flag(IS_WITH_ALL)
             || expr->has_flag(IS_WITH_ANY)) {
    if (OB_FAIL(add_var_to_array_no_dup(normal_query_refs, expr))) {
      LOG_WARN("failed to add var to array no dup", K(ret));
    }
  } else if (expr->is_alias_ref_expr()) {
    ObAliasRefRawExpr *alias = static_cast<ObAliasRefRawExpr *>(expr);
    if (OB_UNLIKELY(!alias->is_ref_query_output())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid alias expr", K(ret), K(*alias));
    } else if (OB_FAIL(add_var_to_array_no_dup(alias_query_refs, alias->get_param_expr(0)))) {
      LOG_WARN("failed to add var to array with out duplicate", K(ret));
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_assignment_subqueries(expr->get_param_expr(i),
                                                           normal_query_refs,
                                                           alias_query_refs)))) {
        LOG_WARN("failed to extract query ref expr", K(ret));
      }
    }
  }
  return ret;
}

int ObUpdateLogPlan::prepare_dml_infos()
{
  int ret = OB_SUCCESS;
  const ObUpdateStmt *update_stmt = get_stmt();
  if (OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    const ObIArray<ObUpdateTableInfo*>& table_infos = update_stmt->get_update_table_info();
    bool has_tg = update_stmt->has_instead_of_trigger();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      ObUpdateTableInfo* table_info = table_infos.at(i);
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

int ObUpdateLogPlan::prepare_table_dml_info_special(const ObDmlTableInfo& table_info,
                                                    IndexDMLInfo* table_dml_info,
                                                    ObIArray<IndexDMLInfo*> &index_dml_infos,
                                                    ObIArray<IndexDMLInfo*> &all_index_dml_infos)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = optimizer_context_.get_schema_guard();
  ObSQLSessionInfo* session_info = optimizer_context_.get_session_info();
  const ObTableSchema* index_schema = NULL;
  const ObUpdateStmt *update_stmt = get_stmt();
  const ObUpdateTableInfo& update_info = static_cast<const ObUpdateTableInfo&>(table_info);
  if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info) || OB_ISNULL(update_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null",K(ret), K(schema_guard), K(session_info), K(update_stmt));
  } else if (OB_FAIL(table_dml_info->init_assignment_info(update_info.assignments_,
                                                          optimizer_context_.get_expr_factory()))) {
    LOG_WARN("failed to init assignemt info", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                    table_info.ref_table_id_, index_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table schema", K(table_info), K(ret));
  } else if (!update_stmt->has_instead_of_trigger() &&
             OB_FAIL(check_update_primary_key(index_schema, table_dml_info))) {
    LOG_WARN("failed to check update unique key", K(ret));
  } else if (!table_info.is_link_table_ &&
             OB_FAIL(check_update_part_key(index_schema, table_dml_info))) {
    LOG_WARN("failed to check update part key", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_dml_info->ck_cst_exprs_.count(); ++i) {
      if (OB_FAIL(ObDMLResolver::copy_schema_expr(optimizer_context_.get_expr_factory(),
                                                  table_dml_info->ck_cst_exprs_.at(i),
                                                  table_dml_info->ck_cst_exprs_.at(i)))) {
        LOG_WARN("failed to copy schema expr", K(ret));
      } else if (OB_FAIL(ObTableAssignment::expand_expr(optimizer_context_.get_expr_factory(),
                                                        update_info.assignments_,
                                                        table_dml_info->ck_cst_exprs_.at(i)))) {
        LOG_WARN("failed to create expanded expr", K(ret));
      }
    }
  }
  
  if (OB_SUCC(ret) && !index_dml_infos.empty()) {
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
      } else if (OB_FAIL(check_index_update(update_info.assignments_, *index_schema, 
                                            update_info.table_id_ != update_info.loc_table_id_,
                                            index_update))) {
        LOG_WARN("failed to check index update", K(ret), K(update_info));
      } else if (!index_update) {
          // do nothing
      } else if (OB_FAIL(generate_index_column_exprs(update_info.table_id_,
                                                     *index_schema,
                                                     update_info.assignments_,
                                                     index_dml_info->column_exprs_))) {
        LOG_WARN("resolve index related column exprs failed", K(ret));
      } else if (OB_FAIL(index_dml_info->init_assignment_info(update_info.assignments_,
                                                              optimizer_context_.get_expr_factory()))) {
        LOG_WARN("failed to init assignment info", K(ret));
      } else if (OB_FAIL(check_update_unique_key(index_schema, index_dml_info))) {
        LOG_WARN("failed to check update unique key", K(ret));
      } else if (!table_info.is_link_table_ &&
                 OB_FAIL(check_update_part_key(index_schema, index_dml_info))) {
        LOG_WARN("failed to check update part key", K(ret));
      } else if (OB_FAIL(index_dml_infos.push_back(index_dml_info))) {
        LOG_WARN("failed to push back index dml info", K(ret));
      }
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
