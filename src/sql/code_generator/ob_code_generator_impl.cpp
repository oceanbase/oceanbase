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

#define USING_LOG_PREFIX SQL_CG
#include "ob_code_generator_impl.h"
#include "lib/container/ob_iarray.h"
#include "lib/alloc/malloc_hook.h"
#include "common/ob_common_types.h"  // for ObQueryFlag
#include "common/row/ob_row_store.h"
#include "share/schema/ob_schema_mgr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/engine/join/ob_basic_nested_loop_join.h"
#include "sql/engine/join/ob_nested_loop_join.h"
#include "sql/engine/join/ob_block_based_nested_loop_join.h"
#include "sql/engine/join/ob_hash_join.h"
#include "sql/engine/join/ob_join.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/table/ob_mv_table_scan.h"
#include "sql/engine/table/ob_row_sample_scan.h"
#include "sql/engine/table/ob_block_sample_scan.h"
#include "sql/engine/table/ob_domain_index.h"
#include "sql/engine/table/ob_table_scan_with_index_back.h"
#include "sql/engine/px/ob_granule_iterator.h"
#include "sql/engine/table/ob_table_lookup.h"
#include "sql/engine/table/ob_multi_part_table_scan.h"
#include "sql/engine/table/ob_table_row_store.h"
#include "sql/engine/join/ob_merge_join.h"
#include "sql/engine/aggregate/ob_groupby.h"
#include "sql/engine/aggregate/ob_hash_groupby.h"
#include "sql/engine/aggregate/ob_merge_groupby.h"
#include "sql/engine/sort/ob_sort.h"
#include "sql/engine/sequence/ob_sequence.h"
#include "sql/engine/basic/ob_topk.h"
#include "sql/engine/dml/ob_table_lock.h"
#include "sql/engine/dml/ob_multi_part_lock.h"
#include "sql/engine/dml/ob_table_delete.h"
#include "sql/engine/dml/ob_table_delete_returning.h"
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/engine/dml/ob_table_merge.h"
#include "sql/engine/dml/ob_table_insert_up.h"
#include "sql/engine/dml/ob_table_insert_returning.h"
#include "sql/engine/dml/ob_table_update.h"
#include "sql/engine/dml/ob_table_update_returning.h"
#include "sql/engine/dml/ob_table_replace.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/dml/ob_table_conflict_row_fetcher.h"
#include "sql/engine/pdml/ob_px_multi_part_delete.h"
#include "sql/engine/pdml/ob_px_multi_part_update.h"
#include "sql/engine/pdml/ob_px_multi_part_insert.h"
#include "sql/engine/basic/ob_expr_values.h"
#include "sql/engine/basic/ob_expr_values_with_child.h"
#include "sql/engine/basic/ob_values.h"
#include "sql/engine/basic/ob_material.h"
#include "sql/engine/basic/ob_select_into.h"
#include "sql/engine/basic/ob_count.h"
#include "sql/engine/basic/ob_monitoring_dump.h"
#include "sql/engine/basic/ob_temp_table_insert.h"
#include "sql/engine/basic/ob_temp_table_access.h"
#include "sql/engine/basic/ob_temp_table_transformation.h"
#include "sql/engine/aggregate/ob_distinct.h"
#include "sql/engine/aggregate/ob_scalar_aggregate.h"
#include "sql/engine/set/ob_set_operator.h"
#include "sql/engine/set/ob_merge_set_operator.h"
#include "sql/engine/set/ob_hash_set_operator.h"
#include "sql/engine/set/ob_merge_union.h"
#include "sql/engine/set/ob_merge_intersect.h"
#include "sql/engine/set/ob_merge_except.h"
#include "sql/engine/set/ob_append.h"
#include "sql/engine/recursive_cte/ob_recursive_union_all.h"
#include "sql/engine/recursive_cte/ob_fake_cte_table.h"
#include "sql/engine/subquery/ob_subplan_filter.h"
#include "sql/engine/subquery/ob_subplan_scan.h"
#include "sql/engine/window_function/ob_window_function.h"
#include "sql/engine/px/ob_px_fifo_coord.h"
#include "sql/engine/px/ob_px_merge_sort_coord.h"
#include "sql/engine/table/ob_link_scan.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/connect_by/ob_nested_loop_connect_by.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/executor/ob_receive.h"
#include "sql/executor/ob_root_transmit.h"
#include "sql/executor/ob_direct_receive.h"
#include "sql/executor/ob_fifo_receive.h"
#include "sql/engine/px/exchange/ob_px_merge_sort_receive.h"
#include "sql/engine/px/ob_light_granule_iterator.h"
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_direct_transmit.h"
#include "sql/executor/ob_distributed_transmit.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_insert_log_plan.h"
#include "sql/parser/ob_parser.h"
#include "share/ob_encryption_util.h"
#include "sql/engine/dml/ob_multi_table_merge.h"
#include "sql/engine/px/exchange/ob_px_transmit.h"
#include "sql/engine/expr/ob_iter_expr_range_param.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/engine/subquery/ob_unpivot.h"
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction;

class ObCodeGeneratorImpl::ColumnIndexProviderImpl : public jit::expr::ObColumnIndexProvider {
public:
  ColumnIndexProviderImpl(const RowDesc& input_row_desc, const int32_t* projector = NULL, int64_t projector_size = 0)
      : left_row_desc_(&input_row_desc), right_row_desc_(NULL), projector_(projector), projector_size_(projector_size)
  {}
  ColumnIndexProviderImpl(const RowDesc& left_row_desc, const RowDesc& right_row_desc)
      : left_row_desc_(&left_row_desc), right_row_desc_(&right_row_desc), projector_(NULL), projector_size_(0)
  {}
  int get_idx(const jit::expr::ObExpr* raw_expr, int64_t& idx) const override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(left_row_desc_->get_idx(raw_expr, idx)) && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get idx failed", K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      if (NULL != right_row_desc_) {
        // left_row and right_row is the input row
        if (OB_FAIL(right_row_desc_->get_idx(raw_expr, idx))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("get idx failed", K(ret));
          }
        } else {
          idx += left_row_desc_->get_column_num();
        }
      }
    } else {
      if (NULL != projector_) {
        // by projector
        int64_t projector_idx = OB_INVALID_INDEX;
        for (int64_t i = 0; i < projector_size_; ++i) {
          if (projector_[i] == idx) {
            projector_idx = i;
            break;
          }
        }  // end for
        if (OB_INVALID_INDEX == projector_idx) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("idx not exist in projector", K(ret), K(idx), K(*left_row_desc_), K(projector_size_));
        }
        idx = projector_idx;
      }
    }
    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ColumnIndexProviderImpl);

private:
  const RowDesc* left_row_desc_;
  const RowDesc* right_row_desc_;
  const int32_t* projector_;
  const int64_t projector_size_;
};
////////////////////////////////////////////////////////////////
ObCodeGeneratorImpl::ObCodeGeneratorImpl(uint64_t min_cluster_version)
    : phy_plan_(NULL), cur_tbl_op_id_(OB_INVALID_ID), min_cluster_version_(min_cluster_version)
{}

template <typename T>
int ObCodeGeneratorImpl::generate_returning_exprs(
    T* phy_op, const common::ObIArray<ObRawExpr*>* returning_exprs, RowDesc* returning_row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(returning_exprs) || OB_ISNULL(returning_row_desc) || OB_ISNULL(phy_op) || OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ARRAY_FOREACH(*returning_exprs, i)
    {
      ObRawExpr* raw_expr = returning_exprs->at(i);
      if ((raw_expr->get_expr_type() != T_REF_COLUMN) && (raw_expr->get_expr_type() != T_REF_ALIAS_COLUMN)) {
        raw_expr->clear_flag(IS_COLUMNLIZED);
      }
      ObSqlExpression* expr = NULL;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), expr))) {
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(generate_expr(*raw_expr, *phy_plan_, *returning_row_desc, *expr))) {
        LOG_WARN("generate expr failed", K(ret));
      } else if (OB_FAIL(phy_op->add_returning_expr(expr))) {
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  phy_plan_ = &phy_plan;
  PhyOpsDesc out_phy_ops;
  if (OB_INVALID_ID != log_plan.get_max_op_id()) {
    phy_plan_->set_next_phy_operator_id(log_plan.get_max_op_id());
  }
  LOG_TRACE("trace code generate", K(phy_plan_), K(log_plan.get_max_op_id()), K(phy_plan_->get_next_phy_operator_id()));
  if (OB_FAIL(postorder_visit(*log_plan.get_plan_root(), out_phy_ops, true))) {
    LOG_WARN("fail to genenerate code", K(ret));
  } else if (OB_FAIL(phy_plan.add_phy_query(out_phy_ops.at(0).first))) {
    LOG_WARN("fail to add phy query", K(ret));
  } else if (OB_FAIL(set_other_properties(log_plan, phy_plan))) {
    LOG_WARN("set other properties failed", K(ret));
  }
  // ignore ret, destroy row_desc anyway
  for (int64_t i = 0; i < out_phy_ops.count(); ++i) {
    if (NULL != out_phy_ops.at(i).second) {
      ob_delete(out_phy_ops.at(i).second);
    }
  }  // end for
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(log_plan.get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_calculable_exprs(const ObDMLStmt& stmt, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_calculable_exprs(stmt.get_calculable_exprs(), stmt, phy_plan, phy_plan.get_pre_calc_exprs()))) {
    LOG_WARN("Failed to generate all calculable_exprs", K(ret));
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_calculable_exprs(const ObIArray<ObHiddenColumnItem>& calculable_exprs,
    const ObDMLStmt& stmt, ObPhysicalPlan& phy_plan, ObDList<ObSqlExpression>& pre_calc_exprs)
{
  int ret = OB_SUCCESS;
  RowDesc row_desc;
  ARRAY_FOREACH(calculable_exprs, i)
  {
    int64_t array_param_index = OB_INVALID_INDEX;
    ObSqlExpression* sql_expr = NULL;
    const ObHiddenColumnItem& hidden_item = calculable_exprs.at(i);
    if (OB_FAIL(create_expression(phy_plan.get_sql_expression_factory(), sql_expr))) {
      LOG_WARN("create sql expression failed", K(i), K(hidden_item), K(ret));
    } else if (OB_ISNULL(sql_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc invalid expr", K(ret), K(sql_expr));
    } else if (OB_FAIL(generate_expr(*hidden_item.expr_, phy_plan, row_desc, *sql_expr))) {
      LOG_WARN("generate sql expr failed", K(ret));
    } else if (OB_UNLIKELY(!pre_calc_exprs.add_last(sql_expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add calcaulable expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::get_array_param_index(hidden_item.expr_, array_param_index))) {
      LOG_WARN("get array param index failed", K(ret), KPC(hidden_item.expr_));
    } else if (array_param_index >= 0 && hidden_item.expr_->get_expr_type() != T_QUESTIONMARK) {
      // if the expr contains bind array param and needs to be actually computed
      // its result needs to be stored in new binding array
      sql_expr->set_need_construct_binding_array(true);
      sql_expr->set_array_param_index(array_param_index);
    }
  }
  if (OB_SUCC(ret)) {
    phy_plan.set_fetch_cur_time(stmt.get_fetch_cur_time());
    phy_plan.set_stmt_type(stmt.get_stmt_type());
    phy_plan.set_literal_stmt_type(stmt.get_literal_stmt_type());
  }
  return ret;
}

int ObCodeGeneratorImpl::set_other_properties(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  // set params info for plan cache
  ObSchemaGetterGuard* schema_guard = log_plan.get_optimizer_context().get_schema_guard();
  ObSQLSessionInfo* my_session = log_plan.get_optimizer_context().get_session_info();
  if (OB_ISNULL(log_plan.get_stmt()) || OB_ISNULL(schema_guard) || OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or schema guard is null", K(log_plan.get_stmt()), K(schema_guard), K(my_session), K(ret));
  } else {
    ret = phy_plan.set_params_info(*(log_plan.get_optimizer_context().get_params()));
  }

  if (OB_SUCC(ret)) {
    int64_t parallel = log_plan.get_optimizer_context().get_parallel();
    // set other params
    // set user var assignment property
    ObPhyPlanType location_type =
        (log_plan.get_location_type() == OB_PHY_PLAN_UNINITIALIZED ? log_plan.get_phy_plan_type()
                                                                   : log_plan.get_location_type());
    phy_plan.set_contains_assignment(log_plan.get_stmt()->is_contains_assignment());
    if (log_plan.get_stmt()->has_nested_sql()) {
      phy_plan.set_has_nested_sql(log_plan.get_stmt()->has_nested_sql());
    }
    phy_plan.set_require_local_execution(log_plan.is_require_local_execution());
    phy_plan.set_plan_type(log_plan.get_phy_plan_type());
    phy_plan.set_location_type(location_type);
    phy_plan.set_param_count(log_plan.get_stmt()->get_pre_param_size());
    phy_plan.set_signature(log_plan.get_signature());
    phy_plan.set_plan_hash_value(log_plan.get_signature());
    phy_plan.set_stmt_type(log_plan.get_stmt()->get_stmt_type());
    phy_plan.set_literal_stmt_type(log_plan.get_stmt()->get_literal_stmt_type());
    phy_plan.set_autoinc_params(const_cast<ObLogPlan&>(log_plan).get_autoinc_params());
    phy_plan.set_affected_last_insert_id(const_cast<ObLogPlan&>(log_plan).get_affected_last_insert_id());
    phy_plan.set_is_contain_virtual_table(log_plan.get_stmt()->get_query_ctx()->is_contain_virtual_table_);
    phy_plan.set_is_contain_inner_table(log_plan.get_stmt()->get_query_ctx()->is_contain_inner_table_);
    phy_plan.set_is_affect_found_row(log_plan.get_stmt()->is_affect_found_rows());
    phy_plan.set_has_top_limit(log_plan.get_stmt()->has_top_limit());
    phy_plan.set_use_px(true);
    phy_plan.set_px_dop(parallel);
    phy_plan.set_expected_worker_count(log_plan.get_expected_worker_count());
    phy_plan.set_is_batched_multi_stmt(log_plan.get_optimizer_context().is_batched_multi_stmt());
    phy_plan.set_need_consistent_snapshot(log_plan.need_consistent_read());
    LOG_DEBUG("set phy plan info", K(parallel), K(location_type), K(log_plan.get_phy_plan_type()));
  }

  if (OB_SUCC(ret)) {
    bool enable = false;
    if (OB_FAIL(log_plan.check_enable_plan_expiration(enable))) {
      LOG_WARN("failed to check enable plan expiration", K(ret));
    } else if (enable) {
      phy_plan.set_enable_plan_expiration(true);
    }
  }

  // set schema version and all base table version in phy plan
  if (OB_SUCC(ret)) {
    const ObIArray<ObSchemaObjVersion>* dependency_table = log_plan.get_stmt()->get_global_dependency_table();
    if (OB_ISNULL(dependency_table)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      bool exist = false;
      bool has_dep_table = false;
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < dependency_table->count(); i++) {
        if (DEPENDENCY_TABLE == dependency_table->at(i).object_type_) {
          uint64_t tenant_id = extract_tenant_id(dependency_table->at(i).object_id_);
          if (OB_FAIL(schema_guard->check_global_index_exist(tenant_id, dependency_table->at(i).object_id_, exist))) {
            LOG_WARN("fail to judge global index", K(ret));
          }
          has_dep_table = true;
        }
      }
      LOG_DEBUG("is contain global index or dep base table", K(exist), K(has_dep_table));
      phy_plan.set_is_contain_global_index(exist);
      phy_plan.set_is_dep_base_table(has_dep_table);

      for (int64_t i = 0; OB_SUCC(ret) && i < dependency_table->count(); i++) {
        if (DEPENDENCY_TABLE == dependency_table->at(i).object_type_) {
          const ObTableSchema* table_schema = NULL;
          if (OB_FAIL(schema_guard->get_table_schema(dependency_table->at(i).get_object_id(), table_schema))) {
            LOG_WARN("fail to get table schema", K(ret), "table_id", dependency_table->at(i).get_object_id());
          } else if (OB_ISNULL(table_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("fail to get table schema", K(ret), "table_id", dependency_table->at(i).get_object_id());
          } else {
            if (table_schema->is_oracle_trx_tmp_table()) {
              phy_plan.set_contain_oracle_trx_level_temporary_table();
            }
            if (table_schema->is_oracle_sess_tmp_table()) {
              phy_plan.set_contain_oracle_session_level_temporary_table();
            }
            LOG_DEBUG("plan contain temporary table",
                "trx level",
                table_schema->is_oracle_trx_tmp_table(),
                "session level",
                table_schema->is_oracle_sess_tmp_table());
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t tenant_schema_version = OB_INVALID_VERSION;
      int64_t sys_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(phy_plan.get_dependency_table().assign(*dependency_table))) {
        LOG_WARN("init dependency table store failed", K(ret));
      } else if (OB_FAIL(
                     schema_guard->get_schema_version(my_session->get_effective_tenant_id(), tenant_schema_version))) {
        LOG_WARN("fail to get schema version", K(ret), K(tenant_schema_version));
      } else if (OB_FAIL(schema_guard->get_schema_version(OB_SYS_TENANT_ID, sys_schema_version))) {
        LOG_WARN("fail to get schema version", K(ret), K(tenant_schema_version));
      } else {
        phy_plan.set_tenant_schema_version(tenant_schema_version);
        phy_plan.set_sys_schema_version(sys_schema_version);
      }
    }
  }

  // set table stat version in phy plan. It's used to re-generate plan when stat is stale
  if (OB_SUCC(ret)) {
    const ObIArray<ObOptTableStatVersion>* table_stat_versions = log_plan.get_stmt()->get_table_stat_versions();
    if (OB_ISNULL(table_stat_versions)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(phy_plan.get_table_stat_versions().assign(*table_stat_versions))) {
      LOG_WARN("init table stat versions failed", K(ret));
    }
  }

  // set merged version. Now it can be used to clean plan cache
  if (OB_SUCC(ret)) {
    phy_plan.set_merged_version(log_plan.get_optimizer_context().get_merged_version());
  }

  // set user and system variables
  if (OB_SUCC(ret)) {
    ret = phy_plan.set_vars(log_plan.get_stmt()->get_query_ctx()->variables_);
  }

  if (OB_SUCC(ret)) {
    // convert insert row param index map
    stmt::StmtType stmt_type = stmt::T_NONE;
    if (OB_FAIL(log_plan.get_stmt_type(stmt_type))) {
      LOG_WARN("get stmt type of log plan failed", K(ret));
    } else if (IS_INSERT_OR_REPLACE_STMT(stmt_type)) {
      const ObInsertLogPlan& insert_log_plan = static_cast<const ObInsertLogPlan&>(log_plan);
      const RowParamMap& row_params = insert_log_plan.get_row_params_map();
      PhyRowParamMap& phy_row_params = phy_plan.get_row_param_map();
      if (OB_FAIL(phy_row_params.prepare_allocate(row_params.count()))) {
        LOG_WARN("prepare allocate physical row param map failed", K(ret), K(row_params.count()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row_params.count(); ++i) {
        phy_row_params.at(i).set_allocator(&phy_plan.get_allocator());
        if (OB_FAIL(phy_row_params.at(i).init(row_params.at(i).count()))) {
          LOG_WARN("init physical row param map failed", K(ret), K(i));
        } else if (OB_FAIL(phy_row_params.at(i).assign(row_params.at(i)))) {
          LOG_WARN("assign row param array failed", K(ret), K(i));
        }
      }
    }
  }
  // set outline data
  if (OB_SUCC(ret)) {
    char* buf = NULL;
    int64_t pos = 0;
    if (NULL == (buf = static_cast<char*>(log_plan.get_allocator().alloc(OB_MAX_SQL_LENGTH)))) {
      LOG_WARN("sql arena allocator alloc failed", K(ret));
    } else if (OB_FAIL(log_plan.print_outline_oneline(buf, OB_MAX_SQL_LENGTH, pos))) {
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
        LOG_WARN("outline data size over flow");
      } else {
        LOG_WARN("fail to print outline data", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_string(
                   phy_plan_->get_allocator(), ObString(pos, buf), phy_plan_->stat_.bl_info_.outline_data_))) {
      LOG_WARN("fail to deep copy outline data", K(ret));
    } else {
      phy_plan_->loc_sensitive_hint_.from(log_plan.get_stmt()->get_stmt_hint());
    }
  }

  // set hints info
  if (OB_SUCC(ret)) {
    char* buf = NULL;
    int64_t pos = 0;
    if (NULL == (buf = static_cast<char*>(log_plan.get_allocator().alloc(OB_MAX_SQL_LENGTH)))) {
      LOG_WARN("sql arena allocator alloc failed", K(ret));
    } else {
      planText plan(buf, OB_MAX_SQL_LENGTH, EXPLAIN_OUTLINE);
      plan.is_oneline_ = true;
      plan.outline_type_ = USED_HINT;
      if (OB_FAIL(log_plan.print_outline(plan, true))) {
        if (OB_SIZE_OVERFLOW == ret) {
          ret = OB_SUCCESS;
          LOG_WARN("hints info size over flow");
        } else {
          LOG_WARN("fail to print outline", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        pos = plan.pos;
        if (OB_FAIL(ob_write_string(
                phy_plan_->get_allocator(), ObString(pos, buf), phy_plan_->stat_.bl_info_.hints_info_))) {
          LOG_WARN("fail to deep copy outline data", K(ret));
        } else {
          const ObStmtHint& stmt_hint = log_plan.get_stmt()->get_stmt_hint();
          phy_plan_->stat_.bl_info_.set_hints_all_worked(stmt_hint.is_hints_all_worked());
        }
      }
    }
  }
  // resolve the first array index of array binding, and store in physical plan
  // if (OB_SUCC(ret)
  //     && (!log_plan.get_optimizer_context().is_batched_multi_stmt())) {
  //   ObExecContext *exec_ctx = log_plan.get_optimizer_context().get_exec_ctx();
  //   ObPhysicalPlanCtx *plan_ctx = nullptr;
  //   if (OB_ISNULL(exec_ctx)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("exec context is null");
  //   } else if (OB_ISNULL(plan_ctx = exec_ctx->get_physical_plan_ctx())) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("plan context is null");
  //   } else {
  //     bool is_found = false;
  //     const ParamStore &param_store = plan_ctx->get_param_store();
  //     for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < param_store.count(); ++i) {
  //       if (param_store.at(i).is_ext()) {
  //         phy_plan.set_first_array_index(i);
  //         is_found = true;
  //       }
  //     }
  //   }
  // }

  return ret;
}

int ObCodeGeneratorImpl::postorder_visit(ObLogicalOperator& op, PhyOpsDesc& out_phy_ops, bool in_root_job)
{
  int ret = OB_SUCCESS;
  bool is_exchange = (log_op_def::LOG_EXCHANGE == op.get_type());
  bool is_link_scan = (log_op_def::LOG_LINK == op.get_type());
  int64_t num = op.get_num_of_child();
  PhyOpsDesc child_phy_ops;
  OZ(check_stack_overflow());
  for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
    ObLogicalOperator* child = op.get_child(i);
    if (OB_UNLIKELY(NULL == child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null pointer");
    } else {
      ret = SMART_CALL(postorder_visit(*child, child_phy_ops, in_root_job && !is_exchange));
    }
  }  // end for

  if (OB_SUCC(ret)) {
    ret = convert(op, child_phy_ops, out_phy_ops, in_root_job);
  }

  ObPhyOperator* phy_op = NULL;
  RowDesc* row_desc = NULL;
  if (OB_SUCC(ret)) {
    if (out_phy_ops.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("should have at least one output phy_ops");
    } else {
      phy_op = out_phy_ops.at(out_phy_ops.count() - 1).first;
      row_desc = out_phy_ops.at(out_phy_ops.count() - 1).second;
      // OB_ASSERT(NULL != phy_op);
      // OB_ASSERT(NULL != row_desc);
    }
  }

  if (OB_SUCC(ret)) {
    ret = convert_common_parts(op, *row_desc, *phy_op);
  }

  if (OB_SUCC(ret) && child_phy_ops.count() > 0 && !is_link_scan) {
    if (OB_FAIL(phy_op->create_child_array(child_phy_ops.count()))) {
      LOG_WARN("create child array failed", K(ret), K(child_phy_ops.count()));
    }
    ARRAY_FOREACH(child_phy_ops, i)
    {
      if (OB_FAIL(phy_op->set_child(static_cast<int32_t>(i), *child_phy_ops.at(i).first))) {
        LOG_WARN("failed to add child op", K(ret), K(i));
      } else {
        child_phy_ops.at(i).first->set_parent(phy_op);
      }
    }  // end for
  }

#ifndef NDEBUG
  if (OB_SUCC(ret)) {
    // debug
    LOG_DEBUG("operator converted", K(op.get_name()), K(*phy_op), K(*row_desc));
  }
#endif

  // destroy row_desc anyway
  for (int64_t i = 0, N = child_phy_ops.count(); i < N; ++i) {
    if (NULL != child_phy_ops.at(i).second) {
      ob_delete(child_phy_ops.at(i).second);
    }
  }  // end for
  return ret;
}

template <typename T>
int ObCodeGeneratorImpl::create_phy_op_desc(
    ObPhyOperatorType type, T*& phy_op, RowDesc*& out_row_desc, PhyOpsDesc& out_ops, const uint64_t op_id)
{
  int ret = OB_SUCCESS;
  out_row_desc = NULL;
  if (OB_ISNULL(phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("code generator not init");
  } else if (OB_FAIL(phy_plan_->alloc_operator_by_type(type, phy_op, op_id))) {
    LOG_WARN("failed to store phy op", K(ret));
  } else if (OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical operator is null");
  } else if (OB_UNLIKELY(NULL == (out_row_desc = OB_NEW(RowDesc, ObModIds::OB_SQL_CG)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory to create row desc");
  } else if (OB_FAIL(out_ops.push_back(std::make_pair(phy_op, out_row_desc)))) {
    LOG_WARN("failed to add phy ops", K(ret));
    ob_delete(out_row_desc);
    out_row_desc = NULL;
  } else if (OB_FAIL(out_row_desc->init())) {
    LOG_WARN("failed to init row desc", K(ret));
  }
  return ret;
}

template <typename T>
int ObCodeGeneratorImpl::create_expression(ObSqlExpressionFactory* factory, T*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(factory)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(factory));
  } else if (OB_FAIL(factory->alloc(expr))) {
    OB_LOG(WARN, "fail to alloc sql expression", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc invalid expr", K(ret));
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_expr(ObRawExpr& raw_expr, const RowDesc& row_desc, ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(generate_expr(raw_expr, *phy_plan_, row_desc, expr))) {
    LOG_WARN("fail to generate expr", K(ret));
  }

  return ret;
}

int ObCodeGeneratorImpl::generate_expr(
    ObRawExpr& raw_expr, ObPhysicalPlan& phy_plan, const RowDesc& row_desc, ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  ColumnIndexProviderImpl column_index_provider(row_desc);
  ObExprGeneratorImpl expr_gen(phy_plan.get_expr_op_factory(),
      phy_plan.get_regexp_op_count(),
      phy_plan.get_like_op_count(),
      &phy_plan.get_next_expr_id(),
      column_index_provider);
  if (OB_FAIL(expr_gen.generate(raw_expr, expr))) {
    LOG_WARN("failed to generate expr", K(ret), K(raw_expr));
  } else {
    phy_plan.set_regexp_op_count(expr_gen.get_cur_regexp_op_count());
    phy_plan.set_like_op_count(expr_gen.get_cur_like_op_count());
  }

  return ret;
}

int ObCodeGeneratorImpl::generate_expr(ObRawExpr& raw_expr, ObIterExprOperator*& iter_expr)
{
  int ret = OB_SUCCESS;
  RowDesc row_desc;
  ColumnIndexProviderImpl column_index_provider(row_desc);
  ObExprGeneratorImpl expr_gen(phy_plan_->get_expr_op_factory(),
      phy_plan_->get_regexp_op_count(),
      phy_plan_->get_like_op_count(),
      &phy_plan_->get_next_expr_id(),
      column_index_provider);
  if (OB_FAIL(expr_gen.generate(raw_expr, iter_expr))) {
    LOG_WARN("generate raw expr failed", K(ret));
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_expr(
    ObRawExpr& raw_expr, const RowDesc& left_row_desc, const RowDesc& right_row_desc, ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  ColumnIndexProviderImpl column_index_provider(left_row_desc, right_row_desc);
  ObExprGeneratorImpl expr_gen(phy_plan_->get_expr_op_factory(),
      phy_plan_->get_regexp_op_count(),
      phy_plan_->get_like_op_count(),
      &phy_plan_->get_next_expr_id(),
      column_index_provider);
  if (OB_FAIL(expr_gen.generate(raw_expr, expr))) {
    LOG_WARN("failed to generate expr", K(ret), K(raw_expr));
  } else {
    phy_plan_->set_regexp_op_count(expr_gen.get_cur_regexp_op_count());
    phy_plan_->set_like_op_count(expr_gen.get_cur_like_op_count());
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_expr(ObRawExpr& raw_expr, ObSqlExpression& expr, const RowDesc& row_desc,
    const int32_t* projector, int64_t projector_size)
{
  int ret = OB_SUCCESS;
  ColumnIndexProviderImpl column_index_provider(row_desc);
  ColumnIndexProviderImpl column_index_provider_real_index(row_desc, projector, projector_size);
  ColumnIndexProviderImpl& idx_provider =
      (T_FUN_GROUPING != raw_expr.get_expr_type()) ? column_index_provider : column_index_provider_real_index;
  ObExprGeneratorImpl expr_gen(phy_plan_->get_expr_op_factory(),
      phy_plan_->get_regexp_op_count(),
      phy_plan_->get_like_op_count(),
      &phy_plan_->get_next_expr_id(),
      idx_provider);
  if (OB_FAIL(expr_gen.generate(raw_expr, expr))) {
    LOG_WARN("failed to generate expr", K(ret), K(raw_expr));
  } else {
    phy_plan_->set_regexp_op_count(expr_gen.get_cur_regexp_op_count());
    phy_plan_->set_like_op_count(expr_gen.get_cur_like_op_count());
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_sql_expr(
    ObSqlExpressionFactory* factory, ObRawExpr* raw_expr, RowDesc& row_desc, ObSqlExpression*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr) || OB_ISNULL(phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw_expr is null", K(ret), K(raw_expr));
  } else {
    if (OB_FAIL(create_expression(factory, expr))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc invalid expr", K(ret), K(expr));
    } else if (OB_FAIL(generate_expr(*raw_expr, *phy_plan_, row_desc, *expr))) {
      LOG_WARN("failed to generate expr", K(ret), K(raw_expr));
    } else { /* Do nothing */
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::add_filter(
    const ObIArray<ObRawExpr*>& filter_exprs, const RowDesc& row_desc, ObPhyOperator& phy_op, const bool startup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(phy_plan_));
  }

  ARRAY_FOREACH(filter_exprs, i)
  {
    ObRawExpr* raw_expr = filter_exprs.at(i);
    ObSqlExpression* expr = NULL;
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null pointer");
    } else if (OB_FAIL(create_expression(phy_op.get_sql_expression_factory(), expr))) {
      LOG_WARN("failed to create expr");
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc invalid expr", K(ret), K(expr));
    } else if (OB_FAIL(generate_expr(*raw_expr, *phy_plan_, row_desc, *expr))) {
      LOG_WARN("failed to generate expr", K(ret), K(i), K(*raw_expr));
    } else if (OB_FAIL(phy_op.add_filter(expr, startup))) {
      LOG_WARN("failed to add filter", K(ret), K(i), K(*expr));
    }
  }  // end for
  return ret;
}

int ObCodeGeneratorImpl::add_compute(
    ObRawExpr& raw_expr, RowDesc& out_row_desc, RowDesc* extra_row_desc, ObPhyOperator& phy_op)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_FAIL(out_row_desc.get_idx(&raw_expr, idx)) && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("get idx failed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    int64_t cur_column_count = out_row_desc.get_column_num();
    bool use_extra_row_desc = raw_expr.is_for_generated_column() && OB_NOT_NULL(extra_row_desc);
    bool add_virtual_column =
        use_extra_row_desc &&
        (phy_op.get_type() == PHY_INSERT || phy_op.get_type() == PHY_MULTI_PART_INSERT ||
            phy_op.get_type() == PHY_INSERT_RETURNING || phy_op.get_type() == PHY_PX_MULTI_PART_INSERT ||
            phy_op.get_type() == PHY_PX_MULTI_PART_DELETE || phy_op.get_type() == PHY_PX_MULTI_PART_UPDATE);
    RowDesc& row_desc = use_extra_row_desc ? *extra_row_desc : out_row_desc;
    ObColumnExpression* expr = NULL;

    ret = OB_SUCCESS;
    if (OB_FAIL(create_expression(phy_op.get_sql_expression_factory(), expr))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_ISNULL(expr) || OB_ISNULL(phy_plan_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc invalid expr", K(ret), K(expr));
    } else if (OB_FAIL(generate_expr(raw_expr, *phy_plan_, row_desc, *expr))) {
      LOG_WARN("failed to generate expr", K(ret), K(raw_expr), K(row_desc));
    } else if (raw_expr.has_flag(IS_SYS_CONNECT_BY_PATH)) {
      if (PHY_NESTED_LOOP_CONNECT_BY != phy_op.get_type() &&
          PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX != phy_op.get_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected operator type", K(phy_op.get_type()), K(ret));
      } else {
        ObConnectBy& connect_by_join = static_cast<ObConnectBy&>(phy_op);
        if (OB_FAIL(connect_by_join.add_sys_connect_by_path(expr))) {
          LOG_WARN("Failed to add sys connect by path sql expression", K(ret));
        } else {
          LOG_DEBUG("Sys connect by path to connect by join added");
        }
      }
    } else if (add_virtual_column && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2220) {
      if (OB_FAIL(phy_op.add_virtual_column_expr(expr))) {
        LOG_WARN("failed to add generated column expr", K(ret), K(*expr));
      } else {
        LOG_DEBUG("add compute to virtual column expr succ", K(*expr));
      }
    } else {
      if (phy_op.get_type() == PHY_PX_REPART_TRANSMIT) {
        LOG_TRACE("skip adding compute expr", K(phy_op.get_name()), K(*expr));
      } else {
        if (OB_FAIL(phy_op.add_compute(expr))) {
          LOG_WARN("failed to add sql expr", K(ret), K(*expr), K(phy_op.get_name()), K(phy_op.get_id()));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(out_row_desc.add_column(&raw_expr))) {
        LOG_WARN("failed to add column desc", K(ret));
      } else {
        expr->set_result_index(cur_column_count);
        LOG_DEBUG("add compute", K(phy_op), K(&out_row_desc), K(cur_column_count), K(raw_expr));
      }
    }
  } else {
    LOG_DEBUG("add compute expr has already exist", K(raw_expr));
  }  // end if
  return ret;
}

int ObCodeGeneratorImpl::add_compute(
    const ObIArray<ObRawExpr*>& output_exprs, RowDesc& out_row_desc, RowDesc* extra_row_desc, ObPhyOperator& phy_op)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(output_exprs, i)
  {
    ObRawExpr* raw_expr = output_exprs.at(i);
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null pointer");
    } else {
      ret = add_compute(*raw_expr, out_row_desc, extra_row_desc, phy_op);
    }
  }  // end for
  return ret;
}

template <typename T>
int ObCodeGeneratorImpl::generate_projector(
    const ObIArray<T*>& output_exprs, const RowDesc& out_row_desc, int32_t*& projector, int64_t& projector_size)
{
  int ret = OB_SUCCESS;
  int64_t N = output_exprs.count();
  if (N > 0) {
    projector = phy_plan_->alloc_projector(N);
    projector_size = N;
    if (NULL == projector) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for projector failed", K(N));
    } else {
      int64_t idx = OB_INVALID_INDEX;
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        const T* output_expr = output_exprs.at(i);
        if (OB_FAIL(out_row_desc.get_idx(output_expr, idx))) {
          LOG_ERROR("the output column does not exist", K(ret), K(*output_expr), K(out_row_desc));
        } else {
          projector[i] = static_cast<int32_t>(idx);
          LOG_DEBUG("get output project", K(i), K(idx), K(*output_expr));
        }
      }
    }
  }
  return ret;
}

template <typename T>
int ObCodeGeneratorImpl::add_projector(const ObIArray<T>& output_exprs, RowDesc& out_row_desc, ObPhyOperator& phy_op)
{
  int ret = OB_SUCCESS;
  int32_t* projector = NULL;
  int64_t projector_size = 0;
  if (OB_FAIL(generate_projector(output_exprs, out_row_desc, projector, projector_size))) {
    LOG_WARN("generate projector failed", K(ret));
  } else if (projector_size > 0) {
    phy_op.set_projector(projector, projector_size);
  }
  return ret;
}

int ObCodeGeneratorImpl::add_column_infos(
    ObLogicalOperator& log_op, ObTableModify& phy_op, const ObIArray<ObColumnRefRawExpr*>& columns)
{
  int ret = OB_SUCCESS;
  ObString column_name;
  ObDMLStmt* stmt = log_op.get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(phy_op.init_column_infos_count(columns.count()))) {
    LOG_WARN("fail to init column infos count", K(ret));
  }
  phy_op.set_need_skip_log_user_error(true);
  ARRAY_FOREACH(columns, i)
  {
    const ObColumnRefRawExpr* column = columns.at(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column expr", K(ret), K(i));
    } else {
      const int64_t table_id = column->get_table_id();
      TableItem* table_item = stmt->get_table_item_by_id(table_id);
      if (OB_ISNULL(table_item)) {
        // column expr is shadow pk, read flag info from column
      } else if (table_item->is_generated_table() && OB_FAIL(recursive_get_column_expr(column, *table_item))) {
        LOG_WARN("failed to recursive get column expr", K(ret));
      }
      if (OB_SUCC(ret)) {
        ColumnContent column_content;
        column_content.auto_filled_timestamp_ = column->get_result_type().has_result_flag(OB_MYSQL_ON_UPDATE_NOW_FLAG);
        column_content.is_nullable_ = !column->get_result_type().has_result_flag(OB_MYSQL_NOT_NULL_FLAG);
        column_content.column_type_ = column->get_data_type();
        column_content.coll_type_ = column->get_collation_type();
        if (OB_FAIL(
                ob_write_string(phy_plan_->get_allocator(), column->get_column_name(), column_content.column_name_))) {
          LOG_WARN("failed to copy column name", K(ret), K(column->get_column_name()));
        } else if (OB_FAIL(phy_op.add_column_info(column_content))) {
          LOG_WARN("failed to add column info", K(ret), K(column->get_column_name()));
        } else {
          LOG_DEBUG("add column info", KPC(column));
        }
      }
    }
  }
  return ret;
}

// recursively lookup column expr's coresponding base table column expr from generated table
int ObCodeGeneratorImpl::recursive_get_column_expr(const ObColumnRefRawExpr*& column, const TableItem& table_item)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* stmt = table_item.ref_query_;
  const ObRawExpr* select_expr = NULL;
  if (OB_ISNULL(column) || OB_ISNULL(stmt) || OB_ISNULL(stmt = stmt->get_real_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(column), K(stmt));
  } else {
    const int64_t offset = column->get_column_id() - OB_APP_MIN_COLUMN_ID;
    if (OB_UNLIKELY(offset < 0 || offset >= stmt->get_select_item_size()) ||
        OB_ISNULL(select_expr = stmt->get_select_item(offset).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected select expr", K(ret), K(offset), K(stmt->get_select_item_size()), K(select_expr));
    } else if (OB_UNLIKELY(!select_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret), K(*select_expr));
    } else {
      const ObColumnRefRawExpr* inner_column = static_cast<const ObColumnRefRawExpr*>(select_expr);
      const TableItem* table_item = stmt->get_table_item_by_id(inner_column->get_table_id());
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (table_item->is_generated_table() &&
                 OB_FAIL(recursive_get_column_expr(inner_column, *table_item))) {
        LOG_WARN("faield to recursive get column expr", K(ret));
      } else {
        column = inner_column;
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::add_column_conv_infos(ObTableModify& phy_op, const ObIArray<ObColumnRefRawExpr*>& columns,
    const ObIArray<ObRawExpr*>& column_convert_exprs)
{
  int ret = OB_SUCCESS;
  ObExprResType res_type;
  if (OB_UNLIKELY(column_convert_exprs.count() != columns.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column convert exprs count don't match", K(column_convert_exprs.count()), K(columns.count()));
  } else if (OB_FAIL(phy_op.init_column_conv_info_count(columns.count()))) {
    LOG_WARN("fail to init column conv infos count", K(ret));
  }
  common::ObIAllocator& allocator = phy_plan_->get_allocator();
  ARRAY_FOREACH(columns, i)
  {
    const ObColumnRefRawExpr* column = columns.at(i);
    const ObRawExpr* column_convert_expr = column_convert_exprs.at(i);
    if (OB_ISNULL(column) || OB_ISNULL(column_convert_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column expr", K(ret), K(i), K(column), K(column_convert_expr));
    } else if (OB_LIKELY(column_convert_expr->get_expr_type() != T_FUN_COLUMN_CONV)) {
      if (OB_FAIL(phy_op.add_column_conv_info(res_type, 0, allocator))) {
        LOG_WARN("failed to add column conv info", K(ret));
      }
    } else {
      ObSysFunRawExpr* expr = static_cast<ObSysFunRawExpr*>(const_cast<ObRawExpr*>(column_convert_expr));
      ObString column_info;
      if (column_convert_expr->get_param_count() >= ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO) {
        const ObRawExpr* ci_expr = column_convert_expr->get_param_expr(5);
        CK(NULL != ci_expr);
        CK(ci_expr->is_const_expr());
        OZ(static_cast<const ObConstRawExpr*>(ci_expr)->get_value().get_string(column_info));
      }
      ObExprOperator* op = expr->get_op();
      CK(NULL != op);
      OZ(phy_op.add_column_conv_info(column->get_result_type(),
          column->get_column_flags(),
          allocator,
          &static_cast<ObExprColumnConv*>(op)->get_str_values(),
          &column_info));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_common_parts(const ObLogicalOperator& op, RowDesc& out_row_desc, ObPhyOperator& phy_op)
{
  int ret = OB_SUCCESS;
  bool is_returning = false;
  bool is_table_insert = false;  // non-pdml insert
  if (op.is_dml_operator()) {
    const ObLogDelUpd& log_op = static_cast<const ObLogDelUpd&>(op);
    is_returning = log_op.is_returning();
    is_table_insert =
        (log_op_def::LOG_INSERT == op.get_type() || log_op_def::LOG_INSERT_ALL == op.get_type()) && !log_op.is_pdml();
  }
  // True cardinality if it is not 0
  if (0 == phy_op.get_rows()) {
    OX(phy_op.set_rows(static_cast<int64_t>(ceil(op.get_card()))));
  }
  OX(phy_op.set_cost(static_cast<int64_t>(op.get_cost())));
  OX(phy_op.set_width(static_cast<int64_t>(op.get_width())));
  OX(phy_op.set_plan_depth(static_cast<int64_t>(op.get_plan_depth())));
  OX(phy_op.set_px_est_size_factor(op.get_px_est_size_factor()));
  // Special treatment for table scan, we must decide which filter is to be evaluated before index back
  OZ(add_filter(op.get_filter_exprs(), out_row_desc, phy_op, false), op.get_name(), out_row_desc);
  OZ(add_filter(op.get_startup_exprs(), out_row_desc, phy_op, true), op.get_name(), out_row_desc);
  if (is_table_insert && !is_returning) {
    // (non-pdml) insert operator hack the output exprs fileds
    // let convert_insert handle its output
  } else {
    OZ(add_compute(op.get_output_exprs(), out_row_desc, NULL, phy_op), op.get_name(), out_row_desc);
    OZ(add_projector(op.get_output_exprs(), out_row_desc, phy_op), op.get_name(), out_row_desc);
    if (PHY_VALUES != phy_op.get_type() && !is_table_insert) {
      // this is the size of the ObObj array, not the size of the projector
      OX(phy_op.set_column_count(out_row_desc.get_column_num()));
    }
  }

#ifndef NDEBUG
  if (OB_SUCC(ret)) {
    const int32_t* projector = phy_op.get_projector();
    int64_t projector_size = phy_op.get_projector_size();
    int64_t column_count = phy_op.get_column_count();
    LOG_INFO("add projector for op",
        "name",
        op.get_name(),
        "col_count",
        column_count,
        "projector",
        ObArrayWrap<int32_t>(projector, projector_size));
  }
#endif

  return ret;
}

int ObCodeGeneratorImpl::copy_row_desc_by_projector(
    const RowDesc& input_row_desc, const int32_t* projector, int64_t projector_size, RowDesc& out_row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == projector || projector_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid projector", K(projector), K(projector_size));
  } else {
    ObRawExpr* raw_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < projector_size; ++i) {
      if (OB_FAIL(input_row_desc.get_column(projector[i], raw_expr))) {
        LOG_ERROR("invalid index", K(input_row_desc), K(projector[i]), K(projector_size), K(ret));
      } else if (OB_FAIL(out_row_desc.add_column(raw_expr))) {
        LOG_WARN("failed to add tid cid", K(ret), K(out_row_desc));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::copy_row_desc(const RowDesc& input_row_desc, RowDesc& out_row_desc)
{
  int ret = OB_SUCCESS;
  ObRawExpr* raw_expr = NULL;
  for (int64_t i = 0, N = input_row_desc.get_column_num(); OB_SUCC(ret) && i < N; ++i) {
    if (OB_FAIL(input_row_desc.get_column(i, raw_expr))) {
      LOG_ERROR("invalid index", K(input_row_desc), K(ret));
    } else if (OB_FAIL(out_row_desc.add_column(raw_expr))) {
      LOG_WARN("failed to add tid cid", K(ret), K(out_row_desc));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_limit(ObLogLimit& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObLimit* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc* input_row_desc = NULL;
  if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (FALSE_IT(input_row_desc = child_ops.at(0).second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(create_phy_op_desc(PHY_LIMIT, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(input_row_desc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(input_row_desc), K(phy_op));
  } else if (OB_FAIL(copy_row_desc(*input_row_desc, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else {
    bool need_calc = false;
    ObRawExpr* limit_raw_expr = NULL;
    ObRawExpr* offset_raw_expr = NULL;
    ObRawExpr* percent_raw_expr = NULL;
    ObSqlExpression* limit_expr = NULL;
    ObSqlExpression* offset_expr = NULL;
    ObSqlExpression* percent_expr = NULL;
    phy_op->set_calc_found_rows(op.get_is_calc_found_rows());
    phy_op->set_is_top_limit(op.is_top_limit());
    phy_op->set_is_fetch_with_ties(op.is_fetch_with_ties());
    limit_raw_expr = op.get_limit_count();
    offset_raw_expr = op.get_limit_offset();
    percent_raw_expr = op.get_limit_percent();
    if (NULL != limit_raw_expr) {
      if (OB_FAIL(generate_sql_expr(
              phy_op->get_sql_expression_factory(), limit_raw_expr, *child_ops.at(0).second, limit_expr))) {
        LOG_WARN("Generation of sql expression fails", K(ret));
      }
    }

    if (OB_SUCC(ret) && NULL != offset_raw_expr) {
      if (OB_FAIL(generate_sql_expr(
              phy_op->get_sql_expression_factory(), offset_raw_expr, *child_ops.at(0).second, offset_expr))) {
        LOG_WARN("Generation of sql expression fails", K(ret));
      }
    }

    if (OB_SUCC(ret) && NULL != percent_raw_expr) {
      if (OB_FAIL(generate_sql_expr(
              phy_op->get_sql_expression_factory(), percent_raw_expr, *child_ops.at(0).second, percent_expr))) {
        LOG_WARN("Generation of sql expression fails", K(ret));
      }
    }
    if (OB_SUCC(ret) && op.is_fetch_with_ties()) {
      if (OB_FAIL(set_sort_columns_for_fetch(phy_op, input_row_desc, op.get_expected_ordering()))) {
        LOG_WARN("failed to set limit sort columns for fetch", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_op->set_limit(limit_expr, offset_expr, percent_expr))) {
        LOG_WARN("failed to set limit", K(ret));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::set_sort_columns_for_fetch(
    ObLimit* phy_op, RowDesc* input_row_desc, ObIArray<OrderItem>& expected_ordering)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_op) || OB_ISNULL(input_row_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected null", K(ret), K(phy_op), K(input_row_desc));
  } else if (OB_FAIL(phy_op->init_sort_columns(expected_ordering.count()))) {
    LOG_WARN("failed to init sort columns", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_ordering.count(); ++i) {
      ObRawExpr* order_expr = expected_ordering.at(i).expr_;
      int64_t idx = OB_INVALID_INDEX;
      if (OB_ISNULL(order_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(input_row_desc->get_idx(order_expr, idx)) && OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get order by expr index", K(ret));
      } else if (OB_ENTRY_NOT_EXIST == ret || idx == OB_INVALID_INDEX) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("order by expr not in child output exprs", K(ret));
      } else {
        ObSortColumn column;
        column.index_ = idx;
        column.set_is_ascending(expected_ordering.at(i).is_ascending());
        column.cs_type_ = order_expr->get_collation_type();
        if (OB_FAIL(phy_op->add_sort_columns(column))) {
          LOG_WARN("failed to add sort columns", K(ret));
        } else { /*do nothing */
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_window_function(
    ObLogWindowFunction& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObWindowFunction* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc* input_row_desc = NULL;
  if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (FALSE_IT(input_row_desc = child_ops.at(0).second)) {
  } else if (OB_FAIL(create_phy_op_desc(PHY_WINDOW_FUNCTION, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(input_row_desc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(input_row_desc), K(phy_op));
  } else if (OB_FAIL(copy_row_desc_by_projector(*child_ops.at(0).second,
                 child_ops.at(0).first->get_projector(),
                 child_ops.at(0).first->get_projector_size(),
                 *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else if (OB_FAIL(phy_op->init(op.get_window_exprs().count()))) {
    LOG_WARN("failed to init the window function.", K(ret));
  } else {
    phy_op->set_parallel(op.is_parallel());
    ColumnIndexProviderImpl idx_provider(
        *child_ops.at(0).second, child_ops.at(0).first->get_projector(), child_ops.at(0).first->get_projector_size());
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_window_exprs().count(); ++i) {
      ObWinFunRawExpr* win_expr = op.get_window_exprs().at(i);
      if (OB_ISNULL(win_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Get unexpected null", K(ret));
      } else if (OB_FAIL(add_window_function_info(idx_provider, phy_op, win_expr, *input_row_desc, *out_row_desc))) {
        LOG_WARN("failed to generate window function info.", K(ret));
      } else { /* do nothing. */
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_group_by(ObLogGroupBy& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;

  ObPhyOperatorType phy_op_type = PHY_INVALID;
  switch (op.get_algo()) {
    case MERGE_AGGREGATE:
      phy_op_type = PHY_MERGE_GROUP_BY;
      break;
    case HASH_AGGREGATE:
      phy_op_type = PHY_HASH_GROUP_BY;
      break;
    case SCALAR_AGGREGATE:
      phy_op_type = PHY_SCALAR_AGGREGATE;
      break;
    default:
      break;
  }

  ObGroupBy* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  common::ObSEArray<ObRawExpr*, 4> group_exprs;
  common::ObSEArray<ObRawExpr*, 4> rollup_exprs;
  if (OB_FAIL(append(group_exprs, op.get_group_by_exprs()))) {
    LOG_WARN("failed to append group by exprs.", K(ret));
  } else if (OB_FAIL(append(rollup_exprs, op.get_rollup_exprs()))) {
    LOG_WARN("failed to append rollup exprs.", K(ret));
  } else { /*do nothing.*/
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(1 != child_ops.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
    } else if (OB_FAIL(create_phy_op_desc(phy_op_type, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
      LOG_WARN("failed to create phy op and desc", K(ret));
    } else if (OB_FAIL(copy_row_desc_by_projector(*child_ops.at(0).second,
                   child_ops.at(0).first->get_projector(),
                   child_ops.at(0).first->get_projector_size(),
                   *out_row_desc))) {
      LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
    } else if (OB_FAIL(phy_op->init(group_exprs.count()))) {
      OB_LOG(WARN, "fail to init group expr", K(ret));
    } else if (OB_FAIL(phy_op->init_rollup(rollup_exprs.count()))) {
      OB_LOG(WARN, "fail to init rollup expr", K(ret));
    } else { /*do nothing.*/
    }
  }

  if (OB_SUCC(ret)) {
    phy_op->set_est_group_cnt(op.get_distinct_card());
    // 1. add group columns and rollup columns
    ColumnIndexProviderImpl idx_provider(
        *child_ops.at(0).second, child_ops.at(0).first->get_projector(), child_ops.at(0).first->get_projector_size());
    int64_t cell_idx = OB_INVALID_INDEX;
    ARRAY_FOREACH(group_exprs, i)
    {
      const ObRawExpr* raw_expr = group_exprs.at(i);
      if (!raw_expr->has_flag(IS_COLUMNLIZED)) {
        if (raw_expr->has_flag(IS_CONST) || raw_expr->has_flag(IS_CONST_EXPR)) {
          continue;  // group by const value, just ignore
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("group by column should have be columnlized", K(*raw_expr));
        }
      } else if (OB_FAIL(idx_provider.get_idx(raw_expr, cell_idx))) {
        LOG_ERROR("group by column should have be columnlized", K(ret), K(*raw_expr));
      } else if (OB_FAIL(phy_op->add_group_column_idx(cell_idx, raw_expr->get_collation_type()))) {
        LOG_WARN("failed to add group by column", K(ret), K(cell_idx));
      }
    }  // end for
    ARRAY_FOREACH(rollup_exprs, i)
    {
      const ObRawExpr* raw_expr = rollup_exprs.at(i);
      if (!raw_expr->has_flag(IS_COLUMNLIZED)) {
        if (raw_expr->has_flag(IS_CONST) || raw_expr->has_flag(IS_CONST_EXPR)) {
          if (OB_FAIL(phy_op->add_rollup_column_idx(OB_INVALID_INDEX, raw_expr->get_collation_type()))) {
            LOG_WARN("failed to add rollup column", K(ret), K(cell_idx));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("group by column should have be columnlized", K(*raw_expr));
        }
      } else if (OB_FAIL(idx_provider.get_idx(raw_expr, cell_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("group by column should have be columnlized", K(*raw_expr));
      } else if (OB_FAIL(phy_op->add_rollup_column_idx(cell_idx, raw_expr->get_collation_type()))) {
        LOG_WARN("failed to add group by column", K(ret), K(cell_idx));
      }
    }  // end for
    // 2. add aggr columns
    int64_t out_idx = out_row_desc->get_column_num();
    const ObIArray<ObRawExpr*>& aggr_exprs = op.get_aggr_funcs();
    ARRAY_FOREACH(aggr_exprs, i)
    {
      ObRawExpr* raw_expr = aggr_exprs.at(i);
      ObAggregateExpression* expr = NULL;
      if (OB_UNLIKELY(!raw_expr->has_flag(IS_AGG))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expected aggr function");
      } else if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), expr))) {
        LOG_WARN("failed to create aggregate expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("alloc invalid expr", K(ret), K(expr));
      } else if (OB_FAIL(generate_expr(*raw_expr,
                     *expr,
                     *child_ops.at(0).second,
                     child_ops.at(0).first->get_projector(),
                     child_ops.at(0).first->get_projector_size()))) {
        LOG_WARN("failed to generate expr", K(ret), K(raw_expr));
      } else if (OB_FAIL(phy_op->add_aggr_column(expr))) {
        LOG_WARN("failed to add sql expr", K(ret), K(expr));
      } else if (OB_FAIL(out_row_desc->add_column(raw_expr))) {  // map expr to idx
        LOG_WARN("failed to add column desc", K(ret));
      } else if (OB_FAIL(raw_expr->add_flag(IS_COLUMNLIZED))) {
        LOG_WARN("failed to add flag IS_COLUMNLIZED", K(ret));
      } else {
        expr->set_result_index(out_idx++);
      }
      // add udf meta
      if (OB_SUCC(ret)) {
        if (raw_expr->get_expr_type() == T_FUN_AGG_UDF) {
          ObAggFunRawExpr* agg_expr = static_cast<ObAggFunRawExpr*>(raw_expr);
          ObSEArray<ObString, 16> udf_attributes;
          ObSEArray<ObExprResType, 16> udf_attributes_types;
          ObSEArray<ObUdfConstArgs, 16> const_results; /* const input expr' param idx */
          ObAggUdfMeta agg_udf_meta;
          agg_udf_meta.udf_meta_ = agg_expr->get_udf_meta();
          const common::ObIArray<ObRawExpr*>& param_exprs = agg_expr->get_real_param_exprs();
          ARRAY_FOREACH_X(param_exprs, idx, cnt, OB_SUCC(ret))
          {
            ObRawExpr* expr = param_exprs.at(idx);
            if (OB_ISNULL(expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("the expr is null", K(ret));
            } else if (OB_FAIL(udf_attributes.push_back(expr->get_expr_name()))) {
              LOG_WARN("failed to push back", K(ret));
            } else if (OB_FAIL(udf_attributes_types.push_back(expr->get_result_type()))) {
              LOG_WARN("failed to push back", K(ret));
            } else if (expr->has_flag(IS_CALCULABLE_EXPR) || expr->has_flag(IS_CONST_EXPR) ||
                       expr->has_flag(IS_CONST)) {
              // generate the sql expression
              ObObj tmp_res;
              ObNewRow empty_row;
              RowDesc row_desc;
              ObSqlExpression* sql_expr = NULL;
              ObExprGeneratorImpl expr_generator(phy_plan_->get_expr_op_factory(), 0, 0, NULL, row_desc);
              if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), sql_expr))) {
                LOG_WARN("failed to create topn expr", K(ret));
              } else if (OB_ISNULL(sql_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("topn_expr is null", K(ret));
              } else if (OB_FAIL(expr_generator.generate(*expr, *sql_expr))) {
                LOG_WARN("failed to generate topn expr", K(ret));
              } else {
                ObUdfConstArgs const_args;
                const_args.sql_calc_ = sql_expr;
                const_args.idx_in_udf_arg_ = idx;
                if (OB_FAIL(const_results.push_back(const_args))) {
                  LOG_WARN("failed to push back const args", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(agg_udf_meta.udf_attributes_.assign(udf_attributes))) {
              LOG_WARN("assign array failed", K(ret));
            } else if (OB_FAIL(agg_udf_meta.udf_attributes_types_.assign(udf_attributes_types))) {
              LOG_WARN("assign array failed", K(ret));
            } else if (OB_FAIL(agg_udf_meta.calculable_results_.assign(const_results))) {
              LOG_WARN("assign const result failed", K(ret));
            } else if (OB_FAIL(phy_op->add_udf_meta(agg_udf_meta))) {
              LOG_WARN("add udf meta to group by failed", K(ret));
            }
          }
        }
      }
    }
    // 3. add rollup information
    if (OB_SUCC(ret)) {
      phy_op->set_rollup(op.has_rollup());
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_link_scan(ObLogLink& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObLinkScan* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc* input_row_desc = NULL;
  const int32_t* input_projector = NULL;
  int64_t input_projector_size = 0;

  // although link phy op has no child, the link log op always has one child,
  // copy child's row_desc to out_row_desc.
  if (1 != child_ops.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(child_ops.count()));
  } else if (OB_ISNULL(input_row_desc = child_ops.at(0).second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input_row_desc is null", K(ret));
  } else if (FALSE_IT(input_projector = child_ops.at(0).first->get_projector()) ||
             FALSE_IT(input_projector_size = child_ops.at(0).first->get_projector_size())) {
    // nothing.
  } else if (OB_FAIL(create_phy_op_desc(PHY_LINK, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(phy_op) || OB_ISNULL(out_row_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_op or out_row_desc null", K(ret), KP(phy_op), KP(out_row_desc));
  } else if (OB_FAIL(
                 copy_row_desc_by_projector(*input_row_desc, input_projector, input_projector_size, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else if (OB_FAIL(op.gen_link_stmt_fmt())) {
    LOG_WARN("failed to generate link stmt", K(ret));
  } else if (OB_FAIL(phy_op->set_param_infos(op.get_param_infos()))) {
    LOG_WARN("failed to set param infos", K(ret));
  } else if (OB_FAIL(phy_op->set_stmt_fmt(op.get_stmt_fmt_buf(), op.get_stmt_fmt_len()))) {
    LOG_WARN("failed to set stmt fmt", K(ret));
  } else {
    phy_op->set_dblink_id(op.get_dblink_id());
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_sort(ObLogSort& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObSort* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc* input_row_desc = NULL;
  const int32_t* input_projector = NULL;
  int64_t input_projector_size = 0;
  // bool convert_runion_generate_sort = false;

  if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (FALSE_IT((input_row_desc = child_ops.at(0).second,
                 input_projector = child_ops.at(0).first->get_projector(),
                 input_projector_size = child_ops.at(0).first->get_projector_size()))) {
  } else if (OB_FAIL(create_phy_op_desc(PHY_SORT, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_FAIL(
                 copy_row_desc_by_projector(*input_row_desc, input_projector, input_projector_size, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else {
    ColumnIndexProviderImpl idx_provider(*child_ops.at(0).second, input_projector, input_projector_size);
    LOG_DEBUG("input projector of sort", "projector", ObArrayWrap<int32_t>(input_projector, input_projector_size));
    const ObIArray<OrderItem>& sort_keys = op.get_sort_keys();
    ret = fill_sort_columns(idx_provider, sort_keys, phy_op, *phy_op);
    // Temporarily disable this optimization
    // phy_op->set_use_compact(false);
    // 2. set topn
    if (OB_SUCC(ret) && NULL != op.get_topn_count()) {
      ObRawExpr* topn_raw_expr = op.get_topn_count();
      ObSqlExpression* topn_expr = NULL;
      if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), topn_expr))) {
        LOG_WARN("failed to create topn expr", K(ret));
      } else if (OB_ISNULL(topn_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("topn_expr is null", K(ret));
      } else if (OB_FAIL(generate_expr(*topn_raw_expr, *child_ops.at(0).second, *topn_expr))) {
        LOG_WARN("failed to generate topn expr", K(ret));
      } else {
        phy_op->set_topn_expr(topn_expr);
        phy_op->set_fetch_with_ties(op.is_fetch_with_ties());
      }
    }

    ObRawExpr* limit_raw_expr = op.get_topk_limit_count();
    if (OB_SUCC(ret) && NULL != limit_raw_expr) {
      ObSqlExpression* limit_expr = NULL;
      ObSqlExpression* offset_expr = NULL;
      ObRawExpr* offset_raw_expr = op.get_topk_offset_count();

      if (OB_FAIL(generate_sql_expr(
              phy_op->get_sql_expression_factory(), limit_raw_expr, *child_ops.at(0).second, limit_expr))) {
        LOG_WARN("Generation of sql expression fails", K(ret));
      }

      if (OB_SUCC(ret) && NULL != offset_raw_expr) {
        if (OB_FAIL(generate_sql_expr(
                phy_op->get_sql_expression_factory(), offset_raw_expr, *child_ops.at(0).second, offset_expr))) {
          LOG_WARN("Generation of sql expression fails", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(phy_op->set_topk_params(
                limit_expr, offset_expr, op.get_minimum_row_count(), op.get_topk_precision()))) {
          LOG_WARN("failed to set topk params", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // set prefix sort
      const int64_t prefix_cnt = op.get_prefix_pos();
      if (prefix_cnt > phy_op->get_sort_columns().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("prefix is larger than sort columns", K(ret));
      } else {
        if (prefix_cnt == phy_op->get_sort_columns().count()) {
          LOG_WARN("all sort columns are prefix, the sort operator should be removed.",
              K(prefix_cnt),
              K(phy_op->get_sort_columns().count()),
              KPC(op.get_plan()));
        }
        phy_op->set_prefix_pos(prefix_cnt);
      }
      phy_op->set_local_merge_sort(op.is_local_merge_sort());
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_temp_table(ObLogTempTableAccess& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  UNUSED(child_ops);
  RowDesc* out_row_desc = NULL;
  ObSEArray<ObRawExpr*, 4> access_exprs;
  ObTempTableAccess* phy_op = NULL;
  if (OB_ISNULL(op.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(create_phy_op_desc(PHY_TEMP_TABLE_ACCESS, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("create subquery scan op and desc failed", K(ret));
  } else if (OB_FAIL(op.get_stmt()->get_column_exprs(op.get_table_id(), access_exprs))) {
    LOG_WARN("failed to gnerate access exprs.", K(ret));
  } else if (OB_FAIL(phy_op->init_output_index(access_exprs.count()))) {
    LOG_WARN("failed to init output index.", K(ret));
  } else {
    ObTableLocationType loc_type = op.get_sharding_info().get_location_type();
    bool is_dist = loc_type == OB_TBL_LOCATION_DISTRIBUTED;
    uint64_t ref_table_id = op.get_ref_table_id();
    phy_plan_->set_use_temp_table(true);
    phy_op->set_distributed(is_dist);
    phy_op->set_temp_table_id(ref_table_id);
    phy_op->set_need_release(op.is_last_access());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); ++i) {
    if (OB_ISNULL(access_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected null expr.", K(ret));
    } else if (!access_exprs.at(i)->is_column_ref_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expected basic column expr", K(ret));
    } else {
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(access_exprs.at(i));
      int64_t index = col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
      if (OB_FAIL(phy_op->add_output_index(index))) {
        LOG_WARN("failed to add column", K(ret), K(i));
      } else if (OB_FAIL(out_row_desc->add_column(col_expr))) {
        LOG_WARN("failed to add column desc", K(ret), K(i));
      } else if (OB_FAIL(col_expr->add_flag(IS_COLUMNLIZED))) {
        LOG_WARN("failed to add flag IS_COLUMNLIZED", K(ret));
      } else { /*do nothing.*/
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_temp_table_insert(
    ObLogTempTableInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  RowDesc* out_row_desc = NULL;
  ObTempTableInsert* phy_op = NULL;
  if (1 != child_ops.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child ops count.", K(ret));
  } else {
    RowDesc* input_row_desc = child_ops.at(0).second;
    const int32_t* input_projector = child_ops.at(0).first->get_projector();
    int64_t input_projector_size = child_ops.at(0).first->get_projector_size();
    if (OB_FAIL(create_phy_op_desc(PHY_TEMP_TABLE_INSERT, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
      LOG_WARN("failed to create phy op and desc", K(ret));
    } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(phy_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(phy_op));
    } else if (OB_FAIL(
                   copy_row_desc_by_projector(*input_row_desc, input_projector, input_projector_size, *out_row_desc))) {
      LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
    } else {
      ObTableLocationType loc_type = op.get_sharding_info().get_location_type();
      bool is_dist = loc_type == OB_TBL_LOCATION_DISTRIBUTED;
      uint64_t ref_table_id = op.get_ref_table_id();
      phy_op->set_distributed(is_dist);
      phy_op->set_temp_table_id(ref_table_id);
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_temp_table_transformation(
    ObLogTempTableTransformation& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  RowDesc* out_row_desc = NULL;
  ObTempTableTransformation* phy_op = NULL;
  int64_t last_child_id = op.get_num_of_child() - 1;
  RowDesc* input_row_desc = child_ops.at(last_child_id).second;
  const int32_t* input_projector = child_ops.at(last_child_id).first->get_projector();
  int64_t input_projector_size = child_ops.at(last_child_id).first->get_projector_size();
  if (OB_FAIL(create_phy_op_desc(PHY_TEMP_TABLE_TRANSFORMATION, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(phy_op));
  } else if (OB_FAIL(
                 copy_row_desc_by_projector(*input_row_desc, input_projector, input_projector_size, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_table_scan(
    ObLogTableScan& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops, bool is_multi_partition_scan)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* schema_guard = NULL;
  CK(OB_NOT_NULL(op.get_plan()));
  CK(OB_NOT_NULL(schema_guard = op.get_plan()->get_optimizer_context().get_sql_schema_guard()));
  uint64_t ref_table_id = op.get_ref_table_id();
  if (OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ref_table_id", K(ret), K(ref_table_id));
  } else if (op.get_is_fake_cte_table()) {
    if (OB_FAIL(convert_cte_pump(op, child_ops, out_ops))) {
      LOG_WARN("convert table scan with index back failed", K(ret));
    }
  } else if (OB_FAIL(convert_normal_table_scan(op, child_ops, out_ops, is_multi_partition_scan))) {
    LOG_WARN("fail to convert normal table scan", K(ret), K(child_ops), K(out_ops));
  }
  if (OB_SUCC(ret) && out_ops.at(out_ops.count() - 1).first != NULL && !op.get_is_fake_cte_table()) {
    ObTableScan* table_scan = dynamic_cast<ObTableScan*>(out_ops.at(out_ops.count() - 1).first);
    if (table_scan != NULL) {
      int64_t schema_version = -1;
      OZ(table_scan->set_filter_before_indexback_flags(op.get_filter_before_index_flags()));
      OZ(schema_guard->get_table_schema_version(table_scan->get_ref_table_id(), schema_version),
          table_scan->get_ref_table_id());
      if (OB_SUCC(ret)) {
        table_scan->set_schema_version(schema_version);
      }
    }
  }
  if (0 != op.get_session_id()) {
    phy_plan_->set_session_id(op.get_session_id());  // for temp table scan
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_normal_table_scan(
    ObLogTableScan& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops, bool is_multi_partition_scan)
{
  int ret = OB_SUCCESS;
  ObTableScan* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  bool is_top = false;
  bool is_get = false;
  ObString t_name;
  ObString index_name;
  ObPhyOperatorType op_type = PHY_INVALID;
  if (op.get_type() == log_op_def::LOG_MV_TABLE_SCAN) {
    op_type = PHY_MV_TABLE_SCAN;
  } else if (op.is_sample_scan()) {
    if (op.get_sample_info().method_ == SampleInfo::ROW_SAMPLE) {
      op_type = PHY_ROW_SAMPLE_SCAN;
    } else if (op.get_sample_info().method_ == SampleInfo::BLOCK_SAMPLE) {
      op_type = PHY_BLOCK_SAMPLE_SCAN;
    } else { /*do nothing*/
    }
  } else if (is_multi_partition_scan) {
    op_type = PHY_MULTI_PART_TABLE_SCAN;
  } else {
    op_type = PHY_TABLE_SCAN;
  }
  if (0 != child_ops.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(child_ops.count()));
  } else if (OB_FAIL(create_phy_op_desc(op_type, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_FAIL(ob_write_string(phy_plan_->get_allocator(), op.get_table_name(), t_name))) {
    LOG_WARN("write table name to phy table scan operator failed", K(ret), K(op));
  } else if (OB_FAIL(ob_write_string(phy_plan_->get_allocator(), op.get_index_name(), index_name))) {
    LOG_WARN("write index name to phy table scan operator failed", K(ret), K(op));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(phy_op));
  } else if (NULL != op.get_pre_query_range() && OB_FAIL(phy_op->set_query_range(*op.get_pre_query_range()))) {
    LOG_WARN("fail deep copy query range", K(ret));
  } else if (OB_FAIL(op.is_top_table_scan(is_top))) {
    LOG_WARN("is_top_table_scan fails unexpectedly", K(ret));
  } else if (OB_FAIL(set_optimization_info(op, phy_op))) {
    LOG_WARN("set optimization info failed", K(ret));
  } else if (OB_FAIL(set_partition_range_info(op, phy_op))) {
    LOG_WARN("set partition range info failed", K(ret));
  } else if (NULL != op.get_pre_query_range() && OB_FAIL(op.get_pre_query_range()->is_get(is_get))) {
    LOG_WARN("extract pre query range get info failed", K(ret));
  } else {
    phy_op->set_is_get(is_get);
    phy_op->set_is_top_table_scan(is_top);
    phy_op->set_table_location_key(op.get_table_id());
    phy_op->set_ref_table_id(op.get_ref_table_id());
    phy_op->set_table_name(t_name);
    phy_op->set_index_name(index_name);
    phy_op->set_is_index_global(op.get_is_index_global());
    phy_op->set_for_update(op.is_for_update(), op.get_for_update_wait_us());
    phy_plan_->set_for_update(op.is_for_update());  // single select can't go READONLY TRANS opti.
    phy_op->set_index_table_id(op.get_index_table_id());
    phy_op->set_table_row_count(op.get_table_row_count());
    phy_op->set_output_row_count(static_cast<int64_t>(op.get_output_row_count()));
    phy_op->set_query_range_row_count(static_cast<int64_t>(op.get_query_range_row_count()));
    phy_op->set_index_back_row_count(static_cast<int64_t>(op.get_index_back_row_count()));
    phy_op->set_estimate_method(op.get_estimate_method());
    phy_op->set_gi_above(op.is_gi_above());
    if (op.is_table_whole_range_scan()) {
      phy_plan_->set_contain_table_scan(true);
    }
    ObSqlExpression* limit_expr = NULL;
    ObSqlExpression* offset_expr = NULL;
    if (NULL != op.get_limit_expr()) {
      if (OB_FAIL(generate_sql_expr(
              phy_op->get_sql_expression_factory(), op.get_limit_expr(), *out_row_desc, limit_expr))) {
        LOG_WARN("Generation of sql expression fails", K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != op.get_offset_expr()) {
      if (OB_FAIL(generate_sql_expr(
              phy_op->get_sql_expression_factory(), op.get_offset_expr(), *out_row_desc, offset_expr))) {
        LOG_WARN("Generation of sql expression fails", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_op->set_limit_offset(limit_expr, offset_expr))) {
        LOG_WARN("Setting for limit and offset in ObTableScan fails", K(ret));
      } else if (NULL != op.get_pre_query_range()) {
        LOG_DEBUG("logical op range", "pre_range", *op.get_pre_query_range());
      } else { /* Do nothing */
      }
    }

    if (OB_SUCC(ret)) {
      ObQueryFlag query_flag;
      if (op.is_whole_range_scan()) {
        phy_op->set_whole_range_scan(true);
      }
      if (op.is_need_feedback() && (op.get_plan()->get_location_type() != OB_PHY_PLAN_UNCERTAIN) &&
          (op.get_plan()->get_phy_plan_type() == OB_PHY_PLAN_LOCAL ||
              op.get_plan()->get_phy_plan_type() == OB_PHY_PLAN_REMOTE)) {
        ++(phy_plan_->get_access_table_num());
        query_flag.is_need_feedback_ = true;
      }
      ObOrderDirection scan_direction = op.get_scan_direction();
      query_flag.index_back_ = op.get_index_back() ? 1 : 0;
      if (is_descending_direction(scan_direction)) {
        query_flag.scan_order_ = ObQueryFlag::Reverse;
      } else {
        query_flag.scan_order_ = ObQueryFlag::Forward;
      }
      phy_op->set_flags(query_flag.flag_);
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObRawExpr*>& access_exprs = op.get_access_exprs();
    LOG_DEBUG("table scan's access columns", K(access_exprs.count()));
    if (OB_FAIL(phy_op->init_output_column(access_exprs.count()))) {
      LOG_WARN("fail to init output column", K(ret));
    }
    // 1. add basic column
    ObArray<uint64_t> output_column_ids;
    ARRAY_FOREACH(access_exprs, i)
    {
      ObRawExpr* expr = access_exprs.at(i);
      uint64_t column_id = 0;
      if (OB_UNLIKELY(OB_ISNULL(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
        column_id = common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID;
        phy_op->set_need_scn(true);
        LOG_DEBUG("need row scn");
      } else if (expr->is_column_ref_expr()) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
        if (!col_expr->has_flag(IS_COLUMN) || col_expr->get_table_id() != op.get_table_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Expected basic column",
              K(col_expr),
              K(col_expr->has_flag(IS_COLUMN)),
              K(col_expr->get_table_id()),
              K(op.get_table_id()));
        } else {
          column_id = col_expr->get_column_id();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr", K(ret), K(*expr));
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(phy_op->add_output_column(column_id))) {
          LOG_WARN("failed to add column", K(ret), K(i));
        } else if (OB_FAIL(output_column_ids.push_back(column_id))) {
          LOG_WARN("failed to add column", K(ret), K(i));
        } else if (OB_FAIL(out_row_desc->add_column(expr))) {
          LOG_WARN("failed to add column desc", K(ret), K(i));
        } else if (OB_FAIL(expr->add_flag(IS_COLUMNLIZED))) {
          LOG_WARN("failed to add flag IS_COLUMNLIZED", K(ret));
        }
      }
    }  // end for

    ObSqlSchemaGuard* schema_guard = NULL;
    const ObTableSchema* table_schema = NULL;
    const ObTableSchema* index_schema = NULL;
    ObSQLSessionInfo* session = NULL;

    if (OB_SUCC(ret)) {
      const uint64_t table_id = op.get_ref_table_id();
      const uint64_t index_id = op.get_index_table_id();
      const ObLogPlan* log_plan = op.get_plan();
      if (OB_INVALID_ID == table_id || OB_INVALID_ID == index_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid id", K(table_id), K(index_id), K(ret));
      } else if (OB_ISNULL(log_plan) || OB_ISNULL(op.get_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("NULL ptr", K(ret), K(log_plan), K(op.get_stmt()));
      } else if (OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard()) ||
                 OB_ISNULL(schema_guard->get_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("NULL ptr", K(ret));
      } else if (OB_ISNULL(session = log_plan->get_optimizer_context().get_session_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session info is null");
      } else if (OB_FAIL(schema_guard->get_table_schema(table_id, table_schema))) {
        LOG_WARN("get table schema failed", K(table_id), K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
        LOG_WARN("get table schema failed", K(index_id), K(ret));
      } else if (OB_ISNULL(table_schema) || OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("NULL ptr", K(ret), K(table_schema), K(index_schema));
      }
    }

    // 2. add virtual column
    ARRAY_FOREACH(access_exprs, i)
    {
      ObRawExpr* expr = access_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
        /*do nothing*/
      } else if (expr->is_column_ref_expr()) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
        bool is_new_no_pk_part_gen_col = false;
        if (col_expr->is_generated_column()) {
          const ObColumnSchemaV2* col_schema = table_schema->get_column_schema(col_expr->get_column_id());
          CK(OB_NOT_NULL(col_schema));
          OX(is_new_no_pk_part_gen_col = col_schema->is_part_key_column() && table_schema->is_new_no_pk_table());
        }

        // situation that need to generate dep expr:
        // 1. col_expr is rowid pseudo column.
        // 2. col expr is generated column and no index is used.
        // 3. col expr is generated column and need index back.
        //
        // exception1: if use rowid index, still need to generate dep expr,
        //             because rowid index is mocked index, still need main table.
        // exception2: in heap table, if col_expr is part key, no need to generate dep expr,
        //             because this column will be hidden primary key.
        if (OB_FAIL(ret)) {
        } else if ((col_expr->is_generated_column() && !is_new_no_pk_part_gen_col &&
                       (!op.is_index_scan() || op.get_index_back() ||
                           ObSQLMockSchemaUtils::is_mock_index(op.get_index_table_id()))) ||
                   (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == col_expr->get_column_id())) {
          if (OB_ISNULL(col_expr->get_dependant_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("Unexpected NULL pointer", K(col_expr->get_dependant_expr()), K(ret));
          } else {
            ObColumnExpression* dependant_expr = NULL;
            if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), dependant_expr))) {
              LOG_WARN("failed to create expr", K(ret));
            } else if (OB_ISNULL(dependant_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("alloc invalid expr", K(dependant_expr), K(ret));
            } else if (OB_FAIL(generate_expr(*col_expr->get_dependant_expr(), *out_row_desc, *dependant_expr))) {
              LOG_WARN("failed to generate expr", K(*col_expr->get_dependant_expr()), K(*out_row_desc), K(ret));
            } else if (OB_FAIL(phy_op->add_virtual_column_expr(dependant_expr))) {
              LOG_WARN("failed to add virtual column expr", K(dependant_expr), K(ret));
            } else {
              dependant_expr->set_result_index(i);
              LOG_TRACE("rowid hidden column expr", K(*dependant_expr), K(*col_expr->get_dependant_expr()));
            }
          }
        } else {
          LOG_DEBUG(
              "no need to cg dep expr for generated expr", K(*col_expr), K(op.is_index_scan()), K(op.get_index_back()));
        }
      }
    }  // end for

    if (OB_SUCC(ret)) {
      if (OB_SUCC(ret) && table_schema->need_part_filter()) {
        // partition split does not support: hidden pk, inner table
        uint64_t filter_table_id = (op.is_index_scan()) ? op.get_index_table_id() : op.get_ref_table_id();
        bool is_get = false;
        if (OB_FAIL(op.is_table_get(is_get))) {
          LOG_WARN("check is table get failed", K(ret));
        } else if (!is_get) {
          ObTableLocation part_filter;
          ObDMLStmt *root_stmt = NULL;
          bool is_dml_table = false;
          if (OB_ISNULL(root_stmt = op.get_plan()->get_optimizer_context().get_root_stmt())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("root stmt is invalid", K(ret), K(root_stmt));
          } else if (FALSE_IT(is_dml_table = root_stmt->check_table_be_modified(filter_table_id))) {
            // do nothing
          } else if (OB_FAIL(part_filter.init_table_location_with_rowkey(*schema_guard,
                                                                         filter_table_id,
                                                                         *session,
                                                                         is_dml_table))) {
            LOG_WARN("init table location with rowkey failed", K(ret), K(filter_table_id));
          } else if (OB_FAIL(phy_op->set_part_filter(part_filter))) {
            LOG_WARN("set part filter failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (share::is_oracle_mapping_real_virtual_table(op.get_ref_table_id())) {
      } else if (OB_FAIL(phy_op->get_table_param().convert(
                     *table_schema, *index_schema, output_column_ids, op.get_index_back()))) {
        LOG_WARN("convert schema failed",
            K(ret),
            K(*table_schema),
            K(*index_schema),
            K(output_column_ids),
            K(op.get_index_back()));
      }
    }
    if (OB_SUCC(ret) && op.get_type() == log_op_def::LOG_MV_TABLE_SCAN) {
      const ObTableSchema* right_schema = NULL;
      const ObIArray<uint64_t>& depend_tids = index_schema->get_depend_table_ids();
      if (depend_tids.count() != 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("depend_tid count is not 1", K(depend_tids.count()));
      } else {
        uint64_t depend_tid = depend_tids.at(0);
        if (OB_FAIL(schema_guard->get_table_schema(depend_tid, right_schema))) {
          LOG_WARN("fail to get depend table id", K(ret), K(depend_tid));
        }
      }
      if (OB_SUCC(ret)) {
        ObMVTableScan* mv_table_scan = static_cast<ObMVTableScan*>(phy_op);
        if (OB_FAIL(mv_table_scan->get_right_table_param().convert_join_mv_rparam(
                *index_schema, *right_schema, output_column_ids))) {
          LOG_WARN("fail to do convert_join_mv_rparam", K(ret));
        } else {
          mv_table_scan->set_right_table_location_key(right_schema->get_table_id());
        }
      }
    }

    if (OB_SUCC(ret) && op.is_sample_scan() && op.get_sample_info().method_ == SampleInfo::ROW_SAMPLE) {
      ObRowSampleScan* sample_scan = static_cast<ObRowSampleScan*>(phy_op);
      sample_scan->set_sample_info(op.get_sample_info());
    }

    if (OB_SUCC(ret) && op.is_sample_scan() && op.get_sample_info().method_ == SampleInfo::BLOCK_SAMPLE) {
      ObBlockSampleScan* sample_scan = static_cast<ObBlockSampleScan*>(phy_op);
      sample_scan->set_sample_info(op.get_sample_info());
    }
  }

  if (OB_SUCC(ret)) {
    if (op.exist_hint()) {
      phy_op->set_exist_hint(true);
      phy_op->set_hint(op.get_const_hint());
    }
    if (OB_FAIL(convert_real_virtual_table(op, *phy_op))) {
      LOG_WARN("failed to convert real virtual table", K(ret));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_table_param(
    ObLogTableScan& op, ObTableScan& phy_op, const uint64_t table_id, const uint64_t index_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  const ObTableSchema* index_schema = NULL;
  const ObLogPlan* log_plan = op.get_plan();
  ObSqlSchemaGuard* schema_guard =
      NULL == op.get_plan() ? NULL : op.get_plan()->get_optimizer_context().get_sql_schema_guard();
  CK(OB_NOT_NULL(schema_guard));
  if (OB_INVALID_ID == table_id || OB_INVALID_ID == index_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id", K(table_id), K(index_id), K(ret));
  } else if (OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(log_plan));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(table_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("get table schema failed", K(index_id), K(ret));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), KP(table_schema), KP(index_schema));
  } else if (0 == phy_op.get_output_column_ids().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output column ids is empty", K(ret));
  } else if (OB_FAIL(phy_op.get_table_param().convert(
                 *table_schema, *index_schema, phy_op.get_output_column_ids(), op.get_index_back()))) {
    LOG_WARN("convert schema failed",
        K(ret),
        K(*table_schema),
        K(*index_schema),
        K(phy_op.get_output_column_ids()),
        K(op.get_index_back()));
  } else {
    phy_op.set_real_schema_version(index_schema->get_schema_version());
    if (UINT64_MAX != phy_op.get_vt_table_id() &&
        share::is_oracle_mapping_real_virtual_table(phy_op.get_vt_table_id())) {
      const ObTableSchema* vt_table_schema = NULL;
      VTMapping* vt_mapping = nullptr;
      get_real_table_vt_mapping(op.get_ref_table_id(), vt_mapping);
      if (OB_ISNULL(vt_mapping)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: vt mapping is null", K(ret), K(phy_op.get_vt_table_id()));
      } else if (OB_FAIL(phy_op.init_key_types(op.get_range_columns().count()))) {
      } else if (OB_FAIL(phy_op.init_key_tenant_ids(op.get_range_columns().count()))) {
      } else if (OB_FAIL(schema_guard->get_table_schema(phy_op.get_vt_table_id(), vt_table_schema))) {
        LOG_WARN("get table schema failed", K(phy_op.get_vt_table_id()), K(ret));
      } else {
        // set vt has tenant_id column
        bool has_tenant_id_col = false;
        for (int64_t nth_col = 0; nth_col < table_schema->get_column_count() && OB_SUCC(ret); ++nth_col) {
          const ObColumnSchemaV2* col_schema = table_schema->get_column_schema_by_idx(nth_col);
          if (OB_ISNULL(col_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is null", K(ret));
          } else if (0 == col_schema->get_column_name_str().case_compare("TENANT_ID")) {
            phy_op.set_has_tenant_id_col(true);
            has_tenant_id_col = true;
            break;
          }
        }
        const ObIArray<ColumnItem>& range_columns = op.get_range_columns();
        for (int64_t k = 0; k < range_columns.count() && OB_SUCC(ret); ++k) {
          uint64_t column_id = UINT64_MAX;
          uint64_t range_column_id = range_columns.at(k).column_id_;
          const ObColumnSchemaV2* vt_col_schema = vt_table_schema->get_column_schema(range_column_id);
          if (OB_ISNULL(vt_col_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: column schema is null", K(range_column_id), K(ret));
          } else if (has_tenant_id_col && 0 == k &&
                     0 != vt_col_schema->get_column_name_str().case_compare("TENANT_ID")) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: the first key must be tenant id", K(range_column_id), K(ret));
          }
          for (int64_t nth_col = 0; nth_col < table_schema->get_column_count() && OB_SUCC(ret); ++nth_col) {
            const ObColumnSchemaV2* col_schema = table_schema->get_column_schema_by_idx(nth_col);
            if (OB_ISNULL(col_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column schema is null", K(ret), K(column_id));
            } else if (0 == col_schema->get_column_name_str().case_compare(vt_col_schema->get_column_name_str())) {
              column_id = col_schema->get_column_id();
              bool extra_tenant_id = false;
              const ObColumnSchemaV2* col_schema = table_schema->get_column_schema(column_id);
              for (int64_t nth_i = vt_mapping->start_pos_;
                   !extra_tenant_id && nth_i < vt_mapping->end_pos_ && OB_SUCC(ret);
                   ++nth_i) {
                if (0 == ObString(with_tenant_id_columns[nth_i]).case_compare(col_schema->get_column_name_str())) {
                  extra_tenant_id = true;
                  break;
                }
              }
              ObObjMeta obj_meta;
              obj_meta.set_type(col_schema->get_data_type());
              obj_meta.set_collation_type(col_schema->get_collation_type());
              OZ(phy_op.add_key_type(obj_meta));
              OZ(phy_op.add_key_with_tenant_ids(extra_tenant_id));
              break;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (UINT64_MAX == column_id) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: column not found", K(ret), K(range_column_id), K(k));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (op.get_range_columns().count() != phy_op.get_key_types().count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("key is not match",
              K(op.get_range_columns().count()),
              K(phy_op.get_key_types().count()),
              K(ret),
              K(op.get_range_columns()),
              K(phy_op.get_key_types()));
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_real_virtual_table(ObLogTableScan& op, ObTableScan& phy_op)
{
  int ret = OB_SUCCESS;
  uint64_t real_table_id = ObSchemaUtils::get_real_table_mappings_tid(op.get_ref_table_id());
  if (OB_INVALID_ID != real_table_id) {
    LOG_DEBUG("trace generate real virtual table", K(op.get_ref_table_id()));
    uint64_t tenant_id = extract_tenant_id(op.get_ref_table_id());
    phy_op.set_is_vt_mapping();
    phy_op.set_vt_table_id(op.get_ref_table_id());
    phy_op.set_ref_table_id(real_table_id);
    phy_op.set_index_table_id(real_table_id);
    VTMapping* vt_mapping = nullptr;
    get_real_table_vt_mapping(op.get_ref_table_id(), vt_mapping);
    LOG_DEBUG("debug real virtual table", K(tenant_id), K(real_table_id), K(op.get_ref_table_id()));
    if (OB_ISNULL(vt_mapping)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: virtual table can't get vt mapping", K(ret), K(real_table_id));
    } else if (op.get_index_back()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: virtual table can't scan with index back", K(ret), K(real_table_id));
    } else {
      const ObIArray<ObRawExpr*>& access_exprs = op.get_access_exprs();
      LOG_DEBUG("debug origin column ids", K(phy_op.get_output_column_ids()));
      OZ(phy_op.init_extra_tenant_ids(access_exprs.count()));
      OZ(phy_op.assign_org_output_column_ids(phy_op.get_output_column_ids()));
      OZ(phy_op.init_output_column(phy_op.get_org_output_column_ids().count()));
      OZ(phy_op.init_output_columns_type(phy_op.get_org_output_column_ids().count()));
      phy_op.set_has_real_tenant_id(vt_mapping->use_real_tenant_id_);
      ARRAY_FOREACH(access_exprs, i)
      {
        ObRawExpr* expr = access_exprs.at(i);
        uint64_t column_id = UINT64_MAX;
        if (OB_UNLIKELY(OB_ISNULL(expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(ret));
        } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("rowscan not supported", K(ret));
        } else {
          ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
          ObRawExpr* mapping_expr = col_expr->get_real_expr();
          ObExpr* mapping_rt_expr = nullptr;
          column_id = col_expr->get_column_id();
          if (OB_ISNULL(mapping_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mapping expr is null", K(ret));
          } else {
            bool extra_tenant_id = false;
            for (int64_t nth_i = vt_mapping->start_pos_;
                 !extra_tenant_id && nth_i < vt_mapping->end_pos_ && OB_SUCC(ret);
                 ++nth_i) {
              if (0 == ObString(with_tenant_id_columns[nth_i]).case_compare(col_expr->get_column_name())) {
                extra_tenant_id = true;
                break;
              }
            }
            ObObjMeta obj_meta;
            obj_meta.set_type(col_expr->get_data_type());
            obj_meta.set_collation_type(col_expr->get_collation_type());
            phy_op.add_output_column_type(obj_meta);
            phy_op.add_extra_column_ids(extra_tenant_id);
            LOG_TRACE("debug columns with tenant ids",
                K(ret),
                K(col_expr->get_column_name()),
                K(col_expr->get_data_type()),
                K(extra_tenant_id),
                K(col_expr->get_collation_type()));
          }
          if (OB_FAIL(ret)) {
          } else if (UINT64_MAX == column_id) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column ref", K(ret), K(column_id));
          } else {
            OZ(phy_op.add_output_column((static_cast<ObColumnRefRawExpr*>(mapping_expr))->get_column_id()));
          }
        }
      }  // end for
      // range key maybe not output
      LOG_TRACE("debug mapping column ids", K(phy_op.get_output_column_ids()), K(phy_op.get_extra_tenant_ids()));
      if (OB_FAIL(ret)) {
      } else {
        OZ(convert_table_param(op, phy_op, phy_op.get_ref_table_id(), phy_op.get_index_table_id()));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_table_scan_with_index_back(
    ObLogTableScan& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObTableScanWithIndexBack* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  bool is_top = false;
  if (0 != child_ops.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(child_ops.count()));
  } else if (OB_FAIL(
                 create_phy_op_desc(PHY_TABLE_SCAN_WITH_DOMAIN_INDEX, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(phy_op));
  } else if (OB_FAIL(op.is_top_table_scan(is_top))) {
    LOG_WARN("is_top_table_scan fails unexpectedly", K(ret));
  } else if (OB_FAIL(set_optimization_info(op, phy_op))) {
    LOG_WARN("set optimazation info failed", K(ret));
  } else if (OB_FAIL(set_partition_range_info(op, phy_op))) {
    LOG_WARN("set partition range info failed", K(ret));
  } else {
    phy_op->set_is_top_table_scan(is_top);
    phy_op->set_table_location_key(op.get_table_id());
    phy_op->set_ref_table_id(op.get_ref_table_id());
    phy_op->set_is_index_global(op.get_is_index_global());
    phy_op->set_for_update(op.is_for_update(), op.get_for_update_wait_us());
    phy_plan_->set_for_update(op.is_for_update());
    phy_op->set_index_table_id(op.get_ref_table_id());
    phy_op->set_table_row_count(op.get_table_row_count());
    phy_op->set_output_row_count(static_cast<int64_t>(op.get_output_row_count()));
    phy_op->set_query_range_row_count(static_cast<int64_t>(op.get_query_range_row_count()));
    phy_op->set_index_back_row_count(static_cast<int64_t>(op.get_index_back_row_count()));
    phy_op->set_estimate_method(op.get_estimate_method());
    ObSqlExpression* limit_expr = NULL;
    ObSqlExpression* offset_expr = NULL;
    if (NULL != op.get_limit_expr()) {
      if (OB_FAIL(generate_sql_expr(
              phy_op->get_sql_expression_factory(), op.get_limit_expr(), *out_row_desc, limit_expr))) {
        LOG_WARN("Generation of sql expression fails", K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != op.get_offset_expr()) {
      if (OB_FAIL(generate_sql_expr(
              phy_op->get_sql_expression_factory(), op.get_offset_expr(), *out_row_desc, offset_expr))) {
        LOG_WARN("Generation of sql expression fails", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_op->set_limit_offset(limit_expr, offset_expr))) {
        LOG_WARN("Setting for limit and offset in ObTableScan fails", K(ret));
      }

      if (OB_SUCC(ret)) {
        ObOrderDirection scan_direction = op.get_scan_direction();
        ObQueryFlag query_flag;
        query_flag.index_back_ = 0;
        if (is_descending_direction(scan_direction)) {
          query_flag.scan_order_ = ObQueryFlag::Reverse;
        } else {
          query_flag.scan_order_ = ObQueryFlag::Forward;
        }
        phy_op->set_flags(query_flag.flag_);
      }
    }
  }

  if (OB_SUCC(ret)) {
    const ObIArray<ObRawExpr*>& access_exprs = op.get_access_exprs();
    LOG_DEBUG("table scan's access columns", K(access_exprs.count()));
    if (OB_FAIL(phy_op->init_output_column(access_exprs.count()))) {
      LOG_WARN("fail to init output column", K(ret));
    }
    // 1. add basic column
    ObArray<uint64_t> output_column_ids;
    ARRAY_FOREACH(access_exprs, i)
    {
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(access_exprs.at(i));
      if (OB_UNLIKELY(
              OB_ISNULL(col_expr) || !col_expr->has_flag(IS_COLUMN) || col_expr->get_table_id() != op.get_table_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("Expected basic column",
            K(col_expr),
            K(col_expr->has_flag(IS_COLUMN)),
            K(col_expr->get_table_id()),
            K(op.get_table_id()));
      } else if (OB_FAIL(phy_op->add_output_column(col_expr->get_column_id()))) {
        LOG_WARN("failed to add column", K(ret), K(i));
      } else if (OB_FAIL(output_column_ids.push_back(col_expr->get_column_id()))) {
        LOG_WARN("store output column ids failed", K(ret));
      } else if (OB_FAIL(out_row_desc->add_column(col_expr))) {
        LOG_WARN("failed to add column desc", K(ret), K(i));
      } else if (OB_FAIL(col_expr->add_flag(IS_COLUMNLIZED))) {
        LOG_WARN("failed to add flag IS_COLUMNLIZED", K(ret));
      } else {
      }
    }  // end for
    // 2. add virtual column
    ARRAY_FOREACH(access_exprs, i)
    {
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(access_exprs.at(i));
      if (col_expr->is_generated_column()) {
        if (OB_ISNULL(col_expr->get_dependant_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Unexpected NULL pointer", K(col_expr->get_dependant_expr()), K(ret));
        } else {
          ObColumnExpression* dependant_expr = NULL;
          if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), dependant_expr))) {
            LOG_WARN("failed to create expr", K(ret));
          } else if (OB_ISNULL(dependant_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("alloc invalid expr", K(dependant_expr), K(ret));
          } else if (OB_FAIL(generate_expr(*col_expr->get_dependant_expr(), *out_row_desc, *dependant_expr))) {
            LOG_WARN("failed to generate expr", K(*col_expr->get_dependant_expr()), K(*out_row_desc), K(ret));
          } else if (OB_FAIL(phy_op->add_virtual_column_expr(dependant_expr))) {
            LOG_WARN("failed to add virtual column expr", K(dependant_expr), K(ret));
          } else {
            dependant_expr->set_result_index(i);
          }
        }
      } else {
      }
    }  // end for
    // construct schema_param
    if (OB_SUCC(ret)) {
      const uint64_t table_id = op.get_ref_table_id();
      const uint64_t index_id = op.get_index_table_id();
      const ObLogPlan* log_plan = op.get_plan();
      ObSchemaGetterGuard* schema_guard = NULL;
      const ObTableSchema* table_schema = NULL;
      const ObTableSchema* index_schema = NULL;
      if (OB_INVALID_ID == table_id || OB_INVALID_ID == index_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid id", K(table_id), K(index_id), K(ret));
      } else if (OB_ISNULL(log_plan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("NULL ptr", K(ret));
      } else if (OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("NULL ptr", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(table_id, table_schema))) {
        LOG_WARN("get table schema failed", K(table_id), K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
        LOG_WARN("get table schema failed", K(index_id), K(ret));
      } else if (OB_ISNULL(table_schema) || OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("NULL ptr", K(ret), KP(table_schema), KP(index_schema));
      } else if (OB_FAIL(phy_op->get_table_param().convert(
                     *table_schema, *index_schema, output_column_ids, op.get_index_back()))) {
        LOG_WARN("convert schema failed",
            K(ret),
            K(*table_schema),
            K(*index_schema),
            K(output_column_ids),
            K(op.get_index_back()));
      }
    }
  }

  if (OB_SUCC(ret) && op.exist_hint()) {
    phy_op->set_exist_hint(true);
    phy_op->set_hint(op.get_const_hint());
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_subplan_tree(ObLogicalOperator& subplan_root, ObPhyOperator*& phy_root)
{
  int ret = OB_SUCCESS;
  PhyOpsDesc out_phy_ops;
  PhyOpsDesc root_trans_ops;
  if (OB_FAIL(postorder_visit(subplan_root, out_phy_ops, true))) {
    LOG_WARN("post order visit logical tree failed", K(ret));
  } else if (out_phy_ops.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output physical operator is unexpected", K(out_phy_ops.count()));
  } else {
    phy_root = out_phy_ops.at(0).first;
  }
  // destroy row_desc anyway
  for (int64_t i = 0; i < out_phy_ops.count(); ++i) {
    if (NULL != out_phy_ops.at(i).second) {
      ob_delete(out_phy_ops.at(i).second);
    }
  }  // end for
  for (int64_t i = 0; i < root_trans_ops.count(); ++i) {
    if (NULL != root_trans_ops.at(i).second) {
      ob_delete(root_trans_ops.at(i).second);
    }
  }  // end for
  return ret;
}

int ObCodeGeneratorImpl::construct_hash_elements_for_connect_by(
    ObLogJoin& op, const PhyOpsDesc& child_ops, ObConnectBy& phy_op)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* left_op = NULL;
  ObLogicalOperator* right_op = NULL;

  ObLogPlan* log_plan = NULL;
  ObSQLSessionInfo* session_info = NULL;
  const ObTimeZoneInfo* time_zone = NULL;
  if (OB_ISNULL(log_plan = op.get_plan()) ||
      OB_ISNULL(session_info = log_plan->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(time_zone = session_info->get_timezone_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret), K(log_plan));
  } else if (OB_ISNULL(left_op = op.get_child(0)) || OB_ISNULL(right_op = op.get_child(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null", K(ret));
  } else {
    const ObRelIds& left_table_set = left_op->get_table_set();
    const ObRelIds& right_table_set = right_op->get_table_set();
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_other_join_conditions().count(); i++) {
      ObRawExpr* other_cond = op.get_other_join_conditions().at(i);
      if (OB_ISNULL(other_cond)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("other condition is null", K(ret));
      } else if (T_OP_EQ == other_cond->get_expr_type()) {
        bool prior_at_left = false;
        bool can_use_as_key = false;
        if (other_cond->get_param_count() != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param count of equal", K(ret), KPC(other_cond));
        } else if (OB_ISNULL(other_cond->get_param_expr(0)) || OB_ISNULL(other_cond->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(ret), KPC(other_cond));
        } else {
          const ObRelIds& left_rel_ids = other_cond->get_param_expr(0)->get_relation_ids();
          const ObRelIds& right_rel_ids = other_cond->get_param_expr(1)->get_relation_ids();
          if (left_rel_ids.is_empty() || right_rel_ids.is_empty()) {
          } else if (left_rel_ids.is_subset(left_table_set) && right_rel_ids.is_subset(right_table_set)) {
            can_use_as_key = true;
            prior_at_left = true;
          } else if (left_rel_ids.is_subset(right_table_set) && right_rel_ids.is_subset(left_table_set)) {
            can_use_as_key = true;
          }
          if (can_use_as_key) {
            ObSqlExpression* left_param = NULL;
            ObSqlExpression* right_param = NULL;
            if (OB_FAIL(create_expression(phy_op.get_sql_expression_factory(), left_param))) {
              LOG_WARN("failed to create expr", K(ret));
            } else if (OB_FAIL(create_expression(phy_op.get_sql_expression_factory(), right_param))) {
              LOG_WARN("failed to create expr", K(ret));
            } else if (OB_ISNULL(left_param) || OB_ISNULL(right_param)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("alloc invalid expr", K(ret), K(left_param), K(right_param));
            } else if (OB_FAIL(generate_expr(
                           *other_cond->get_param_expr(0), *child_ops.at(prior_at_left ? 0 : 1).second, *left_param))) {
              LOG_WARN("failed to generate expr", K(ret), K(other_cond->get_param_expr(0)), K(child_ops));
            } else if (OB_FAIL(generate_expr(*other_cond->get_param_expr(1),
                           *child_ops.at(prior_at_left ? 1 : 0).second,
                           *right_param))) {
              LOG_WARN("failed to generate expr", K(ret), K(other_cond->get_param_expr(0)), K(child_ops));
            } else if (OB_FAIL(phy_op.add_hash_key_expr(prior_at_left ? right_param : left_param))) {
              LOG_WARN("push back hash key expr failed", K(ret));
            } else if (OB_FAIL(phy_op.add_hash_probe_expr(prior_at_left ? left_param : right_param))) {
              LOG_WARN("push back hash probe expr failed", K(ret));
            } else if (OB_FAIL(phy_op.add_equal_condition_info(other_cond->get_result_type().get_calc_type(),
                           other_cond->get_result_type().get_calc_collation_type()))) {
              LOG_WARN("add equal condition info failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_join(ObLogJoin& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObPhyOperatorType type = PHY_INVALID;
  switch (op.get_join_algo()) {
    case NESTED_LOOP_JOIN: {
      type = CONNECT_BY_JOIN != op.get_join_type()
                 ? PHY_NESTED_LOOP_JOIN
                 : (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2250
                           ? PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX
                           : (op.get_nl_params().count() > 0 ? PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX
                                                             : PHY_NESTED_LOOP_CONNECT_BY));
      break;
    }
    case MERGE_JOIN: {
      type = PHY_MERGE_JOIN;
      break;
    }
    case HASH_JOIN: {
      type = PHY_HASH_JOIN;
      break;
    }
    default: {
      LOG_WARN("unknown join algorithm", K(op.get_join_algo()));
      break;
    }
  }
  ObJoin* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  if (OB_UNLIKELY(2 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (OB_FAIL(create_phy_op_desc(type, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret), K(type));
  } else if (OB_ISNULL(phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan is null", K(ret));
  } else {
    // mark the phy_plan's is_late_mat flag
    bool is_late_mat = (phy_plan_->get_is_late_materialized() || op.is_late_mat());
    phy_plan_->set_is_late_materialized(is_late_mat);

    // print log if possible
    if (OB_NOT_NULL(op.get_stmt()) &&
        (stmt::T_INSERT == op.get_stmt()->get_stmt_type() || stmt::T_UPDATE == op.get_stmt()->get_stmt_type() ||
            stmt::T_DELETE == op.get_stmt()->get_stmt_type()) &&
        true == is_late_mat) {
      LOG_WARN("INSERT, UPDATE or DELETE smt should not be marked as late materialized.",
          K(op.get_stmt()->get_stmt_type()),
          K(is_late_mat),
          K(*op.get_stmt()));
    }

    if (op.is_partition_wise()) {
      phy_plan_->set_is_wise_join(op.is_partition_wise());  // set is_wise_join
    }
    phy_op->set_join_type(op.get_join_type());
    if (MERGE_JOIN == op.get_join_algo()) {
      // A.1. add equijoin conditions
      const ObIArray<ObRawExpr*>& equal_join_conds = op.get_equal_join_conditions();
      ARRAY_FOREACH(equal_join_conds, i)
      {
        ObRawExpr* raw_expr = equal_join_conds.at(i);
        ObSqlExpression* expr = NULL;
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("null pointer", K(ret));
        } else if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), expr))) {
          LOG_WARN("failed to create expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("alloc invalid expr", K(ret), K(expr));
        } else if (OB_FAIL(generate_expr(*raw_expr, *child_ops.at(0).second, *child_ops.at(1).second, *expr))) {
          LOG_WARN("failed to generate expr", K(ret), K(raw_expr));
        } else if (OB_FAIL(phy_op->add_equijoin_condition(expr))) {
          LOG_WARN("failed to add sql expr", K(ret), K(expr));
        } else {
          LOG_DEBUG("equijoin condition", K(*raw_expr), K(*expr));
        }
      }  // end for
      // A.2. add merge directions
      if (OB_SUCC(ret)) {
        ObMergeJoin* merge_join = static_cast<ObMergeJoin*>(phy_op);
        const ObIArray<ObOrderDirection>& merge_directions = op.get_merge_directions();
        bool left_unique = false;
        if (OB_FAIL(merge_join->set_merge_directions(merge_directions))) {
          LOG_WARN("fail to set merge directions", K(ret));
        } else if (OB_FAIL(op.is_left_unique(left_unique))) {
          LOG_WARN("fail to check left unique", K(ret), K(op));
        } else {
          merge_join->set_left_unique(left_unique);
          LOG_DEBUG("merge join left unique", K(left_unique));
        }
      }
    } else if (NESTED_LOOP_JOIN == op.get_join_algo()) {  // nested loop join
      if (0 != op.get_equal_join_conditions().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("equal join conditions' count should equal 0", K(ret));
      } else {
        ObBasicNestedLoopJoin* nlj_op = static_cast<ObBasicNestedLoopJoin*>(phy_op);
        const common::ObIArray<std::pair<int64_t, ObRawExpr*> >& exec_params = op.get_nl_params();
        if (OB_ISNULL(nlj_op)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("nlj_op is null", K(ret));
        } else if (OB_FAIL(nlj_op->init_param_count(exec_params.count()))) {
          LOG_WARN("fail to init param count", K(ret));
        } else {
          ARRAY_FOREACH(exec_params, i)
          {
            const std::pair<int64_t, ObRawExpr*>& param_expr = exec_params.at(i);
            ObSqlExpression* expr = NULL;
            if (OB_FAIL(create_expression(nlj_op->get_sql_expression_factory(), expr))) {
              LOG_WARN("failed to create expr", K(ret));
            } else if (OB_ISNULL(expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("alloc invalid expr", K(ret), K(expr));
            } else if (OB_FAIL(generate_expr(*param_expr.second, *child_ops.at(0).second, *expr))) {
              LOG_WARN("failed to generate expr", K(ret), K(param_expr.second));
            } else if (OB_FAIL(nlj_op->add_nlj_param(expr, param_expr.first))) {
              LOG_WARN("failed to add nlj param", K(ret), K(expr));
            }
          }
          if (OB_SUCC(ret) && PHY_NESTED_LOOP_JOIN == type) {
            bool use_batch_nlj = false;
            ObNestedLoopJoin* nlj = static_cast<ObNestedLoopJoin*>(phy_op);
            int64_t mem_limit = -1;
            if (OB_ISNULL(nlj)) {
              ret = OB_ERR_NULL_VALUE;
              LOG_WARN("nlj is null", K(ret));
            } else if (OB_FAIL(op.can_use_batch_nlj(use_batch_nlj))) {
              LOG_WARN("Failed to check use batch nested loop join", K(ret));
            } else if (use_batch_nlj) {
              nlj->set_use_group(use_batch_nlj);
              if (mem_limit >= 1 && mem_limit < ObNestedLoopJoin::DEFAULT_MEM_LIMIT) {
                nlj->set_mem_limit(mem_limit);
              }
            }
          }
        }
      }
    } else if (HASH_JOIN == op.get_join_algo()) {
      const ObIArray<ObRawExpr*>& equal_join_conds = op.get_equal_join_conditions();
      ARRAY_FOREACH(equal_join_conds, i)
      {
        ObRawExpr* raw_expr = equal_join_conds.at(i);
        ObSqlExpression* expr = NULL;
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("null pointer", K(ret));
        } else if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), expr))) {
          LOG_WARN("failed to create expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("alloc invalid expr", K(ret), K(expr));
        } else if (OB_ISNULL(child_ops.at(0).second) || OB_ISNULL(child_ops.at(1).second)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("bad null", K(ret), K(child_ops.at(0).second), K(child_ops.at(1).second));
        } else if (OB_FAIL(generate_expr(*raw_expr, *child_ops.at(0).second, *child_ops.at(1).second, *expr))) {
          LOG_WARN("failed to generate expr", K(ret), K(raw_expr));
        } else if (OB_FAIL(phy_op->add_equijoin_condition(expr))) {
          LOG_WARN("failed to add sql expr", K(ret), K(expr));
        } else {
          LOG_DEBUG("equijoin condition", K(*raw_expr), K(*expr));
        }
      }  // end for
    } else {
      // do nothing
    }

    const common::ObIArray<std::pair<int64_t, ObRawExpr*> >& exec_params = op.get_exec_params();
    // level pseudo column as a exec param
    if (OB_SUCC(ret) && CONNECT_BY_JOIN == op.get_join_type()) {
      ObConnectBy* nlj_op = static_cast<ObConnectBy*>(phy_op);
      if (OB_ISNULL(nlj_op)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("nlj_op is null", K(ret));
      } else if (exec_params.count() == 0) {
        // Do nothing
      } else if (exec_params.count() != 1) {
        // Only one ? expr for all level expr in connent by clause.
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected exec params count in connect by", K(exec_params.count()), K(ret));
      } else if (OB_FAIL(nlj_op->init_exec_param_count(exec_params.count()))) {
        LOG_WARN("fail to init param count", K(ret));
      } else {
        ARRAY_FOREACH(exec_params, i)
        {
          const std::pair<int64_t, ObRawExpr*>& param_expr = exec_params.at(i);
          LOG_DEBUG("connect by", K(param_expr.first), K(param_expr.second), K(ret));
          if (OB_FAIL(nlj_op->add_exec_param(param_expr.first))) {
            LOG_WARN("failed to add nlj param", K(ret));
          }
        }
      }
    }

    // 2. add other join conditions
    const ObIArray<ObRawExpr*>& other_join_conds = op.get_other_join_conditions();
    ARRAY_FOREACH(other_join_conds, i)
    {
      bool replaced = false;
      ObRawExpr* raw_expr = other_join_conds.at(i);
      ObSqlExpression* expr = NULL;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null pointer", K(ret));
      } else if (!exec_params.empty() &&
                 ObRawExprUtils::replace_level_column(raw_expr, exec_params.at(0).second, replaced)) {
        LOG_WARN("Failed to replace all level expr", K(ret));
      } else if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), expr))) {
        LOG_WARN("failed to create expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("alloc invalid expr", K(ret), K(expr));
      } else if (OB_FAIL(generate_expr(*raw_expr, *child_ops.at(0).second, *child_ops.at(1).second, *expr))) {
        LOG_WARN("failed to generate expr", K(ret), K(raw_expr));
      } else if (OB_FAIL(phy_op->add_other_join_condition(expr))) {
        LOG_WARN("failed to add sql expr", K(ret), K(expr));
      } else {
        LOG_DEBUG("equijoin condition", K(*raw_expr), K(*expr), K(expr->get_expr_items()));
      }
    }  // end for

    // 3. construct row desc
    if (OB_SUCC(ret)) {
      if (LEFT_SEMI_JOIN == op.get_join_type() || LEFT_ANTI_JOIN == op.get_join_type()) {
        // only output left row, use left row desc as output
        if (OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
          LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
        }
      } else if (RIGHT_SEMI_JOIN == op.get_join_type() || RIGHT_ANTI_JOIN == op.get_join_type()) {
        if (OB_FAIL(copy_row_desc(*child_ops.at(1).second, *out_row_desc))) {
          LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(1).second));
        }
      } else {
        if (OB_FAIL(copy_row_desc_by_projector(*child_ops.at(0).second,
                child_ops.at(0).first->get_projector(),
                child_ops.at(0).first->get_projector_size(),
                *out_row_desc))) {
          LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
        } else if (OB_FAIL(copy_row_desc_by_projector(*child_ops.at(1).second,
                       child_ops.at(1).first->get_projector(),
                       child_ops.at(1).first->get_projector_size(),
                       *out_row_desc))) {
          LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(1).second));
        }
      }
    }
    // 4. deal with connect by join
    if (OB_SUCC(ret)) {
      if (CONNECT_BY_JOIN == op.get_join_type()) {
        if (OB_FAIL(generate_pump_row_desc(op, phy_op, child_ops.at(0), child_ops.at(1)))) {
          LOG_WARN("fail to record column idx", KPC(phy_op), K(ret));
        } else if (OB_FAIL(generate_root_row_desc(phy_op, child_ops.at(0), child_ops.at(1)))) {
          LOG_WARN("fail to record column idx", KPC(phy_op), K(ret));
        } else if (OB_FAIL(generate_pseudo_column_row_desc(op, *phy_op, *out_row_desc))) {
          LOG_WARN("fail to generate pseudo column row desc", KPC(phy_op), K(ret));
        } else if (OB_FAIL(set_connect_by_ctx(op, *phy_op))) {
          LOG_WARN("fail to set nocycle", K(ret));
        } else if (OB_FAIL(add_connect_by_prior_exprs(op, *child_ops.at(0).second, *phy_op))) {
          LOG_WARN("fail to add connect_by prior exprs", K(ret));
        } else if (OB_FAIL(add_connect_by_root_exprs(op, *out_row_desc, *phy_op))) {
          LOG_WARN("fail to add connect_by_root expr", K(ret));
        } else if (PHY_NESTED_LOOP_CONNECT_BY == type &&
                   OB_FAIL(construct_hash_elements_for_connect_by(op, child_ops, *static_cast<ObConnectBy*>(phy_op)))) {
          LOG_WARN("construct hash elements for connect by failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::set_connect_by_ctx(const ObLogJoin& log_join, ObJoin& phy_join)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_join.get_stmt()) || OB_UNLIKELY(false == log_join.get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", KPC(log_join.get_stmt()), K(ret));
  } else {
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(log_join.get_stmt());
    if (select_stmt->is_nocycle()) {
      phy_join.set_nocycle();
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::add_connect_by_prior_exprs(ObLogJoin& log_join, const RowDesc& row_desc, ObJoin& phy_join)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < log_join.get_connect_by_prior_exprs().count(); ++i) {
    ObRawExpr* prior_expr = log_join.get_connect_by_prior_exprs().at(i);
    ObSqlExpression* expr = NULL;
    if (OB_ISNULL(prior_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null pointer", K(ret));
    } else if (OB_FAIL(create_expression(phy_join.get_sql_expression_factory(), expr))) {
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc invalid expr", K(ret), K(expr));
    } else if (OB_FAIL(generate_expr(*prior_expr, row_desc, *expr))) {
      LOG_WARN("failed to generate expr", K(prior_expr), KPC(prior_expr), K(ret));
    } else if (OB_FAIL(phy_join.add_connect_by_prior_expr(expr))) {
      LOG_WARN("failed to add sql expr", K(expr), K(ret));
    } else {
      LOG_DEBUG("prior expr", K(prior_expr), KPC(prior_expr));
    }
  }

  return ret;
}

int ObCodeGeneratorImpl::add_connect_by_root_exprs(ObLogJoin& log_join, RowDesc& row_desc, ObJoin& phy_join)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_join.get_stmt()) || OB_UNLIKELY(false == log_join.get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", KPC(log_join.get_stmt()), K(ret));
  } else {
    ObSelectStmt* stmt = static_cast<ObSelectStmt*>(log_join.get_stmt());
    ObSEArray<ObRawExpr*, 8> connect_by_root_exprs;
    if (OB_FAIL(stmt->get_connect_by_root_exprs(connect_by_root_exprs))) {
      LOG_WARN("failed to get connect by root exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_root_exprs.count(); ++i) {
      ObRawExpr* root_expr = connect_by_root_exprs.at(i);
      ObColumnExpression* calc_expr = NULL;
      if (OB_FAIL(create_expression(phy_join.get_sql_expression_factory(), calc_expr))) {
        LOG_WARN("failed to create expr", K(ret));
      } else if (OB_ISNULL(calc_expr) || OB_ISNULL(root_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("alloc invalid expr", KPC(calc_expr), KPC(root_expr), K(ret));
      } else if (OB_FAIL(generate_expr(*root_expr, row_desc, *calc_expr))) {
        LOG_WARN("failed to generate expr", KPC(root_expr), KPC(calc_expr), K(ret));
      } else if (OB_FAIL(phy_join.add_connect_by_root_expr(calc_expr))) {
        LOG_WARN("failed to add sql expr", KPC(calc_expr), KPC(root_expr), K(ret));
      } else if (OB_FAIL(row_desc.add_column(root_expr))) {
        LOG_WARN("fail to add column", KPC(root_expr), K(ret));
      } else {
        calc_expr->set_result_index(row_desc.get_column_num() - 1);
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_pump_row_desc(ObLogJoin& log_join, ObJoin* join,
    const std::pair<ObPhyOperator*, RowDesc*>& left_child, const std::pair<ObPhyOperator*, RowDesc*>& right_child)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> column_idx;
  ObRawExpr* raw_expr = NULL;
  ObRawExpr* origin_expr = NULL;
  const RowDesc* left_row_desc = left_child.second;
  const RowDesc* right_row_desc = right_child.second;
  bool need_pump_row_desc = false;

  if (OB_ISNULL(left_row_desc) || OB_ISNULL(right_row_desc) || OB_ISNULL(log_join.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child row desc is NULL", KPC(left_row_desc), KPC(right_row_desc), K(ret));
  }

  /*
   * We need pump row.
   * select c1, c2, level from troot where level < 3 start with c1 = 1 connect by prior c1 = c2 and level < 1;
   * Don't need pump row, never get a cycle.
   * select 1 from dual connect by level < 3;
   *
   */
  need_pump_row_desc = log_join.get_stmt()->has_prior();
  LOG_DEBUG("connect by", K(need_pump_row_desc), K(*left_row_desc), K(*right_row_desc));
  int64_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < left_row_desc->get_column_num() && need_pump_row_desc; ++i) {
    if (OB_FAIL(left_row_desc->get_column(i, raw_expr))) {
      LOG_WARN("fail to get column", K(i), K(ret));
    } else if (OB_FAIL(right_row_desc->get_column(i, origin_expr))) {
      LOG_WARN("fail to get column", K(i), K(ret));
    } else if (OB_FAIL(right_row_desc->get_idx(origin_expr, idx))) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_FAIL(column_idx.push_back(idx))) {
      LOG_WARN("fail to push back idx", K(idx), K(i), K(ret));
    } else {
      LOG_DEBUG("connect by add a pump row desc column", KPC(raw_expr), KPC(origin_expr), K(i), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(join)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join operator is NULL", K(ret));
  } else if (OB_FAIL(join->set_pump_row_desc(column_idx))) {
    LOG_WARN("fail to set origin column idx", K(ret));
  } else {
    LOG_DEBUG("connect by pump rowdesc", K(column_idx), K(left_row_desc->get_column_num()), K(ret));
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_pseudo_column_row_desc(ObLogJoin& op, ObJoin& join, RowDesc& out_row_desc)
{
  int ret = OB_SUCCESS;
  int64_t column_num = out_row_desc.get_column_num();
  if (OB_FAIL(join.init_pseudo_column_row_desc())) {
    LOG_WARN("fail to init pseudo column row desc", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_pseudo_columns().count(); ++i) {
    ObPseudoColumnRawExpr* expr = op.get_pseudo_columns().at(i);
    if (OB_FAIL(out_row_desc.add_column(expr))) {
      LOG_WARN("fail to add column", K(i), KPC(expr), K(ret));
    } else if (OB_FAIL(join.add_pseudo_column(expr, column_num + i))) {
      LOG_WARN("fail to add column", K(i), KPC(expr), K(column_num), K(ret));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_cte_pseudo_column_row_desc(
    ObLogSet& op, ObRecursiveUnionAll& phy_set_op, RowDesc& out_row_desc)
{
  int ret = OB_SUCCESS;
  UNUSED(op);
  int64_t column_num = out_row_desc.get_column_num();
  if (OB_FAIL(phy_set_op.init_cte_pseudo_column())) {
    LOG_WARN("Failed to init pseudo column row desc", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < out_row_desc.get_column_num(); ++i) {
    if (T_CTE_SEARCH_COLUMN == out_row_desc.get_columns().at(i)->get_expr_type()) {
      ObPseudoColumnRawExpr* expr = static_cast<ObPseudoColumnRawExpr*>(out_row_desc.get_columns().at(i));
      if (OB_FAIL(phy_set_op.add_cte_pseudo_column(expr, i))) {
        LOG_WARN("Failed to add search column", K(ret), K(i), KPC(expr), K(column_num));
      }
    } else if (T_CTE_CYCLE_COLUMN == out_row_desc.get_columns().at(i)->get_expr_type()) {
      ObPseudoColumnRawExpr* expr = static_cast<ObPseudoColumnRawExpr*>(out_row_desc.get_columns().at(i));
      if (OB_FAIL(phy_set_op.add_cte_pseudo_column(expr, i))) {
        LOG_WARN("Failed to add cycle column", K(ret), K(i), KPC(expr), K(column_num));
      } else {
        ObRawExpr *v_raw_expr, *d_v_raw_expr;
        ObSqlExpression* v_expr = nullptr;
        ObSqlExpression* d_v_expr = nullptr;
        expr->get_cte_cycle_value(v_raw_expr, d_v_raw_expr);
        if (OB_ISNULL(v_raw_expr) || OB_ISNULL(d_v_raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid raw expr", K(ret));
        } else if (OB_FAIL(create_expression(phy_set_op.get_sql_expression_factory(), v_expr))) {
          LOG_WARN("Failed to create expr", K(ret));
        } else if (OB_ISNULL(v_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Invalid expr", K(ret), K(v_expr));
        } else if (OB_FAIL(generate_expr(*v_raw_expr, out_row_desc, *v_expr))) {
          LOG_WARN("Failed to generate expr", K(ret), K(i), KPC(v_raw_expr));
        } else if (OB_FAIL(create_expression(phy_set_op.get_sql_expression_factory(), d_v_expr))) {
          LOG_WARN("Failed to create expr", K(ret));
        } else if (OB_ISNULL(d_v_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Invalid expr", K(ret), K(d_v_expr));
        } else if (OB_FAIL(generate_expr(*d_v_raw_expr, out_row_desc, *d_v_expr))) {
          LOG_WARN("Failed to generate expr", K(ret), K(i), KPC(d_v_raw_expr));
        } else if (OB_FAIL(phy_set_op.set_cycle_pseudo_values(*v_expr, *d_v_expr))) {
          LOG_WARN("Failed to set cycle values", K(ret), K(i), K(*expr));
        } else {
          LOG_DEBUG("Cycle values", K(ret), KPC(v_expr), KPC(d_v_expr), KPC(d_v_raw_expr), KPC(v_raw_expr));
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_root_row_desc(ObJoin* join, const std::pair<ObPhyOperator*, RowDesc*>& left_child,
    const std::pair<ObPhyOperator*, RowDesc*>& right_child)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> column_idx;
  ObRawExpr* raw_expr = NULL;
  ObRawExpr* origin_expr = NULL;
  const RowDesc* left_row_desc = left_child.second;
  const RowDesc* right_row_desc = right_child.second;

  if (OB_ISNULL(left_row_desc) || OB_ISNULL(right_row_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child row desc is NULL", KPC(left_row_desc), KPC(right_row_desc), K(ret));
  }

  int64_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < right_row_desc->get_column_num(); ++i) {
    if (OB_FAIL(right_row_desc->get_column(i, raw_expr))) {
      LOG_WARN("fail to get column", K(i), K(ret));
    } else if (OB_FAIL(left_row_desc->get_column(i, origin_expr))) {
      // order siblings by may add extra output at right side
      if (T_INT == raw_expr->get_expr_type()) {
        if (OB_FAIL(column_idx.push_back(ObJoin::DUMMY_OUPUT))) {
          LOG_WARN("fail to push back idx", K(i), K(ret));
        }
      } else {
        if (OB_FAIL(column_idx.push_back(ObJoin::UNUSED_POS))) {
          LOG_WARN("fail to push back idx", K(i), K(ret));
        }
        LOG_DEBUG("left output expr must be in right row desc", KPC(raw_expr), K(i), K(ret));
      }
    } else if (OB_FAIL(left_row_desc->get_idx(origin_expr, idx))) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_FAIL(column_idx.push_back(idx))) {
      LOG_WARN("fail to push back idx", K(idx), K(i), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(join)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join operator is NULL", K(ret));
  } else if (OB_FAIL(join->set_root_row_desc(column_idx))) {
    LOG_WARN("fail to set origin column idx", K(ret));
  } else {
    LOG_DEBUG("connect by root rowdesc", K(column_idx), K(left_row_desc->get_column_num()), K(ret));
  }
  return ret;
}

class LGITargetOp {
public:
  bool operator()(const ObPhyOperator& op) const
  {
    return PHY_LIGHT_GRANULE_ITERATOR == op.get_type();
  }
};

int ObCodeGeneratorImpl::convert_exchange(
    ObLogExchange& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops, bool in_root_job)
{
  int ret = OB_SUCCESS;
  ObPhyOperatorType type = PHY_INVALID;
  if (OB_ISNULL(op.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op stmt is null", K(ret));
  } else if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (OB_ISNULL(child_ops.at(0).first) || OB_ISNULL(child_ops.at(0).second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid children", KP(child_ops.at(0).first), KP(child_ops.at(0).second), K(ret));
  } else if (op.is_producer()) {
    ObTransmit* transmit = NULL;
    if (op.get_is_remote()) {
      type = PHY_DIRECT_TRANSMIT;
      RowDesc* out_row_desc = NULL;
      if (OB_FAIL(create_phy_op_desc(type, transmit, out_row_desc, out_ops, op.get_op_id()))) {
        LOG_WARN("failed to create phy op and desc", K(ret));
      } else if (child_ops.at(0).first->get_output_count() > 0 && child_ops.at(0).second->get_column_num() > 0 &&
                 OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
        // no column reorder for exchange
        LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
      } else {
        transmit->set_split_task_count(1);
        transmit->set_parallel_server_count(1);
        transmit->set_server_parallel_thread_count(1);
        transmit->get_job_conf().set_task_split_type(ObTaskSpliter::REMOTE_IDENTITY_SPLIT);
      }
    } else {
      ObArray<ObSqlExpression*> dist_exprs;
      RowDesc* out_row_desc = NULL;
      ObSqlExpression* part_expr = NULL;
      ObSqlExpression* sub_part_expr = NULL;
      const int32_t* input_projector = child_ops.at(0).first->get_projector();
      int64_t input_projector_size = child_ops.at(0).first->get_projector_size();
      const ObIArray<ObRawExpr*>& repart_keys = op.get_repart_keys();
      const ObIArray<ObRawExpr*>& repart_sub_keys = op.get_repart_sub_keys();
      const ObIArray<ObRawExpr*>& repart_func_exprs = op.get_repart_func_exprs();
      const ObIArray<ObExchangeInfo::HashExpr>& hash_exprs = op.get_hash_dist_exprs();

      if (OB_REPARTITION_NO_REPARTITION != op.get_repartition_type() && !op.is_slave_mapping()) {
        type = PHY_PX_REPART_TRANSMIT;
      } else if (ObPQDistributeMethod::MAX_VALUE != op.get_dist_method()) {
        type = PHY_PX_DIST_TRANSMIT;
      } else {
        type = PHY_PX_REDUCE_TRANSMIT;
      }
      phy_plan_->inc_px_exchange_out_op_count();
      if (OB_FAIL(create_phy_op_desc(type, transmit, out_row_desc, out_ops, op.get_op_id()))) {
        LOG_WARN("failed to create phy op and desc", K(ret));
      } else if (child_ops.at(0).second->get_column_num() > 0 && child_ops.at(0).first->get_output_count() > 0 &&
                 OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
        LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
      } else if (OB_FAIL(transmit->init_repart_columns(repart_keys.count(), repart_sub_keys.count()))) {
        OB_LOG(WARN, "fail to init distinct column count", K(ret));
      } else if (OB_FAIL(transmit->init_hash_dist_columns(hash_exprs.count()))) {
        LOG_WARN("init hash columns failed", K(ret));
      } else {
        transmit->set_slave_mapping_type(op.get_slave_mapping_type());
        ColumnIndexProviderImpl idx_provider(*child_ops.at(0).second, input_projector, input_projector_size);
        LOG_DEBUG(
            "input projector of transmit", "projector", ObArrayWrap<int32_t>(input_projector, input_projector_size));
        // 0. set the repart func to transmit
        if (repart_keys.count() == 0 && repart_sub_keys.count() == 0) {
          // no repart request, do nothing
        } else if (OB_FAIL(create_expression(transmit->get_sql_expression_factory(), part_expr))) {
          LOG_WARN("failed to create expr", K(ret));
        } else if (OB_ISNULL(part_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to gen ObSqlExpression");
        } else if (OB_FAIL(create_expression(transmit->get_sql_expression_factory(), sub_part_expr))) {
          LOG_WARN("failed to create expr", K(ret));
        } else if (OB_ISNULL(sub_part_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to gen ObSqlExpression");
        } else if (OB_FAIL(set_part_func(repart_func_exprs, out_row_desc, part_expr, sub_part_expr))) {
          LOG_WARN("fail to set part func to transmit", K(ret));
        } else if (OB_FAIL(transmit->set_repart_func(*part_expr, *sub_part_expr))) {
          LOG_WARN("transmit op set repart func failed");
        }
        if (PHY_PX_REPART_TRANSMIT == type) {
          const common::ObIArray<ObRawExpr*>& output_exprs = op.get_output_exprs();
          for (int64_t i = 0, N = output_exprs.count(); OB_SUCC(ret) && i < N; i++) {
            ObRawExpr* expr = output_exprs.at(i);
            if (OB_ISNULL(expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("raw expr is NULL", K(ret), K(expr));
            } else if (expr->get_expr_type() == T_PDML_PARTITION_ID) {
              ObPxTransmit* px_transmit = static_cast<ObPxTransmit*>(transmit);
              px_transmit->set_partition_id_column_idx(i);
              break;
            }
          }
        }

        // 1. add repart columns
        int64_t repart_idx = OB_INVALID_INDEX;
        ARRAY_FOREACH(repart_keys, i)
        {
          repart_idx = OB_INVALID_INDEX;
          const ObRawExpr* raw_expr = repart_keys.at(i);
          if (OB_ISNULL(raw_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("raw_expr is NULL", K(ret));
          } else if (!raw_expr->has_flag(IS_COLUMNLIZED)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("raw expr should be columnlized", K(*raw_expr), K(i), K(ret));
          } else if (OB_FAIL(idx_provider.get_idx(raw_expr, repart_idx))) {
            LOG_WARN("failed to find column", K(*raw_expr), K(i), K(ret));
          } else if (OB_FAIL(transmit->add_repart_column(repart_idx, raw_expr->get_collation_type()))) {
            LOG_WARN("failed to add repart column", K(repart_idx), K(ret));
          } else { /*do nothing*/
          }
        }  // end for

        // 1. add repart sub columns
        int64_t repart_sub_idx = OB_INVALID_INDEX;
        ARRAY_FOREACH(repart_sub_keys, j)
        {
          repart_sub_idx = OB_INVALID_INDEX;
          const ObRawExpr* raw_expr = repart_sub_keys.at(j);
          if (OB_ISNULL(raw_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("raw expr is NULL", K(ret));
          } else if (!raw_expr->has_flag(IS_COLUMNLIZED)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("repart sub column should have be columnlized", K(*raw_expr), K(j), K(ret));
          } else if (OB_FAIL(idx_provider.get_idx(raw_expr, repart_sub_idx))) {
            LOG_WARN("failed to find column", K(*raw_expr), K(j), K(ret));
          } else if (OB_FAIL(transmit->add_repart_sub_column(repart_sub_idx, raw_expr->get_collation_type()))) {
            LOG_WARN("failed to add repart sub column", K(repart_sub_idx), K(ret));
          } else { /*do nothing*/
          }
        }  // end for

        // add hash distribute columns
        int64_t col_idx = OB_INVALID_INDEX;
        FOREACH_CNT_X(expr, hash_exprs, OB_SUCC(ret))
        {
          if (OB_ISNULL(expr->expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL expr", K(ret));
          } else if (OB_FAIL(idx_provider.get_idx(expr->expr_, col_idx)) && OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("get expr index failed", K(ret));
          } else if (!expr->expr_->has_flag(IS_COLUMNLIZED) || OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            ObSqlExpression* sql_expr = NULL;
            if (OB_FAIL(create_expression(transmit->get_sql_expression_factory(), sql_expr))) {
              LOG_WARN("failed to create expr", K(ret));
            } else if (OB_FAIL(generate_expr(*expr->expr_, *child_ops.at(0).second, *sql_expr))) {
              LOG_WARN("generate expr failed", K(ret));
            } else if (OB_ISNULL(sql_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null expr generated", K(ret));
            } else if (OB_FAIL(dist_exprs.push_back(sql_expr))) {
              LOG_WARN("array push back failed", K(ret));
            } else if (OB_FAIL(transmit->add_hash_dist_column(
                           true /* is expr */, dist_exprs.count() - 1, expr->cmp_type_))) {
              LOG_WARN("add hash distribute expression failed", K(ret));
            }
          } else if (OB_FAIL(transmit->add_hash_dist_column(false /* not expr */, col_idx, expr->cmp_type_))) {
            LOG_WARN("add hash distribute column failed", K(ret), K(col_idx), K(*expr));
          }
        }
        if (OB_SUCC(ret) && !dist_exprs.empty()) {
          if (OB_FAIL(transmit->set_dist_exprs(dist_exprs))) {
            LOG_WARN("set dist exprs failed", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          int spliter_type = ObTaskSpliter::INVALID_SPLIT;
          bool simple_insert = false;
          if (op.get_stmt()->is_insert_stmt()) {
            simple_insert = !static_cast<const ObInsertStmt*>(op.get_stmt())->value_from_select();
            simple_insert =
                simple_insert && static_cast<const ObInsertStmt*>(op.get_stmt())->get_subquery_exprs().empty();
          }
          if (simple_insert) {
            spliter_type = ObTaskSpliter::INSERT_SPLIT;
          } else {
            spliter_type = ObTaskSpliter::DISTRIBUTED_SPLIT;
          }
          transmit->set_split_task_count(op.get_slice_count());
          transmit->set_parallel_server_count(1);
          transmit->set_server_parallel_thread_count(1);
          transmit->get_job_conf().set_task_split_type(spliter_type);
          transmit->set_repartition_type(op.get_repartition_type());
          transmit->set_repartition_table_id(op.get_repartition_table_id());
          transmit->set_dist_method(op.get_dist_method());
          transmit->set_px_single(op.is_px_single());
          transmit->set_px_dop(op.get_px_dop());
          transmit->set_px_id(op.get_px_id());
          transmit->set_dfo_id(op.get_dfo_id());
          transmit->set_unmatch_row_dist_method(op.get_unmatch_row_dist_method());
          LOG_TRACE("CG transmit", K(op.get_dfo_id()), K(op.get_dist_method()));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<const ObPhyOperator*, 1> lgi_ops;
      LGITargetOp is_lgi_op;
      if (OB_FAIL(ObPhyOperator::find_target_ops(child_ops.at(0).first, is_lgi_op, lgi_ops))) {
        LOG_WARN("find lgi target ops failed", K(ret), K(child_ops.at(0).first));
      } else {
        transmit->set_has_lgi(!lgi_ops.empty());
      }
    }
  } else {
    if (op.get_is_remote()) {
      type = PHY_DIRECT_RECEIVE;
      ObReceive* receive = NULL;
      RowDesc* out_row_desc = NULL;
      if (OB_FAIL(create_phy_op_desc(type, receive, out_row_desc, out_ops, op.get_op_id()))) {
        LOG_WARN("failed to create phy op and desc", K(ret));
      } else if (child_ops.at(0).first->get_output_count() > 0 && child_ops.at(0).second->get_column_num() > 0 &&
                 OB_FAIL(copy_row_desc_by_projector(*child_ops.at(0).second,
                     child_ops.at(0).first->get_projector(),
                     child_ops.at(0).first->get_projector_size(),
                     *out_row_desc))) {
        LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
      } else {
        // do nothing
      }
    } else {
      if (min_cluster_version_ >= CLUSTER_VERSION_141) {
        if (in_root_job || op.is_rescanable()) {
          if (op.is_merge_sort()) {
            type = PHY_PX_MERGE_SORT_COORD;
          } else {
            type = PHY_PX_FIFO_COORD;
          }
          if (op.is_local_order()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected plan that has merge sort receive with local order in root job", K(ret));
          }
        } else if (op.is_merge_sort()) {
          type = PHY_PX_MERGE_SORT_RECEIVE;
        } else {
          type = PHY_PX_FIFO_RECEIVE;
        }
        ObReceive* receive = NULL;
        RowDesc* out_row_desc = NULL;
        if (OB_FAIL(ret)) {
          LOG_WARN("unexpect plan", K(ret));
        } else if (OB_FAIL(create_phy_op_desc(type, receive, out_row_desc, out_ops, op.get_op_id()))) {
          LOG_WARN("failed to create phy op and desc", K(ret));
        } else if (OB_ISNULL(receive) || OB_ISNULL(out_row_desc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Create phy op success, but receive or row desc is NULL", K(ret));
        } else if (child_ops.at(0).first->get_output_count() > 0 && child_ops.at(0).second->get_column_num() > 0 &&
                   OB_FAIL(copy_row_desc_by_projector(*child_ops.at(0).second,
                       child_ops.at(0).first->get_projector(),
                       child_ops.at(0).first->get_projector_size(),
                       *out_row_desc))) {
          LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
        } else {
          if (PHY_PX_MERGE_SORT_COORD == type || PHY_PX_FIFO_COORD == type) {
            ObPxCoord* coord = static_cast<ObPxCoord*>(receive);
            coord->set_expected_worker_count(op.get_expected_worker_count());
            LOG_TRACE("map worker to px coordinator",
                K(type),
                "id",
                op.get_operator_id(),
                "count",
                op.get_expected_worker_count());
          }
          if (PHY_PX_MERGE_SORT_RECEIVE == type || PHY_MERGE_SORT_RECEIVE == type || PHY_PX_MERGE_SORT_COORD == type) {
            const int32_t* input_projector = child_ops.at(0).first->get_projector();
            int64_t input_projector_size = child_ops.at(0).first->get_projector_size();
            ColumnIndexProviderImpl idx_provider(*child_ops.at(0).second, input_projector, input_projector_size);
            const ObIArray<OrderItem>& sort_keys = op.get_sort_keys();
            if (PHY_MERGE_SORT_RECEIVE == type) {
              ret = fill_sort_columns(idx_provider, sort_keys, receive, static_cast<ObMergeSortReceive&>(*receive));
            } else if (PHY_PX_MERGE_SORT_RECEIVE == type) {
              ObPxMergeSortReceive& merge_sort_receive = static_cast<ObPxMergeSortReceive&>(*receive);
              ret = fill_sort_columns(idx_provider, sort_keys, &merge_sort_receive, merge_sort_receive);
              if (OB_SUCC(ret)) {
                merge_sort_receive.set_local_order(op.is_local_order());
              }
            } else if (PHY_PX_MERGE_SORT_COORD == type) {
              ret = fill_sort_columns(idx_provider, sort_keys, receive, static_cast<ObPxMergeSortCoord&>(*receive));
            }
          }
        }
      } else {  //  ob 1.4 code, remove after 1.4
        type = PHY_FIFO_RECEIVE;
        ObFifoReceive* receive = NULL;
        RowDesc* out_row_desc = NULL;
        if (OB_FAIL(create_phy_op_desc(type, receive, out_row_desc, out_ops, op.get_op_id()))) {
          LOG_WARN("failed to create phy op and desc", K(ret));
        } else if (OB_ISNULL(receive)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Create phy op success, but receive is NULL", K(ret));
        } else if (child_ops.at(0).first->get_output_count() > 0 && child_ops.at(0).second->get_column_num() > 0 &&
                   OB_FAIL(copy_row_desc_by_projector(*child_ops.at(0).second,
                       child_ops.at(0).first->get_projector(),
                       child_ops.at(0).first->get_projector_size(),
                       *out_row_desc))) {
          LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
        } else if (op.is_merge_sort()) {  // todo extract
          receive->set_is_merge_sort(true);
          const int32_t* input_projector = child_ops.at(0).first->get_projector();
          int64_t input_projector_size = child_ops.at(0).first->get_projector_size();
          ColumnIndexProviderImpl idx_provider(*child_ops.at(0).second, input_projector, input_projector_size);
          const ObIArray<OrderItem>& sort_keys = op.get_sort_keys();
          ret = fill_sort_columns(idx_provider, sort_keys, receive, *receive);
        } else {
          receive->set_partition_order_specified(op.is_task_order());
          if (op.get_parent() != NULL && op.get_parent()->get_type() == log_op_def::LOG_APPEND) {
            receive->set_need_set_affected_row(true);
          }
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_distinct(ObLogDistinct& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObPhyOperatorType phy_op_type = PHY_INVALID;
  if (MERGE_AGGREGATE == op.get_algo()) {
    phy_op_type = PHY_MERGE_DISTINCT;
  } else if (HASH_AGGREGATE == op.get_algo()) {
    phy_op_type = PHY_HASH_DISTINCT;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Distinct only has MERGE/HASH", K(op.get_algo()), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObDistinct* distinct_op = NULL;
    RowDesc* out_row_desc = NULL;
    RowDesc* input_row_desc = NULL;
    const int32_t* input_projector = NULL;
    int64_t input_projector_size = 0;
    const ObIArray<ObRawExpr*>& distinct_columns = op.get_distinct_exprs();
    if (OB_UNLIKELY(1 != child_ops.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("wrong # of children ops", K(ret));
    } else if (FALSE_IT((input_row_desc = child_ops.at(0).second,
                   input_projector = child_ops.at(0).first->get_projector(),
                   input_projector_size = child_ops.at(0).first->get_projector_size()))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(create_phy_op_desc(phy_op_type, distinct_op, out_row_desc, out_ops, op.get_op_id()))) {
      LOG_WARN("failed to create phy op and desc", K(ret));
    } else if (OB_FAIL(copy_row_desc(*input_row_desc, *out_row_desc))) {
      LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
    } else if (OB_FAIL(distinct_op->init(distinct_columns.count()))) {
      OB_LOG(WARN, "fail to init distinct column count", K(ret));
    } else {
      distinct_op->set_block_mode(op.get_block_mode());
      ColumnIndexProviderImpl idx_provider(*child_ops.at(0).second, input_projector, input_projector_size);
      LOG_DEBUG(
          "input projector of distinct", "projector", ObArrayWrap<int32_t>(input_projector, input_projector_size));
      // 1. add distinct columns
      int64_t distinct_idx = OB_INVALID_INDEX;
      ARRAY_FOREACH(distinct_columns, i)
      {
        const ObRawExpr* raw_expr = distinct_columns.at(i);
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("null pointer", K(ret));
        } else if (!raw_expr->has_flag(IS_COLUMNLIZED)) {
          if (raw_expr->has_flag(IS_CONST) || raw_expr->has_flag(IS_CONST_EXPR)) {
            continue;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("distinct column should have be columnlized", K(*raw_expr));
          }
        } else if (OB_FAIL(idx_provider.get_idx(raw_expr, distinct_idx))) {
          LOG_ERROR("failed to find column", K(ret), K(*raw_expr), K(raw_expr));
        } else if (OB_FAIL(distinct_op->add_distinct_column(distinct_idx, raw_expr->get_collation_type()))) {
          LOG_WARN("failed to add distinct column", K(ret), K(distinct_idx));
        }
      }  // end for
    }
  }

  return ret;
}

int ObCodeGeneratorImpl::convert_set(ObLogSet& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;

  ObPhyOperatorType phy_op_type = PHY_INVALID;
  ObSelectStmt::SetOperator set_op_type = op.get_set_op();
  SetAlgo set_algo = op.get_algo();
  if (INVALID_SET_ALGO == set_algo) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set operation only supports merge/hash", K(ret));
  } else {
    switch (set_op_type) {
      case ObSelectStmt::UNION:
        if (op.is_recursive_union()) {
          phy_op_type = PHY_RECURSIVE_UNION_ALL;
        } else {
          phy_op_type = (MERGE_SET == set_algo ? PHY_MERGE_UNION : PHY_HASH_UNION);
        }
        break;
      case ObSelectStmt::INTERSECT:
        phy_op_type = (MERGE_SET == set_algo ? PHY_MERGE_INTERSECT : PHY_HASH_INTERSECT);
        break;
      case ObSelectStmt::EXCEPT:
        phy_op_type = (MERGE_SET == set_algo ? PHY_MERGE_EXCEPT : PHY_HASH_EXCEPT);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid set operation", K(ret), K(set_op_type));
        break;
    }
  }
  ObSetOperator* phy_set_op = NULL;
  RowDesc* out_row_desc = NULL;
  if (OB_FAIL(ret)) {
  } else if (2 != child_ops.count() && (PHY_MERGE_UNION != phy_op_type || op.is_set_distinct())) {
    // merge union all suport multi child
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(phy_op_type), K(child_ops.count()), K(op.is_set_distinct()));
  } else if (OB_FAIL(create_phy_op_desc(phy_op_type, phy_set_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op", K(ret));
  } else {
    phy_set_op->set_distinct(op.is_set_distinct());
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 8> out_exprs;
    ObRawExpr* set_expr = NULL;
    if (OB_FAIL(op.get_set_exprs(out_exprs))) {
      LOG_WARN("failed to get output exprs", K(ret));
    }
    ARRAY_FOREACH(out_exprs, i)
    {
      if (OB_ISNULL(set_expr = ObTransformUtils::get_expr_in_cast(out_exprs.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected set expr", K(ret), K(set_expr));
      } else if (OB_UNLIKELY(!set_expr->has_flag(IS_SET_OP) && !set_expr->is_pseudo_column_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected set expr", K(ret), K(set_expr->has_flag(IS_SET_OP)));
      } else if (OB_FAIL(out_row_desc->add_column(set_expr))) {
        LOG_WARN("add column to out row desc failed", K(ret));
      } else { /*do nothing*/
      }
    }  // end for
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(phy_set_op->init(out_row_desc->get_column_num()))) {
      LOG_WARN("fail to init set operator", K(ret));
    } else if (MERGE_SET == set_algo) {
      // add compare direction
      const ObIArray<ObOrderDirection>& set_directions = op.get_set_directions();
      ARRAY_FOREACH(set_directions, i)
      {
        if (OB_FAIL(static_cast<ObMergeSetOperator*>(phy_set_op)->add_set_direction(set_directions.at(i)))) {
          LOG_WARN("failed to add set direction", K(set_directions.at(i)), K(ret));
        }
      }
      // set l/r map array
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(static_cast<ObMergeSetOperator*>(phy_set_op)->set_map_array(op.get_map_array()))) {
        LOG_WARN("assign map array failed", K(ret));
      }
    }

    // add cs types
    for (int64_t i = 0, N = out_row_desc->get_column_num(); OB_SUCC(ret) && i < N; i++) {
      ObRawExpr* raw_expr = NULL;
      if (OB_FAIL(out_row_desc->get_column(i, raw_expr))) {
        LOG_WARN("Failed to get raw_expr", K(raw_expr), K(i), K(ret));
      } else if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Raw expr is NULL", K(raw_expr), K(ret));
      } else if (OB_FAIL(phy_set_op->add_collation_type(raw_expr->get_collation_type()))) {
        LOG_WARN("Failed to add collation type", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && PHY_RECURSIVE_UNION_ALL == phy_op_type) {
    ObRecursiveUnionAll* r_union = nullptr;
    ObFakeCTETable* last_cte_table = nullptr;
    if (OB_ISNULL(r_union = static_cast<ObRecursiveUnionAll*>(phy_set_op))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected NULL", K(ret));
    } else if (OB_FAIL(generate_cte_pseudo_column_row_desc(op, *r_union, *out_row_desc))) {
      LOG_WARN("Failed to generate cte pseudo", K(ret));
    } else if (OB_FAIL(phy_cte_tables_.pop_back(last_cte_table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to pop last cte table op", K(ret));
    } else if (OB_ISNULL(last_cte_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Last cte table op cann't be null!", K(ret));
    } else {
      r_union->set_fake_cte_table(last_cte_table);

      if (op.is_breadth_search()) {
        r_union->set_search_strategy(ObRecursiveInnerData::SearchStrategyType::BREADTH_FRIST);
      } else {
        r_union->set_search_strategy(ObRecursiveInnerData::SearchStrategyType::DEPTH_FRIST);
      }

      const ObIArray<OrderItem>& search_order = op.get_search_ordering();
      OZ(r_union->search_by_col_lists_.init(search_order.count()));
      OZ(r_union->init_op_schema_obj(search_order.count()));
      ARRAY_FOREACH(search_order, i)
      {
        const ObRawExpr* raw_expr = search_order.at(i).expr_;
        if (raw_expr->is_column_ref_expr()) {
          const ObColumnRefRawExpr* col_expr = static_cast<const ObColumnRefRawExpr*>(raw_expr);
          int64_t sort_idx = col_expr->get_cte_generate_column_projector_offset();
          ObSortColumn sort_column;
          sort_column.index_ = sort_idx;
          sort_column.cs_type_ = raw_expr->get_collation_type();
          sort_column.set_is_ascending(search_order.at(i).is_ascending());
          ObOpSchemaObj op_schema_obj(ITEM_TO_OBJ_TYPE(raw_expr->get_data_type()), search_order.at(i).order_type_);
          if (OB_FAIL(r_union->search_by_col_lists_.push_back(sort_column))) {
            LOG_WARN("Failed to add search by order", K(ret), K(search_order.at(i)));
          } else if (OB_FAIL(r_union->get_op_schema_objs_for_update().push_back(op_schema_obj))) {
            LOG_WARN("Failed to push back element", K(ret));
          } else {
            LOG_DEBUG("Search by column", K(sort_column), K(op_schema_obj), K(raw_expr->get_data_type()));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The search by expr must be cte table column raw expr", K(ret));
        }
      }

      const ObIArray<ColumnItem>& cycle_items = op.get_cycle_items();
      OZ(r_union->cycle_by_col_lists_.init(cycle_items.count()));
      ARRAY_FOREACH(cycle_items, i)
      {
        const ObRawExpr* raw_expr = cycle_items.at(i).expr_;
        if (raw_expr->is_column_ref_expr()) {
          ObColumnInfo col_info;
          col_info.cs_type_ = raw_expr->get_collation_type();
          col_info.index_ = cycle_items.at(i).column_id_;
          if (OB_FAIL(r_union->cycle_by_col_lists_.push_back(col_info))) {
            LOG_WARN("Failed to add cycle by order", K(ret), K(cycle_items.at(i)));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The cycle by expr must be cte table column raw expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_sort_for_set(PhyOpsDesc& child_ops)
{
  int ret = OB_SUCCESS;
  ObPhyOperator* child_operators[2] = {NULL};
  RowDesc* child_row_desc[2] = {NULL};
  ObSort* sort_ops[2] = {NULL};
  RowDesc* sort_row_desc[2] = {NULL};
  if (OB_UNLIKELY(2 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret));
  } else {
    child_operators[0] = child_ops.at(0).first;
    child_row_desc[0] = child_ops.at(0).second;
    child_operators[1] = child_ops.at(1).first;
    child_row_desc[1] = child_ops.at(1).second;
    child_ops.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
      if (OB_FAIL(create_phy_op_desc(PHY_SORT, sort_ops[i], sort_row_desc[i], child_ops, OB_INVALID_ID))) {
        LOG_WARN("create sort operator and row desc failed", K(ret));
      } else if (OB_FAIL(copy_row_desc_by_projector(*child_row_desc[i],
                     child_operators[i]->get_projector(),
                     child_operators[i]->get_projector_size(),
                     *sort_row_desc[i]))) {
        LOG_WARN("copy row desc by projector failed", K(ret), K(i));
      } else {
        if (OB_FAIL(convert_internal_sort(
                sort_row_desc[i]->get_columns(), *sort_ops[i], *sort_row_desc[i], *child_operators[i]))) {
          LOG_WARN("convert interval sort failed", K(ret));
        }
      }
    }
    for (int64_t j = 0; j < 2; ++j) {
      if (NULL != child_row_desc[j]) {
        ob_delete(child_row_desc[j]);
        child_row_desc[j] = NULL;
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_internal_sort(
    const ObIArray<ObRawExpr*>& sort_column, ObSort& sort, RowDesc& sort_row_desc, ObPhyOperator& child_op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort.init_sort_columns(sort_column.count()))) {
    SQL_CG_LOG(WARN, "fail to init sort columns.", K(ret));
  } else if (OB_FAIL(sort.init_op_schema_obj(sort_column.count()))) {
    LOG_WARN("fail to init sort schema obj array", K(ret));
  }
  ARRAY_FOREACH(sort_column, i)
  {
    int64_t sort_idx = OB_INVALID_INDEX;
    const ObRawExpr* raw_expr = sort_column.at(i);
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(i));
    } else if (OB_FAIL(sort_row_desc.get_idx(raw_expr, sort_idx))) {
      LOG_ERROR("failed to find idx for sort column", K(ret));
    } else if (OB_FAIL(sort.add_sort_column(sort_idx,
                   raw_expr->get_collation_type(),
                   true,
                   ITEM_TO_OBJ_TYPE(raw_expr->get_data_type()),
                   default_asc_direction()))) {
      LOG_WARN("add typed sort column failed", K(ret));
    } else {
      // add sort column type to op_shema_objs_
      ObOpSchemaObj op_schema_obj(ITEM_TO_OBJ_TYPE(raw_expr->get_data_type()));
      if (OB_FAIL(sort.get_op_schema_objs_for_update().push_back(op_schema_obj))) {
        LOG_WARN("failed to push back element", K(ret));
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t N = sort_row_desc.get_column_num();
    if (OB_FAIL(create_identity_projector(sort, N))) {
      LOG_WARN("create identity projector failed", K(N));
    } else {
      child_op.set_parent(&sort);
      sort.set_column_count(N);
      if (OB_FAIL(sort.set_child(0, child_op))) {
        LOG_WARN("set sort operator child failed", K(ret));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_subplan_scan(ObLogSubPlanScan& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  UNUSED(child_ops);
  int ret = OB_SUCCESS;
  ObSubPlanScan* subquery_scan = NULL;
  RowDesc* out_row_desc = NULL;
  if (OB_FAIL(create_phy_op_desc(PHY_SUBPLAN_SCAN, subquery_scan, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("create subquery scan op and desc failed", K(ret));
  } else {
    // 1. add basic column
    const ObIArray<ObRawExpr*>& access_exprs = op.get_access_exprs();
    if (OB_FAIL(subquery_scan->init_output_index(access_exprs.count()))) {
      OB_LOG(WARN, "fail to init output index", K(ret));
    }
    ARRAY_FOREACH(access_exprs, i)
    {
      if (OB_ISNULL(access_exprs.at(i)) || !access_exprs.at(i)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expected basic column expr", K(ret));
      } else {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(access_exprs.at(i));
        if (OB_FAIL(subquery_scan->add_output_index(col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID))) {
          LOG_WARN("failed to add column", K(ret), K(i));
        } else if (OB_FAIL(out_row_desc->add_column(col_expr))) {
          LOG_WARN("failed to add column desc", K(ret), K(i));
        } else {
          if (OB_FAIL(col_expr->add_flag(IS_COLUMNLIZED))) {
            LOG_WARN("failed to add flag IS_COLUMNLIZED", K(ret));
          }
        }
      }
    }  // end for
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_subplan_filter(
    ObLogSubPlanFilter& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObSubPlanFilter* subplan_filter = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc* child_row_desc = NULL;
  if (OB_UNLIKELY(0 >= child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (FALSE_IT(child_row_desc = child_ops.at(0).second)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(create_phy_op_desc(PHY_SUBPLAN_FILTER, subplan_filter, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create subplan filter op and desc", K(ret));
  } else if (OB_FAIL(copy_row_desc(*child_row_desc, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret));
  } else if (op.is_update_set()) {
    if (2 != child_ops.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update set vector too many child", K(ret));
    } else if (OB_ISNULL(child_ops.at(1).first) || OB_ISNULL(child_ops.at(1).second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child_ops first or second is null", K(ret));
    } else if (OB_FAIL(copy_row_desc_by_projector(*child_ops.at(1).second,
                   child_ops.at(1).first->get_projector(),
                   child_ops.at(1).first->get_projector_size(),
                   *out_row_desc))) {
      LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(1).second));
    } else {
      subplan_filter->set_update_set(true);
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<std::pair<int64_t, ObRawExpr*> >& rescan_params = op.get_exec_params();
    if (OB_FAIL(subplan_filter->init_rescan_param(rescan_params.count()))) {
      OB_LOG(WARN, "fail to init rescan param", K(ret));
    }
    ARRAY_FOREACH(rescan_params, i)
    {
      ObSqlExpression* sql_expr = NULL;
      int64_t param_idx = rescan_params.at(i).first;
      ObRawExpr* raw_expr = rescan_params.at(i).second;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null pointer", K(ret));
      } else if (OB_FAIL(create_expression(subplan_filter->get_sql_expression_factory(), sql_expr))) {
        LOG_WARN("create sql expression failed", K(ret));
      } else if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("alloc invalid expr", K(ret), K(sql_expr));
      } else if (OB_FAIL(generate_expr(*raw_expr, *child_row_desc, *sql_expr))) {
        LOG_WARN("generate sql expr failed", K(ret));
      } else if (OB_FAIL(subplan_filter->add_rescan_param(sql_expr, param_idx))) {
        LOG_WARN("add rescan param failed", K(ret));
      }
    }
    // set up one-time exprs
    const ObIArray<std::pair<int64_t, ObRawExpr*> >& onetime_exprs = op.get_onetime_exprs();
    if (OB_SUCC(ret) && OB_FAIL(subplan_filter->init_onetime_exprs(onetime_exprs.count()))) {
      OB_LOG(WARN, "fail to init onetime exprs", K(ret));
    }
    ARRAY_FOREACH(onetime_exprs, i)
    {
      ObSqlExpression* sql_expr = NULL;
      int64_t param_idx = onetime_exprs.at(i).first;
      ObRawExpr* raw_expr = onetime_exprs.at(i).second;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null pointer", K(ret));
      } else if (OB_FAIL(create_expression(subplan_filter->get_sql_expression_factory(), sql_expr))) {
        LOG_WARN("create sql expression failed", K(ret));
      } else if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("alloc invalid expr", K(ret), K(sql_expr));
      } else if (OB_FAIL(generate_expr(*raw_expr, *child_row_desc, *sql_expr))) {
        LOG_WARN("generate sql expr failed", K(ret));
      } else if (OB_FAIL(subplan_filter->add_onetime_expr(sql_expr, param_idx))) {
        LOG_WARN("add one-time expr failed", K(ret));
      }
    }
    // setup onetime_idxs and initplan_idxs
    if (OB_SUCC(ret)) {
      subplan_filter->add_onetime_idxs(op.get_onetime_idxs());
      subplan_filter->add_initplan_idxs(op.get_initplan_idxs());
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::handle_pdml_shadow_pk(const common::ObIArray<ObColumnRefRawExpr*>& index_dml_column_exprs,
    RowDesc* out_row_desc, RowDesc* extra_row_desc, ObPhyOperator* phy_op)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> calc_exprs;
  for (int i = 0; i < index_dml_column_exprs.count() && OB_SUCC(ret); i++) {
    ObColumnRefRawExpr* column_ref_expr = index_dml_column_exprs.at(i);
    if (column_ref_expr->is_virtual_generated_column() && !OB_ISNULL(column_ref_expr->get_dependant_expr()) &&
        column_ref_expr->get_dependant_expr()->get_expr_type() == T_OP_SHADOW_UK_PROJECT) {
      column_ref_expr->set_for_generated_column();
      if (OB_FAIL(calc_exprs.push_back(column_ref_expr))) {
        LOG_WARN("failed to add shadow pk column", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && calc_exprs.count() > 0) {
    if (OB_FAIL(add_compute(calc_exprs, *out_row_desc, extra_row_desc, *phy_op))) {
      LOG_WARN("failed to add compute for calc exprs", K(ret), K(calc_exprs));
    } else {
      LOG_DEBUG("add inner compute success", K(calc_exprs));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_for_update(ObLogForUpdate& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  // infos of the lock table
  TableItem* lock_table = NULL;
  ObSEArray<ObColumnRefRawExpr*, 4> table_columns;
  ObSEArray<ObColumnRefRawExpr*, 4> rowkey_exprs;
  bool is_nullable = false;

  // phy operator related
  ObTableLock* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  int32_t* rowkey_projector = NULL;
  int64_t rowkey_projector_size = -1;

  if (OB_ISNULL(stmt = op.get_stmt()) || OB_UNLIKELY(op.get_for_update_tables().count() != 1) ||
      OB_ISNULL(lock_table = stmt->get_table_item_by_id(op.get_for_update_tables().at(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(op.get_for_update_tables()), K(lock_table));
  } else if (OB_FAIL(op.get_table_columns(lock_table->table_id_, table_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(op.get_rowkey_exprs(lock_table->table_id_, rowkey_exprs))) {
    LOG_WARN("failed to get rowkey exprs", K(ret));
  } else if (OB_FAIL(op.is_rowkey_nullable(lock_table->table_id_, is_nullable))) {
    LOG_WARN("failed to check is rowkey nullable", K(ret));
  } else if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong # of children", K(ret));
  } else if (OB_FAIL(create_phy_op_desc(PHY_LOCK, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret));
  } else if (OB_FAIL(add_table_column_ids(op, phy_op, table_columns, rowkey_exprs.count()))) {
    LOG_WARN("fail to add column id to table update operator", K(ret));
  } else if (OB_FAIL(add_column_infos(op, *phy_op, table_columns))) {
    LOG_WARN("failed to add column infos", K(ret));
  } else if (OB_FAIL(generate_projector(rowkey_exprs, *out_row_desc, rowkey_projector, rowkey_projector_size))) {
    LOG_WARN("failed to generate projector", K(ret));
  } else {
    phy_op->set_table_id(lock_table->table_id_);
    phy_op->set_index_tid(lock_table->ref_id_);
    phy_op->set_rowkey_projector(rowkey_projector, rowkey_projector_size);
    phy_op->set_need_filter_null_row(is_nullable);
    phy_op->set_wait_time(op.get_wait_ts());
    phy_op->set_gi_above(op.is_gi_above());
    phy_plan_->set_for_update(true);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_table_dml_param(op, *phy_op))) {
      LOG_WARN("failed to convert table dml param", K(ret));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_multi_table_for_update(
    ObLogForUpdate& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  // infos of the lock table
  // phy operator related
  ObAppend* append_op = NULL;
  ObMultiPartLock* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  ObTableDMLInfo table_dml_info;
  if (OB_FAIL(create_phy_op_desc(PHY_MULTI_LOCK, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret));
  } else if (OB_FAIL(phy_op->init_table_dml_info_array(op.get_for_update_tables().count()))) {
    LOG_WARN("failed to init table dml info array", K(ret));
  } else if (OB_FAIL(phy_plan_->alloc_operator_by_type(PHY_APPEND, append_op))) {
    LOG_WARN("failed to alloc operator type", K(ret));
  } else if (OB_FAIL(append_op->create_child_array(op.get_for_update_tables().count()))) {
    LOG_WARN("failed to create child array", K(ret));
  } else {
    phy_op->set_subplan_root(append_op);
    phy_plan_->set_for_update(true);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_for_update_tables().count(); ++i) {
    ObPhyOperator* subplan_root = NULL;
    table_dml_info.reset();
    if (OB_FAIL(convert_for_update_subplan(
            op, op.get_for_update_tables().at(i), *out_row_desc, subplan_root, table_dml_info))) {
      LOG_WARN("failed to convert subplan for update", K(ret));
    } else if (OB_FAIL(append_op->set_child(static_cast<int32_t>(i), *subplan_root))) {
      LOG_WARN("failed to set child for append operator", K(ret));
    } else if (OB_FAIL(phy_op->add_table_dml_info(i, table_dml_info))) {
      LOG_WARN("failed to add table dml info", K(ret));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_for_update_subplan(ObLogForUpdate& op, const uint64_t table_id, RowDesc& row_desc,
    ObPhyOperator*& subplan_root, ObTableDMLInfo& table_dml_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnRefRawExpr*, 4> table_columns;
  ObSEArray<ObColumnRefRawExpr*, 4> rowkey_exprs;
  TableItem* table_item = NULL;
  bool is_nullable = false;
  if (OB_ISNULL(op.get_plan()) || OB_ISNULL(op.get_stmt()) ||
      OB_ISNULL(table_item = op.get_stmt()->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(op.get_plan()), K(op.get_stmt()));
  } else if (OB_FAIL(op.get_rowkey_exprs(table_id, rowkey_exprs))) {
    LOG_WARN("failed to get rowkey exprs", K(ret));
  } else if (OB_FAIL(op.get_table_columns(table_id, table_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(op.is_rowkey_nullable(table_id, is_nullable))) {
    LOG_WARN("failed to check is rowkey nullable", K(ret));
  } else if (OB_FAIL(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), 1))) {
    LOG_WARN("failed to allocate index array", K(ret));
  } else {
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(0);
    TableLocationArray& table_locs = phy_dml_info.table_locs_;
    DMLSubPlanArray& subplans = phy_dml_info.dml_subplans_;
    phy_dml_info.table_id_ = table_id;
    phy_dml_info.index_tid_ = table_item->ref_id_;

    table_dml_info.need_check_filter_null_ = is_nullable;
    table_dml_info.distinct_algo_ = T_DISTINCT_NONE;
    table_dml_info.rowkey_cnt_ = rowkey_exprs.count();

    // 1. create table location
    if (OB_SUCC(ret)) {
      ObTableLocation* tbl_location = NULL;
      if (OB_FAIL(table_locs.allocate_array(phy_plan_->get_allocator(), 1))) {
        LOG_WARN("failed to allocate table location array", K(ret));
      } else if (OB_ISNULL(tbl_location = static_cast<ObTableLocation*>(
                               phy_plan_->get_allocator().alloc(sizeof(ObTableLocation))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate table location buffer", K(ret));
      } else {
        tbl_location = new (tbl_location) ObTableLocation(phy_plan_->get_allocator());
        if (OB_FAIL(tbl_location->init_table_location(*op.get_plan()->get_optimizer_context().get_sql_schema_guard(),
                table_id,
                table_item->ref_id_,
                *op.get_stmt(),
                row_desc,
                true))) {
          LOG_WARN("failed to init table location", K(ret));
        } else {
          table_locs.at(0) = tbl_location;
        }
      }
    }
    // 2. create sub lock plan
    if (OB_SUCC(ret)) {
      PhyOpsDesc out_ops;
      ObTableLock* lock_op = NULL;
      ObPhyOperator* values_op = NULL;
      RowDesc* out_row_desc = NULL;
      if (OB_FAIL(subplans.allocate_array(phy_plan_->get_allocator(), 1))) {
        LOG_WARN("failed to allocate subplan array", K(ret));
      } else if (OB_FAIL(convert_index_values(table_id, rowkey_exprs, row_desc, out_ops, subplans.at(0)))) {
        LOG_WARN("failed to convert index values", K(ret));
      } else if (OB_UNLIKELY(out_ops.count() != 1) || OB_ISNULL(values_op = out_ops.at(0).first)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index value ops is invalid", K(ret), K(out_ops.count()));
      } else if (OB_FAIL(create_phy_op_desc(PHY_LOCK, lock_op, out_row_desc, out_ops, OB_INVALID_ID))) {
        LOG_WARN("failed to create phy op desc", K(ret));
      } else if (OB_FAIL(lock_op->set_child(0, *values_op))) {
        LOG_WARN("failed to set child for lock operator", K(ret));
      } else if (OB_FAIL(copy_row_desc(*out_ops.at(0).second, *out_row_desc))) {
        LOG_WARN("failed to copy row desc", K(ret));
      } else if (OB_FAIL(add_table_column_ids(op, lock_op, table_columns, rowkey_exprs.count()))) {
        LOG_WARN("failed to add column id to table lock operator", K(ret));
      } else if (OB_FAIL(add_column_infos(op, *lock_op, table_columns))) {
        LOG_WARN("failed to add column infos", K(ret));
      } else {
        lock_op->set_table_id(table_id);
        lock_op->set_index_tid(table_item->ref_id_);
        lock_op->set_from_multi_table_dml(true);
        lock_op->set_column_count(out_row_desc->get_column_num());
        lock_op->set_wait_time(op.get_wait_ts());
        if (OB_FAIL(add_projector(rowkey_exprs, *out_row_desc, *lock_op))) {
          LOG_WARN("failed to add projector", K(ret));
        } else if (OB_FAIL(convert_table_dml_param(op, *lock_op))) {
          LOG_WARN("failed to convert table dml param", K(ret));
        } else {
          subplans.at(0).subplan_root_ = lock_op;
          subplan_root = lock_op;
        }
      }
      for (int64_t i = 0; i < out_ops.count(); ++i) {
        if (out_ops.at(i).second != NULL) {
          ob_delete(out_ops.at(i).second);
          out_ops.at(i).second = NULL;
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_pdml_delete(ObLogDelete& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObPxMultiPartDelete* pdml_delete = NULL;
  RowDesc* out_row_desc = NULL;
  ObPhyOperatorType phy_type = PHY_PX_MULTI_PART_DELETE;
  if (1 != child_ops.count()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(create_phy_op_desc(phy_type, pdml_delete, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy pdml delete operator and row out desc", K(ret));
  } else if (OB_FAIL(copy_row_desc_by_projector(*child_ops.at(0).second,
                 child_ops.at(0).first->get_projector(),
                 child_ops.at(0).first->get_projector_size(),
                 *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else {
    const ObIArray<TableColumns>* delete_index_table = op.get_all_table_columns();
    CK(OB_NOT_NULL(delete_index_table));
    CK(1 == delete_index_table->count());                         // single table
    CK(1 == delete_index_table->at(0).index_dml_infos_.count());  // main table / index
    CK(OB_NOT_NULL(pdml_delete->get_phy_plan()));
    phy_plan_->set_use_pdml(true);
    pdml_delete->set_is_returning(op.pdml_is_returning());  // return row in current pdml-delete?
    pdml_delete->set_with_barrier(op.need_barrier());
    pdml_delete->set_is_pdml_index_maintain(op.is_index_maintenance());
    if (OB_SUCC(ret)) {
      int64_t partition_expr_idx = OB_INVALID_INDEX;
      const IndexDMLInfo& index_dml_info = delete_index_table->at(0).index_dml_infos_.at(0);
      LOG_TRACE("get the index dml info", K(index_dml_info));
      pdml_delete->set_table_id(index_dml_info.loc_table_id_);
      pdml_delete->set_index_tid(index_dml_info.index_tid_);
      pdml_delete->set_need_filter_null_row(index_dml_info.need_filter_null_);
      pdml_delete->set_distinct_algo(index_dml_info.distinct_algo_);
      pdml_delete->get_dml_table_desc().index_tid_ = index_dml_info.index_tid_;
      pdml_delete->get_dml_table_desc().partition_cnt_ = index_dml_info.part_cnt_;
      if (OB_FAIL(get_pdml_partition_id_column_idx(*out_row_desc, partition_expr_idx))) {
        LOG_WARN("failed to get partition id column idx", K(ret));
      } else if (OB_FAIL(
                     add_table_column_ids(op, pdml_delete, index_dml_info.column_exprs_, index_dml_info.rowkey_cnt_))) {
        LOG_WARN("failed to add column ids and rowkey count", K(ret));
      }
      if (OB_SUCC(ret)) {
        pdml_delete->get_dml_row_desc().set_part_id_index(partition_expr_idx);
      }
      if (OB_SUCC(ret) && op.is_index_maintenance()) {
        // find shadow pk expr and add it to out row desc and calc_exprs
        if (OB_FAIL(handle_pdml_shadow_pk(index_dml_info.column_exprs_, out_row_desc, out_row_desc, pdml_delete))) {
          LOG_WARN("failed to handle pdml shadow pk", K(ret), K(index_dml_info.column_exprs_));
        }
      }

      if (OB_SUCC(ret)) {
        int32_t* delete_projector = NULL;
        int64_t delete_projector_size = 0;
        if (OB_FAIL(generate_pdml_delete_projector(
                index_dml_info, *out_row_desc, delete_projector, delete_projector_size))) {
          LOG_WARN("failed to generate pdml delete projector", K(ret));
        } else {
          LOG_TRACE("pdml delete projector", K(*delete_projector), K(delete_projector_size));
          pdml_delete->set_pdml_delete_projector(delete_projector, delete_projector_size);
        }
      }
      LOG_TRACE("pdml cg information", K(ret), K(*out_row_desc), K(index_dml_info), K(partition_expr_idx));
    }
  }

  // trigger
  if (OB_SUCC(ret)) {
    // TODO
  }
  // foreign key
  if (OB_SUCC(ret)) {
    // TODO
  }
  return ret;
}

int ObCodeGeneratorImpl::get_pdml_partition_id_column_idx(const RowDesc& row_desc, int64_t& idx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr*>& exprs = row_desc.get_columns();
  bool found = false;
  for (int64_t i = 0; i < exprs.count(); i++) {
    const ObRawExpr* expr = exprs.at(i);
    if (T_PDML_PARTITION_ID == expr->get_expr_type()) {
      idx = i;
      found = true;
      break;
    }
  }
  if (!found) {
    idx = NO_PARTITION_ID_FLAG;  // NO_PARTITION_ID_FLAG = -2
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_delete(ObLogDelete& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObTableModify* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  ObPhyOperatorType type = PHY_INVALID;
  if (op.is_pdml()) {
    type = PHY_PX_MULTI_PART_DELETE;
  } else if (op.is_multi_part_dml()) {
    type = PHY_MULTI_PART_DELETE;
  } else if (op.is_returning()) {
    type = PHY_DELETE_RETURNING;
  } else {
    type = PHY_DELETE;
  }
  if (1 != child_ops.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (OB_FAIL(create_phy_op_desc(type, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else if (OB_FAIL(set_need_check_pk_is_null(op, phy_op))) {
    LOG_WARN("failed to get need_ck_pk_is_null", K(ret));
  } else {
    const ObIArray<TableColumns>* tables_columns = op.get_all_table_columns();
    CK(OB_NOT_NULL(tables_columns));
    CK(1 == tables_columns->count());
    CK(tables_columns->at(0).index_dml_infos_.count() > 0);
    if (OB_SUCC(ret)) {
      const IndexDMLInfo& primary_dml_info = tables_columns->at(0).index_dml_infos_.at(0);
      phy_op->set_table_id(primary_dml_info.loc_table_id_);
      phy_op->set_index_tid(primary_dml_info.index_tid_);
      phy_op->set_need_filter_null_row(primary_dml_info.need_filter_null_);
      phy_op->set_distinct_algo(primary_dml_info.distinct_algo_);
      phy_op->set_gi_above(op.is_gi_above());

      // 1. set columns and exprs required by update interface
      if (OB_FAIL(add_table_column_ids(op, phy_op, primary_dml_info.column_exprs_, primary_dml_info.rowkey_cnt_))) {
        LOG_WARN("fail to add column id to table update operator", K(ret));
      }
      if (OB_SUCC(ret)) {
        const int32_t* input_projector = child_ops.at(0).first->get_projector();
        int64_t input_projector_size = child_ops.at(0).first->get_projector_size();
        OZ(copy_projector(*phy_op, primary_dml_info.column_exprs_.count(), input_projector, input_projector_size));
      }

      if (OB_SUCC(ret) && op.is_returning()) {
        if (OB_FAIL(generate_returning_exprs(phy_op, &op.get_output_exprs(), out_row_desc))) {
          LOG_WARN("failed to generate returning exprs", K(ret));
        } else {
          out_row_desc->reset();
          ARRAY_FOREACH(op.get_output_exprs(), i)
          {
            ObRawExpr* raw_expr = op.get_output_exprs().at(i);
            OZ(out_row_desc->add_column(raw_expr));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_table_dml_param(op, *phy_op))) {
      LOG_ERROR("fail to convert table dml param", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_foreign_keys(op, *phy_op))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::copy_projector(ObPhyOperator& op, int64_t N, const int32_t* projector, int64_t projector_size)
{
  int ret = OB_SUCCESS;
  int32_t* new_projector = NULL;
  if (OB_UNLIKELY(OB_ISNULL(projector) || projector_size <= 0 || N <= 0 || N > projector_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expected", K(ret), K(projector), K(projector_size));
  } else if (NULL == (new_projector = phy_plan_->alloc_projector(N))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no memory");
  } else {
    for (int64_t i = 0; i < N; ++i) {
      new_projector[i] = projector[i];
    }
    op.set_projector(new_projector, N);
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_update(ObLogUpdate& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObTableModify* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  const RowDesc* input_row_desc = child_ops.at(0).second;
  const int32_t* input_projector = NULL;
  int64_t input_projector_size = 0;
  const ObIArray<TableColumns>* all_table_columns = op.get_all_table_columns();
  ObPhyOperatorType type = PHY_INVALID;

  if (op.is_pdml()) {
    type = PHY_PX_MULTI_PART_UPDATE;
  } else if (op.is_returning()) {
    type = PHY_UPDATE_RETURNING;
  } else {
    type = PHY_UPDATE;
  }
  if (OB_ISNULL(all_table_columns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update column or expr not set", K(all_table_columns));
  } else if (OB_UNLIKELY(1 != op.get_tables_assignments()->count() || 1 != all_table_columns->count())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("multi-table update not supported", K(ret), K(all_table_columns->count()));
  } else if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong # of children", K(ret));
  } else if (FALSE_IT((input_row_desc = child_ops.at(0).second,
                 input_projector = child_ops.at(0).first->get_projector(),
                 input_projector_size = child_ops.at(0).first->get_projector_size()))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(input_row_desc) || OB_ISNULL(input_projector) || input_projector_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected", K(ret), K(input_row_desc), K(input_projector), K(input_projector_size));
  } else if (OB_FAIL(create_phy_op_desc(type, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_FAIL(set_need_check_pk_is_null(op, phy_op))) {
    LOG_WARN("failed to get need_ck_pk_is_null", K(ret));
  } else {
    ObTableUpdateReturning* phy_op_returning = NULL;
    ObPxMultiPartUpdate* phy_op_pdml = NULL;
    if (op.is_pdml()) {
      phy_op_pdml = static_cast<ObPxMultiPartUpdate*>(phy_op);
    } else if (op.is_returning()) {
      phy_op_returning = static_cast<ObTableUpdateReturning*>(phy_op);
    }
    const IndexDMLInfo& primary_index_info = all_table_columns->at(0).index_dml_infos_.at(0);
    const ObIArray<ObColumnRefRawExpr*>& table_columns = primary_index_info.column_exprs_;
    const ObTableAssignment& table_assigns = op.get_tables_assignments()->at(0);
    // basic setup
    phy_op->set_table_id(primary_index_info.loc_table_id_);
    phy_op->set_index_tid(primary_index_info.index_tid_);
    phy_op->set_need_filter_null_row(primary_index_info.need_filter_null_);
    phy_op->set_distinct_algo(primary_index_info.distinct_algo_);
    phy_op->set_gi_above(op.is_gi_above());
    phy_plan_->set_ignore(op.is_ignore());
    phy_op->set_ignore(op.is_ignore());

    // 1. set columns and exprs required by update interface
    if (OB_FAIL(add_table_column_ids(op, phy_op, table_columns, primary_index_info.rowkey_cnt_))) {
      LOG_WARN("fail to add column id to table update operator", K(ret));
    }

    // 3. construct a project to calc new value for Update
#ifndef NDEBUG
    if (OB_SUCC(ret)) {
      int64_t column_cnt = table_columns.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(input_row_desc->get_column(input_projector[i]));
        LOG_DEBUG("col expr", K(i), K(*col_expr));
        if (OB_ISNULL(col_expr) || !col_expr->has_flag(IS_COLUMN) ||
            col_expr->get_column_id() != table_columns.at(i)->get_column_id() ||
            col_expr->get_table_id() != table_columns.at(i)->get_table_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("input row desc is invalid", K(ret));
        }
      }
    }
#endif

    if (OB_SUCC(ret)) {
      const ObIArray<ObColumnRefRawExpr*>* columns = op.get_table_columns();
      if (OB_ISNULL(columns)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected", K(columns));
      } else if (OB_FAIL(add_column_infos(op, *phy_op, *columns))) {
        LOG_WARN("failed to add column infos", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (phy_op_pdml != NULL) {
        OZ(convert_update_assignments(table_columns,
            table_assigns.assignments_,
            *input_row_desc,
            input_projector,
            input_projector_size,
            *phy_op_pdml));
      } else {
        ObTableUpdate* update_op = static_cast<ObTableUpdate*>(phy_op);
        OZ(convert_update_assignments(table_columns,
            table_assigns.assignments_,
            *input_row_desc,
            input_projector,
            input_projector_size,
            *update_op));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObAssignments& table_assigns = op.get_tables_assignments()->at(0).assignments_;
    RowDesc extra_row_desc;
    if (OB_FAIL(convert_foreign_keys(op, *phy_op))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    } else if (OB_FAIL(extra_row_desc.init())) {
      LOG_WARN("failed to init extra row desc", K(ret));
    } else if (OB_FAIL(generate_update_new_row_desc(table_assigns, *input_row_desc, extra_row_desc))) {
      LOG_WARN("failed to generate update new row desc", K(ret));
    } else if (OB_FAIL(convert_check_constraint(op, *phy_op, extra_row_desc))) {
      LOG_WARN("failed to convert check constraints", K(ret));
    } else if (PHY_UPDATE_RETURNING == type) {
      ObTableUpdateReturning* phy_update_return = static_cast<ObTableUpdateReturning*>(phy_op);
      phy_update_return->set_updated_projector(
          const_cast<int32_t*>(phy_op->get_projector()), phy_op->get_projector_size());
      if (OB_FAIL(generate_returning_exprs(phy_op, &op.get_output_exprs(), &extra_row_desc))) {
        LOG_WARN("failed to generate returning exprs", K(ret));
      } else {
        ARRAY_FOREACH(op.get_output_exprs(), i)
        {
          ObRawExpr* raw_expr = op.get_output_exprs().at(i);
          OZ(out_row_desc->add_column(raw_expr));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_table_dml_param(op, *phy_op))) {
      LOG_ERROR("fail to convert table dml param", K(ret));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_pdml_update(ObLogUpdate& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObPxMultiPartUpdate* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  const RowDesc* input_row_desc = child_ops.at(0).second;
  const int32_t* input_projector = NULL;
  int64_t input_projector_size = 0;
  const ObIArray<TableColumns>* all_table_columns = op.get_all_table_columns();
  int64_t partition_expr_idx = OB_INVALID_INDEX;
  ObPhyOperatorType type = PHY_INVALID;
  phy_plan_->set_use_pdml(true);
  type = PHY_PX_MULTI_PART_UPDATE;
  if (OB_ISNULL(all_table_columns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update column or expr not set", K(all_table_columns));
  } else if (OB_UNLIKELY(1 != op.get_tables_assignments()->count() || 1 != all_table_columns->count())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("multi-table update not supported", K(ret), K(all_table_columns->count()));
  } else if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong # of children", K(ret));
  } else if (FALSE_IT((input_row_desc = child_ops.at(0).second,
                 input_projector = child_ops.at(0).first->get_projector(),
                 input_projector_size = child_ops.at(0).first->get_projector_size()))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(input_row_desc) || OB_ISNULL(input_projector) || input_projector_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected", K(ret), K(input_row_desc), K(input_projector), K(input_projector_size));
  } else if (OB_FAIL(create_phy_op_desc(type, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_FAIL(copy_row_desc_by_projector(
                 *child_ops.at(0).second, input_projector, input_projector_size, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else if (OB_FAIL(get_pdml_partition_id_column_idx(*out_row_desc, partition_expr_idx))) {
    LOG_WARN("failed to get pdml partition id column idx", K(ret));
  } else {
    // if (op.is_returning()) {
    //  phy_op->set_use_pdml();
    //}
    const IndexDMLInfo& dml_index_info = all_table_columns->at(0).index_dml_infos_.at(0);
    const ObIArray<ObColumnRefRawExpr*>& table_columns = dml_index_info.column_exprs_;
    const ObTableAssignment& table_assigns = op.get_tables_assignments()->at(0);
    // basic setup
    phy_op->get_dml_row_desc().set_part_id_index(partition_expr_idx);
    phy_op->set_table_id(dml_index_info.loc_table_id_);
    phy_op->set_index_tid(dml_index_info.index_tid_);
    phy_op->get_dml_table_desc().index_tid_ = dml_index_info.index_tid_;
    phy_op->get_dml_table_desc().partition_cnt_ = dml_index_info.part_cnt_;
    phy_op->set_need_filter_null_row(dml_index_info.need_filter_null_);
    phy_op->set_distinct_algo(dml_index_info.distinct_algo_);
    phy_op->set_is_returning(op.pdml_is_returning());
    phy_op->set_is_pdml_index_maintain(op.is_index_maintenance());
    phy_plan_->set_ignore(op.is_ignore());
    // 1. set columns and exprs required by update interface
    if (OB_FAIL(add_table_column_ids(op, phy_op, table_columns, dml_index_info.rowkey_cnt_))) {
      LOG_WARN("fail to add column id to table update operator", K(ret));
    }
    if (OB_SUCC(ret) && op.is_index_maintenance()) {
      if (OB_FAIL(handle_pdml_shadow_pk(dml_index_info.column_exprs_, out_row_desc, out_row_desc, phy_op))) {
        LOG_WARN("failed to handle pdml shadow pk", K(ret), K(dml_index_info.column_exprs_));
      }
    }
#ifndef NDEBUG
    if (OB_SUCC(ret)) {
      int64_t column_cnt = table_columns.count();
      bool has_shadow_pk = false;
      bool has_checked_shadow_pk = false;
      int64_t facted_column_cnt = column_cnt;
      for (int i = 0; i < table_columns.count() && OB_SUCC(ret); i++) {
        ObColumnRefRawExpr* check_column_ref_expr = table_columns.at(i);
        if (check_column_ref_expr->is_virtual_generated_column() &&
            !OB_ISNULL(check_column_ref_expr->get_dependant_expr()) &&
            check_column_ref_expr->get_dependant_expr()->get_expr_type() == T_OP_SHADOW_UK_PROJECT) {
          has_shadow_pk = true;
        }
      }
      if (OB_SUCC(ret) && has_shadow_pk) {
        facted_column_cnt = column_cnt - 1;
      } else {
        facted_column_cnt = column_cnt;
      }
      LOG_TRACE("input projector", K(*input_projector), K(input_projector_size), K(column_cnt), K(facted_column_cnt));
      for (int64_t i = 0; OB_SUCC(ret) && i < facted_column_cnt; ++i) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(input_row_desc->get_column(input_projector[i]));
        ObColumnRefRawExpr* table_column_expr = table_columns.at(i);
        LOG_DEBUG("col expr", K(i), K(*col_expr));
        if (OB_ISNULL(col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("input row desc is invalid", K(i), K(facted_column_cnt), KP(col_expr), K(dml_index_info), K(ret));
        } else if (OB_ISNULL(table_column_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("table column expr is iinvalid", K(ret));
        } else if (table_column_expr->is_virtual_generated_column() &&
                   !OB_ISNULL(table_column_expr->get_dependant_expr()) &&
                   table_column_expr->get_dependant_expr()->get_expr_type() == T_OP_SHADOW_UK_PROJECT) {
          has_checked_shadow_pk = true;
        }
        if (OB_SUCC(ret)) {
          int column_idx = has_checked_shadow_pk ? i + 1 : i;
          if (column_idx >= column_cnt) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column idx is out of range", K(ret), K(column_idx), K(column_cnt));
          } else if (!col_expr->has_flag(IS_COLUMN) ||
                     col_expr->get_column_id() != table_columns.at(column_idx)->get_column_id() ||
                     col_expr->get_table_id() != table_columns.at(column_idx)->get_table_id()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("input row desc is invalid",
                K(ret),
                K(i),
                K(column_cnt),
                K(*col_expr),
                K(table_columns.at(column_idx)),
                K(table_columns));
          }
        }
      }
    }
#endif

    if (OB_SUCC(ret)) {
      const ObIArray<ObColumnRefRawExpr*>* columns = op.get_table_columns();
      if (OB_ISNULL(columns)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected", K(columns));
      } else if (OB_FAIL(add_column_infos(op, *phy_op, *columns))) {
        LOG_WARN("failed to add column infos", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      OZ(build_old_projector(table_columns, phy_op, *out_row_desc));
      OZ(convert_update_assignments(table_columns,
          table_assigns.assignments_,
          *out_row_desc,
          phy_op->get_old_projector(),
          phy_op->get_old_projector_size(),
          *phy_op));
    }
  }
  if (OB_SUCC(ret)) {
    const ObAssignments& table_assigns = op.get_tables_assignments()->at(0).assignments_;
    RowDesc extra_row_desc;
    if (OB_FAIL(convert_foreign_keys(op, *phy_op))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    } else if (OB_FAIL(extra_row_desc.init())) {
      LOG_WARN("failed to init extra row desc", K(ret));
    } else if (OB_FAIL(generate_update_new_row_desc(table_assigns, *input_row_desc, extra_row_desc))) {
      LOG_WARN("failed to generate update new row desc", K(ret));
    } else if (OB_FAIL(convert_check_constraint(op, *phy_op, extra_row_desc))) {
      LOG_WARN("failed to convert check constraints", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_table_dml_param(op, *phy_op))) {
      LOG_ERROR("fail to convert table dml param", K(ret));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::get_column_ref_base_cid(
    const ObLogicalOperator& op, ObColumnRefRawExpr* col, uint64_t& base_cid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op.get_stmt()) || OB_ISNULL(col)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    const ColumnItem* item = op.get_stmt()->get_column_item_by_id(col->get_table_id(), col->get_column_id());
    if (OB_ISNULL(item)) {
      // No ColumnItem for generated columns, return col->column_id_ directly. e.g.:
      //   create table t1 (c1 int primary key, c2 int, c3 int, unique key uk_c1(c1))
      //   partition by hash(c1) partitions 2;
      // column shadow_pk_0: is generated column generated by c1.
      base_cid = col->get_column_id();
    } else {
      base_cid = item->base_cid_;
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::add_table_column_ids(
    ObLogicalOperator& op, ObTableModify* phy_op, const ObIArray<ObColumnRefRawExpr*>& columns_ids, int64_t rowkey_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer", K(ret));
  } else if (OB_FAIL(phy_op->init_column_ids_count(columns_ids.count()))) {
    LOG_WARN("fail to init column ids count", K(ret));
  } else if (rowkey_cnt > 0) {
    if (OB_FAIL(phy_op->init_primary_key_ids(rowkey_cnt))) {
      LOG_WARN("init primary key ids failed", K(ret), K(rowkey_cnt));
    }
  }
  ARRAY_FOREACH(columns_ids, i)
  {
    ObColumnRefRawExpr* item = columns_ids.at(i);
    uint64_t base_cid = OB_INVALID_ID;
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column item", K(i), K(item));
    } else if (OB_FAIL(get_column_ref_base_cid(op, item, base_cid))) {
      LOG_WARN("get base column id failed", K(ret), K(item));
    } else if (OB_FAIL(phy_op->add_column_id(base_cid))) {
      LOG_WARN("fail to add column id", K(ret));
    } else if (i < rowkey_cnt) {
      if (OB_FAIL(phy_op->add_primary_key_id(base_cid))) {
        LOG_WARN("add primary key id failed", K(ret));
      }
    }
  }
  return ret;
}

// in insert into on duplicate key update or merge into
// we need to fetch the old row in partition storage if the statement performs an update action
// this function used to build the old row desc of the update action
template <class T>
int ObCodeGeneratorImpl::build_update_source_row_desc(
    ObLogicalOperator& op, const ObIArray<ObColumnRefRawExpr*>& all_columns, T* phy_op, RowDesc& source_row_desc)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(phy_op));
  OZ(phy_op->init_scan_column_id_array(all_columns.count()));
  ARRAY_FOREACH(all_columns, i)
  {
    uint64_t base_cid = OB_INVALID_ID;
    if (OB_FAIL(get_column_ref_base_cid(op, all_columns.at(i), base_cid))) {
      LOG_WARN("get base table's column id failed", K(ret));
    } else {
      OZ(source_row_desc.add_column(all_columns.at(i)));
      OZ(phy_op->add_scan_column_id(base_cid));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_generated_column(
    const ObIArray<ObColumnRefRawExpr*>& all_columns, ObPhyOperator& phy_op, RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  ObColumnExpression* dependant_expr = NULL;
  ARRAY_FOREACH(all_columns, i)
  {
    ObColumnRefRawExpr* col_expr = all_columns.at(i);
    CK(OB_NOT_NULL(col_expr));
    int64_t col_index = OB_INVALID_ID;
    if (OB_SUCC(ret) && col_expr->is_generated_column()) {
      if (OB_FAIL(row_desc.get_idx(col_expr, col_index))) {
        LOG_WARN("get index failed", K(ret));
      } else {
        ObRawExpr* dependant_raw_expr = col_expr->get_dependant_expr();
        OZ(create_expression(phy_op.get_sql_expression_factory(), dependant_expr));
        CK(OB_NOT_NULL(dependant_raw_expr));
        OZ(generate_expr(*dependant_raw_expr, row_desc, *dependant_expr));
        OZ(phy_op.add_virtual_column_expr(dependant_expr));
        if (OB_SUCC(ret)) {
          dependant_expr->set_result_index(col_index);
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_generated_column(const ObIArray<ObColumnRefRawExpr*>& all_columns,
    ObPhyOperator& phy_op, RowDesc& row_desc, InsertTableInfo*& insert_table_info)
{
  int ret = OB_SUCCESS;
  ObColumnExpression* dependant_expr = NULL;
  if (OB_ISNULL(insert_table_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_table_info), K(ret));
  } else {
    ARRAY_FOREACH(all_columns, i)
    {
      ObColumnRefRawExpr* col_expr = all_columns.at(i);
      CK(OB_NOT_NULL(col_expr));
      int64_t col_index = OB_INVALID_ID;
      if (OB_SUCC(ret) && col_expr->is_generated_column()) {
        if (OB_FAIL(row_desc.get_idx(col_expr, col_index))) {
          LOG_WARN("get index failed", K(ret));
        } else {
          ObRawExpr* dependant_raw_expr = col_expr->get_dependant_expr();
          OZ(create_expression(phy_op.get_sql_expression_factory(), dependant_expr));
          CK(OB_NOT_NULL(dependant_raw_expr));
          OZ(generate_expr(*dependant_raw_expr, row_desc, *dependant_expr));
          OZ(insert_table_info->add_virtual_column_expr(dependant_expr));
          if (OB_SUCC(ret)) {
            dependant_expr->set_result_index(col_index);
          }
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_replace(ObLogInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObTableReplace* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc col_row_desc;
  const RowDesc* input_row_desc = child_ops.at(0).second;
  const int32_t* input_projector = NULL;
  int64_t input_projector_size = 0;
  ObPhyOperatorType op_type = op.is_multi_part_dml() ? PHY_MULTI_TABLE_REPLACE : PHY_REPLACE;
  CK(OB_NOT_NULL(op.get_all_table_columns()),
      !op.get_all_table_columns()->empty(),
      !op.get_all_table_columns()->at(0).index_dml_infos_.empty());
  CK(OB_NOT_NULL(phy_plan_));
  CK(1 == child_ops.count());
  OZ(create_phy_op_desc(op_type, phy_op, out_row_desc, out_ops, op.get_op_id()));
  CK(OB_NOT_NULL(phy_op), OB_NOT_NULL(out_row_desc));
  if (OB_SUCC(ret)) {
    phy_op->set_gi_above(op.is_gi_above());
    phy_op->set_ignore(op.is_ignore());
    phy_op->set_table_id(op.get_all_table_columns()->at(0).index_dml_infos_.at(0).loc_table_id_);
    phy_op->set_index_tid(op.get_all_table_columns()->at(0).index_dml_infos_.at(0).index_tid_);
    phy_plan_->set_ignore(op.is_ignore());
    const ObIArray<ObColumnRefRawExpr*>* columns = op.get_table_columns();
    if (OB_ISNULL(columns)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected", K(columns));
    } else {
      if (OB_FAIL(add_column_infos(op, *phy_op, *columns))) {
        LOG_WARN("failed to add column infos", K(ret));
      } else if (OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
        LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
      } else if (FALSE_IT((input_row_desc = child_ops.at(0).second,
                     input_projector = child_ops.at(0).first->get_projector(),
                     input_projector_size = child_ops.at(0).first->get_projector_size()))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(input_row_desc) || OB_ISNULL(input_projector) || input_projector_size <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected", K(ret), K(input_row_desc), K(input_projector), K(input_projector_size));
      } else if (OB_FAIL(phy_op->init_column_res_type_count(input_projector_size))) {
        LOG_WARN("fail to init column infos count", K(ret));
      } else {
        ObRawExpr* raw_expr = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < input_projector_size; ++i) {
          if (OB_FAIL(input_row_desc->get_column(input_projector[i], raw_expr))) {
            LOG_ERROR("invalid index", K(input_row_desc), K(input_projector[i]), K(input_projector_size), K(ret));
          } else if (OB_FAIL(phy_op->add_column_res_type(raw_expr->get_result_type().get_type()))) {
            LOG_WARN("failed to push res obj type", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      const ObIArray<uint64_t>* primary_key_ids = op.get_primary_key_ids();
      if (OB_ISNULL(primary_key_ids)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get primary key ids", K(primary_key_ids));
      } else if (OB_FAIL(phy_op->set_primary_key_ids(*primary_key_ids))) {
        LOG_WARN("fail to set primary key", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
    if (OB_ISNULL(table_columns)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get table columns", K(table_columns));
    } else if (OB_FAIL(add_table_column_ids(op, phy_op, *table_columns))) {
      LOG_WARN("fail to add column id", K(ret));
    } else if (OB_FAIL(col_row_desc.init())) {
      LOG_WARN("fail to init table columns row desc", K(ret));
    } else {
      ARRAY_FOREACH(*table_columns, i)
      {
        ObColumnRefRawExpr* col_expr = table_columns->at(i);
        if (OB_UNLIKELY(OB_ISNULL(col_expr) || !col_expr->has_flag(IS_COLUMN))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Expected basic column", K(col_expr));
        } else if (OB_FAIL(col_row_desc.add_column(col_expr))) {
          LOG_WARN("failed to add column desc", K(ret), K(i));
        } else if (OB_FAIL(col_expr->add_flag(IS_COLUMNLIZED))) {
          LOG_WARN("failed to add flag IS_COLUMNLIZED", K(ret));
        } else {
        }
      }  // end for

      // add virtual column
      ARRAY_FOREACH(*table_columns, i)
      {
        ObColumnRefRawExpr* col_expr = table_columns->at(i);
        if (col_expr->is_generated_column()) {
          if (OB_ISNULL(col_expr->get_dependant_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("Unexpected NULL pointer", K(col_expr->get_dependant_expr()), K(ret));
          } else {
            ObColumnExpression* dependant_expr = NULL;
            if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), dependant_expr))) {
              LOG_WARN("failed to create expr", K(ret));
            } else if (OB_ISNULL(dependant_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("alloc invalid expr", K(dependant_expr), K(ret));
            } else if (OB_FAIL(generate_expr(*col_expr->get_dependant_expr(), col_row_desc, *dependant_expr))) {
              LOG_WARN("failed to generate expr", K(*col_expr->get_dependant_expr()), K(col_row_desc), K(ret));
            } else if (OB_FAIL(phy_op->add_virtual_column_expr(dependant_expr))) {
              LOG_WARN("failed to add virtual column expr", K(dependant_expr), K(ret));
            } else {
              dependant_expr->set_result_index(i);
            }
          }
        } else {
        }
      }  // end for
    }
  }
  if (OB_SUCC(ret)) {
    phy_op->set_only_one_unique_key(op.is_only_one_unique_key());
    const ObIArray<uint64_t>* primary_columns = op.get_primary_key_ids();
    if (OB_ISNULL(primary_columns)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get primary_columns", K(primary_columns));
    } else if (OB_FAIL(phy_op->set_primary_key_ids(*primary_columns))) {
      LOG_WARN("fail to set primary key", K(ret));
    }
  }
  if (OB_SUCC(ret) && op.is_multi_part_dml()) {
    // convert multi table dml
    RowDesc table_row_desc;
    ObMultiTableReplace* multi_table_replace = static_cast<ObMultiTableReplace*>(phy_op);
    if (OB_FAIL(generate_multi_replace_row_desc(op, table_row_desc))) {
      LOG_WARN("generate multi replace row desc failed", K(ret));
    } else if (OB_FAIL(convert_multi_table_replace_info(table_row_desc, op, *multi_table_replace))) {
      LOG_WARN("convert replace global index info failed", K(ret));
    } else if (OB_FAIL(convert_duplicate_key_checker(table_row_desc,
                   table_row_desc,
                   op.get_dupkey_checker(),
                   multi_table_replace->get_duplicate_key_checker()))) {
      LOG_WARN("convert duplicate key checker failed", K(ret));
    } else {
      multi_table_replace->get_duplicate_key_checker().set_dml_flag(storage::INSERT_RETURN_ALL_DUP);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_table_dml_param(op, *phy_op))) {
      LOG_ERROR("fail to convert table dml param", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_foreign_keys(op, *phy_op))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    RowDesc extra_row_desc;
    OZ(extra_row_desc.init());
    OZ(generate_insert_new_row_desc(op.get_table_columns(), op.get_value_exprs(), *out_row_desc, extra_row_desc));
    OZ(add_compute(op.get_output_exprs(), *out_row_desc, &extra_row_desc, *phy_op),
        op.get_name(),
        *out_row_desc,
        extra_row_desc);
    OZ(add_projector(op.get_output_exprs(), *out_row_desc, *phy_op), op.get_name(), *out_row_desc);
    OX(phy_op->set_column_count(out_row_desc->get_column_num()));
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_conflict_row_fetcher(
    ObLogConflictRowFetcher& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObTableConflictRowFetcher* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  if (OB_UNLIKELY(!child_ops.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (OB_FAIL(
                 create_phy_op_desc(PHY_TABLE_CONFLICT_ROW_FETCHER, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(phy_op) || OB_ISNULL(out_row_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create phy op desc fail", K(phy_op), K(out_row_desc));
  } else if (OB_FAIL(phy_op->init_conflict_column_ids(op.get_conflict_exprs().count()))) {
    LOG_WARN("init conflict column ids failed", K(ret));
  } else if (OB_FAIL(phy_op->init_access_column_ids(op.get_access_exprs().count()))) {
    LOG_WARN("init access column ids failed", K(ret));
  } else {
    phy_op->set_table_id(op.get_table_id());
    phy_op->set_index_tid(op.get_index_tid());
    phy_op->set_only_data_table(op.get_only_data_table());
    // convert conflict column id
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_conflict_exprs().count(); ++i) {
      const ObColumnRefRawExpr* conflict_column = op.get_conflict_exprs().at(i);
      if (OB_ISNULL(conflict_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("conflict column is null");
      } else if (OB_FAIL(phy_op->add_conflict_column_ids(conflict_column->get_column_id()))) {
        LOG_WARN("add conflict column id failed", K(ret), KPC(conflict_column));
      }
    }
    // convert access column id
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_access_exprs().count(); ++i) {
      ObColumnRefRawExpr* access_column = op.get_access_exprs().at(i);
      if (OB_FAIL(out_row_desc->add_column(access_column))) {
        LOG_WARN("add access column to out row desc failed", K(ret));
      } else if (OB_FAIL(phy_op->add_access_column_ids(access_column->get_column_id()))) {
        LOG_WARN("add access column ids to conflict row fetcher failed", K(ret), KPC(access_column));
      }
    }
    // convert virtual column expr
    OZ(convert_generated_column(op.get_access_exprs(), *phy_op, *out_row_desc));
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_insert_up(ObLogInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObTableInsertUp* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc insert_row_desc;
  RowDesc update_row_desc;
  ObPhyOperatorType op_type = op.is_multi_part_dml() ? PHY_MULTI_TABLE_INSERT_UP : PHY_INSERT_ON_DUP;
  CK(OB_NOT_NULL(op.get_all_table_columns()),
      !op.get_all_table_columns()->empty(),
      !op.get_all_table_columns()->at(0).index_dml_infos_.empty());
  OZ(create_phy_op_desc(op_type, phy_op, out_row_desc, out_ops, op.get_op_id()));
  CK(OB_NOT_NULL(phy_op), OB_NOT_NULL(out_row_desc), OB_NOT_NULL(phy_plan_));
  OZ(insert_row_desc.init());
  OZ(update_row_desc.init());
  if (OB_SUCC(ret)) {
    phy_op->set_table_id(op.get_all_table_columns()->at(0).index_dml_infos_.at(0).loc_table_id_);
    phy_op->set_index_tid(op.get_all_table_columns()->at(0).index_dml_infos_.at(0).index_tid_);
    phy_op->set_gi_above(op.is_gi_above());
    phy_op->set_ignore(op.is_ignore());
    phy_plan_->set_ignore(op.is_ignore());
  }
  const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(table_columns)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get table columns", K(ret));
    } else if (OB_FAIL(add_table_column_ids(op, phy_op, *table_columns))) {
      LOG_WARN("fail to add column id", K(ret));
    } else if (OB_FAIL(add_column_infos(op, *phy_op, *table_columns))) {
      LOG_WARN("failed to add column infos", K(ret));
    } else if (OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
      LOG_WARN("fail to copy projector from child", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<uint64_t>* duplicate_columns = op.get_primary_key_ids();
    if (OB_ISNULL(duplicate_columns)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get primary key ids", K(duplicate_columns));
    } else if (OB_FAIL(phy_op->set_primary_key_ids(*duplicate_columns))) {
      LOG_WARN("fail to set primary key", K(ret));
    }
  }
  if (OB_SUCC(ret) && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200) {
    if (OB_FAIL(copy_row_desc(*out_row_desc, insert_row_desc))) {
      LOG_WARN("copy row desc failed", K(ret), KPC(out_row_desc));
    }
  }
  if (OB_SUCC(ret)) {
    // build insert dest row desc in insert into on duplicate update
    if (OB_FAIL(construct_basic_row_desc(op.get_output_exprs(), insert_row_desc))) {
      LOG_WARN("fail to build insert row desc", K(ret));
    } else if (OB_FAIL(build_update_source_row_desc(op, *table_columns, phy_op, update_row_desc))) {
      LOG_WARN("fail to build right row desc for insert up", K(ret));
    } else if (OB_FAIL(convert_generated_column(*table_columns, *phy_op, update_row_desc))) {
      LOG_WARN("fail to convert generated column for insert up", K(ret));
    } else if (OB_FAIL(phy_op->init_update_related_column_array(table_columns->count()))) {
      LOG_WARN("fail to init ids for update", K(ret));
    }
    ARRAY_FOREACH(*table_columns, i)
    {
      uint64_t base_cid = OB_INVALID_ID;
      ObColumnRefRawExpr* expr = table_columns->at(i);
      CK(OB_NOT_NULL(expr));
      OZ(get_column_ref_base_cid(op, expr, base_cid));
      OZ(phy_op->add_update_related_column_id(base_cid));
    }
  }
  // assignment columns
  if (OB_SUCC(ret)) {
    const ObTableAssignment& table_assigns = op.get_tables_assignments()->at(0);
    if (OB_UNLIKELY(op.get_tables_assignments()->count() > 1)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("multi-table update not supported", K(ret));
    } else if (OB_FAIL(phy_op->init_assignment_expr_count(table_assigns.assignments_.count()))) {
      LOG_WARN("fail to init assignment array", K(ret));
    }
    ARRAY_FOREACH(table_assigns.assignments_, i)
    {
      const ObAssignment assign = table_assigns.assignments_.at(i);
      ObRawExpr* raw_expr = assign.expr_;
      ObColumnExpression* expr = NULL;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get raw expr", K(i), K(assign));
      } else if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), expr))) {
        LOG_WARN("failed to create expr");
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("alloc invalid expr", K(ret), K(expr));
      } else if (OB_FAIL(generate_expr(*raw_expr, insert_row_desc, update_row_desc, *expr))) {
        LOG_WARN("failed to generate expr", K(ret), K(i), K(*raw_expr));
      } else if (OB_FAIL(phy_op->add_assignment_expr(expr))) {
        LOG_WARN("fail to add expr", K(ret));
      } else if (OB_FAIL(update_row_desc.add_column(raw_expr))) {
        LOG_WARN("failed to add column desc", K(ret), K(i));
      } else {
        expr->set_result_index(update_row_desc.get_column_num() - 1);
        LOG_DEBUG("right row desc add update expr", K(raw_expr), KPC(raw_expr));
      }
    }  // end for
    if (OB_SUCC(ret)) {
      OZ(build_old_projector(*table_columns, phy_op, update_row_desc));
      OZ(convert_update_assignments(*table_columns,
          table_assigns.assignments_,
          update_row_desc,
          phy_op->get_old_projector(),
          phy_op->get_old_projector_size(),
          *phy_op));
    }
  }

  if (OB_SUCC(ret)) {
    const ObIArray<ObRawExpr*>& output_exprs = op.get_output_exprs();
    ARRAY_FOREACH(output_exprs, i)
    {
      ObRawExpr* raw_expr = output_exprs.at(i);
      if (OB_ISNULL(raw_expr)) {
        LOG_WARN("get output expr fail", K(i), K(output_exprs));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(raw_expr->clear_flag(IS_COLUMNLIZED))) {
        LOG_WARN("fail to clear flag", K(ret));
      } else {
      }
    }
  }
  if (OB_SUCC(ret) && op.is_multi_part_dml()) {
    ObMultiTableInsertUp* multi_table_insert_up = static_cast<ObMultiTableInsertUp*>(phy_op);
    RowDesc* source_row_desc = &update_row_desc;
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200) {
      if (OB_FAIL(replace_insert_row_desc(op, insert_row_desc))) {
        LOG_WARN("replace insert row desc failed", K(ret), K(op), K(insert_row_desc));
      } else {
        // in the old observer before version 2110, insert row is same as the update scan row
        // so use update row desc to generate duplicate key conflict row scan info
        // but after version 2110, insert row consists of values row and convert calc row
        // so must use insert row desc to generate duplicate key conflict row scan info
        source_row_desc = &insert_row_desc;
      }
    }
    if (OB_FAIL(convert_multi_table_insert_up_info(update_row_desc, op, *multi_table_insert_up))) {
      LOG_WARN("convert multi table insert update info failed", K(ret));
    } else if (OB_FAIL(convert_duplicate_key_checker(*source_row_desc,
                   update_row_desc,
                   op.get_dupkey_checker(),
                   multi_table_insert_up->get_duplicate_key_checker()))) {
      LOG_WARN("convert duplicate key checker failed", K(ret));
    } else {
      multi_table_insert_up->get_duplicate_key_checker().set_dml_flag(storage::INSERT_RETURN_ONE_DUP);
    }
  }
  if (OB_SUCC(ret) && !op.is_multi_part_dml()) {
    if (OB_FAIL(convert_foreign_keys(op, *phy_op))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    } else if (OB_FAIL(convert_table_dml_param(op, *phy_op))) {
      LOG_ERROR("fail to convert table dml param", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    RowDesc extra_row_desc;
    OZ(extra_row_desc.init());
    OZ(generate_insert_new_row_desc(op.get_table_columns(), op.get_value_exprs(), *out_row_desc, extra_row_desc));
    OZ(add_compute(op.get_output_exprs(), *out_row_desc, &extra_row_desc, *phy_op),
        op.get_name(),
        *out_row_desc,
        extra_row_desc);
    OZ(add_projector(op.get_output_exprs(), *out_row_desc, *phy_op), op.get_name(), *out_row_desc);
    OX(phy_op->set_column_count(out_row_desc->get_column_num()));
  }

  return ret;
}

int ObCodeGeneratorImpl::replace_insert_row_desc(ObLogInsert& op, RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(op.get_table_columns()));
  CK(op.get_output_exprs().count() == op.get_table_columns()->count());
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_output_exprs().count(); ++i) {
    OZ(row_desc.replace_column(op.get_output_exprs().at(i), op.get_table_columns()->at(i)));
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_multi_table_insert_up_info(
    RowDesc& update_row_desc, ObLogInsert& op, ObMultiTableInsertUp& phy_op)
{
  int ret = OB_SUCCESS;
  ObAppend* append_op = NULL;
  ObSEArray<ObPhyOperator*, 8> subplan_roots;
  ObTableDMLInfo table_dml_info;
  const TableColumns* table_columns = NULL;
  const ObIArray<IndexDMLInfo>* index_dml_infos = NULL;
  CK(OB_NOT_NULL(op.get_all_table_columns()));
  CK(op.get_all_table_columns()->count() > 0);
  if (OB_SUCC(ret)) {
    table_columns = &(op.get_all_table_columns()->at(0));
    index_dml_infos = &(table_columns->index_dml_infos_);
  }
  OZ(phy_op.init_table_dml_info_array(1));
  OZ(phy_plan_->alloc_operator_by_type(PHY_APPEND, append_op));
  if (OB_SUCC(ret)) {
    phy_op.set_subplan_root(append_op);
  }
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), index_dml_infos->count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos->count(); ++i) {
    const IndexDMLInfo& index_dml_info = index_dml_infos->at(i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    DMLSubPlanArray& subplans = phy_dml_info.dml_subplans_;
    OZ(phy_dml_info.table_locs_.allocate_array(phy_plan_->get_allocator(), 3));
    OZ(subplans.allocate_array(phy_plan_->get_allocator(), ObMultiTableInsertUp::DML_OP_CNT));
    OZ(generate_index_location(index_dml_info, op, update_row_desc, phy_dml_info.table_locs_.at(0)));
    if (OB_SUCC(ret) && 0 == i && op.get_part_hint() != NULL) {
      OZ(phy_dml_info.table_locs_.at(0)->add_part_hint_ids(op.get_part_hint()->part_ids_));
    }
    if (OB_SUCC(ret)) {
      if (index_dml_info.assignments_.empty()) {
        phy_dml_info.table_locs_.at(1) = NULL;
        phy_dml_info.table_locs_.at(2) = NULL;
      } else {
        OZ(generate_update_index_location(index_dml_info,
            op,
            index_dml_info.assignments_,
            update_row_desc,
            phy_dml_info.table_locs_.at(1),
            phy_dml_info.table_locs_.at(2)));
        if (OB_SUCC(ret) && 0 == i && op.get_part_hint() != NULL) {
          OZ(phy_dml_info.table_locs_.at(1)->add_part_hint_ids(op.get_part_hint()->part_ids_));
          OZ(phy_dml_info.table_locs_.at(2)->add_part_hint_ids(op.get_part_hint()->part_ids_));
        }
      }
    }
    if (OB_SUCC(ret) && !index_dml_info.assignments_.empty()) {
      // delete subplan will be produced by update operation of insert_up
      // so if assignments is empty, it indicates no need to generate delete subplan
      OZ(convert_delete_subplan(op, index_dml_info, update_row_desc, subplans.at(ObMultiTableInsertUp::DELETE_OP)));
      OZ(subplan_roots.push_back(subplans.at(ObMultiTableInsertUp::DELETE_OP).subplan_root_));
    }
    OZ(convert_insert_subplan(op, index_dml_info, update_row_desc, subplans.at(ObMultiTableInsertUp::INSERT_OP)));
    OZ(subplan_roots.push_back(subplans.at(ObMultiTableInsertUp::INSERT_OP).subplan_root_));
    if (OB_SUCC(ret) && !index_dml_info.assignments_.empty()) {
      // update subplan will be produce by update operation of insert_up
      // so if assignments is empty, it indicate no need to generate update subplan
      OZ(convert_update_subplan(
          op, 0 != i, index_dml_info, update_row_desc, subplans.at(ObMultiTableInsertUp::UPDATE_OP)));
      OZ(subplan_roots.push_back(subplans.at(ObMultiTableInsertUp::UPDATE_OP).subplan_root_));
    }
  }
  if (OB_SUCC(ret) && !subplan_roots.empty()) {
    OZ(append_op->create_child_array(subplan_roots.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
    OZ(append_op->set_child(static_cast<int32_t>(i), *subplan_roots.at(i)));
  }
  OZ(convert_multi_table_update_column_info(index_dml_infos->at(0), update_row_desc, table_dml_info.assign_columns_));
  OZ(phy_op.add_table_dml_info(0, table_dml_info));
  return ret;
}

int ObCodeGeneratorImpl::convert_merge(ObLogMerge& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObAppend* append_op = nullptr;
  ObSEArray<ObPhyOperator*, 2> subplan_roots;
  ObTableDMLInfo table_dml_info;
  ObTableMerge* phy_op = nullptr;
  RowDesc* out_row_desc = nullptr;
  RowDesc new_row_desc;  // target table new row desc
  ObPhyOperatorType op_type = op.is_multi_part_dml() ? PHY_MULTI_TABLE_MERGE : PHY_MERGE;
  const ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(op.get_stmt());
  const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
  if (OB_ISNULL(op.get_all_table_columns()) || OB_ISNULL(table_columns) ||
      OB_UNLIKELY(op.get_all_table_columns()->count() != 1) ||
      OB_UNLIKELY(op.get_all_table_columns()->at(0).index_dml_infos_.empty()) || OB_ISNULL(op.get_stmt()) ||
      OB_UNLIKELY(!op.get_stmt()->is_merge_stmt()) || OB_ISNULL(phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input param is invalid", K(ret));
  } else if (OB_FAIL(create_phy_op_desc(op_type, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(phy_op) || OB_ISNULL(out_row_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create phy op desc fail", K(phy_op), K(out_row_desc));
  } else if (OB_FAIL(add_column_infos(op, *phy_op, *table_columns))) {
    LOG_WARN("failed to add column infos", K(ret));
  } else if (OB_FAIL(copy_row_desc_by_projector(*child_ops.at(0).second,
                 child_ops.at(0).first->get_projector(),
                 child_ops.at(0).first->get_projector_size(),
                 *out_row_desc))) {
    LOG_WARN("fail to copy projector from child", K(ret));
  } else {
    phy_op->set_gi_above(op.is_gi_above());
    phy_op->set_table_id(op.get_all_table_columns()->at(0).index_dml_infos_.at(0).loc_table_id_);
    phy_op->set_index_tid(op.get_all_table_columns()->at(0).index_dml_infos_.at(0).index_tid_);
    phy_op->set_has_insert_clause(merge_stmt->has_insert_clause());
    phy_op->set_has_update_clause(merge_stmt->has_update_clause());
    OZ(generate_rowkey_desc(op, *out_row_desc, *phy_op));
    OZ(new_row_desc.init());
    OZ(construct_basic_row_desc(*table_columns, new_row_desc));
    OZ(convert_generated_column(*table_columns, *phy_op, new_row_desc));
  }

  if (OB_SUCC(ret)) {
    const ObIArray<uint64_t>* primary_key_ids = op.get_primary_key_ids();
    CK(OB_NOT_NULL(primary_key_ids));
    OZ(phy_op->set_primary_key_ids(*primary_key_ids));
    OZ(add_table_column_ids(op, phy_op, *table_columns));
    // some hack here, insert new rows are produced by output exprs
    // check convert_common_parts and the output of the merge operator
  }

  RowDesc update_row_desc;
  if (OB_SUCC(ret) && merge_stmt->has_update_clause()) {
    int32_t* update_origin_projector = NULL;
    int64_t update_origin_projector_size = 0;
    OZ(update_row_desc.init());
    OZ(construct_basic_row_desc(*table_columns, update_row_desc));
    OZ(generate_update_info_for_merge(op, *phy_op));
    OZ(generate_assign_expr_for_merge(op, *phy_op, *out_row_desc, update_row_desc));
    OZ(generate_projector(*table_columns, *out_row_desc, update_origin_projector, update_origin_projector_size));
    if (OB_SUCC(ret)) {
      phy_op->set_column_projector(update_origin_projector, update_origin_projector_size);
    }
  }

  // generate condition exprs
  MatchCondition match_cond(*phy_op);
  DeleteCondition delete_cond(*phy_op);
  UpdateCondition update_cond(*phy_op);
  InsertCondition insert_cond(*phy_op);

  OZ(convert_exprs(op.get_match_condition(), out_row_desc, match_cond));
  OZ(convert_exprs(op.get_update_condition(), out_row_desc, update_cond));
  OZ(convert_exprs(op.get_insert_condition(), out_row_desc, insert_cond));
  OZ(convert_exprs(op.get_delete_condition(), &new_row_desc, out_row_desc, delete_cond));
  OZ(convert_table_dml_param(op, *phy_op));
  OZ(convert_check_constraint(op, *phy_op, new_row_desc));

  if (OB_SUCC(ret) && op.is_multi_part_dml()) {
    ObMultiTableMerge* multi_merge = static_cast<ObMultiTableMerge*>(phy_op);
    OZ(multi_merge->init_table_dml_info_array(op.get_all_table_columns()->count()));
    OZ(phy_plan_->alloc_operator_by_type(PHY_APPEND, append_op));
    OX(multi_merge->set_subplan_root(append_op));

    if (OB_SUCC(ret)) {
      table_dml_info.reset();
      const TableColumns& all_table_columns = op.get_all_table_columns()->at(0);
      OZ(convert_global_index_merge_info(op, all_table_columns, update_row_desc, subplan_roots, table_dml_info));
      OZ(multi_merge->add_table_dml_info(0, table_dml_info));
    }

    if (OB_SUCC(ret) && !subplan_roots.empty()) {
      OZ(append_op->create_child_array(subplan_roots.count()));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); i++) {
      OZ(append_op->set_child(static_cast<int32_t>(i), *subplan_roots.at(i)));
    }
  }

  return ret;
}

int ObCodeGeneratorImpl::generate_assign_expr_for_merge(
    ObLogMerge& op, ObTableMerge& phy_op, const RowDesc& out_row_desc, RowDesc& update_row_desc)
{
  int ret = OB_SUCCESS;
  const ObTableAssignment& table_assigns = op.get_tables_assignments()->at(0);
  if (OB_UNLIKELY(op.get_tables_assignments()->count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge into is expected to have one target table", K(ret));
  } else if (OB_FAIL(phy_op.init_assignment_expr_count(table_assigns.assignments_.count()))) {
    LOG_WARN("fail to init assignment array", K(ret));
  }
  ARRAY_FOREACH(table_assigns.assignments_, i)
  {
    const ObAssignment assign = table_assigns.assignments_.at(i);
    ObRawExpr* raw_expr = assign.expr_;
    ObColumnExpression* expr = NULL;
    int64_t idx = OB_INVALID_INDEX;
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get raw expr", K(i), K(assign));
    } else if (OB_FAIL(create_expression(phy_op.get_sql_expression_factory(), expr))) {
      LOG_WARN("failed to create expr");
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc invalid expr", K(ret), K(expr));
    } else if (OB_FAIL(generate_expr(*raw_expr, out_row_desc, *expr))) {
      LOG_WARN("failed to generate expr", K(ret), K(i), K(*raw_expr), K(out_row_desc));
    } else if (OB_FAIL(phy_op.add_assignment_expr(expr))) {
      LOG_WARN("fail to add expr", K(ret));
    } else if (OB_FAIL(update_row_desc.add_column(raw_expr))) {
      LOG_WARN("failed to add column desc", K(ret), K(i));
    } else if (OB_FAIL(update_row_desc.get_idx(raw_expr, idx))) {
      LOG_WARN("failed to get update row desc", K(ret));
    } else {
      expr->set_result_index(idx);
    }
  }  // end for
  if (OB_SUCC(ret)) {
    OZ(build_old_projector(*op.get_table_columns(), &phy_op, update_row_desc));
    OZ(convert_update_assignments(*op.get_table_columns(),
        table_assigns.assignments_,
        update_row_desc,
        phy_op.get_old_projector(),
        phy_op.get_old_projector_size(),
        phy_op));
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_update_info_for_merge(ObLogMerge& op, ObTableMerge& phy_op)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
  if (OB_ISNULL(table_columns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get ids for update", K(table_columns));
  } else if (OB_FAIL(phy_op.init_scan_column_id_array(table_columns->count()))) {
    LOG_WARN("failed to init scan column id array", K(ret));
    //  } else if (OB_FAIL(phy_op.init_update_related_column_array(table_columns->count()))) {
    //    LOG_WARN("fail to init ids for update", K(ret));
  }
  ARRAY_FOREACH(*table_columns, i)
  {
    ObColumnRefRawExpr* expr = table_columns->at(i);
    uint64_t base_cid = OB_INVALID_ID;
    if (OB_FAIL(get_column_ref_base_cid(op, expr, base_cid))) {
      LOG_WARN("get base table's column id failed", K(ret), K(expr));
    } else if (OB_FAIL(phy_op.add_scan_column_id(base_cid))) {
      LOG_WARN("failed to add scan column id", K(ret));
      //    } else if (OB_FAIL(phy_op.add_update_related_column_id(base_cid))) {
      //      LOG_WARN("failed to add cid", K(ret));
    } else {
      LOG_DEBUG("add ids for update");
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_rowkey_desc(
    const ObLogMerge& log_op, const RowDesc& out_row_desc, ObTableMerge& phy_op)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr*>* rowkey_exprs = log_op.get_rowkey_exprs();
  if (OB_ISNULL(rowkey_exprs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey exprs is NULL", K(ret));
  }
  ObSEArray<int64_t, 3> rowkey_idxs;
  int64_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs->count(); ++i) {
    ObRawExpr* rowkey_expr = rowkey_exprs->at(i);
    if (OB_FAIL(out_row_desc.get_idx(rowkey_expr, idx))) {
      LOG_WARN("row key expr must be in out row desc", KPC(rowkey_expr), K(out_row_desc), K(ret));
    } else if (OB_FAIL(rowkey_idxs.push_back(idx))) {
      LOG_WARN("fail to push back idx", K(idx), KPC(rowkey_expr), K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(phy_op.set_rowkey_desc(rowkey_idxs))) {
    LOG_WARN("fail to set rowkey desc", K(ret));
  }

  return ret;
}
int ObCodeGeneratorImpl::convert_exprs(
    const common::ObIArray<ObRawExpr*>* raw_exprs, RowDesc* row_desc, ExprFunction& add_expr_operator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_exprs) || OB_ISNULL(row_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument", KPC(raw_exprs), KPC(row_desc), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs->count(); ++i) {
    ObRawExpr* raw_expr = raw_exprs->at(i);
    ObSqlExpression* expr = NULL;
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid raw expr", K(i), K(ret));
    } else if (OB_FAIL(create_expression(add_expr_operator.get_phy_op().get_sql_expression_factory(), expr))) {
      LOG_WARN("failed to create expr");
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc invalid expr", K(ret), K(expr));
    } else if (OB_FAIL(generate_expr(*raw_expr, *row_desc, *expr))) {
      LOG_WARN("failed to generate expr", K(ret), K(i), K(*raw_expr));
    } else if (OB_FAIL(add_expr_operator(expr))) {
      LOG_WARN("fail to add expr", K(ret));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_exprs(
    const common::ObIArray<ObRawExpr*>* raw_exprs, RowDesc* row_desc, ObMultiTableInsert& phy_op, int64_t idx)
{
  int ret = OB_SUCCESS;
  InsertTableInfo* insert_table_info = NULL;
  if (OB_ISNULL(row_desc) || OB_ISNULL(raw_exprs) || OB_ISNULL(insert_table_info = phy_op.get_insert_table_info(idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument", KPC(row_desc), K(raw_exprs), K(insert_table_info), K(ret));
  } else {
    ObSEArray<ObSqlExpression*, 8> conds_expr;
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs->count(); ++i) {
      ObRawExpr* raw_expr = raw_exprs->at(i);
      ObSqlExpression* expr = NULL;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid raw expr", K(i), K(raw_expr), K(ret));
      } else if (OB_FAIL(create_expression(phy_op.get_sql_expression_factory(), expr))) {
        LOG_WARN("failed to create expr");
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("alloc invalid expr", K(ret), K(expr));
      } else if (OB_FAIL(generate_expr(*raw_expr, *row_desc, *expr))) {
        LOG_WARN("failed to generate expr", K(ret), K(i), K(*raw_expr));
      } else if (OB_FAIL(conds_expr.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(insert_table_info->add_match_conds_exprs(expr))) {
        LOG_WARN("fail to add expr", K(ret));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_exprs(const common::ObIArray<ObRawExpr*>* raw_exprs, RowDesc* left_row_desc,
    RowDesc* right_row_desc, ExprFunction& function)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_exprs) || OB_ISNULL(left_row_desc) || OB_ISNULL(right_row_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument", KPC(raw_exprs), KPC(left_row_desc), K(right_row_desc), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs->count(); ++i) {
    ObRawExpr* raw_expr = raw_exprs->at(i);
    ObSqlExpression* expr = NULL;
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid raw expr", K(i), K(ret));
    } else if (OB_FAIL(create_expression(function.get_phy_op().get_sql_expression_factory(), expr))) {
      LOG_WARN("failed to create expr");
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc invalid expr", K(ret), K(expr));
    } else if (OB_FAIL(generate_expr(*raw_expr, *left_row_desc, *right_row_desc, *expr))) {
      LOG_WARN("failed to generate expr", K(ret), K(i), K(*raw_expr));
    } else if (OB_FAIL(function(expr))) {
      LOG_WARN("fail to add expr", K(ret));
    }
  }
  return ret;
}

template <class ExprType, class OpType>
int ObCodeGeneratorImpl::build_old_projector(const ObIArray<ExprType*>& old_columns, OpType* phy_op, RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  int32_t* old_projector = NULL;
  int64_t old_projector_size = 0;
  CK(OB_NOT_NULL(phy_op));
  OZ(generate_projector(old_columns, row_desc, old_projector, old_projector_size));
  if (OB_SUCC(ret)) {
    phy_op->set_old_projector(old_projector, old_projector_size);
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_insert(ObLogInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObTableInsert* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  PhyOpsDesc value_child_ops;
  ObPhyOperatorType op_type;
  if (op.is_multi_part_dml()) {
    op_type = PHY_MULTI_PART_INSERT;
  } else if (op.is_returning()) {
    op_type = PHY_INSERT_RETURNING;
  } else {
    op_type = PHY_INSERT;
  }
  CK(OB_NOT_NULL(op.get_all_table_columns()),
      !op.get_all_table_columns()->empty(),
      !op.get_all_table_columns()->at(0).index_dml_infos_.empty());
  CK(1 == child_ops.count());
  OZ(create_phy_op_desc(op_type, phy_op, out_row_desc, out_ops, op.get_op_id()));
  CK(OB_NOT_NULL(phy_op), OB_NOT_NULL(out_row_desc));
  const uint64_t table_id = op.get_all_table_columns()->at(0).index_dml_infos_.at(0).loc_table_id_;
  if (OB_SUCC(ret)) {
    // set up rowkey info for insert
    if (OB_SUCC(ret)) {
      const ObIArray<uint64_t>* primary_key_ids = op.get_primary_key_ids();
      if (OB_ISNULL(primary_key_ids)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get primary key ids", K(primary_key_ids));
      } else if (OB_FAIL(phy_op->set_primary_key_ids(*primary_key_ids))) {
        LOG_WARN("fail to set primary key", K(ret));
      }
    }
    phy_op->set_table_id(table_id);
    phy_op->set_index_tid(op.get_all_table_columns()->at(0).index_dml_infos_.at(0).index_tid_);
    phy_op->set_ignore(op.is_ignore());
    phy_op->set_gi_above(op.is_gi_above());
    phy_plan_->set_ignore(op.is_ignore());
    const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
    if (OB_ISNULL(table_columns)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get table columns", K(table_columns));
    } else if (OB_FAIL(add_table_column_ids(op, phy_op, *table_columns))) {
      LOG_WARN("fail to add column id", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObColumnRefRawExpr*>* columns = op.get_table_columns();
    const ObIArray<ObRawExpr*>* column_convert_exprs = op.get_column_convert_exprs();
    if (OB_ISNULL(columns) || OB_ISNULL(column_convert_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected", K(columns), K(column_convert_exprs));
    } else if (OB_FAIL(add_column_infos(op, *phy_op, *columns))) {
      LOG_WARN("failed to add column infos", K(ret));
    } else if (OB_FAIL(add_column_conv_infos(*phy_op, *columns, *column_convert_exprs))) {
      LOG_WARN("failed to add column conv infos", K(ret));
    } else if (OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
      LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
    }
  }
  if (OB_SUCC(ret) && op.is_multi_part_dml()) {
    ObMultiPartInsert& multi_insert = static_cast<ObMultiPartInsert&>(*phy_op);
    if (OB_FAIL(convert_insert_index_info(op, multi_insert, *out_row_desc))) {
      LOG_WARN("convert insert index info failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    RowDesc extra_row_desc;
    if (OB_FAIL(convert_foreign_keys(op, *phy_op))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    } else if (OB_FAIL(extra_row_desc.init())) {
      LOG_WARN("failed to init extra row desc", K(ret));
    } else if (OB_FAIL(generate_insert_new_row_desc(
                   op.get_table_columns(), op.get_value_exprs(), *out_row_desc, extra_row_desc))) {
      LOG_WARN("failed to generate insert new row desc", K(ret));
    } else if (OB_FAIL(convert_check_constraint(op, *phy_op, extra_row_desc))) {
      LOG_WARN("failed to convert check constraints", K(ret));
    }
  }
  // construct ObTableDMLParam
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_table_dml_param(op, *phy_op))) {
      LOG_ERROR("fail to convert table dml param", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    RowDesc extra_row_desc;
    // insert hack the operator's output
    OZ(extra_row_desc.init());
    OZ(generate_insert_new_row_desc(op.get_table_columns(), op.get_value_exprs(), *out_row_desc, extra_row_desc));
    OZ(add_compute(op.get_value_exprs(), *out_row_desc, &extra_row_desc, *phy_op),
        op.get_name(),
        *out_row_desc,
        extra_row_desc);
    OZ(add_projector(op.get_value_exprs(), *out_row_desc, *phy_op), op.get_name(), *out_row_desc);
    OX(phy_op->set_column_count(out_row_desc->get_column_num()));
    if (OB_SUCC(ret) && op.is_returning()) {
      if (PHY_MULTI_PART_INSERT == phy_op->get_type()) {
        static_cast<ObMultiPartInsert*>(phy_op)->set_insert_row_exprs();
      } else if (PHY_INSERT_RETURNING == phy_op->get_type()) {
        static_cast<ObTableInsertReturning*>(phy_op)->set_insert_row_exprs();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid phy operator type", K(ret), K(phy_op->get_type()));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(generate_returning_exprs(phy_op, &op.get_output_exprs(), &extra_row_desc))) {
          LOG_WARN("failed to generate returning exprs", K(ret));
        } else {
          out_row_desc->reset();
          ARRAY_FOREACH(op.get_output_exprs(), i)
          {
            ObRawExpr* raw_expr = op.get_output_exprs().at(i);
            OZ(out_row_desc->add_column(raw_expr));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail convert insert", "op_name", op.get_name(), "op_id", op.get_op_id(), K(ret));
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_multi_table_insert(
    ObLogInsertAll& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObMultiTableInsert* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  ObPhyOperatorType op_type = PHY_MULTI_TABLE_INSERT;
  if (OB_ISNULL(op.get_all_table_columns()) ||
      OB_UNLIKELY(op.get_all_table_columns()->count() < 1 || child_ops.count() != 1 ||
                  op.get_all_table_columns()->count() != op.get_multi_value_exprs().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument",
        K(op.get_all_table_columns()),
        K(child_ops.count()),
        K(op.get_multi_value_exprs().count()),
        K(ret));
  } else if (OB_FAIL(create_phy_op_desc(op_type, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op desc", K(ret));
  } else if (OB_ISNULL(phy_op) || OB_ISNULL(out_row_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(phy_op), K(out_row_desc), K(ret));
  } else if (OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else {
    // 1. gen basic row desc
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_all_table_columns()->count(); ++i) {
      RowDesc extra_row_desc;
      const ObIArray<ObColumnRefRawExpr*>& table_columns =
          op.get_all_table_columns()->at(i).index_dml_infos_.at(0).column_exprs_;
      if (OB_FAIL(extra_row_desc.init())) {
        LOG_WARN("failed to init extra row desc", K(ret));
      } else if (OB_FAIL(generate_insert_new_row_desc(
                     &table_columns, op.get_multi_value_exprs().at(i), *out_row_desc, extra_row_desc))) {
        LOG_WARN("failed to generate insert new row desc", K(ret));
      } else if (OB_FAIL(add_compute(op.get_multi_value_exprs().at(i), *out_row_desc, &extra_row_desc, *phy_op))) {
        LOG_WARN("failed to add compute");
      } else if (OB_FAIL(add_projector(op.get_multi_value_exprs().at(i), *out_row_desc, *phy_op))) {
        LOG_WARN("failed to add projector", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // 2. gen multi table insert subplan
      if (OB_FAIL(convert_insert_index_info(op, *phy_op, *out_row_desc))) {
        LOG_WARN("convert insert index info failed", K(ret));
        // 3. convert multi table insert info
      } else if (op.is_multi_conditions_insert() &&
                 OB_FAIL(convert_multi_insert_conditions(*phy_op, op, out_row_desc))) {
        LOG_WARN("failed to convert multi insert conditions", K(ret));
      } else {
        phy_op->set_column_count(out_row_desc->get_column_num());
        phy_op->set_is_multi_insert_first(op.is_multi_insert_first());
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail convert multi table insert", "op_name", op.get_name(), "op_id", op.get_op_id(), K(ret));
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_multi_insert_conditions(
    ObMultiTableInsert& phy_op, ObLogInsertAll& op, RowDesc* out_row_desc)
{
  int ret = OB_SUCCESS;
  if (op.is_multi_conditions_insert()) {
    if (OB_ISNULL(op.get_multi_insert_table_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(op.get_multi_insert_table_info()), K(ret));
    } else {
      int64_t pre_idx = -1;
      for (int i = 0; OB_SUCC(ret) && i < op.get_multi_insert_table_info()->count(); ++i) {
        if (pre_idx == op.get_multi_insert_table_info()->at(i).when_conds_idx_) {
          phy_op.get_insert_table_info(i)->set_when_conds_idx(pre_idx);
        } else if (OB_FAIL(convert_exprs(
                       &op.get_multi_insert_table_info()->at(i).when_conds_expr_, out_row_desc, phy_op, i))) {
          LOG_WARN("failed to convert exprs", K(ret));
        } else {
          pre_idx = op.get_multi_insert_table_info()->at(i).when_conds_idx_;
          phy_op.get_insert_table_info(i)->set_when_conds_idx(pre_idx);
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_pdml_insert_projector(const IndexDMLInfo& dml_index_info, const RowDesc& out_row_desc,
    ObLogInsert& op, int32_t*& projector, int64_t& projector_size)
{
  int ret = OB_SUCCESS;
  projector_size = op.get_table_columns()->count();
  projector = NULL;

  if (OB_ISNULL(projector = phy_plan_->alloc_projector(projector_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for projector failed", K(ret), K(projector_size));
  }

  for (int64_t col_i = 0; OB_SUCC(ret) && col_i < projector_size; ++col_i) {
    const ObColumnRefRawExpr* column_expr_of_col_i = op.get_table_columns()->at(col_i);
    const ObRawExpr* value_expr_of_col_i = NULL;

    if (OB_ISNULL(column_expr_of_col_i)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    }

    for (int64_t j = 0; OB_SUCC(ret) && j < dml_index_info.column_exprs_.count(); ++j) {
      if (OB_ISNULL(dml_index_info.column_exprs_.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret));
      } else if (dml_index_info.column_exprs_.at(j) == column_expr_of_col_i) {
        value_expr_of_col_i = dml_index_info.column_convert_exprs_.at(j);
        break;
      }
    }

    if (OB_ISNULL(value_expr_of_col_i)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown column expr", K(ret), KPC(column_expr_of_col_i));
    } else {
      int64_t offset = -1;
      const ObRawExpr* origin_expr = value_expr_of_col_i;
      for (int64_t k = 0; k < out_row_desc.get_column_num(); ++k) {
        if (out_row_desc.get_column(k) == origin_expr) {
          offset = k;
          break;
        }
      }
      if (OB_UNLIKELY(-1 == offset)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item not found", K(ret), KPC(origin_expr), K(out_row_desc));
      } else {
        projector[col_i] = offset;
      }
    }
  }

  LOG_TRACE("check projector",
      K(op.get_name()),
      K(op.get_op_id()),
      "projector",
      ObArrayWrap<int32_t>(projector, projector_size));
  return ret;
}

int ObCodeGeneratorImpl::generate_pdml_delete_projector(
    const IndexDMLInfo& dml_index_info, const RowDesc& out_row_desc, int32_t*& projector, int64_t& projector_size)
{
  int ret = OB_SUCCESS;
  projector_size = dml_index_info.column_exprs_.count();
  projector = NULL;

  if (OB_ISNULL(projector = phy_plan_->alloc_projector(projector_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for projector failed", K(ret), K(projector_size));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < projector_size; i++) {
    const ObColumnRefRawExpr* dml_expr = dml_index_info.column_exprs_.at(i);
    if (OB_ISNULL(dml_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dml expr is null", K(ret));
    }
    int64_t offset = -1;
    for (int64_t j = 0; OB_SUCC(ret) && j < out_row_desc.get_column_num(); j++) {
      if (out_row_desc.get_column(j) == dml_expr) {
        offset = j;
        break;
      }
    }
    if (OB_UNLIKELY(-1 == offset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item not found", K(ret), KPC(dml_expr), K(out_row_desc), K(i));
    } else {
      projector[i] = offset;
    }
  }

  LOG_DEBUG("check projector", "projector", ObArrayWrap<int32_t>(projector, projector_size));
  return ret;
}

int ObCodeGeneratorImpl::convert_pdml_insert(ObLogInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObPxMultiPartInsert* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  const RowDesc* input_row_desc = child_ops.at(0).second;
  const int32_t* input_projector = NULL;
  int64_t input_projector_size = 0;
  ObPhyOperatorType op_type = PHY_PX_MULTI_PART_INSERT;
  ObInsertLogPlan* log_plan = NULL;

  if (OB_ISNULL(op.get_all_table_columns()) || op.get_all_table_columns()->empty() ||
      op.get_all_table_columns()->at(0).index_dml_infos_.empty() || 1 != child_ops.count() ||
      OB_ISNULL(op.get_table_columns()) || OB_ISNULL(op.get_column_convert_exprs()) ||
      OB_ISNULL(log_plan = static_cast<ObInsertLogPlan*>(op.get_plan()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid all table columns",
        K(ret),
        KPC(op.get_all_table_columns()),
        K(child_ops.count()),
        K(op.get_table_columns()),
        K(op.get_column_convert_exprs()));
  } else if (OB_FAIL(create_phy_op_desc(op_type, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("fail to create phy op desc", K(ret));
  } else if (OB_ISNULL(phy_op) || OB_ISNULL(out_row_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", K(ret), K(phy_op), K(out_row_desc));
  } else if (FALSE_IT((input_row_desc = child_ops.at(0).second,
                 input_projector = child_ops.at(0).first->get_projector(),
                 input_projector_size = child_ops.at(0).first->get_projector_size()))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(input_row_desc) || OB_ISNULL(input_projector) || input_projector_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected", K(ret), K(input_row_desc), K(input_projector), K(input_projector_size));
  } else if (OB_FAIL(copy_row_desc_by_projector(
                 *child_ops.at(0).second, input_projector, input_projector_size, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));

  } else {
    const IndexDMLInfo& dml_index_info = op.get_all_table_columns()->at(0).index_dml_infos_.at(0);

    phy_plan_->set_use_pdml(true);
    phy_op->set_table_location_uncertain(op.is_table_location_uncertain());
    phy_op->set_is_returning(op.pdml_is_returning());
    phy_op->set_table_id(dml_index_info.loc_table_id_);
    phy_op->set_index_tid(dml_index_info.index_tid_);
    phy_op->set_ignore(op.is_ignore());
    phy_op->set_is_pdml_index_maintain(op.is_index_maintenance());
    phy_plan_->set_ignore(op.is_ignore());
    if (OB_FAIL(add_table_column_ids(op, phy_op, *op.get_table_columns()))) {
      LOG_WARN("fail to add column id", K(ret));
    } else if (OB_FAIL(add_column_infos(op, *phy_op, *op.get_table_columns()))) {
      LOG_WARN("failed to add column infos", K(ret));
    } else if (OB_FAIL(add_column_conv_infos(*phy_op, *op.get_table_columns(), *op.get_column_convert_exprs()))) {
      LOG_WARN("failed to add column conv infos", K(ret));
    } else {
      int64_t partition_expr_idx = OB_INVALID_INDEX;
      if (OB_FAIL(get_pdml_partition_id_column_idx(*out_row_desc, partition_expr_idx))) {
        LOG_WARN("fail to get partition idx", K(ret));
      } else {
        phy_op->get_dml_row_desc().set_part_id_index(partition_expr_idx);
        phy_op->get_dml_table_desc().index_tid_ = dml_index_info.index_tid_;
        phy_op->get_dml_table_desc().partition_cnt_ = dml_index_info.part_cnt_;
      }
      LOG_TRACE("check pdml part expr",
          K(phy_op->get_dml_row_desc()),
          K(phy_op->get_dml_table_desc()),
          KPC(op.get_all_table_columns()));
    }

    if (OB_SUCC(ret) && op.is_index_maintenance()) {
      RowDesc extra_row_desc;
      if (OB_FAIL(extra_row_desc.init())) {
        LOG_WARN("failed to init extra row desc", K(ret));
      } else if (OB_FAIL(generate_insert_new_row_desc(&dml_index_info.column_exprs_,
                     dml_index_info.column_convert_exprs_,
                     *out_row_desc,
                     extra_row_desc))) {
      } else if (OB_FAIL(handle_pdml_shadow_pk(dml_index_info.column_exprs_, out_row_desc, &extra_row_desc, phy_op))) {
        LOG_WARN("failed to handle pdml shadow pk", K(ret), K(dml_index_info.column_exprs_));
      }
    }

    if (OB_SUCC(ret) && !op.is_index_maintenance()) {
      RowDesc extra_row_desc;
      if (OB_FAIL(convert_foreign_keys(op, *phy_op))) {
        LOG_WARN("failed to convert foreign keys", K(ret));
      } else if (OB_FAIL(extra_row_desc.init())) {
        LOG_WARN("failed to init extra row desc", K(ret));
      } else if (OB_FAIL(generate_insert_new_row_desc(&dml_index_info.column_exprs_,
                     dml_index_info.column_convert_exprs_,
                     *out_row_desc,
                     extra_row_desc))) {
        LOG_WARN("failed to generate insert new row desc", K(ret));
      } else if (OB_FAIL(add_compute(dml_index_info.column_convert_exprs_, *out_row_desc, &extra_row_desc, *phy_op))) {
        LOG_WARN("fail to add comput", K(ret));
      } else if (OB_FAIL(convert_check_constraint(op, *phy_op, extra_row_desc))) {
        LOG_WARN("failed to convert check constraints", K(ret));
      }
      LOG_TRACE("check extra_row_desc", K(extra_row_desc), K(dml_index_info.column_convert_exprs_));
    }

    if (OB_SUCC(ret)) {
      int32_t* projector = NULL;
      int64_t projector_size = 0;
      if (OB_FAIL(generate_pdml_insert_projector(dml_index_info, *out_row_desc, op, projector, projector_size))) {
        LOG_WARN("fail to generate pdml insert projector", K(ret));
      } else {
        phy_op->set_insert_projector(projector, projector_size);
      }
    }
  }

  // construct ObTableDMLParam
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_table_dml_param(op, *phy_op))) {
      LOG_ERROR("fail to convert table dml param", K(ret));
    }
  }

  return ret;
}

int ObCodeGeneratorImpl::convert_insert_index_info(
    ObLogInsertAll& op, ObMultiTableInsert& phy_op, const RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  ObAppend* append_op = NULL;
  ObSEArray<ObPhyOperator*, 8> subplan_roots;
  RowDesc table_row_desc;
  const ObIArray<TableColumns>* all_table_columns = NULL;
  if (OB_ISNULL(phy_plan_) || OB_ISNULL(all_table_columns = op.get_all_table_columns())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K_(phy_plan), K(all_table_columns), K(ret));
  } else if (OB_FAIL(phy_op.init_table_dml_info_array(all_table_columns->count()))) {
    LOG_WARN("init table insert info array failed", K(ret));
  } else if (OB_FAIL(phy_op.init_multi_table_insert_infos(all_table_columns->count()))) {
    LOG_WARN("failed to init multi table insert infos", K(ret));
  } else if (OB_FAIL(table_row_desc.init())) {
    LOG_WARN("init table row desc failed", K(ret));
  } else if (OB_FAIL(table_row_desc.assign(row_desc))) {
    LOG_WARN("assign table row desc failed", K(ret));
  } else if (OB_FAIL(phy_plan_->alloc_operator_by_type(PHY_APPEND, append_op))) {
    LOG_WARN("alloc operator by type failed", K(ret));
  } else {
    phy_op.set_subplan_root(append_op);
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < all_table_columns->count(); ++index) {
    const ObIArray<IndexDMLInfo>* index_dml_infos = &(all_table_columns->at(index).index_dml_infos_);
    const ObIArray<ObColumnRefRawExpr*>* table_columns =
        &(all_table_columns->at(index).index_dml_infos_.at(0).column_exprs_);
    const ObIArray<ObRawExpr*>& value_exprs = op.get_multi_value_exprs().at(index);
    ObTableDMLInfo table_dml_info;
    InsertTableInfo* insert_table_info = NULL;
    if (OB_ISNULL(index_dml_infos) || OB_ISNULL(table_columns) || OB_ISNULL(op.get_multi_column_convert_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "invalid argument", K(index_dml_infos), K(table_columns), K(op.get_multi_column_convert_exprs()), K(ret));
    } else if (OB_FAIL(InsertTableInfo::create_insert_table_info(phy_plan_->get_allocator(), insert_table_info))) {
      LOG_WARN("failed to create insert table info", K(ret));
    } else if (OB_ISNULL(insert_table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(insert_table_info), K(ret));
    } else if (OB_UNLIKELY(table_columns->count() != value_exprs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column count", K(table_columns->count()), K(value_exprs.count()));
    } else if (OB_FAIL(
                   table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), index_dml_infos->count()))) {
      LOG_WARN("failed to allocate array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < value_exprs.count(); ++i) {
      int64_t idx = OB_INVALID_INDEX;
      if (OB_FAIL(table_row_desc.get_idx(value_exprs.at(i), idx))) {
        LOG_WARN("get table column failed", K(ret));
      } else if (OB_FAIL(table_row_desc.replace_column(value_exprs.at(i), table_columns->at(i)))) {
        LOG_WARN("replace table column failed", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(convert_generated_column(*table_columns, phy_op, table_row_desc, insert_table_info))) {
      LOG_WARN("failed to convert generated column", K(ret));
    } else { /*do nothing*/
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos->count(); ++i) {
      const IndexDMLInfo& index_dml_info = index_dml_infos->at(i);
      ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
      phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
      phy_dml_info.index_tid_ = index_dml_info.index_tid_;
      phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
      if (OB_FAIL(phy_dml_info.table_locs_.allocate_array(phy_plan_->get_allocator(), 1))) {
        LOG_WARN("allocate table location array failed", K(ret));
      } else if (OB_FAIL(generate_index_location(index_dml_info, op, table_row_desc, phy_dml_info.table_locs_.at(0)))) {
        LOG_WARN("generate index table location failed", K(ret));
      } else if (OB_FAIL(phy_dml_info.dml_subplans_.allocate_array(phy_plan_->get_allocator(), 1))) {
        LOG_WARN("allocate dml subplan temp array failed", K(ret));
      } else if (OB_FAIL(
                     convert_insert_subplan(op, index_dml_info, table_row_desc, phy_dml_info.dml_subplans_.at(0)))) {
        LOG_WARN("convert insert op temps failed", K(ret));
      } else if (OB_ISNULL(phy_dml_info.dml_subplans_.at(0).subplan_root_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(phy_dml_info.dml_subplans_.at(0).subplan_root_));
        // only deal with main table. index table don't need to deal with it
      } else if (i == 0 && OB_FAIL(convert_check_constraint(op,
                               index,
                               phy_op,
                               *phy_dml_info.dml_subplans_.at(0).subplan_root_,
                               table_row_desc,
                               insert_table_info))) {
        LOG_WARN("failed to convert check constraints", K(ret));
      } else if (OB_FAIL(subplan_roots.push_back(phy_dml_info.dml_subplans_.at(0).subplan_root_))) {
        LOG_WARN("append child operator to append op failed", K(ret));
      } else if (0 == i && !op.get_part_hints().empty()) {
        // if multi partition insert values come from select clause, we can't calculate partitions
        // accurately in the generating plan phase, so delay the partition matching to the execution
        // phase
        const ObPartHint* part_hint = NULL;
        if (OB_FAIL(op.get_part_hint(part_hint, index_dml_info.table_id_))) {
          LOG_WARN("failed to get part hint", K(ret), K(index_dml_info.table_id_));
        } else if (part_hint == NULL) {
          /*do nothing*/
        } else if (OB_FAIL(phy_dml_info.table_locs_.at(0)->add_part_hint_ids(part_hint->part_ids_))) {
          LOG_WARN("add part hint ids failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_op.add_table_dml_info(index, table_dml_info))) {
        LOG_WARN("failed to add table dml info", K(ret));
      } else if (OB_FAIL(phy_op.add_multi_table_insert_infos(insert_table_info))) {
        LOG_WARN("failed to add multi table insert infos", K(ret));
      } else { /*do nothing */
      }
    }
  }
  if (OB_SUCC(ret) && !subplan_roots.empty()) {
    if (OB_FAIL(append_op->create_child_array(subplan_roots.count()))) {
      LOG_WARN("failed to create child array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
        if (OB_FAIL(append_op->set_child(static_cast<int32_t>(i), *subplan_roots.at(i)))) {
          LOG_WARN("failed to set child", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_insert_index_info(ObLogInsert& op, ObMultiPartInsert& phy_op, const RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  ObAppend* append_op = NULL;
  ObSEArray<ObPhyOperator*, 8> subplan_roots;
  ObTableDMLInfo table_dml_info;
  RowDesc table_row_desc;
  const ObIArray<IndexDMLInfo>* index_dml_infos = &(op.get_all_table_columns()->at(0).index_dml_infos_);
  const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
  const ObIArray<ObRawExpr*>& value_exprs = op.get_value_exprs();
  if (OB_ISNULL(index_dml_infos) || OB_ISNULL(table_columns) || OB_ISNULL(phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(index_dml_infos), K(table_columns), K_(phy_plan));
  } else if (OB_FAIL(phy_op.init_table_dml_info_array(1))) {
    LOG_WARN("init table insert info array failed", K(ret));
  } else if (OB_UNLIKELY(table_columns->count() != value_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column count", K(table_columns->count()), K(value_exprs.count()));
  } else if (OB_FAIL(table_row_desc.init())) {
    LOG_WARN("init table row desc failed", K(ret));
  } else if (OB_FAIL(table_row_desc.assign(row_desc))) {
    LOG_WARN("assign table row desc failed", K(ret));
  } else if (OB_FAIL(phy_plan_->alloc_operator_by_type(PHY_APPEND, append_op))) {
    LOG_WARN("alloc operator by type failed", K(ret));
  } else {
    phy_op.set_subplan_root(append_op);
  }
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), index_dml_infos->count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < value_exprs.count(); ++i) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(table_row_desc.get_idx(value_exprs.at(i), idx)) && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get table column failed", K(ret));
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(table_row_desc.replace_column(value_exprs.at(i), table_columns->at(i)))) {
        LOG_WARN("replace table column failed", K(ret), KPC(value_exprs.at(i)), KPC(table_columns->at(i)));
      }
    } else if (OB_FAIL(table_row_desc.add_column(table_columns->at(i)))) {
      LOG_WARN("add column to table row desc failed", K(ret), K(table_columns->at(i)));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos->count(); ++i) {
    const IndexDMLInfo& index_dml_info = index_dml_infos->at(i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    if (OB_FAIL(phy_dml_info.table_locs_.allocate_array(phy_plan_->get_allocator(), 1))) {
      LOG_WARN("allocate table location array failed", K(ret));
    } else if (OB_FAIL(generate_index_location(index_dml_info, op, table_row_desc, phy_dml_info.table_locs_.at(0)))) {
      LOG_WARN("generate index table location failed", K(ret));
    } else if (OB_FAIL(phy_dml_info.dml_subplans_.allocate_array(phy_plan_->get_allocator(), 1))) {
      LOG_WARN("allocate dml subplan temp array failed", K(ret));
    } else if (OB_FAIL(convert_insert_subplan(op, index_dml_info, table_row_desc, phy_dml_info.dml_subplans_.at(0)))) {
      LOG_WARN("convert insert op temps failed", K(ret));
    } else if (OB_FAIL(subplan_roots.push_back(phy_dml_info.dml_subplans_.at(0).subplan_root_))) {
      LOG_WARN("append child operator to append op failed", K(ret));
    } else if (0 == i && op.get_part_hint() != NULL) {
      // if multi partition insert values come from select clause, we can't calculate partitions accurately in
      // the generating plan phase, so delay the partition matching to the execution phase
      if (OB_FAIL(phy_dml_info.table_locs_.at(0)->add_part_hint_ids(op.get_part_hint()->part_ids_))) {
        LOG_WARN("add part hint ids failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !subplan_roots.empty()) {
    OZ(append_op->create_child_array(subplan_roots.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
    OZ(append_op->set_child(static_cast<int32_t>(i), *subplan_roots.at(i)));
  }
  OZ(phy_op.add_table_dml_info(0, table_dml_info));
  return ret;
}

int ObCodeGeneratorImpl::generate_multi_replace_row_desc(ObLogInsert& op, RowDesc& new_desc)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
  if (OB_ISNULL(table_columns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(table_columns));
  } else if (OB_FAIL(new_desc.init())) {
    LOG_WARN("init table row desc failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns->count(); ++i) {
    if (OB_FAIL(new_desc.add_column(table_columns->at(i)))) {
      LOG_WARN("add column to table row desc failed", K(ret), K(table_columns->at(i)));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_duplicate_key_scan_info(
    RowDesc& row_desc, ObLogicalOperator* log_scan_op, ObUniqueIndexScanInfo& scan_info)
{
  int ret = OB_SUCCESS;
  ObLogPlan *log_plan = NULL;
  ObPhyOperator *scan_root = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObDMLStmt *root_stmt = NULL;
  if (OB_ISNULL(log_scan_op)
      || OB_ISNULL(phy_plan_)
      || OB_ISNULL(log_plan = log_scan_op->get_plan())
      || OB_ISNULL(session_info = log_plan->get_optimizer_context().get_session_info())
      || OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard())
      || OB_ISNULL(root_stmt = log_plan->get_optimizer_context().get_root_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(log_scan_op), K(phy_plan_), K(log_plan), K(session_info), K(schema_guard));
  } else if (OB_UNLIKELY(!log_scan_op->is_duplicated_checker_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log operator isn't duplicated checker op", KPC(log_scan_op));
  } else if (OB_FAIL(convert_subplan_tree(*log_scan_op, scan_root))) {
    LOG_WARN("convert subplan tree failed", K(ret));
  } else {
    // generate rowkey expr
    ObSqlExpression* rowkey_expr = NULL;
    ObLogConflictRowFetcher* log_fetcher_op = static_cast<ObLogConflictRowFetcher*>(log_scan_op);
    const common::ObIArray<ObColumnRefRawExpr*>& conflict_exprs = log_fetcher_op->get_conflict_exprs();
    scan_info.conflict_column_cnt_ = conflict_exprs.count();
    scan_info.index_fetcher_op_ = scan_root;
    scan_info.table_id_ = log_fetcher_op->get_table_id();
    scan_info.index_tid_ = log_fetcher_op->get_index_tid();
    if (OB_FAIL(schema_guard->get_partition_cnt(scan_info.index_tid_, scan_info.part_cnt_))) {
      LOG_WARN("get partition cnt from schema guard failed", K(ret), K(scan_info));
    }
    // projection for conflick row checker expr
    // including global unique index's shadow pk
    for (int64_t i = 0; OB_SUCC(ret) && i < conflict_exprs.count(); ++i) {
      if (OB_FAIL(create_expression(scan_root->get_sql_expression_factory(), rowkey_expr))) {
        LOG_WARN("create rowkey sql expression failed", K(ret));
      } else if (OB_FAIL(generate_expr(*conflict_exprs.at(i), row_desc, *rowkey_expr))) {
        LOG_WARN("generate rowkey sql expression failed", K(ret), KPC(conflict_exprs.at(i)));
      } else if (OB_FAIL(ObSqlExpressionUtil::add_expr_to_list(scan_info.index_conflict_exprs_, rowkey_expr))) {
        LOG_WARN("fail to add expr to list", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // generate index location
    void* buf = NULL;
    if (OB_ISNULL(buf = phy_plan_->get_allocator().alloc(sizeof(ObTableLocation)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate table location buffer failed", K(ret), K(sizeof(ObTableLocation)));
    } else {
      scan_info.index_location_ = new (buf) ObTableLocation(phy_plan_->get_allocator());
      bool is_dml_table = root_stmt->check_table_be_modified(scan_info.index_tid_);
      if (OB_FAIL(scan_info.index_location_->init_table_location_with_rowkey(
                                            *schema_guard,
                                            scan_info.index_tid_,
                                            *session_info,
                                            is_dml_table))) {
        LOG_WARN("init index location failed", K(ret), KPC(log_scan_op));
      } else {
        scan_info.index_location_->set_table_id(scan_info.table_id_);
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_duplicate_key_checker(RowDesc& insert_row_desc, RowDesc& table_row_desc,
    ObLogDupKeyChecker& log_dupkey_checker, ObDuplicatedKeyChecker& phy_dupkey_checker)
{
  int ret = OB_SUCCESS;
  ObUniqueIndexScanInfo& table_scan_info = phy_dupkey_checker.get_table_scan_info();
  common::ObArrayWrap<ObUniqueIndexScanInfo>& gui_scan_infos = phy_dupkey_checker.get_gui_scan_infos();
  ObUniqueIndexScanInfo& gui_lookup_info = phy_dupkey_checker.get_gui_lookup_info();
  ObLogicalOperator* table_scan_root = log_dupkey_checker.get_table_scan_root();
  common::ObIArray<ObLogicalOperator*>& gui_scan_roots = log_dupkey_checker.get_gui_scan_roots();
  ObLogicalOperator* gui_lookup_root = log_dupkey_checker.get_gui_lookup_root();
  phy_dupkey_checker.set_unique_index_cnt(log_dupkey_checker.get_unique_index_cnt());
  phy_dupkey_checker.set_physical_plan(phy_plan_);
  if (OB_FAIL(convert_duplicate_key_scan_info(insert_row_desc, table_scan_root, table_scan_info))) {
    LOG_WARN("convert duplicate key scan info failed", K(ret));
  } else if (!gui_scan_roots.empty()) {
    ObAppend* append_op = NULL;
    if (OB_FAIL(convert_duplicate_key_scan_info(table_row_desc, gui_lookup_root, gui_lookup_info))) {
      LOG_WARN("convert duplicate key scan info failed", K(ret));
    } else if (OB_FAIL(phy_plan_->alloc_operator_by_type(PHY_APPEND, append_op))) {
      LOG_WARN("alloc operator by type failed", K(ret));
    } else if (OB_FAIL(gui_scan_infos.allocate_array(phy_plan_->get_allocator(), gui_scan_roots.count()))) {
      LOG_WARN("allocate gui scan info failed", K(ret), K(gui_scan_infos.count()));
    } else if (OB_FAIL(append_op->create_child_array(gui_scan_roots.count()))) {
      LOG_WARN("create child array failed", K(ret), K(gui_scan_roots.count()));
    } else {
      phy_dupkey_checker.set_gui_scan_root(append_op);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < gui_scan_roots.count(); ++i) {
      if (OB_FAIL(convert_duplicate_key_scan_info(insert_row_desc, gui_scan_roots.at(i), gui_scan_infos.at(i)))) {
        LOG_WARN("convert duplicate key scan info failed", K(ret));
      } else if (OB_FAIL(append_op->set_child(static_cast<int32_t>(i), *gui_scan_infos.at(i).index_fetcher_op_))) {
        LOG_WARN("set child op to append op failed", K(ret), K(i));
      }
    }
  }
  // convert unique constraint checker info
  if (OB_SUCC(ret)) {
    const ObIArray<ObUniqueConstraintInfo>* log_constraint_infos = log_dupkey_checker.get_constraint_infos();
    ObArrayWrap<ObPhyUniqueConstraintInfo>& phy_constraint_infos = phy_dupkey_checker.get_unique_constraint_infos();
    ObSqlExpressionFactory* sql_expr_factory = table_scan_info.index_fetcher_op_->get_sql_expression_factory();
    CK(OB_NOT_NULL(log_constraint_infos));
    OZ(phy_constraint_infos.allocate_array(phy_plan_->get_allocator(), log_constraint_infos->count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < log_constraint_infos->count(); ++i) {
      const ObIArray<ObColumnRefRawExpr*>& constraint_columns = log_constraint_infos->at(i).constraint_columns_;
      ObDList<ObSqlExpression>& phy_constraint_columns = phy_constraint_infos.at(i).constraint_columns_;
      if (OB_FAIL(ob_write_string(phy_plan_->get_allocator(),
              log_constraint_infos->at(i).constraint_name_,
              phy_constraint_infos.at(i).constraint_name_))) {
        LOG_WARN("write constraint name to phy constraint info failed", K(ret), K(log_constraint_infos->at(i)));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < constraint_columns.count(); ++j) {
        ObSqlExpression* rowkey_expr = NULL;
        OZ(create_expression(sql_expr_factory, rowkey_expr));
        OZ(generate_expr(*constraint_columns.at(j), table_row_desc, *rowkey_expr));
        OZ(ObSqlExpressionUtil::add_expr_to_list(phy_constraint_columns, rowkey_expr));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_multi_table_replace_info(
    RowDesc& table_row_desc, ObLogInsert& op, ObMultiTableReplace& phy_op)
{
  int ret = OB_SUCCESS;
  ObAppend* append_op = NULL;
  ObSEArray<ObPhyOperator*, 8> subplan_roots;
  ObTableDMLInfo table_dml_info;
  const ObIArray<IndexDMLInfo>* index_dml_infos = &(op.get_all_table_columns()->at(0).index_dml_infos_);
  if (OB_ISNULL(index_dml_infos) || OB_ISNULL(phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(index_dml_infos), K_(phy_plan));
  } else if (OB_FAIL(phy_op.init_table_dml_info_array(1))) {
    LOG_WARN("init table insert info array failed", K(ret));
  } else if (OB_FAIL(phy_plan_->alloc_operator_by_type(PHY_APPEND, append_op))) {
    LOG_WARN("alloc operator by type failed", K(ret));
  } else {
    phy_op.set_subplan_root(append_op);
  }
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), index_dml_infos->count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos->count(); ++i) {
    const IndexDMLInfo& index_dml_info = index_dml_infos->at(i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    DMLSubPlanArray& subplans = phy_dml_info.dml_subplans_;
    if (OB_FAIL(phy_dml_info.table_locs_.allocate_array(phy_plan_->get_allocator(), 1))) {
      LOG_WARN("allocate table location array failed", K(ret));
    } else if (OB_FAIL(generate_index_location(index_dml_info, op, table_row_desc, phy_dml_info.table_locs_.at(0)))) {
      LOG_WARN("generate index table location failed", K(ret));
    } else if (OB_FAIL(subplans.allocate_array(phy_plan_->get_allocator(), ObMultiTableReplace::DML_OP_CNT))) {
      LOG_WARN("allocate dml subplan temp array failed", K(ret));
    } else if (OB_FAIL(convert_delete_subplan(
                   op, index_dml_info, table_row_desc, subplans.at(ObMultiTableReplace::DELETE_OP)))) {
      LOG_WARN("convert delete subplan failed", K(ret));
    } else if (OB_FAIL(subplan_roots.push_back(subplans.at(ObMultiTableReplace::DELETE_OP).subplan_root_))) {
      LOG_WARN("append child operator to append op failed", K(ret));
    } else if (OB_FAIL(convert_insert_subplan(
                   op, index_dml_info, table_row_desc, subplans.at(ObMultiTableReplace::INSERT_OP)))) {
      LOG_WARN("convert insert op temps failed", K(ret));
    } else if (OB_FAIL(subplan_roots.push_back(subplans.at(ObMultiTableReplace::INSERT_OP).subplan_root_))) {
      LOG_WARN("append child operator to append op failed", K(ret));
    } else if (op.get_part_hint() != NULL) {
      if (OB_FAIL(phy_dml_info.table_locs_.at(0)->add_part_hint_ids(op.get_part_hint()->part_ids_))) {
        LOG_WARN("add part hint ids failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !subplan_roots.empty()) {
    OZ(append_op->create_child_array(subplan_roots.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
    OZ(append_op->set_child(static_cast<int32_t>(i), *subplan_roots.at(i)));
  }
  if (OB_SUCC(ret)) {
    OZ(phy_op.add_table_dml_info(0, table_dml_info));
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_global_index_update_info(ObLogDelUpd& op, const TableColumns& table_columns,
    const ObAssignments& assignments, const RowDesc& row_desc, common::ObIArray<ObPhyOperator*>& subplan_roots,
    ObTableDMLInfo& table_dml_info)
{
  int ret = OB_SUCCESS;
  RowDesc new_row_desc;
  ObSchemaGetterGuard* schema_guard = NULL;
  OZ(new_row_desc.init());
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), table_columns.index_dml_infos_.count()));
  if (OB_ISNULL(op.get_plan()) || OB_ISNULL(op.get_plan()->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null or schema guard is null");
  } else {
    schema_guard = op.get_plan()->get_optimizer_context().get_schema_guard();
  }
  table_dml_info.set_enable_row_movement(true);
  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.index_dml_infos_.count(); ++i) {
    new_row_desc.reset();
    bool is_global_index = (0 != i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    const IndexDMLInfo& index_dml_info = table_columns.index_dml_infos_.at(i);
    const ObTableSchema* table_schema = NULL;
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    DMLSubPlanArray& subplans = phy_dml_info.dml_subplans_;
    if (0 == i) {
      // primary index table
      table_dml_info.need_check_filter_null_ = index_dml_info.need_filter_null_;
      table_dml_info.rowkey_cnt_ = index_dml_info.rowkey_cnt_;
      table_dml_info.distinct_algo_ = index_dml_info.distinct_algo_;
    }

    if (share::is_oracle_mode()) {
      if (OB_FAIL(schema_guard->get_table_schema(index_dml_info.index_tid_, table_schema))) {
        LOG_WARN("get table schema failed", K(index_dml_info.index_tid_), K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(index_dml_info.index_tid_));
      } else if (table_schema->is_user_table() && !table_schema->is_enable_row_movement()) {
        table_dml_info.set_enable_row_movement(false);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(phy_dml_info.table_locs_.allocate_array(phy_plan_->get_allocator(), 2))) {
      LOG_WARN("allocate table location array failed", K(ret));
    } else if (OB_FAIL(generate_update_index_location(index_dml_info,
                   op,
                   assignments,
                   row_desc,
                   phy_dml_info.table_locs_.at(0),
                   phy_dml_info.table_locs_.at(1)))) {
      LOG_WARN("generate index table location failed", K(ret));
    } else if (OB_FAIL(subplans.allocate_array(phy_plan_->get_allocator(), ObMultiPartUpdate::DML_OP_CNT))) {
      LOG_WARN("allocate dml subplan temp array failed", K(ret));
    } else if (OB_FAIL(
                   convert_delete_subplan(op, index_dml_info, row_desc, subplans.at(ObMultiPartUpdate::DELETE_OP)))) {
      LOG_WARN("convert delete op temps failed", K(ret));
    } else if (OB_FAIL(subplan_roots.push_back(subplans.at(ObMultiPartUpdate::DELETE_OP).subplan_root_))) {
      LOG_WARN("append child operator to append op failed", K(ret));
    } else if (OB_FAIL(generate_update_new_row_desc(index_dml_info.assignments_, row_desc, new_row_desc))) {
      LOG_WARN("generate update new row desc failed", K(ret), K(row_desc), K_(index_dml_info.assignments));
    } else if (OB_FAIL(convert_insert_subplan(
                   op, index_dml_info, new_row_desc, subplans.at(ObMultiPartUpdate::INSERT_OP)))) {
      LOG_WARN("convert insert op temps failed", K(ret));
    } else if (OB_FAIL(subplan_roots.push_back(subplans.at(ObMultiPartUpdate::INSERT_OP).subplan_root_))) {
      LOG_WARN("append child operator to append op failed", K(ret));
    } else if (OB_FAIL(convert_update_subplan(
                   op, is_global_index, index_dml_info, row_desc, subplans.at(ObMultiPartUpdate::UPDATE_OP)))) {
      LOG_WARN("convert update op temps failed", K(ret));
    } else if (OB_FAIL(subplan_roots.push_back(subplans.at(ObMultiPartUpdate::UPDATE_OP).subplan_root_))) {
      LOG_WARN("append child operator to append op failed", K(ret));
    }
  }
  OZ(convert_multi_table_update_column_info(
      table_columns.index_dml_infos_.at(0), row_desc, table_dml_info.assign_columns_));
  return ret;
}

int ObCodeGeneratorImpl::convert_multi_table_update_column_info(
    const IndexDMLInfo& primary_index_info, const RowDesc& update_row_desc, ObAssignColumns& assign_columns)
{
  int ret = OB_SUCCESS;
  OZ(generate_projector(primary_index_info.column_exprs_,
      update_row_desc,
      assign_columns.old_projector_,
      assign_columns.old_projector_size_));
  OZ(convert_update_assignments(primary_index_info.column_exprs_,
      primary_index_info.assignments_,
      update_row_desc,
      assign_columns.old_projector_,
      assign_columns.old_projector_size_,
      assign_columns));
  return ret;
}

int ObCodeGeneratorImpl::convert_multi_table_update(ObLogUpdate& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObMultiPartUpdate* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  ObAppend* append_op = NULL;
  ObSEArray<ObPhyOperator*, 2> subplan_roots;
  ObTableDMLInfo table_dml_info;
  const RowDesc* input_row_desc = child_ops.at(0).second;
  const ObIArray<TableColumns>* all_table_columns = op.get_all_table_columns();
  const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
  const ObTablesAssignments* tas = op.get_tables_assignments();
  CK(OB_NOT_NULL(input_row_desc));
  CK(OB_NOT_NULL(all_table_columns));
  CK(OB_NOT_NULL(table_columns));
  CK(OB_NOT_NULL(tas));
  CK(all_table_columns->count() == tas->count());
  OZ(create_phy_op_desc(PHY_MULTI_PART_UPDATE, phy_op, out_row_desc, out_ops, op.get_op_id()));
  OZ(add_column_infos(op, *phy_op, *table_columns));
  OZ(phy_op->init_table_dml_info_array(all_table_columns->count()));
  OZ(phy_plan_->alloc_operator_by_type(PHY_APPEND, append_op));
  OZ(set_need_check_pk_is_null(op, phy_op));
  if (OB_SUCC(ret)) {
    phy_op->set_subplan_root(append_op);
  }
  if (OB_SUCC(ret) && op.get_stmt_id_expr() != nullptr) {
    int64_t stmt_id_idx = OB_INVALID_INDEX;
    if (OB_FAIL(input_row_desc->get_idx(op.get_stmt_id_expr(), stmt_id_idx))) {
      LOG_WARN("get index failed", K(ret), KPC(op.get_stmt_id_expr()));
    } else if (OB_INVALID_INDEX == stmt_id_idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt_id_idx is invalid", K(ret));
    } else {
      phy_op->set_stmt_id_idx(stmt_id_idx);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns->count(); ++i) {
    table_dml_info.reset();
    const TableColumns& table_columns = all_table_columns->at(i);
    const ObTableAssignment& ta = tas->at(i);
    OZ(convert_global_index_update_info(
        op, table_columns, ta.assignments_, *input_row_desc, subplan_roots, table_dml_info));
    OZ(phy_op->add_table_dml_info(i, table_dml_info));
  }
  if (OB_SUCC(ret) && !subplan_roots.empty()) {
    OZ(append_op->create_child_array(subplan_roots.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
    OZ(append_op->set_child(static_cast<int32_t>(i), *subplan_roots.at(i)));
  }
  if (OB_SUCC(ret) && op.is_returning()) {
    RowDesc new_row_desc;
    const TableColumns& table_columns = all_table_columns->at(0);
    if (OB_FAIL(new_row_desc.init())) {
      LOG_WARN("failed to init new row desc", K(ret));
    } else if (OB_FAIL(generate_update_new_row_desc(
                   table_columns.index_dml_infos_.at(0).assignments_, *input_row_desc, new_row_desc))) {
      LOG_WARN("failed to generate update row desc", K(ret));
    } else if (OB_FAIL(generate_returning_exprs(phy_op, &op.get_output_exprs(), &new_row_desc))) {
      LOG_WARN("failed to generate returing exprs", K(ret));
    } else {
      ARRAY_FOREACH(op.get_output_exprs(), i)
      {
        ObRawExpr* raw_expr = op.get_output_exprs().at(i);
        OZ(out_row_desc->add_column(raw_expr));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_global_index_delete_info(ObLogDelUpd& op, const TableColumns& table_columns,
    RowDesc& row_desc, common::ObIArray<ObPhyOperator*>& subplan_roots, ObTableDMLInfo& table_dml_info)
{
  int ret = OB_SUCCESS;
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), table_columns.index_dml_infos_.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.index_dml_infos_.count(); ++i) {
    const IndexDMLInfo& index_dml_info = table_columns.index_dml_infos_.at(i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    DMLSubPlanArray& subplans = phy_dml_info.dml_subplans_;
    if (0 == i) {
      table_dml_info.need_check_filter_null_ = index_dml_info.need_filter_null_;
      table_dml_info.rowkey_cnt_ = index_dml_info.rowkey_cnt_;
      table_dml_info.distinct_algo_ = index_dml_info.distinct_algo_;
    }
    if (OB_FAIL(phy_dml_info.table_locs_.allocate_array(phy_plan_->get_allocator(), 1))) {
      LOG_WARN("allocate table location array failed", K(ret));
    } else if (OB_FAIL(generate_index_location(index_dml_info, op, row_desc, phy_dml_info.table_locs_.at(0)))) {
      LOG_WARN("generate index table location failed", K(ret));
    } else if (OB_FAIL(subplans.allocate_array(phy_plan_->get_allocator(), ObMultiPartDelete::DML_OP_CNT))) {
      LOG_WARN("allocate dml subplan temp array failed", K(ret));
    } else if (OB_FAIL(
                   convert_delete_subplan(op, index_dml_info, row_desc, subplans.at(ObMultiPartDelete::DELETE_OP)))) {
      LOG_WARN("convert delete op temps failed", K(ret));
    } else if (OB_FAIL(subplan_roots.push_back(subplans.at(ObMultiPartDelete::DELETE_OP).subplan_root_))) {
      LOG_WARN("append child operator to append op failed", K(ret));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_multi_table_delete(ObLogDelete& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObAppend* append_op = NULL;
  RowDesc* out_row_desc = NULL;
  ObTableDMLInfo table_dml_info;
  ObMultiPartDelete* phy_op = NULL;
  ObSEArray<ObPhyOperator*, 2> subplan_roots;
  const ObIArray<TableColumns>* table_columns = op.get_all_table_columns();
  CK(OB_NOT_NULL(table_columns));
  if (OB_FAIL(create_phy_op_desc(PHY_MULTI_PART_DELETE, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_FAIL(copy_row_desc(*child_ops.at(0).second, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else if (OB_FAIL(phy_op->init_table_dml_info_array(table_columns->count()))) {
    LOG_WARN("init table delete info array failed", K(ret), K(table_columns->count()));
  } else if (OB_FAIL(phy_plan_->alloc_operator_by_type(PHY_APPEND, append_op))) {
    LOG_WARN("alloc operator by type failed", K(ret));
  } else {
    phy_op->set_subplan_root(append_op);
  }
  OZ(set_need_check_pk_is_null(op, phy_op));
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_all_table_columns()->count(); ++i) {
    table_dml_info.reset();
    const TableColumns& table_columns = op.get_all_table_columns()->at(i);
    OZ(convert_global_index_delete_info(op, table_columns, *out_row_desc, subplan_roots, table_dml_info));
    OZ(phy_op->add_table_dml_info(i, table_dml_info));
  }
  if (OB_SUCC(ret) && !subplan_roots.empty()) {
    OZ(append_op->create_child_array(subplan_roots.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
    OZ(append_op->set_child(static_cast<int32_t>(i), *subplan_roots.at(i)));
  }
  if (OB_SUCC(ret) && op.is_returning()) {
    if (OB_FAIL(generate_returning_exprs(phy_op, &op.get_output_exprs(), out_row_desc))) {
      LOG_WARN("failed to generate returning exprs", K(ret));

    } else {
      out_row_desc->reset();
      ARRAY_FOREACH(op.get_output_exprs(), i)
      {
        ObRawExpr* raw_expr = op.get_output_exprs().at(i);
        OZ(out_row_desc->add_column(raw_expr));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_index_location(
    const IndexDMLInfo& index_dml_info, ObLogDelUpd& op, RowDesc& row_desc, ObTableLocation*& index_location)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (OB_ISNULL(phy_plan_) || OB_ISNULL(op.get_plan()) || OB_ISNULL(op.get_stmt())) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan is null", K(ret), K_(phy_plan), K(op.get_plan()), K(op.get_stmt()));
  } else if (OB_ISNULL(op.get_plan()->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null");
  } else if (OB_ISNULL(buf = phy_plan_->get_allocator().alloc(sizeof(ObTableLocation)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate table location buffer failed", K(ret), K(sizeof(ObTableLocation)));
  } else {
    index_location = new (buf) ObTableLocation(phy_plan_->get_allocator());
    if (OB_FAIL(index_location->init_table_location(*op.get_plan()->get_optimizer_context().get_sql_schema_guard(),
            index_dml_info.loc_table_id_,
            index_dml_info.index_tid_,
            *op.get_stmt(),
            row_desc,
            true))) {
      LOG_WARN("init index location failed", K(ret), K(index_dml_info));
    }
  }
  return ret;
}

template <class T>
int ObCodeGeneratorImpl::construct_basic_row_desc(const ObIArray<T*>& basic_exprs, RowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(basic_exprs, i)
  {
    const T* expr = basic_exprs.at(i);
    OZ(row_desc.add_column(const_cast<T*>(expr)));
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_update_new_row_desc(
    const ObAssignments& assignments, const RowDesc& row_desc, RowDesc& new_row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_row_desc(row_desc, new_row_desc))) {
    LOG_WARN("copy row desc failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
    if (OB_FAIL(new_row_desc.swap_position(assignments.at(i).column_expr_, assignments.at(i).expr_))) {
      LOG_WARN("swap assignment expr position failed", K(ret), K(i), K(assignments));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_update_index_location(const IndexDMLInfo& index_info, ObLogDelUpd& op,
    const ObAssignments& assignments, const RowDesc& row_desc, ObTableLocation*& old_tbl_loc,
    ObTableLocation*& new_tbl_loc)
{
  int ret = OB_SUCCESS;
  RowDesc new_row_desc;
  if (OB_FAIL(new_row_desc.init())) {
    LOG_WARN("init new row desc failed", K(ret));
  } else if (OB_FAIL(generate_index_location(index_info, op, const_cast<RowDesc&>(row_desc), old_tbl_loc))) {
    LOG_WARN("generate old row location failed", K(ret), K(index_info), K(row_desc));
  } else if (OB_FAIL(generate_update_new_row_desc(assignments, row_desc, new_row_desc))) {
    LOG_WARN("generate update new row desc failed", K(ret));
  } else if (OB_FAIL(generate_index_location(index_info, op, new_row_desc, new_tbl_loc))) {
    LOG_WARN("generate new row location failed", K(ret), K(index_info), K(new_row_desc));
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_insert_new_row_desc(const ObIArray<ObColumnRefRawExpr*>* table_columns,
    const ObIArray<ObRawExpr*>& output_exprs, const RowDesc& orig_row_desc, RowDesc& extra_row_desc)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr*>& row_desc_exprs = orig_row_desc.get_columns();
  int64_t col_idx = -1;
  OV(OB_NOT_NULL(table_columns));
  OV(output_exprs.count() == table_columns->count());
  for (int i = 0; OB_SUCC(ret) && i < row_desc_exprs.count(); i++) {
    OV(OB_NOT_NULL(row_desc_exprs.at(i)));
    if (has_exist_in_array(output_exprs, row_desc_exprs.at(i), &col_idx)) {
      OV(0 <= col_idx && col_idx < table_columns->count(), OB_ERR_UNEXPECTED, col_idx);
      OV(OB_NOT_NULL(table_columns->at(col_idx)));
      OZ(extra_row_desc.add_column(table_columns->at(col_idx)));
    } else {
      OZ(extra_row_desc.add_column(row_desc_exprs.at(i)));
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < table_columns->count(); i++) {
    OV(OB_NOT_NULL(table_columns->at(i)));
    if (!has_exist_in_array(extra_row_desc.get_columns(), static_cast<ObRawExpr*>(table_columns->at(i)))) {
      OZ(extra_row_desc.add_column(table_columns->at(i)));
    }
  }
  return ret;
}

template <typename T>
int ObCodeGeneratorImpl::convert_common_dml_subplan(ObLogDelUpd& op, ObPhyOperatorType phy_op_type,
    const ObIArray<T*>& related_exprs, const IndexDMLInfo& index_dml_info, const RowDesc& table_row_desc,
    PhyOpsDesc& out_ops, DMLSubPlan& dml_subplan)
{
  int ret = OB_SUCCESS;
  ObTableModify* phy_op = NULL;
  ObPhyOperator* values_op = NULL;
  RowDesc* out_row_desc = NULL;
  CK(OB_NOT_NULL(phy_plan_));
  OZ(convert_index_values(index_dml_info.index_tid_, related_exprs, table_row_desc, out_ops, dml_subplan));
  CK(out_ops.count() == 1);
  CK(OB_NOT_NULL(values_op = out_ops.at(0).first));
  OZ(create_phy_op_desc(phy_op_type, phy_op, out_row_desc, out_ops, OB_INVALID_ID));
  OZ(phy_op->set_child(0, *values_op));
  OZ(copy_row_desc(*out_ops.at(0).second, *out_row_desc));
  if (OB_SUCC(ret)) {
    phy_op->set_table_id(index_dml_info.loc_table_id_);
    phy_op->set_index_tid(index_dml_info.index_tid_);
    phy_op->set_from_multi_table_dml(true);
    phy_op->set_column_count(out_row_desc->get_column_num());
    phy_op->set_ignore(op.is_ignore());
    OZ(add_column_infos(op, *phy_op, index_dml_info.column_exprs_));
    dml_subplan.subplan_root_ = phy_op;
    const ObIArray<ObColumnRefRawExpr*>& table_columns = index_dml_info.column_exprs_;
    OZ(add_table_column_ids(op, phy_op, table_columns, index_dml_info.rowkey_cnt_));
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_insert_subplan(ObLogDelUpd& op,  // no const, see convert_foreign_keys.
    const IndexDMLInfo& index_dml_info, const RowDesc& table_row_desc, DMLSubPlan& dml_subplan)
{
  int ret = OB_SUCCESS;
  PhyOpsDesc out_ops;
  CK(OB_NOT_NULL(phy_plan_));
  if (OB_SUCC(ret)) {
    phy_plan_->set_ignore(op.is_ignore());
  }
  OZ(convert_common_dml_subplan(
      op, PHY_INSERT, index_dml_info.column_exprs_, index_dml_info, table_row_desc, out_ops, dml_subplan));
  CK(out_ops.count() == 2);
  if (OB_SUCC(ret)) {
    RowDesc* out_row_desc = out_ops.at(1).second;
    ObPhyOperator* dml_op = out_ops.at(1).first;
    OZ(add_projector(index_dml_info.column_exprs_, *out_row_desc, *dml_op));
  }
  for (int64_t i = 0; i < out_ops.count(); ++i) {
    if (out_ops.at(i).second != NULL) {
      ob_delete(out_ops.at(i).second);
      out_ops.at(i).second = NULL;
    }
  }
  if (OB_SUCC(ret) && (log_op_def::LOG_INSERT == op.get_type() || log_op_def::LOG_INSERT_ALL == op.get_type())) {
    if (OB_FAIL(convert_foreign_keys(op, *dml_subplan.subplan_root_))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    }
  }
  if (OB_SUCC(ret) && (log_op_def::LOG_INSERT == op.get_type() || log_op_def::LOG_INSERT_ALL == op.get_type())) {
    if (OB_FAIL(convert_table_dml_param(op, *dml_subplan.subplan_root_))) {
      LOG_WARN("fail to convert table dml param", K(ret));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_update_subplan(ObLogDelUpd& op,  // no const, see convert_foreign_keys.
    bool is_global_index, const IndexDMLInfo& index_dml_info, const RowDesc& table_row_desc, DMLSubPlan& dml_subplan)
{
  int ret = OB_SUCCESS;
  ObTableUpdate* phy_op = NULL;
  PhyOpsDesc out_ops;
  RowDesc* out_row_desc = NULL;
  ObSEArray<ObRawExpr*, 8> update_related_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.column_exprs_.count(); ++i) {
    if (OB_FAIL(update_related_exprs.push_back(index_dml_info.column_exprs_.at(i)))) {
      LOG_WARN("store index dml info column expr failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.assignments_.count(); ++i) {
    if (OB_FAIL(update_related_exprs.push_back(index_dml_info.assignments_.at(i).expr_))) {
      LOG_WARN("store assignment expr failed", K(ret));
    }
  }
  OZ(convert_common_dml_subplan(
      op, PHY_UPDATE, update_related_exprs, index_dml_info, table_row_desc, out_ops, dml_subplan));
  CK(out_ops.count() == 2);
  CK(OB_NOT_NULL(phy_plan_));
  if (OB_SUCC(ret)) {
    phy_op = static_cast<ObTableUpdate*>(out_ops.at(1).first);
    out_row_desc = out_ops.at(1).second;
    OZ(convert_update_assignments(index_dml_info.column_exprs_,
        index_dml_info.assignments_,
        *out_row_desc,
        out_ops.at(0).first->get_projector(),
        out_ops.at(0).first->get_projector_size(),
        *phy_op));
  }
  if (OB_SUCC(ret)) {
    phy_plan_->set_ignore(op.is_ignore());
    phy_op->set_is_global_index(is_global_index);
    phy_op->set_column_count(out_row_desc->get_column_num());
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_foreign_keys(op, *phy_op))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    RowDesc new_row_desc;
    if (OB_FAIL(new_row_desc.init())) {
      LOG_WARN("failed to init extra row desc", K(ret));
    } else if (OB_FAIL(generate_update_new_row_desc(index_dml_info.assignments_, table_row_desc, new_row_desc))) {
      LOG_WARN("failed to generate update new row desc", K(ret));
    } else if (OB_FAIL(convert_check_constraint(op, *phy_op, new_row_desc))) {
      LOG_WARN("failed to convert check constraints", K(ret));
    } else if (FALSE_IT(new_row_desc.reset())) {
    } else if (OB_FAIL(generate_update_new_row_desc(index_dml_info.assignments_, *out_row_desc, new_row_desc))) {
      LOG_WARN("failed to generate update new row desc", K(ret));
    }
    // convert new shadow pk project expr
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.column_exprs_.count(); ++i) {
      const ObColumnRefRawExpr* col_expr = index_dml_info.column_exprs_.at(i);
      int64_t result_idx = OB_INVALID_INDEX;
      if (OB_ISNULL(col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret));
      } else if (is_shadow_column(col_expr->get_column_id())) {
        const ObRawExpr* spk_raw_expr = col_expr->get_dependant_expr();
        ObColumnExpression* new_spk_expr = nullptr;
        if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), new_spk_expr))) {
          LOG_WARN("create expression failed", K(ret));
        } else if (OB_FAIL(generate_expr(const_cast<ObRawExpr&>(*spk_raw_expr), new_row_desc, *new_spk_expr))) {
          LOG_WARN("failed to generate sql expression", K(ret));
        } else if (OB_FAIL(new_row_desc.get_idx(col_expr, result_idx))) {
          LOG_WARN("get column index failed", K(ret), KPC(col_expr), K(new_row_desc));
        } else if (OB_FAIL(phy_op->add_new_spk_expr(new_spk_expr))) {
          LOG_WARN("add new spk expr failed", K(ret), KPC(new_spk_expr));
        } else {
          new_spk_expr->set_result_index(result_idx);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_table_dml_param(op, *phy_op))) {
      LOG_WARN("fail to convert table dml param", K(ret));
    }
  }
  // release out ops must be last
  for (int64_t i = 0; i < out_ops.count(); ++i) {
    if (out_ops.at(i).second != NULL) {
      ob_delete(out_ops.at(i).second);
      out_ops.at(i).second = NULL;
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_delete_subplan(ObLogDelUpd& op,  // no const, see convert_foreign_keys.
    const IndexDMLInfo& index_dml_info, const RowDesc& table_row_desc, DMLSubPlan& dml_subplan)
{
  int ret = OB_SUCCESS;
  PhyOpsDesc out_ops;
  CK(OB_NOT_NULL(phy_plan_));
  if (OB_SUCC(ret)) {
    phy_plan_->set_ignore(op.is_ignore());
  }
  OZ(convert_common_dml_subplan(
      op, PHY_DELETE, index_dml_info.column_exprs_, index_dml_info, table_row_desc, out_ops, dml_subplan));
  CK(out_ops.count() == 2);
  if (OB_SUCC(ret)) {
    RowDesc* out_row_desc = out_ops.at(1).second;
    ObPhyOperator* dml_op = out_ops.at(1).first;
    OZ(add_projector(index_dml_info.column_exprs_, *out_row_desc, *dml_op));
  }
  for (int64_t i = 0; i < out_ops.count(); ++i) {
    if (out_ops.at(i).second != NULL) {
      ob_delete(out_ops.at(i).second);
      out_ops.at(i).second = NULL;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(convert_foreign_keys(op, *dml_subplan.subplan_root_))) {
    LOG_WARN("failed to convert foreign keys", K(ret));
  }
  if (OB_SUCC(ret) && log_op_def::LOG_DELETE == op.get_type()) {
    if (OB_FAIL(convert_table_dml_param(op, *dml_subplan.subplan_root_))) {
      LOG_WARN("fail to convert table dml param", K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObCodeGeneratorImpl::convert_index_values(uint64_t table_id, const ObIArray<T*>& output_exprs,
    const RowDesc& table_row_desc, PhyOpsDesc& out_ops, DMLSubPlan& dml_subplan)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> access_exprs;
  ObArray<ObRawExpr*> calc_exprs;
  ObTableRowStore* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_exprs.count(); i++) {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(table_row_desc.get_idx(output_exprs.at(i), idx)) && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_ISNULL(output_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("output expr is null");
      } else if (output_exprs.at(i)->is_column_ref_expr()) {
        const ObColumnRefRawExpr* col_expr = static_cast<const ObColumnRefRawExpr*>(output_exprs.at(i));
        if (col_expr->is_generated_column()) {
          if (OB_FAIL(calc_exprs.push_back(const_cast<ObColumnRefRawExpr*>(col_expr)))) {
            LOG_WARN("add calc expr failed", K(ret), KPC(col_expr));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column expr don't found in row desc and not virtual column", KPC(col_expr));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr don't found in row desc and not column ref", KPC(output_exprs.at(i)));
      }
    } else if (OB_FAIL(access_exprs.push_back(output_exprs.at(i)))) {
      LOG_WARN("store output expr to access exprs failed", K(ret), KPC(output_exprs.at(i)));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_projector(
            access_exprs, table_row_desc, dml_subplan.value_projector_, dml_subplan.value_projector_size_))) {
      LOG_WARN("generate delete index value projector failed", K(ret), K(access_exprs), K(table_row_desc));
    } else if (OB_FAIL(create_phy_op_desc(PHY_TABLE_ROW_STORE, phy_op, out_row_desc, out_ops, OB_INVALID_ID))) {
      LOG_WARN("failed to create values op and desc", K(ret));
    } else if (OB_FAIL(copy_row_desc_by_projector(
                   table_row_desc, dml_subplan.value_projector_, dml_subplan.value_projector_size_, *out_row_desc))) {
      LOG_WARN("copy row desc by projector failed", K(ret));
    } else if (OB_FAIL(add_compute(calc_exprs, *out_row_desc, NULL, *phy_op))) {
      LOG_WARN("add compute expr to values op failed", K(ret));
    } else if (OB_FAIL(add_projector(output_exprs, *out_row_desc, *phy_op))) {
      LOG_WARN("add projector to values op failed", K(ret));
    } else {
      phy_op->set_table_id(table_id);
      phy_op->set_column_count(out_row_desc->get_column_num());
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_table_lookup(ObLogTableLookup& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObTableLookup* table_lookup = NULL;
  RowDesc* out_row_desc = NULL;
  PhyOpsDesc table_scan_child_ops;
  PhyOpsDesc table_scan_out_ops;
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSQLSessionInfo *my_session = NULL;
  ObDMLStmt *root_stmt = NULL;
  if (OB_ISNULL(op.get_index_back_scan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table scan operator", K(ret));
  } else if (OB_FAIL(convert_table_scan(*op.get_index_back_scan(), table_scan_child_ops, table_scan_out_ops, true))) {
    LOG_WARN("failed to convert table scan", K(ret));
  } else if (OB_UNLIKELY(table_scan_out_ops.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have one operator", K(table_scan_out_ops.count()), K(ret));
  } else if (OB_ISNULL(table_scan_out_ops.at(0).first) || OB_ISNULL(table_scan_out_ops.at(0).second)) {
    LOG_WARN("get unexpected null", K(table_scan_out_ops.at(0).first), K(table_scan_out_ops.at(0).second), K(ret));
  } else if (OB_FAIL(convert_common_parts(
                 *op.get_index_back_scan(), *table_scan_out_ops.at(0).second, *table_scan_out_ops.at(0).first))) {
    LOG_WARN("failed to convert commom parts", K(ret));
  } else if (OB_FAIL(create_phy_op_desc(PHY_TABLE_LOOKUP, table_lookup, out_row_desc, out_ops, OB_INVALID_ID))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(table_lookup)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(table_lookup));
  } else if (OB_FAIL(copy_row_desc(*table_scan_out_ops.at(0).second, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else if (OB_ISNULL(op.get_plan()) ||
             OB_ISNULL(schema_guard = op.get_plan()->get_optimizer_context().get_schema_guard()) ||
             OB_ISNULL(root_stmt = op.get_plan()->get_optimizer_context().get_root_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected null", K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_ref_table_id(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(my_session = op.get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null session", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(ret));
  } else {
    ObLookupInfo& lookup_info = table_lookup->get_lookup_info();
    table_lookup->set_remote_plan(table_scan_out_ops.at(0).first);
    lookup_info.table_id_ = op.get_table_id();
    lookup_info.ref_table_id_ = op.get_ref_table_id();
    lookup_info.partition_cnt_ = table_schema->get_partition_cnt();
    lookup_info.partition_num_ = table_schema->get_all_part_num();
    lookup_info.is_old_no_pk_table_ = table_schema->is_old_no_pk_table();  // only for old no-pk table
    uint64_t fetch_tenant_id = extract_tenant_id(lookup_info.ref_table_id_);
    if (is_sys_table(lookup_info.ref_table_id_) || is_fake_table(lookup_info.ref_table_id_)) {
      fetch_tenant_id = OB_SYS_TENANT_ID;
    }
    if (OB_FAIL(schema_guard->get_schema_version(fetch_tenant_id, lookup_info.schema_version_))) {
      LOG_WARN("fail to get schema version", K(ret), K(lookup_info));
    }
    // for parititon id getter
    if (OB_SUCC(ret)) {
      ObTableLocation& partition_id_getter = table_lookup->get_part_id_getter();
      // the other function may be more effective, TODO
      bool is_dml_table = root_stmt->check_table_be_modified(lookup_info.table_id_);
      if (OB_FAIL(partition_id_getter.init_table_location_with_row_desc(
          *op.get_plan()->get_optimizer_context().get_sql_schema_guard(),
          lookup_info.ref_table_id_,
          *child_ops.at(0).second,
          *my_session,
          is_dml_table))) {
        LOG_WARN("the partition id init failed", K(ret));
      } else {
        partition_id_getter.set_table_id(op.get_table_id());
      }
    }
  }
#ifndef NDEBUG
  if (OB_SUCC(ret)) {
    // debug
    LOG_DEBUG("operator converted", K(op.get_index_back_scan()->get_name()), K(*op.get_index_back_scan()));
  }
#endif
  // in any case, destroy row desc
  if (table_scan_out_ops.count() > 0 && NULL != table_scan_out_ops.at(0).second) {
    ob_delete(table_scan_out_ops.at(0).second);
  }
  return ret;
}

int ObCodeGeneratorImpl::create_identity_projector(ObPhyOperator& op, const int64_t projector_size)
{
  int ret = OB_SUCCESS;
  int32_t* projector = NULL;
  if (NULL == (projector = phy_plan_->alloc_projector(projector_size))) {
    LOG_WARN("no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t i = 0; i < projector_size; ++i) {
      projector[i] = static_cast<int32_t>(i);
    }
    op.set_projector(projector, projector_size);
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_expr_values(ObLogExprValues& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  RowDesc* out_row_desc = NULL;
  if (0 == child_ops.count()) {
    ObExprValues* phy_op = NULL;
    if (OB_FAIL(create_phy_op_desc(PHY_EXPR_VALUES, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
      LOG_WARN("failed to create phy op and desc", K(ret));
    } else if (OB_ISNULL(phy_op) || OB_ISNULL(out_row_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("creaet phy op fail", K(phy_op), K(out_row_desc));
    } else if (OB_FAIL(convert_expr_values(*phy_op, *out_row_desc, op, child_ops, out_ops))) {
      LOG_WARN("fail convert expr values", K(ret));
    }
  } else if (1 == child_ops.count()) {
    ObExprValuesWithChild* phy_op = NULL;
    if (OB_FAIL(create_phy_op_desc(PHY_EXPR_VALUES_WITH_CHILD, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
      LOG_WARN("failed to create phy op and desc", K(ret));
    } else if (OB_ISNULL(phy_op) || OB_ISNULL(out_row_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("creaet phy op fail", K(phy_op), K(out_row_desc));
    } else if (OB_FAIL(convert_expr_values(*phy_op, *out_row_desc, op, child_ops, out_ops))) {
      LOG_WARN("fail convert expr values", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  }
  return ret;
}

template <typename T>
int ObCodeGeneratorImpl::convert_expr_values(
    T& phy_op, RowDesc& out_row_desc, ObLogExprValues& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  UNUSED(child_ops);
  UNUSED(out_ops);
  common::ObIArray<ObRawExpr*>& output_exprs = op.get_output_exprs();
  phy_op.set_column_count(output_exprs.count());
  if (op.is_need_columnlized()) {
    ARRAY_FOREACH(output_exprs, i)
    {
      ObRawExpr* raw_expr = output_exprs.at(i);
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid raw expr", K(i), K(output_exprs));
      } else if (OB_FAIL(out_row_desc.add_column(raw_expr))) {
        LOG_WARN("failed to add column desc", K(ret), K(i));
      }
    }
  }
  const common::ObIArray<ObRawExpr*>& values_expr = op.get_value_exprs();
  RowDesc* row_desc = &out_row_desc;
  if (OB_SUCC(ret)) {
    bool use_range_param = false;
    if (OB_FAIL(op.check_range_param_continuous(use_range_param))) {
      LOG_WARN("check expr values whther use range param failed", K(ret));
    } else if (use_range_param && PHY_EXPR_VALUES == phy_op.get_type()) {
      ObExprOperatorFactory expr_factory(phy_plan_->get_allocator());
      int64_t row_cnt = op.get_value_exprs().count() / op.get_output_exprs().count();
      if (OB_FAIL(phy_op.init_range_params(row_cnt))) {
        LOG_WARN("init range params failed", K(ret), K(row_cnt));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
        int64_t param_idx_start = OB_INVALID_INDEX;
        int64_t param_idx_end = OB_INVALID_INDEX;
        ObIterExprRangeParam* range_expr = NULL;
        if (OB_FAIL(op.get_value_param_range(i, param_idx_start, param_idx_end))) {
          LOG_WARN("get value param range failed", K(ret), K(i), K(row_cnt));
        } else if (OB_FAIL(expr_factory.alloc(T_OP_RANGE_PARAM, range_expr))) {
          LOG_WARN("allocate range param expr failed", K(ret));
        } else if (OB_FAIL(phy_op.add_range_param(range_expr))) {
          LOG_WARN("add range param to expr value op failed", K(ret), KPC(range_expr));
        } else {
          range_expr->set_param_range(param_idx_start, param_idx_end);
        }
      }
    } else if (OB_FAIL(phy_op.init_value_count(values_expr.count()))) {
      LOG_WARN("init value count failed", K(ret));
    } else {
      ARRAY_FOREACH(values_expr, i)
      {
        ObRawExpr* raw_expr = values_expr.at(i);
        ObSqlExpression* expr = NULL;
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid raw expr", K(i), K(values_expr));
        } else if (OB_FAIL(create_expression(phy_op.get_sql_expression_factory(), expr))) {
          LOG_WARN("failed to create expr");
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("alloc invalid expr", K(ret), K(expr));
        } else if (OB_FAIL(generate_expr(*raw_expr, *row_desc, *expr))) {
          LOG_WARN("failed to generate expr", K(ret), K(i), K(*raw_expr));
        } else if (OB_FAIL(phy_op.add_value(expr))) {
          LOG_WARN("failed to add filter", K(ret), K(i), K(*expr));
        } else if (!op.is_need_columnlized()) {  // select 1, 2, 3
          LOG_DEBUG("add value expr", KPC(raw_expr), KPC(expr));
          if (OB_FAIL(out_row_desc.add_column(raw_expr))) {
            LOG_WARN("failed to add column desc", K(ret), K(i));
          }
        }
      }  // end for
    }

    if (OB_SUCC(ret) && 0 != phy_op.get_column_count()) {
      phy_op.set_exact_rows(true);
      phy_op.set_rows(phy_op.get_size() / phy_op.get_column_count());
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_values(ObLogValues& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObValues* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc empty_row_desc;
  if (0 != child_ops.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (OB_FAIL(create_phy_op_desc(PHY_VALUES, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_FAIL(phy_op->set_row_store(op.get_row_store()))) {
    LOG_WARN("failed to set row store for VALUES operator", K(ret));
  } else {
    phy_op->set_column_count(op.get_col_count());
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_material(ObLogMaterial& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  UNUSED(op);
  ObMaterial* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  const int32_t* projector = NULL;
  int64_t projector_size = 0;
  RowDesc* input_row_desc = NULL;
  if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (FALSE_IT((input_row_desc = child_ops.at(0).second,
                 projector = child_ops.at(0).first->get_projector(),
                 projector_size = child_ops.at(0).first->get_projector_size()))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(create_phy_op_desc(PHY_MATERIAL, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_FAIL(copy_row_desc_by_projector(*input_row_desc, projector, projector_size, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else {
  }
  return ret;
}

template <class UpdateOp>
int ObCodeGeneratorImpl::convert_update_assignments(const ObIArray<ObColumnRefRawExpr*>& all_columns,
    const ObAssignments& assigns, const jit::expr::ObColumnIndexProvider& assign_expr_desc,
    const int32_t* old_projector, int64_t old_projector_size, UpdateOp& up_op)
{
  int ret = OB_SUCCESS;
  int32_t* projector = NULL;
  RowDesc update_column_desc;
  ObSEArray<ObColumnRefRawExpr*, 16> shadow_pk_exprs;
  CK(OB_NOT_NULL(old_projector));
  CK(all_columns.count() <= old_projector_size);
  if (OB_ISNULL(projector = phy_plan_->alloc_projector(all_columns.count()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory to create projector", K(ret), K(all_columns.count()));
  } else if (OB_FAIL(update_column_desc.init())) {
    LOG_WARN("init update column desc failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_columns.count(); ++i) {
    ObColumnRefRawExpr* target_column = all_columns.at(i);
    projector[i] = old_projector[i];
    if (OB_ISNULL(target_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target column is null", K(ret), KPC(target_column));
    } else if (OB_FAIL(update_column_desc.add_column(target_column))) {
      LOG_WARN("add column to update column desc failed", K(ret), KPC(target_column), K(update_column_desc));
    } else if (is_shadow_column(target_column->get_column_id())) {
      if (OB_FAIL(shadow_pk_exprs.push_back(target_column))) {
        LOG_WARN("add target column to shadow pk exprs failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t update_column_cnt = assigns.count() + shadow_pk_exprs.count();
    if (OB_FAIL(up_op.init_updated_column_count(phy_plan_->get_allocator(), update_column_cnt))) {
      SQL_CG_LOG(WARN, "fail to init updated column count", K(ret), K(update_column_cnt));
    }
  }
  ARRAY_FOREACH(assigns, i)
  {
    ObColumnRefRawExpr* target_column = assigns.at(i).column_expr_;
    int64_t update_col_idx = OB_INVALID_INDEX;
    int64_t projector_idx = OB_INVALID_INDEX;
    if (OB_FAIL(update_column_desc.get_idx(target_column, update_col_idx)) ||
        OB_FAIL(assign_expr_desc.get_idx(assigns.at(i).expr_, projector_idx))) {
      LOG_WARN("get index failed", K(ret));
    } else if (OB_UNLIKELY(update_col_idx < 0 || update_col_idx >= all_columns.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index val", K(update_col_idx), K(all_columns.count()), K(ret));
    } else {
      projector[update_col_idx] = static_cast<int32_t>(projector_idx);
      OZ(up_op.set_updated_column_info(i,
          assigns.at(i).base_column_id_,
          update_col_idx,
          target_column->get_result_type().has_result_flag(OB_MYSQL_ON_UPDATE_NOW_FLAG)));
    }
  }  // end for i
  // handle shadow pk expr, shadow primary key will updated when pk was updated
  ARRAY_FOREACH(shadow_pk_exprs, i)
  {
    ObColumnRefRawExpr* spk_column = shadow_pk_exprs.at(i);
    int64_t update_col_idx = OB_INVALID_INDEX;
    if (OB_FAIL(update_column_desc.get_idx(spk_column, update_col_idx))) {
      LOG_WARN("get index from update column desc failed", K(ret), KPC(spk_column));
    } else if (OB_FAIL(up_op.set_updated_column_info(
                   assigns.count() + i, spk_column->get_column_id(), update_col_idx, false))) {
      LOG_WARN("set updated column info failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    up_op.set_updated_projector(projector, update_column_desc.get_column_num());
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_select_into(ObLogSelectInto& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObSelectInto* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc* input_row_desc = NULL;
  if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (FALSE_IT(input_row_desc = child_ops.at(0).second)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(create_phy_op_desc(PHY_SELECT_INTO, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(input_row_desc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(input_row_desc), K(phy_op));
  } else if (OB_FAIL(copy_row_desc(*input_row_desc, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else if (OB_FAIL(phy_op->set_outfile_name(op.get_outfile_name(), phy_plan_->get_allocator()))) {
    LOG_WARN("fail to set outfile name", K(op.get_outfile_name()), K(ret));
  } else if (OB_FAIL(phy_op->set_user_vars(op.get_user_vars(), phy_plan_->get_allocator()))) {
    LOG_WARN("fail to set user vars", K(op.get_user_vars()), K(ret));
  } else if (OB_FAIL(phy_op->set_filed_str(op.get_filed_str(), phy_plan_->get_allocator()))) {
    LOG_WARN("fail to set filed str", K(op.get_filed_str()), K(ret));
  } else if (OB_FAIL(phy_op->set_line_str(op.get_line_str(), phy_plan_->get_allocator()))) {
    LOG_WARN("fail to set line str", K(op.get_line_str()), K(ret));
  } else {
    phy_op->set_into_type(op.get_into_type());
    phy_op->set_closed_cht(op.get_closed_cht());
    phy_op->set_is_optional(op.get_is_optional());
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_topk(ObLogTopk& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObTopK* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc* input_row_desc = NULL;
  if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (FALSE_IT(input_row_desc = child_ops.at(0).second)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(create_phy_op_desc(PHY_TOPK, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(input_row_desc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(input_row_desc), K(phy_op));
  } else if (OB_FAIL(copy_row_desc(*input_row_desc, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else {
    ObSqlExpression* limit_expr = NULL;
    ObSqlExpression* offset_expr = NULL;
    ObRawExpr* limit_raw_expr = op.get_topk_limit_count();
    ObRawExpr* offset_raw_expr = op.get_topk_limit_offset();

    if (OB_ISNULL(limit_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limit_raw_expr should not be NULL", K(ret));
    } else if (OB_FAIL(generate_sql_expr(
                   phy_op->get_sql_expression_factory(), limit_raw_expr, *child_ops.at(0).second, limit_expr))) {
      LOG_WARN("Generation of sql expression fails", K(ret));
    }

    if (OB_SUCC(ret) && NULL != offset_raw_expr) {
      if (OB_FAIL(generate_sql_expr(
              phy_op->get_sql_expression_factory(), offset_raw_expr, *child_ops.at(0).second, offset_expr))) {
        LOG_WARN("Generation of sql expression fails", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(
              phy_op->set_topk_params(limit_expr, offset_expr, op.get_minimum_row_count(), op.get_topk_precision()))) {
        LOG_WARN("failed to set topk params", K(ret));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_append(ObLogAppend& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;

  ObPhyOperatorType phy_op_type = PHY_APPEND;
  ObAppend* phy_append_op = NULL;
  RowDesc* out_row_desc = NULL;
  if (child_ops.count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (OB_FAIL(create_phy_op_desc(phy_op_type, phy_append_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObIArray<ObRawExpr*>& out_exprs = op.get_output_exprs();
    ARRAY_FOREACH(out_exprs, i)
    {
      ObRawExpr* out_expr = out_exprs.at(i);
      if (OB_FAIL(out_row_desc->add_column(out_expr))) {
        LOG_WARN("add column to out row desc failed", K(ret));
      } else { /*do nothing*/
      }
    }  // end for
  }    // end if
  return ret;
}

int ObCodeGeneratorImpl::convert_count(ObLogCount& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObCount* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc* input_row_desc = NULL;
  if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong # of childern", K(ret), K(child_ops.count()));
  } else if (FALSE_IT(input_row_desc = child_ops.at(0).second)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(create_phy_op_desc(PHY_COUNT, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(input_row_desc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(out_row_desc), K(input_row_desc), K(phy_op));
  } else if (OB_FAIL(copy_row_desc_by_projector(*input_row_desc,
                 child_ops.at(0).first->get_projector(),
                 child_ops.at(0).first->get_projector_size(),
                 *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else {
    ObRawExpr* rownum_limit_expr = op.get_rownum_limit_expr();
    if (OB_NOT_NULL(rownum_limit_expr)) {
      ObSqlExpression* sql_expr = NULL;
      if (OB_FAIL(
              generate_sql_expr(phy_op->get_sql_expression_factory(), rownum_limit_expr, *input_row_desc, sql_expr))) {
        LOG_WARN("failed to generate sql expression", K(ret));
      } else if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("generated sql expr is null", K(ret));
      } else {
        phy_op->set_rownum_limit_expr(sql_expr);
      }
    }
    // get anti-monotone filters
    if (OB_SUCC(ret) && !op.get_filter_exprs().empty()) {
      ObSEArray<ObRawExpr*, 4> anti_monotone_filters;
      ObSEArray<ObRawExpr*, 4> non_anti_monotone_filters;
      if (OB_FAIL(classify_anti_monotone_filter_exprs(
              op.get_filter_exprs(), non_anti_monotone_filters, anti_monotone_filters))) {
        LOG_WARN("failed to classify anti-monotone filters exprs", K(ret));
      } else if (anti_monotone_filters.empty()) {
        /*do nothing*/
      } else {
        LOG_DEBUG("succeed to classify anti-monotone filters", K(anti_monotone_filters), K(non_anti_monotone_filters));
        op.get_filter_exprs().reset();
        if (OB_FAIL(op.get_filter_exprs().assign(non_anti_monotone_filters))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else { /*do nothing*/
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < anti_monotone_filters.count(); i++) {
          ObRawExpr* raw_expr = anti_monotone_filters.at(i);
          ObSqlExpression* sql_expr = NULL;
          if (OB_ISNULL(raw_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("get unexected null", K(ret));
          } else if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), sql_expr))) {
            LOG_WARN("failed to create sql_expr", K(ret));
          } else if (OB_ISNULL(sql_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("get unexpected null", K(ret), K(sql_expr));
          } else if (OB_FAIL(generate_expr(*raw_expr, *input_row_desc, *sql_expr))) {
            LOG_WARN("failed to generate sql_expr", K(ret), K(raw_expr));
          } else if (OB_FAIL(phy_op->add_anti_monotone_filter_exprs(sql_expr))) {
            LOG_WARN("failed to add sql sql_expr", K(ret), K(sql_expr));
          } else { /*do nothing*/
          }
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::classify_anti_monotone_filter_exprs(const ObIArray<ObRawExpr*>& input_filters,
    ObIArray<ObRawExpr*>& non_anti_monotone_filters, ObIArray<ObRawExpr*>& anti_monotone_filters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_filters.count(); i++) {
    ObRawExpr* raw_expr = input_filters.at(i);
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (raw_expr->has_flag(CNT_ROWNUM) && !raw_expr->has_flag(CNT_COLUMN) &&
               !raw_expr->has_flag(CNT_SUB_QUERY)) {
      ret = anti_monotone_filters.push_back(raw_expr);
    } else {
      ret = non_anti_monotone_filters.push_back(raw_expr);
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_sequence(ObLogSequence& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObSequence* phy_op = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc* input_row_desc = NULL;
  const int32_t* input_projector = NULL;
  int64_t input_projector_size = 0;
  if (OB_UNLIKELY(1 < child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (OB_FAIL(create_phy_op_desc(PHY_SEQUENCE, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (1 == child_ops.count()) {
    input_row_desc = child_ops.at(0).second;
    input_projector = child_ops.at(0).first->get_projector();
    input_projector_size = child_ops.at(0).first->get_projector_size();
    if (OB_ISNULL(out_row_desc) || OB_ISNULL(input_row_desc) || OB_ISNULL(phy_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(input_row_desc), K(phy_op));
    } else if (OB_FAIL(
                   copy_row_desc_by_projector(*input_row_desc, input_projector, input_projector_size, *out_row_desc))) {
      LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
    }
  } else {
    // child ops = 0, for insert stmt
  }

  if (OB_SUCC(ret)) {
    const ObIArray<uint64_t>& ids = op.get_sequence_ids();
    ARRAY_FOREACH_X(ids, idx, cnt, OB_SUCC(ret))
    {
      if (OB_FAIL(phy_op->add_uniq_nextval_sequence_id(ids.at(idx)))) {
        LOG_WARN("failed to set sequence", K(ids), K(ret));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::set_optimization_info(ObLogTableScan& log_ts, ObTableScan* phy_ts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_ts) || OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(phy_ts->set_est_row_count_record(log_ts.get_est_row_count_record()))) {
    LOG_WARN("failed to set estimation row count records", K(ret));
  } else {
    phy_ts->set_table_row_count(log_ts.get_table_row_count());
    phy_ts->set_output_row_count(static_cast<int64_t>(log_ts.get_output_row_count()));
    phy_ts->set_phy_query_range_row_count(static_cast<int64_t>(log_ts.get_phy_query_range_row_count()));
    phy_ts->set_query_range_row_count(static_cast<int64_t>(log_ts.get_query_range_row_count()));
    phy_ts->set_index_back_row_count(static_cast<int64_t>(log_ts.get_index_back_row_count()));
    if (OB_NOT_NULL(log_ts.get_table_opt_info())) {
      phy_ts->set_optimization_method(log_ts.get_table_opt_info()->optimization_method_);
      phy_ts->set_available_index_count(log_ts.get_table_opt_info()->available_index_id_.count());
      if (OB_FAIL(phy_ts->set_available_index_name(
              log_ts.get_table_opt_info()->available_index_name_, phy_plan_->get_allocator()))) {
        LOG_WARN("failed to set available index name", K(ret));
      } else if (OB_FAIL(phy_ts->set_unstable_index_name(
                     log_ts.get_table_opt_info()->unstable_index_name_, phy_plan_->get_allocator()))) {
        LOG_WARN("failedd to set unstable index name", K(ret));
      } else if (OB_FAIL(phy_ts->set_pruned_index_name(
                     log_ts.get_table_opt_info()->pruned_index_name_, phy_plan_->get_allocator()))) {
        LOG_WARN("failedd to set prunned index name", K(ret));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::set_partition_range_info(ObLogTableScan& log_op, ObTableScan* phy_op)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = log_op.get_table_id();
  uint64_t ref_table_id = log_op.get_location_table_id();
  uint64_t index_id = log_op.get_index_table_id();
  ObLogPlan* log_plan = log_op.get_plan();
  ObDMLStmt* stmt = log_op.get_stmt();
  const ObTablePartitionInfo* tbl_part_info = log_op.get_table_partition_info();
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  const ObTableSchema* index_schema = NULL;
  ObRawExpr* part_expr = NULL;
  ObRawExpr* subpart_expr = NULL;
  ObSEArray<uint64_t, 2> part_column_ids;
  ObSEArray<uint64_t, 2> subpart_column_ids;
  ObSEArray<uint64_t, 2> rowkey_column_ids;
  if (OB_ISNULL(tbl_part_info) && PHY_MULTI_PART_TABLE_SCAN == phy_op->get_type()) {
    // do nothing, global index back scan don't has tbl_part_info.
  } else if (OB_ISNULL(phy_op) || OB_ISNULL(log_plan) || OB_ISNULL(stmt) || OB_ISNULL(tbl_part_info) ||
             OB_ISNULL(phy_op->get_sql_expression_factory())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(phy_op),
        K(log_plan),
        K(tbl_part_info),
        K(phy_op->get_sql_expression_factory()),
        K(stmt),
        K(ret));
  } else if (OB_INVALID_ID == table_id || OB_INVALID_ID == ref_table_id || OB_INVALID_ID == index_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid table id", K(table_id), K(ref_table_id), K(index_id), K(ret));
  } else if (is_virtual_table(ref_table_id) || is_inner_table(ref_table_id) || is_cte_table(ref_table_id)) {
    /*do nothing*/
  } else if (!stmt->is_select_stmt() || tbl_part_info->get_table_location().has_generated_column()) {
    /*do nothing*/
  } else if (OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ref_table_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("get index schema failed", K(index_id), K(ret));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(table_schema), K(index_schema), K(ret));
  } else if (!table_schema->is_partitioned_table()) {
    /*do nothing*/
  } else if (OB_FAIL(index_schema->get_rowkey_info().get_column_ids(rowkey_column_ids))) {
    LOG_WARN("failed to get index rowkey column ids", K(ret));
  } else if (OB_ISNULL(part_expr = stmt->get_part_expr(table_id, ref_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null part expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(part_expr, part_column_ids))) {
    LOG_WARN("failed to check pure column part expr", K(ret));
  } else if (ObPartitionLevel::PARTITION_LEVEL_TWO == table_schema->get_part_level() &&
             OB_ISNULL(subpart_expr = stmt->get_subpart_expr(table_id, ref_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null subpart expr", K(ret));
  } else if (NULL != subpart_expr && OB_FAIL(ObRawExprUtils::extract_column_ids(subpart_expr, subpart_column_ids))) {
    LOG_WARN("failed to check pure column part expr", K(ret));
  } else {
    bool is_valid = true;
    ObSEArray<int64_t, 4> part_range_pos;
    ObSEArray<int64_t, 4> subpart_range_pos;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < part_column_ids.count(); i++) {
      bool is_find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < rowkey_column_ids.count(); j++) {
        if (part_column_ids.at(i) == rowkey_column_ids.at(j)) {
          is_find = true;
          if (OB_FAIL(part_range_pos.push_back(j))) {
            LOG_WARN("failed to push back range pos", K(ret));
          } else { /*do nothing*/
          }
        }
      }
      if (!is_find) {
        is_valid = false;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subpart_column_ids.count(); i++) {
      bool is_find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < rowkey_column_ids.count(); j++) {
        if (subpart_column_ids.at(i) == rowkey_column_ids.at(j)) {
          is_find = true;
          if (OB_FAIL(subpart_range_pos.push_back(j))) {
            LOG_WARN("failed to push back range pos", K(ret));
          } else { /*do nothing*/
          }
        }
      }
      if (!is_find) {
        is_valid = false;
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      ObSqlExpression* part_expr = NULL;
      ObSqlExpression* subpart_expr = NULL;
      const ObTableLocation& tbl_location = tbl_part_info->get_table_location();
      if (OB_FAIL(ObSqlExpressionUtil::copy_sql_expression(
              *phy_op->get_sql_expression_factory(), tbl_location.get_part_expr(), part_expr))) {
        LOG_WARN("failed to copy sql expression", K(ret));
      } else if (OB_FAIL(ObSqlExpressionUtil::copy_sql_expression(
                     *phy_op->get_sql_expression_factory(), tbl_location.get_subpart_expr(), subpart_expr))) {
        LOG_WARN("failed to copy sql expression", K(ret));
      } else if (OB_FAIL(phy_op->set_part_expr(part_expr))) {
        LOG_WARN("failed to set part expr", K(ret));
      } else if (OB_FAIL(phy_op->set_subpart_expr(subpart_expr))) {
        LOG_WARN("failed to set subpart expr", K(ret));
      } else if (OB_FAIL(phy_op->set_partition_range_pos(part_range_pos))) {
        LOG_WARN("failed to set partition range pos", K(ret));
      } else if (OB_FAIL(phy_op->set_subpartition_range_pos(subpart_range_pos))) {
        LOG_WARN("failed to set subpartition range pos", K(ret));
      } else {
        phy_op->set_partition_level(table_schema->get_part_level());
        phy_op->set_partition_type(table_schema->get_part_option().get_part_func_type());
        phy_op->set_subpartition_type(table_schema->get_sub_part_option().get_part_func_type());
        LOG_DEBUG(
            "partition range pos", K(table_schema->get_part_level()), K(part_range_pos), K(subpart_range_pos), K(ret));
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::set_part_func(const ObIArray<ObRawExpr*>& part_func_exprs, const RowDesc* row_desc,
    ObSqlExpression* part_expr, ObSqlExpression* sub_part_expr)
{
  int ret = OB_SUCCESS;
  if (0 == part_func_exprs.count()) {
    // do nothing
  } else if (1 == part_func_exprs.count() || 2 == part_func_exprs.count()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_func_exprs.count(); ++i) {
      ObRawExpr* part_func = part_func_exprs.at(i);
      ObSqlExpression* target_expr = (i == 0) ? part_expr : sub_part_expr;
      if (OB_FAIL(generate_expr(*part_func, *row_desc, *target_expr))) {
        LOG_WARN("failed to generate the part func sql expression");
      } else {
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only support less than 2 part func now", K(part_func_exprs.count()));
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_check_constraint(ObLogInsertAll& log_op, uint64_t index,
    ObMultiTableInsert& parent_phy_op, ObTableModify& phy_op, RowDesc& out_row_desc,
    InsertTableInfo*& insert_table_info)
{
  int ret = OB_SUCCESS;
  ObLogPlan* log_plan = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(log_plan = log_op.get_plan()) ||
      OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard()) || OB_ISNULL(insert_table_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(log_op), K(insert_table_info), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(phy_op.get_index_tid(), table_schema))) {
    LOG_WARN("failed to get table schema", K(phy_op.get_index_tid()), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(phy_op.get_index_tid()), K(ret));
  } else if (!(table_schema->is_user_table() || table_schema->is_tmp_table())) {
    // do nothing, especially for global index.
    LOG_DEBUG("skip convert constraint",
        "table_id",
        table_schema->get_table_name_str(),
        "table_type",
        table_schema->get_table_type());
  } else if (NULL != log_op.get_multi_insert_table_info()) {
    if (OB_UNLIKELY(index >= log_op.get_multi_insert_table_info()->count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(index), K(log_op.get_multi_insert_table_info()->count()));
    } else {
      const ObIArray<ObRawExpr*>& check_constraint_exprs =
          log_op.get_multi_insert_table_info()->at(index).check_constraint_exprs_;
      for (uint64_t i = 0; OB_SUCC(ret) && i < check_constraint_exprs.count(); ++i) {
        ObSqlExpression* sql_expr = NULL;
        if (OB_ISNULL(check_constraint_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("null pointer", K(ret), K(check_constraint_exprs.at(i)));
        } else if (OB_FAIL(generate_sql_expr(parent_phy_op.get_sql_expression_factory(),
                       check_constraint_exprs.at(i),
                       out_row_desc,
                       sql_expr))) {
          LOG_WARN("failed to generate sql expression", K(ret));
        } else if (OB_ISNULL(sql_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("generated sql expr is null", K(ret));
        } else if (OB_FAIL(insert_table_info->add_check_constraint_expr(sql_expr))) {
          LOG_WARN("failed to add check_constraint_expr", K(ret), K(i), KPC(sql_expr));
        }
      }
    }
  } else {
    LOG_TRACE("skip convert check constraint");
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_check_constraint(ObLogDelUpd& log_op, ObTableModify& phy_op, RowDesc& out_row_desc)
{
  int ret = OB_SUCCESS;
  ObLogPlan* log_plan = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;

  if (OB_ISNULL(log_plan = log_op.get_plan()) ||
      OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(log_op), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(phy_op.get_index_tid(), table_schema))) {
    LOG_WARN("failed to get table schema", K(phy_op.get_index_tid()), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(phy_op.get_index_tid()), K(ret));
  } else if (!(table_schema->is_user_table() || table_schema->is_tmp_table())) {
    // do nothing, especially for global index.
    LOG_DEBUG("skip convert constraint",
        "table_id",
        table_schema->get_table_name_str(),
        "table_type",
        table_schema->get_table_type());
  } else if (NULL != log_op.get_check_constraint_exprs()) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < log_op.get_check_constraint_exprs()->count(); ++i) {
      ObSqlExpression* sql_expr = NULL;
      if (OB_ISNULL(log_op.get_check_constraint_exprs()->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null pointer");
      } else if (OB_FAIL(generate_sql_expr(phy_op.get_sql_expression_factory(),
                     log_op.get_check_constraint_exprs()->at(i),
                     out_row_desc,
                     sql_expr))) {
        LOG_WARN("failed to generate sql expression", K(ret));
      } else if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("generated sql expr is null", K(ret));
      } else if (OB_FAIL(phy_op.add_check_constraint_expr(sql_expr))) {
        LOG_WARN("failed to add check_constraint_expr", K(ret), K(i), KPC(sql_expr));
      }
    }
  }

  return ret;
}

int ObCodeGeneratorImpl::convert_foreign_keys(ObLogDelUpd& log_op, ObTableModify& phy_op)
{
  int ret = OB_SUCCESS;
  ObLogPlan* log_plan = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  const ObIArray<ObForeignKeyInfo>* fk_infos = NULL;
  const ObIArray<uint64_t>* value_column_ids = NULL;
  const ObIArray<uint64_t>* name_column_ids = NULL;
  uint64_t name_table_id = OB_INVALID_ID;
  const ObTableSchema* table_schema = NULL;

  if (OB_ISNULL(log_plan = log_op.get_plan()) || OB_ISNULL(phy_plan_) ||
      OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(log_op), K(phy_plan_), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(phy_op.get_index_tid(), table_schema))) {
    LOG_WARN("failed to get table schema", K(phy_op.get_index_tid()), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(phy_op.get_index_tid()), K(ret));
  } else if (!table_schema->is_user_table()) {
    // do nothing, especially for global index.
    LOG_DEBUG("skip convert foreign key",
        "table_id",
        table_schema->get_table_name_str(),
        "table_type",
        table_schema->get_table_type());
  } else if (OB_ISNULL(fk_infos = &table_schema->get_foreign_key_infos())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("foreign key infos is null", K(ret));
  } else if (OB_FAIL(phy_op.init_foreign_key_args(table_schema->get_foreign_key_real_count()))) {
    LOG_WARN("failed to init foreign key stmts", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos->count(); i++) {
      const ObForeignKeyInfo& fk_info = fk_infos->at(i);
      ObForeignKeyArg fk_arg;
      if (share::is_oracle_mode()) {
        if (!fk_info.enable_flag_ && fk_info.validate_flag_) {
          const ObSimpleDatabaseSchema* database_schema = NULL;
          if (OB_FAIL(schema_guard->get_database_schema(table_schema->get_database_id(), database_schema))) {
            LOG_WARN("get database schema failed", K(ret), K(table_schema->get_database_id()));
          } else if (OB_ISNULL(database_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("database_schema is null", K(ret));
          } else {
            ret = OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE;
            LOG_USER_ERROR(OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE,
                database_schema->get_database_name_str().length(),
                database_schema->get_database_name_str().ptr(),
                fk_info.foreign_key_name_.length(),
                fk_info.foreign_key_name_.ptr());
            LOG_WARN("no insert/delete/update on table with constraint disabled and validated", K(ret));
          }
        } else if (!fk_info.enable_flag_) {
          continue;
        }
      }
      if (OB_SUCC(ret) && (fk_info.child_column_ids_.count() != fk_info.parent_column_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child column count and parent column count is not equal",
            K(ret),
            K(fk_info.child_column_ids_),
            K(fk_info.parent_column_ids_));
      }
      if (OB_SUCC(ret) && fk_info.parent_table_id_ == fk_info.child_table_id_) {
        fk_arg.is_self_ref_ = true;
      }
      if (OB_SUCC(ret) && fk_info.table_id_ == fk_info.child_table_id_) {
        name_table_id = fk_info.parent_table_id_;
        name_column_ids = &fk_info.parent_column_ids_;
        value_column_ids = &fk_info.child_column_ids_;
        if (phy_op.get_type() == PHY_INSERT || phy_op.get_type() == PHY_UPDATE || phy_op.get_type() == PHY_MERGE ||
            phy_op.get_type() == PHY_MULTI_TABLE_MERGE || phy_op.get_type() == PHY_INSERT_ON_DUP ||
            phy_op.get_type() == PHY_INSERT_RETURNING || phy_op.get_type() == PHY_UPDATE_RETURNING ||
            phy_op.get_type() == PHY_DELETE_RETURNING || phy_op.get_type() == PHY_REPLACE) {
          fk_arg.ref_action_ = ACTION_CHECK_EXIST;
        } else {
          fk_arg.ref_action_ = ACTION_INVALID;
        }
        if (OB_FAIL(add_fk_arg_to_phy_op(
                fk_arg, name_table_id, *name_column_ids, *value_column_ids, *schema_guard, phy_op))) {
          LOG_WARN("failed to add fk arg to phy op", K(ret));
        }
      }
      if (OB_SUCC(ret) && fk_info.table_id_ == fk_info.parent_table_id_) {
        name_table_id = fk_info.child_table_id_;
        name_column_ids = &fk_info.child_column_ids_;
        value_column_ids = &fk_info.parent_column_ids_;
        if (phy_op.get_type() == PHY_UPDATE || phy_op.get_type() == PHY_UPDATE_RETURNING ||
            phy_op.get_type() == PHY_MERGE || phy_op.get_type() == PHY_MULTI_TABLE_MERGE ||
            phy_op.get_type() == PHY_INSERT_ON_DUP) {
          fk_arg.ref_action_ = fk_info.update_action_;
        } else if (phy_op.get_type() == PHY_DELETE || phy_op.get_type() == PHY_DELETE_RETURNING ||
                   phy_op.get_type() == PHY_REPLACE) {
          fk_arg.ref_action_ = fk_info.delete_action_;
        } else {
          fk_arg.ref_action_ = ACTION_INVALID;
        }
        if (OB_FAIL(add_fk_arg_to_phy_op(
                fk_arg, name_table_id, *name_column_ids, *value_column_ids, *schema_guard, phy_op))) {
          LOG_WARN("failed to add fk arg to phy op", K(ret));
        }
      }
    }  // for
    if (OB_SUCC(ret) && fk_infos->count() > 0) {
      OX(phy_plan_->set_need_serial_exec(true));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::need_fire_update_event(const ObTableSchema& table_schema, const ObString& update_events,
    const ObLogUpdate& log_update, const ObSQLSessionInfo& session, ObIAllocator& allocator, bool& need_fire)
{
  int ret = OB_SUCCESS;
  if (update_events.empty()) {
    need_fire = true;
  } else {
    need_fire = false;
    ObParser parser(allocator, session.get_sql_mode());
    ParseResult parse_result;
    const ParseNode* update_columns_node = NULL;
    const ObAssignments& assignments = log_update.get_tables_assignments()->at(0).assignments_;
    ObString column_name;
    OZ(parser.parse(update_events, parse_result));
    OV(parse_result.result_tree_ != NULL);
    OV(parse_result.result_tree_->children_ != NULL);
    OX(update_columns_node = parse_result.result_tree_->children_[0]);
    OV(update_columns_node != NULL);
    OV(update_columns_node->type_ == T_TG_COLUMN_LIST, OB_ERR_UNEXPECTED, update_columns_node->type_);
    OV(update_columns_node->num_child_ > 0, OB_ERR_UNEXPECTED, update_columns_node->num_child_);
    OV(update_columns_node->children_ != NULL);
    for (int64_t i = 0; OB_SUCC(ret) && !need_fire && i < update_columns_node->num_child_; i++) {
      const ParseNode* column_node = update_columns_node->children_[i];
      const ObColumnSchemaV2* column_schema = NULL;
      OV(column_node != NULL);
      OV(column_node->type_ == T_IDENT, OB_ERR_UNEXPECTED, column_node->type_);
      OV(column_node->str_value_ != NULL && column_node->str_len_ > 0);
      OX(column_name.assign_ptr(column_node->str_value_, static_cast<int32_t>(column_node->str_len_)));
      OX(column_schema = table_schema.get_column_schema(column_name));
      OV(column_schema != NULL);
      for (int64_t j = 0; OB_SUCC(ret) && j < assignments.count(); j++) {
        if (column_schema->get_column_id() == assignments.at(j).base_column_id_) {
          OX(need_fire = !assignments.at(j).is_implicit_);
          break;
        }
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::add_fk_arg_to_phy_op(ObForeignKeyArg& fk_arg, uint64_t name_table_id,
    const ObIArray<uint64_t>& name_column_ids, const ObIArray<uint64_t>& value_column_ids,
    ObSchemaGetterGuard& schema_guard, ObTableModify& phy_op)
{
  int ret = OB_SUCCESS;
  bool need_handle = true;
  const ObDatabaseSchema* database_schema = NULL;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  if (OB_FAIL(need_foreign_key_handle(fk_arg, value_column_ids, phy_op, need_handle))) {
    LOG_WARN("failed to check if need handle foreign key", K(ret));
  } else if (!need_handle) {
    LOG_DEBUG("skip foreign key handle", K(fk_arg));
  } else if (OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(phy_plan_), K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(name_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(fk_arg), K(name_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(name_table_id), K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), database_schema))) {
    LOG_WARN("failed to get database schema", K(table_schema->get_database_id()), K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null", K(table_schema->get_database_id()), K(ret));
  } else if (OB_FAIL(deep_copy_ob_string(
                 phy_plan_->get_allocator(), database_schema->get_database_name(), fk_arg.database_name_))) {
    LOG_WARN("failed to deep copy ob string", K(fk_arg), K(table_schema->get_table_name()), K(ret));
  } else if (OB_FAIL(
                 deep_copy_ob_string(phy_plan_->get_allocator(), table_schema->get_table_name(), fk_arg.table_name_))) {
    LOG_WARN("failed to deep copy ob string", K(fk_arg), K(table_schema->get_table_name()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && need_handle && i < name_column_ids.count(); i++) {
    ObForeignKeyColumn fk_column;
    if (OB_ISNULL(column_schema = (table_schema->get_column_schema(name_column_ids.at(i))))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(fk_arg), K(name_column_ids.at(i)), K(ret));
    } else if (OB_FAIL(deep_copy_ob_string(
                   phy_plan_->get_allocator(), column_schema->get_column_name_str(), fk_column.name_))) {
      LOG_WARN("failed to deep copy ob string", K(fk_arg), K(column_schema->get_column_name_str()), K(ret));
    } else if (fk_arg.is_self_ref_ && 0 > (fk_column.name_idx_ = phy_op.get_column_idx(name_column_ids.at(i)))) {
      /**
       * fk_column.name_idx_ is used only for self ref row, that is to say name table and
       * value table is same table.
       * otherwise name_column_ids.at(i) will indicate columns in name table, not value table,
       * and phy_op is value table here.
       */
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("foreign key column id is not in colunm ids", K(fk_arg), K(name_column_ids.at(i)), K(ret));
    } else if (0 > (fk_column.idx_ = phy_op.get_column_idx(value_column_ids.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("foreign key column id is not in colunm ids", K(fk_arg), K(value_column_ids.at(i)), K(ret));
    } else if (OB_FAIL(fk_arg.columns_.push_back(fk_column))) {
      LOG_WARN("failed to push foreign key column", K(fk_arg), K(fk_column), K(ret));
    }
  }
  if (OB_SUCC(ret) && need_handle) {
    if (OB_FAIL(phy_op.add_foreign_key_arg(fk_arg))) {
      LOG_WARN("failed to add foreign key arg", K(fk_arg), K(ret));
    } else {
      phy_plan_->set_has_nested_sql(true);
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::need_foreign_key_handle(const ObForeignKeyArg& fk_arg,
    const ObIArray<uint64_t>& value_column_ids, const ObTableModify& phy_op, bool& need_handle)
{
  int ret = OB_SUCCESS;
  need_handle = true;
  if (ACTION_INVALID == fk_arg.ref_action_) {
    need_handle = false;
  } else if (phy_op.get_type() == PHY_UPDATE || phy_op.get_type() == PHY_UPDATE_RETURNING) {
    // check if foreign key operation is necessary.
    // no matter current table is parent table or child table, the value_column_ids will
    // represent the foreign key related columns of the current table. so we only need to
    // check if these columns maybe updated, by checking if the two arrays are intersected.
    // merge into and insert on dup both have insert semantics, so can not skip foreign
    // key check. see future improvment in aone issue above.
    const ObIArray<uint64_t>* updated_column_ids = NULL;
    bool has_intersect = false;
    if (PHY_UPDATE == phy_op.get_type() || PHY_UPDATE_RETURNING == phy_op.get_type()) {
      const ObTableUpdate& update_op = static_cast<const ObTableUpdate&>(phy_op);
      updated_column_ids = update_op.get_updated_column_ids();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected phy op type", K(fk_arg), K(phy_op.get_type()), K(ret));
    }
    if (OB_SUCC(ret) && !OB_ISNULL(updated_column_ids)) {
      for (int64_t i = 0; !has_intersect && i < value_column_ids.count(); i++) {
        for (int64_t j = 0; !has_intersect && j < updated_column_ids->count(); j++) {
          has_intersect = (value_column_ids.at(i) == updated_column_ids->at(j));
        }
      }
    }
    need_handle = has_intersect;
  } else {
    // nothing.
  }
  return ret;
}

int ObCodeGeneratorImpl::convert(
    ObLogicalOperator& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops, bool in_root_job)
{
  int ret = OB_SUCCESS;
  switch (op.get_type()) {
    case log_op_def::LOG_LIMIT:
      ret = convert_limit(static_cast<ObLogLimit&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_GROUP_BY:
      ret = convert_group_by(static_cast<ObLogGroupBy&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_SORT:
      ret = convert_sort(static_cast<ObLogSort&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_MV_TABLE_SCAN:
    case log_op_def::LOG_TABLE_SCAN:
      ret = convert_table_scan(static_cast<ObLogTableScan&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_JOIN:
      ret = convert_join(static_cast<ObLogJoin&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_EXCHANGE:
      ret = convert_exchange(static_cast<ObLogExchange&>(op), child_ops, out_ops, in_root_job);
      break;
    case log_op_def::LOG_DISTINCT:
      ret = convert_distinct(static_cast<ObLogDistinct&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_FOR_UPD: {
      ObLogForUpdate& for_update_op = static_cast<ObLogForUpdate&>(op);
      if (for_update_op.is_multi_part_dml()) {
        ret = convert_multi_table_for_update(for_update_op, child_ops, out_ops);
      } else {
        ret = convert_for_update(for_update_op, child_ops, out_ops);
      }
      break;
    }
    case log_op_def::LOG_DELETE: {
      ObLogDelete& delete_op = static_cast<ObLogDelete&>(op);
      if (delete_op.is_multi_part_dml()) {
        ret = convert_multi_table_delete(delete_op, child_ops, out_ops);
      } else if (delete_op.is_pdml()) {
        ret = convert_pdml_delete(delete_op, child_ops, out_ops);
      } else {
        ret = convert_delete(delete_op, child_ops, out_ops);
      }
    } break;
    case log_op_def::LOG_UPDATE: {
      ObLogUpdate& update_op = static_cast<ObLogUpdate&>(op);
      if (update_op.is_multi_part_dml()) {
        ret = convert_multi_table_update(update_op, child_ops, out_ops);
      } else if (update_op.is_pdml()) {
        ret = convert_pdml_update(update_op, child_ops, out_ops);
      } else {
        ret = convert_update(update_op, child_ops, out_ops);
      }
    } break;
    case log_op_def::LOG_INSERT: {
      ObLogInsert& insert_op = static_cast<ObLogInsert&>(op);
      if (insert_op.is_replace()) {
        ret = convert_replace(insert_op, child_ops, out_ops);
      } else if (insert_op.get_insert_up()) {
        ret = convert_insert_up(insert_op, child_ops, out_ops);
      } else if (insert_op.is_pdml()) {
        ret = convert_pdml_insert(insert_op, child_ops, out_ops);
      } else {
        ret = convert_insert(insert_op, child_ops, out_ops);
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(phy_plan_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
        } else {
          phy_plan_->set_is_insert_select(static_cast<ObLogInsert&>(op).is_insert_select());
        }
      }
    } break;
    case log_op_def::LOG_INSERT_ALL: {
      ObLogInsertAll& insert_op = static_cast<ObLogInsertAll&>(op);
      ret = convert_multi_table_insert(insert_op, child_ops, out_ops);
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(phy_plan_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
        } else {
          phy_plan_->set_is_insert_select(true);
        }
      }
    } break;
    case log_op_def::LOG_MERGE: {
      ret = convert_merge(static_cast<ObLogMerge&>(op), child_ops, out_ops);
    } break;
    case log_op_def::LOG_EXPR_VALUES:
      ret = convert_expr_values(static_cast<ObLogExprValues&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_VALUES:
      ret = convert_values(static_cast<ObLogValues&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_SET:
      ret = convert_set(static_cast<ObLogSet&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_SUBPLAN_FILTER:
      ret = convert_subplan_filter(static_cast<ObLogSubPlanFilter&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_SUBPLAN_SCAN:
      ret = convert_subplan_scan(static_cast<ObLogSubPlanScan&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_MATERIAL:
      ret = convert_material(static_cast<ObLogMaterial&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_WINDOW_FUNCTION:
      ret = convert_window_function(static_cast<ObLogWindowFunction&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_SELECT_INTO:
      ret = convert_select_into(static_cast<ObLogSelectInto&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_TOPK:
      ret = convert_topk(static_cast<ObLogTopk&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_APPEND:
      ret = convert_append(static_cast<ObLogAppend&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_COUNT:
      ret = convert_count(static_cast<ObLogCount&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_GRANULE_ITERATOR: {
      ret = convert_granule_iterator(static_cast<ObLogGranuleIterator&>(op), child_ops, out_ops);
      break;
    }
    case log_op_def::LOG_TABLE_LOOKUP:
      ret = convert_table_lookup(static_cast<ObLogTableLookup&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_CONFLICT_ROW_FETCHER:
      ret = convert_conflict_row_fetcher(static_cast<ObLogConflictRowFetcher&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_SEQUENCE:
      ret = convert_sequence(static_cast<ObLogSequence&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_FUNCTION_TABLE:
      ret = OB_NOT_SUPPORTED;
      break;
    case log_op_def::LOG_MONITORING_DUMP:
      ret = convert_monitoring_dump(static_cast<ObLogMonitoringDump&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_UNPIVOT:
      ret = convert_unpivot(static_cast<ObLogUnpivot&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_LINK:
      ret = convert_link_scan(static_cast<ObLogLink&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_TEMP_TABLE_ACCESS:
      ret = convert_temp_table(static_cast<ObLogTempTableAccess&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_TEMP_TABLE_INSERT:
      ret = convert_temp_table_insert(static_cast<ObLogTempTableInsert&>(op), child_ops, out_ops);
      break;
    case log_op_def::LOG_TEMP_TABLE_TRANSFORMATION:
      ret = convert_temp_table_transformation(static_cast<ObLogTempTableTransformation&>(op), child_ops, out_ops);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown logical operator", K(op.get_type()));
      break;
  }

  if (OB_SUCC(ret)) {
    // check convert phy operator type with get_phy_op_type() again.
    ObPhyOperatorType type = PHY_INVALID;
    ObPhyOperator* phy_op = out_ops.at(out_ops.count() - 1).first;
    if (out_ops.empty() || OB_ISNULL(out_ops.at(out_ops.count() - 1).first)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no phy operator converted", K(ret), K(op.get_name()), K(out_ops.count()));
    } else if (OB_FAIL(get_phy_op_type(op, type, in_root_job))) {
      LOG_WARN("get phy operator type failed", K(ret));
    } else if (type != out_ops.at(out_ops.count() - 1).first->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("converted phy operator type not the same with get_phy_op_type()",
          K(ret),
          K(op.get_name()),
          "get_type",
          ob_phy_operator_type_str(type),
          "convert_type",
          ob_phy_operator_type_str(out_ops.at(out_ops.count() - 1).first->get_type()));
    } else if (phy_op->is_dml_operator()) {
      ObTableModify* phy_dml_op = static_cast<ObTableModify*>(phy_op);
      ObMultiDMLInfo* multi_dml_info = dynamic_cast<ObMultiDMLInfo*>(phy_op);
      if (OB_NOT_NULL(multi_dml_info)) {
        // success to dynamic_cast, spec is multi_XXX_dml_operator
        if (!phy_dml_op->is_multi_dml()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("multi_xxx_dml operator is_multi_dml tag should br true", K(phy_dml_op->get_type()));
        }
      }
    }
  }
  return ret;
}

// FIXME : We should split the big switch case into logical operator class.
int ObCodeGeneratorImpl::get_phy_op_type(ObLogicalOperator& log_op, ObPhyOperatorType& type, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  type = PHY_INVALID;
  switch (log_op.get_type()) {
    case log_op_def::LOG_LIMIT: {
      type = PHY_LIMIT;
      break;
    }
    case log_op_def::LOG_GROUP_BY: {
      auto& op = static_cast<ObLogGroupBy&>(log_op);
      switch (op.get_algo()) {
        case MERGE_AGGREGATE:
          type = PHY_MERGE_GROUP_BY;
          break;
        case HASH_AGGREGATE:
          type = PHY_HASH_GROUP_BY;
          break;
        case SCALAR_AGGREGATE:
          type = PHY_SCALAR_AGGREGATE;
          break;
        default:
          break;
      }
      break;
    }
    case log_op_def::LOG_SORT: {
      type = PHY_SORT;
      break;
    }
    case log_op_def::LOG_MV_TABLE_SCAN: {
      type = PHY_MV_TABLE_SCAN;
      break;
    }
    case log_op_def::LOG_TABLE_SCAN: {
      auto& op = static_cast<ObLogTableScan&>(log_op);
      if (op.get_is_fake_cte_table()) {
        type = PHY_FAKE_CTE_TABLE;
      } else if (op.is_sample_scan()) {
        if (op.get_sample_info().method_ == SampleInfo::ROW_SAMPLE) {
          type = PHY_ROW_SAMPLE_SCAN;
        } else if (op.get_sample_info().method_ == SampleInfo::BLOCK_SAMPLE) {
          type = PHY_BLOCK_SAMPLE_SCAN;
        }
      } else if (op.get_is_multi_part_table_scan()) {
        type = PHY_MULTI_PART_TABLE_SCAN;
      } else {
        type = PHY_TABLE_SCAN;
      }
      break;
    }
    case log_op_def::LOG_JOIN: {
      auto& op = static_cast<ObLogJoin&>(log_op);
      switch (op.get_join_algo()) {
        case NESTED_LOOP_JOIN: {
          type = CONNECT_BY_JOIN != op.get_join_type()
                     ? PHY_NESTED_LOOP_JOIN
                     : (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2250
                               ? PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX
                               : (op.get_nl_params().count() > 0 ? PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX
                                                                 : PHY_NESTED_LOOP_CONNECT_BY));
          break;
        }
        case MERGE_JOIN: {
          type = PHY_MERGE_JOIN;
          break;
        }
        case HASH_JOIN: {
          type = PHY_HASH_JOIN;
          break;
        }
        default: {
          break;
        }
      }
      break;
    }
    case log_op_def::LOG_JOIN_FILTER: {
      type = PHY_JOIN_FILTER;
      break;
    }
    case log_op_def::LOG_EXCHANGE: {
      // copy from convert_exchange
      auto& op = static_cast<ObLogExchange&>(log_op);
      if (op.is_producer()) {
        if (op.get_is_remote()) {
          type = PHY_DIRECT_TRANSMIT;
        } else if (OB_REPARTITION_NO_REPARTITION != op.get_repartition_type() && !op.is_slave_mapping()) {
          type = PHY_PX_REPART_TRANSMIT;
        } else if (ObPQDistributeMethod::MAX_VALUE != op.get_dist_method()) {
          type = PHY_PX_DIST_TRANSMIT;
        } else {
          type = PHY_PX_REDUCE_TRANSMIT;
        }
        phy_plan_->inc_px_exchange_out_op_count();
      } else {
        if (op.get_is_remote()) {
          type = PHY_DIRECT_RECEIVE;
        } else {
          if (min_cluster_version_ >= CLUSTER_VERSION_141) {
            if (in_root_job || op.is_rescanable()) {
              if (op.is_merge_sort()) {
                type = PHY_PX_MERGE_SORT_COORD;
              } else {
                type = PHY_PX_FIFO_COORD;
              }
              if (op.is_local_order()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected plan that has merge sort receive "
                         "with local order in root job",
                    K(ret));
              }
            } else if (op.is_merge_sort()) {
              type = PHY_PX_MERGE_SORT_RECEIVE;
            } else {
              type = PHY_PX_FIFO_RECEIVE;
            }
          } else {
            type = PHY_FIFO_RECEIVE;
          }
        }
      }
      break;
    }
    case log_op_def::LOG_DISTINCT: {
      auto& op = static_cast<ObLogDistinct&>(log_op);
      if (MERGE_AGGREGATE == op.get_algo()) {
        type = PHY_MERGE_DISTINCT;
      } else if (HASH_AGGREGATE == op.get_algo()) {
        type = PHY_HASH_DISTINCT;
      }
      break;
    }
    case log_op_def::LOG_DELETE: {
      auto& op = static_cast<ObLogDelete&>(log_op);
      if (op.is_multi_part_dml()) {
        type = PHY_MULTI_PART_DELETE;
      } else if (op.is_pdml()) {
        type = PHY_PX_MULTI_PART_DELETE;
      } else {
        if (op.is_returning()) {
          type = PHY_DELETE_RETURNING;
        } else {
          type = PHY_DELETE;
        }
      }
      break;
    }
    case log_op_def::LOG_UPDATE: {
      auto& op = static_cast<ObLogUpdate&>(log_op);
      if (op.is_multi_part_dml()) {
        type = PHY_MULTI_PART_UPDATE;
      } else if (op.is_pdml()) {
        type = PHY_PX_MULTI_PART_UPDATE;
      } else {
        if (op.is_returning()) {
          type = PHY_UPDATE_RETURNING;
        } else {
          type = PHY_UPDATE;
        }
      }
      break;
    }
    case log_op_def::LOG_FOR_UPD: {
      auto& op = static_cast<ObLogForUpdate&>(log_op);
      if (op.is_multi_part_dml()) {
        type = PHY_MULTI_LOCK;
      } else {
        type = PHY_LOCK;
      }
      break;
    }
    case log_op_def::LOG_INSERT: {
      auto& op = static_cast<ObLogInsert&>(log_op);
      phy_plan_->set_is_insert_select(op.is_insert_select());
      if (op.is_replace()) {
        type = op.is_multi_part_dml() ? PHY_MULTI_TABLE_REPLACE : PHY_REPLACE;
      } else if (op.get_insert_up()) {
        type = op.is_multi_part_dml() ? PHY_MULTI_TABLE_INSERT_UP : PHY_INSERT_ON_DUP;
      } else if (op.is_pdml()) {
        type = PHY_PX_MULTI_PART_INSERT;
      } else {
        type = op.is_multi_part_dml() ? PHY_MULTI_PART_INSERT : (op.is_returning() ? PHY_INSERT_RETURNING : PHY_INSERT);
      }
      break;
    }
    case log_op_def::LOG_INSERT_ALL: {
      type = PHY_MULTI_TABLE_INSERT;
      break;
    }
    case log_op_def::LOG_MERGE: {
      auto& op = static_cast<ObLogMerge&>(log_op);
      type = PHY_MERGE;
      type = op.is_multi_part_dml() ? PHY_MULTI_TABLE_MERGE : PHY_MERGE;
      break;
    }
    case log_op_def::LOG_EXPR_VALUES: {
      auto& op = static_cast<ObLogExprValues&>(log_op);
      type = op.get_num_of_child() == 0 ? PHY_EXPR_VALUES : PHY_EXPR_VALUES_WITH_CHILD;
      break;
    }
    case log_op_def::LOG_VALUES: {
      type = PHY_VALUES;
      break;
    }
    case log_op_def::LOG_SET: {
      auto& op = static_cast<ObLogSet&>(log_op);
      switch (op.get_set_op()) {
        case ObSelectStmt::UNION:
          if (op.is_recursive_union()) {
            type = PHY_RECURSIVE_UNION_ALL;
          } else {
            type = (MERGE_SET == op.get_algo() ? PHY_MERGE_UNION : PHY_HASH_UNION);
          }
          break;
        case ObSelectStmt::INTERSECT:
          type = (MERGE_SET == op.get_algo() ? PHY_MERGE_INTERSECT : PHY_HASH_INTERSECT);
          break;
        case ObSelectStmt::EXCEPT:
          type = (MERGE_SET == op.get_algo() ? PHY_MERGE_EXCEPT : PHY_HASH_EXCEPT);
          break;
        default:
          break;
      }
      break;
    }
    case log_op_def::LOG_SUBPLAN_FILTER: {
      type = PHY_SUBPLAN_FILTER;
      break;
    }
    case log_op_def::LOG_SUBPLAN_SCAN: {
      type = PHY_SUBPLAN_SCAN;
      break;
    }
    case log_op_def::LOG_MATERIAL: {
      type = PHY_MATERIAL;
      break;
    }
    case log_op_def::LOG_WINDOW_FUNCTION: {
      type = PHY_WINDOW_FUNCTION;
      break;
    }
    case log_op_def::LOG_SELECT_INTO: {
      type = PHY_SELECT_INTO;
      break;
    }
    case log_op_def::LOG_TOPK: {
      type = PHY_TOPK;
      break;
    }
    case log_op_def::LOG_APPEND: {
      type = PHY_APPEND;
      break;
    }
    case log_op_def::LOG_COUNT: {
      type = PHY_COUNT;
      break;
    }
    case log_op_def::LOG_GRANULE_ITERATOR: {
      type = PHY_GRANULE_ITERATOR;
      break;
    }
    case log_op_def::LOG_TABLE_LOOKUP: {
      type = PHY_TABLE_LOOKUP;
      break;
    }
    case log_op_def::LOG_CONFLICT_ROW_FETCHER: {
      type = PHY_TABLE_CONFLICT_ROW_FETCHER;
      break;
    }
    case log_op_def::LOG_SEQUENCE: {
      type = PHY_SEQUENCE;
      break;
    }
    case log_op_def::LOG_FUNCTION_TABLE: {
      type = PHY_FUNCTION_TABLE;
      break;
    }
    case log_op_def::LOG_MONITORING_DUMP: {
      type = PHY_MONITORING_DUMP;
      break;
    }
    case log_op_def::LOG_TEMP_TABLE_INSERT: {
      type = PHY_TEMP_TABLE_INSERT;
      break;
    }
    case log_op_def::LOG_TEMP_TABLE_ACCESS: {
      type = PHY_TEMP_TABLE_ACCESS;
      break;
    }
    case log_op_def::LOG_TEMP_TABLE_TRANSFORMATION: {
      type = PHY_TEMP_TABLE_TRANSFORMATION;
      break;
    }
    case log_op_def::LOG_UNPIVOT: {
      type = PHY_UNPIVOT;
      break;
    }
    case log_op_def::LOG_LINK: {
      type = PHY_LINK;
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown logical operator", K(log_op.get_type()));
      break;
  }
  return ret;
}

class TSCTargetOp {
public:
  bool operator()(const ObPhyOperator& op) const
  {
    return op.is_table_scan() && PHY_FAKE_TABLE != op.get_type();
  }
};

class SingleDMLTargetOp {
public:
  bool operator()(const ObPhyOperator& op) const
  {
    return op.is_dml_operator() && PHY_MULTI_PART_UPDATE != op.get_type() && PHY_MULTI_PART_INSERT != op.get_type() &&
           PHY_MULTI_PART_DELETE != op.get_type() && PHY_MULTI_TABLE_MERGE != op.get_type() &&
           PHY_MULTI_TABLE_REPLACE != op.get_type() && PHY_MULTI_TABLE_INSERT_UP != op.get_type();
  }
};

int ObCodeGeneratorImpl::convert_light_granule_iterator(
    ObLogGranuleIterator& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObLightGranuleIterator* phy_op = nullptr;
  ObLogicalOperator* child_log_op = nullptr;
  ObLogTableScan* log_tsc = nullptr;
  ObLogPlan* log_plan = op.get_plan();
  RowDesc* out_row_desc = nullptr;
  RowDesc* input_row_desc = nullptr;
  ObSEArray<const ObPhyOperator*, 2> tsc_ops;
  ObSEArray<const ObPhyOperator*, 1> dml_ops;
  TSCTargetOp is_tsc_op;
  SingleDMLTargetOp is_dml_op;

  if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (FALSE_IT(input_row_desc = child_ops.at(0).second)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(child_log_op = op.get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(create_phy_op_desc(PHY_LIGHT_GRANULE_ITERATOR, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(input_row_desc) || OB_ISNULL(phy_op) || OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(input_row_desc), K(phy_op));
  } else if (OB_FAIL(copy_row_desc(*input_row_desc, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else if (OB_FAIL(ObPhyOperator::find_target_ops(child_ops.at(0).first, is_tsc_op, tsc_ops))) {
    LOG_WARN("find tsc ops failed", K(ret));
  } else if (OB_FAIL(ObPhyOperator::find_target_ops(child_ops.at(0).first, is_dml_op, dml_ops))) {
    LOG_WARN("find dml ops failed", K(ret));
  } else if (OB_UNLIKELY(tsc_ops.empty()) || OB_UNLIKELY(dml_ops.count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gi table scan ops is empty", K(ret), K(tsc_ops.count()), K(dml_ops.count()));
  } else if (OB_FAIL(phy_op->init_lgi_scan_infos(tsc_ops.count()))) {
    LOG_WARN("init lgi scan infos failed", K(ret), K(tsc_ops.count()));
  } else {
    phy_op->set_is_pwj(2 == tsc_ops.count());
    if (!dml_ops.empty()) {
      if (OB_ISNULL(dml_ops.at(0)) || OB_UNLIKELY(!dml_ops.at(0)->is_dml_operator())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dml ops is invalid", K(ret), K(dml_ops));
      } else {
        const ObTableModify* dml_op = static_cast<const ObTableModify*>(dml_ops.at(0));
        phy_op->set_dml_location_key(dml_op->get_table_id());
        phy_op->set_dml_op_id(dml_op->get_id());
        const_cast<ObTableModify*>(dml_op)->set_gi_above(true);
      }
    }
    ARRAY_FOREACH(tsc_ops, idx)
    {
      if (OB_ISNULL(tsc_ops.at(idx)) || OB_UNLIKELY(!tsc_ops.at(idx)->is_table_scan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tsc ops is invalid", K(ret), K(tsc_ops));
      } else {
        const ObTableScan* tsc_op = static_cast<const ObTableScan*>(tsc_ops.at(idx));
        ObLightGranuleIterator::ObLGIScanInfo lgi_scan_info;
        lgi_scan_info.table_location_key_ = tsc_op->get_table_location_key();
        lgi_scan_info.ref_table_id_ = tsc_op->get_location_table_id();
        lgi_scan_info.tsc_op_id_ = tsc_op->get_id();
        const_cast<ObTableScan*>(tsc_op)->set_gi_above(true);
        if (OB_FAIL(phy_op->add_lgi_scan_info(lgi_scan_info))) {
          LOG_WARN("add lgi scan info failed", K(ret), K(lgi_scan_info));
        }
      }
    }
    // init array param indexs
    ObExecContext* exec_ctx = log_plan->get_optimizer_context().get_exec_ctx();
    if (OB_SUCC(ret) && exec_ctx != nullptr) {
      ObSEArray<int64_t, 16> array_param_idxs;
      const ParamStore& param_store = exec_ctx->get_physical_plan_ctx()->get_param_store();
      for (int64_t i = 0; OB_SUCC(ret) && i < param_store.count(); ++i) {
        if (param_store.at(i).is_ext()) {
          if (OB_FAIL(array_param_idxs.push_back(i))) {
            LOG_WARN("add array param index failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(phy_op->append_array_param_idxs(array_param_idxs))) {
        LOG_WARN("append array param idxs failed", K(ret), K(array_param_idxs));
      }
    }
    if (OB_SUCC(ret)) {
      ObPhyPlanType execute_type = phy_plan_->get_plan_type();
      if (execute_type == OB_PHY_PLAN_LOCAL) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported at this time", K(ret));
      } else {
        LOG_DEBUG("executor will fill the input of GI operator");
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_granule_iterator(
    ObLogGranuleIterator& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  UNUSED(op);
  UNUSED(child_ops);
  UNUSED(out_ops);
  ObGranuleIterator* phy_op = NULL;
  ObLogicalOperator* child_log_op = NULL;
  ObLogTableScan* log_tsc = NULL;
  RowDesc* out_row_desc = NULL;
  RowDesc* input_row_desc = NULL;
  bool partition_granule = false;
  if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong # of children", K(ret), K(child_ops.count()));
  } else if (FALSE_IT(input_row_desc = child_ops.at(0).second)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(child_log_op = op.get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(create_phy_op_desc(PHY_GRANULE_ITERATOR, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(input_row_desc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(input_row_desc), K(phy_op));
  } else if (OB_FAIL(copy_row_desc(*input_row_desc, *out_row_desc))) {
    LOG_WARN("failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else {
    phy_op->set_tablet_size(op.get_tablet_size());
    phy_op->set_gi_flags(op.get_gi_flags());
    if (log_op_def::LOG_TABLE_SCAN == child_log_op->get_type()) {
      log_tsc = static_cast<ObLogTableScan*>(child_log_op);
      phy_op->set_related_id(log_tsc->get_ref_table_id());
    }

    ObPhyPlanType execute_type = phy_plan_->get_plan_type();
    if (execute_type == OB_PHY_PLAN_LOCAL) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported at this time", K(ret));
    } else {
      LOG_DEBUG("executor will fill the input of GI operator");
    }
    LOG_TRACE("convert gi operator",
        K(ret),
        "tablet size",
        op.get_tablet_size(),
        "affinitize",
        op.access_all(),
        "pwj gi",
        op.pwj_gi(),
        "param down",
        op.with_param_down(),
        "asc",
        op.asc_partition_order(),
        "desc",
        op.desc_partition_order(),
        "flags",
        op.get_gi_flags());
  }
  return ret;
}

int ObCodeGeneratorImpl::fill_sort_columns(const ColumnIndexProviderImpl& idx_provider,
    const ObIArray<OrderItem>& sort_keys, ObPhyOperator* phy_op, ObSortableTrait& merge_receive)
{
  int ret = OB_SUCCESS;
  int64_t sort_idx = OB_INVALID_INDEX;
  if (OB_FAIL(merge_receive.init_sort_columns(sort_keys.count()))) {
    LOG_WARN("fail to init sort column", K(ret));
  } else if (NULL != phy_op && 
      OB_FAIL(phy_op->init_op_schema_obj(sort_keys.count()))) {
    LOG_WARN("fail to get op schema obj array", K(ret));
  }
  ARRAY_FOREACH(sort_keys, i)
  {
    const ObRawExpr* raw_expr = sort_keys.at(i).expr_;
    if (!raw_expr->has_flag(IS_COLUMNLIZED)) {
      if (raw_expr->has_flag(IS_CONST) || raw_expr->has_flag(IS_CONST_EXPR)) {
        continue;  // sort by const value, just ignore
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sort column should have be columnlized", K(*raw_expr));
      }
    } else if (OB_FAIL(idx_provider.get_idx(raw_expr, sort_idx))) {
      LOG_ERROR("failed to find column", K(ret), K(*raw_expr), K(raw_expr));
    } else if (OB_FAIL(merge_receive.add_sort_column(sort_idx,
                   raw_expr->get_collation_type(),
                   sort_keys.at(i).is_ascending(),
                   ITEM_TO_OBJ_TYPE(raw_expr->get_data_type()),
                   sort_keys.at(i).order_type_))) {
      LOG_WARN("failed to add typed sort column", K(ret), K(sort_idx));
    } else if (NULL != phy_op) {
      ObOpSchemaObj op_schema_obj(ITEM_TO_OBJ_TYPE(raw_expr->get_data_type()), sort_keys.at(i).order_type_);
      if (OB_FAIL(phy_op->get_op_schema_objs_for_update().push_back(op_schema_obj))) {
        LOG_WARN("failed to push back element", K(ret));
      } else {
        // do nothing
      }
    }
  }  // end for
  return ret;
}

int ObCodeGeneratorImpl::convert_cte_pump(ObLogTableScan& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  RowDesc* out_row_desc = nullptr;
  ObFakeCTETable* fake_cte_op = nullptr;
  ObPhyOperatorType op_type = PHY_FAKE_CTE_TABLE;
  if (0 != child_ops.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Wrong # of children", K(ret), K(child_ops.count()));
  } else if (OB_FAIL(create_phy_op_desc(op_type, fake_cte_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("Failed to create phy op and desc", K(ret));
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(fake_cte_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get an unexpected null", K(ret), K(out_row_desc), K(fake_cte_op));
  } else if (OB_FAIL(phy_cte_tables_.push_back(fake_cte_op))) {
    LOG_WARN("Failed to push back fake cte table", K(ret));
  } else {

    const ObIArray<ObRawExpr*>& access_exprs = op.get_access_exprs();
    LOG_DEBUG("Table scan's access columns", K(access_exprs.count()));
    OZ(fake_cte_op->column_involved_offset_.init(access_exprs.count()));
    ARRAY_FOREACH(access_exprs, i)
    {
      ObRawExpr* expr = access_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (expr->has_flag(IS_CONST)) {
      } else if (OB_UNLIKELY(!expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", K(ret), KPC(expr));
      } else {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
        if (OB_UNLIKELY(col_expr->get_table_id() != op.get_table_id())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Expected basic column", K(ret), KPC(col_expr));
        } else if (OB_FAIL(out_row_desc->add_column(col_expr))) {
          LOG_WARN("Failed to add column desc", K(ret), K(i));
        } else if (OB_FAIL(col_expr->add_flag(IS_COLUMNLIZED))) {
          LOG_WARN("Failed to add flag IS_COLUMNLIZED", K(ret));
        } else if (OB_FAIL(fake_cte_op->column_involved_offset_.push_back(
                       col_expr->get_cte_generate_column_projector_offset()))) {
          LOG_WARN("Failed to add column offset", K(ret));
        }
      }
    }  // end for
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_table_dml_param(ObLogDelUpd& log_op, ObTableModify& phy_op)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = phy_op.get_index_tid();
  const ObLogPlan* log_plan = log_op.get_plan();
  ObSchemaGetterGuard* schema_guard = NULL;
  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid table id ", K(ret), K(table_id));
  } else if (OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL log plan", K(ret), KP(log_plan));
  } else if (OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL schema guard", K(ret));
  } else if (OB_FAIL(fill_table_dml_param(schema_guard, table_id, phy_op))) {
    LOG_ERROR("fail to fill table dml param", K(ret), K(table_id));
  }
  return ret;
}

int ObCodeGeneratorImpl::fill_table_dml_param(
    share::schema::ObSchemaGetterGuard* guard, const uint64_t table_id, ObTableModify& phy_op)
{
  int ret = OB_SUCCESS;
  int64_t t_version = OB_INVALID_VERSION;
  const ObTableSchema* table_schema = NULL;
  const ObTableSchema* index_schema = NULL;
  ObSEArray<const ObTableSchema*, 16> index_schemas;
  const uint64_t tenant_id = is_sys_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
  if (OB_ISNULL(guard) || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(guard), K(table_id));
  } else if (OB_FAIL(guard->get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(guard->get_schema_version(tenant_id, t_version))) {
    LOG_WARN("get tenant schema version fail", K(ret), K(tenant_id));
  } else if (OB_ISNULL(phy_op.get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null phy plan", K(ret));
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
      LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      index_schema = NULL;
      if (OB_FAIL(guard->get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("index schema is null", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (!index_schema->is_valid()) {
        // skip invalid index
      } else if (!index_schema->is_index_table()) {
        // skip none index table
      } else if (index_schema->is_global_index_table()) {
        // skip global index
      } else if (is_final_invalid_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
        // skip invalid index status
      } else if (OB_FAIL(index_schemas.push_back(index_schema))) {
        LOG_WARN("push index schema fail", K(ret), K(simple_index_infos.at(i).table_id_));
      }
    }

    if (OB_SUCC(ret) &&
        OB_FAIL(phy_op.get_table_param().convert(table_schema, index_schemas, t_version, phy_op.get_column_ids()))) {
      LOG_WARN("fail to convert table param", K(ret), K(phy_op.get_table_param()));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_monitoring_dump(
    ObLogMonitoringDump& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  int ret = OB_SUCCESS;
  ObMonitoringDump* phy_op = nullptr;
  RowDesc* out_row_desc = nullptr;
  RowDesc* input_row_desc = nullptr;
  if (OB_UNLIKELY(1 != child_ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Wrong # of children", K(ret), K(child_ops.count()));
  } else if (OB_FAIL(create_phy_op_desc(PHY_MONITORING_DUMP, phy_op, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("Failed to create phy op and desc", K(ret));
  } else if (FALSE_IT(input_row_desc = child_ops.at(0).second)) {
  } else if (OB_ISNULL(out_row_desc) || OB_ISNULL(input_row_desc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(out_row_desc), K(input_row_desc), K(phy_op));
  } else if (OB_FAIL(copy_row_desc(*input_row_desc, *out_row_desc))) {
    LOG_WARN("Failed to copy row desc", K(ret), K(*child_ops.at(0).second));
  } else {
    phy_op->set_flags(op.get_flags());
    phy_op->set_dst_op_id(op.get_dst_op_id());
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_global_index_merge_info(ObLogMerge& op, const TableColumns& table_columns,
    RowDesc& row_desc, common::ObIArray<ObPhyOperator*>& subplan_roots, ObTableDMLInfo& table_dml_info)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = nullptr;
  ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(op.get_stmt());
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), table_columns.index_dml_infos_.count()));

  if (OB_ISNULL(op.get_plan()) || OB_ISNULL(schema_guard = op.get_plan()->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null or schema guard is null", K(ret));
  }

  table_dml_info.set_enable_row_movement(true);
  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.index_dml_infos_.count(); i++) {
    bool is_global_index = (0 != i);
    const ObTableSchema* table_schema = nullptr;
    const IndexDMLInfo& index_dml_info = table_columns.index_dml_infos_.at(i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    DMLSubPlanArray& subplans = phy_dml_info.dml_subplans_;

    if (OB_FAIL(schema_guard->get_table_schema(index_dml_info.index_tid_, table_schema))) {
      LOG_WARN("get table schema failed", K(index_dml_info.index_tid_), K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(index_dml_info.index_tid_), K(ret));
    } else if (table_schema->is_user_table() && !table_schema->is_enable_row_movement()) {
      table_dml_info.set_enable_row_movement(false);
    }

    OZ(phy_dml_info.table_locs_.allocate_array(phy_plan_->get_allocator(), 1));
    RowDesc insert_row_desc;
    OZ(generate_merge_index_location(index_dml_info, op, insert_row_desc, phy_dml_info.table_locs_));
    OZ(subplans.allocate_array(phy_plan_->get_allocator(), ObMultiTableMerge::DML_OP_CNT));

    if (merge_stmt->has_update_clause()) {
      OZ(convert_delete_subplan(op, index_dml_info, row_desc, subplans.at(ObMultiTableMerge::DELETE_OP)));
      OZ(subplan_roots.push_back(subplans.at(ObMultiTableMerge::DELETE_OP).subplan_root_));
    }

    OZ(convert_insert_subplan(op, index_dml_info, insert_row_desc, subplans.at(ObMultiTableMerge::INSERT_OP)));
    OZ(subplan_roots.push_back(subplans.at(ObMultiTableMerge::INSERT_OP).subplan_root_));
  }

  return ret;
}

int ObCodeGeneratorImpl::generate_merge_index_location(
    const IndexDMLInfo& index_dml_info, ObLogMerge& op, RowDesc& insert_row_desc, TableLocationArray& new_tbl_loc)
{
  int ret = OB_SUCCESS;
  ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(op.get_stmt());

  const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
  OZ(insert_row_desc.init());

  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns->count(); i++) {
    OZ(insert_row_desc.add_column(table_columns->at(i)));
  }

  OZ(generate_index_location(index_dml_info, op, insert_row_desc, new_tbl_loc.at(0)));

  return ret;
}

int ObCodeGeneratorImpl::add_window_function_info(const ColumnIndexProviderImpl& idx_provider, ObPhyOperator* phy_op,
    ObWinFunRawExpr* win_expr, const RowDesc& input_row_desc, RowDesc& output_row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(win_expr) || OB_ISNULL(phy_op) || OB_ISNULL(phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret));
  } else {
    FuncInfo func_info;
    ObRawExpr* agg_expr = win_expr->get_agg_expr();
    const ObIArray<ObRawExpr*>& func_params = win_expr->get_func_params();
    if (OB_FAIL(func_info.set_allocator(&phy_plan_->get_allocator()))) {
      LOG_WARN("failed to set allocator.", K(ret));
    } else if (OB_FAIL(func_info.init(func_params.count(),
                   win_expr->get_partition_exprs().count(),
                   win_expr->get_order_items().count()))) {
      LOG_WARN("failed to init the func info.", K(ret));
    } else if (OB_FAIL(output_row_desc.add_column(win_expr))) {  // map expr to idx
      LOG_WARN("failed to add column desc", K(ret));
    } else if (OB_FAIL(win_expr->add_flag(IS_COLUMNLIZED))) {
      LOG_WARN("failed to add flag IS_COLUMNLIZED", K(ret));
    } else if (NULL == agg_expr) {  // do nothing.
    } else if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), func_info.aggr_column_))) {
      LOG_WARN("failed create expression for window function.", K(ret));
    } else if (OB_ISNULL(func_info.aggr_column_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected null.", K(ret));
    } else if (OB_FAIL(generate_expr(*agg_expr, input_row_desc, *func_info.aggr_column_))) {
      LOG_WARN("generate sql expr failed", K(ret));
    } else {
      func_info.aggr_column_->set_result_index(output_row_desc.get_column_num() - 1);
    }
    if (OB_FAIL(ret)) {
    } else {
      ObSqlExpression* upper_sql_exprs[BOUND_EXPR_MAX];
      ObSqlExpression* lower_sql_exprs[BOUND_EXPR_MAX];
      func_info.func_type_ = win_expr->get_func_type();
      func_info.win_type_ = win_expr->get_window_type();
      func_info.is_distinct_ = win_expr->is_distinct();
      func_info.is_ignore_null_ = win_expr->is_ignore_null();
      func_info.is_from_first_ = win_expr->is_from_first();
      func_info.my_phy_plan_ = phy_plan_;
      func_info.result_index_ = output_row_desc.get_column_num() - 1;

      func_info.upper_.is_preceding_ = win_expr->get_upper().is_preceding_;
      func_info.upper_.is_unbounded_ = BOUND_UNBOUNDED == win_expr->get_upper().type_;
      func_info.upper_.is_nmb_literal_ = win_expr->get_upper().is_nmb_literal_;
      func_info.upper_.my_phy_plan_ = phy_plan_;
      func_info.lower_.is_preceding_ = win_expr->get_lower().is_preceding_;
      func_info.lower_.is_unbounded_ = BOUND_UNBOUNDED == win_expr->get_lower().type_;
      func_info.lower_.is_nmb_literal_ = win_expr->get_lower().is_nmb_literal_;
      func_info.lower_.my_phy_plan_ = phy_plan_;
      // add window function params.
      ObSqlExpression* new_sql_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < func_params.count(); ++i) {
        if (OB_ISNULL(func_params.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("raw expr is null", K(ret), K(i));
        } else if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), new_sql_expr))) {
          LOG_WARN("create sql expression failed", K(ret));
        } else if (OB_ISNULL(new_sql_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("alloc invalid expr", K(ret), K(new_sql_expr));
        } else if (OB_FAIL(generate_expr(*func_params.at(i), input_row_desc, *new_sql_expr))) {
          LOG_WARN("generate sql expr failed", K(ret));
        } else if (OB_FAIL(func_info.params_.push_back(new_sql_expr))) {
          LOG_WARN("push back sql expr failed", K(ret));
        } else { /* do nothing.*/
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < 2; ++j) {
        Bound* bound = 0 == j ? &win_expr->get_upper() : &win_expr->get_lower();
        ObSqlExpression* tmp_sql_expr = NULL;
        for (int64_t k = 0; OB_SUCC(ret) && k < BOUND_EXPR_MAX; ++k) {
          if (OB_FAIL(generate_sql_expr_for_window_function(phy_op, input_row_desc, bound->exprs_[k], tmp_sql_expr))) {
            LOG_WARN("failed to generate sql expr for window function.", K(ret));
          } else { /*do nothing.*/
          }
          if (OB_FAIL(ret)) {
          } else if (0 == j) {
            upper_sql_exprs[k] = tmp_sql_expr;
          } else {
            lower_sql_exprs[k] = tmp_sql_expr;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_sql_expr_for_window_function(
                     phy_op, input_row_desc, win_expr->get_upper().interval_expr_, func_info.upper_.sql_expr_))) {
        LOG_WARN("failed generate sql expr for window function.", K(ret));
      } else if (OB_FAIL(generate_sql_expr_for_window_function(
                     phy_op, input_row_desc, win_expr->get_lower().interval_expr_, func_info.lower_.sql_expr_))) {
        LOG_WARN("failed generate sql expr for window function.", K(ret));
      } else if (FALSE_IT(
                     MEMCPY(func_info.upper_.sql_exprs_, upper_sql_exprs, sizeof(ObSqlExpression*) * BOUND_EXPR_MAX))) {
      } else if (FALSE_IT(
                     MEMCPY(func_info.lower_.sql_exprs_, lower_sql_exprs, sizeof(ObSqlExpression*) * BOUND_EXPR_MAX))) {
      } else if (OB_FAIL(generate_part_and_sort_columns(idx_provider,
                     win_expr->get_partition_exprs(),
                     win_expr->get_order_items(),
                     func_info.partition_cols_,
                     func_info.sort_cols_))) {
        LOG_WARN("failed to generate part and sort columns.", K(ret));
      } else if (OB_FAIL(static_cast<ObWindowFunction*>(phy_op)->add_func_info(func_info))) {
        LOG_WARN("faield to add func info into window function", K(ret));
      } else { /* do nothing. */
      }
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_part_and_sort_columns(const ColumnIndexProviderImpl& idx_provider,
    const ObIArray<ObRawExpr*>& partition_exprs, const ObIArray<OrderItem>& order_items,
    common::ObIArray<ObColumnInfo>& partition_cols, common::ObIArray<ObSortColumn>& sort_cols)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_exprs.count(); i++) {
    bool skip = false;
    int64_t cell_idx = OB_INVALID_INDEX;
    const ObRawExpr* raw_expr = partition_exprs.at(i);
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get unexpected null", K(ret));
    } else if (OB_FAIL(get_cell_idx(idx_provider, raw_expr, cell_idx, skip))) {
      LOG_WARN("failed to get cell idx.", K(ret));
    } else if (!skip) {
      ObColumnInfo column;
      column.index_ = cell_idx;
      column.cs_type_ = raw_expr->get_collation_type();
      ret = partition_cols.push_back(column);
    } else { /* do nothing. */
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); i++) {
    bool skip = false;
    int64_t cell_idx = OB_INVALID_INDEX;
    const ObRawExpr* raw_expr = order_items.at(i).expr_;
    bool is_asc = order_items.at(i).is_ascending();
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get unexpected null", K(ret));
    } else if (OB_FAIL(get_cell_idx(idx_provider, raw_expr, cell_idx, skip))) {
      LOG_WARN("failed to get cell idx.", K(ret));
    } else if (!skip) {
      ObSortColumn column;
      column.index_ = cell_idx;
      column.set_is_ascending(is_asc);
      column.cs_type_ = raw_expr->get_collation_type();
      ret = sort_cols.push_back(column);
    } else { /* do nothing. */
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::generate_sql_expr_for_window_function(
    ObPhyOperator* phy_op, const RowDesc& input_row_desc, ObRawExpr*& raw_expr, ObSqlExpression*& sql_expr)
{
  int ret = OB_SUCCESS;
  ObSqlExpression* _tmp_sql_expr = NULL;
  if (OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret));
  } else if (NULL == raw_expr) {  // do nothing.
  } else if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), _tmp_sql_expr))) {
    LOG_WARN("failed create expression for window function.", K(ret));
  } else if (OB_ISNULL(_tmp_sql_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("alloc invalid _tmp_sql_expr", K(ret));
  } else if (OB_FAIL(generate_expr(*raw_expr, input_row_desc, *_tmp_sql_expr))) {
    LOG_WARN("generate sql expr failed", K(ret));
  } else {
    sql_expr = _tmp_sql_expr;
  }
  return ret;
}

int ObCodeGeneratorImpl::get_cell_idx(
    const ColumnIndexProviderImpl& idx_provider, const ObRawExpr* raw_expr, int64_t& cell_idx, bool& skip)
{
  int ret = OB_SUCCESS;
  skip = false;
  cell_idx = OB_INVALID_INDEX;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret));
  } else if (raw_expr->has_flag(IS_CONST) || raw_expr->has_flag(IS_CONST_EXPR)) {
    skip = true;
    LOG_TRACE("skip const expr", K(*raw_expr), K(cell_idx));
  }
  if (OB_SUCC(ret) && !skip) {
    if (!raw_expr->has_flag(IS_COLUMNLIZED)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("param expr should have be columnlized", K(*raw_expr));
    } else if (OB_FAIL(idx_provider.get_idx(raw_expr, cell_idx))) {
      LOG_ERROR("partition by expr should have be columnlized", K(ret), K(*raw_expr));
    }
  }
  return ret;
}

int ObCodeGeneratorImpl::convert_unpivot(ObLogUnpivot& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops)
{
  UNUSED(child_ops);
  int ret = OB_SUCCESS;
  ObUnpivot* unpivot = NULL;
  RowDesc* out_row_desc = NULL;
  ObUnpivotInfo& info = op.unpivot_info_;
  const ObIArray<ObRawExpr*>& access_exprs = op.get_access_exprs();
  if (OB_UNLIKELY(!info.has_unpivot()) ||
      OB_UNLIKELY((access_exprs.count() - info.old_column_count_) % info.get_new_column_count() != 0) ||
      OB_UNLIKELY((access_exprs.count() - info.old_column_count_) / info.get_new_column_count() <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unpivot arrgument is unexpectd", K(ret), K(access_exprs.count()), K(info));
  } else if (OB_FAIL(create_phy_op_desc(PHY_UNPIVOT, unpivot, out_row_desc, out_ops, op.get_op_id()))) {
    LOG_WARN("create subquery scan op and desc failed", K(ret));
  } else {
    unpivot->unpivot_info_ = op.unpivot_info_;

    if (OB_FAIL(unpivot->init_output_index(access_exprs.count()))) {
      OB_LOG(WARN, "fail to init output index", K(ret));
    }
    ARRAY_FOREACH(access_exprs, i)
    {
      if (OB_ISNULL(access_exprs.at(i)) || !access_exprs.at(i)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expected basic column expr", K(ret));
      } else {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(access_exprs.at(i));
        if (OB_FAIL(unpivot->add_output_index(col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID))) {
          LOG_WARN("failed to add column", K(ret), K(i));
        } else if (!col_expr->is_unpivot_mocked_column()) {
          if (OB_FAIL(out_row_desc->add_column(col_expr))) {
            LOG_WARN("failed to add column desc", K(ret), K(i));
          } else if (OB_FAIL(col_expr->add_flag(IS_COLUMNLIZED))) {
            LOG_WARN("failed to add flag IS_COLUMNLIZED", K(ret));
          }
        }
      }
    }  // end for
    LOG_DEBUG("finish convert_unpivot", KPC(unpivot));
  }
  return ret;
}

// if is heap table and part table, no need to check pk is null.
// because part key can be null, and part key is hidden primary key in heap table.
int ObCodeGeneratorImpl::set_need_check_pk_is_null(ObLogDelUpd& op, ObTableModify* phy_op)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  bool need_ck = true;
  CK(OB_NOT_NULL(phy_op));
  CK(OB_NOT_NULL(op.get_all_table_columns()));
  if (OB_SUCC(ret)) {
    const uint64_t table_id = op.get_all_table_columns()->at(0).index_dml_infos_.at(0).index_tid_;
    const ObLogPlan* log_plan = op.get_plan();
    CK(OB_INVALID_ID != table_id);
    CK(OB_NOT_NULL(log_plan));

    CK(OB_NOT_NULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard()));
    CK(OB_NOT_NULL(schema_guard->get_schema_guard()));
    OZ(schema_guard->get_table_schema(table_id, table_schema));
    CK(OB_NOT_NULL(table_schema));
    OX(need_ck = !(table_schema->is_new_no_pk_table() && table_schema->is_partitioned_table()));
    OX(phy_op->set_need_check_pk_is_null(need_ck));
  }
  return ret;
}
