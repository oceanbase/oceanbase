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
#include "sql/optimizer/ob_log_mv_table_scan.h"
#include "sql/optimizer/ob_log_plan.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObLogMVTableScan::ObLogMVTableScan(ObLogPlan& plan)
    : ObLogTableScan(plan), right_table_partition_info_(plan.get_allocator()), depend_table_id_(OB_INVALID_ID)
{}

int ObLogMVTableScan::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogTableScan::copy_without_child(out))) {
    LOG_WARN("fail to copy_without_child", K(ret));
  } else {
    static_cast<ObLogMVTableScan*>(out)->depend_table_id_ = depend_table_id_;
  }
  return ret;
}

int ObLogMVTableScan::index_back_check()
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  const ObTableSchema* table_schema = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  uint64_t ref_id = get_ref_table_id();
  const ObIArray<ObRawExpr*>& filters = get_filter_exprs();
  filter_before_index_back_.reset();
  ObBitSet<> index_and_basetable_rk_set;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_plan is NULL or get_plan()->get_optimizer_context() returns null", K(get_plan()), K(opt_ctx), K(ret));
  } else if (NULL == (schema_guard = opt_ctx->get_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument passed in", K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_id, table_schema)) || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid schema", K(table_schema), K(ref_id), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); i++) {
    const ObColumnSchemaV2* col = table_schema->get_column_schema_by_idx(i);
    uint64_t cid = col->get_column_id();
    if (OB_FAIL(index_and_basetable_rk_set.add_member(cid))) {
      LOG_WARN("fail to add cid to bitset", K(ret), K(cid));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptimizerUtil::check_if_column_ids_covered(
            filters, index_and_basetable_rk_set, false, OB_INVALID_ID, filter_before_index_back_))) {
      LOG_WARN("Failed to check filter index back");
    }
  }
  return ret;
}

int ObLogMVTableScan::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;

  ObDMLStmt* stmt = NULL;
  ObOptimizerContext* opt_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt()) ||
      OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret));
  } else {
    const ObTableSchema* table_schema = NULL;
    ObSchemaGetterGuard* schema_guard = opt_ctx->get_schema_guard();
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Null pointer in get_plan()->get_optimizer_context()->get_schema_guard()", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(get_index_table_id(), table_schema)) || OB_ISNULL(table_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("table schema is null", K(ref_table_id_), K(ret));
    } else if (table_schema->get_join_conds().count() % 2 != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid join condition count", K(ret), "join_cnd_cnt", table_schema->get_join_conds().count());
    } else {
      uint64_t cid = OB_INVALID_ID;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_join_conds().count(); i++) {
        const std::pair<uint64_t, uint64_t>& t1 = table_schema->get_join_conds().at(i);
        if (t1.first == ref_table_id_) {
          cid = t1.second;
        } else {
          cid = table_schema->gen_materialized_view_column_id(t1.second);
        }

        ObColumnRefRawExpr* expr = NULL;
        const ObColumnSchemaV2* col = table_schema->get_column_schema(cid);
        ObRawExpr* raw_expr = stmt->get_column_expr_by_id(get_table_id(), cid);

        if (NULL == col) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cannot find get column", K(ret), K(cid));
        } else if (NULL != raw_expr) {
          expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
        } else if (OB_FAIL(opt_ctx->get_expr_factory().create_raw_expr(T_REF_COLUMN, expr))) {
          LOG_WARN("failed to create raw expr from index column", K(i), K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(i), K(ret));
        } else {
          expr->set_ref_id(get_table_id(), cid);
          ObObjMeta meta_type = col->get_meta_type();
          if (ob_is_string_or_lob_type(col->get_data_type())) {
            meta_type.set_collation_level(CS_LEVEL_IMPLICIT);
          }
          expr->set_meta_type(meta_type);

          if (OB_FAIL(expr->formalize(opt_ctx->get_session_info()))) {
            LOG_WARN("failed to formalize the new expr", K(ret), K(col->get_meta_type()), K(cid));
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(access_exprs_.push_back(expr))) {
            LOG_WARN("failed to add row key expr", K(ret));
          } else { /* do nothing */
          }
        } else { /* do nothing */
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogTableScan::allocate_expr_post(ctx))) {
      LOG_WARN("fail to allocate_expr_post", K(ret));
    }
  }

  return ret;
}

int ObLogMVTableScan::init_table_location_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogTableScan::init_table_location_info())) {
    LOG_WARN("fail to calculate_table_location", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard* schema_guard = NULL;
    ObSqlSchemaGuard* sql_schema_guard = NULL;
    ObIPartitionLocationCache* location_cache = NULL;
    ObDMLStmt* stmt = NULL;
    ObOptimizerContext* opt_ctx = NULL;
    ObExecContext* exec_ctx = NULL;
    const ParamStore* params = NULL;
    ObSEArray<ObRawExpr*, 5> part_order;

    if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt())) ||
        OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) || OB_ISNULL(exec_ctx = opt_ctx->get_exec_ctx()) ||
        OB_ISNULL(schema_guard = opt_ctx->get_schema_guard()) ||
        OB_ISNULL(sql_schema_guard = opt_ctx->get_sql_schema_guard()) ||
        OB_ISNULL(location_cache = opt_ctx->get_location_cache()) || OB_ISNULL(params = opt_ctx->get_params()) ||
        OB_ISNULL(table_partition_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get unexpected null",
          K(ret),
          K(get_plan()),
          K(stmt),
          K(opt_ctx),
          K(exec_ctx),
          K(schema_guard),
          KP(sql_schema_guard),
          K(location_cache),
          K(params),
          K(table_partition_info_));
    } else {
      const common::ObDataTypeCastParams dtc_params =
          ObBasicSessionInfo::create_dtc_params(opt_ctx->get_session_info());
      ObArray<ObRawExpr*> correlated_filters;
      ObArray<ObRawExpr*> uncorrelated_filters;
      ObReplicaLocation first_rep_loc;
      ObReplicaLocation cur_rep_loc;
      if (OB_FAIL(ObOptimizerUtil::extract_parameterized_correlated_filters(
              get_filter_exprs(), params->count(), correlated_filters, uncorrelated_filters))) {
        LOG_WARN("Failed to extract correlated filters", K(ret));
      } else if (OB_FAIL(right_table_partition_info_.init_table_location(*sql_schema_guard,
                     *stmt,
                     exec_ctx->get_my_session(),
                     uncorrelated_filters,
                     depend_table_id_,
                     depend_table_id_,
                     part_hint_,
                     dtc_params,
                     false,
                     &part_order))) {
        LOG_WARN("Failed to initialize table location", K(ret), K(depend_table_id_));
      } else {
        const ObPhyTableLocationInfo& phy_tbl_location = table_partition_info_->get_phy_tbl_location_info();
        const ObPhyPartitionLocationInfoIArray& part_loc_list = phy_tbl_location.get_phy_part_loc_info_list();
        if (part_loc_list.count() < 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part_loc_list count cannot be zero");
        } else {
          if (OB_FAIL(part_loc_list.at(0).get_selected_replica(first_rep_loc))) {
            LOG_WARN("fail to get first selected replica", K(ret), K(part_loc_list.at(0)));
          } else if (OB_UNLIKELY(!first_rep_loc.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("first replica location is invalid", K(ret), K(first_rep_loc));
          }
          for (int64_t i = 1; OB_SUCC(ret) && i < part_loc_list.count(); i++) {
            cur_rep_loc.reset();
            if (OB_FAIL(part_loc_list.at(i).get_selected_replica(cur_rep_loc))) {
              LOG_WARN("fail to get cur selected replica", K(ret), K(part_loc_list.at(i)));
            } else if (OB_UNLIKELY(!cur_rep_loc.is_valid())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("cur replica location is invalid", K(ret), K(cur_rep_loc));
            } else if (first_rep_loc.server_ != cur_rep_loc.server_) {
              ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_ERR_DISTRIBUTED_NOT_SUPPORTED, "materialized view across distributed node");
              break;
            }
          }
        }
        /*if (OB_SUCCESS == ret) {
          right_table_partition_info_.get_table_location().set_fixed_replica_location(first_rep_loc.server_);
        }*/
      }
      if (OB_SUCCESS != ret) {
        // do nothing
      } else if (OB_FAIL(right_table_partition_info_.calc_phy_table_loc_and_select_fixed_location(*exec_ctx,
                     first_rep_loc.server_,
                     schema_guard,
                     *params,
                     *location_cache,
                     dtc_params,
                     index_table_id_,
                     scan_direction_))) {
        LOG_WARN("Failed to calculate table location and select fixed location", K(ret));
      } else if (OB_FAIL(get_plan()->add_global_table_partition_info(&right_table_partition_info_))) {
        LOG_WARN("Failed to add global table partition info", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

uint64_t ObLogMVTableScan::hash(uint64_t seed) const
{
  seed = do_hash(depend_table_id_, seed);
  seed = ObLogTableScan::hash(seed);

  return seed;
}
