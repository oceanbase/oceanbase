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
#include "sql/code_generator/ob_tsc_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "share/inner_table/ob_inner_table_schema.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

int ObTscCgService::generate_tsc_ctdef(ObLogTableScan &op, ObTableScanCtDef &tsc_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASScanCtDef &scan_ctdef = tsc_ctdef.scan_ctdef_;
  ObQueryFlag query_flag;
  if (op.is_need_feedback() &&
      (op.get_plan()->get_optimizer_context().get_phy_plan_type() == OB_PHY_PLAN_LOCAL ||
        op.get_plan()->get_optimizer_context().get_phy_plan_type() == OB_PHY_PLAN_REMOTE)) {
    ++(cg_.phy_plan_->get_access_table_num());
    query_flag.is_need_feedback_ = true;
  }

  ObOrderDirection scan_direction = op.get_scan_direction();
  if (is_descending_direction(scan_direction)) {
    query_flag.scan_order_ = ObQueryFlag::Reverse;
  } else {
    query_flag.scan_order_ = ObQueryFlag::Forward;
  }
  tsc_ctdef.scan_flags_ = query_flag;

  if (OB_SUCC(ret) && op.get_table_type() == share::schema::EXTERNAL_TABLE) {
    const ObTableSchema *table_schema = nullptr;
    ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
    ObBasicSessionInfo *session_info = cg_.opt_ctx_->get_session_info();
    if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is null", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(),
                                               op.get_ref_table_id(),
                                               op.get_stmt(),
                                               table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
    } else if (OB_FAIL(ObSQLUtils::check_location_access_priv(table_schema->get_external_file_location(),
                                                              cg_.opt_ctx_->get_session_info()))) {
      LOG_WARN("fail to check location access priv", K(ret));
    } else {
      scan_ctdef.is_external_table_ = true;
      if (OB_FAIL(scan_ctdef.external_file_format_str_.store_str(table_schema->get_external_file_format()))) {
        LOG_WARN("fail to set string", K(ret));
      } else if (OB_FAIL(scan_ctdef.external_file_location_.store_str(table_schema->get_external_file_location()))) {
        LOG_WARN("fail to set string", K(ret));
      } else if (OB_FAIL(scan_ctdef.external_file_access_info_.store_str(table_schema->get_external_file_location_access_info()))) {
        LOG_WARN("fail to set access info", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && (OB_NOT_NULL(op.get_flashback_query_expr()))) {
    if (OB_FAIL(cg_.generate_rt_expr(*op.get_flashback_query_expr(),
                                     tsc_ctdef.flashback_item_.flashback_query_expr_))) {
      LOG_WARN("generate flashback query expr failed", K(ret));
    } else {
      const ObExecContext *exec_ctx = nullptr;
      tsc_ctdef.flashback_item_.flashback_query_type_ = op.get_flashback_query_type();
      tsc_ctdef.flashback_item_.fq_read_tx_uncommitted_ = op.get_fq_read_tx_uncommitted();
      if (OB_ISNULL(exec_ctx = cg_.opt_ctx_->get_exec_ctx())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else if (cg_.opt_ctx_->is_online_ddl()) {
        ObSqlCtx *sql_ctx = const_cast<ObExecContext *>(exec_ctx)->get_sql_ctx();
        sql_ctx->flashback_query_expr_ = op.get_flashback_query_expr();
      }
    }
  }
  if (OB_SUCC(ret)) {
    bool has_rowscn = false;
    scan_ctdef.ref_table_id_ = op.get_real_index_table_id();
    if (OB_FAIL(generate_das_scan_ctdef(op, scan_ctdef, has_rowscn))) {
      LOG_WARN("generate das scan ctdef failed", K(ret), K(scan_ctdef.ref_table_id_));
    } else {
      tsc_ctdef.flashback_item_.need_scn_ |= has_rowscn;
    }
  }

  //to generate dynamic tsc partition pruning info
  if (OB_SUCC(ret)) {
    ObTablePartitionInfo *info = op.get_table_partition_info();
    const ObTableSchema *table_schema = nullptr;
    ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table location info", K(ret));
    } else if (info->get_table_location().use_das() &&
               info->get_table_location().get_has_dynamic_exec_param()) {
      if (OB_FAIL(tsc_ctdef.allocate_dppr_table_loc())) {
        LOG_WARN("allocate dppr table location failed", K(ret));
      } else if (OB_FAIL(tsc_ctdef.das_dppr_tbl_->assign(info->get_table_location()))) {
        LOG_WARN("assign dynamic partition pruning table location failed", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(),
                                                        scan_ctdef.ref_table_id_,
                                                        op.get_stmt(),
                                                        table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
      } else if (OB_FAIL(generate_table_loc_meta(op.get_table_id(),
                                                 *op.get_stmt(),
                                                 *table_schema,
                                                 *cg_.opt_ctx_->get_session_info(),
                                                 tsc_ctdef.das_dppr_tbl_->get_loc_meta()))) {
        LOG_WARN("generate table loc meta failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && op.get_index_back()) {
    void *lookup_buf = cg_.phy_plan_->get_allocator().alloc(sizeof(ObDASScanCtDef));
    void *loc_meta_buf = cg_.phy_plan_->get_allocator().alloc(sizeof(ObDASTableLocMeta));
    if (OB_ISNULL(lookup_buf) || OB_ISNULL(loc_meta_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate lookup ctdef buffer failed", K(ret), K(lookup_buf), K(loc_meta_buf));
    } else {
      bool has_rowscn = false;
      const ObTableSchema *table_schema = nullptr;
      ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
      tsc_ctdef.lookup_ctdef_ = new(lookup_buf) ObDASScanCtDef(cg_.phy_plan_->get_allocator());
      tsc_ctdef.lookup_ctdef_->ref_table_id_ = op.get_real_ref_table_id();
      tsc_ctdef.lookup_loc_meta_ = new(loc_meta_buf) ObDASTableLocMeta(cg_.phy_plan_->get_allocator());

      if (OB_FAIL(generate_das_scan_ctdef(op, *tsc_ctdef.lookup_ctdef_, has_rowscn))) {
        LOG_WARN("generate das lookup scan ctdef failed", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(),
                                                        op.get_ref_table_id(),
                                                        op.get_stmt(),
                                                        table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
      } else if (OB_FAIL(generate_table_loc_meta(op.get_table_id(),
                                                 *op.get_stmt(),
                                                 *table_schema,
                                                 *cg_.opt_ctx_->get_session_info(),
                                                 *tsc_ctdef.lookup_loc_meta_))) {
        LOG_WARN("generate table loc meta failed", K(ret));
      } else {
        tsc_ctdef.flashback_item_.need_scn_ |= has_rowscn;
      }

      if (OB_SUCC(ret) && op.get_index_back() && op.get_is_index_global()) {
        if (OB_ISNULL(op.get_calc_part_id_expr()) || op.get_rowkey_exprs().empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("calc_part_id_expr is null or rowkeys` count is zero", K(ret));
        } else if (OB_FAIL(cg_.generate_calc_part_id_expr(*op.get_calc_part_id_expr(),
                                                        tsc_ctdef.lookup_loc_meta_,
                                                        tsc_ctdef.calc_part_id_expr_))) {
          LOG_WARN("fail to generate calc part id expr", K(ret), KP(op.get_calc_part_id_expr()));
        } else if (OB_FAIL(cg_.generate_rt_exprs(op.get_rowkey_exprs(), tsc_ctdef.global_index_rowkey_exprs_))) {
          LOG_WARN("fail to generate rowkey exprs", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObArray<int64_t> bnlj_params;
    OZ(op.extract_bnlj_param_idxs(bnlj_params));
    OZ(tsc_ctdef.bnlj_param_idxs_.assign(bnlj_params));
  }

  OZ (cg_.generate_rt_exprs(op.get_ext_file_column_exprs(),
                            tsc_ctdef.scan_ctdef_.pd_expr_spec_.ext_file_column_exprs_));
  OZ (cg_.generate_rt_exprs(op.get_ext_column_convert_exprs(),
                            tsc_ctdef.scan_ctdef_.pd_expr_spec_.ext_column_convert_exprs_));
  if (OB_SUCC(ret)) {
    for (int i = 0; i < op.get_ext_file_column_exprs().count(); i++) {
      tsc_ctdef.scan_ctdef_.pd_expr_spec_.ext_file_column_exprs_.at(i)->extra_
          = op.get_ext_file_column_exprs().at(i)->get_extra();
    }
  }

  LOG_DEBUG("generate tsc ctdef finish", K(ret), K(op), K(tsc_ctdef));
  return ret;
}

int ObTscCgService::generate_table_param(const ObLogTableScan &op, ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObTableID index_id = scan_ctdef.ref_table_id_;
  const ObTableSchema *table_schema = NULL;
  const bool pd_agg = ObPushdownFilterUtils::is_aggregate_pushdown_storage(scan_ctdef.pd_expr_spec_.pd_storage_flag_);
  ObArray<uint64_t> tsc_out_cols;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  CK(OB_NOT_NULL(schema_guard));
  if (OB_INVALID == index_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id", K(index_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(), index_id, op.get_stmt(), table_schema))) {
    LOG_WARN("get table schema failed", K(index_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(table_schema));
  } else if (table_schema->is_spatial_index() && FALSE_IT(scan_ctdef.table_param_.set_is_spatial_index(true))) {
  } else if (OB_FAIL(extract_das_output_column_ids(op, index_id, *table_schema, tsc_out_cols))) {
    LOG_WARN("extract tsc output column ids failed", K(ret));
  } else if (FALSE_IT(scan_ctdef.table_param_.get_enable_lob_locator_v2()
                          = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0))) {
  } else if (OB_FAIL(scan_ctdef.table_param_.convert(*table_schema,
                                                     scan_ctdef.access_column_ids_,
                                                     &tsc_out_cols,
                                                     is_oracle_mapping_real_virtual_table(op.get_ref_table_id())))) {/* for real agent table , use mysql mode compulsory*/
    LOG_WARN("convert schema failed", K(ret), K(*table_schema),
             K(scan_ctdef.access_column_ids_), K(op.get_index_back()));
  } else if (pd_agg && OB_FAIL(scan_ctdef.table_param_.convert_agg(scan_ctdef.access_column_ids_,
                                                                   scan_ctdef.aggregate_column_ids_))) {
    LOG_WARN("convert agg failed", K(ret), K(*table_schema),
              K(scan_ctdef.aggregate_column_ids_), K(op.get_index_back()));
  } else if (OB_FAIL(generate_das_result_output(tsc_out_cols, scan_ctdef, op.get_trans_info_expr(), pd_agg))) {
    LOG_WARN("failed to init result outputs", K(ret));
  }
  return ret;
}

int ObTscCgService::generate_das_result_output(const ObIArray<uint64_t> &output_cids,
                                               ObDASScanCtDef &scan_ctdef,
                                               const ObRawExpr *trans_info_expr,
                                               const bool include_agg)
{
  int ret = OB_SUCCESS;
  ExprFixedArray &access_exprs = scan_ctdef.pd_expr_spec_.access_exprs_;
  ExprFixedArray &agg_exprs = scan_ctdef.pd_expr_spec_.pd_storage_aggregate_output_;
  int64_t access_column_cnt = scan_ctdef.access_column_ids_.count();
  int64_t access_expr_cnt = access_exprs.count();
  int64_t agg_expr_cnt = include_agg ? agg_exprs.count() : 0;
  int64_t trans_expr_cnt = trans_info_expr == nullptr ? 0 : 1;
  if (OB_UNLIKELY(access_column_cnt != access_expr_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access column count is invalid", K(ret), K(access_column_cnt), K(access_expr_cnt));
  } else if (OB_FAIL(scan_ctdef.result_output_.init(output_cids.count() + agg_expr_cnt + trans_expr_cnt))) {
    LOG_WARN("init result output failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_cids.count(); ++i) {
    int64_t idx = OB_INVALID_INDEX;
    if (!has_exist_in_array(scan_ctdef.access_column_ids_, output_cids.at(i), &idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output column does not exist in access column ids", K(ret),
               K(scan_ctdef.access_column_ids_), K(output_cids.at(i)), K(output_cids),
               K(scan_ctdef.ref_table_id_), K(scan_ctdef.ref_table_id_));
    } else if (OB_FAIL(scan_ctdef.result_output_.push_back(access_exprs.at(idx)))) {
      LOG_WARN("store access expr to result output exprs failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && include_agg) {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_expr_cnt; ++i) {
      if (scan_ctdef.result_output_.push_back(agg_exprs.at(i))) {
        LOG_WARN("store agg expr to result output exprs failed", K(ret), K(agg_expr_cnt));
      }
    }
  }

  // When the lookup occurs, the result_output of the das task
  // during index_scan and the main table lookup will have trans_info_expr
  if (OB_SUCC(ret) && trans_expr_cnt > 0) {
    ObExpr *e = NULL;
    if (OB_FAIL(cg_.generate_rt_expr(*trans_info_expr, e))) {
      LOG_WARN("fail to generate rt exprt", K(ret), KPC(trans_info_expr));
    } else if (OB_FAIL(scan_ctdef.result_output_.push_back(e))) {
      LOG_WARN("fail to push back trans_info expr", K(ret));
    } else {
      scan_ctdef.trans_info_expr_ = e;
    }
  }
  return ret;
}

int ObTscCgService::generate_agent_vt_access_meta(const ObLogTableScan &op, ObTableScanSpec &spec)
{
  int ret = OB_SUCCESS;
  AgentVtAccessMeta &agent_vt_meta = spec.agent_vt_meta_;
  ObArray<ObRawExpr*> tsc_columns; //these columns need by TSC operator
  VTMapping *vt_mapping = nullptr;
  const ObTableSchema *table_schema = nullptr;
  agent_vt_meta.vt_table_id_ = op.get_ref_table_id();
  spec.is_vt_mapping_ = true;
  get_real_table_vt_mapping(op.get_ref_table_id(), vt_mapping);
  if (OB_FAIL(extract_tsc_access_columns(op, tsc_columns))) {
    LOG_WARN("extract column exprs need by TSC operator failed", K(ret));
  } else if (OB_FAIL(agent_vt_meta.access_exprs_.init(tsc_columns.count()))) {
    LOG_WARN("init access expr capacity failed", K(ret), K(tsc_columns.count()));
  } else if (OB_FAIL(agent_vt_meta.access_column_ids_.init(tsc_columns.count()))) {
    LOG_WARN("init access column ids failed", K(ret), K(tsc_columns.count()));
  } else if (OB_FAIL(agent_vt_meta.access_row_types_.init(tsc_columns.count()))) {
    LOG_WARN("init access row types failed", K(ret), K(tsc_columns.count()));
  } else if (OB_FAIL(cg_.generate_rt_exprs(tsc_columns, agent_vt_meta.access_exprs_))) {
    LOG_WARN("generate virtual agent table TSC access exprs failed", K(ret));
  } else if (OB_FAIL(cg_.mark_expr_self_produced(tsc_columns))) {
    LOG_WARN("makr expr self produced failed", K(ret));
  }
  ARRAY_FOREACH(tsc_columns, i) {
    ObRawExpr *expr = tsc_columns.at(i);
    uint64_t column_id = UINT64_MAX;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rowscan not supported", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "rowscan not supported");
    } else {
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr *>(expr);
      column_id = col_expr->get_column_id();
      ObObjMeta obj_meta;
      obj_meta.set_type(col_expr->get_data_type());
      obj_meta.set_collation_type(col_expr->get_collation_type());
      if (OB_FAIL(agent_vt_meta.access_column_ids_.push_back(column_id))) {
        LOG_WARN("store access column id failed", K(ret), KPC(col_expr));
      } else if (OB_FAIL(agent_vt_meta.access_row_types_.push_back(obj_meta))) {
        LOG_WARN("failed to push back row types", K(ret));
      } else {}
    }
  } // end for

  if (OB_SUCC(ret) && !op.get_range_columns().empty()) {
    const ObTableSchema *vt_table_schema = NULL;
    const ObIArray<ColumnItem> &range_columns = op.get_range_columns();
    agent_vt_meta.key_types_.set_capacity(range_columns.count());
    if (OB_FAIL(cg_.opt_ctx_->get_sql_schema_guard()->get_table_schema(op.get_table_id(),
                                                                       op.get_real_ref_table_id(),
                                                                       op.get_stmt(),
                                                                       table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(op.get_real_ref_table_id()));
    } else if (OB_FAIL(cg_.opt_ctx_->get_sql_schema_guard()->get_table_schema(op.get_table_id(),
                                                                              agent_vt_meta.vt_table_id_,
                                                                              op.get_stmt(),
                                                                              vt_table_schema))) {
      LOG_WARN("get table schema failed", K(agent_vt_meta.vt_table_id_), K(ret));
    } else {
      // set vt has tenant_id column
      for (int64_t nth_col = 0; OB_SUCC(ret) && nth_col < range_columns.count(); ++nth_col) {
        const ColumnItem &col_item = range_columns.at(nth_col);
        if (0 == col_item.column_name_.case_compare("TENANT_ID")) {
          spec.has_tenant_id_col_ = true;
          spec.tenant_id_col_idx_ = nth_col;
          break;
        }
      }
      for (int64_t k = 0; k < range_columns.count() && OB_SUCC(ret); ++k) {
        uint64_t column_id = UINT64_MAX;
        uint64_t range_column_id = range_columns.at(k).column_id_;
        const ObColumnSchemaV2 *vt_col_schema = vt_table_schema->get_column_schema(range_column_id);
        if (OB_ISNULL(vt_col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: column schema is null", K(range_column_id), K(ret));
        }
        for (int64_t nth_col = 0; nth_col < table_schema->get_column_count() && OB_SUCC(ret); ++nth_col) {
          const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema_by_idx(nth_col);
          if (OB_ISNULL(col_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is null", K(ret), K(column_id));
          } else if (0 == col_schema->get_column_name_str().case_compare(vt_col_schema->get_column_name_str())) {
            column_id = col_schema->get_column_id();
            ObObjMeta obj_meta;
            obj_meta.set_type(col_schema->get_data_type());
            obj_meta.set_collation_type(col_schema->get_collation_type());
            OZ(agent_vt_meta.key_types_.push_back(obj_meta));
            break;
          }
        }
        if (UINT64_MAX == column_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: column not found", K(ret), K(range_column_id), K(k));
        }
      }
      if (OB_SUCC(ret) && op.get_range_columns().count() != agent_vt_meta.key_types_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("key is not match", K(op.get_range_columns().count()),
          K(agent_vt_meta.key_types_.count()), K(ret), K(op.get_range_columns()),
          K(agent_vt_meta.key_types_));
      }
    }
  }
  return ret;
}

int ObTscCgService::generate_tsc_filter(const ObLogTableScan &op, ObTableScanSpec &spec)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> nonpushdown_filters;
  ObArray<ObRawExpr *> scan_pushdown_filters;
  ObArray<ObRawExpr *> lookup_pushdown_filters;
  ObDASScanCtDef &scan_ctdef = spec.tsc_ctdef_.scan_ctdef_;
  ObDASScanCtDef *lookup_ctdef = spec.tsc_ctdef_.lookup_ctdef_;
  if (OB_FAIL(const_cast<ObLogTableScan &>(op).extract_pushdown_filters(nonpushdown_filters,
                                    scan_pushdown_filters,
                                    lookup_pushdown_filters))) {
    LOG_WARN("extract pushdown filters failed", K(ret));
  } else if (op.get_contains_fake_cte()) {
    // do nothing
  } else if (OB_FAIL(generate_pd_storage_flag(op.get_plan(),
                                              op.get_ref_table_id(),
                                              op.get_access_exprs(),
                                              op.get_type(),
                                              false, /*generate_pd_storage_flag*/
                                              scan_ctdef.pd_expr_spec_))) {
    LOG_WARN("generate pd storage flag for scan ctdef failed", K(ret));
  } else if (lookup_ctdef != nullptr &&
      OB_FAIL(generate_pd_storage_flag(op.get_plan(),
                                       op.get_ref_table_id(),
                                       op.get_access_exprs(),
                                       op.get_type(),
                                       op.get_index_back() && op.get_is_index_global(), /*generate_pd_storage_flag*/
                                       lookup_ctdef->pd_expr_spec_))) {
    LOG_WARN("generate pd storage flag for lookup ctdef failed", K(ret));
  }

  if (OB_SUCC(ret) && !scan_pushdown_filters.empty()) {
    bool pd_filter = ObPushdownFilterUtils::is_filter_pushdown_storage(
        scan_ctdef.pd_expr_spec_.pd_storage_flag_);
    if (OB_FAIL(cg_.generate_rt_exprs(scan_pushdown_filters, scan_ctdef.pd_expr_spec_.pushdown_filters_))) {
      LOG_WARN("generate scan ctdef pushdown filter");
    } else if (pd_filter) {
      ObPushdownFilterConstructor filter_constructor(&cg_.phy_plan_->get_allocator(), cg_);
      if (OB_FAIL(filter_constructor.apply(
          scan_pushdown_filters, scan_ctdef.pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
        LOG_WARN("failed to apply filter constructor", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !lookup_pushdown_filters.empty()) {
    bool pd_filter = ObPushdownFilterUtils::is_filter_pushdown_storage(
        lookup_ctdef->pd_expr_spec_.pd_storage_flag_);
    if (OB_FAIL(cg_.generate_rt_exprs(lookup_pushdown_filters, lookup_ctdef->pd_expr_spec_.pushdown_filters_))) {
      LOG_WARN("generate lookup ctdef pushdown filter failed", K(ret));
    } else if (pd_filter) {
      ObPushdownFilterConstructor filter_constructor(&cg_.phy_plan_->get_allocator(), cg_);
      if (OB_FAIL(filter_constructor.apply(
          lookup_pushdown_filters, lookup_ctdef->pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
        LOG_WARN("failed to apply filter constructor", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cg_.generate_rt_exprs(nonpushdown_filters, spec.filters_))) {
      LOG_WARN("generate filter expr failed", K(ret));
    }
  }
  return ret;
}


int ObTscCgService::generate_pd_storage_flag(const ObLogPlan *log_plan,
                                             const uint64_t ref_table_id,
                                             const ObIArray<ObRawExpr *> &access_exprs,
                                             const log_op_def::ObLogOpType op_type,
                                             const bool is_global_index_lookup,
                                             ObPushdownExprSpec &pd_spec)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  bool pd_blockscan = false, pd_filter = false;
  ObBasicSessionInfo *session_info = NULL;
  if (OB_ISNULL(log_plan) ||
      OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(log_plan), K(schema_guard));
  } else if (OB_FALSE_IT(session_info = log_plan->get_optimizer_context().get_session_info())) {
  } else if (OB_ISNULL(session_info) ||
             is_sys_table(ref_table_id) ||
             is_virtual_table(ref_table_id)) {
  } else {
    int pd_level = 0;
    uint64_t tenant_id = session_info->get_effective_tenant_id();
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      LOG_WARN("failed to init tenant config", K(tenant_id));
    } else {
      pd_level = tenant_config->_pushdown_storage_level;
      pd_blockscan = ObPushdownFilterUtils::is_blockscan_pushdown_enabled(pd_level);
      pd_filter = ObPushdownFilterUtils::is_filter_pushdown_enabled(pd_level);

      // pushdown filter only support scan now
      if (pd_blockscan) {
        if (log_op_def::LOG_TABLE_SCAN == op_type) {
          if (is_global_index_lookup) {
            pd_blockscan = false;
          }
        } else {
          pd_blockscan = false;
        }
      }
      LOG_DEBUG("chaser debug pd block", K(op_type), K(pd_blockscan));
      if (!pd_blockscan) {
        pd_filter = false;
      } else {
        FOREACH_CNT_X(e, access_exprs, pd_blockscan || pd_filter) {
          if (T_ORA_ROWSCN == (*e)->get_expr_type()) {
            pd_blockscan = false;
            pd_filter = false;
          } else {
            auto col = static_cast<ObColumnRefRawExpr *>(*e);
            if (col->is_lob_column() && cg_.cur_cluster_version_ < CLUSTER_VERSION_4_1_0_0) {
              pd_filter = false;
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (pd_blockscan) {
      ObPushdownFilterUtils::set_blockscan_pushdown_storage(pd_spec.pd_storage_flag_);
    }
    if (pd_filter) {
      ObPushdownFilterUtils::set_filter_pushdown_storage(pd_spec.pd_storage_flag_);
    }
  }
  return ret;
}

//extract the columns that required by DAS scan:
//1. all columns required by TSC operator outputs
//2. all columns required by TSC operator filters
//3. all columns required by pushdown aggr expr
int ObTscCgService::extract_das_access_exprs(const ObLogTableScan &op,
                                             ObTableID scan_table_id,
                                             ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  if (op.get_index_back() && scan_table_id == op.get_real_index_table_id()) {
    //this das scan is index scan and will lookup the data table later
    //index scan + lookup data table: the index scan only need access
    //range condition columns + index filter columns + the data table rowkeys
    const ObIArray<ObRawExpr*> &range_conditions = op.get_range_conditions();
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(range_conditions, access_exprs))) {
      LOG_WARN("extract column exprs failed", K(ret));
    }
    //store index filter columns
    if (OB_SUCC(ret)) {
      ObArray<ObRawExpr *> nonpushdown_filters;
      ObArray<ObRawExpr *> scan_pushdown_filters;
      ObArray<ObRawExpr *> lookup_pushdown_filters;
      ObArray<ObRawExpr *> filter_columns; // the column in scan pushdown filters
      if (OB_FAIL(const_cast<ObLogTableScan &>(op).extract_pushdown_filters(nonpushdown_filters,
                                        scan_pushdown_filters,
                                        lookup_pushdown_filters))) {
        LOG_WARN("extract pushdown filters failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(scan_pushdown_filters,
                                                              filter_columns))) {
        LOG_WARN("extract column exprs failed", K(ret));
      } else if (OB_FAIL(append_array_no_dup(access_exprs, filter_columns))) {
        LOG_WARN("append filter column to access exprs failed", K(ret));
      }
    }
    //store data table rowkeys
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append_array_no_dup(access_exprs, op.get_rowkey_exprs()))) {
        LOG_WARN("append the data table rowkey expr failed", K(ret), K(op.get_rowkey_exprs()));
      } else if (OB_FAIL(append_array_no_dup(access_exprs, op.get_part_exprs()))) {
        LOG_WARN("append the data table part expr failed", K(ret), K(op.get_part_exprs()));
      } else if (NULL != op.get_group_id_expr() && op.use_batch()
                 && OB_FAIL(add_var_to_array_no_dup(access_exprs,
                               const_cast<ObRawExpr *>(op.get_group_id_expr())))) {
        LOG_WARN("fail to add group id", K(ret));
      }
    }
  } else if (OB_FAIL(access_exprs.assign(op.get_access_exprs()))) {
    LOG_WARN("assign access exprs failed", K(ret));
  }
  if (OB_SUCC(ret) && is_oracle_mapping_real_virtual_table(op.get_ref_table_id())) {
    //the access exprs are the agent virtual table columns, but das need the real table columns
    //now to replace the real table column
    for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); ++i) {
      ObRawExpr *expr = access_exprs.at(i);
      ObRawExpr *mapping_expr = nullptr;
      uint64_t column_id = UINT64_MAX;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(expr));
      } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("rowscan not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "rowscan not supported");
      } else if (OB_ISNULL(mapping_expr = op.get_real_expr(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mapping expr is null", K(ret), KPC(expr));
      } else {
        //replace the agent virtual table column expr
        access_exprs.at(i) = mapping_expr;
      }
    }
  }
  // main table need remove virtual generated column access exprs.
  if (scan_table_id == op.get_real_ref_table_id()) {
    ObArray<ObRawExpr*> tmp_access_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); ++i) {
      ObRawExpr *expr = access_exprs.at(i);
      if (expr->is_column_ref_expr() &&
          static_cast<ObColumnRefRawExpr *>(expr)->is_virtual_generated_column() &&
          !static_cast<ObColumnRefRawExpr *>(expr)->is_xml_column()) {
        // do nothing.
      } else {
        if (OB_FAIL(add_var_to_array_no_dup(tmp_access_exprs, expr))) {
          LOG_WARN("failed to add param expr", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(access_exprs.assign(tmp_access_exprs))) {
      LOG_WARN("failed to remove generated column exprs", K(ret));
    }
  }
  return ret;
}

//extract these column exprs need by TSC operator, these column will output by DAS scan
int ObTscCgService::extract_tsc_access_columns(const ObLogTableScan &op,
                                               ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  //TSC operator only need to access these columns output by DAS scan
  //TSC operator only need these columns:
  //1. columns in non-pushdown(to storage) filters
  //2. columns in TSC operator outputs
  ObArray<ObRawExpr*> tsc_exprs;
  ObArray<ObRawExpr*> scan_pushdown_filters;
  ObArray<ObRawExpr*> lookup_pushdown_filters;
  if (OB_FAIL(const_cast<ObLogTableScan &>(op).extract_pushdown_filters(tsc_exprs, //non-pushdown filters
                                          scan_pushdown_filters,
                                          lookup_pushdown_filters))) {
    LOG_WARN("extract pushdown filters failed", K(ret));
  } else if (OB_FAIL(append_array_no_dup(tsc_exprs, op.get_output_exprs()))) {
    LOG_WARN("append output exprs failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(tsc_exprs, access_exprs, true))) {
    LOG_WARN("extract column exprs failed", K(ret));
  }
  LOG_TRACE("extract tsc access columns", K(ret), K(tsc_exprs), K(access_exprs));
  return ret;
}

int ObTscCgService::generate_geo_access_ctdef(const ObLogTableScan &op, const ObTableSchema &index_schema,
                                              ObArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  uint64_t mbr_col_id = 0;
  bool is_found = false;
  if (OB_FAIL(index_schema.get_index_info().get_spatial_mbr_col_id(mbr_col_id))) {
    LOG_WARN("fail to get spatial mbr column id", K(ret), K(index_schema.get_index_info()));
  } else {
    const ObIArray<ObRawExpr*> &log_access_exprs = op.get_access_exprs();
    for (uint32_t i = 0; i < log_access_exprs.count() && OB_SUCC(ret); i++) {
      ObRawExpr *expr = log_access_exprs.at(i);
      if (T_REF_COLUMN == expr->get_expr_type()) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr *>(expr);
        if (mbr_col_id == col_expr->get_column_id()
            && op.get_table_id() == col_expr->get_table_id()) {
          access_exprs.push_back(expr);
          is_found = true;
        }
      }
    }
    if (!is_found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mbr column expr not found", K(ret));
    }
  }

  return ret;
}

int ObTscCgService::generate_access_ctdef(const ObLogTableScan &op,
                                          ObDASScanCtDef &scan_ctdef,
                                          bool &has_rowscn)
{
  int ret = OB_SUCCESS;
  has_rowscn = false;
  const ObTableSchema *table_schema = nullptr;
  ObTableID table_id = scan_ctdef.ref_table_id_;
  ObArray<uint64_t> access_column_ids;
  ObArray<ObRawExpr*> access_exprs;
  if (OB_FAIL(cg_.opt_ctx_->get_schema_guard()->get_table_schema(MTL_ID(), table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(table_id));
  } else if (OB_FAIL(extract_das_access_exprs(op, scan_ctdef.ref_table_id_, access_exprs))) {
    LOG_WARN("extract das access exprs failed", K(ret));
  } else if (table_schema->is_spatial_index()
             && OB_FAIL(generate_geo_access_ctdef(op, *table_schema, access_exprs))) {
    LOG_WARN("extract das geo access exprs failed", K(ret));
  }
  ARRAY_FOREACH(access_exprs, i) {
    ObRawExpr *expr = access_exprs.at(i);
    if (OB_UNLIKELY(OB_ISNULL(expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
      //only data table need to produce rowscn
      if (table_schema->is_index_table()) {
        if (op.is_index_scan() && !op.get_index_back()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rowscn only can be produced by data table", K(ret));
        }
      } else if (OB_FAIL(access_column_ids.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
        LOG_WARN("store output column ids failed", K(ret));
      } else {
        has_rowscn = true;
        LOG_DEBUG("need row scn");
      }
    } else if (T_PSEUDO_GROUP_ID == expr->get_expr_type()) {
      OZ(access_column_ids.push_back(common::OB_HIDDEN_GROUP_IDX_COLUMN_ID));
    } else if (T_PSEUDO_EXTERNAL_FILE_COL == expr->get_expr_type()) {
      //TODO EXTERNAL-TABLE
    } else {
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr *>(expr);
      bool is_mapping_vt_table = op.get_real_ref_table_id() != op.get_ref_table_id();
      ObTableID real_table_id = is_mapping_vt_table ? op.get_real_ref_table_id() : op.get_table_id();
      if (!col_expr->has_flag(IS_COLUMN) || col_expr->get_table_id() != real_table_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Expected basic column", K(ret),
                 K(*col_expr), K(col_expr->has_flag(IS_COLUMN)),
                 K(col_expr->get_table_id()), K(real_table_id), K(op.get_real_ref_table_id()), K(op.get_ref_table_id()), K(op.get_table_id()), K(op.get_real_index_table_id()));
      } else if (OB_FAIL(access_column_ids.push_back(col_expr->get_column_id()))) {
        LOG_WARN("store column id failed", K(ret));
      }
    }
  } // end for
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cg_.generate_rt_exprs(access_exprs, scan_ctdef.pd_expr_spec_.access_exprs_))) {
      LOG_WARN("generate rt exprs failed", K(ret), K(access_exprs));
    } else if (OB_FAIL(cg_.mark_expr_self_produced(access_exprs))) {
      LOG_WARN("makr expr self produced failed", K(ret), K(access_exprs));
    } else if (OB_FAIL(scan_ctdef.access_column_ids_.assign(access_column_ids))) {
      LOG_WARN("assign output column ids failed", K(ret));
    }
  }
  LOG_DEBUG("cherry pick the final access exprs", K(ret),
           K(table_id), K(op.get_access_exprs()), K(access_exprs), K(access_column_ids));
  return ret;
}

int ObTscCgService::generate_pushdown_aggr_ctdef(const ObLogTableScan &op,
                                                 ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObAggFunRawExpr*> &pushdown_aggr_exprs = op.get_pushdown_aggr_exprs();
  const uint64_t aggregate_output_count = pushdown_aggr_exprs.count();
  if (op.get_index_back() && aggregate_output_count > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pushdown aggr to table scan not supported in index lookup",
             K(op.get_table_id()), K(op.get_ref_table_id()), K(op.get_index_table_id()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "pushdown aggr to table scan not supported in index lookup");
  } else {
    ExprFixedArray &aggregate_output = scan_ctdef.pd_expr_spec_.pd_storage_aggregate_output_;
    if (aggregate_output_count > 0) {
      ObPushdownFilterUtils::set_aggregate_pushdown_storage(scan_ctdef.pd_expr_spec_.pd_storage_flag_);
      OZ(scan_ctdef.aggregate_column_ids_.init(aggregate_output_count));
      if (OB_FAIL(aggregate_output.reserve(aggregate_output_count))) {
        LOG_WARN("init aggregate output array", K(ret), K(aggregate_output_count));
      } else {
        // pushdown_aggr_exprs in convert_table_scan need to set IS_COLUMNLIZED flag
        ObExpr *e = NULL;
        FOREACH_CNT_X(raw_expr, pushdown_aggr_exprs, OB_SUCC(ret)) {
          CK(OB_NOT_NULL(*raw_expr));
          OZ(cg_.generate_rt_expr(*(*raw_expr), e));
          CK(OB_NOT_NULL(e));
          OZ(aggregate_output.push_back(e));
          OZ(cg_.mark_expr_self_produced(*raw_expr));
        }
      }
    }

    ARRAY_FOREACH(pushdown_aggr_exprs, i) {
      ObAggFunRawExpr *aggr_expr = pushdown_aggr_exprs.at(i);
      ObRawExpr *param_expr = NULL;
      ObColumnRefRawExpr* col_expr = NULL;
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggr expr is null", K(ret));
      } else if (aggr_expr->get_real_param_exprs().empty()) {
        OZ(scan_ctdef.aggregate_column_ids_.push_back(OB_COUNT_AGG_PD_COLUMN_ID));
      } else if (OB_ISNULL(param_expr = aggr_expr->get_param_expr(0)) ||
                 OB_UNLIKELY(!param_expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected basic column", K(ret), K(param_expr));
      } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr *>(param_expr))) {
      } else if (OB_UNLIKELY(col_expr->get_table_id() != op.get_table_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expected basic column", K(ret), K(col_expr->get_table_id()),
                                           K(op.get_table_id()));
      } else {
        OZ(scan_ctdef.aggregate_column_ids_.push_back(col_expr->get_column_id()));
      }
    }
  }
  return ret;
}

int ObTscCgService::generate_das_scan_ctdef(const ObLogTableScan &op,
                                            ObDASScanCtDef &scan_ctdef,
                                            bool &has_rowscn)
{
  int ret = OB_SUCCESS;
  // 1. add basic column
  if (OB_FAIL(generate_access_ctdef(op, scan_ctdef, has_rowscn))) {
    LOG_WARN("generate access ctdef failed", K(ret), K(scan_ctdef.ref_table_id_));
  }
  //2. generate pushdown aggr column
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_pushdown_aggr_ctdef(op, scan_ctdef))) {
      LOG_WARN("generate pushdown aggr ctdef failed", K(ret));
    }
  }

  // 3. cg trans_info_expr
  if (OB_SUCC(ret)) {
    ObRawExpr *trans_info_expr = op.get_trans_info_expr();
    if (OB_NOT_NULL(trans_info_expr)) {
      if (OB_FAIL(cg_.generate_rt_expr(*op.get_trans_info_expr(),
                                       scan_ctdef.pd_expr_spec_.trans_info_expr_))) {
        LOG_WARN("generate trans info expr failed", K(ret));
      }
    }
  }

  //4. generate batch scan ctdef
  if (OB_SUCC(ret) && op.use_batch()) {
    if (OB_FAIL(cg_.generate_rt_expr(*op.get_group_id_expr(), scan_ctdef.group_id_expr_))) {
      LOG_WARN("generate group id expr failed", K(ret));
    }
  }
  //5. generate table schema version
  if (OB_SUCC(ret)) {
    ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
    if (OB_FAIL(schema_guard->get_table_schema_version(scan_ctdef.ref_table_id_,
                                                       scan_ctdef.schema_version_))) {
      LOG_WARN("get table schema version failed", K(ret), K(scan_ctdef.ref_table_id_));
    }
  }
  //6. generate table param
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_table_param(op, scan_ctdef))) {
      LOG_WARN("generate table param failed", K(ret));
    }
  }
  return ret;
}

int ObTscCgService::extract_das_output_column_ids(const ObLogTableScan &op,
                                                  ObTableID table_id,
                                                  const ObTableSchema &index_schema,
                                                  ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> das_output_cols;

  if (op.get_index_back() && op.get_real_index_table_id() == table_id) {
    //this situation is index lookup, and the index table scan is being processed
    //the output column id of index lookup is the rowkey of the data table
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(cg_.opt_ctx_->get_schema_guard()->get_table_schema(MTL_ID(), op.get_real_ref_table_id(), table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
    } else if (OB_FAIL(table_schema->get_rowkey_column_ids(output_cids))) {
      LOG_WARN("get rowkey column ids failed", K(ret));
    } else if (nullptr != op.get_group_id_expr() && op.use_batch()) {
      if (OB_FAIL(output_cids.push_back(OB_HIDDEN_GROUP_IDX_COLUMN_ID))) {
        LOG_WARN("store group column id failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (op.get_is_index_global() && table_schema->is_heap_table()) {
      if (!table_schema->is_partitioned_table()) {
        // do nothing
      } else if (table_schema->get_partition_key_info().get_size() > 0 &&
                OB_FAIL(table_schema->get_partition_key_info().get_column_ids(output_cids))) {
        LOG_WARN("failed to get column ids", K(ret));
      } else if (table_schema->get_subpartition_key_info().get_size() > 0 &&
                OB_FAIL(table_schema->get_subpartition_key_info().get_column_ids(output_cids))) {
        LOG_WARN("failed to get column ids", K(ret));
      }
    }

    if (OB_SUCC(ret) && index_schema.is_spatial_index()) {
      uint64_t mbr_col_id;
      if (OB_FAIL(index_schema.get_index_info().get_spatial_mbr_col_id(mbr_col_id))) {
        LOG_WARN("fail to get spatial mbr column id", K(ret), K(index_schema.get_index_info()));
      } else if (OB_FAIL(output_cids.push_back(mbr_col_id))) {
        LOG_WARN("store cell colum id failed", K(ret), K(mbr_col_id));
      }
    }
    //column expr in non-pushdown filter need to be output,
    //because filter_row will use it in TSC operator
  } else if (OB_FAIL(extract_tsc_access_columns(op, das_output_cols))) {
    LOG_WARN("extract tsc access columns failed", K(ret));
  } else if (is_oracle_mapping_real_virtual_table(op.get_ref_table_id())) {
    //for virtual agent table, DAS scan need the colum of the mapping real table
    for (int64_t i = 0; OB_SUCC(ret) && i < das_output_cols.count(); ++i) {
      ObRawExpr *mapping_expr = op.get_real_expr(das_output_cols.at(i));
      if (!mapping_expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mapping expr type is invalid", K(ret), KPC(mapping_expr), KPC(das_output_cols.at(i)));
      } else if (OB_FAIL(output_cids.push_back(static_cast<ObColumnRefRawExpr*>(mapping_expr)->get_column_id()))) {
        LOG_WARN("store real output colum id failed", K(ret), KPC(mapping_expr));
      }
    }
  } else if (nullptr != op.get_group_id_expr() && op.use_batch() &&
      OB_FAIL(das_output_cols.push_back(const_cast<ObRawExpr*>(op.get_group_id_expr())))) {
    LOG_WARN("store group id expr failed", K(ret));
  } else if (OB_FAIL(extract_das_column_ids(das_output_cols, output_cids))) {
    LOG_WARN("extract column ids failed", K(ret));
  }
  return ret;
}
int ObTscCgService::extract_das_column_ids(const ObIArray<ObRawExpr*> &column_exprs,
                                           ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
    if (OB_ISNULL(column_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_ORA_ROWSCN == column_exprs.at(i)->get_expr_type()) {
      if (OB_FAIL(column_ids.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
        LOG_WARN("store ora rowscan failed", K(ret));
      }
    } else if (T_PSEUDO_GROUP_ID == column_exprs.at(i)->get_expr_type()) {
      if (OB_FAIL(column_ids.push_back(OB_HIDDEN_GROUP_IDX_COLUMN_ID))) {
        LOG_WARN("store group column id failed", K(ret));
      }
    } else if (column_exprs.at(i)->is_column_ref_expr()) {
      const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr*>(column_exprs.at(i));
      if (OB_FAIL(column_ids.push_back(col_expr->get_column_id()))) {
        LOG_WARN("store column id failed", K(ret));
      }
    } else {
      //other column exprs not produced in DAS, ignore it
    }
  }
  return ret;
}

int ObTscCgService::generate_table_loc_meta(uint64_t table_loc_id,
                                            const ObDMLStmt &stmt,
                                            const ObTableSchema &table_schema,
                                            const ObSQLSessionInfo &session,
                                            ObDASTableLocMeta &loc_meta)
{
  int ret = OB_SUCCESS;
  loc_meta.reset();
  loc_meta.table_loc_id_ = table_loc_id;
  ObTableID real_table_id =
      share::is_oracle_mapping_real_virtual_table(table_schema.get_table_id()) ?
            ObSchemaUtils::get_real_table_mappings_tid(table_schema.get_table_id())
              : table_schema.get_table_id();
  loc_meta.ref_table_id_ = real_table_id;
  loc_meta.is_dup_table_ = table_schema.is_duplicate_table();
  loc_meta.is_external_table_ = table_schema.is_external_table();
  loc_meta.is_external_files_on_disk_ =
      ObSQLUtils::is_external_files_on_local_disk(table_schema.get_external_file_location());
  bool is_weak_read = false;
  if (OB_ISNULL(cg_.opt_ctx_) || OB_ISNULL(cg_.opt_ctx_->get_exec_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(cg_.opt_ctx_), K(ret));
  } else if (stmt.get_query_ctx()->has_dml_write_stmt_) {
    loc_meta.select_leader_ = 1;
    loc_meta.is_weak_read_ = 0;
  } else if (OB_FAIL(ObTableLocation::get_is_weak_read(stmt, &session,
                                                       cg_.opt_ctx_->get_exec_ctx()->get_sql_ctx(),
                                                       is_weak_read))) {
    LOG_WARN("get is weak read failed", K(ret));
  } else if (is_weak_read) {
    loc_meta.is_weak_read_ = 1;
    loc_meta.select_leader_ = 0;
  } else if (loc_meta.is_dup_table_) {
    loc_meta.select_leader_ = 0;
    loc_meta.is_weak_read_ = 0;
  } else {
    //strong consistency read policy is used by default
    loc_meta.select_leader_ = 1;
    loc_meta.is_weak_read_ = 0;
  }
  if (OB_SUCC(ret) && !table_schema.is_global_index_table()) {
    TableLocRelInfo *rel_info = nullptr;
    ObTableID data_table_id = table_schema.is_index_table() ?
                              table_schema.get_data_table_id() :
                              real_table_id;
    rel_info = cg_.opt_ctx_->get_loc_rel_info_by_id(table_loc_id, data_table_id);
    if (OB_ISNULL(rel_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("relation info is nullptr", K(ret), K(table_loc_id), K(table_schema.get_table_id()));
    } else if (rel_info->related_ids_.count() <= 1) {
      //the first table id is the source table, <=1 mean no dependency table
    } else {
      loc_meta.related_table_ids_.set_capacity(rel_info->related_ids_.count() - 1);
      for (int64_t i = 0; OB_SUCC(ret) && i < rel_info->related_ids_.count(); ++i) {
        if (rel_info->related_ids_.at(i) != loc_meta.ref_table_id_) {
          if (OB_FAIL(loc_meta.related_table_ids_.push_back(rel_info->related_ids_.at(i)))) {
            LOG_WARN("store related table id failed", K(ret), KPC(rel_info), K(i), K(loc_meta));
          }
        }
      }
    }
  }
  return ret;
}



}  // namespace sql
}  // namespace oceanbase
