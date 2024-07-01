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
  ObDASBaseCtDef *root_ctdef = nullptr;
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
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table scahem ptr", K(ret));
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
    if (op.is_text_retrieval_scan()) {
      scan_ctdef.ir_scan_type_ = ObTSCIRScanType::OB_IR_INV_IDX_SCAN;
    }
    if (OB_FAIL(generate_das_scan_ctdef(op, scan_ctdef, has_rowscn))) {
      LOG_WARN("generate das scan ctdef failed", K(ret), K(scan_ctdef.ref_table_id_));
    } else {
      tsc_ctdef.flashback_item_.need_scn_ |= has_rowscn;
      root_ctdef = &scan_ctdef;
    }
  }
  if (OB_SUCC(ret) && op.das_need_keep_ordering()) {
    tsc_ctdef.is_das_keep_order_ = true;
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
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr to table schema", K(ret));
      } else if (OB_FAIL(generate_table_loc_meta(op.get_table_id(),
                                                 *op.get_stmt(),
                                                 *table_schema,
                                                 *cg_.opt_ctx_->get_session_info(),
                                                 tsc_ctdef.das_dppr_tbl_->get_loc_meta()))) {
        LOG_WARN("generate table loc meta failed", K(ret));
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

  bool need_attach = false;
  if (OB_SUCC(ret) && op.is_text_retrieval_scan()) {
    if (OB_FAIL(generate_text_ir_ctdef(op, tsc_ctdef, root_ctdef))) {
      LOG_WARN("failed to generate text ir ctdef", K(ret));
    } else {
      need_attach = true;
    }
  }

  if (OB_SUCC(ret) && op.is_multivalue_index_scan()) {
    ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
    ObExpr *doc_id_col_expr = scan_ctdef.result_output_.at(scan_ctdef.result_output_.count() - 1);
    if (OB_FAIL(generate_doc_id_lookup_ctdef(op, tsc_ctdef, root_ctdef, doc_id_col_expr, aux_lookup_ctdef))) {
      LOG_WARN("failed to generate text ir ctdef", K(ret));
    } else {
      root_ctdef = aux_lookup_ctdef;
      need_attach = true;
    }
  }

  if (OB_SUCC(ret) && op.get_index_back()) {
    ObDASTableLookupCtDef *lookup_ctdef = nullptr;
    if (OB_FAIL(generate_table_lookup_ctdef(op, tsc_ctdef, root_ctdef, lookup_ctdef))) {
      LOG_WARN("generate table lookup ctdef failed", K(ret));
    } else {
      root_ctdef = lookup_ctdef;
    }
  }

  if (OB_SUCC(ret) && need_attach) {
    tsc_ctdef.lookup_ctdef_ = nullptr;
    tsc_ctdef.lookup_loc_meta_ = nullptr;
    tsc_ctdef.attach_spec_.attach_ctdef_ = root_ctdef;
  }

  LOG_DEBUG("generate tsc ctdef finish", K(ret), K(op), K(tsc_ctdef),
                                                    K(tsc_ctdef.scan_ctdef_.pd_expr_spec_.ext_file_column_exprs_));
  return ret;
}

int ObTscCgService::generate_table_param(const ObLogTableScan &op,
                                         ObDASScanCtDef &scan_ctdef,
                                         common::ObIArray<uint64_t> &tsc_out_cols)
{
  int ret = OB_SUCCESS;
  ObTableID index_id = scan_ctdef.ref_table_id_;
  const ObTableSchema *table_schema = NULL;
  const bool pd_agg = scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown();
  const bool pd_group_by =  scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_group_by_pushdown();
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  CK(OB_NOT_NULL(schema_guard));
  if (OB_UNLIKELY(pd_agg && 0 == scan_ctdef.aggregate_column_ids_.count()) ||
      OB_UNLIKELY(pd_group_by && 0 == scan_ctdef.group_by_column_ids_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(pd_agg), K(scan_ctdef.aggregate_column_ids_.count()),
        K(pd_group_by), K(scan_ctdef.group_by_column_ids_.count()));
  } else if (OB_INVALID == index_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id", K(index_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(), index_id, op.get_stmt(), table_schema))) {
    LOG_WARN("get table schema failed", K(index_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(table_schema));
  } else if (table_schema->is_spatial_index() && FALSE_IT(scan_ctdef.table_param_.set_is_spatial_index(true))) {
  } else if (table_schema->is_fts_index() && FALSE_IT(scan_ctdef.table_param_.set_is_fts_index(true))) {
  } else if (table_schema->is_multivalue_index_aux() && FALSE_IT(scan_ctdef.table_param_.set_is_multivalue_index(true))) {
  } else if (OB_FAIL(extract_das_output_column_ids(op, scan_ctdef, *table_schema, tsc_out_cols))) {
    LOG_WARN("extract tsc output column ids failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(scan_ctdef.table_param_.get_enable_lob_locator_v2()
                          = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0))) {
  } else if (OB_FAIL(scan_ctdef.table_param_.convert(*table_schema,
                                                     scan_ctdef.access_column_ids_,
                                                     scan_ctdef.pd_expr_spec_.pd_storage_flag_,
                                                     &tsc_out_cols,
                                                     is_oracle_mapping_real_virtual_table(op.get_ref_table_id())))) {/* for real agent table , use mysql mode compulsory*/
    LOG_WARN("convert schema failed", K(ret), K(*table_schema),
             K(scan_ctdef.access_column_ids_), K(op.get_index_back()));
  } else if ((pd_agg || pd_group_by) &&
             OB_FAIL(scan_ctdef.table_param_.convert_group_by(*table_schema,
                                                              scan_ctdef.access_column_ids_,
                                                              scan_ctdef.aggregate_column_ids_,
                                                              scan_ctdef.group_by_column_ids_,
                                                              scan_ctdef.pd_expr_spec_.pd_storage_flag_))) {
    LOG_WARN("convert group by failed", K(ret), K(*table_schema),
             K(scan_ctdef.aggregate_column_ids_), K(scan_ctdef.group_by_column_ids_));
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
      if (OB_FAIL(scan_ctdef.result_output_.push_back(agg_exprs.at(i)))) {
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
      column_id = OB_HIDDEN_TRANS_VERSION_COLUMN_ID;
      ObObjMeta obj_meta;
      obj_meta.set_type(ObIntType);
      if (OB_FAIL(agent_vt_meta.access_column_ids_.push_back(column_id))) {
        LOG_WARN("store access column id failed", K(ret));
      } else if (OB_FAIL(agent_vt_meta.access_row_types_.push_back(obj_meta))) {
        LOG_WARN("failed to push back row types", K(ret));
      }
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
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to table schema", K(ret));
    } else if (OB_FAIL(cg_.opt_ctx_->get_sql_schema_guard()->get_table_schema(op.get_table_id(),
                                                                              agent_vt_meta.vt_table_id_,
                                                                              op.get_stmt(),
                                                                              vt_table_schema))) {
      LOG_WARN("get table schema failed", K(agent_vt_meta.vt_table_id_), K(ret));
    } else if (OB_ISNULL(vt_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to virtual table schema", K(ret));
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
  ObDASScanCtDef *lookup_ctdef = spec.tsc_ctdef_.get_lookup_ctdef();
  if (OB_FAIL(op.extract_pushdown_filters(nonpushdown_filters,
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
    bool pd_filter = scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_filter_pushdown();
    if (OB_FAIL(cg_.generate_rt_exprs(scan_pushdown_filters, scan_ctdef.pd_expr_spec_.pushdown_filters_))) {
      LOG_WARN("generate scan ctdef pushdown filter");
    } else if (pd_filter) {
      ObPushdownFilterConstructor filter_constructor(
          &cg_.phy_plan_->get_allocator(), cg_, &op,
          scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_use_column_store());
      if (OB_FAIL(filter_constructor.apply(
          scan_pushdown_filters, scan_ctdef.pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
        LOG_WARN("failed to apply filter constructor", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !lookup_pushdown_filters.empty()) {
    bool pd_filter = lookup_ctdef->pd_expr_spec_.pd_storage_flag_.is_filter_pushdown();
    if (OB_FAIL(cg_.generate_rt_exprs(lookup_pushdown_filters, lookup_ctdef->pd_expr_spec_.pushdown_filters_))) {
      LOG_WARN("generate lookup ctdef pushdown filter failed", K(ret));
    } else if (pd_filter) {
      ObPushdownFilterConstructor filter_constructor(
          &cg_.phy_plan_->get_allocator(), cg_, &op,
          lookup_ctdef->pd_expr_spec_.pd_storage_flag_.is_use_column_store());
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
  bool pd_blockscan = false;
  bool pd_filter = false;
  bool enable_skip_index = false;
  ObBasicSessionInfo *session_info = NULL;
  if (OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FALSE_IT(session_info = log_plan->get_optimizer_context().get_session_info())) {
  } else if (OB_ISNULL(session_info) ||
             is_sys_table(ref_table_id) ||
             is_virtual_table(ref_table_id)) {
  } else {
    pd_blockscan = pd_spec.pd_storage_flag_.is_blockscan_pushdown();
    pd_filter = pd_spec.pd_storage_flag_.is_filter_pushdown();
    enable_skip_index = pd_spec.pd_storage_flag_.is_apply_skip_index();
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

    if (!pd_blockscan) {
      pd_filter = false;
    } else {
      FOREACH_CNT_X(e, access_exprs, pd_blockscan || pd_filter) {
        if (T_ORA_ROWSCN == (*e)->get_expr_type() || T_PSEUDO_EXTERNAL_FILE_URL == (*e)->get_expr_type()) {
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
  if (OB_SUCC(ret)) {
    enable_skip_index = enable_skip_index && pd_filter;
    pd_spec.pd_storage_flag_.set_blockscan_pushdown(pd_blockscan);
    pd_spec.pd_storage_flag_.set_filter_pushdown(pd_filter);
    pd_spec.pd_storage_flag_.set_enable_skip_index(enable_skip_index);
    LOG_DEBUG("chaser debug pd block", K(op_type), K(pd_blockscan), K(pd_filter), K(enable_skip_index));
  }
  return ret;
}

//extract the columns that required by DAS scan:
//1. all columns required by TSC operator outputs
//2. all columns required by TSC operator filters
//3. all columns required by pushdown aggr expr
int ObTscCgService::extract_das_access_exprs(const ObLogTableScan &op,
                                             ObDASScanCtDef &scan_ctdef,
                                             ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableID &scan_table_id = scan_ctdef.ref_table_id_;
  if ((op.is_text_retrieval_scan() && scan_table_id != op.get_ref_table_id())
      || (op.is_multivalue_index_scan() && scan_table_id == op.get_doc_id_index_table_id())) {
    // non main table scan in text retrieval
    if (OB_FAIL(extract_text_ir_access_columns(op, scan_ctdef, access_exprs))) {
      LOG_WARN("failed to extract text ir access columns", K(ret));
    }
  } else if (op.get_index_back() && scan_table_id == op.get_real_index_table_id()) {
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
      }
    }
  } else if (OB_FAIL(access_exprs.assign(op.get_access_exprs()))) {
    LOG_WARN("assign access exprs failed", K(ret));
  }
  // store group_id_expr when use group id
  if (OB_SUCC(ret) && op.use_group_id()) {
    const ObRawExpr* group_id_expr = op.get_group_id_expr();
    if (group_id_expr != nullptr &&
        OB_FAIL(add_var_to_array_no_dup(access_exprs, const_cast<ObRawExpr*>(group_id_expr)))) {
      LOG_WARN("failed to push back group id expr", K(ret));
    }
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
        // keep orign expr as access expr
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
  } else if (op.is_text_retrieval_scan() && OB_FAIL(filter_out_match_exprs(tsc_exprs))) {
    // the matching columns of match expr are only used as semantic identifiers and are not actually accessed
    LOG_WARN("failed to filter out fts exprs", K(ret));
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
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to table schema", K(ret));
  } else if (OB_FAIL(extract_das_access_exprs(op, scan_ctdef, access_exprs))) {
    LOG_WARN("extract das access exprs failed", K(ret));
  } else if (table_schema->is_spatial_index()
             && OB_FAIL(generate_geo_access_ctdef(op, *table_schema, access_exprs))) {
    LOG_WARN("extract das geo access exprs failed", K(ret));
  } else if (table_schema->is_multivalue_index_aux()
             && OB_FAIL(extract_doc_id_index_back_access_columns(op, access_exprs))) {
    LOG_WARN("append das multivlaue doc id access exprs failed", K(ret));
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
    } else if (T_PSEUDO_EXTERNAL_FILE_URL == expr->get_expr_type()) {
    } else if (T_PSEUDO_PARTITION_LIST_COL == expr->get_expr_type()) {
    } else {
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr *>(expr);
      bool is_mapping_vt_table = op.get_real_ref_table_id() != op.get_ref_table_id();
      ObTableID real_table_id = is_mapping_vt_table ? op.get_real_ref_table_id() : op.get_table_id();
      if (!col_expr->has_flag(IS_COLUMN) || (col_expr->get_table_id() != real_table_id && !col_expr->is_doc_id_column())) {
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
  const ObIArray<ObRawExpr*> &group_by_columns = op.get_pushdown_groupby_columns();
  const uint64_t group_by_column_count = group_by_columns.count();
  if (op.is_text_retrieval_scan()) {
    // text retrieval scan on fulltext index
    if (OB_FAIL(generate_text_ir_pushdown_expr_ctdef(op, scan_ctdef))) {
      LOG_WARN("failed to generate text ir pushdown aggregate ctdef", K(ret), K(op));
    }
  } else if (op.get_index_back() && aggregate_output_count > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pushdown aggr to table scan not supported in index lookup",
             K(op.get_table_id()), K(op.get_ref_table_id()), K(op.get_index_table_id()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "pushdown aggr to table scan not supported in index lookup");
  } else {
    ExprFixedArray &aggregate_output = scan_ctdef.pd_expr_spec_.pd_storage_aggregate_output_;
    if (aggregate_output_count > 0) {
      scan_ctdef.pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
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

    if (OB_SUCC(ret) && group_by_column_count > 0) {
      scan_ctdef.pd_expr_spec_.pd_storage_flag_.set_group_by_pushdown(true);
      OZ(scan_ctdef.group_by_column_ids_.init(group_by_column_count));
      ARRAY_FOREACH(group_by_columns, i) {
        ObRawExpr *group_expr = group_by_columns.at(i);
        ObColumnRefRawExpr* col_expr = NULL;
        if (OB_ISNULL(group_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group expr is null", K(ret));
        } else if (OB_UNLIKELY(!group_expr->is_column_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected basic column", K(ret), K(group_expr));
        } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr *>(group_expr))) {
        } else if (OB_UNLIKELY(col_expr->get_table_id() != op.get_table_id())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("expected basic column", K(ret), K(col_expr->get_table_id()),
                                            K(op.get_table_id()));
        } else {
          OZ(scan_ctdef.group_by_column_ids_.push_back(col_expr->get_column_id()));
        }
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
  if (OB_SUCC(ret) && op.use_group_id()) {
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
  ObArray<uint64_t> tsc_out_cols;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_table_param(op, scan_ctdef, tsc_out_cols))) {
      LOG_WARN("generate table param failed", K(ret));
    }
  }
  //7. generate das result output
  if (OB_SUCC(ret)) {
    const bool pd_agg = scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown();
    if (OB_FAIL(generate_das_result_output(tsc_out_cols, scan_ctdef, op.get_trans_info_expr(), pd_agg))) {
      LOG_WARN("failed to init result outputs", K(ret));
    }
  }
  return ret;
}

int ObTscCgService::extract_das_output_column_ids(const ObLogTableScan &op,
                                                  ObDASScanCtDef &scan_ctdef,
                                                  const ObTableSchema &index_schema,
                                                  ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> das_output_cols;
  const ObTableID &table_id = scan_ctdef.ref_table_id_;

  if ((op.is_text_retrieval_scan() && table_id != op.get_ref_table_id()) ||
      (op.is_multivalue_index_scan() && table_id == op.get_doc_id_index_table_id())) {
    // non main table scan in text retrieval
    if (OB_FAIL(extract_text_ir_das_output_column_ids(op, scan_ctdef, output_cids))) {
      LOG_WARN("failed to extract text retrieval das output column ids", K(ret));
    }
  } else if (op.get_index_back() && op.get_real_index_table_id() == table_id) {
    //this situation is index lookup, and the index table scan is being processed
    //the output column id of index lookup is the rowkey of the data table
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(cg_.opt_ctx_->get_schema_guard()->get_table_schema(MTL_ID(), op.get_real_ref_table_id(), table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to table schema", K(ret));
    } else if (OB_FAIL(table_schema->get_rowkey_column_ids(output_cids))) {
      LOG_WARN("get rowkey column ids failed", K(ret));
    } else if (nullptr != op.get_group_id_expr() && op.use_group_id()) {
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

    if (OB_SUCC(ret) && index_schema.is_multivalue_index()) {
      uint64_t doc_id_col_id = OB_INVALID_ID;
      uint64_t ft_col_id = OB_INVALID_ID;
      const ObColumnSchemaV2 *doc_id_col_schema = nullptr;
      if (OB_FAIL(index_schema.get_fulltext_column_ids(doc_id_col_id, ft_col_id))) {
        LOG_WARN("fail to get fulltext column ids", K(ret));
      } else if (OB_INVALID_ID == doc_id_col_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get doc id column.", K(ret));
      } else if (OB_FAIL(output_cids.push_back(doc_id_col_id))) {
        LOG_WARN("store colum id failed", K(ret));
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
      if (T_ORA_ROWSCN == mapping_expr->get_expr_type()) {
        OZ (output_cids.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID));
      } else if (!mapping_expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mapping expr type is invalid", K(ret), KPC(mapping_expr), KPC(das_output_cols.at(i)));
      } else if (OB_FAIL(output_cids.push_back(static_cast<ObColumnRefRawExpr*>(mapping_expr)->get_column_id()))) {
        LOG_WARN("store real output colum id failed", K(ret), KPC(mapping_expr));
      }
    }
  } else if (nullptr != op.get_group_id_expr() && op.use_group_id() &&
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
    ObTableID data_table_id = table_schema.is_index_table() && !table_schema.is_rowkey_doc_id() ?
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

int ObTscCgService::generate_text_ir_ctdef(const ObLogTableScan &op,
                                           ObTableScanCtDef &tsc_ctdef,
                                           ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  ObMatchFunRawExpr *match_against = op.get_text_retrieval_info().match_expr_;
  ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  ObDASIRScanCtDef *ir_scan_ctdef = nullptr;
  ObDASSortCtDef *sort_ctdef = nullptr;
  ObExpr *index_back_doc_id_column = nullptr;
  const bool use_approx_pre_agg = true; // TODO: support differentiate use approx agg or not
  if (OB_ISNULL(match_against) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP(match_against), KP(schema_guard));
  } else if (OB_UNLIKELY(OB_INVALID_ID == op.get_text_retrieval_info().inv_idx_tid_
      || (op.need_text_retrieval_calc_relevance() && OB_INVALID_ID == op.get_text_retrieval_info().fwd_idx_tid_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fulltext index table id", K(ret), KPC(match_against));
  } else if (OB_UNLIKELY(ObTSCIRScanType::OB_IR_INV_IDX_SCAN != tsc_ctdef.scan_ctdef_.ir_scan_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ir scan type for inverted index scan", K(ret), K(tsc_ctdef.scan_ctdef_));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_IR_SCAN, ctdef_alloc, ir_scan_ctdef))) {
    LOG_WARN("allocate ir scan ctdef failed", K(ret));
  } else if (op.need_text_retrieval_calc_relevance()) {
    ObDASScanCtDef *inv_idx_scan_ctdef = &tsc_ctdef.scan_ctdef_;
    ObDASScanCtDef *inv_idx_agg_ctdef = nullptr;
    ObDASScanCtDef *doc_id_idx_agg_ctdef = nullptr;
    ObDASScanCtDef *fwd_idx_agg_ctdef = nullptr;
    bool has_rowscn = false;
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, ctdef_alloc, inv_idx_agg_ctdef))) {
      LOG_WARN("allocate inv idx agg ctdef failed", K(ret));
    } else {
      inv_idx_agg_ctdef->ref_table_id_ = op.get_text_retrieval_info().inv_idx_tid_;
      inv_idx_agg_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
      inv_idx_agg_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_INV_IDX_AGG;
      if (OB_FAIL(generate_das_scan_ctdef(op, *inv_idx_agg_ctdef, has_rowscn))) {
        LOG_WARN("failed to generate das scan ctdef", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, ctdef_alloc, doc_id_idx_agg_ctdef))) {
        LOG_WARN("allocate doc id idx agg ctdef failed", K(ret));
      } else {
        doc_id_idx_agg_ctdef->ref_table_id_ = op.get_text_retrieval_info().doc_id_idx_tid_;
        doc_id_idx_agg_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
        doc_id_idx_agg_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG;
        if (OB_FAIL(generate_das_scan_ctdef(op, *doc_id_idx_agg_ctdef, has_rowscn))) {
          LOG_WARN("failed to generate das scan ctdef", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, ctdef_alloc, fwd_idx_agg_ctdef))) {
        LOG_WARN("allocate fwd idx agg ctdef failed", K(ret));
      } else {
        fwd_idx_agg_ctdef->ref_table_id_ = op.get_text_retrieval_info().fwd_idx_tid_;
        fwd_idx_agg_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
        fwd_idx_agg_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_FWD_IDX_AGG;
        if (OB_FAIL(generate_das_scan_ctdef(op, *fwd_idx_agg_ctdef, has_rowscn))) {
          LOG_WARN("generate das scan ctdef failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t ir_scan_children_cnt = use_approx_pre_agg ? 3 : 4;
      if (OB_ISNULL(ir_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &ctdef_alloc, ir_scan_children_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate ir scan ctdef children failed", K(ret));
      } else {
        ir_scan_ctdef->children_cnt_ = ir_scan_children_cnt;
        if (use_approx_pre_agg) {
          // TODO: reduce more scan with approx
          ir_scan_ctdef->children_[0] = inv_idx_scan_ctdef;
          ir_scan_ctdef->children_[1] = inv_idx_agg_ctdef;
          ir_scan_ctdef->children_[2] = doc_id_idx_agg_ctdef;
          ir_scan_ctdef->has_inv_agg_ = true;
          ir_scan_ctdef->has_doc_id_agg_ = true;
        } else {
          ir_scan_ctdef->children_[0] = inv_idx_scan_ctdef;
          ir_scan_ctdef->children_[1] = inv_idx_agg_ctdef;
          ir_scan_ctdef->children_[2] = doc_id_idx_agg_ctdef;
          ir_scan_ctdef->children_[3] = fwd_idx_agg_ctdef;
          ir_scan_ctdef->has_inv_agg_ = true;
          ir_scan_ctdef->has_doc_id_agg_ = true;
          ir_scan_ctdef->has_fwd_agg_ = true;
        }
      }
    }
  } else {
    if (OB_ISNULL(ir_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &ctdef_alloc, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate ir scan ctdef children failed", K(ret));
    } else {
      ir_scan_ctdef->children_cnt_ = 1;
      ir_scan_ctdef->children_[0] = &tsc_ctdef.scan_ctdef_;
    }
  }

  if (OB_SUCC(ret)) {
    root_ctdef = ir_scan_ctdef;
    if (OB_FAIL(generate_text_ir_spec_exprs(op, *ir_scan_ctdef))) {
      LOG_WARN("failed to generate text ir spec exprs", K(ret), KPC(match_against));
    } else {
      const ObCostTableScanInfo *est_cost_info = op.get_est_cost_info();
      int partition_row_cnt = 0;
      if (!use_approx_pre_agg
          || nullptr == est_cost_info
          || nullptr == est_cost_info->table_meta_info_
          || 0 == est_cost_info->table_meta_info_->part_count_) {
        // No estimated info or approx agg not allowed, do total document count on execution;
      } else {
        partition_row_cnt = est_cost_info->table_meta_info_->table_row_count_ / est_cost_info->table_meta_info_->part_count_;
      }
      ir_scan_ctdef->estimated_total_doc_cnt_ = partition_row_cnt;
      index_back_doc_id_column = ir_scan_ctdef->inv_scan_doc_id_col_;
    }
  }

  if (OB_SUCC(ret) && op.get_text_retrieval_info().need_sort()) {
    ObSEArray<OrderItem, 2> order_items;
    if (OB_FAIL(order_items.push_back(op.get_text_retrieval_info().sort_key_))) {
      LOG_WARN("append order item array failed", K(ret));
    } else if (OB_FAIL(generate_das_sort_ctdef(
        order_items,
        op.get_text_retrieval_info().with_ties_,
        op.get_text_retrieval_info().topk_limit_expr_,
        op.get_text_retrieval_info().topk_offset_expr_,
        ir_scan_ctdef,
        sort_ctdef))) {
      LOG_WARN("generate sort ctdef failed", K(ret));
    } else {
      root_ctdef = sort_ctdef;
    }
  }

  if (OB_SUCC(ret) && op.get_index_back()) {
    ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
    ObDASBaseCtDef *ir_output_ctdef = nullptr == sort_ctdef ?
        static_cast<ObDASBaseCtDef *>(ir_scan_ctdef) : static_cast<ObDASBaseCtDef *>(sort_ctdef);
    if (OB_FAIL(generate_doc_id_lookup_ctdef(
        op, tsc_ctdef, ir_output_ctdef, index_back_doc_id_column, aux_lookup_ctdef))) {
      LOG_WARN("generate doc id lookup ctdef failed", K(ret));
    } else if (OB_FAIL(append_fts_relavence_project_col(aux_lookup_ctdef, ir_scan_ctdef))) {
      LOG_WARN("failed to append fts relavence project col", K(ret));
    } else {
      root_ctdef = aux_lookup_ctdef;
    }
  }
  return ret;
}

int ObTscCgService::append_fts_relavence_project_col(
    ObDASIRAuxLookupCtDef *aux_lookup_ctdef,
    ObDASIRScanCtDef *ir_scan_ctdef)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(ir_scan_ctdef)) {
    if (ir_scan_ctdef->relevance_proj_col_ != nullptr) {
      ObArray<ObExpr*> result_outputs;
      if (OB_FAIL(result_outputs.push_back(ir_scan_ctdef->relevance_proj_col_))) {
        LOG_WARN("store relevance projector column expr failed", K(ret));
      } else if (OB_FAIL(append(result_outputs, aux_lookup_ctdef->result_output_))) {
        LOG_WARN("append tmp array failed", K(ret));
      } else {
        aux_lookup_ctdef->result_output_.destroy();
        if (OB_FAIL(aux_lookup_ctdef->result_output_.init(result_outputs.count()))) {
          LOG_WARN("reserve slot failed", K(ret));
        } else if (OB_FAIL(aux_lookup_ctdef->result_output_.assign(result_outputs))) {
          LOG_WARN("store relevance projector column expr failed", K(ret));
        } else {
          aux_lookup_ctdef->relevance_proj_col_ = ir_scan_ctdef->relevance_proj_col_;
        }
      }
    }
  }
  return ret;
}

int ObTscCgService::extract_text_ir_access_columns(
    const ObLogTableScan &op,
    const ObDASScanCtDef &scan_ctdef,
    ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  const ObTextRetrievalInfo &tr_info = op.get_text_retrieval_info();
  if (scan_ctdef.ref_table_id_ == op.get_doc_id_index_table_id()) {
    if (OB_FAIL(extract_doc_id_index_back_access_columns(op, access_exprs))) {
      LOG_WARN("failed to extract doc id index back access columns", K(ret));
    }
  } else {
    switch (scan_ctdef.ir_scan_type_) {
    case ObTSCIRScanType::OB_IR_INV_IDX_SCAN:
      if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(tr_info.token_cnt_column_)))) {
        LOG_WARN("failed to push token cnt column to access exprs", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(tr_info.doc_id_column_)))) {
        LOG_WARN("failed to push document id column to access exprs", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(tr_info.doc_length_column_)))) {
        LOG_WARN("failed to add document length column to access exprs", K(ret));
      }
      break;
    case ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG:
      if (OB_FAIL(add_var_to_array_no_dup(access_exprs, tr_info.total_doc_cnt_->get_param_expr((0))))) {
        LOG_WARN("failed to push document id column to access exprs", K(ret));
      }
      break;
    case ObTSCIRScanType::OB_IR_INV_IDX_AGG:
      if (OB_FAIL(add_var_to_array_no_dup(access_exprs, tr_info.related_doc_cnt_->get_param_expr(0)))) {
        LOG_WARN("failed to push token cnt column to access exprs", K(ret));
      }
      break;
    case ObTSCIRScanType::OB_IR_FWD_IDX_AGG:
      if (OB_FAIL(add_var_to_array_no_dup(access_exprs, tr_info.doc_token_cnt_->get_param_expr(0)))) {
        LOG_WARN("failed to push token cnt column to access exprs", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected text ir scan type", K(ret), K(scan_ctdef));
    }
  }
  return ret;
}

int ObTscCgService::extract_text_ir_das_output_column_ids(
    const ObLogTableScan &op,
    const ObDASScanCtDef &scan_ctdef,
    ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  const ObTextRetrievalInfo &tr_info = op.get_text_retrieval_info();
  if (scan_ctdef.ref_table_id_ == op.get_doc_id_index_table_id()) {
    if (OB_FAIL(extract_doc_id_index_back_output_column_ids(op, output_cids))) {
      LOG_WARN("failed to get doc id index back cids", K(ret), K(scan_ctdef.ref_table_id_));
    }
  } else if (ObTSCIRScanType::OB_IR_INV_IDX_SCAN == scan_ctdef.ir_scan_type_) {
    if (OB_FAIL(output_cids.push_back(
        static_cast<ObColumnRefRawExpr *>(tr_info.token_cnt_column_)->get_column_id()))) {
      LOG_WARN("failed to push output token cnt col id", K(ret));
    } else if (OB_FAIL(output_cids.push_back(
        static_cast<ObColumnRefRawExpr *>(tr_info.doc_id_column_)->get_column_id()))) {
      LOG_WARN("failed to push output doc id col id", K(ret));
    } else if (OB_FAIL(output_cids.push_back(
        static_cast<ObColumnRefRawExpr *>(tr_info.doc_length_column_)->get_column_id()))) {
      LOG_WARN("failed to push output doc length col id", K(ret));
    }
  }
  return ret;
}

int ObTscCgService::generate_text_ir_pushdown_expr_ctdef(
    const ObLogTableScan &op,
    ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  const uint64_t scan_table_id = scan_ctdef.ref_table_id_;
  const ObTextRetrievalInfo &tr_info = op.get_text_retrieval_info();
  if (!scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown()) {
    // this das scan do not need aggregate pushdown
  } else {
    ObSEArray<ObAggFunRawExpr *, 2> agg_expr_arr;
    switch (scan_ctdef.ir_scan_type_) {
    case ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG:
      if (OB_FAIL(add_var_to_array_no_dup(agg_expr_arr, tr_info.total_doc_cnt_))) {
        LOG_WARN("failed to push document id column to access exprs", K(ret));
      }
      break;
    case ObTSCIRScanType::OB_IR_INV_IDX_AGG:
      if (OB_FAIL(add_var_to_array_no_dup(agg_expr_arr, tr_info.related_doc_cnt_))) {
        LOG_WARN("failed to push document id column to access exprs", K(ret));
      }
      break;
    case ObTSCIRScanType::OB_IR_FWD_IDX_AGG:
      if (OB_FAIL(add_var_to_array_no_dup(agg_expr_arr, tr_info.doc_token_cnt_))) {
        LOG_WARN("failed to push document id column to access exprs", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected text ir scan type with aggregate", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(scan_ctdef.aggregate_column_ids_.init(agg_expr_arr.count()))) {
      LOG_WARN("failed to init aggregate column ids", K(ret), K(agg_expr_arr.count()));
    } else if (OB_FAIL(scan_ctdef.pd_expr_spec_.pd_storage_aggregate_output_.reserve(agg_expr_arr.count()))) {
      LOG_WARN("failed to reserve memory for aggregate output expr array", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < agg_expr_arr.count(); ++i) {
      ObAggFunRawExpr *agg_expr = agg_expr_arr.at(i);
      ObExpr *expr = nullptr;
      ObRawExpr *param_expr = nullptr;
      ObColumnRefRawExpr *param_col_expr = nullptr;
      if (OB_ISNULL(agg_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected agg expr", K(ret), KPC(agg_expr));
      } else if (OB_FAIL(cg_.generate_rt_expr(*agg_expr, expr))) {
        LOG_WARN("failed to generate runtime expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate runtime expr", K(ret), KPC(agg_expr));
      } else if (OB_FAIL(scan_ctdef.pd_expr_spec_.pd_storage_aggregate_output_.push_back(expr))) {
        LOG_WARN("failed to append expr to aggregate output", K(ret));
      } else if (OB_FAIL(cg_.mark_expr_self_produced(agg_expr))) {
        LOG_WARN("failed to mark raw agg expr", K(ret), KPC(agg_expr));
      } else if (OB_UNLIKELY(agg_expr->get_real_param_exprs().empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected count all agg expr in text retrieval scan", K(ret));
      } else if (OB_ISNULL(param_expr = agg_expr->get_param_expr(0))
          || OB_UNLIKELY(!param_expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected agg param expr type", K(ret), KPC(param_expr));
      } else if (OB_ISNULL(param_col_expr = static_cast<ObColumnRefRawExpr *>(param_expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null param column expr", K(ret));
      } else if (OB_UNLIKELY(param_col_expr->get_table_id() != op.get_table_id() && !param_col_expr->is_doc_id_column())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexoected column to aggregate", K(ret), KPC(param_col_expr), K(op.get_table_id()));
      } else if (OB_FAIL(scan_ctdef.aggregate_column_ids_.push_back(param_col_expr->get_column_id()))) {
        LOG_WARN("failed to append aggregate column ids", K(ret));
      }
    }
  }
  return ret;
}

int ObTscCgService::generate_text_ir_spec_exprs(const ObLogTableScan &op,
                                                ObDASIRScanCtDef &text_ir_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr *, 4> result_output;
  const ObTextRetrievalInfo &tr_info = op.get_text_retrieval_info();
  if (OB_ISNULL(tr_info.match_expr_) || OB_ISNULL(tr_info.relevance_expr_) ||
      OB_ISNULL(tr_info.doc_id_column_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(cg_.mark_expr_self_produced(tr_info.match_expr_))) {
    LOG_WARN("failed to mark raw agg expr", K(ret), KPC(tr_info.match_expr_));
  } else if (OB_FAIL(cg_.generate_rt_expr(*tr_info.match_expr_->get_search_key(), text_ir_scan_ctdef.search_text_))) {
    LOG_WARN("cg rt expr for search text failed", K(ret));
  } else if (OB_ISNULL(tr_info.pushdown_match_filter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match filter", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*tr_info.pushdown_match_filter_, text_ir_scan_ctdef.match_filter_))) {
    LOG_WARN("cg rt expr for match filter failed", K(ret));
  } else {
    const UIntFixedArray &inv_scan_col_id = text_ir_scan_ctdef.get_inv_idx_scan_ctdef()->access_column_ids_;
    const ObColumnRefRawExpr *doc_id_column = static_cast<ObColumnRefRawExpr *>(tr_info.doc_id_column_);
    const ObColumnRefRawExpr *doc_length_column = static_cast<ObColumnRefRawExpr *>(tr_info.doc_length_column_);

    int64_t doc_id_col_idx = -1;
    int64_t doc_length_col_idx = -1;
    for (int64_t i = 0; i < inv_scan_col_id.count(); ++i) {
      if (inv_scan_col_id.at(i) == doc_id_column->get_column_id()) {
        doc_id_col_idx = i;
      } else if (inv_scan_col_id.at(i) == doc_length_column->get_column_id()) {
        doc_length_col_idx = i;
      }
    }
    if (OB_UNLIKELY(-1 == doc_id_col_idx || -1 == doc_length_col_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected doc id not found in inverted index scan access columns",
          K(ret), K(text_ir_scan_ctdef), K(doc_id_col_idx), K(doc_length_col_idx));
    } else {
      text_ir_scan_ctdef.inv_scan_doc_id_col_ =
          text_ir_scan_ctdef.get_inv_idx_scan_ctdef()->pd_expr_spec_.access_exprs_.at(doc_id_col_idx);
      text_ir_scan_ctdef.inv_scan_doc_length_col_ =
          text_ir_scan_ctdef.get_inv_idx_scan_ctdef()->pd_expr_spec_.access_exprs_.at(doc_length_col_idx);
      if (OB_FAIL(result_output.push_back(text_ir_scan_ctdef.inv_scan_doc_id_col_))) {
        LOG_WARN("failed to append output exprs", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && op.need_text_retrieval_calc_relevance()) {
    if (OB_ISNULL(tr_info.relevance_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null relevance expr", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_expr(*tr_info.relevance_expr_, text_ir_scan_ctdef.relevance_expr_))) {
      LOG_WARN("cg rt expr for relevance expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && (op.need_text_retrieval_calc_relevance() || nullptr != tr_info.pushdown_match_filter_)) {
    if (OB_FAIL(cg_.generate_rt_expr(*tr_info.match_expr_,
                                            text_ir_scan_ctdef.relevance_proj_col_))) {
      LOG_WARN("cg rt expr for relevance score proejction failed", K(ret));
    } else if (OB_ISNULL(text_ir_scan_ctdef.relevance_proj_col_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected relevance pseudo score colum not found", K(ret));
    } else if (OB_FAIL(result_output.push_back(text_ir_scan_ctdef.relevance_proj_col_))) {
      LOG_WARN("failed to append relevance expr", K(ret));
    }
  }

  if (FAILEDx(text_ir_scan_ctdef.result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret), K(result_output));
  }

  return ret;
}

int ObTscCgService::generate_doc_id_lookup_ctdef(const ObLogTableScan &op,
                                                 ObTableScanCtDef &tsc_ctdef,
                                                 ObDASBaseCtDef *ir_scan_ctdef,
                                                 ObExpr *doc_id_expr,
                                                 ObDASIRAuxLookupCtDef *&aux_lookup_ctdef)
{
  int ret = OB_SUCCESS;

  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;
  ObDASScanCtDef *scan_ctdef = nullptr;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  uint64_t doc_id_index_tid = OB_INVALID_ID;

  aux_lookup_ctdef = nullptr;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_ref_table_id(), data_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
  } else if (OB_ISNULL(data_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get data table schema", K(ret));
  } else if (OB_FAIL(data_schema->get_doc_id_rowkey_tid(doc_id_index_tid))) {
   LOG_WARN("failed to get doc id rowkey index tid", K(ret), KPC(data_schema));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_ref_table_id(),
                                                    doc_id_index_tid,
                                                    op.get_stmt(),
                                                    index_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(doc_id_index_tid));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get doc_id index schema", K(ret), K(doc_id_index_tid));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, cg_.phy_plan_->get_allocator(), scan_ctdef))) {
    LOG_WARN("alloc das ctdef failed", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_IR_AUX_LOOKUP, cg_.phy_plan_->get_allocator(), aux_lookup_ctdef))) {
    LOG_WARN("alloc aux lookup ctdef failed", K(ret));
  } else if (OB_ISNULL(aux_lookup_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &cg_.phy_plan_->get_allocator(), 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    bool has_rowscn = false;
    ObArray<ObExpr*> result_outputs;
    scan_ctdef->ref_table_id_ = doc_id_index_tid;
    aux_lookup_ctdef->children_cnt_ = 2;
    ObDASTableLocMeta *scan_loc_meta = OB_NEWx(ObDASTableLocMeta, &cg_.phy_plan_->get_allocator(), cg_.phy_plan_->get_allocator());
    if (OB_ISNULL(scan_loc_meta)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate scan location meta failed", K(ret));
    } else if (OB_FAIL(generate_das_scan_ctdef(op, *scan_ctdef, has_rowscn))) {
      LOG_WARN("generate das lookup scan ctdef failed", K(ret));
    } else if (OB_FAIL(result_outputs.assign(scan_ctdef->result_output_))) {
      LOG_WARN("construct aux lookup ctdef failed", K(ret));
    } else if (OB_ISNULL(doc_id_expr) || OB_UNLIKELY(!scan_ctdef->rowkey_exprs_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected doc id expr status", K(ret), KPC(doc_id_expr), KPC(scan_ctdef));
    } else if (OB_FAIL(scan_ctdef->rowkey_exprs_.reserve(1))) {
      LOG_WARN("failed to reserve doc id lookup rowkey exprs", K(ret));
    } else if (OB_FAIL(scan_ctdef->rowkey_exprs_.push_back(doc_id_expr))) {
      LOG_WARN("failed to append doc id rowkey expr", K(ret));
    } else if (OB_FAIL(generate_table_loc_meta(op.get_table_id(),
                                               *op.get_stmt(),
                                               *index_schema,
                                               *cg_.opt_ctx_->get_session_info(),
                                               *scan_loc_meta))) {
      LOG_WARN("generate table loc meta failed", K(ret));
    } else if (OB_FAIL(tsc_ctdef.attach_spec_.attach_loc_metas_.push_back(scan_loc_meta))) {
      LOG_WARN("store scan loc meta failed", K(ret));
    } else {
      aux_lookup_ctdef->children_[0] = ir_scan_ctdef;
      aux_lookup_ctdef->children_[1] = scan_ctdef;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(aux_lookup_ctdef->result_output_.assign(result_outputs))) {
        LOG_WARN("assign result output failed", K(ret));
      }
    }
  }

  return ret;
}

int ObTscCgService::generate_table_lookup_ctdef(const ObLogTableScan &op,
                                                ObTableScanCtDef &tsc_ctdef,
                                                ObDASBaseCtDef *scan_ctdef,
                                                ObDASTableLookupCtDef *&lookup_ctdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
  tsc_ctdef.lookup_loc_meta_ = OB_NEWx(ObDASTableLocMeta, &allocator, allocator);
  if (OB_ISNULL(tsc_ctdef.lookup_loc_meta_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate lookup location meta buffer failed", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN,
                                                       cg_.phy_plan_->get_allocator(),
                                                       tsc_ctdef.lookup_ctdef_))) {
    LOG_WARN("alloc das ctdef failed", K(ret));
  } else {
    bool has_rowscn = false;
    const ObTableSchema *table_schema = nullptr;
    ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
    tsc_ctdef.lookup_ctdef_->ref_table_id_ = op.get_real_ref_table_id();

    if (OB_FAIL(generate_das_scan_ctdef(op, *tsc_ctdef.lookup_ctdef_, has_rowscn))) {
      LOG_WARN("generate das lookup scan ctdef failed", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(),
                                                      op.get_ref_table_id(),
                                                      op.get_stmt(),
                                                      table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to table schema", K(ret));
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
      } else if (OB_FAIL(cg_.generate_rt_exprs(op.get_rowkey_exprs(),
                                               tsc_ctdef.global_index_rowkey_exprs_))) {
        LOG_WARN("fail to generate rowkey exprs", K(ret));
      }
    }

    if (OB_SUCC(ret) && op.get_index_back()) {
      ObArray<ObRawExpr*> rowkey_exprs;
      if (OB_FAIL(rowkey_exprs.assign(op.get_rowkey_exprs()))) {
        LOG_WARN("failed to assign rowkey exprs", K(ret));
      } else if (!op.get_is_index_global() && OB_FAIL(mapping_oracle_real_agent_virtual_exprs(op, rowkey_exprs))) {
        LOG_WARN("failed to mapping oracle real virtual exprs", K(ret));
      } else if (OB_FAIL(cg_.generate_rt_exprs(rowkey_exprs, tsc_ctdef.lookup_ctdef_->rowkey_exprs_))) {
        LOG_WARN("failed to generate main table rowkey exprs", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_LOOKUP, allocator, lookup_ctdef))) {
      LOG_WARN("alloc aux lookup ctdef failed", K(ret));
    } else if (OB_ISNULL(lookup_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      lookup_ctdef->children_cnt_ = 2;
      if (OB_FAIL(tsc_ctdef.attach_spec_.attach_loc_metas_.push_back(tsc_ctdef.lookup_loc_meta_))) {
        LOG_WARN("store scan loc meta failed", K(ret));
      } else {
        lookup_ctdef->children_[0] = scan_ctdef;
        lookup_ctdef->children_[1] = tsc_ctdef.lookup_ctdef_;
      }
    }
  }

  //generate lookup result output exprs
  if (OB_SUCC(ret)) {
    ObArray<ObExpr*> result_outputs;
    if (OB_FAIL(result_outputs.assign(tsc_ctdef.lookup_ctdef_->result_output_))) {
      LOG_WARN("assign result output failed", K(ret));
    } else if (DAS_OP_IR_AUX_LOOKUP == scan_ctdef->op_type_) {
      //add relevance score pseudo column to final scan result output
      ObDASIRAuxLookupCtDef *aux_lookup_ctdef = static_cast<ObDASIRAuxLookupCtDef*>(scan_ctdef);
      if (aux_lookup_ctdef->relevance_proj_col_ != nullptr) {
        if (OB_FAIL(result_outputs.push_back(aux_lookup_ctdef->relevance_proj_col_))) {
          LOG_WARN("store result outputs failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(lookup_ctdef->result_output_.assign(result_outputs))) {
        LOG_WARN("assign result output failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTscCgService::extract_doc_id_index_back_access_columns(
    const ObLogTableScan &op,
    ObIArray<ObRawExpr *> &access_exprs)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> domain_col_exprs;
  if (OB_UNLIKELY(0 == op.get_domain_exprs().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty domain expr array", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(op.get_domain_exprs(), domain_col_exprs, true))) {
    LOG_WARN("failed to extract domain column ref exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < domain_col_exprs.count(); ++i) {
    ObRawExpr *raw_expr = domain_col_exprs.at(i);
    ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(raw_expr);
    if (col_expr->is_doc_id_column()
        || (col_expr->get_table_id() == op.get_table_id() && col_expr->is_rowkey_column())) {
      if (OB_FAIL(add_var_to_array_no_dup(access_exprs, raw_expr))) {
        LOG_WARN("failed to push doc id index back access column to access exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTscCgService::extract_doc_id_index_back_output_column_ids(
    const ObLogTableScan &op,
    ObIArray<uint64_t> &output_cids)
{
  // outpout main table rowkey for index back
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  ObArray<uint64_t> rowkey_cids;
  if (OB_FAIL(cg_.opt_ctx_->get_schema_guard()->get_table_schema(MTL_ID(), op.get_real_ref_table_id(), table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table schema", K(ret));
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_cids))) {
    LOG_WARN("get rowkey column ids failed", K(ret));
  } else if (OB_FAIL(append(output_cids, rowkey_cids))) {
    LOG_WARN("failed to append output column ids", K(ret));
  }
  return ret;
}

int ObTscCgService::filter_out_match_exprs(ObIArray<ObRawExpr*> &exprs) {
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> temp_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (!exprs.at(i)->has_flag(CNT_MATCH_EXPR) && OB_FAIL(temp_exprs.push_back(exprs.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(exprs.assign(temp_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  return ret;
}

int ObTscCgService::generate_das_sort_ctdef(
    const ObIArray<OrderItem> &sort_keys,
    const bool fetch_with_ties,
    ObRawExpr *topk_limit_expr,
    ObRawExpr *topk_offset_expr,
    ObDASBaseCtDef *child_ctdef,
    ObDASSortCtDef *&sort_ctdef)
{
  int ret = OB_SUCCESS;
  const int64_t sort_cnt = sort_keys.count();
  ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();
  if (OB_UNLIKELY(0 == sort_cnt) || OB_ISNULL(child_ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sort arg", K(ret), K(sort_cnt), KPC(child_ctdef));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_SORT, ctdef_alloc, sort_ctdef))) {
    LOG_WARN("alloc sort ctdef failed ", K(ret));
  } else if (OB_FAIL(sort_ctdef->sort_collations_.init(sort_cnt))) {
    LOG_WARN("failed to init sort collations", K(ret));
  } else if (OB_FAIL(sort_ctdef->sort_cmp_funcs_.init(sort_cnt))) {
    LOG_WARN("failed to init sort cmp funcs", K(ret));
  } else if (OB_FAIL(sort_ctdef->sort_exprs_.init(sort_cnt))) {
    LOG_WARN("failed to init sort exprs", K(ret));
  } else if (OB_ISNULL(sort_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &ctdef_alloc, 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate ir scan ctdef children failed", K(ret));
  } else if (nullptr != topk_limit_expr &&
        OB_FAIL(cg_.generate_rt_expr(*topk_limit_expr, sort_ctdef->limit_expr_))) {
    LOG_WARN("cg rt expr for top-k limit expr failed", K(ret));
  } else if (nullptr != topk_offset_expr &&
        OB_FAIL(cg_.generate_rt_expr(*topk_offset_expr, sort_ctdef->offset_expr_))) {
    LOG_WARN("cg rt expr for top-k offset expr failed", K(ret));
  } else {
    sort_ctdef->children_cnt_ = 1;
    sort_ctdef->children_[0] = child_ctdef;
    sort_ctdef->fetch_with_ties_ = fetch_with_ties;
  }

  ObSEArray<ObExpr *, 4> result_output;
  int64_t field_idx = 0;
  for (int64_t i = 0; i < sort_keys.count() && OB_SUCC(ret); ++i) {
    const OrderItem &order_item = sort_keys.at(i);
    ObExpr *expr = nullptr;
    if (OB_FAIL(cg_.generate_rt_expr(*order_item.expr_, expr))) {
      LOG_WARN("failed to generate rt expr", K(ret));
    } else {
      ObSortFieldCollation field_collation(field_idx++,
          expr->datum_meta_.cs_type_,
          order_item.is_ascending(),
          (order_item.is_null_first() ^ order_item.is_ascending()) ? NULL_LAST : NULL_FIRST);
      ObSortCmpFunc cmp_func;
      cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
          expr->datum_meta_.type_,
          expr->datum_meta_.type_,
          field_collation.null_pos_,
          field_collation.cs_type_,
          expr->datum_meta_.scale_,
          lib::is_oracle_mode(),
          expr->obj_meta_.has_lob_header(),
          expr->datum_meta_.precision_,
          expr->datum_meta_.precision_);
      if (OB_ISNULL(cmp_func.cmp_func_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cmp_func is null, check datatype is valid", K(ret));
      } else if (OB_FAIL(sort_ctdef->sort_cmp_funcs_.push_back(cmp_func))) {
        LOG_WARN("failed to append sort function", K(ret));
      } else if (OB_FAIL(sort_ctdef->sort_collations_.push_back(field_collation))) {
        LOG_WARN("failed to push back field collation", K(ret));
      } else if (OB_FAIL(sort_ctdef->sort_exprs_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        field_idx++;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_array_no_dup(result_output, sort_ctdef->sort_exprs_))) {
    LOG_WARN("failed to append sort exprs to result output", K(ret));
  } else if (ObDASTaskFactory::is_attached(child_ctdef->op_type_)
      && OB_FAIL(append_array_no_dup(result_output, static_cast<ObDASAttachCtDef *>(child_ctdef)->result_output_))) {
    LOG_WARN("failed to append child result output", K(ret));
  } else if (OB_FAIL(sort_ctdef->result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret));
  }
  return ret;
}

int ObTscCgService::mapping_oracle_real_agent_virtual_exprs(const ObLogTableScan &op,
                                                            common::ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mapping_real_virtual_table(op.get_ref_table_id())) {
    //the access exprs are the agent virtual table columns, but das need the real table columns
    //now to replace the real table column
    for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); ++i) {
      ObRawExpr *expr = access_exprs.at(i);
      ObRawExpr *mapping_expr = nullptr;
      uint64_t column_id = UINT64_MAX;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr expr", K(expr), K(ret));
      } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
        // keep orign expr as access expr
      } else if (OB_ISNULL(mapping_expr = op.get_real_expr(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mapping expr is null", K(ret), KPC(expr));
      } else {
        //replace the agent virtual table column expr
        access_exprs.at(i) = mapping_expr;
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
