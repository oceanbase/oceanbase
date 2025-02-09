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
#include "ob_tsc_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "src/share/vector_index/ob_vector_index_util.h"
#include "share/domain_id/ob_domain_id.h"
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
  if (op.is_new_query_range()) {
    query_flag.set_is_new_query_range();
  }
  OZ(generate_mr_mv_scan_flag(op, query_flag));
  tsc_ctdef.scan_flags_ = query_flag;
  if (op.use_index_merge()) {
    tsc_ctdef.use_index_merge_ = true;
  }
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
      const ObString &table_format_or_properties = table_schema->get_external_file_format().empty() ?
                                            table_schema->get_external_properties() :
                                            table_schema->get_external_file_format();
      if (table_format_or_properties.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_format_or_properties is empty", K(ret));
      } else if (OB_FAIL(scan_ctdef.external_file_format_str_.store_str(table_format_or_properties))) {
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
    if (op.is_text_retrieval_scan() || op.is_post_vec_idx_scan()) {
      scan_ctdef.ir_scan_type_ = ObTSCIRScanType::OB_IR_INV_IDX_SCAN;
    }
    DASScanCGCtx cg_ctx;
    if (OB_UNLIKELY(op.use_index_merge())) {
      // tsc_ctdef.scan_ctdef will not be generated in index merge
    } else if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, scan_ctdef, has_rowscn))) {
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
    } else if ((info->get_table_location().use_das() && info->get_table_location().get_has_dynamic_exec_param())
               || info->get_table_location().is_dynamic_replica_select_table()) {
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
    DASScanCGCtx cg_ctx;
    if (OB_FAIL(generate_text_ir_ctdef(op, cg_ctx, tsc_ctdef, tsc_ctdef.scan_ctdef_, root_ctdef))) {
      LOG_WARN("failed to generate text ir ctdef", K(ret));
    } else {
      need_attach = true;
    }
  }

  if (OB_SUCC(ret) && op.is_multivalue_index_scan()) {
    if (OB_FAIL(generate_multivalue_ir_ctdef(op, tsc_ctdef, root_ctdef))) {
      LOG_WARN("failed to generate multivalue ir ctdef", K(ret));
    } else {
      need_attach = true;
    }
  }

  if (OB_SUCC(ret) && op.is_spatial_index_scan()) {
    if (OB_FAIL(generate_gis_ir_ctdef(op, tsc_ctdef, root_ctdef))) {
      LOG_WARN("failed to generate spatial ir ctdef", K(ret));
    } else {
      need_attach = true;
    }
  }

  if (OB_SUCC(ret) && (op.is_post_vec_idx_scan() || op.is_pre_vec_idx_scan())) {
    if (OB_FAIL(generate_vec_idx_ctdef(op, tsc_ctdef, root_ctdef))) {
      LOG_WARN("failed to generate text ir ctdef", K(ret));
    } else {
      need_attach = true;
    }
  }

  if (OB_SUCC(ret) && op.use_index_merge()) {
    ObDASIndexMergeCtDef *index_merge_ctdef = nullptr;
    if (OB_FAIL(generate_index_merge_ctdef(op, tsc_ctdef, index_merge_ctdef))) {
      LOG_WARN("failed to generate index merge ctdef", K(ret));
    } else if (OB_ISNULL(index_merge_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr index merge ctdef", K(ret));
    } else {
      root_ctdef = index_merge_ctdef;
      need_attach = true;
    }
  }

  ObDASAttachCtDef *domain_id_merge_ctdef = nullptr;
  if (OB_FAIL(ret)) {
  } else if (op.get_index_back()) {
    ObDASTableLookupCtDef *lookup_ctdef = nullptr;
    if (OB_FAIL(generate_table_lookup_ctdef(op, tsc_ctdef, root_ctdef, lookup_ctdef, domain_id_merge_ctdef))) {
      LOG_WARN("generate table lookup ctdef failed", K(ret));
    } else if (op.is_tsc_with_domain_id()) {
      need_attach = true;
      if (op.get_is_index_global()) {
        root_ctdef = domain_id_merge_ctdef;
      } else {
        root_ctdef = lookup_ctdef;
      }
    } else {
      root_ctdef = lookup_ctdef;
    }
  } else if (op.is_tsc_with_domain_id()) {
    if (OB_UNLIKELY(root_ctdef != &scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, root ctdef isn't equal to scan ctdef", K(ret));
    } else if (OB_FAIL(generate_das_scan_ctdef_with_domain_id(op, tsc_ctdef, &scan_ctdef, domain_id_merge_ctdef))) {
      LOG_WARN("fail to generate das scan ctdef with doc id", K(ret));
    } else {
      scan_ctdef.multivalue_idx_ = op.get_multivalue_col_idx();
      scan_ctdef.multivalue_type_ = op.get_multivalue_type();
      root_ctdef = domain_id_merge_ctdef;
      need_attach = true;
    }
  }

  if (OB_SUCC(ret) && op.get_vector_index_info().is_hnsw_vec_scan()) {
    if (OB_FAIL(calc_enable_use_simplified_scan(op, tsc_ctdef.lookup_ctdef_->result_output_, root_ctdef))) {
      LOG_WARN("assign result output failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && op.has_func_lookup()) {
    ObDASBaseCtDef *rowkey_scan_ctdef = nullptr;
    ObDASBaseCtDef *main_lookup_ctdef = nullptr;
    if (op.get_index_back()) {
      rowkey_scan_ctdef = static_cast<ObDASTableLookupCtDef *>(root_ctdef)->children_[0];
      main_lookup_ctdef = tsc_ctdef.lookup_ctdef_;
    } else {
      rowkey_scan_ctdef = root_ctdef;
    }
    if (OB_FAIL(generate_functional_lookup_ctdef(op, tsc_ctdef, rowkey_scan_ctdef, main_lookup_ctdef, root_ctdef))) {
      LOG_WARN("failed to generate functional lookup ctdef", K(ret));
    } else {
      need_attach = true;
    }
  }

  if (OB_SUCC(ret) && need_attach) {
    if (!op.get_is_index_global()) {
      tsc_ctdef.lookup_ctdef_ = nullptr;
      tsc_ctdef.lookup_loc_meta_ = nullptr;
    }
    tsc_ctdef.attach_spec_.attach_ctdef_ = root_ctdef;
  }

  LOG_DEBUG("generate tsc ctdef finish", K(ret), K(op), K(tsc_ctdef),
                                                    K(tsc_ctdef.scan_ctdef_.pd_expr_spec_.ext_file_column_exprs_));
  return ret;
}

int ObTscCgService::calc_enable_use_simplified_scan(
    const ObLogTableScan &op,
    const ExprFixedArray &result_outputs,
    ObDASBaseCtDef *root_ctdef)
{
  int ret = OB_SUCCESS;
  bool is_access_pk = false;
  const ObDMLStmt *stmt = nullptr;
  ObRawExpr *vector_expr = nullptr;

  if (OB_ISNULL(stmt = op.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (OB_ISNULL(vector_expr = stmt->get_first_vector_expr())) {
  } else if (!vector_expr->has_flag(IS_CUT_CALC_EXPR)) {
  } else {
    // real result output is pk column
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt *>(stmt);
    is_access_pk = true;
    for (int64_t i = 0; is_access_pk && OB_SUCC(ret) && i < select_stmt->get_select_items().count(); ++i) {
      const SelectItem &si = select_stmt->get_select_items().at(i);
      if (OB_ISNULL(si.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item expr is null", K(ret));
      } else if (si.expr_->is_column_ref_expr()) {
        const ObColumnRefRawExpr* col_ref = static_cast<const ObColumnRefRawExpr*>(si.expr_);
        if (OB_ISNULL(col_ref)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ref expr is null", K(ret));
        } else if (!col_ref->is_rowkey_column()) {
          is_access_pk = false;
        }
      } else {
        is_access_pk = false;
      }
    }

    if (OB_SUCC(ret) && is_access_pk) {
      bool enable_simplified_scan = true;
      const ObDASVecAuxScanCtDef* vec_scan_def = nullptr;
      const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
      const ObDASSortCtDef* sort_scan_def = nullptr;
      ObDASScanCtDef *vid_rowkey_ctdef = nullptr;
      ObArray<ObExpr *> extra_access_exprs;

      if (OB_FAIL(ObDASUtils::find_target_ctdef(root_ctdef, DAS_OP_VEC_SCAN, vec_scan_def))) {
        LOG_WARN("fail to vec scan define", K(ret));
      } else if (OB_FAIL(ObDASUtils::find_target_ctdef(root_ctdef, DAS_OP_SORT, sort_scan_def))) {
        LOG_WARN("fail to sort scan define", K(ret));
      } else if (OB_FAIL(ObDASUtils::find_target_ctdef(root_ctdef, DAS_OP_IR_AUX_LOOKUP, aux_lookup_ctdef))) {
        LOG_WARN("find vec aux lookup definition failed");
      } else {
        vid_rowkey_ctdef = const_cast<ObDASScanCtDef*>(aux_lookup_ctdef->get_lookup_scan_ctdef());
        if (OB_ISNULL(vid_rowkey_ctdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get ctdef vid rowkey", K(ret));
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < result_outputs.count(); ++i) {
        ObExpr* expr = result_outputs.at(i);
        if (has_exist_in_array(vid_rowkey_ctdef->result_output_, expr)) {
        } else if (OB_FAIL(extra_access_exprs.push_back(expr))) {
          LOG_WARN("fail to store extra columns", K(ret));
        }
      }

      if (OB_SUCC(ret) && extra_access_exprs.count() > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < extra_access_exprs.count() && enable_simplified_scan; ++i) {
          const ObExpr* expr = extra_access_exprs.at(i);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to expected expr", K(ret), K(i));
          } else if (T_REF_COLUMN == expr->type_) {
            const ObDASSortCtDef* sort_ctdef = sort_scan_def;
            const ObExpr* sort_expr = nullptr;
            bool is_contains = false;
            for (int64_t j = 0; !is_contains && OB_SUCC(ret) && j < sort_ctdef->sort_exprs_.count(); ++j) {
              const ObExpr* sort_expr = sort_ctdef->sort_exprs_.at(j);
              if (OB_ISNULL(sort_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("fail to expected sort expr", K(ret), K(j));
              } else if (sort_expr->is_vector_sort_expr()) {
                if (OB_FAIL(sort_expr->contain_expr(expr, is_contains))) {
                  LOG_WARN("fail to get contain expr", K(ret), K(j));
                }
              }
            } // end for
            enable_simplified_scan = is_contains;
          }
        } // end for
      }

      if (OB_SUCC(ret) && enable_simplified_scan) {
        ObDASVecAuxScanCtDef* tmp_vec_scan_def = const_cast<ObDASVecAuxScanCtDef*>(vec_scan_def);
        tmp_vec_scan_def->access_pk_ = enable_simplified_scan;
      }
    }
  }
  return ret;
}

int ObTscCgService::generate_table_param(const ObLogTableScan &op,
                                         const DASScanCGCtx &cg_ctx,
                                         ObDASScanCtDef &scan_ctdef,
                                         common::ObIArray<uint64_t> &tsc_out_cols)
{
  int ret = OB_SUCCESS;
  ObTableID index_id = scan_ctdef.ref_table_id_;
  const ObTableSchema *table_schema = NULL;
  const bool pd_agg = scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown();
  const bool pd_group_by =  scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_group_by_pushdown();
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  ObBasicSessionInfo *session_info = cg_.opt_ctx_->get_session_info();
  int64_t route_policy = 0;
  bool is_cs_replica_query = false;
  CK(OB_NOT_NULL(schema_guard), OB_NOT_NULL(session_info));
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
  } else if (table_schema->is_vec_index() && FALSE_IT(scan_ctdef.table_param_.set_is_vec_index(true))) {
  } else if (FALSE_IT(scan_ctdef.table_param_.set_is_partition_table(table_schema->is_partitioned_table()))) {
  } else if (OB_FAIL(extract_das_output_column_ids(op, scan_ctdef, *table_schema, cg_ctx, tsc_out_cols))) {
    LOG_WARN("extract tsc output column ids failed", K(ret));
  } else if (OB_FAIL(session_info->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
    LOG_WARN("get route policy failed", K(ret));
  } else {
    is_cs_replica_query = ObRoutePolicyType::COLUMN_STORE_ONLY == route_policy;
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(scan_ctdef.table_param_.get_enable_lob_locator_v2()
                          = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0))) {
  } else if (OB_FAIL(scan_ctdef.table_param_.convert(*table_schema,
                                                     scan_ctdef.access_column_ids_,
                                                     scan_ctdef.pd_expr_spec_.pd_storage_flag_,
                                                     &tsc_out_cols,
                                                     is_oracle_mapping_real_virtual_table(op.get_ref_table_id()), /* for real agent table , use mysql mode compulsory*/
                                                     is_cs_replica_query))) {
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
                                               common::ObIArray<ObExpr *> &domain_id_expr,
                                               common::ObIArray<uint64_t> &domain_id_col_ids,
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
  if (OB_UNLIKELY(access_column_cnt != access_expr_cnt || domain_id_expr.count() != domain_id_col_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access column count is invalid", K(ret), K(access_column_cnt), K(access_expr_cnt), K(domain_id_expr), K(domain_id_col_ids));
  } else if (OB_FAIL(scan_ctdef.result_output_.init(output_cids.count() + domain_id_expr.count() + agg_expr_cnt + trans_expr_cnt))) {
    LOG_WARN("init result output failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_cids.count(); ++i) {
    int64_t idx = OB_INVALID_INDEX;
    if (has_exist_in_array(domain_id_col_ids, output_cids.at(i), &idx)) {
      if (OB_FAIL(scan_ctdef.result_output_.push_back(domain_id_expr.at(idx)))) {
        LOG_WARN("fail to push back domain id", K(ret), K(domain_id_col_ids.at(idx)));
      }
    } else if (!has_exist_in_array(scan_ctdef.access_column_ids_, output_cids.at(i), &idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output column does not exist in access column ids", K(ret),
               K(scan_ctdef.access_column_ids_), K(output_cids.at(i)), K(output_cids),
               K(scan_ctdef.ref_table_id_), K(scan_ctdef.ref_table_id_), K(scan_ctdef.ir_scan_type_));
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
  if (OB_NOT_NULL(op.get_auto_split_filter())) {
    ObRawExpr *auto_split_expr = const_cast<ObRawExpr *>(op.get_auto_split_filter());
    if (OB_FAIL(scan_pushdown_filters.push_back(auto_split_expr))) {
      LOG_WARN("fail to push back auto split filter", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (op.use_index_merge()) {
    // full filters is used for final check when in index merge
    // we need to pushdown full filters to lookup as much as possible to avoid
    // the transmission of large results during DAS remote execution
    // all index table scan filters are generated in @generate_das_scan_ctdef()
    const ObIArray<ObRawExpr*> &full_filters = op.get_full_filters();
    ObDASBaseCtDef *attach_ctdef = spec.tsc_ctdef_.attach_spec_.attach_ctdef_;

    if (OB_ISNULL(attach_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("attach ctdef is null", K(ret));
    } else if (attach_ctdef->op_type_ == ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP) {
      if (OB_FAIL(nonpushdown_filters.assign(full_filters))) {
        LOG_WARN("failed to assign full filter to non-pushdown filters", K(ret));
      }
    } else if (OB_FAIL(op.extract_nonpushdown_filters(full_filters,
                                               nonpushdown_filters,
                                               lookup_pushdown_filters))) {
      LOG_WARN("failed to extract lookup pushdown filters", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (lookup_ctdef != nullptr && OB_FAIL(generate_pd_storage_flag(op.get_plan(),
                                                  op.get_ref_table_id(),
                                                  op.get_access_exprs(),
                                                  op.get_type(),
                                                  op.get_index_back() && op.get_is_index_global(),
                                                  op.use_column_store(),
                                                  lookup_ctdef->pd_expr_spec_))) {
      LOG_WARN("generate pd storage flag for lookup ctdef failed", K(ret));
    }
  } else if (OB_FAIL(op.extract_pushdown_filters(nonpushdown_filters,
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
                                              op.use_column_store(),
                                              scan_ctdef.pd_expr_spec_))) {
    LOG_WARN("generate pd storage flag for scan ctdef failed", K(ret));
  } else if (lookup_ctdef != nullptr &&
      OB_FAIL(generate_pd_storage_flag(op.get_plan(),
                                       op.get_ref_table_id(),
                                       op.get_access_exprs(),
                                       op.get_type(),
                                       op.get_index_back() && op.get_is_index_global(), /*generate_pd_storage_flag*/
                                       op.use_column_store(),
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
                                             const bool use_column_store,
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
        if ((use_column_store && T_ORA_ROWSCN == (*e)->get_expr_type()) || T_PSEUDO_EXTERNAL_FILE_URL == (*e)->get_expr_type()
            || T_PSEUDO_OLD_NEW_COL == (*e)->get_expr_type()) {
          pd_blockscan = false;
          pd_filter = false;
        } else if (T_ORA_ROWSCN != (*e)->get_expr_type()) {
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
                                             const DASScanCGCtx &cg_ctx,
                                             ObDASScanCtDef &scan_ctdef,
                                             ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableID &scan_table_id = scan_ctdef.ref_table_id_;
  const bool use_index_merge = scan_ctdef.is_index_merge_;
  if (cg_ctx.is_func_lookup_ && scan_table_id != op.get_rowkey_doc_table_id()) {
    const ObTextRetrievalInfo &tr_info = op.get_lookup_tr_infos().at(cg_ctx.curr_func_lookup_idx_);
    if (OB_FAIL(extract_text_ir_access_columns(op, tr_info, scan_ctdef, access_exprs))) {
      LOG_WARN("failed to extract text ir access columns for functional lookup", K(ret));
    }
  } else if (cg_ctx.is_merge_fts_index_) {
    const ObTextRetrievalInfo &tr_info = op.get_merge_tr_infos().at(cg_ctx.curr_merge_fts_idx_);
    if (OB_FAIL(extract_text_ir_access_columns(op, tr_info, scan_ctdef, access_exprs))) {
      LOG_WARN("failed to extract text ir access columns for index merge", K(ret));
    }
  } else if ((!op.is_scan_domain_id_table(scan_table_id) && scan_table_id != op.get_rowkey_doc_table_id())
      && ((op.is_text_retrieval_scan() && scan_table_id != op.get_ref_table_id())
          || (op.is_multivalue_index_scan() && scan_table_id == op.get_doc_id_index_table_id()))) {
    // non main table scan in text retrieval
    if (OB_FAIL(extract_text_ir_access_columns(op, op.get_text_retrieval_info(), scan_ctdef, access_exprs))) {
      LOG_WARN("failed to extract text ir access columns", K(ret));
    }
  } else if (cg_ctx.is_func_lookup_ && scan_table_id == op.get_rowkey_doc_table_id()) {
    if (OB_FAIL(extract_rowkey_doc_access_columns(op, scan_ctdef, access_exprs))) {
      LOG_WARN("fail to extract rowkey doc access columns", K(ret));
    }
  } else if ((op.is_post_vec_idx_scan() || (op.is_pre_vec_idx_scan() && (op.is_vec_index_table_id(scan_table_id) || scan_ctdef.ir_scan_type_ == OB_VEC_COM_AUX_SCAN))) &&
              (scan_table_id != op.get_ref_table_id() || scan_ctdef.ir_scan_type_ == OB_VEC_COM_AUX_SCAN) &&
              (!op.is_scan_domain_id_table(scan_table_id) || scan_ctdef.ir_scan_type_ == OB_VEC_ROWKEY_VID_SCAN)) {
    if (OB_FAIL(extract_vec_ir_access_columns(op, scan_ctdef, access_exprs))) {
      LOG_WARN("failed to extract vector access columns", K(ret));
    }
  } else if (op.is_scan_domain_id_table(scan_table_id)) {
    if (OB_FAIL(extract_rowkey_domain_id_access_columns(op, scan_ctdef, access_exprs))) {
      LOG_WARN("fail to extract rowkey doc access columns", K(ret));
    }
  } else if (op.need_doc_id_index_back() && scan_table_id == op.get_doc_id_index_table_id()) {
    // directly extract doc id index back access columns when scan doc_id_index_table
    if (OB_FAIL(extract_doc_id_index_back_access_columns(op, access_exprs))) {
      LOG_WARN("failed to extract doc id index back access columns", K(scan_table_id), K(ret));
    }
  } else if (op.get_index_back() && (scan_table_id != op.get_real_ref_table_id() || use_index_merge)) {
    //this das scan is index scan and will lookup the data table later
    //index scan + lookup data table: the index scan only need access
    //range condition columns + index filter columns + the data table rowkeys
    const ObIArray<ObRawExpr*> &range_conditions = use_index_merge ?
        op.get_index_range_conds(scan_ctdef.index_merge_idx_) : op.get_range_conditions();
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(range_conditions, access_exprs))) {
      LOG_WARN("extract column exprs failed", K(ret));
    }

    //store index filter columns
    if (OB_SUCC(ret)) {
      ObArray<ObRawExpr *> filter_columns;
      ObArray<ObRawExpr *> nonpushdown_filters;
      ObArray<ObRawExpr *> scan_pushdown_filters;
      ObArray<ObRawExpr *> lookup_pushdown_filters;
      if (use_index_merge &&
          OB_FAIL(scan_pushdown_filters.assign(op.get_index_filters(scan_ctdef.index_merge_idx_)))) {
        LOG_WARN("failed to assign index merge filters", K(ret));
      } else if (!use_index_merge &&
          OB_FAIL(const_cast<ObLogTableScan &>(op).extract_pushdown_filters(nonpushdown_filters,
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
  } else if (op.use_index_merge()) {
    // add lookup pushdown exprs when use index merge
    ObArray<ObRawExpr*> nonpushdown_filters;
    ObArray<ObRawExpr*> lookup_pushdown_filters;
    ObArray<ObRawExpr*> filter_columns;
    const ObIArray<ObRawExpr*> &full_filters = op.get_full_filters();
    if (OB_FAIL(op.extract_nonpushdown_filters(full_filters,
                                               nonpushdown_filters,
                                               lookup_pushdown_filters))) {
      LOG_WARN("failed to extract lookup pushdown filters", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(lookup_pushdown_filters,
                                                            filter_columns))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(access_exprs, filter_columns))) {
      LOG_WARN("failed to append filter columns", K(ret));
    }
  }
  // extrace auto split filter column expr if need
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(op.get_auto_split_filter())) {
      ObArray<ObRawExpr *> auto_split_filter_columns;
      ObRawExpr *auto_split_expr = const_cast<ObRawExpr *>(op.get_auto_split_filter());
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(auto_split_expr,
                                                       auto_split_filter_columns))) {
        LOG_WARN("extract column exprs failed", K(ret));
      } else if (OB_FAIL(append_array_no_dup(access_exprs, auto_split_filter_columns))) {
        LOG_WARN("append filter column to access exprs failed", K(ret));
      }
    }
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
          !static_cast<ObColumnRefRawExpr *>(expr)->is_xml_column()
          && !static_cast<ObColumnRefRawExpr *>(expr)->is_doc_id_column()
          && !static_cast<ObColumnRefRawExpr *>(expr)->is_vec_hnsw_vid_column()
          && !static_cast<ObColumnRefRawExpr *>(expr)->is_vec_cid_column()
          && !static_cast<ObColumnRefRawExpr *>(expr)->is_vec_pq_cids_column()) {
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
  LOG_TRACE("extract das access exprs", K(scan_table_id), K(op.get_real_ref_table_id()),
      K(op.get_index_back()), K(scan_ctdef.is_index_merge_), K(access_exprs));
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
  const bool need_filter_out_match_expr = op.is_text_retrieval_scan() || op.has_func_lookup() || op.use_index_merge();
  if (op.use_index_merge()) {
    // assign full filters for lookup of index merge, and extract columns as its output
    if (OB_FAIL(tsc_exprs.assign(op.get_full_filters()))) {
      LOG_WARN("failed to assign full filters", K(ret));
    }
  } else if (OB_FAIL(const_cast<ObLogTableScan &>(op).extract_pushdown_filters(tsc_exprs, //non-pushdown filters
                                          scan_pushdown_filters,
                                          lookup_pushdown_filters))) {
    LOG_WARN("extract pushdown filters failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_array_no_dup(tsc_exprs, op.get_output_exprs()))) {
    LOG_WARN("append output exprs failed", K(ret));
  } else if (need_filter_out_match_expr && OB_FAIL(filter_out_match_exprs(tsc_exprs))) {
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
                                          const DASScanCGCtx &cg_ctx,
                                          ObDASScanCtDef &scan_ctdef,
                                          common::ObIArray<ObExpr *> &domain_id_expr,
                                          common::ObIArray<uint64_t> &domain_id_col_ids,
                                          bool &has_rowscn)
{
  int ret = OB_SUCCESS;
  has_rowscn = false;
  const ObTableSchema *table_schema = nullptr;
  ObTableID table_id = scan_ctdef.ref_table_id_;
  ObArray<uint64_t> access_column_ids;
  ObArray<ObRawExpr*> access_exprs;
  ObArray<ObRawExpr*> scan_param_access_exprs;
  ObArray<ObRawExpr*> domain_id_access_expr;

  if (OB_FAIL(cg_.opt_ctx_->get_schema_guard()->get_table_schema(MTL_ID(), table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to table schema", K(ret));
  } else if (OB_FAIL(extract_das_access_exprs(op, cg_ctx, scan_ctdef, access_exprs))) {
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
    bool is_domain_id_access_expr = false;
    int64_t domain_type = ObDomainIdUtils::MAX;
    if (OB_UNLIKELY(OB_ISNULL(expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
      //only data table need to produce rowscn
      if (table_schema->is_index_table() && op.is_index_scan() && !op.get_index_back()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rowscn only can be produced by data table", K(ret));
      } else if (OB_FAIL(access_column_ids.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
        LOG_WARN("store output column ids failed", K(ret));
      } else {
        has_rowscn = true;
        LOG_DEBUG("need row scn");
      }
    } else if (T_PSEUDO_OLD_NEW_COL == expr->get_expr_type()) {
      OZ(access_column_ids.push_back(OB_MAJOR_REFRESH_MVIEW_OLD_NEW_COLUMN_ID));
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
      const bool is_doc_fun_lookup = cg_ctx.is_func_lookup_ && table_schema->is_rowkey_doc_id();
      const bool is_vec_rowkey_vid_scan = scan_ctdef.ir_scan_type_ == OB_VEC_ROWKEY_VID_SCAN;
      bool domain_id_in_rowkey_domain = op.is_scan_domain_id_table(table_schema->get_table_id());
      real_table_id = (domain_id_in_rowkey_domain || is_doc_fun_lookup || is_vec_rowkey_vid_scan) ? table_id : real_table_id;
      if (!col_expr->has_flag(IS_COLUMN) || (col_expr->get_table_id() != real_table_id && !ObDomainIdUtils::is_domain_id_index_col_expr(col_expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Expected basic column", K(ret),
                 K(*col_expr), K(col_expr->has_flag(IS_COLUMN)),
                 K(col_expr->get_table_id()), K(real_table_id), K(op.get_real_ref_table_id()), K(op.get_ref_table_id()), K(op.get_table_id()), K(op.get_real_index_table_id()));
      } else if (op.is_tsc_with_domain_id() && table_schema->is_user_table() && ObDomainIdUtils::is_domain_id_index_col_expr(col_expr)) {
        // skip domain id column in data table
        is_domain_id_access_expr = true;
        ObIndexType index_type = ObIndexType::INDEX_TYPE_MAX;
        if (col_expr->is_vec_pq_cids_column() || col_expr->is_vec_cid_column()) {
          if (OB_FAIL(ObVectorIndexUtil::get_vector_index_type(cg_.opt_ctx_->get_schema_guard(), *table_schema, col_expr->get_column_id(), index_type))) {
            LOG_WARN("fail to get vector index type", K(ret), KPC(col_expr));
          }
        }
        if (OB_SUCC(ret)) {
          domain_type = ObDomainIdUtils::get_domain_type_by_col_expr(col_expr, index_type);
          if (domain_type == ObDomainIdUtils::MAX) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get valid domain type by col expr", K(ret), KPC(col_expr));
          }
        }
      } else if (OB_FAIL(access_column_ids.push_back(col_expr->get_column_id()))) {
        LOG_WARN("store column id failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (is_domain_id_access_expr) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr *>(expr);
        if (OB_FAIL(domain_id_access_expr.push_back(expr))) {
          LOG_WARN("fail to push back domain id access expr", K(ret));
        } else if (OB_FAIL(domain_id_col_ids.push_back(col_expr->get_column_id()))) {
          LOG_WARN("fail to push back domain id col id", K(ret));
        }
      } else if (OB_FAIL(scan_param_access_exprs.push_back(expr))) {
        LOG_WARN("fail to push back scan param access expr", K(ret));
      }
    }
  } // end for
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cg_.generate_rt_exprs(scan_param_access_exprs, scan_ctdef.pd_expr_spec_.access_exprs_))) {
      LOG_WARN("generate rt exprs failed", K(ret), K(scan_param_access_exprs));
    } else if (OB_FAIL(cg_.generate_rt_exprs(domain_id_access_expr, domain_id_expr))) {
      LOG_WARN("fail to generate no access rt exprs", K(ret));
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
                                                 const DASScanCGCtx &cg_ctx,
                                                 ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObAggFunRawExpr*> &pushdown_aggr_exprs = op.get_pushdown_aggr_exprs();
  const uint64_t aggregate_output_count = pushdown_aggr_exprs.count();
  const ObIArray<ObRawExpr*> &group_by_columns = op.get_pushdown_groupby_columns();
  const uint64_t group_by_column_count = group_by_columns.count();
  if ((!cg_ctx.is_func_lookup_ && op.is_text_retrieval_scan()) || // text retrieval scan on fulltext index
     (cg_ctx.is_func_lookup_ && !cg_ctx.is_rowkey_doc_scan_in_func_lookup()) || // func lookup on fulltext index (exclude the rowkey_doc scan in func lookup)
     cg_ctx.is_merge_fts_index_) { // text retrieval scan on index merge
    // text retrieval scan on fulltext index
    const ObTextRetrievalInfo &tr_info = cg_ctx.is_func_lookup_ ? op.get_lookup_tr_infos().at(cg_ctx.curr_func_lookup_idx_)
                                       : cg_ctx.is_merge_fts_index_ ? op.get_merge_tr_infos().at(cg_ctx.curr_merge_fts_idx_)
                                       : op.get_text_retrieval_info();
    if (OB_FAIL(generate_text_ir_pushdown_expr_ctdef(tr_info, op, scan_ctdef))) {
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
                                            const DASScanCGCtx &cg_ctx,
                                            ObDASScanCtDef &scan_ctdef,
                                            bool &has_rowscn)
{
  int ret = OB_SUCCESS;
  ObArray<ObExpr *> domain_id_expr;
  ObArray<uint64_t> domain_id_col_ids;
  // 1. add basic column
  if (OB_FAIL(generate_access_ctdef(op, cg_ctx, scan_ctdef, domain_id_expr, domain_id_col_ids, has_rowscn))) {
    LOG_WARN("generate access ctdef failed", K(ret), K(scan_ctdef.ref_table_id_));
  }
  //2. generate pushdown aggr column
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_pushdown_aggr_ctdef(op, cg_ctx, scan_ctdef))) {
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

  if (OB_SUCC(ret)) {
    ObRawExpr *auto_split_expr = const_cast<ObRawExpr*>(op.get_auto_split_filter());
    const uint64_t auto_split_filter_type = op.get_auto_split_filter_type();
    if (OB_NOT_NULL(auto_split_expr) && OB_INVALID_ID != auto_split_filter_type) {
      if (OB_FAIL(cg_.generate_rt_expr(*auto_split_expr,
                                       scan_ctdef.pd_expr_spec_.auto_split_expr_))) {
        LOG_WARN("generate auto split filter expr failed", K(ret));
      } else if (OB_FAIL(cg_.generate_rt_exprs(op.get_auto_split_params(),
                                               scan_ctdef.pd_expr_spec_.auto_split_params_))) {
        LOG_WARN("generate auto split params failed", K(ret));
      } else {
        scan_ctdef.pd_expr_spec_.auto_split_filter_type_ = auto_split_filter_type;
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
    if (OB_FAIL(generate_table_param(op, cg_ctx, scan_ctdef, tsc_out_cols))) {
      LOG_WARN("generate table param failed", K(ret));
    }
  }
  //7. generate das result output
  if (OB_SUCC(ret)) {
    const bool pd_agg = scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown();
    if (OB_FAIL(generate_das_result_output(tsc_out_cols, domain_id_expr, domain_id_col_ids, scan_ctdef, op.get_trans_info_expr(), pd_agg))) {
      LOG_WARN("failed to init result outputs", K(ret));
    }
  }
  //8. generate rowkey exprs and index pushdown filters when use index merge
  if (OB_SUCC(ret) && scan_ctdef.is_index_merge_) {
    ObArray<ObRawExpr*> rowkey_exprs;
    ObArray<ObRawExpr*> scan_pushdown_filters;
    if (OB_FAIL(rowkey_exprs.assign(op.get_rowkey_exprs()))) {
      LOG_WARN("failed to assign rowkey exprs", K(ret));
    } else if (!op.get_is_index_global() && OB_FAIL(mapping_oracle_real_agent_virtual_exprs(op, rowkey_exprs))) {
      LOG_WARN("failed to mapping oracle real virtual exprs", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_exprs(rowkey_exprs, scan_ctdef.rowkey_exprs_))) {
      LOG_WARN("failed to generate main table rowkey exprs", K(ret));
    } else if (OB_FAIL(op.get_index_filters(scan_ctdef.index_merge_idx_, scan_pushdown_filters))) {
      LOG_WARN("failed to get index filters", K(ret));
    } else if (!scan_pushdown_filters.empty()) {
      if (OB_FAIL(generate_pd_storage_flag(op.get_plan(),
                                           op.get_ref_table_id(),
                                           op.get_access_exprs(),
                                           op.get_type(),
                                           op.get_index_back() && op.get_is_index_global(),
                                           op.use_column_store(),
                                           scan_ctdef.pd_expr_spec_))) {
        LOG_WARN("failed to generate pd storage flag for index scan ctdef", K(scan_ctdef.ref_table_id_), K(ret));
      } else if (OB_FAIL(cg_.generate_rt_exprs(scan_pushdown_filters, scan_ctdef.pd_expr_spec_.pushdown_filters_))) {
        LOG_WARN("failed to generate index scan pushdown filter", K(scan_ctdef.ref_table_id_), K(ret));
      } else if (scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_filter_pushdown()) {
        ObPushdownFilterConstructor filter_constructor(
            &cg_.phy_plan_->get_allocator(), cg_, &op,
            scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_use_column_store());
        if (OB_FAIL(filter_constructor.apply(
            scan_pushdown_filters, scan_ctdef.pd_expr_spec_.pd_storage_filters_.get_pushdown_filter()))) {
          LOG_WARN("failed to apply filter constructor", K(ret));
        }
        LOG_TRACE("index merge pushdown filters", K(scan_ctdef.ref_table_id_), K(scan_pushdown_filters));
      }
    }
  }

  return ret;
}

int ObTscCgService::extract_das_output_column_ids(const ObLogTableScan &op,
                                                  ObDASScanCtDef &scan_ctdef,
                                                  const ObTableSchema &index_schema,
                                                  const DASScanCGCtx &cg_ctx,
                                                  ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> das_output_cols;
  const ObTableID &table_id = scan_ctdef.ref_table_id_;
  const bool use_index_merge = scan_ctdef.is_index_merge_;

  if (op.need_doc_id_index_back() && table_id == op.get_doc_id_index_table_id() &&
      // use the 'OB_IR_DOC_ID_IDX_AGG' to compute the total doc number, and the 'OB_IR_DOC_ID_IDX_AGG' satisfies the above requirements also.
      // but we do not need the rowkey column, so we use the 'extract_rowkey_doc_output_columns_ids' method to get the output_cids.
      scan_ctdef.ir_scan_type_ != ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG) {
    if (OB_FAIL(extract_doc_id_index_back_output_column_ids(op, output_cids))) {
      LOG_WARN("failed to extract doc id index back output column ids", K(ret));
    }
  } else if ((op.is_text_retrieval_scan() && table_id != op.get_ref_table_id() &&
      !op.is_scan_domain_id_table(table_id) &&
      table_id != op.get_rowkey_doc_table_id())
      || (cg_ctx.is_func_lookup_ && table_id != op.get_rowkey_doc_table_id())) {
    const ObTextRetrievalInfo &tr_info = cg_ctx.is_func_lookup_
        ? op.get_lookup_tr_infos().at(cg_ctx.curr_func_lookup_idx_)
        : op.get_text_retrieval_info();
    if (OB_FAIL(extract_text_ir_das_output_column_ids(tr_info, scan_ctdef, output_cids))) {
      LOG_WARN("failed to extract text retrieval das output column ids", K(ret));
    }
  } else if (cg_ctx.is_func_lookup_ && table_id == op.get_rowkey_doc_table_id()) {
    const bool output_rowkey = !cg_ctx.is_func_lookup_;
    if (OB_FAIL(extract_rowkey_domain_id_output_columns_ids(index_schema, op, scan_ctdef, output_rowkey, output_cids))) {
      LOG_WARN("fail to extract rowkey doc output columns ids", K(ret));
    }
  } else if ((op.is_post_vec_idx_scan()
          || (op.is_pre_vec_idx_scan() && (op.is_vec_index_table_id(table_id) || scan_ctdef.ir_scan_type_ == OB_VEC_COM_AUX_SCAN)))
          && (table_id != op.get_ref_table_id() || scan_ctdef.ir_scan_type_ == OB_VEC_COM_AUX_SCAN)
          && (!op.is_scan_domain_id_table(table_id) || scan_ctdef.ir_scan_type_ == OB_VEC_ROWKEY_VID_SCAN)) {
    // non main table scan in text retrieval
    if (OB_FAIL(extract_vector_das_output_column_ids(index_schema, op, scan_ctdef, output_cids))) {
      LOG_WARN("failed to extract vector das output column ids", K(ret));
    }
  } else if (op.is_scan_domain_id_table(table_id)) {
    if (OB_FAIL(extract_rowkey_domain_id_output_columns_ids(index_schema, op, scan_ctdef, true, output_cids))) {
      LOG_WARN("fail to extract rowkey doc output columns ids", K(ret));
    }
  } else if (cg_ctx.is_merge_fts_index_) {
    const ObTextRetrievalInfo &tr_info = op.get_merge_tr_infos().at(cg_ctx.curr_merge_fts_idx_);
    if (OB_FAIL(extract_text_ir_das_output_column_ids(tr_info, scan_ctdef, output_cids))) {
      LOG_WARN("failed to extract text retrieval das output column ids", K(ret));
    }
  } else if ((op.get_index_back() || op.is_multivalue_index_scan()) && (op.get_real_ref_table_id() != table_id || use_index_merge)) {
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
      if (OB_FAIL(index_schema.get_fulltext_column_ids(doc_id_col_id, ft_col_id))) {
        LOG_WARN("fail to get fulltext column ids", K(ret));
      } else if (OB_INVALID_ID == doc_id_col_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get doc id column.", K(ret));
      } else if (OB_FAIL(output_cids.push_back(doc_id_col_id))) {
        LOG_WARN("store colum id failed", K(ret));
      } else if (!op.get_index_back()){
        if (OB_FAIL(extract_tsc_access_columns(op, das_output_cols))) {
          LOG_WARN("extract tsc access columns failed", K(ret));
        } else if (OB_FAIL(extract_das_column_ids(das_output_cols, output_cids))) {
          LOG_WARN("extract column ids failed", K(ret));
        }
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
  } else if (op.has_func_lookup() && op.get_real_index_table_id() == table_id) {
    // main scan in functional lookup, need to output extra rowkey exprs for further lookup on functional index
    ObArray<uint64_t> rowkey_column_ids;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(cg_.opt_ctx_->get_schema_guard()->get_table_schema(MTL_ID(), op.get_real_ref_table_id(), table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to table schema", K(ret));
    } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_column_ids))) {
      LOG_WARN("get rowkey column ids failed", K(ret));
    } else if (OB_FAIL(append_array_no_dup(output_cids, rowkey_column_ids))) {
      LOG_WARN("fail to append rowkey cids to output cids for functional lookup", K(ret));
    }
  } else if (op.is_tsc_with_domain_id() && index_schema.is_user_table()) {
    const common::ObIArray<int64_t>& domain_types = op.get_rowkey_domain_types();
    const common::ObIArray<uint64_t>& domain_tids = op.get_rowkey_domain_tids();
    if (domain_types.count() != domain_tids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect domain table count", K(ret), K(domain_types.count()), K(domain_tids.count()));
    } else if (OB_FAIL(scan_ctdef.domain_id_idxs_.init(domain_types.count()))) {
      LOG_WARN("fail to init domain id idx array ", K(ret));
    } else if (OB_FAIL(scan_ctdef.domain_types_.init(domain_types.count()))) {
      LOG_WARN("fail to init domain type array ", K(ret));
    } else if (OB_FAIL(scan_ctdef.domain_tids_.init(domain_types.count()))) {
      LOG_WARN("fail to init domain tid array ", K(ret));
    } else {
      ObSEArray<uint64_t, 5> domain_id_col_id;
      DomainIdxs domain_id_idx;
      for (int64_t i = 0; OB_SUCC(ret) && i < domain_types.count(); i++) {
        domain_id_col_id.reuse();
        domain_id_idx.reuse();
        ObDomainIdUtils::ObDomainIDType cur_type = static_cast<ObDomainIdUtils::ObDomainIDType>(domain_types.at(i));
        if (OB_FAIL(ObDomainIdUtils::get_domain_id_col_by_tid(
            cur_type, &index_schema, cg_.opt_ctx_->get_sql_schema_guard(), domain_tids.at(i), domain_id_col_id))) {
          LOG_WARN("fail to get domain id cid", K(ret), K(index_schema), K(domain_id_col_id));
        } else if (is_contain(domain_id_col_id, OB_INVALID_ID)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get domain id column", K(ret));
        } else {
          int64_t idx = OB_INVALID_ID;
          for (int64_t j = 0; OB_SUCC(ret) && j < domain_id_col_id.count(); ++j) {
            if (has_exist_in_array(output_cids, domain_id_col_id.at(j), &idx)) {
              if (OB_FAIL(domain_id_idx.push_back(idx))) {
                LOG_WARN("failed to push back domain id idx", K(ret));
              }
            } else {
              // push -1 into idx, means do not need to fill this domain id
              if (OB_FAIL(domain_id_idx.push_back(idx))) {
                LOG_WARN("failed to push back domain id idx", K(ret));
              }
            }
          }
          if (OB_SUCC(ret) && !domain_id_idx.empty()) {
            if (OB_FAIL(OB_FAIL(scan_ctdef.domain_id_idxs_.push_back(domain_id_idx)))) {
              LOG_WARN("fail to push domain id col idx", K(ret), K(domain_id_idx));
            } else if (OB_FAIL(scan_ctdef.domain_types_.push_back(cur_type))) {
              LOG_WARN("fail to push domain type", K(ret));
            } else if (OB_FAIL(scan_ctdef.domain_tids_.push_back(domain_tids.at(i)))) {
              LOG_WARN("fail to push domain tid", K(ret), K(domain_tids.at(i)));
            }
          }
        }
      }
    }
  }

  LOG_TRACE("extract das output column ids", K(ret), K(table_id), K(op.get_ref_table_id()),
    K(op.get_index_back()), K(scan_ctdef.is_index_merge_), K(output_cids));
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
    } else if (T_PSEUDO_OLD_NEW_COL == column_exprs.at(i)->get_expr_type()) {
      if (OB_FAIL(column_ids.push_back(OB_MAJOR_REFRESH_MVIEW_OLD_NEW_COLUMN_ID))) {
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
  const bool is_das_empty_part = loc_meta.das_empty_part_;
  loc_meta.reset();
  loc_meta.das_empty_part_ = is_das_empty_part;
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
  int64_t route_policy = 0;
  bool is_weak_read = false;
  // broadcast table (insert into select) read local for materialized view create,here three conditions:
  // 1. inner sql tag weak read
  // 2. is complete refresh
  // 3. is broadcast table
  const bool is_new_mv_create = ObConsistencyLevel::WEAK == stmt.get_query_ctx()->get_global_hint().read_consistency_
                                && table_schema.is_broadcast_table() && session.get_ddl_info().is_mview_complete_refresh();
  if (OB_ISNULL(cg_.opt_ctx_) || OB_ISNULL(cg_.opt_ctx_->get_exec_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(cg_.opt_ctx_), K(ret));
  } else if (OB_FAIL(session.get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
    LOG_WARN("get route policy failed", K(ret));
  } else if (stmt.get_query_ctx()->has_dml_write_stmt_
             && !is_new_mv_create) {
    loc_meta.select_leader_ = 1;
    loc_meta.is_weak_read_ = 0;
  } else if (OB_FAIL(session.get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
    LOG_WARN("get route policy failed", K(ret));
  } else if (OB_FAIL(ObTableLocation::get_is_weak_read(stmt, &session,
                                                       cg_.opt_ctx_->get_exec_ctx()->get_sql_ctx(),
                                                       is_weak_read))) {
    LOG_WARN("get is weak read failed", K(ret));
  } else if (is_weak_read) {
    loc_meta.is_weak_read_ = 1;
    loc_meta.select_leader_ = 0;
  } else if (loc_meta.is_dup_table_
             || is_new_mv_create) {
    loc_meta.select_leader_ = 0;
    loc_meta.is_weak_read_ = 0;
  } else {
    //strong consistency read policy is used by default
    loc_meta.select_leader_ = 1;
    loc_meta.is_weak_read_ = 0;
  }

  loc_meta.route_policy_ = route_policy;
  // For rowkey doc auxiliary tables, table scan must be together with the data table,
  // and it does not have a relative table. Also, there is no relative table for global index.
  if (OB_SUCC(ret) && !table_schema.is_global_index_table() && !table_schema.is_rowkey_doc_id() && !table_schema.is_vec_rowkey_vid_type()) {
    TableLocRelInfo *rel_info = nullptr;
    ObTableID data_table_id = table_schema.is_index_table() && !table_schema.is_vec_rowkey_vid_type() ?
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

int ObTscCgService::generate_multivalue_ir_ctdef(const ObLogTableScan &op,
                                                 ObTableScanCtDef &tsc_ctdef,
                                                 ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;

  int64_t rowkey_cnt = 0;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(cg_.opt_ctx_->get_schema_guard()->get_table_schema(MTL_ID(), op.get_real_ref_table_id(), table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to table schema", K(ret));
  } else {
    rowkey_cnt = table_schema->get_rowkey_column_num();
  }

  if (OB_SUCC(ret)) {
    ObDASScanCtDef *scan_ctdef = &tsc_ctdef.scan_ctdef_;
    ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
    ObDASSortCtDef *sort_ctdef = nullptr;
    ObExpr *doc_id_col_expr = nullptr;

    if (scan_ctdef->result_output_.count() < rowkey_cnt + 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to generate multivalue lookup ctdef, scan_ctdef.result_output_.count() is unexpected", K(ret));
    } else if (FALSE_IT(doc_id_col_expr = scan_ctdef->result_output_.at(rowkey_cnt))) {
    } else if (OB_FAIL(generate_doc_id_lookup_ctdef(op, tsc_ctdef, root_ctdef, doc_id_col_expr, aux_lookup_ctdef))) {
      LOG_WARN("failed to generate doc id lookup ctdef", K(ret));
    } else if (OB_FAIL(scan_ctdef->rowkey_exprs_.init(rowkey_cnt))) {
      LOG_WARN("failed to init rowkey exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
        ObExpr *expr = scan_ctdef->result_output_.at(i);
        if (OB_FAIL(scan_ctdef->rowkey_exprs_.push_back(expr))) {
          LOG_WARN("append rowkey exprs failed", K(ret));
        }
      }

      if (OB_SUCC(ret) && OB_FAIL(generate_das_sort_ctdef(scan_ctdef->rowkey_exprs_, aux_lookup_ctdef, sort_ctdef))) {
        LOG_WARN("generate sort ctdef failed", K(ret));
      } else {
        root_ctdef = sort_ctdef;
      }
    }
  }

  return ret;
}

int ObTscCgService::generate_gis_ir_ctdef(const ObLogTableScan &op,
                                          ObTableScanCtDef &tsc_ctdef,
                                          ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;

  ObDASScanCtDef *scan_ctdef = &tsc_ctdef.scan_ctdef_;
  ObSEArray<ObExpr *, 2> rowkey_exprs;
  if (scan_ctdef->result_output_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to generate gis ir ctdef, scan_ctdef.result_output_.count() is 0", K(ret));
  } else {
    int64_t rowkey_cnt = scan_ctdef->result_output_.count() - 1;
    if (scan_ctdef->trans_info_expr_ != nullptr) {
      rowkey_cnt = rowkey_cnt - 1;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
      ObExpr *expr = scan_ctdef->result_output_.at(i);
      if (OB_FAIL(rowkey_exprs.push_back(expr))) {
        LOG_WARN("append rowkey exprs failed", K(ret));
      }
    }
  }

  ObDASSortCtDef *sort_ctdef = nullptr;
  if (OB_SUCC(ret) && OB_FAIL(generate_das_sort_ctdef(rowkey_exprs, scan_ctdef, sort_ctdef))) {
    LOG_WARN("generate sort ctdef failed", K(ret));
  } else {
    root_ctdef = sort_ctdef;
  }
  return ret;
}

int ObTscCgService::generate_vec_aux_table_ctdef(const ObLogTableScan &op,
                                                  ObTSCIRScanType ir_scan_type,
                                                  uint64_t table_id,
                                                  ObDASScanCtDef *&aux_ctdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();
  bool has_rowscn = false;
  DASScanCGCtx cg_ctx;
  if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, ctdef_alloc, aux_ctdef))) {
    LOG_WARN("allocate delta buf table ctdef failed", K(ret));
  } else if (OB_ISNULL(aux_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to generate das scan ctdef, aux_ctdef is null", K(ret));
  } else {
    aux_ctdef->ref_table_id_ = table_id;
    aux_ctdef->ir_scan_type_ = ir_scan_type;
    if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, *aux_ctdef, has_rowscn))) {
      LOG_WARN("failed to generate das scan ctdef", K(ret));
    }
  }
  return ret;
}

int ObTscCgService::generate_vec_aux_idx_tbl_ctdef(const ObLogTableScan &op,
                                                  ObDASScanCtDef *&first_aux_ctdef,
                                                  ObDASScanCtDef *&second_aux_ctdef,
                                                  ObDASScanCtDef *&third_aux_ctdef,
                                                  ObDASScanCtDef *&forth_aux_ctdef)
{
  int ret = OB_SUCCESS;
  const ObVecIndexInfo &vc_info = op.get_vector_index_info();

  ObTSCIRScanType first_ir_scan_type = vc_info.is_hnsw_vec_scan() ? OB_VEC_DELTA_BUF_SCAN : OB_VEC_IVF_CENTROID_SCAN;
  ObTSCIRScanType second_ir_scan_type = vc_info.is_hnsw_vec_scan() ? OB_VEC_IDX_ID_SCAN : OB_VEC_IVF_CID_VEC_SCAN;
  ObTSCIRScanType third_ir_scan_type = vc_info.is_hnsw_vec_scan() ? OB_VEC_SNAPSHOT_SCAN : OB_VEC_IVF_ROWKEY_CID_SCAN;
  ObTSCIRScanType forth_ir_scan_type = vc_info.is_hnsw_vec_scan() ? OB_VEC_ROWKEY_VID_SCAN : OB_VEC_IVF_SPECIAL_AUX_SCAN;
  if (OB_FAIL(generate_vec_aux_table_ctdef(op, first_ir_scan_type, vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX), first_aux_ctdef))) {
    LOG_WARN("failed to generate vec aux idx tbl ctdef", K(ret), K(first_ir_scan_type));
  } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, second_ir_scan_type, vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX), second_aux_ctdef))) {
    LOG_WARN("failed to generate vec aux idx tbl ctdef", K(ret), K(second_ir_scan_type));
  } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, third_ir_scan_type, vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX), third_aux_ctdef))) {
    LOG_WARN("failed to generate vec aux idx tbl ctdef", K(ret), K(third_ir_scan_type));
  } else if (!vc_info.is_ivf_flat_scan() && OB_FAIL(generate_vec_aux_table_ctdef(op, forth_ir_scan_type, vc_info.get_aux_table_id(ObVectorAuxTableIdx::VEC_FOURTH_AUX_TBL_IDX), forth_aux_ctdef))) {
    LOG_WARN("failed to generate vec aux idx tbl ctdef", K(ret), K(forth_ir_scan_type));
  }
  if (OB_FAIL(ret)) {
  } else {
    ExprFixedArray *target_rowkey_exprs = nullptr;
    if (vc_info.is_hnsw_vec_scan()) {
      target_rowkey_exprs = &forth_aux_ctdef->rowkey_exprs_;
    } else if (vc_info.is_ivf_vec_scan()) {
      target_rowkey_exprs = &third_aux_ctdef->rowkey_exprs_;
    }

    if (OB_ISNULL(target_rowkey_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target_rowkey_exprs is null", K(ret));
    } else {
      ObArray<ObRawExpr *> rowkey_exprs;
      if (OB_FAIL(rowkey_exprs.assign(op.get_rowkey_exprs()))) {
        LOG_WARN("failed to assign rowkey exprs", K(ret));
      } else if (OB_FAIL(cg_.generate_rt_exprs(rowkey_exprs, *target_rowkey_exprs))) {
        LOG_WARN("failed to generate main table rowkey exprs", K(ret));
      }
    }
  }

  return ret;
}

int ObTscCgService::generate_vec_idx_ctdef(const ObLogTableScan &op,
                                           ObTableScanCtDef &tsc_ctdef,
                                           ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  ObDASVecAuxScanCtDef *vec_scan_ctdef = nullptr;
  const ObTableSchema *main_index_table_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  ObDASSortCtDef *sort_ctdef = nullptr;
  int64_t dim = 0;
  bool is_aux_table_all_inited = false;
  const ObVecIndexInfo &vc_info = op.get_vector_index_info();
  ObVectorAuxTableIdx main_index_tid = ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else if (OB_FAIL(op.get_vector_index_info().check_vec_aux_table_is_all_inited(is_aux_table_all_inited))) {
  } else if (!is_aux_table_all_inited) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index aux table is not all inited", K(ret));
  } else if (op.is_pre_vec_idx_scan() && OB_FALSE_IT(tsc_ctdef.scan_ctdef_.ir_scan_type_ = ObTSCIRScanType::OB_VEC_FILTER_SCAN)) {
  } else if (OB_UNLIKELY(ObTSCIRScanType::OB_IR_INV_IDX_SCAN != tsc_ctdef.scan_ctdef_.ir_scan_type_
            && ObTSCIRScanType::OB_VEC_FILTER_SCAN != tsc_ctdef.scan_ctdef_.ir_scan_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ir scan type for inverted index scan", K(ret), K(tsc_ctdef.scan_ctdef_));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_VEC_SCAN, ctdef_alloc, vec_scan_ctdef))) {
    LOG_WARN("allocate ir scan ctdef failed", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(vc_info.main_table_tid_, data_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(vc_info.main_table_tid_));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_vector_index_info().get_aux_table_id(main_index_tid), main_index_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(op.get_vector_index_info().get_aux_table_id(main_index_tid)));
  } else if (OB_ISNULL(main_index_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(*main_index_table_schema, *data_table_schema, dim))) {
    LOG_WARN("fail to get vector_index_column dim", K(ret));
  } else {
    ObDASBaseCtDef *inv_idx_scan_ctdef = root_ctdef; // for pre filter, its local index ctdef, for post filter, its delta_buf_scan_ctdef (vec index table ctdef)
    ObDASScanCtDef *first_aux_ctdef = nullptr;  // HNSW_DELTA_BUF_TABLE     | IVF_CENTROID_TABLE
    ObDASScanCtDef *second_aux_ctdef = nullptr; // HNSW_INDEX_ID_TABLE      | IVF_CID_VEC_TABLE or IVF_PQ_CODE_TABLE
    ObDASScanCtDef *third_aux_ctdef = nullptr;  // HNSW_SNAPSHOT_DATA_TABLE | IVF_ROWKEY_CID_TABLE or IVF_PQ_ROWKEY_CID_TABLE
    ObDASScanCtDef *fourth_aux_ctdef = nullptr; // HNSW_ROWKEY_VID_TABLE    | null or IVF_SQ_META_TABLE or IVF_PQ_ID_TABLE
    ObDASScanCtDef *com_aux_ctdef = nullptr;    // main table

    if (OB_FAIL(generate_vec_aux_idx_tbl_ctdef(op, first_aux_ctdef, second_aux_ctdef, third_aux_ctdef, fourth_aux_ctdef))) {
      LOG_WARN("fail to generate_vec_aux_idx_tbl_ctdef", K(ret));
    } else if (OB_FAIL(vc_info.is_hnsw_vec_scan() && generate_vec_aux_table_ctdef(op, ObTSCIRScanType::OB_VEC_COM_AUX_SCAN, vc_info.main_table_tid_, com_aux_ctdef))) {
      LOG_WARN("fail to generate_vec_aux_table_ctdef", K(ret));
    } else {
      int64_t vec_child_task_cnt = 6;
      if (!vc_info.is_hnsw_vec_scan()) {
        if (vc_info.is_ivf_flat_scan()) {
          vec_child_task_cnt = 4;
        } else {
          vec_child_task_cnt = 5;
        }
      }
      if (OB_ISNULL(vec_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &ctdef_alloc, vec_child_task_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate ir scan ctdef children failed", K(ret));
      } else if (OB_FAIL(ob_write_string(ctdef_alloc, main_index_table_schema->get_index_params(), vec_scan_ctdef->vec_index_param_))) {
        LOG_WARN("fail to get index param", K(ret));
      } else {
        vec_scan_ctdef->children_cnt_ = vec_child_task_cnt; // number of ObDASScanCtDef
        vec_scan_ctdef->children_[0] = inv_idx_scan_ctdef;
        vec_scan_ctdef->children_[1] = first_aux_ctdef;
        vec_scan_ctdef->children_[2] = second_aux_ctdef;
        vec_scan_ctdef->children_[3] = third_aux_ctdef;
        if (!vc_info.is_ivf_flat_scan()) {
          vec_scan_ctdef->children_[4] = fourth_aux_ctdef;
        }
        if (vc_info.is_hnsw_vec_scan()) {
          vec_scan_ctdef->children_[5] = com_aux_ctdef;
        }
        vec_scan_ctdef->dim_ = dim;
        vec_scan_ctdef->vec_type_ = op.get_vector_index_info().vec_type_;
        vec_scan_ctdef->selectivity_ = op.get_vector_index_info().selectivity_;
        vec_scan_ctdef->row_count_ = op.get_vector_index_info().row_count_;
        vec_scan_ctdef->algorithm_type_ = op.get_vector_index_info().algorithm_type_;
      }
    }

    if (OB_SUCC(ret)) {
      ObRawExpr *expr = op.get_vector_index_info().sort_key_.expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not be nullptr", K(ret));
      } else if (expr->is_vector_sort_expr()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
          if (expr->get_param_expr(i)->is_column_ref_expr() && OB_FAIL(cg_.mark_expr_self_produced(expr->get_param_expr(i)))) {
            LOG_WARN("mark expr self produced failed", K(ret), KPC(expr->get_param_expr(i)));
          }
        }
      }
    }

  }

  if (OB_SUCC(ret)) {
    root_ctdef = vec_scan_ctdef;
    if (OB_FAIL(generate_vec_ir_spec_exprs(op, *vec_scan_ctdef))) {
      LOG_WARN("failed to generate vec ir spec exprs", K(ret));
    }
  }

  if (OB_SUCC(ret) && op.get_vector_index_info().need_sort()) {
    ObSEArray<OrderItem, 2> order_items;
    if (OB_FAIL(order_items.push_back(op.get_vector_index_info().sort_key_))) {
      LOG_WARN("append order item array failed", K(ret));
    } else if (OB_FAIL(generate_das_sort_ctdef(order_items, false, op.get_vector_index_info().topk_limit_expr_,
        op.get_vector_index_info().topk_offset_expr_, vec_scan_ctdef, sort_ctdef))) {
      LOG_WARN("generate sort ctdef failed", K(ret));
    } else {
      root_ctdef = sort_ctdef;
    }
  }

  if (OB_SUCC(ret) && op.get_index_back() && vc_info.is_hnsw_vec_scan()) {
    ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
    ObDASBaseCtDef *vir_output_ctdef =  nullptr == sort_ctdef ?
        static_cast<ObDASBaseCtDef *>(vec_scan_ctdef) : static_cast<ObDASBaseCtDef *>(sort_ctdef);
    if (OB_FAIL(generate_vec_id_lookup_ctdef(op, tsc_ctdef,
                                            vir_output_ctdef,
                                            aux_lookup_ctdef,
                                            vec_scan_ctdef->inv_scan_vec_id_col_))) {
      LOG_WARN("generate vid lookup ctdef failed", K(ret));
    } else {
      root_ctdef = aux_lookup_ctdef;
    }
  }
  return ret;
}

int ObTscCgService::generate_text_ir_ctdef(const ObLogTableScan &op,
                                           const DASScanCGCtx &cg_ctx,
                                           ObTableScanCtDef &tsc_ctdef,
                                           ObDASScanCtDef &scan_ctdef,
                                           ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  const ObTextRetrievalInfo &tr_info = cg_ctx.is_func_lookup_ ? op.get_lookup_tr_infos().at(cg_ctx.curr_func_lookup_idx_)
                                     : cg_ctx.is_merge_fts_index_ ? op.get_merge_tr_infos().at(cg_ctx.curr_merge_fts_idx_)
                                     : op.get_text_retrieval_info();
  ObMatchFunRawExpr *match_against = tr_info.match_expr_;
  ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  ObDASIRScanCtDef *ir_scan_ctdef = nullptr;
  ObDASSortCtDef *sort_ctdef = nullptr;
  ObDASScanCtDef *inv_idx_scan_ctdef = nullptr;
  ObExpr *index_back_doc_id_column = nullptr;
  bool has_rowscn = false;
  const bool use_approx_pre_agg = true; // TODO: support differentiate use approx agg or not
  if (OB_ISNULL(match_against) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP(match_against), KP(schema_guard));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tr_info.inv_idx_tid_
      || (tr_info.need_calc_relevance_ && OB_INVALID_ID == tr_info.fwd_idx_tid_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fulltext index table id", K(ret), KPC(match_against));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_IR_SCAN, ctdef_alloc, ir_scan_ctdef))) {
    LOG_WARN("allocate ir scan ctdef failed", K(ret));
  } else if (OB_UNLIKELY(!cg_ctx.is_func_lookup_ && ObTSCIRScanType::OB_IR_INV_IDX_SCAN != scan_ctdef.ir_scan_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ir scan type for inverted index scan", K(ret), K(scan_ctdef));
  } else {
    if (!cg_ctx.is_func_lookup_) {
      inv_idx_scan_ctdef = &scan_ctdef;
    } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, ctdef_alloc, inv_idx_scan_ctdef))) {
      LOG_WARN("allocate inv idx_scan_ctdef_failed", K(ret));
    } else {
      inv_idx_scan_ctdef->ref_table_id_ = tr_info.inv_idx_tid_;
      inv_idx_scan_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_INV_IDX_SCAN;
      if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, *inv_idx_scan_ctdef, has_rowscn))) {
        LOG_WARN("failed to generate das scan ctdef", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (tr_info.need_calc_relevance_) {
    ObDASScanCtDef *inv_idx_agg_ctdef = nullptr;
    ObDASScanCtDef *doc_id_idx_agg_ctdef = nullptr;
    ObDASScanCtDef *fwd_idx_agg_ctdef = nullptr;
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, ctdef_alloc, inv_idx_agg_ctdef))) {
      LOG_WARN("allocate inv idx agg ctdef failed", K(ret));
    } else {
      inv_idx_agg_ctdef->ref_table_id_ = tr_info.inv_idx_tid_;
      inv_idx_agg_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
      inv_idx_agg_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_INV_IDX_AGG;
      if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, *inv_idx_agg_ctdef, has_rowscn))) {
        LOG_WARN("failed to generate das scan ctdef", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, ctdef_alloc, doc_id_idx_agg_ctdef))) {
        LOG_WARN("allocate doc id idx agg ctdef failed", K(ret));
      } else {
        doc_id_idx_agg_ctdef->ref_table_id_ = tr_info.doc_id_idx_tid_;
        doc_id_idx_agg_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
        doc_id_idx_agg_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG;
        if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, *doc_id_idx_agg_ctdef, has_rowscn))) {
          LOG_WARN("failed to generate das scan ctdef", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, ctdef_alloc, fwd_idx_agg_ctdef))) {
        LOG_WARN("allocate fwd idx agg ctdef failed", K(ret));
      } else {
        fwd_idx_agg_ctdef->ref_table_id_ = tr_info.fwd_idx_tid_;
        fwd_idx_agg_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
        fwd_idx_agg_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_FWD_IDX_AGG;
        if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, *fwd_idx_agg_ctdef, has_rowscn))) {
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
      ir_scan_ctdef->children_[0] = &scan_ctdef;
    }
  }

  if (OB_SUCC(ret)) {
    root_ctdef = ir_scan_ctdef;
    if (OB_FAIL(generate_text_ir_spec_exprs(tr_info, *ir_scan_ctdef))) {
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

  if (OB_SUCC(ret) && tr_info.need_sort()) {
    ObSEArray<OrderItem, 2> order_items;
    if (OB_FAIL(order_items.push_back(tr_info.sort_key_))) {
      LOG_WARN("append order item array failed", K(ret));
    } else if (OB_FAIL(generate_das_sort_ctdef(
        order_items,
        tr_info.with_ties_,
        tr_info.topk_limit_expr_,
        tr_info.topk_offset_expr_,
        ir_scan_ctdef,
        sort_ctdef))) {
      LOG_WARN("generate sort ctdef failed", K(ret));
    } else {
      root_ctdef = sort_ctdef;
    }
  }

  if (OB_SUCC(ret) && op.get_index_back() && !cg_ctx.is_func_lookup_) {
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

int ObTscCgService::generate_index_merge_ctdef(const ObLogTableScan &op,
                                               ObTableScanCtDef &tsc_ctdef,
                                               ObDASIndexMergeCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  const IndexMergePath *path = nullptr;
  common::ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();
  if (OB_ISNULL(op.get_access_path())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    OB_ASSERT(op.use_index_merge());
    path = static_cast<const IndexMergePath*>(op.get_access_path());
    ObIndexMergeNode *root = path->root_;
    if (OB_FAIL(generate_index_merge_node_ctdef(op, tsc_ctdef, root, ctdef_alloc, root_ctdef))) {
      LOG_WARN("failed to generate index merge ctdef", K(root_ctdef));
    }
  }
  return ret;
}

int ObTscCgService::generate_index_merge_node_ctdef(const ObLogTableScan &op,
                                                    ObTableScanCtDef &tsc_ctdef,
                                                    ObIndexMergeNode *node,
                                                    common::ObIAllocator &alloc,
                                                    ObDASIndexMergeCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  DASScanCGCtx cg_ctx;
  bool has_rowscn = false;
  if (OB_ISNULL(node) || OB_UNLIKELY(!node->is_merge_node())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected index merge node", KPC(node), K(ret));
  } else {
    ObDASIndexMergeCtDef *merge_ctdef = nullptr;
    int64_t children_cnt = node->children_.count();
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_INDEX_MERGE, alloc, merge_ctdef))) {
      LOG_WARN("failed to allocate index merge ctdef", K(ret));
    } else if (OB_ISNULL(merge_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &alloc, children_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate child ctdef array", K(children_cnt), K(ret));
    } else if (OB_FAIL(merge_ctdef->merge_node_types_.prepare_allocate(children_cnt))) {
      LOG_WARN("failed to allocate merge node types", K(ret));
    } else if (FALSE_IT(merge_ctdef->merge_type_ = node->node_type_)) {
    } else {
      // TODO: merge all fts nodes with priority to reduce overhead
      ObArray<ObExpr*> merge_output;
      int64_t index_merge_fts_idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt; ++i) {
        ObIndexMergeNode *child = node->children_.at(i);
        ObDASBaseCtDef *child_ctdef = nullptr;
        if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null child", K(ret));
        } else if (child->is_merge_node()) {
          ObDASIndexMergeCtDef *child_merge_ctdef = nullptr;
          if (OB_FAIL(SMART_CALL(generate_index_merge_node_ctdef(op, tsc_ctdef, child, alloc, child_merge_ctdef)))) {
            LOG_WARN("failed to generate index merge node ctdef", K(ret));
          } else {
            child_ctdef = child_merge_ctdef;
          }
        } else {
          DASScanCGCtx cg_ctx;
          ObDASScanCtDef *scan_ctdef = nullptr;
          if (OB_ISNULL(child->ap_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null access path", KPC(child), K(ret));
          } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, alloc, scan_ctdef))) {
            LOG_WARN("failed to allocate scan ctdef", K(ret));
          } else if (OB_ISNULL(scan_ctdef)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null scan ctdef", K(ret));
          } else {
            scan_ctdef->ref_table_id_ = child->ap_->index_id_;
            scan_ctdef->index_merge_idx_ = child->scan_node_idx_;
            scan_ctdef->is_index_merge_ = true;
            if (child->node_type_ == INDEX_MERGE_FTS_INDEX) {
              // Currently, only a single level of union merge is supported, thus an incremental idx can be used directly.
              // FIXME: use a unique idx to identify the corresponding fts index precisely.
              cg_ctx.set_curr_merge_fts_idx(index_merge_fts_idx++);
              scan_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_INV_IDX_SCAN;
            }
            if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, *scan_ctdef, has_rowscn))) {
              LOG_WARN("failed to generate das scan ctdef", KPC(scan_ctdef), K(ret));
            } else if (OB_NOT_NULL(child->ap_->get_query_range_provider())) {
              if (NULL != child->ap_->pre_range_graph_) {
                scan_ctdef->is_new_query_range_ = true;
                if (OB_FAIL(scan_ctdef->pre_range_graph_.deep_copy(*child->ap_->pre_range_graph_))) {
                  LOG_WARN("failed to deep copy pre range graph", K(ret));
                } else if (!op.is_skip_scan()) {
                  scan_ctdef->pre_range_graph_.reset_skip_scan_range();
                }
              } else {
                scan_ctdef->is_new_query_range_ = false;
                if (OB_FAIL(scan_ctdef->pre_query_range_.deep_copy(*child->ap_->pre_query_range_))) {
                  LOG_WARN("failed to deep copy pre query range", K(ret));
                } else if (!op.is_skip_scan()) {
                  scan_ctdef->pre_query_range_.reset_skip_scan_range();
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(child->ap_->get_query_range_provider()->is_get(scan_ctdef->is_get_))) {
                LOG_WARN("failed to get whether get for query range", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              if (INDEX_MERGE_FTS_INDEX == child->node_type_) {
                ObDASBaseCtDef *ir_scan_ctdef = nullptr;
                if (OB_FAIL(generate_text_ir_ctdef(op, cg_ctx, tsc_ctdef, *scan_ctdef, ir_scan_ctdef))) {
                  LOG_WARN("failed to generate text ir ctdef", K(ret));
                } else {
                  child_ctdef = ir_scan_ctdef;
                }
              } else {
                child_ctdef = scan_ctdef;
              }
            }
          }
          if (OB_SUCC(ret) && OB_NOT_NULL(child_ctdef) && !child->ap_->is_ordered_by_pk_) {
            // for non-ROR situations, we need to insert a sort iter
            ObDASSortCtDef *sort_ctdef = nullptr;
            ObSEArray<OrderItem, 2> order_items;
            ObArray<ObRawExpr*> rowkey_exprs;
            if (OB_FAIL(rowkey_exprs.assign(op.get_rowkey_exprs()))) {
              LOG_WARN("failed to assign rowkey exprs", K(ret));
            } else if (!op.get_is_index_global() && OB_FAIL(mapping_oracle_real_agent_virtual_exprs(op, rowkey_exprs))) {
              LOG_WARN("failed to mapping oracle real virtual exprs", K(ret));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs.count(); i++) {
                if (OB_FAIL(order_items.push_back(OrderItem(rowkey_exprs.at(i), op.get_scan_direction())))) {
                  LOG_WARN("failed to push back order item", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_SORT, alloc, sort_ctdef))) {
                  LOG_WARN("failed to allocate sort ctdef", K(ret));
                } else if (OB_FAIL(generate_das_sort_ctdef(order_items,
                                                          false,
                                                          nullptr,
                                                          nullptr,
                                                          child_ctdef,
                                                          sort_ctdef))) {
                  LOG_WARN("failed to generate das sort ctdef", K(ret));
                } else {
                  child_ctdef = sort_ctdef;
                }
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(child_ctdef)) {
          merge_ctdef->children_[i] = child_ctdef;
          merge_ctdef->merge_node_types_.at(i) = child->node_type_;
          ExprFixedArray *result_output = nullptr;
          if (child_ctdef->op_type_ == DAS_OP_TABLE_SCAN) {
            ObDASScanCtDef *scan_ctdef = static_cast<ObDASScanCtDef*>(child_ctdef);
            result_output = &scan_ctdef->result_output_;
          } else if (ObDASTaskFactory::is_attached(child_ctdef->op_type_)) {
            ObDASAttachCtDef *attach_ctdef = static_cast<ObDASAttachCtDef*>(child_ctdef);
            result_output = &attach_ctdef->result_output_;
          }
          if (OB_ISNULL(result_output)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null result output", K(ret));
          } else if (OB_FAIL(append_array_no_dup(merge_output, *result_output))) {
            LOG_WARN("failed to append result output", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(merge_ctdef)) {
        ObArray<ObRawExpr*> rowkey_exprs;
        if (OB_FAIL(rowkey_exprs.assign(op.get_rowkey_exprs()))) {
          LOG_WARN("failed to assign rowkey exprs", K(ret));
        } else if (!op.get_is_index_global() && OB_FAIL(mapping_oracle_real_agent_virtual_exprs(op, rowkey_exprs))) {
          LOG_WARN("failed to mapping oracle real virtual exprs", K(ret));
        } else if (OB_FAIL(cg_.generate_rt_exprs(rowkey_exprs, merge_ctdef->rowkey_exprs_))) {
          LOG_WARN("failed to generate rt rowkey exprs", K(ret));
        } else if (OB_FAIL(merge_ctdef->result_output_.assign(merge_output))) {
          LOG_WARN("failed to assign result output", K(ret));
        } else {
          merge_ctdef->children_cnt_ = children_cnt;
          merge_ctdef->is_reverse_ = is_descending_direction(op.get_scan_direction());
          root_ctdef = merge_ctdef;
        }
      }
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

int ObTscCgService::extract_vec_ir_access_columns(
    const ObLogTableScan &op,
    const ObDASScanCtDef &scan_ctdef,
    ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  const ObVecIndexInfo &vec_info = op.get_vector_index_info();
  if (scan_ctdef.ref_table_id_ == op.get_doc_id_index_table_id()) {
    if (OB_FAIL(extract_vec_id_index_back_access_columns(op, access_exprs))) {
      LOG_WARN("failed to extract vid index back access columns", K(ret));
    }
  } else {
    switch (scan_ctdef.ir_scan_type_) {
      case ObTSCIRScanType::OB_IR_INV_IDX_SCAN: {
        if (vec_info.is_ivf_vec_scan()) {
          if (OB_FAIL(add_var_to_array_no_dup(
                  access_exprs, static_cast<ObRawExpr *>(vec_info.get_aux_table_column(IVF_CENTROID_CID_COL))))) {
            LOG_WARN("failed to add document length column to access exprs", K(ret));
          } else if (OB_FAIL(add_var_to_array_no_dup(
                         access_exprs,
                         static_cast<ObRawExpr *>(vec_info.get_aux_table_column(IVF_CENTROID_CENTER_COL))))) {
            LOG_WARN("failed to add document length column to access exprs", K(ret));
          }
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr *>(vec_info.vec_id_column_)))) {
          LOG_WARN("failed to push document id column to access exprs", K(ret));
        }
        break;
      }
      case ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN: {
        if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(HNSW_DELTA_VID_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(HNSW_DELTA_TYPE_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(HNSW_DELTA_VECTOR_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        }
        break;
      }
      case ObTSCIRScanType::OB_VEC_IDX_ID_SCAN: {
        if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(HNSW_INDEX_ID_SCN_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(HNSW_INDEX_ID_VID_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(HNSW_INDEX_ID_TYPE_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(HNSW_INDEX_ID_VECTOR_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        }
        break;
      }
      case ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN: {
        if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(HNSW_SNAPSHOT_KEY_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(HNSW_SNAPSHOT_DATA_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        }
        break;
      }
      case ObTSCIRScanType::OB_VEC_COM_AUX_SCAN: {
        if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.target_vec_column_)))) {
          LOG_WARN("failed to push document id column to access exprs", K(ret));
        }
        break;
      }
      case ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN: {
        if (OB_FAIL(extract_rowkey_domain_id_access_columns(op, scan_ctdef, access_exprs))) {
          LOG_WARN("fail to extract rowkey vid access columns", K(ret));
        }
        break;
      }
      case ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN:
      case ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN:
      case ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN:
      case ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN: {
        if (OB_FAIL(extract_ivf_access_columns(op, scan_ctdef, access_exprs))) {
          LOG_WARN("fail to extract rowkey vid access columns", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected text ir scan type", K(ret), K(scan_ctdef));
      }
    }
  }
  return ret;
}

int ObTscCgService::extract_ivf_rowkey_access_columns(const ObLogTableScan &op,
                                                      const ObDASScanCtDef &scan_ctdef,
                                                      ObIArray<ObRawExpr*> &access_exprs,
                                                      int rowkey_start_idx,
                                                      int table_order_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_array_no_dup(access_exprs, op.get_rowkey_exprs()))) {
    LOG_WARN("append the data table rowkey expr failed", K(ret), K(op.get_rowkey_exprs()));
  }
  return ret;
}

int ObTscCgService::extract_ivf_access_columns(const ObLogTableScan &op,
                                              const ObDASScanCtDef &scan_ctdef,
                                              ObIArray<ObRawExpr*> &access_exprs)
{
   int ret = OB_SUCCESS;
   const ObVecIndexInfo &vec_info = op.get_vector_index_info();
   switch (scan_ctdef.ir_scan_type_) {
    case ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN: {
      if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_CENTROID_CID_COL))))) {
        LOG_WARN("failed to add document length column to access exprs", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_CENTROID_CENTER_COL))))) {
        LOG_WARN("failed to add document length column to access exprs", K(ret));
      }
      break;
    }
    case ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN: {
      if (vec_info.is_ivf_flat_scan() || vec_info.is_ivf_sq_scan()) {
        if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_CID_VEC_CID_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_CID_VEC_VECTOR_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (vec_info.is_ivf_flat_scan()) {
          if (OB_FAIL(extract_ivf_rowkey_access_columns(op, scan_ctdef, access_exprs, IVF_FLAT_ROWKEY_START, 0))) {
            LOG_WARN("failed to extract ivf rowkey access columns", K(ret));
          }
        } else {
          if (OB_FAIL(extract_ivf_rowkey_access_columns(op, scan_ctdef, access_exprs, IVF_SQ_ROWKEY_START, 0))) {
            LOG_WARN("failed to extract ivf rowkey access columns", K(ret));
          }
        }
      } else if (vec_info.is_ivf_pq_scan()) {
        if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_PQ_CODE_CID_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_PQ_CODE_PIDS_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(extract_ivf_rowkey_access_columns(op, scan_ctdef, access_exprs, IVF_PQ_ROWKEY_START, 0))) {
          LOG_WARN("failed to extract ivf rowkey access columns", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ivf scan type", K(ret));
      }
      break;
    }
    case ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN: {
      if (vec_info.is_ivf_flat_scan() || vec_info.is_ivf_sq_scan()) {
        if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_ROWKEY_CID_CID))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (vec_info.is_ivf_flat_scan()) {
          if (OB_FAIL(extract_ivf_rowkey_access_columns(op, scan_ctdef, access_exprs, IVF_FLAT_ROWKEY_START, 1))) {
            LOG_WARN("failed to extract ivf rowkey access columns", K(ret));
          }
        } else {
          if (OB_FAIL(extract_ivf_rowkey_access_columns(op, scan_ctdef, access_exprs, IVF_SQ_ROWKEY_START, 1))) {
            LOG_WARN("failed to extract ivf rowkey access columns", K(ret));
          }
        }
      } else if (vec_info.is_ivf_pq_scan()) {
        if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_PQ_ROWKEY_CID_CID_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_PQ_ROWKEY_CID_PIDS_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(extract_ivf_rowkey_access_columns(op, scan_ctdef, access_exprs, IVF_PQ_ROWKEY_START, 1))) {
          LOG_WARN("failed to extract ivf rowkey access columns", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ivf scan type", K(ret));
      }
      break;
    }
    case ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN: {
      if (vec_info.is_ivf_sq_scan()) {
        if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_SQ_META_ID_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_SQ_META_VEC_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        }
      } else if (vec_info.is_ivf_pq_scan()) {
        if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_PQ_ID_PID_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(vec_info.get_aux_table_column(IVF_PQ_ID_CENTER_COL))))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ivf scan type", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected text ir scan type", K(ret), K(scan_ctdef));
    }
   }
   return ret;
}

int ObTscCgService::extract_text_ir_access_columns(
    const ObLogTableScan &op,
    const ObTextRetrievalInfo &tr_info,
    const ObDASScanCtDef &scan_ctdef,
    ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(scan_ctdef.ref_table_id_ == op.get_rowkey_domain_id_tid(ObDomainIdUtils::ObDomainIDType::DOC_ID))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected text ir access table", K(ret));
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
      // ordinary full-text lookup: use the doc id to get rowkey.
      if (scan_ctdef.ref_table_id_ == op.get_doc_id_index_table_id()) {
        if (OB_FAIL(extract_doc_id_index_back_access_columns(op, access_exprs))) {
          LOG_WARN("failed to extract doc id index back access columns", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected text ir scan type", K(ret), K(scan_ctdef));
      }
    }
  }
  return ret;
}

int ObTscCgService::extract_vector_hnsw_das_output_column_ids(const ObLogTableScan &op,
                                                              const ObDASScanCtDef &scan_ctdef,
                                                              ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  const ObVecIndexInfo &vec_info = op.get_vector_index_info();
  switch(scan_ctdef.ir_scan_type_) {
    case ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN: {
      if (OB_ISNULL(vec_info.get_aux_table_column(HNSW_DELTA_VID_COL))
        || OB_ISNULL(vec_info.get_aux_table_column(HNSW_DELTA_TYPE_COL))
        || OB_ISNULL(vec_info.get_aux_table_column(HNSW_DELTA_VECTOR_COL))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column", K(ret), KP(vec_info.get_aux_table_column(HNSW_DELTA_VID_COL)),
                                                      KP(vec_info.get_aux_table_column(HNSW_DELTA_TYPE_COL)),
                                                      KP(vec_info.get_aux_table_column(HNSW_DELTA_VECTOR_COL)));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(HNSW_DELTA_VID_COL))->get_column_id()))) {
        LOG_WARN("failed to push output vid col id", K(ret));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(HNSW_DELTA_VECTOR_COL))->get_column_id()))) {
        LOG_WARN("failed to push output vector col id", K(ret));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(HNSW_DELTA_TYPE_COL))->get_column_id()))) {
        LOG_WARN("failed to push output type col id", K(ret));
      }
      break;
    }

    case ObTSCIRScanType::OB_VEC_IDX_ID_SCAN: {
        if (OB_ISNULL(vec_info.get_aux_table_column(HNSW_INDEX_ID_SCN_COL))
        || OB_ISNULL(vec_info.get_aux_table_column(HNSW_INDEX_ID_VID_COL))
        || OB_ISNULL(vec_info.get_aux_table_column(HNSW_INDEX_ID_TYPE_COL))
        || OB_ISNULL(vec_info.get_aux_table_column(HNSW_INDEX_ID_VECTOR_COL))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column", K(ret), KP(vec_info.get_aux_table_column(HNSW_INDEX_ID_SCN_COL)),
                                                      KP(vec_info.get_aux_table_column(HNSW_INDEX_ID_VID_COL)),
                                                      KP(vec_info.get_aux_table_column(HNSW_INDEX_ID_TYPE_COL)),
                                                      KP(vec_info.get_aux_table_column(HNSW_INDEX_ID_VECTOR_COL)));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(HNSW_INDEX_ID_SCN_COL))->get_column_id()))) {
        LOG_WARN("failed to push output index_id_scn col id", K(ret));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(HNSW_INDEX_ID_VID_COL))->get_column_id()))) {
        LOG_WARN("failed to push output index_id_vid col id", K(ret));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(HNSW_INDEX_ID_TYPE_COL))->get_column_id()))) {
        LOG_WARN("failed to push output index_id_types col id", K(ret));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(HNSW_INDEX_ID_VECTOR_COL))->get_column_id()))) {
        LOG_WARN("failed to push output index_id_vector col id", K(ret));
      }
      break;
    }

    case ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN: {
      if (OB_ISNULL(vec_info.get_aux_table_column(HNSW_SNAPSHOT_KEY_COL))
        || OB_ISNULL(vec_info.get_aux_table_column(HNSW_SNAPSHOT_DATA_COL))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column", K(ret), KP(vec_info.get_aux_table_column(HNSW_SNAPSHOT_KEY_COL)),
                                                      KP(vec_info.get_aux_table_column(HNSW_SNAPSHOT_DATA_COL)));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(HNSW_SNAPSHOT_KEY_COL))->get_column_id()))) {
        LOG_WARN("failed to push output snapshot_key col id", K(ret));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(HNSW_SNAPSHOT_DATA_COL))->get_column_id()))) {
        LOG_WARN("failed to push output snapshot_data col id", K(ret));
      }
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vec scan type", K(ret), K(scan_ctdef));
    }
  }
  return ret;
}

int ObTscCgService::extract_vector_ivf_rowkey_output_column_ids(const ObLogTableScan &op,
                                                                const ObDASScanCtDef &scan_ctdef,
                                                                ObIArray<uint64_t> &output_cids,
                                                                int rowkey_start_idx,
                                                                int table_order_idx)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> rowkey_columns;
  if (OB_FAIL(extract_ivf_rowkey_access_columns(op, scan_ctdef, rowkey_columns, rowkey_start_idx, table_order_idx))) {
    LOG_WARN("failed to extract ivf rowkey access columns", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < rowkey_columns.count(); ++i) {
      if (OB_ISNULL(rowkey_columns.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(rowkey_columns.at(i))->get_column_id()))) {
        LOG_WARN("failed to push output column id", K(ret));
      }
    }
  }
  return ret;
}

int ObTscCgService::extract_vector_ivf_das_output_column_ids(const ObLogTableScan &op,
                                                              const ObDASScanCtDef &scan_ctdef,
                                                              ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  const ObVecIndexInfo &vec_info = op.get_vector_index_info();
  switch(scan_ctdef.ir_scan_type_) {
    case ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN: {
      if (OB_ISNULL(vec_info.get_aux_table_column(IVF_CENTROID_CID_COL))
        || OB_ISNULL(vec_info.get_aux_table_column(IVF_CENTROID_CENTER_COL))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column", K(ret), KP(vec_info.get_aux_table_column(IVF_CENTROID_CID_COL)),
                                                  KP(vec_info.get_aux_table_column(IVF_CENTROID_CENTER_COL)));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_CENTROID_CID_COL))->get_column_id()))) {
        LOG_WARN("failed to push output vid col id", K(ret));
      } else if (OB_FAIL(output_cids.push_back(
          static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_CENTROID_CENTER_COL))->get_column_id()))) {
        LOG_WARN("failed to push output vector col id", K(ret));
      }
      break;
    }
    case ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN: {
      if (vec_info.is_ivf_flat_scan() || vec_info.is_ivf_sq_scan()) {
        if (OB_ISNULL(vec_info.get_aux_table_column(IVF_CID_VEC_CID_COL))
          || OB_ISNULL(vec_info.get_aux_table_column(IVF_CID_VEC_VECTOR_COL))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column", K(ret), KP(vec_info.get_aux_table_column(IVF_CID_VEC_CID_COL)),
                                                    KP(vec_info.get_aux_table_column(IVF_CID_VEC_VECTOR_COL)));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_CID_VEC_CID_COL))->get_column_id()))) {
          LOG_WARN("failed to push output vid col id", K(ret));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_CID_VEC_VECTOR_COL))->get_column_id()))) {
          LOG_WARN("failed to push output vector col id", K(ret));
        } else if (vec_info.is_ivf_flat_scan()) {
          if (OB_FAIL(extract_vector_ivf_rowkey_output_column_ids(op, scan_ctdef, output_cids, IVF_FLAT_ROWKEY_START, 0))) {
            LOG_WARN("failed to extract vector ivf rowkey output column ids", K(ret));
          }
        } else {
          if (OB_FAIL(extract_vector_ivf_rowkey_output_column_ids(op, scan_ctdef, output_cids, IVF_SQ_ROWKEY_START, 0))) {
            LOG_WARN("failed to extract ivf rowkey access columns", K(ret));
          }
        }
      } else if (vec_info.is_ivf_pq_scan()) {
        if (OB_ISNULL(vec_info.get_aux_table_column(IVF_PQ_CODE_CID_COL))
          || OB_ISNULL(vec_info.get_aux_table_column(IVF_PQ_CODE_PIDS_COL))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column", K(ret), KP(vec_info.get_aux_table_column(IVF_PQ_CODE_CID_COL)),
                                                    KP(vec_info.get_aux_table_column(IVF_PQ_CODE_PIDS_COL)));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_PQ_CODE_CID_COL))->get_column_id()))) {
          LOG_WARN("failed to push output vid col id", K(ret));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_PQ_CODE_PIDS_COL))->get_column_id()))) {
          LOG_WARN("failed to push output vector col id", K(ret));
        } else if (OB_FAIL(extract_vector_ivf_rowkey_output_column_ids(op, scan_ctdef, output_cids, IVF_PQ_ROWKEY_START, 0/*table_order_idx*/))) {
          LOG_WARN("failed to extract vector ivf rowkey output column ids", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ivf scan type", K(ret));
      }
      break;
    }
    case ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN: {
      if (vec_info.is_ivf_flat_scan() || vec_info.is_ivf_sq_scan()) {
        if (OB_ISNULL(vec_info.get_aux_table_column(IVF_ROWKEY_CID_CID))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column", K(ret), KP(vec_info.get_aux_table_column(IVF_ROWKEY_CID_CID)));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_ROWKEY_CID_CID))->get_column_id()))) {
          LOG_WARN("failed to push output vid col id", K(ret));
        } else if (vec_info.is_ivf_flat_scan()) {
          if (OB_FAIL(extract_vector_ivf_rowkey_output_column_ids(op, scan_ctdef, output_cids, IVF_FLAT_ROWKEY_START, 1))) {
            LOG_WARN("failed to extract vector ivf rowkey output column ids", K(ret));
          }
        } else {
          if (OB_FAIL(extract_vector_ivf_rowkey_output_column_ids(op, scan_ctdef, output_cids, IVF_SQ_ROWKEY_START, 1))) {
            LOG_WARN("failed to extract ivf rowkey access columns", K(ret));
          }
        }
      } else if (vec_info.is_ivf_pq_scan()) {
        if (OB_ISNULL(vec_info.get_aux_table_column(IVF_PQ_ROWKEY_CID_CID_COL))
        || OB_ISNULL(vec_info.get_aux_table_column(IVF_PQ_ROWKEY_CID_PIDS_COL))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column", K(ret), KP(vec_info.get_aux_table_column(IVF_PQ_ROWKEY_CID_CID_COL)),
                                                    KP(vec_info.get_aux_table_column(IVF_PQ_ROWKEY_CID_PIDS_COL)));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_PQ_ROWKEY_CID_CID_COL))->get_column_id()))) {
          LOG_WARN("failed to push output vid col id", K(ret));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_PQ_ROWKEY_CID_PIDS_COL))->get_column_id()))) {
          LOG_WARN("failed to push output vector col id", K(ret));
        } else if (OB_FAIL(extract_vector_ivf_rowkey_output_column_ids(op, scan_ctdef, output_cids, IVF_PQ_ROWKEY_START, 1))) {
          LOG_WARN("failed to extract vector ivf rowkey output column ids", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ivf scan type", K(ret));
      }
      break;
    }
    case ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN: {
      if (vec_info.is_ivf_sq_scan()) {
        if (OB_ISNULL(vec_info.get_aux_table_column(IVF_SQ_META_ID_COL))
        || OB_ISNULL(vec_info.get_aux_table_column(IVF_SQ_META_VEC_COL))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column", K(ret), KP(vec_info.get_aux_table_column(IVF_SQ_META_ID_COL)),
                                                    KP(vec_info.get_aux_table_column(IVF_SQ_META_VEC_COL)));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_SQ_META_ID_COL))->get_column_id()))) {
          LOG_WARN("failed to push output vid col id", K(ret));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_SQ_META_VEC_COL))->get_column_id()))) {
          LOG_WARN("failed to push output vector col id", K(ret));
        }
      } else if (vec_info.is_ivf_pq_scan()) {
        if (OB_ISNULL(vec_info.get_aux_table_column(IVF_PQ_ID_PID_COL))
          || OB_ISNULL(vec_info.get_aux_table_column(IVF_PQ_ID_CENTER_COL))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column", K(ret), KP(vec_info.get_aux_table_column(IVF_PQ_ID_PID_COL)),
                                                    KP(vec_info.get_aux_table_column(IVF_PQ_ID_CENTER_COL)));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_PQ_ID_PID_COL))->get_column_id()))) {
          LOG_WARN("failed to push output vid col id", K(ret));
        } else if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.get_aux_table_column(IVF_PQ_ID_CENTER_COL))->get_column_id()))) {
          LOG_WARN("failed to push output vector col id", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ivf scan type", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vec scan type", K(ret), K(scan_ctdef));
    }
  }
  return ret;
}


int ObTscCgService::extract_vector_das_output_column_ids(const ObTableSchema &index_schema,
                                                         const ObLogTableScan &op,
                                                         const ObDASScanCtDef &scan_ctdef,
                                                         ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  const ObVecIndexInfo &vec_info = op.get_vector_index_info();
  bool is_column_all_inited = false;
  if (scan_ctdef.ref_table_id_ == op.get_doc_id_index_table_id()) {
    if (OB_FAIL(extract_doc_id_index_back_output_column_ids(op, output_cids))) {
      LOG_WARN("failed to get vid index back cids", K(ret), K(scan_ctdef.ref_table_id_));
    }
  } else if (vec_info.check_vec_aux_column_is_all_inited(is_column_all_inited)) {
    LOG_WARN("fail to check vec aux column is all inited", K(ret), K(scan_ctdef.ir_scan_type_), K(vec_info.aux_table_column_.count()));
  } else if (!is_column_all_inited) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vec aux column is all inited", K(ret));
  } else {
    switch(scan_ctdef.ir_scan_type_) {
      case ObTSCIRScanType::OB_NOT_A_SPEC_SCAN: {
        break;
      }
      case ObTSCIRScanType::OB_IR_INV_IDX_SCAN: {
        if (vec_info.is_hnsw_vec_scan()) {
          if (OB_ISNULL(vec_info.vec_id_column_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vec id column", K(ret), KP(vec_info.vec_id_column_));
          } else if (OB_FAIL(output_cids.push_back(
              static_cast<ObColumnRefRawExpr *>(vec_info.vec_id_column_)->get_column_id()))) {
            LOG_WARN("failed to push output vid col id", K(ret));
          }
        }
        break;
      }
      case ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN:
      case ObTSCIRScanType::OB_VEC_IDX_ID_SCAN:
      case ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN: {
        if (OB_FAIL(extract_vector_hnsw_das_output_column_ids(op, scan_ctdef, output_cids))) {
          LOG_WARN("failed to extract_vector_hnsw_das_output_column_ids", K(ret));
        }
        break;
      }
      case ObTSCIRScanType::OB_VEC_COM_AUX_SCAN: {
        if (OB_FAIL(output_cids.push_back(
            static_cast<ObColumnRefRawExpr *>(vec_info.target_vec_column_)->get_column_id()))) {
          LOG_WARN("failed to push output vid col id", K(ret));
        }
        break;
      }
      case ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN: {
        if (OB_FAIL(extract_rowkey_domain_id_output_columns_ids(index_schema, op, scan_ctdef, true, output_cids))) {
          LOG_WARN("fail to extract rowkey vid output column ids", K(ret));
        }
        // do nothing now
        break;
      }
      case ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN:
      case ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN:
      case ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN:
      case ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN: {
        if (OB_FAIL(extract_vector_ivf_das_output_column_ids(op, scan_ctdef, output_cids))) {
          LOG_WARN("failed to extract_vector_ivf_das_output_column_ids", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vec scan type", K(ret), K(scan_ctdef));
      }
    }
  }
  return ret;
}

int ObTscCgService::extract_text_ir_das_output_column_ids(
    const ObTextRetrievalInfo &tr_info,
    const ObDASScanCtDef &scan_ctdef,
    ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  if (ObTSCIRScanType::OB_IR_INV_IDX_SCAN == scan_ctdef.ir_scan_type_) {
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
    const ObTextRetrievalInfo &tr_info,
    const ObLogTableScan &op,
    ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  const uint64_t scan_table_id = scan_ctdef.ref_table_id_;
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

int ObTscCgService::generate_vec_ir_spec_exprs(const ObLogTableScan &op,
                                              ObDASVecAuxScanCtDef &vec_ir_scan_ctdef)
{
  int ret = OB_SUCCESS;
  const ObVecIndexInfo &vec_info = op.get_vector_index_info();
  ObSEArray<ObExpr *, 4> result_output;
  if (vec_info.is_hnsw_vec_scan()) {
    const ObDASScanCtDef *target_ctdef = op.is_pre_vec_idx_scan() ? vec_ir_scan_ctdef.get_vec_aux_tbl_ctdef(vec_ir_scan_ctdef.get_rowkey_vid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN)
                                          : static_cast<const ObDASScanCtDef *>(vec_ir_scan_ctdef.get_inv_idx_scan_ctdef());
    if (OB_ISNULL(vec_info.vec_id_column_) || OB_ISNULL(target_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), KP(vec_info.vec_id_column_), KP(target_ctdef));
    } else {
      const UIntFixedArray &scan_col_id = target_ctdef->access_column_ids_;
      const ObColumnRefRawExpr *vec_id_column = static_cast<ObColumnRefRawExpr *>(vec_info.vec_id_column_);

      int64_t vec_id_col_idx = -1;
      for (int64_t i = 0; i < scan_col_id.count() && vec_id_col_idx == -1; ++i) {
        uint64_t cur_col_id = scan_col_id.at(i);
        if (cur_col_id == vec_id_column->get_column_id()) {
          vec_id_col_idx = i;
        }
      }
      if (OB_UNLIKELY(-1 == vec_id_col_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vid col idx not found in inverted index scan access columns",
            K(ret), K(vec_id_column->get_column_id()), K(vec_ir_scan_ctdef), K(vec_id_col_idx));
      } else {
        vec_ir_scan_ctdef.inv_scan_vec_id_col_ =
            target_ctdef->pd_expr_spec_.access_exprs_.at(vec_id_col_idx);
        if (OB_FAIL(result_output.push_back(vec_ir_scan_ctdef.inv_scan_vec_id_col_))) {
          LOG_WARN("failed to append output exprs", K(ret));
        }
      }
    }
  } else {
    ObArray<ObRawExpr *> rowkey_exprs;
    if (OB_FAIL(rowkey_exprs.assign(op.get_rowkey_exprs()))) {
      LOG_WARN("failed to assign rowkey exprs", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_exprs(rowkey_exprs, result_output))) {
      LOG_WARN("failed to generate main table rowkey exprs", K(ret));
    }
  }

  if (FAILEDx(vec_ir_scan_ctdef.result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret), K(result_output));
  }
  return ret;
}

int ObTscCgService::generate_text_ir_spec_exprs(const ObTextRetrievalInfo &tr_info,
                                                ObDASIRScanCtDef &text_ir_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr *, 4> result_output;
  if (OB_ISNULL(tr_info.match_expr_) || OB_ISNULL(tr_info.relevance_expr_) ||
      OB_ISNULL(tr_info.doc_id_column_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(cg_.mark_expr_self_produced(tr_info.match_expr_))) {
    LOG_WARN("failed to mark raw agg expr", K(ret), KPC(tr_info.match_expr_));
  } else if (OB_FAIL(cg_.generate_rt_expr(*tr_info.match_expr_->get_search_key(), text_ir_scan_ctdef.search_text_))) {
    LOG_WARN("cg rt expr for search text failed", K(ret));
  } else {
    text_ir_scan_ctdef.mode_flag_ = tr_info.match_expr_->get_mode_flag();
    const UIntFixedArray &inv_scan_col_id = text_ir_scan_ctdef.get_inv_idx_scan_ctdef()->access_column_ids_;
    const ObColumnRefRawExpr *doc_id_column = tr_info.doc_id_column_;
    const ObColumnRefRawExpr *doc_length_column = tr_info.doc_length_column_;

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

  if (OB_SUCC(ret)) {
    // mark match columns in match_expr produced
    ObIArray<ObRawExpr*> &match_columns = tr_info.match_expr_->get_match_columns();
    for (int64_t i = 0; OB_SUCC(ret) && i < match_columns.count(); ++i) {
      if (OB_FAIL(cg_.mark_expr_self_produced(match_columns.at(i)))) {
        LOG_WARN("failed to mark match column expr as produced", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && nullptr != tr_info.pushdown_match_filter_) {
    if (OB_FAIL(cg_.generate_rt_expr(*tr_info.pushdown_match_filter_, text_ir_scan_ctdef.match_filter_))) {
      LOG_WARN("cg rt expr for match filter failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && tr_info.need_calc_relevance_) {
    if (OB_ISNULL(tr_info.relevance_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null relevance expr", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_expr(*tr_info.relevance_expr_, text_ir_scan_ctdef.relevance_expr_))) {
      LOG_WARN("cg rt expr for relevance expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && (tr_info.need_calc_relevance_ || nullptr != tr_info.pushdown_match_filter_)) {
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

int ObTscCgService::generate_vec_id_lookup_ctdef(const ObLogTableScan &op,
                                                  ObTableScanCtDef &tsc_ctdef,
                                                  ObDASBaseCtDef *vec_scan_ctdef,
                                                  ObDASIRAuxLookupCtDef *&aux_lookup_ctdef,
                                                  ObExpr* vid_col)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;
  ObDASScanCtDef *scan_ctdef = nullptr;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  uint64_t vec_id_index_tid = OB_INVALID_ID;

  aux_lookup_ctdef = nullptr;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_ref_table_id(), data_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
  } else if (OB_ISNULL(data_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get data table schema", K(ret));
  } else if (OB_FAIL(data_schema->get_vec_id_rowkey_tid(vec_id_index_tid))) {
    LOG_WARN("failed to get vid rowkey index tid", K(ret), KPC(data_schema));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_ref_table_id(),
                                                    vec_id_index_tid,
                                                    op.get_stmt(),
                                                    index_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(vec_id_index_tid));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get doc_id index schema", K(ret), K(vec_id_index_tid));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, cg_.phy_plan_->get_allocator(), scan_ctdef))) {
    LOG_WARN("alloc das ctdef failed", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_IR_AUX_LOOKUP, cg_.phy_plan_->get_allocator(), aux_lookup_ctdef))) {
    LOG_WARN("alloc aux lookup ctdef failed", K(ret));
  } else if (OB_ISNULL(aux_lookup_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &cg_.phy_plan_->get_allocator(), 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    bool has_rowscn = false;
    DASScanCGCtx cg_ctx;
    ObArray<ObExpr*> result_outputs;
    scan_ctdef->ref_table_id_ = vec_id_index_tid;
    aux_lookup_ctdef->children_cnt_ = 2;
    ObDASTableLocMeta *scan_loc_meta = OB_NEWx(ObDASTableLocMeta, &cg_.phy_plan_->get_allocator(), cg_.phy_plan_->get_allocator());
    const int vid_rowkey_rowkey_cnt = 1;
    if (OB_ISNULL(scan_loc_meta)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate scan location meta failed", K(ret));
    } else if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, *scan_ctdef, has_rowscn))) {
      LOG_WARN("generate das lookup scan ctdef failed", K(ret));
    } else if (OB_FAIL(result_outputs.assign(scan_ctdef->result_output_))) {
      LOG_WARN("construct aux lookup ctdef failed", K(ret));
    } else if (OB_FAIL(scan_ctdef->rowkey_exprs_.init(vid_rowkey_rowkey_cnt))) {
      LOG_WARN("init rowkey exprs failed", K(ret));
    } else if (OB_FAIL(scan_ctdef->rowkey_exprs_.push_back(vid_col))) {
      LOG_WARN("push back vid expr failed", K(ret));
    } else if (OB_FAIL(generate_table_loc_meta(op.get_table_id(),
                                               *op.get_stmt(),
                                               *index_schema,
                                               *cg_.opt_ctx_->get_session_info(),
                                               *scan_loc_meta))) {
      LOG_WARN("generate table loc meta failed", K(ret));
    } else if (OB_FAIL(tsc_ctdef.attach_spec_.attach_loc_metas_.push_back(scan_loc_meta))) {
      LOG_WARN("store scan loc meta failed", K(ret));
    } else {
      aux_lookup_ctdef->children_[0] = vec_scan_ctdef;
      aux_lookup_ctdef->children_[1] = scan_ctdef;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(aux_lookup_ctdef->result_output_.assign(result_outputs))) {
      LOG_WARN("assign result output failed", K(ret));
    } else {

    }
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
    DASScanCGCtx cg_ctx;
    ObArray<ObExpr*> result_outputs;
    scan_ctdef->ref_table_id_ = doc_id_index_tid;
    aux_lookup_ctdef->children_cnt_ = 2;
    ObDASTableLocMeta *scan_loc_meta = OB_NEWx(ObDASTableLocMeta, &cg_.phy_plan_->get_allocator(), cg_.phy_plan_->get_allocator());
    if (OB_ISNULL(scan_loc_meta)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate scan location meta failed", K(ret));
    } else if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, *scan_ctdef, has_rowscn))) {
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
      if (op.is_multivalue_index_scan()) {
        ObDASScanCtDef *index_ctdef = static_cast<ObDASScanCtDef *>(ir_scan_ctdef);
        if (OB_FAIL(append_array_no_dup(result_outputs, index_ctdef->result_output_))) {
          LOG_WARN("append result output failed", K(ret));
        }
      }

      if (OB_SUCC(ret) && OB_FAIL(aux_lookup_ctdef->result_output_.assign(result_outputs))) {
        LOG_WARN("assign result output failed", K(ret));
      }
    }
  }

  return ret;
}

int ObTscCgService::extract_rowkey_doc_access_columns(
    const ObLogTableScan &op,
    const ObDASScanCtDef &scan_ctdef,
    ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  bool doc_id_is_found = false;
  const ObIArray<ObRawExpr *> &exprs = op.get_rowkey_id_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, expr is nullptr", K(ret), K(i), K(exprs));
    } else if (ObRawExpr::EXPR_COLUMN_REF != expr->get_expr_class()) {
      // just skip, nothing to do.
    } else if (!doc_id_is_found && static_cast<ObColumnRefRawExpr *>(expr)->is_doc_id_column()) {
      doc_id_is_found = true;
      if (OB_FAIL(access_exprs.push_back(expr))) {
        LOG_WARN("fail to add doc id access expr", K(ret), KPC(expr));
      }
    } else if (static_cast<ObColumnRefRawExpr *>(expr)->is_rowkey_column()) {
      if (OB_FAIL(access_exprs.push_back(expr))) {
        LOG_WARN("fail to add doc id access expr", K(ret), KPC(expr));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!doc_id_is_found)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, doc id raw expr isn't found", K(ret), K(exprs), K(scan_ctdef));
  }
  return ret;
}

int ObTscCgService::generate_das_scan_ctdef_with_doc_id(
    const ObLogTableScan &op,
    ObTableScanCtDef &tsc_ctdef,
    ObDASScanCtDef *scan_ctdef,
    ObDASAttachCtDef *&domain_id_merge_ctdef)
{
  int ret = OB_SUCCESS;
  ObArray<ObExpr*> result_outputs;
  const common::ObIArray<int64_t>& with_domain_types = op.get_rowkey_domain_types();
  const common::ObIArray<uint64_t>& domain_table_ids = op.get_rowkey_domain_tids();
  ObDASDocIdMergeCtDef *doc_id_merge_ctdef = nullptr;
  ObDASScanCtDef *rowkey_doc_scan_ctdef = nullptr;
  DASScanCGCtx cg_ctx;
  if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_DOC_ID_MERGE, cg_.phy_plan_->get_allocator(),
          doc_id_merge_ctdef))) {
    LOG_WARN("fail to allocate to doc id merge ctdef", K(ret));
  } else if (OB_ISNULL(doc_id_merge_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &cg_.phy_plan_->get_allocator(), 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate doc id merge ctdef child array memory", K(ret));
  } else if (OB_FAIL(generate_rowkey_domain_id_ctdef(op, cg_ctx, domain_table_ids.at(0), with_domain_types.at(0), tsc_ctdef, rowkey_doc_scan_ctdef))) {
    LOG_WARN("fail to generate rowkey doc ctdef", K(ret));
  } else if (OB_FAIL(result_outputs.assign(scan_ctdef->result_output_))) {
    LOG_WARN("construct aux lookup ctdef failed", K(ret));
  } else if (OB_UNLIKELY(result_outputs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, result outputs is nullptr", K(ret));
  } else {
    doc_id_merge_ctdef->children_cnt_ = 2;
    doc_id_merge_ctdef->children_[0] = scan_ctdef;
    doc_id_merge_ctdef->children_[1] = rowkey_doc_scan_ctdef;
    if (OB_FAIL(doc_id_merge_ctdef->result_output_.assign(result_outputs))) {
      LOG_WARN("fail to assign result output", K(ret));
    } else {
      domain_id_merge_ctdef = doc_id_merge_ctdef;
    }
  }
  return ret;
}

int ObTscCgService::generate_das_scan_ctdef_with_vec_vid(
    const ObLogTableScan &op,
    ObTableScanCtDef &tsc_ctdef,
    ObDASScanCtDef *scan_ctdef,
    ObDASAttachCtDef *&domain_id_merge_ctdef)
{
  int ret = OB_SUCCESS;
  ObArray<ObExpr*> result_outputs;
  const common::ObIArray<int64_t>& with_domain_types = op.get_rowkey_domain_types();
  const common::ObIArray<uint64_t>& domain_table_ids = op.get_rowkey_domain_tids();
  ObDASVIdMergeCtDef *vid_merge_ctdef = nullptr;
  ObDASScanCtDef *rowkey_vid_scan_ctdef = nullptr;
  DASScanCGCtx cg_ctx;
  if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_VID_MERGE, cg_.phy_plan_->get_allocator(),
          vid_merge_ctdef))) {
    LOG_WARN("fail to allocate to doc id merge ctdef", K(ret));
  } else if (OB_ISNULL(vid_merge_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &cg_.phy_plan_->get_allocator(), 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate doc id merge ctdef child array memory", K(ret));
  } else if (OB_FAIL(generate_rowkey_domain_id_ctdef(op, cg_ctx, domain_table_ids.at(0), with_domain_types.at(0), tsc_ctdef, rowkey_vid_scan_ctdef))) {
    LOG_WARN("fail to generate rowkey doc ctdef", K(ret));
  } else if (OB_FAIL(result_outputs.assign(scan_ctdef->result_output_))) {
    LOG_WARN("construct aux lookup ctdef failed", K(ret));
  } else if (OB_UNLIKELY(result_outputs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, result outputs is nullptr", K(ret));
  } else {
    vid_merge_ctdef->children_cnt_ = 2;
    vid_merge_ctdef->children_[0] = scan_ctdef;
    vid_merge_ctdef->children_[1] = rowkey_vid_scan_ctdef;
    if (OB_FAIL(vid_merge_ctdef->result_output_.assign(result_outputs))) {
      LOG_WARN("fail to assign result output", K(ret));
    } else {
      domain_id_merge_ctdef = vid_merge_ctdef;
    }
  }
  return ret;
}

int ObTscCgService::generate_das_scan_ctdef_with_domain_id(
    const ObLogTableScan &op,
    ObTableScanCtDef &tsc_ctdef,
    ObDASScanCtDef *scan_ctdef,
    ObDASAttachCtDef *&domain_id_merge_ctdef)
{
  int ret = OB_SUCCESS;
  ObArray<ObExpr*> result_outputs;
  const common::ObIArray<int64_t>& with_domain_types = op.get_rowkey_domain_types();
  const common::ObIArray<uint64_t>& domain_table_ids = op.get_rowkey_domain_tids();
  ObDASDomainIdMergeCtDef *tmp_domain_id_merge_ctdef = nullptr;
  uint64_t tenant_data_version = 0;
  int64_t child_cnt = with_domain_types.count() + 1;
  if (OB_ISNULL(scan_ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(scan_ctdef));
  } else if (with_domain_types.count() != domain_table_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid domain type info", K(ret), K(with_domain_types), K(domain_table_ids));
  } else if (OB_ISNULL(cg_.opt_ctx_->get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get session info", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(cg_.opt_ctx_->get_session_info()->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_3_5_1) {
    // old version
    if (with_domain_types.count() != 1 ||
        (with_domain_types.at(0) != ObDomainIdUtils::ObDomainIDType::DOC_ID &&
         with_domain_types.at(0) != ObDomainIdUtils::ObDomainIDType::VID)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid domain type for old version", K(ret), K(with_domain_types));
    } else if (with_domain_types.at(0) == ObDomainIdUtils::ObDomainIDType::DOC_ID) { // doc id
      if (OB_FAIL(generate_das_scan_ctdef_with_doc_id(op, tsc_ctdef, scan_ctdef, domain_id_merge_ctdef))) {
        LOG_WARN("fail to generate doc id ctdef", K(ret));
      }
    } else { // vid
      if (OB_FAIL(generate_das_scan_ctdef_with_vec_vid(op, tsc_ctdef, scan_ctdef, domain_id_merge_ctdef))) {
        LOG_WARN("fail to generate vec vid ctdef", K(ret));
      }
    }
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_DOMAIN_ID_MERGE, cg_.phy_plan_->get_allocator(),
          tmp_domain_id_merge_ctdef))) {
    LOG_WARN("fail to allocate to domain id merge ctdef", K(ret));
  } else if (OB_ISNULL(tmp_domain_id_merge_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &cg_.phy_plan_->get_allocator(), child_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate domain id merge ctdef child array memory", K(ret));
  } else if (OB_FAIL(tmp_domain_id_merge_ctdef->domain_types_.prepare_allocate(with_domain_types.count()))) {
    LOG_WARN("fail to allocate domain types array memory", K(ret));
  } else {
    tmp_domain_id_merge_ctdef->children_cnt_ = child_cnt;
    tmp_domain_id_merge_ctdef->children_[0] = scan_ctdef;
    for (int64_t i = 0; OB_SUCC(ret) && i < with_domain_types.count(); i++) {
      ObDASScanCtDef *rowkey_domain_scan_ctdef = nullptr;
      DASScanCGCtx cg_ctx;
      if (OB_FAIL(generate_rowkey_domain_id_ctdef(op, cg_ctx, domain_table_ids.at(i), with_domain_types.at(i), tsc_ctdef, rowkey_domain_scan_ctdef))) {
        LOG_WARN("fail to generate rowkey domain ctdef", K(ret));
      } else {
        tmp_domain_id_merge_ctdef->domain_types_.at(i) = with_domain_types.at(i);
        tmp_domain_id_merge_ctdef->children_[i + 1] = rowkey_domain_scan_ctdef;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(result_outputs.assign(scan_ctdef->result_output_))) {
      LOG_WARN("construct aux lookup ctdef failed", K(ret));
    } else if (OB_UNLIKELY(result_outputs.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, result outputs is nullptr", K(ret));
    } else if (OB_FAIL(tmp_domain_id_merge_ctdef->result_output_.assign(result_outputs))) {
      LOG_WARN("fail to assign result output", K(ret));
    } else {
      domain_id_merge_ctdef = tmp_domain_id_merge_ctdef;
    }
  }
  return ret;
}

int ObTscCgService::extract_rowkey_domain_id_access_columns(
    const ObLogTableScan &op,
    const ObDASScanCtDef &scan_ctdef,
    ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  bool domain_id_is_found = false;
  const ObIArray<ObRawExpr *> &exprs = op.get_rowkey_id_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, expr is nullptr", K(ret), K(i), K(exprs));
    } else if (ObRawExpr::EXPR_COLUMN_REF != expr->get_expr_class()) {
      // just skip, nothing to do.
    } else if (static_cast<ObColumnRefRawExpr *>(expr)->get_table_id() != scan_ctdef.ref_table_id_) {
      // just skip, nothing to do.
    } else if (ObDomainIdUtils::is_domain_id_index_col_expr(expr)) {
      domain_id_is_found = true;
      if (OB_FAIL(access_exprs.push_back(expr))) {
        LOG_WARN("fail to add domain id access expr", K(ret), KPC(expr));
      }
    } else if (static_cast<ObColumnRefRawExpr *>(expr)->is_rowkey_column()) {
      if (OB_FAIL(access_exprs.push_back(expr))) {
        LOG_WARN("fail to add domain id access expr", K(ret), KPC(expr));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!domain_id_is_found)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, domain id raw expr isn't found", K(ret), K(exprs), K(scan_ctdef));
  }
  return ret;
}

int ObTscCgService::extract_rowkey_domain_id_output_columns_ids(
    const share::schema::ObTableSchema &schema,
    const ObLogTableScan &op,
    const ObDASScanCtDef &scan_ctdef,
    const bool need_output_rowkey,
    ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  bool domain_id_is_found = false;
  const ObIArray<ObRawExpr *> &exprs = op.get_rowkey_id_exprs();
  ObArray<ObRawExpr *> access_exprs;
  // NOTE(liyao): ivf pq have 2 domain id index cols, so cannot exit the loop when domain_id_is_found == true
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, expr is nullptr", K(ret), K(i), K(exprs));
    } else if (ObRawExpr::EXPR_COLUMN_REF != expr->get_expr_class()) {
      // just skip, nothing to do.
    } else if (static_cast<ObColumnRefRawExpr *>(expr)->get_table_id() != schema.get_table_id()) {
      // just skip, nothing to do.
    } else if (ObDomainIdUtils::is_domain_id_index_col_expr(expr)) {
      domain_id_is_found = true;
      if (OB_FAIL(access_exprs.push_back(expr))) {
        LOG_WARN("fail to add doc id access expr", K(ret), KPC(expr));
      }
    } else if (need_output_rowkey && static_cast<ObColumnRefRawExpr *>(expr)->is_rowkey_column()) {
      if (OB_FAIL(access_exprs.push_back(expr))) {
        LOG_WARN("fail to add doc id access expr", K(ret), KPC(expr));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!domain_id_is_found)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, vid id raw expr isn't found", K(ret), K(exprs), K(scan_ctdef));
  } else if (OB_FAIL(extract_das_column_ids(access_exprs, output_cids))) {
    LOG_WARN("extract column ids failed", K(ret));
  }
  return ret;
}

int ObTscCgService::generate_rowkey_domain_id_ctdef(
    const ObLogTableScan &op,
    const DASScanCGCtx &cg_ctx,
    const uint64_t domain_tid,
    int64_t domain_type,
    ObTableScanCtDef &tsc_ctdef,
    ObDASScanCtDef *&rowkey_domain_scan_ctdef)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *rowkey_domain_id_schema = nullptr;
  ObDASScanCtDef *scan_ctdef = nullptr;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  uint64_t rowkey_domain_id_tid = domain_tid;
  ObDomainIdUtils::ObDomainIDType cur_type = static_cast<ObDomainIdUtils::ObDomainIDType>(domain_type);

  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, schema guard is nullptr", K(ret), KP(cg_.opt_ctx_));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_ref_table_id(), data_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(op.get_ref_table_id()));
  } else if (OB_ISNULL(data_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get data table schema", K(ret));
  } else if (rowkey_domain_id_tid == OB_INVALID_ID &&
             OB_FAIL(ObDomainIdUtils::get_domain_tid_table_by_type(cur_type, data_schema, rowkey_domain_id_tid))) {
   LOG_WARN("failed to get rowkey doc tid", K(ret), KPC(data_schema));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_ref_table_id(),
                                                    rowkey_domain_id_tid,
                                                    op.get_stmt(),
                                                    rowkey_domain_id_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(rowkey_domain_id_tid));
  } else if (OB_ISNULL(rowkey_domain_id_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rowkey doc schema", K(ret), K(rowkey_domain_id_tid));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, cg_.phy_plan_->get_allocator(), scan_ctdef))) {
    LOG_WARN("alloc das ctdef failed", K(ret));
  } else {
    bool has_rowscn = false;
    scan_ctdef->ref_table_id_ = rowkey_domain_id_tid;
    ObDASTableLocMeta *scan_loc_meta =
      OB_NEWx(ObDASTableLocMeta, &cg_.phy_plan_->get_allocator(), cg_.phy_plan_->get_allocator());
    if (OB_ISNULL(scan_loc_meta)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate scan location meta failed", K(ret));
    } else if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, *scan_ctdef, has_rowscn))) {
      LOG_WARN("generate das lookup scan ctdef failed", K(ret));
    } else if (OB_FAIL(generate_table_loc_meta(op.get_table_id(),
                                               *op.get_stmt(),
                                               *rowkey_domain_id_schema,
                                               *cg_.opt_ctx_->get_session_info(),
                                               *scan_loc_meta))) {
      LOG_WARN("generate table loc meta failed", K(ret));
    } else if (OB_FAIL(tsc_ctdef.attach_spec_.attach_loc_metas_.push_back(scan_loc_meta))) {
      LOG_WARN("store scan loc meta failed", K(ret));
    } else {
      rowkey_domain_scan_ctdef = scan_ctdef;
    }
  }
  return ret;
}

int ObTscCgService::check_lookup_iter_type(ObDASBaseCtDef *scan_ctdef, bool &need_proj_relevance_score) {
  int ret = OB_SUCCESS;
  ObDASIRScanCtDef *ctdef = NULL;
  if (OB_ISNULL(scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan ctdef is null", K(ret));
  } else if (OB_UNLIKELY(need_proj_relevance_score)) {
    // do nothing
  } else {
    if (scan_ctdef->op_type_ == ObDASOpType::DAS_OP_IR_SCAN) {
      ctdef = static_cast<ObDASIRScanCtDef*>(scan_ctdef);
      if (OB_NOT_NULL(ctdef) && ctdef->need_proj_relevance_score()) {
        need_proj_relevance_score = true;
      }
    }
    if (!need_proj_relevance_score) {
      for (int64_t i = 0 ; OB_SUCC(ret) && !need_proj_relevance_score && i < scan_ctdef->children_cnt_ ; i ++) {
        ret = SMART_CALL(check_lookup_iter_type(scan_ctdef->children_[i], need_proj_relevance_score));
      }
    }
  }
  return ret;
}

int ObTscCgService::generate_table_lookup_ctdef(const ObLogTableScan &op,
                                                ObTableScanCtDef &tsc_ctdef,
                                                ObDASBaseCtDef *scan_ctdef,
                                                ObDASTableLookupCtDef *&lookup_ctdef,
                                                ObDASAttachCtDef *&domain_id_merge_ctdef)
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
    DASScanCGCtx cg_ctx;
    const ObTableSchema *table_schema = nullptr;
    ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
    tsc_ctdef.lookup_ctdef_->ref_table_id_ = op.get_real_ref_table_id();

    if (OB_FAIL(generate_das_scan_ctdef(op, cg_ctx, *tsc_ctdef.lookup_ctdef_, has_rowscn))) {
      LOG_WARN("generate das lookup scan ctdef failed", K(ret));
    }  else if (OB_FAIL(schema_guard->get_table_schema(op.get_table_id(),
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
      // lookup to main table should invoke get
      tsc_ctdef.lookup_ctdef_->is_get_ = true;
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

  if (OB_SUCC(ret) && op.is_tsc_with_domain_id()) {
    if (OB_FAIL(generate_das_scan_ctdef_with_domain_id(op, tsc_ctdef, tsc_ctdef.lookup_ctdef_, domain_id_merge_ctdef))) {
      LOG_WARN("fail to generate das scan ctdef with doc id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObDASOpType lookup_type = ObDASOpType::DAS_OP_INVALID;
    bool need_proj_relevance_score = false;
    uint64_t data_version = 0;

    if (OB_FAIL(check_lookup_iter_type(scan_ctdef, need_proj_relevance_score))) {
      LOG_WARN("check lookup iter type failed", K(ret));
    } else if (need_proj_relevance_score && OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
      LOG_WARN("get tenant data version failed", K(ret), K(data_version), K(MTL_ID()));
    } else if (FALSE_IT(lookup_type = (need_proj_relevance_score && (data_version >= DATA_VERSION_4_3_5_1)) ?
                                          ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP :
                                          ObDASOpType::DAS_OP_TABLE_LOOKUP)) {
    } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(lookup_type, allocator, lookup_ctdef))) {
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
        if (op.is_tsc_with_domain_id()) {
          lookup_ctdef->children_[1] = static_cast<ObDASBaseCtDef *>(domain_id_merge_ctdef);
        } else {
          lookup_ctdef->children_[1] = static_cast<ObDASBaseCtDef *>(tsc_ctdef.lookup_ctdef_);
        }
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
    } else if (lookup_ctdef->op_type_ == ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP) {
      ObDASIndexProjLookupCtDef *cache_lookup_ctdef = static_cast<ObDASIndexProjLookupCtDef* >(lookup_ctdef);
      if (OB_ISNULL(cache_lookup_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cast lookup ctdef to the op type struct failed", K(ret), K(lookup_ctdef->op_type_));
      } else {
        ObIArray<ObExpr *> &rowkey_scan_output = ObDASTaskFactory::is_attached(scan_ctdef->op_type_) ?
                                                     static_cast<ObDASAttachCtDef *>(scan_ctdef)->result_output_ :
                                                     static_cast<ObDASScanCtDef *>(scan_ctdef)->result_output_;
        if (OB_FAIL(cache_lookup_ctdef->index_scan_proj_exprs_.assign(rowkey_scan_output))) {
          LOG_WARN("failed to assign index scan project column exprs", K(ret));
        } else if (OB_FAIL(append_array_no_dup(result_outputs, cache_lookup_ctdef->index_scan_proj_exprs_))) {
          LOG_WARN("failed to append final result outputs", K(ret));
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

int ObTscCgService::extract_vec_id_index_back_access_columns(
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
    if (col_expr->is_vec_hnsw_vid_column()
        || (col_expr->get_table_id() == op.get_table_id() && col_expr->is_rowkey_column())) {
      if (OB_FAIL(add_var_to_array_no_dup(access_exprs, raw_expr))) {
        LOG_WARN("failed to push vid index back access column to access exprs", K(ret));
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
    } else if (!exprs.at(i)->has_flag(CNT_MATCH_EXPR)) {
      if (OB_FAIL(temp_exprs.push_back(exprs.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (OB_FAIL(flatten_and_filter_match_exprs(exprs.at(i), temp_exprs))) {
      LOG_WARN("failed to flatten and filter match exprs", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(exprs.assign(temp_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  return ret;
}

int ObTscCgService::flatten_and_filter_match_exprs(ObRawExpr *expr, ObIArray<ObRawExpr *> &res_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(expr));
  } else if (expr->is_match_against_expr()) {
    // skip
  } else if (!expr->has_flag(CNT_MATCH_EXPR)) {
    if (OB_FAIL(res_exprs.push_back(expr))) {
      LOG_WARN("failed to append flattened expr", K(ret));
    }
  } else {
    const int64_t param_cnt = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
      if (OB_FAIL(SMART_CALL(flatten_and_filter_match_exprs(expr->get_param_expr(i), res_exprs)))) {
        LOG_WARN("failed to flatten and filter match exprs", K(ret), K(i), KPC(expr));
      }
    }
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
      ObSortFieldCollation field_collation(field_idx,
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
  } else if (child_ctdef->op_type_ == DAS_OP_TABLE_SCAN
      && OB_FAIL(append_array_no_dup(result_output, static_cast<ObDASScanCtDef *>(child_ctdef)->result_output_))) {
    LOG_WARN("failed to append child result output", K(ret));
  } else if (OB_FAIL(sort_ctdef->result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret));
  } else {
    LOG_TRACE("generate sort ctdef finished", K(sort_keys), K(sort_ctdef->sort_exprs_), K(result_output), K(ret));
  }
  return ret;
}

int ObTscCgService::generate_das_sort_ctdef(
    const ObIArray<ObExpr *> &sort_keys,
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
  } else {
    sort_ctdef->children_cnt_ = 1;
    sort_ctdef->children_[0] = child_ctdef;
    sort_ctdef->fetch_with_ties_ = false;
  }

  ObSEArray<ObExpr *, 4> result_output;
  int64_t field_idx = 0;
  for (int64_t i = 0; i < sort_keys.count() && OB_SUCC(ret); ++i) {
    ObExpr *expr = sort_keys.at(i);
    ObSortFieldCollation field_collation(field_idx, expr->datum_meta_.cs_type_, true, NULL_FIRST);
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

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_array_no_dup(result_output, sort_ctdef->sort_exprs_))) {
    LOG_WARN("failed to append sort exprs to result output", K(ret));
  } else if (ObDASTaskFactory::is_attached(child_ctdef->op_type_)
      && OB_FAIL(append_array_no_dup(result_output, static_cast<ObDASAttachCtDef *>(child_ctdef)->result_output_))) {
    LOG_WARN("failed to append child result output", K(ret));
  } else if (child_ctdef->op_type_ == DAS_OP_TABLE_SCAN
      && OB_FAIL(append_array_no_dup(result_output, static_cast<ObDASScanCtDef *>(child_ctdef)->result_output_))) {
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

int ObTscCgService::generate_mr_mv_scan_flag(const ObLogTableScan &op, ObQueryFlag &query_flag) const
{
  int ret = OB_SUCCESS;
  const ObLogPlan *log_plan = op.get_plan();
  query_flag.mr_mv_scan_ = op.get_mr_mv_scan();
  if (!query_flag.is_mr_mview_query() && nullptr != log_plan) {
    // for query OLD_NEW data from normal table(use hint to mview path)
    bool has_enable_param = false;
    const ObOptParamHint &opt_params = log_plan->get_stmt()->get_query_ctx()->get_global_hint().opt_params_;
    if (OB_FAIL(opt_params.has_opt_param(ObOptParamHint::HIDDEN_COLUMN_VISIBLE, has_enable_param))) {
      LOG_WARN("check has hint hidden_column_visible failed", K(ret), K(opt_params));
    } else if (has_enable_param) {
      const common::ObIArray<ObRawExpr *> &access_exprs = op.get_access_exprs();
      FOREACH_CNT(e, access_exprs) {
        if (T_PSEUDO_OLD_NEW_COL == (*e)->get_expr_type()) {
          query_flag.mr_mv_scan_ = ObQueryFlag::MRMVScanMode::RealTimeMode;
          break;
        }
      }
    }
  }
  return ret;
}

int ObTscCgService::generate_functional_lookup_ctdef(const ObLogTableScan &op,
                                                     ObTableScanCtDef &tsc_ctdef,
                                                     ObDASBaseCtDef *rowkey_scan_ctdef,
                                                     ObDASBaseCtDef *main_lookup_ctdef,
                                                     ObDASBaseCtDef *&root_ctdef)
{
  // Functional lookup will scan rowkey from one table (main table or secondary index) first,
  // and then do functional lookup on specific secondary index to calculate index-related exprs.
  // Can also do main table lookup after rowkey scan if needed.
  int ret = OB_SUCCESS;
  const ObIArray<ObTextRetrievalInfo> &lookup_tr_infos = op.get_lookup_tr_infos();
  const bool has_main_lookup = nullptr != main_lookup_ctdef;
  ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();
  ObDASFuncLookupCtDef *tmp_func_lookup_ctdef = nullptr;
  ObDASIndexProjLookupCtDef *root_lookup_ctdef = nullptr;
  ObArray<ObExpr *> func_lookup_result_outputs;
  ObArray<ObExpr *> final_result_outputs;
  DASScanCGCtx cg_ctx;
  if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_FUNC_LOOKUP, ctdef_alloc, tmp_func_lookup_ctdef))) {
    LOG_WARN("allocate functional lookup ctdef failed", K(ret));
  } else {
    tmp_func_lookup_ctdef->main_lookup_cnt_ = has_main_lookup ? 1 : 0;
    tmp_func_lookup_ctdef->func_lookup_cnt_ = lookup_tr_infos.count();
    tmp_func_lookup_ctdef->doc_id_lookup_cnt_ = lookup_tr_infos.count() > 0 ? 1 : 0;
    tmp_func_lookup_ctdef->children_cnt_ = tmp_func_lookup_ctdef->main_lookup_cnt_
        + tmp_func_lookup_ctdef->func_lookup_cnt_ + tmp_func_lookup_ctdef->doc_id_lookup_cnt_;
    if (OB_ISNULL(tmp_func_lookup_ctdef->children_
        = OB_NEW_ARRAY(ObDASBaseCtDef *, &ctdef_alloc, tmp_func_lookup_ctdef->children_cnt_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate functional lookup ctdef children failed", K(ret));
    } else {
      if (has_main_lookup) {
        tmp_func_lookup_ctdef->children_[0] = main_lookup_ctdef;
        if (OB_UNLIKELY(main_lookup_ctdef->op_type_ != DAS_OP_TABLE_SCAN)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected main lookup ctdef type", K(ret), KPC(main_lookup_ctdef));
        } else if (OB_FAIL(func_lookup_result_outputs.assign(
            static_cast<ObDASScanCtDef *>(main_lookup_ctdef)->result_output_))) {
          LOG_WARN("failed to append func lookup result", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && lookup_tr_infos.count() > 0) {
    // generate rowkey->doc_id lookup scan
    const int64_t doc_id_lookup_ctdef_idx = has_main_lookup ? 1 : 0;
    ObDASScanCtDef *doc_id_lookup_scan_ctdef = nullptr;
    ObArray<ObRawExpr *> rowkey_exprs;
    uint64_t doc_tid = OB_INVALID_ID;
    cg_ctx.set_is_func_lookup();
    if (OB_FAIL(generate_rowkey_domain_id_ctdef(op, cg_ctx, doc_tid, ObDomainIdUtils::ObDomainIDType::DOC_ID, tsc_ctdef, doc_id_lookup_scan_ctdef))) {
      LOG_WARN("generate doc_id lookup scan ctdef failed", K(ret));
    } else if (OB_FAIL(rowkey_exprs.assign(op.get_rowkey_exprs()))) {
      LOG_WARN("failed to assign rowkey exprs", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_exprs(rowkey_exprs, doc_id_lookup_scan_ctdef->rowkey_exprs_))) {
      LOG_WARN("failed to generate rowkey exprs for doc_id lookup scan", K(ret));
    } else {
      tmp_func_lookup_ctdef->children_[doc_id_lookup_ctdef_idx] = doc_id_lookup_scan_ctdef;

      for (int64_t i = 0; OB_SUCC(ret) && i < doc_id_lookup_scan_ctdef->result_output_.count(); ++i) {
        ObExpr *doc_id_lookup_result = doc_id_lookup_scan_ctdef->result_output_.at(i);
        if (OB_ISNULL(doc_id_lookup_result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null rowkey expr", K(ret));
        } else if (doc_id_lookup_result->type_ == T_PSEUDO_ROW_TRANS_INFO_COLUMN
            || doc_id_lookup_result->type_ == T_PSEUDO_GROUP_ID) {
          // skip
        } else if (nullptr == tmp_func_lookup_ctdef->lookup_doc_id_expr_) {
          tmp_func_lookup_ctdef->lookup_doc_id_expr_ = doc_id_lookup_result;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than one doc id result expr for rowkey 2 doc_id lookup", K(ret), KPC(doc_id_lookup_scan_ctdef));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < lookup_tr_infos.count(); ++i) {
      cg_ctx.reset();
      cg_ctx.set_func_lookup_idx(i);
      const int64_t func_lookup_base_idx = doc_id_lookup_ctdef_idx + 1;
      const int64_t cur_children_idx = func_lookup_base_idx + i;
      ObDASBaseCtDef *tr_lookup_scan_ctdef = nullptr;
      if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_IR_SCAN, ctdef_alloc, tr_lookup_scan_ctdef))) {
        LOG_WARN("allocate text retrieval lookup scan failed", K(ret));
      } else if (OB_FAIL(generate_text_ir_ctdef(op, cg_ctx, tsc_ctdef, tsc_ctdef.scan_ctdef_, tr_lookup_scan_ctdef))) {
        LOG_WARN("failed to generate text retrieval ctdef", K(ret));
      } else if (OB_UNLIKELY(tr_lookup_scan_ctdef->op_type_ != DAS_OP_IR_SCAN)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected lookup tr scan type", K(ret));
      } else if (OB_FAIL(append_array_no_dup(
          func_lookup_result_outputs, static_cast<ObDASIRScanCtDef *>(tr_lookup_scan_ctdef)->result_output_))) {
        LOG_WARN("failed to append func lookup result", K(ret));
      } else {
        tmp_func_lookup_ctdef->children_[cur_children_idx] = tr_lookup_scan_ctdef;
      }
    }
  }

  if (FAILEDx(tmp_func_lookup_ctdef->result_output_.assign(func_lookup_result_outputs))) {
    LOG_WARN("failed to assign func lookup result output", K(ret));
  } else if (OB_FAIL(final_result_outputs.assign(func_lookup_result_outputs))) {
    LOG_WARN("failed to append final lookup result output", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_INDEX_PROJ_LOOKUP, ctdef_alloc, root_lookup_ctdef))) {
    LOG_WARN("failed to allocate das ctdef", K(ret));
  } else if (OB_ISNULL(root_lookup_ctdef->children_
      = OB_NEW_ARRAY(ObDASBaseCtDef *, &ctdef_alloc, 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate root lookup ctdef childern failed", K(ret));
  } else if (OB_FAIL(append_array_no_dup(final_result_outputs, tmp_func_lookup_ctdef->result_output_))) {
    LOG_WARN("failed to append final result outputs", K(ret));
  } else {
    root_lookup_ctdef->children_cnt_ = 2;
    root_lookup_ctdef->children_[0] = rowkey_scan_ctdef;
    root_lookup_ctdef->children_[1] = tmp_func_lookup_ctdef;

    if ((!has_main_lookup && rowkey_scan_ctdef->op_type_ == ObDASOpType::DAS_OP_TABLE_SCAN)
        || ObDASTaskFactory::is_attached(rowkey_scan_ctdef->op_type_)) {
      // When rowkey scan is a normal table scan with no main table lookup,
      // rowkey scan will project all output columns on base table for table scan
      // When rowkey scan is an attached scan, need to project its output if exist
      ObIArray<ObExpr *> &rowkey_scan_output = ObDASTaskFactory::is_attached(rowkey_scan_ctdef->op_type_)
        ? static_cast<ObDASAttachCtDef *>(rowkey_scan_ctdef)->result_output_
        : static_cast<ObDASScanCtDef *>(rowkey_scan_ctdef)->result_output_;
      if (OB_FAIL(root_lookup_ctdef->index_scan_proj_exprs_.assign(rowkey_scan_output))) {
        LOG_WARN("failed to assign index scan project column exprs", K(ret));
      } else if (OB_FAIL(append_array_no_dup(final_result_outputs, root_lookup_ctdef->index_scan_proj_exprs_))) {
        LOG_WARN("failed to append final result outputs", K(ret));
      }
    }

    if (FAILEDx(root_lookup_ctdef->result_output_.assign(final_result_outputs))) {
      LOG_WARN("failed to append root lookup result outputs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    root_ctdef = root_lookup_ctdef;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
