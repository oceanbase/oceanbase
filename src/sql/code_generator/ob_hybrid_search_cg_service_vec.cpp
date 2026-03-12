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
#include "sql/code_generator/ob_hybrid_search_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_multi_match_query.h"
#include "sql/das/search/ob_das_match_phrase_query.h"
#include "sql/das/search/ob_das_topk_collect_op.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase
{
namespace sql
{
int ObHybridSearchCgService::generate_ctdef(ObLogTableScan &op, const ObFusionNode *fusion_node, ObDASFusionCtDef *&fusion_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASFusionCtDef *new_fusion_ctdef = nullptr;
  ObArray<ObExpr *> rowkey_exprs;
  ObExpr *size_expr = nullptr;
  ObExpr *offset_expr = nullptr;
  ObExpr *window_size_expr = nullptr;
  ObExpr *rank_constant_expr = nullptr;
  ObExpr *min_score_expr = nullptr;
  ObSEArray<ObExpr*, 8> result_output_exprs;
  ObSEArray<ObExpr*, 8> score_exprs;
  ObSEArray<ObExpr*, 8> weight_exprs;
  ObSEArray<ObExpr*, 8> path_top_k_limit_exprs;

  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(fusion_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan or fusion node is null", K(ret), KP(cg_.phy_plan_), KP(fusion_node));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_FUSION_QUERY, cg_.phy_plan_->get_allocator(), new_fusion_ctdef))) {
    LOG_WARN("failed to allocate fusion ctdef", K(ret));
  } else if (OB_ISNULL(new_fusion_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion ctdef is null", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(op.get_rowkey_exprs(), rowkey_exprs))) {
    LOG_WARN("failed to generate rowkey exprs", K(ret));
  } else if (OB_FAIL(result_output_exprs.assign(rowkey_exprs))) {
    LOG_WARN("failed to assign rowkey exprs to result_output", K(ret));
  } else if (OB_NOT_NULL(fusion_node->size_) && OB_FAIL(cg_.generate_rt_expr(*fusion_node->size_, size_expr))) {
    LOG_WARN("failed to generate size rt expr", K(ret));
  } else if (OB_NOT_NULL(fusion_node->from_) && OB_FAIL(cg_.generate_rt_expr(*fusion_node->from_, offset_expr))) {
    LOG_WARN("failed to generate offset rt expr", K(ret));
  } else if (OB_NOT_NULL(fusion_node->window_size_) && OB_FAIL(cg_.generate_rt_expr(*fusion_node->window_size_, window_size_expr))) {
    LOG_WARN("failed to generate window size rt expr", K(ret));
  } else if (OB_NOT_NULL(fusion_node->min_score_) && OB_FAIL(cg_.generate_rt_expr(*fusion_node->min_score_, min_score_expr))) {
    LOG_WARN("failed to generate min score rt expr", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(fusion_node->path_top_k_limit_, path_top_k_limit_exprs))) {
    LOG_WARN("failed to generate path top k limit exprs", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(fusion_node->weight_cols_,weight_exprs))) {
    LOG_WARN("failed to generate weight calc exprs", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(fusion_node->score_cols_, score_exprs))) {
    LOG_WARN("failed to generate score calc exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < score_exprs.count(); ++i) {
      if (OB_FAIL(result_output_exprs.push_back(score_exprs.at(i)))) {
        LOG_WARN("failed to push back score expr to result_output", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && fusion_node->method_ == ObFusionMethod::RRF && OB_FAIL(cg_.generate_rt_expr(*fusion_node->rank_const_, rank_constant_expr))) {
      LOG_WARN("failed to generate rank constant rt expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(new_fusion_ctdef->init(
        fusion_node->search_index_,
        fusion_node->has_search_subquery_,
        fusion_node->has_vector_subquery_,
        fusion_node->is_top_k_query_,
        fusion_node->method_,
        fusion_node->has_hybrid_fusion_op_,
        size_expr,
        offset_expr,
        window_size_expr,
        rank_constant_expr,
        min_score_expr,
        rowkey_exprs,
        score_exprs,
        result_output_exprs,
        weight_exprs,
        path_top_k_limit_exprs))) {
      LOG_WARN("failed to init fusion ctdef", K(ret));
    } else {
      fusion_ctdef = new_fusion_ctdef;
    }
  }
  return ret;
}

int ObHybridSearchCgService::generate_ctdef(ObLogTableScan &op, const ObVecSearchNode *vec_search_node, ObDASVecIndexDriverCtDef *&res_vec_index_driver_ctdef)
{
  int ret = OB_SUCCESS;

  // 1. generate filter ctdef
  ObDASBaseCtDef *filter_ctdef = nullptr;
  if (vec_search_node->filter_nodes_.count() > 0) {
    if (vec_search_node->filter_nodes_.count() != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid filter nodes count", K(ret), K(vec_search_node->filter_nodes_.count()));
    } else {
      ObHybridSearchNodeBase *filter_node = static_cast<ObHybridSearchNodeBase*>(vec_search_node->filter_nodes_.at(0));
      if (OB_FAIL(generate_hybrid_search_node_ctdef(op, filter_node, filter_ctdef))) {
        LOG_WARN("failed to generate filter ctdef", K(ret));
      } else if (OB_ISNULL(filter_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter ctdef is null", K(ret));
      }
    }
  }

  ObDASBaseCtDef *extracted_filter_ctdef = nullptr;
  if (OB_SUCC(ret) && OB_NOT_NULL(vec_search_node->extracted_scalar_node_)) {
    ObScalarQueryNode *extracted_node = vec_search_node->extracted_scalar_node_;
    if (OB_FAIL(generate_hybrid_search_node_ctdef(op, extracted_node, extracted_filter_ctdef))) {
      LOG_WARN("failed to generate extracted filter ctdef", K(ret));
    } else if (OB_ISNULL(extracted_filter_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extracted filter ctdef is null", K(ret));
    }
  }

  // 2. generate sort_expr, limit_expr, offset_expr
  ObExpr *sort_expr = nullptr;
  ObExpr *limit_expr = nullptr;
  ObExpr *offset_expr = nullptr;
  if (OB_SUCC(ret)) {
    ObRawExpr *expr = vec_search_node->get_sort_key().expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sort key expr is null", K(ret));
    } else if (expr->is_vector_sort_expr()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (expr->get_param_expr(i)->is_column_ref_expr() && OB_FAIL(cg_.mark_expr_self_produced(expr->get_param_expr(i)))) {
          LOG_WARN("mark expr self produced failed", K(ret), KPC(expr->get_param_expr(i)));
        }
      }
    }
  }

  if (FAILEDx(cg_.generate_rt_expr(*vec_search_node->get_sort_key().expr_, sort_expr))) {
    LOG_WARN("generate sort expr failed", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*vec_search_node->get_topk_limit_expr(), limit_expr))) {
    LOG_WARN("generate limit expr failed", K(ret));
  } else if (OB_NOT_NULL(vec_search_node->get_topk_offset_expr())
             && OB_FAIL(cg_.generate_rt_expr(*vec_search_node->get_topk_offset_expr(), offset_expr))) {
    LOG_WARN("generate offset expr failed", K(ret));
  }

  // 3. generate vec index scan ctdef (HNSW)
  ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();
  ObDASVecIndexHNSWScanCtDef *vec_index_scan_ctdef = nullptr;
  if (FAILEDx(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_VEC_INDEX_HNSW_SCAN, ctdef_alloc, vec_index_scan_ctdef))) {
    LOG_WARN("allocate vec index hnsw scan ctdef failed", K(ret));
  } else {
    int64_t vec_child_task_cnt = 4;
    ObDASScanCtDef *delta_buf_table_ctdef = nullptr;
    ObDASScanCtDef *index_id_table_ctdef = nullptr;
    ObDASScanCtDef *snapshot_table_ctdef = nullptr;
    ObDASScanCtDef *com_aux_vec_table_ctdef = nullptr;
    ObVecSearchNode::VecIndexScanParams delta_buf_table_params;
    ObVecSearchNode::VecIndexScanParams index_id_table_params;
    ObVecSearchNode::VecIndexScanParams snapshot_table_params;
    ObVecSearchNode::VecIndexScanParams com_aux_vec_table_params;

    if (OB_FAIL(vec_search_node->get_delta_buf_table_params(delta_buf_table_params))) {
      LOG_WARN("failed to get delta buf table params", K(ret));
    } else if (OB_FAIL(vec_search_node->get_index_id_table_params(index_id_table_params))) {
      LOG_WARN("failed to get index id table params", K(ret));
    } else if (OB_FAIL(vec_search_node->get_snapshot_table_params(snapshot_table_params))) {
      LOG_WARN("failed to get snapshot table params", K(ret));
    } else if (OB_FAIL(vec_search_node->get_com_aux_vec_table_params(com_aux_vec_table_params))) {
      LOG_WARN("failed to get com aux vec table params", K(ret));
    } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, delta_buf_table_params, delta_buf_table_ctdef, false))) {
      LOG_WARN("failed to generate delta buf table ctdef", K(ret));
    } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, index_id_table_params, index_id_table_ctdef, false))) {
      LOG_WARN("failed to generate index id table ctdef", K(ret));
    } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, snapshot_table_params, snapshot_table_ctdef, false))) {
      LOG_WARN("failed to generate snapshot table ctdef", K(ret));
    } else if (OB_FAIL(generate_vec_aux_table_ctdef(op, com_aux_vec_table_params, com_aux_vec_table_ctdef, true))) {
      LOG_WARN("failed to generate com aux vec table ctdef", K(ret));
    } else if (OB_ISNULL(vec_index_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &ctdef_alloc, vec_child_task_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate vec index hnsw scan ctdef children failed", K(ret));
    } else {
      vec_index_scan_ctdef->children_cnt_ = vec_child_task_cnt;
      vec_index_scan_ctdef->children_[0] = delta_buf_table_ctdef;
      vec_index_scan_ctdef->children_[1] = index_id_table_ctdef;
      vec_index_scan_ctdef->children_[2] = snapshot_table_ctdef;
      vec_index_scan_ctdef->children_[3] = com_aux_vec_table_ctdef;
    }
  }

  // 4. set vector query param for vec index scan ctdef
  int64_t dim = 0;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *main_index_table_schema = nullptr;
  ObTableID data_table_id = vec_search_node->get_data_table_id();
  ObTableID main_index_table_id = vec_search_node->main_index_table_id_;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  if (FAILEDx(schema_guard->get_table_schema(data_table_id, data_table_schema))) {
    LOG_WARN("get data table schema failed", K(ret), K(data_table_id));
  } else if (OB_FAIL(schema_guard->get_table_schema(main_index_table_id, main_index_table_schema))) {
    LOG_WARN("get main index table schema failed", K(ret), K(main_index_table_id));
  } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(main_index_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(*main_index_table_schema, *data_table_schema, dim))) {
    LOG_WARN("fail to get vector_index_column dim", K(ret));
  } else if (OB_FAIL(ob_write_string(ctdef_alloc, main_index_table_schema->get_index_params(), vec_index_scan_ctdef->vec_index_param_))) {
    LOG_WARN("fail to write index params", K(ret));
  } else if (OB_FAIL(vec_index_scan_ctdef->query_param_.assign(vec_search_node->get_query_param()))) {
    LOG_WARN("fail to assign query param", K(ret));
  } else {
    vec_index_scan_ctdef->vector_index_param_ = vec_search_node->get_vector_index_param();
    vec_index_scan_ctdef->algorithm_type_ = vec_search_node->get_algorithm_type();
    vec_index_scan_ctdef->vec_type_ = vec_search_node->get_vec_type();
    vec_index_scan_ctdef->dim_ = dim;
    vec_index_scan_ctdef->sort_expr_ = sort_expr;
  }

  // 5. generate vec index driver ctdef
  ObDASVecIndexDriverCtDef *vec_index_driver_ctdef = nullptr;
  int64_t vec_index_driver_child_cnt = 1;
  if (OB_NOT_NULL(filter_ctdef)) {
    vec_index_driver_child_cnt++;
  }
  if (OB_NOT_NULL(extracted_filter_ctdef)) {
    vec_index_driver_child_cnt++;
  }

  if (FAILEDx(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_VEC_INDEX_DRIVER, ctdef_alloc, vec_index_driver_ctdef))) {
    LOG_WARN("allocate vec index driver ctdef failed", K(ret));
  } else if (OB_ISNULL(vec_index_driver_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &ctdef_alloc, vec_index_driver_child_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate vec index driver ctdef children failed", K(ret));
  } else {
    vec_index_driver_ctdef->children_cnt_ = vec_index_driver_child_cnt;
    int child_idx = 1;
    vec_index_driver_ctdef->children_[0] = vec_index_scan_ctdef;
    if (OB_NOT_NULL(filter_ctdef)) {
      vec_index_driver_ctdef->children_[child_idx++] = filter_ctdef;
    }
    if (OB_NOT_NULL(extracted_filter_ctdef)) {
      vec_index_driver_ctdef->children_[child_idx++] = extracted_filter_ctdef;
    }
    vec_index_driver_ctdef->sort_expr_ = sort_expr;
    vec_index_driver_ctdef->limit_expr_ = limit_expr;
    vec_index_driver_ctdef->offset_expr_ = offset_expr;
  }

  // 6. set vec index param for vec index driver ctdef
  if (FAILEDx(ob_write_string(ctdef_alloc, main_index_table_schema->get_index_params(), vec_index_driver_ctdef->vec_index_param_))) {
    LOG_WARN("fail to write index params", K(ret));
  } else if (OB_FAIL(vec_index_driver_ctdef->query_param_.assign(vec_search_node->get_query_param()))) {
    LOG_WARN("fail to assign query param", K(ret));
  } else {
    vec_index_driver_ctdef->vector_index_param_ = vec_search_node->get_vector_index_param();
    vec_index_driver_ctdef->algorithm_type_ = vec_search_node->get_algorithm_type();
    vec_index_driver_ctdef->vec_type_ = vec_search_node->get_vec_type();
    vec_index_driver_ctdef->row_count_ = vec_search_node->get_row_count();
    vec_index_driver_ctdef->dim_ = dim;
    if (vec_search_node->search_option_ != nullptr) {
      vec_index_driver_ctdef->filter_mode_ = vec_search_node->search_option_->filter_mode_;
    }
  }

  // 7. generate vec iter output
  ObSEArray<ObExpr *, 4> result_output;
  ObArray<ObRawExpr *> rowkey_exprs;
  if (FAILEDx(rowkey_exprs.assign(op.get_rowkey_exprs()))) {
    LOG_WARN("failed to assign rowkey exprs", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(rowkey_exprs, result_output))) {
    LOG_WARN("failed to generate main table rowkey exprs", K(ret));
  } else if (OB_FAIL(vec_index_scan_ctdef->result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret), K(result_output));
  } else if (OB_FAIL(vec_index_driver_ctdef->result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret), K(result_output));
  } else if (OB_NOT_NULL(filter_ctdef)) {
    const ObDASAttachCtDef *attach_filter_ctdef = static_cast<const ObDASAttachCtDef*>(filter_ctdef);
    if (OB_FAIL(append_array_no_dup(vec_index_driver_ctdef->result_output_, attach_filter_ctdef->result_output_))) {
      LOG_WARN("failed to append filter output exprs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    res_vec_index_driver_ctdef = vec_index_driver_ctdef;
  }

  return ret;
}

int ObHybridSearchCgService::generate_vec_aux_table_ctdef(ObLogTableScan &op,
                                                          const ObVecSearchNode::VecIndexScanParams &vec_index_scan_params,
                                                          ObDASScanCtDef *&scan_ctdef,
                                                          bool is_get)
{
  int ret = OB_SUCCESS;
  const ObTableID &index_table_id = vec_index_scan_params.table_id_;
  const ObTableID &data_table_id = op.get_ref_table_id();
  ObDASScanCtDef *aux_ctdef = nullptr;
  ObOptimizerContext *opt_ctx = nullptr;
  ObSqlSchemaGuard *schema_guard = nullptr;
  ObIAllocator &ctdef_alloc = cg_.phy_plan_->get_allocator();

  if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, ctdef_alloc, aux_ctdef))) {
    LOG_WARN("allocate delta buf table ctdef failed", K(ret));
  } else if (OB_ISNULL(aux_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aux ctdef is null", K(ret));
  } else if (OB_ISNULL(opt_ctx = cg_.opt_ctx_) ||
             OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("opt ctx or schema guard is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema_version(index_table_id, aux_ctdef->schema_version_))) {
    LOG_WARN("get table schema version failed", K(ret), K(index_table_id));
  } else {
    aux_ctdef->ref_table_id_ = index_table_id;
    aux_ctdef->ir_scan_type_ = vec_index_scan_params.scan_type_;
    aux_ctdef->is_get_ = is_get;
  }

  // 1. generate access
  if (OB_FAIL(ret)) {
  } else if (vec_index_scan_params.access_exprs_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access exprs is empty", K(ret));
  } else {
    ObArray<uint64_t> access_col_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < vec_index_scan_params.access_exprs_.count(); ++i) {
      ObRawExpr *expr = vec_index_scan_params.access_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_UNLIKELY(T_REF_COLUMN != expr->get_expr_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported expr type", K(ret), K(*expr));
      } else {
        ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(expr);
        if (OB_FAIL(access_col_ids.push_back(col_expr->get_column_id()))) {
          LOG_WARN("push back column id failed", K(ret));
        }
      }
    } // end for

    if (FAILEDx(cg_.mark_expr_self_produced(vec_index_scan_params.access_exprs_))) {
      LOG_WARN("mark expr self produced failed", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_exprs(vec_index_scan_params.access_exprs_, aux_ctdef->pd_expr_spec_.access_exprs_))) {
      LOG_WARN("generate rt exprs failed", K(ret));
    } else if (OB_FAIL(aux_ctdef->access_column_ids_.assign(access_col_ids))) {
      LOG_WARN("assign access column ids failed", K(ret));
    }
  }

  // 2. generate table param
  if (OB_SUCC(ret)) {
    int64_t route_policy = 0;
    bool is_cs_replica_query = false;
    const ObBasicSessionInfo *session_info = nullptr;
    const ObTableSchema *table_schema = nullptr;

    if (OB_ISNULL(session_info = opt_ctx->get_session_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(index_table_id, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(index_table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table schema", K(ret), K(index_table_id));
    } else {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(session_info->get_effective_tenant_id()));
      if (OB_FAIL(session_info->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
        LOG_WARN("get route policy failed", K(ret));
      } else {
        is_cs_replica_query = ObRoutePolicyType::COLUMN_STORE_ONLY == route_policy;
        aux_ctdef->table_param_.set_is_partition_table(table_schema->is_partitioned_table());
        aux_ctdef->table_param_.set_is_mlog_table(table_schema->is_mlog_table());
        aux_ctdef->table_param_.set_plan_enable_rich_format(opt_ctx->get_enable_rich_vector_format());
        aux_ctdef->table_param_.get_enable_lob_locator_v2() = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0);
      }
    }

    if (FAILEDx(aux_ctdef->table_param_.convert(*table_schema,
                                                aux_ctdef->access_column_ids_,
                                                aux_ctdef->pd_expr_spec_.pd_storage_flag_,
                                                &aux_ctdef->access_column_ids_,
                                                is_oracle_mapping_real_virtual_table(data_table_id),
                                                is_cs_replica_query))) {
      LOG_WARN("convert table param failed", K(ret), K(data_table_id), K(index_table_id));
    }
  }

  // 3. generate result output
  const ExprFixedArray &access_exprs = aux_ctdef->pd_expr_spec_.access_exprs_;
  if (FAILEDx(aux_ctdef->result_output_.init(access_exprs.count()))) {
    LOG_WARN("init result output failed", K(ret));
  } else if (OB_FAIL(append_array_no_dup(aux_ctdef->result_output_, access_exprs))) {
    LOG_WARN("append array no dup failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    scan_ctdef = aux_ctdef;
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
