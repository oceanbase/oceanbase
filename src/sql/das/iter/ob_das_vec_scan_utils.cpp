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
#include "sql/das/iter/ob_das_vec_scan_utils.h"

#define USING_LOG_PREFIX SQL_DAS

namespace oceanbase
{
namespace sql
{
int ObDasVecScanUtils::set_lookup_key(ObRowkey &rowkey, ObTableScanParam &scan_param, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObNewRange look_range;
  if (OB_FAIL(look_range.build_range(table_id, rowkey))) {
    LOG_WARN("build lookup range failed", K(ret));
  } else if (OB_FAIL(scan_param.key_ranges_.push_back(look_range))) {
    LOG_WARN("store lookup key range failed", K(ret));
  }
  return ret;
}

int ObDasVecScanUtils::set_lookup_range(const ObNewRange &look_range, ObTableScanParam &scan_param, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scan_param.key_ranges_.push_back(look_range))) {
    LOG_WARN("store lookup key range failed", K(ret));
  }
  return ret;
}

void ObDasVecScanUtils::release_scan_param(ObTableScanParam &scan_param)
{
  scan_param.destroy_schema_guard();
  scan_param.snapshot_.reset();
  scan_param.destroy();
}

void ObDasVecScanUtils::set_whole_range(ObNewRange &scan_range, common::ObTableID table_id)
{
  scan_range.table_id_ = table_id;
  scan_range.set_whole_range();
}

int ObDasVecScanUtils::get_distance_expr_type(ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              ObExprVectorDistance::ObVecDisType &dis_type)
{
  int ret = OB_SUCCESS;

  switch (expr.type_) {
    case T_FUN_SYS_L2_DISTANCE:
      dis_type = ObExprVectorDistance::ObVecDisType::EUCLIDEAN;
      break;
    case T_FUN_SYS_INNER_PRODUCT:
      dis_type = ObExprVectorDistance::ObVecDisType::DOT;
      break;
    case T_FUN_SYS_NEGATIVE_INNER_PRODUCT:
      dis_type = ObExprVectorDistance::ObVecDisType::DOT;
      break;
    case T_FUN_SYS_COSINE_DISTANCE:
      dis_type = ObExprVectorDistance::ObVecDisType::COSINE;
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support vector sort expr", K(ret), K(expr.type_));
      break;
  }

  if (OB_SUCC(ret) && ObExprVectorDistance::distance_funcs[dis_type] == nullptr) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support dis_type", K(ret), K(dis_type));
  }

  return ret;
}

int ObDasVecScanUtils::get_real_search_vec(common::ObArenaAllocator &allocator,
                                           ObDASSortRtDef *sort_rtdef,
                                           ObExpr *origin_vec,
                                           ObString &real_search_vec)
{
  int ret = OB_SUCCESS;

  ObDatum *search_vec_datum = NULL;
  if (OB_ISNULL(sort_rtdef) || OB_ISNULL(origin_vec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), K(sort_rtdef), K(origin_vec));
  } else if (OB_FAIL(origin_vec->eval(*(sort_rtdef->eval_ctx_), search_vec_datum))) {
    LOG_WARN("eval vec arg failed", K(ret));
  } else if (OB_FALSE_IT(real_search_vec = search_vec_datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator,
                                                               ObLongTextType,
                                                               CS_TYPE_BINARY,
                                                               origin_vec->obj_meta_.has_lob_header(),
                                                               real_search_vec))) {
    LOG_WARN("failed to get real data.", K(ret));
  }

  return ret;
}

int ObDasVecScanUtils::init_limit(const ObDASVecAuxScanCtDef *ir_ctdef,
                                  ObDASVecAuxScanRtDef *ir_rtdef,
                                  const ObDASSortCtDef *sort_ctdef,
                                  ObDASSortRtDef *sort_rtdef,
                                  common::ObLimitParam &limit_param)
{
  int ret = OB_SUCCESS;
  ObDASScanRtDef *base_rtdef = nullptr;
  if (OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef) || OB_ISNULL(sort_ctdef) || OB_ISNULL(sort_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), K(ir_ctdef), K(ir_rtdef), K(sort_ctdef), K(sort_rtdef));
  } else if (ObDASOpType::DAS_OP_TABLE_SCAN == ir_rtdef->get_inv_idx_scan_rtdef()->op_type_) {
    base_rtdef = static_cast<ObDASScanRtDef *>(ir_rtdef->get_inv_idx_scan_rtdef());
  } else if (ObDASOpType::DAS_OP_SORT == ir_rtdef->get_inv_idx_scan_rtdef()->op_type_) {
    ObDASSortRtDef *sort_rtdef = static_cast<ObDASSortRtDef *>(ir_rtdef->get_inv_idx_scan_rtdef());
    if (ObDASOpType::DAS_OP_TABLE_SCAN == sort_rtdef->children_[0]->op_type_) {
      base_rtdef = static_cast<ObDASScanRtDef *>(sort_rtdef->children_[0]);
    }
  }

  if (OB_ISNULL(base_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null base rtdef", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
  } else if (nullptr != sort_ctdef && nullptr != sort_rtdef) {
    // try init top-k limits
    bool is_null = false;
    if (OB_UNLIKELY((nullptr != sort_ctdef->limit_expr_ || nullptr != sort_ctdef->offset_expr_) &&
                    base_rtdef->limit_param_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected top k limit with table scan limit pushdown", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
    } else if (nullptr != sort_ctdef->limit_expr_) {
      ObDatum *limit_datum = nullptr;
      if (OB_FAIL(sort_ctdef->limit_expr_->eval(*sort_rtdef->eval_ctx_, limit_datum))) {
        LOG_WARN("failed to eval limit expr", K(ret));
      } else if (limit_datum->is_null()) {
        is_null = true;
        limit_param.limit_ = 0;
      } else {
        limit_param.limit_ = limit_datum->get_int() < 0 ? 0 : limit_datum->get_int();
      }
    }

    if (OB_SUCC(ret) && !is_null && nullptr != sort_ctdef->offset_expr_) {
      ObDatum *offset_datum = nullptr;
      if (OB_FAIL(sort_ctdef->offset_expr_->eval(*sort_rtdef->eval_ctx_, offset_datum))) {
        LOG_WARN("failed to eval offset expr", K(ret));
      } else if (offset_datum->is_null()) {
        limit_param.offset_ = 0;
      } else {
        limit_param.offset_ = offset_datum->get_int() < 0 ? 0 : offset_datum->get_int();
      }
    }
  } else {
    // init with table scan pushdown limit
    limit_param = base_rtdef->limit_param_;
  }

  if (OB_SUCC(ret) && false ) {
    const ObDASBaseRtDef *index_rtdef = ir_rtdef->get_inv_idx_scan_rtdef();
    int64_t limit = limit_param.limit_;

    if (limit > 0 && OB_NOT_NULL(index_rtdef->table_loc_)) {
      ObDASTableLoc *table_loc = index_rtdef->table_loc_;
      uint64_t partiton = table_loc->get_tablet_locs().size();
      if (partiton > 1) {
        limit_param.limit_ = 2 * ((limit / partiton) + 1 );
      }
    }
  }

  return ret;
}

int ObDasVecScanUtils::init_sort(const ObDASVecAuxScanCtDef *ir_ctdef,
                                 ObDASVecAuxScanRtDef *ir_rtdef,
                                 const ObDASSortCtDef *sort_ctdef,
                                 ObDASSortRtDef *sort_rtdef,
                                 const common::ObLimitParam &limit_param,
                                 ObExpr *&search_vec,
                                 ObExpr *&distance_calc)
{
  int ret = OB_SUCCESS;
  const int64_t top_k_cnt = limit_param.is_valid() ? (limit_param.limit_ + limit_param.offset_) : INT64_MAX;
  if (OB_ISNULL(sort_ctdef) || OB_ISNULL(sort_rtdef) || OB_ISNULL(ir_ctdef) || OB_ISNULL(sort_rtdef->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sort def", K(ret), KPC(sort_rtdef), KPC(sort_ctdef), KPC(ir_ctdef));
  } else {
    for (int i = 0; i < sort_ctdef->sort_exprs_.count() && OB_SUCC(ret) && OB_ISNULL(search_vec); ++i) {
      ObExpr *expr = sort_ctdef->sort_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr ptr", K(ret));
      } else if (expr->is_vector_sort_expr()) {
        distance_calc = expr;
        if (expr->arg_cnt_ != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected arg num", K(ret), K(expr->arg_cnt_));
        } else if (expr->args_[0]->is_const_expr()) {
          search_vec = expr->args_[0];
        } else if (expr->args_[1]->is_const_expr()) {
          search_vec = expr->args_[1];
        }
      }
    }
  }
  return ret;
}

int ObDasVecScanUtils::reuse_iter(const share::ObLSID &ls_id,
                                  ObDASScanIter *iter,
                                  ObTableScanParam &scan_param,
                                  const ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;

  const ObTabletID &scan_tablet_id = scan_param.tablet_id_;
  scan_param.need_switch_param_ =
      scan_param.need_switch_param_ || (scan_tablet_id.is_valid() && (tablet_id != scan_tablet_id));
  scan_param.tablet_id_ = tablet_id;
  scan_param.ls_id_ = ls_id;

  if (OB_NOT_NULL(iter) && OB_FAIL(iter->reuse())) {
    LOG_WARN("reuse iter failed", K(ret));
  }

  return ret;
}

int ObDasVecScanUtils::init_scan_param(const share::ObLSID &ls_id,
                                       const common::ObTabletID &tablet_id,
                                       const ObDASScanCtDef *ctdef,
                                       ObDASScanRtDef *rtdef,
                                       transaction::ObTxDesc *tx_desc,
                                       transaction::ObTxReadSnapshot *snapshot,
                                       ObTableScanParam &scan_param,
                                       bool is_get)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctdef), K(rtdef));
  } else {
    scan_param.tenant_id_ = MTL_ID();
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = is_get;
    scan_param.is_for_foreign_check_ = rtdef->is_for_foreign_check_;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &rtdef->scan_allocator_;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.force_refresh_lc_ = rtdef->force_refresh_lc_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.ext_file_column_exprs_ = &(ctdef->pd_expr_spec_.ext_file_column_exprs_);
    scan_param.ext_column_convert_exprs_ = &(ctdef->pd_expr_spec_.ext_column_convert_exprs_);
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.limit_param_ = rtdef->limit_param_;
    scan_param.need_scn_ = rtdef->need_scn_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.fb_snapshot_ = rtdef->fb_snapshot_;
    scan_param.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    if (rtdef->is_for_foreign_check_) {
      scan_param.trans_desc_ = tx_desc;
    }
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    if (rtdef->sample_info_ != nullptr) {
      scan_param.sample_info_ = *rtdef->sample_info_;
    }
    if (OB_NOT_NULL(snapshot)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("snapshot is null", K(ret));
    }
    if (OB_NOT_NULL(tx_desc)) {
      scan_param.tx_id_ = tx_desc->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("init column ids failed", K(ret));
    }
    // external table scan params
    if (OB_SUCC(ret) && ctdef->is_external_table_) {
      scan_param.external_file_access_info_ = ctdef->external_file_access_info_.str_;
      scan_param.external_file_location_ = ctdef->external_file_location_.str_;
      if (OB_FAIL(scan_param.external_file_format_.load_from_string(ctdef->external_file_format_str_.str_,
                                                                    *scan_param.allocator_))) {
        LOG_WARN("fail to load from string", K(ret));
      } else {
        uint64_t max_idx = 0;
        for (int i = 0; i < scan_param.ext_file_column_exprs_->count(); i++) {
          max_idx = std::max(max_idx, scan_param.ext_file_column_exprs_->at(i)->extra_);
        }
        scan_param.external_file_format_.csv_format_.file_column_nums_ = static_cast<int64_t>(max_idx);
      }
    }
  }
  return ret;
}

int ObDasVecScanUtils::init_vec_aux_scan_param(const share::ObLSID &ls_id,
                                               const common::ObTabletID &tablet_id,
                                               const sql::ObDASScanCtDef *ctdef,
                                               sql::ObDASScanRtDef *rtdef,
                                               transaction::ObTxDesc *tx_desc,
                                               transaction::ObTxReadSnapshot *snapshot,
                                               ObTableScanParam &scan_param,
                                               bool is_get)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(
          ObDasVecScanUtils::init_scan_param(ls_id, tablet_id, ctdef, rtdef, tx_desc, snapshot, scan_param, is_get))) {
    LOG_WARN("failed to generate init vec aux scan param", K(ret));
  } else {
    scan_param.is_for_foreign_check_ = false;
    scan_param.op_ = nullptr;
    scan_param.output_exprs_ = nullptr;
    scan_param.aggregate_exprs_ = nullptr;
    scan_param.row2exprs_projector_ = nullptr;
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
