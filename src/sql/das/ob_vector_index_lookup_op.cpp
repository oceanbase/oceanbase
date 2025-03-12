/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/ob_das_vec_define.h"
#include "sql/das/ob_vector_index_lookup_op.h"
#include "sql/das/ob_das_utils.h"
#include "src/sql/engine/expr/ob_expr_lob_utils.h"
#include "src/storage/access/ob_table_scan_iterator.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace transaction;
using namespace share;

namespace sql
{

int ObVectorIndexLookupOp::init(const ObDASBaseCtDef *table_lookup_ctdef,
                                ObDASBaseRtDef *table_lookup_rtdef,
                                transaction::ObTxDesc *tx_desc,
                                transaction::ObTxReadSnapshot *snapshot,
                                storage::ObTableScanParam &scan_param)
{
  INIT_SUCC(ret);
  const ObDASTableLookupCtDef *tbl_lookup_ctdef = nullptr;
  ObDASTableLookupRtDef *tbl_lookup_rtdef = nullptr;
  const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
  ObDASIRAuxLookupRtDef *aux_lookup_rtdef = nullptr;
  const ObDASVecAuxScanCtDef *vir_scan_ctdef = nullptr;
  ObDASVecAuxScanRtDef *vir_scan_rtdef = nullptr;
  if (OB_ISNULL(table_lookup_ctdef) || OB_ISNULL(table_lookup_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table lookup param is nullptr", KP(table_lookup_ctdef), KP(table_lookup_rtdef));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(table_lookup_ctdef,
                                                     table_lookup_rtdef,
                                                     DAS_OP_TABLE_LOOKUP,
                                                     tbl_lookup_ctdef,
                                                     tbl_lookup_rtdef))) {
    LOG_WARN("find data table lookup def failed", K(ret));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(tbl_lookup_ctdef,
                                                     tbl_lookup_rtdef,
                                                     DAS_OP_IR_AUX_LOOKUP,
                                                     aux_lookup_ctdef,
                                                     aux_lookup_rtdef))) {
    LOG_WARN("find ir aux lookup def failed", K(ret));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(aux_lookup_ctdef,
                                                     aux_lookup_rtdef,
                                                     DAS_OP_VEC_SCAN,
                                                     vir_scan_ctdef,
                                                     vir_scan_rtdef))) {
    LOG_WARN("find ir scan def failed", K(ret));
  } else {
    if (OB_FAIL(ObDomainIndexLookupOp::init(tbl_lookup_ctdef->get_lookup_scan_ctdef(),
                                            tbl_lookup_rtdef->get_lookup_scan_rtdef(),
                                            vir_scan_ctdef->get_inv_idx_scan_ctdef(),
                                            vir_scan_rtdef->get_inv_idx_scan_rtdef(),
                                            aux_lookup_ctdef->get_lookup_scan_ctdef(),
                                            aux_lookup_rtdef->get_lookup_scan_rtdef(),
                                            tx_desc,
                                            snapshot,
                                            scan_param))) {
      LOG_WARN("failed to init domain index lookup op", K(ret));
    } else if (OB_ISNULL(doc_id_lookup_rtdef_) || OB_ISNULL(lookup_rtdef_)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("lookup rtdef is nullptr", KP(doc_id_lookup_rtdef_), KP(lookup_rtdef_));
    } else {
      need_scan_aux_ = true;
      doc_id_lookup_ctdef_ = aux_lookup_ctdef->get_lookup_scan_ctdef();
      doc_id_lookup_rtdef_ = aux_lookup_rtdef->get_lookup_scan_rtdef();
      doc_id_expr_ = vir_scan_ctdef->inv_scan_vec_id_col_;
      vec_eval_ctx_ = vir_scan_rtdef->eval_ctx_;
      delta_buf_ctdef_ = vir_scan_ctdef->get_delta_tbl_ctdef();
      delta_buf_rtdef_ = vir_scan_rtdef->get_delta_tbl_rtdef();
      index_id_ctdef_ = vir_scan_ctdef->get_index_id_tbl_ctdef();
      index_id_rtdef_ = vir_scan_rtdef->get_index_id_tbl_rtdef();
      snapshot_ctdef_ = vir_scan_ctdef->get_snapshot_tbl_ctdef();
      snapshot_rtdef_ = vir_scan_rtdef->get_snapshot_tbl_rtdef();
      com_aux_vec_ctdef_ = vir_scan_ctdef->get_com_aux_tbl_ctdef();
      com_aux_vec_rtdef_ = vir_scan_rtdef->get_com_aux_tbl_rtdef();
      set_dim(vir_scan_ctdef->dim_);
      doc_id_lookup_rtdef_->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
      lookup_rtdef_->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
      if (DAS_OP_SORT == aux_lookup_ctdef->get_doc_id_scan_ctdef()->op_type_) {
        sort_ctdef_ = static_cast<const ObDASSortCtDef *>(aux_lookup_ctdef->get_doc_id_scan_ctdef());
        sort_rtdef_ = static_cast<ObDASSortRtDef *>(aux_lookup_rtdef->get_doc_id_scan_rtdef());
        if (OB_FAIL(init_limit(vir_scan_ctdef, vir_scan_rtdef))) {
          LOG_WARN("failed to init limit", K(ret), KPC(vir_scan_ctdef), KPC(vir_scan_rtdef));
        } else if (nullptr != sort_ctdef_ && OB_FAIL(init_sort(vir_scan_ctdef, vir_scan_rtdef))) {
          LOG_WARN("failed to init sort", K(ret), KPC(vir_scan_ctdef), KPC(vir_scan_rtdef));
        } else if (OB_FAIL(set_vec_index_param(vir_scan_ctdef->vec_index_param_))) {
          LOG_WARN("failed to set vec index param", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::init_limit(const ObDASVecAuxScanCtDef *ir_ctdef,
                                      ObDASVecAuxScanRtDef *ir_rtdef)
{
  int ret = OB_SUCCESS;
  if (nullptr != sort_ctdef_ && nullptr != sort_rtdef_) {
    // try init top-k limits
    bool is_null = false;
    if (OB_UNLIKELY((nullptr != sort_ctdef_->limit_expr_ || nullptr != sort_ctdef_->offset_expr_)
        && ir_rtdef->get_inv_idx_scan_rtdef()->limit_param_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected top k limit with table scan limit pushdown", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
    } else if (nullptr != sort_ctdef_->limit_expr_) {
      ObDatum *limit_datum = nullptr;
      if (OB_FAIL(sort_ctdef_->limit_expr_->eval(*sort_rtdef_->eval_ctx_, limit_datum))) {
        LOG_WARN("failed to eval limit expr", K(ret));
      } else if (limit_datum->is_null()) {
        is_null = true;
        limit_param_.limit_ = 0;
      } else {
        limit_param_.limit_ = limit_datum->get_int() < 0 ? 0 : limit_datum->get_int();
      }
    }

    if (OB_SUCC(ret) && !is_null && nullptr != sort_ctdef_->offset_expr_) {
      ObDatum *offset_datum = nullptr;
      if (OB_FAIL(sort_ctdef_->offset_expr_->eval(*sort_rtdef_->eval_ctx_, offset_datum))) {
        LOG_WARN("failed to eval offset expr", K(ret));
      } else if (offset_datum->is_null()) {
        limit_param_.offset_ = 0;
      } else {
        limit_param_.offset_ = offset_datum->get_int() < 0 ? 0 : offset_datum->get_int();
      }
    }
  } else {
    // init with table scan pushdown limit
    limit_param_ = ir_rtdef->get_inv_idx_scan_rtdef()->limit_param_;
  }

  if (OB_SUCC(ret)) {
    if (limit_param_.offset_ + limit_param_.limit_ > MAX_VSAG_QUERY_RES_SIZE) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_WARN(OB_NOT_SUPPORTED, "query size (limit + offset) is more than 16384");
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::init_sort(const ObDASVecAuxScanCtDef *ir_ctdef,
                                    ObDASVecAuxScanRtDef *ir_rtdef)
{
  int ret = OB_SUCCESS;
  const int64_t top_k_cnt = limit_param_.is_valid() ? (limit_param_.limit_ + limit_param_.offset_) : INT64_MAX;
  if (OB_ISNULL(sort_ctdef_) || OB_ISNULL(sort_rtdef_) || OB_ISNULL(ir_ctdef) || OB_ISNULL(sort_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sort def", K(ret), KPC(sort_rtdef_), KPC(sort_ctdef_), KPC(ir_ctdef));
  } else {
    for (int i = 0; i < sort_ctdef_->sort_exprs_.count() && OB_SUCC(ret) && OB_ISNULL(search_vec_); ++i) {
      ObExpr *expr = sort_ctdef_->sort_exprs_.at(i);
      if (expr->is_vector_sort_expr()) {
        if (expr->arg_cnt_ != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected arg num", K(ret), K(expr->arg_cnt_));
        } else if (expr->args_[0]->is_const_expr()) {
          search_vec_ = expr->args_[0];
        } else if (expr->args_[1]->is_const_expr()) {
          search_vec_ = expr->args_[1];
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::init_base_idx_scan_param(const share::ObLSID &ls_id,
                                                    const common::ObTabletID &tablet_id,
                                                    const sql::ObDASScanCtDef *ctdef,
                                                    sql::ObDASScanRtDef *rtdef,
                                                    transaction::ObTxDesc *tx_desc,
                                                    transaction::ObTxReadSnapshot *snapshot,
                                                    ObTableScanParam &scan_param,
                                                    bool reverse_order)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(ctdef), KPC(rtdef), K(ls_id), K(tablet_id));
  } else {
    uint64_t tenant_id = MTL_ID();
    scan_param.tenant_id_ = tenant_id;
    scan_param.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
    scan_param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = false; // scan
    scan_param.is_for_foreign_check_ = false;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &rtdef->scan_allocator_;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.force_refresh_lc_ = rtdef->force_refresh_lc_;
    scan_param.output_exprs_ = nullptr;
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = nullptr;
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = nullptr;
    scan_param.row2exprs_projector_ = nullptr;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.limit_param_ = rtdef->limit_param_;
    scan_param.need_scn_ = rtdef->need_scn_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.fb_snapshot_ = rtdef->fb_snapshot_;
    scan_param.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    ObQueryFlag query_flag(ObQueryFlag::Forward, // scan_order
                          false, // daily_merge
                          false, // optimize
                          false, // sys scan
                          true, // full_row
                          false, // index_back
                          false, // query_stat
                          ObQueryFlag::MysqlMode, // sql_mode
                          false // read_latest
                        );
    query_flag.scan_order_ = reverse_order ? ObQueryFlag::Reverse : ObQueryFlag::Forward;
    scan_param.scan_flag_.flag_ = query_flag.flag_;
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_NOT_NULL(tx_desc)) {
      scan_param.tx_id_ = tx_desc->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }

    if (OB_NOT_NULL(snapshot)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null snapshot", K(ret), KP(snapshot));
    }

    if (FAILEDx(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to init column ids", K(ret));
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::gen_scan_range(const int64_t col_cnt, common::ObTableID table_id, ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  scan_range.table_id_ = table_id;
  scan_range.set_whole_range();
  return ret;
}

int ObVectorIndexLookupOp::init_delta_buffer_scan_param()
{
  int ret = OB_SUCCESS;
  ObNewRange scan_range;
  if (OB_FAIL(init_base_idx_scan_param(ls_id_, delta_buf_tablet_id_, delta_buf_ctdef_,
                                      delta_buf_rtdef_,tx_desc_, snapshot_,
                                      delta_buf_scan_param_, true))) {
    LOG_WARN("failed to generate init delta buffer scan param", K(ret));
  } else if (OB_FAIL(gen_scan_range(DELTA_BUF_PRI_KEY_CNT, delta_buf_ctdef_->ref_table_id_, scan_range))) {
    LOG_WARN("failed to generate init delta buffer scan range", K(ret));
  } else if (OB_FAIL(delta_buf_scan_param_.key_ranges_.push_back(scan_range))) {
    LOG_WARN("failed to append scan range", K(ret));
  }
  return ret;
}

int ObVectorIndexLookupOp::init_index_id_scan_param()
{
  int ret = OB_SUCCESS;
  ObNewRange scan_range;
  if (OB_FAIL(init_base_idx_scan_param(ls_id_, index_id_tablet_id_, index_id_ctdef_,
                                      index_id_rtdef_,tx_desc_, snapshot_,
                                      index_id_scan_param_, true))) {
    LOG_WARN("failed to generate init delta buffer scan param", K(ret));
  } else if (OB_FAIL(gen_scan_range(INDEX_ID_PRI_KEY_CNT, index_id_ctdef_->ref_table_id_, scan_range))) {
    LOG_WARN("failed to generate init delta buffer scan range", K(ret));
  } else if (OB_FAIL(index_id_scan_param_.key_ranges_.push_back(scan_range))) {
    LOG_WARN("failed to append scan range", K(ret));
  }
  return ret;
}

int ObVectorIndexLookupOp::init_snapshot_scan_param()
{
  int ret = OB_SUCCESS;
  ObNewRange scan_range;
  if (OB_FAIL(init_base_idx_scan_param(ls_id_, snapshot_tablet_id_, snapshot_ctdef_,
                                      snapshot_rtdef_,tx_desc_, snapshot_,
                                      snapshot_scan_param_))) {
    LOG_WARN("failed to generate init delta buffer scan param", K(ret));
  } else if (OB_FAIL(gen_scan_range(SNAPSHOT_PRI_KEY_CNT, snapshot_ctdef_->ref_table_id_, scan_range))) {
    LOG_WARN("failed to generate init delta buffer scan range", K(ret));
  } else if (OB_FAIL(snapshot_scan_param_.key_ranges_.push_back(scan_range))) {
    LOG_WARN("failed to append scan range", K(ret));
  }
  return ret;
}

int ObVectorIndexLookupOp::init_com_aux_vec_scan_param()
{
  INIT_SUCC(ret);
  ObNewRange scan_range;
  int64_t rowkey_cnt = doc_id_lookup_ctdef_->result_output_.count();
  if (nullptr != doc_id_lookup_ctdef_->trans_info_expr_) {
    rowkey_cnt = rowkey_cnt - 1;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_base_idx_scan_param(ls_id_, com_aux_vec_tablet_id_, com_aux_vec_ctdef_,
                                      com_aux_vec_rtdef_, tx_desc_, snapshot_,
                                      com_aux_vec_scan_param_))) {
    LOG_WARN("failed to generate init vid rowkey scan param", K(ret));
  }
  return ret;
}

int ObVectorIndexLookupOp::reset_lookup_state()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDomainIndexLookupOp::reset_lookup_state())) {
    LOG_WARN("failed to reset lookup state for domain index lookup op", K(ret));
  } else {
    if (nullptr != lookup_iter_) {
      doc_id_scan_param_.key_ranges_.reuse();
      doc_id_scan_param_.ss_key_ranges_.reuse();
    }
    if (nullptr != delta_buf_iter_) {
      delta_buf_scan_param_.key_ranges_.reuse();
      delta_buf_scan_param_.ss_key_ranges_.reuse();
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::fetch_index_table_rowkey()
{
  int ret = OB_SUCCESS;
  ObNewRow *row = nullptr;
  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(adaptor_vid_iter_)) {
    if (OB_FAIL(process_adaptor_state())) {
      LOG_WARN("failed to process_adaptor_state", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(adaptor_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get adaptor_vid_iter", K(ret));
  } else if (OB_FAIL(adaptor_vid_iter_->get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next next row from text retrieval iter", K(ret));
    }
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be null", K(ret));
  } else if (row->get_count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be one row", K(row->get_count()), K(ret));
  } else if (OB_FALSE_IT(doc_id_key_obj_ = row->get_cell(0))) {
  } else if (OB_FAIL(set_lookup_vid_key())) {
    LOG_WARN("failed to set lookup vid query key", K(ret));
  }
  return ret;
}

int ObVectorIndexLookupOp::fetch_index_table_rowkeys(int64_t &count, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObNewRow *row = nullptr;
  int64_t index_scan_row_cnt = 0;
  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(adaptor_vid_iter_)) {
    if (OB_FAIL(process_adaptor_state())) {
      LOG_WARN("failed to process_adaptor_state", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(adaptor_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get adaptor_vid_iter", K(ret));
  } else if (OB_FALSE_IT(adaptor_vid_iter_->set_batch_size(capacity))) {
  } else if (OB_FAIL(adaptor_vid_iter_->get_next_rows(row, index_scan_row_cnt))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next next row from text retrieval iter", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) && index_scan_row_cnt > 0) {
    if (OB_FAIL(set_lookup_vid_keys(row, index_scan_row_cnt))) {
      LOG_WARN("failed to set lookup vid query key", K(ret));
    } else {
      count += index_scan_row_cnt;
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::set_lookup_vid_key()
{
  int ret = OB_SUCCESS;
  ObNewRange doc_id_range;
  ObRowkey doc_id_rowkey(&doc_id_key_obj_, 1);
  doc_id_scan_param_.key_ranges_.reuse();
  uint64_t ref_table_id = doc_id_lookup_ctdef_->ref_table_id_;
  if (OB_FAIL(doc_id_range.build_range(ref_table_id, doc_id_rowkey))) {
    LOG_WARN("build vid lookup range failed", K(ret));
  } else if (OB_FAIL(doc_id_scan_param_.key_ranges_.push_back(doc_id_range))) {
    LOG_WARN("store lookup key range failed", K(ret));
  } else {
    LOG_DEBUG("generate vid scan range", K(ret), K(doc_id_range));
  }
  return ret;
}

int ObVectorIndexLookupOp::reuse_scan_iter(bool need_switch_param)
{
  int ret = OB_SUCCESS;

  reset_lookup_state();
  ObITabletScan &tsc_service = get_tsc_service();
  doc_id_scan_param_.need_switch_param_ = need_switch_param;
  if (OB_NOT_NULL(adaptor_vid_iter_)) { // maybe only reset vid iter when need need_switch_param ?
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
  if (OB_NOT_NULL(delta_buf_rtdef_)) {
    const ObTabletID &scan_tablet_id = delta_buf_scan_param_.tablet_id_;
    delta_buf_scan_param_.need_switch_param_ = scan_tablet_id.is_valid() && scan_tablet_id != delta_buf_tablet_id_;
    if (OB_FAIL(tsc_service.reuse_scan_iter(delta_buf_scan_param_.need_switch_param_, delta_buf_iter_))) {
      LOG_WARN("failed to reuse scan iter", K(ret));
    } else if (nullptr != rowkey_iter_) {
      delta_buf_scan_param_.key_ranges_.reuse();
      delta_buf_scan_param_.ss_key_ranges_.reuse();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(index_id_rtdef_)) {
    const ObTabletID &scan_tablet_id = index_id_scan_param_.tablet_id_;
    index_id_scan_param_.need_switch_param_ = scan_tablet_id.is_valid() && scan_tablet_id != index_id_tablet_id_;
    if (OB_FAIL(tsc_service.reuse_scan_iter(index_id_scan_param_.need_switch_param_, index_id_iter_))) {
      LOG_WARN("failed to reuse scan iter", K(ret));
    } else if (nullptr != rowkey_iter_) {
      index_id_scan_param_.key_ranges_.reuse();
      index_id_scan_param_.ss_key_ranges_.reuse();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(snapshot_rtdef_)) {
    const ObTabletID &scan_tablet_id = snapshot_scan_param_.tablet_id_;
    snapshot_scan_param_.need_switch_param_ = scan_tablet_id.is_valid() && scan_tablet_id != snapshot_tablet_id_;
    if (OB_FAIL(tsc_service.reuse_scan_iter(snapshot_scan_param_.need_switch_param_, snapshot_iter_))) {
      LOG_WARN("failed to reuse scan iter", K(ret));
    } else if (nullptr != rowkey_iter_) {
      snapshot_scan_param_.key_ranges_.reuse();
      snapshot_scan_param_.ss_key_ranges_.reuse();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(com_aux_vec_rtdef_)) {
    const ObTabletID &scan_tablet_id = com_aux_vec_scan_param_.tablet_id_;
    com_aux_vec_scan_param_.need_switch_param_ = scan_tablet_id.is_valid() && scan_tablet_id != com_aux_vec_tablet_id_;
    if (OB_FAIL(tsc_service.reuse_scan_iter(com_aux_vec_scan_param_.need_switch_param_, com_aux_vec_iter_))) {
      LOG_WARN("failed to reuse scan iter", K(ret));
    } else if (nullptr != rowkey_iter_) {
      com_aux_vec_scan_param_.key_ranges_.reuse();
      com_aux_vec_scan_param_.ss_key_ranges_.reuse();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDomainIndexLookupOp::reuse_scan_iter())) {
    LOG_WARN("failed to reuse scan iter", K(ret));
  } else if (OB_FAIL(tsc_service.reuse_scan_iter(doc_id_scan_param_.need_switch_param_, rowkey_iter_))) {
    LOG_WARN("failed to reuse scan iter", K(ret));
  }
  vec_op_alloc_.reset();
  return ret;
}

int ObVectorIndexLookupOp::set_lookup_vid_key(ObRowkey& doc_id_rowkey)
{
  int ret = OB_SUCCESS;
  ObNewRange doc_id_range;
  uint64_t ref_table_id = doc_id_lookup_ctdef_->ref_table_id_;
  if (OB_FAIL(doc_id_range.build_range(ref_table_id, doc_id_rowkey))) {
    LOG_WARN("build doc id lookup range failed", K(ret));
  } else if (OB_FAIL(doc_id_scan_param_.key_ranges_.push_back(doc_id_range))) {
    LOG_WARN("store lookup key range failed", K(ret));
  } else {
    LOG_DEBUG("generate doc id scan range", K(ret), K(doc_id_range));
  }
  return ret;
}

int ObVectorIndexLookupOp::set_lookup_vid_keys(ObNewRow *row, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row) || row->get_count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error row key", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*vec_eval_ctx_);
    batch_info_guard.set_batch_size(size);
    doc_id_scan_param_.key_ranges_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      batch_info_guard.set_batch_idx(i);
      ObRowkey doc_id_rowkey(&(row->get_cell(i)), 1);
      if (OB_FAIL(set_lookup_vid_key(doc_id_rowkey))) {
        LOG_WARN("failed to set lookup vid key", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::get_aux_table_rowkeys(const int64_t lookup_row_cnt)
{
  int ret = OB_SUCCESS;
  doc_id_lookup_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
  int64_t rowkey_cnt = 0;
  if (index_end_ && doc_id_scan_param_.key_ranges_.empty()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(do_aux_table_lookup())) {
    LOG_WARN("failed to do aux table lookup", K(ret));
  } else if (OB_FAIL(rowkey_iter_->get_next_rows(rowkey_cnt, lookup_row_cnt))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  if (OB_FAIL(ret) && ret != OB_ITER_END) {
  } else if (rowkey_cnt > 0) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*doc_id_lookup_rtdef_->eval_ctx_);
    batch_info_guard.set_batch_size(rowkey_cnt);
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
      batch_info_guard.set_batch_idx(i);
      if (OB_FAIL(set_main_table_lookup_key())) {
        LOG_WARN("failed to set main table lookup key", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::revert_iter_for_complete_data()
{
  INIT_SUCC(ret);

  ObITabletScan &tsc_service = get_tsc_service();
  if (OB_FAIL(tsc_service.revert_scan_iter(com_aux_vec_iter_))) {
    LOG_WARN("revert scan iterator failed", K(ret));
  }

  com_aux_vec_scan_param_.key_ranges_.reuse();
  com_aux_vec_scan_param_.ss_key_ranges_.reuse();
  doc_id_scan_param_.key_ranges_.reuse();
  doc_id_scan_param_.ss_key_ranges_.reuse();
  com_aux_vec_iter_ = NULL;
  com_aux_vec_scan_param_.destroy_schema_guard();
  com_aux_vec_scan_param_.~ObTableScanParam();
  //trans_info_array_.destroy();

  return ret;
}

int ObVectorIndexLookupOp::vector_do_index_lookup()
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  ObNewRowIterator *&storage_iter = com_aux_vec_iter_;
  if (com_aux_vec_scan_param_.key_ranges_.empty()) {
    //do nothing
  } else if (storage_iter == nullptr) {
    //first index lookup, init scan param and do table scan
    if (OB_FAIL(init_com_aux_vec_scan_param())) {
      LOG_WARN("init scan param failed", K(ret));
    } else if (OB_FAIL(tsc_service.table_scan(com_aux_vec_scan_param_,
                                         storage_iter))) {
      if (OB_SNAPSHOT_DISCARDED == ret && com_aux_vec_scan_param_.fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to scan table", K(com_aux_vec_scan_param_), K(ret));
      }
    }
  } else {
    const ObTabletID &storage_tablet_id = com_aux_vec_scan_param_.tablet_id_;
    com_aux_vec_scan_param_.need_switch_param_ =
                            (storage_tablet_id.is_valid() &&
                             storage_tablet_id != com_aux_vec_tablet_id_ ?
                             true : false);
    com_aux_vec_scan_param_.tablet_id_ = tablet_id_;
    com_aux_vec_scan_param_.ls_id_ = ls_id_;
    ObITabletScan &tsc_service = get_tsc_service();
    if (OB_FAIL(tsc_service.reuse_scan_iter(com_aux_vec_scan_param_.need_switch_param_, com_aux_vec_iter_))) {
      LOG_WARN("failed to reuse iter", K(ret));
    } else if (OB_FAIL(tsc_service.table_rescan(com_aux_vec_scan_param_, storage_iter))) {
      LOG_WARN("table_rescan scan iter failed", K(ret));
    }
  }
  return ret;
}

void ObVectorIndexLookupOp::reuse_scan_param_complete_data()
{
  doc_id_scan_param_.key_ranges_.reuse();
  doc_id_scan_param_.ss_key_ranges_.reuse();
  com_aux_vec_scan_param_.key_ranges_.reuse();
  com_aux_vec_scan_param_.ss_key_ranges_.reuse();
}

int ObVectorIndexLookupOp::prepare_state(const ObVidAdaLookupStatus& cur_state,
                                         ObVectorQueryAdaptorResultContext &ada_ctx)
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  switch(cur_state) {
    case ObVidAdaLookupStatus::STATES_INIT: {
      if (nullptr == delta_buf_iter_) {
        delta_buf_scan_param_.need_switch_param_ = false;
        // init doc_id -> rowkey table iterator as rowkey iter
        if (OB_FAIL(init_delta_buffer_scan_param())) {
          LOG_WARN("failed to init delta buf table lookup scan param", K(ret));
        } else if (OB_FAIL(tsc_service.table_scan(delta_buf_scan_param_, delta_buf_iter_))) {
          if (OB_SNAPSHOT_DISCARDED == ret && delta_buf_scan_param_.fb_snapshot_.is_valid()) {
            ret = OB_INVALID_QUERY_TIMESTAMP;
          } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("fail to scan table", K(delta_buf_scan_param_), K(ret));
          }
        }
      } else {
        const ObTabletID &scan_tablet_id = delta_buf_scan_param_.tablet_id_;
        delta_buf_scan_param_.need_switch_param_ = scan_tablet_id.is_valid() && (delta_buf_tablet_id_ != scan_tablet_id);
        delta_buf_scan_param_.tablet_id_ = delta_buf_tablet_id_;
        delta_buf_scan_param_.ls_id_ = ls_id_;
        if (OB_FAIL(tsc_service.reuse_scan_iter(delta_buf_scan_param_.need_switch_param_, delta_buf_iter_))) {
          LOG_WARN("failed to reuse delta buf table scan iterator", K(ret));
        } else if (OB_FAIL(tsc_service.table_rescan(delta_buf_scan_param_, delta_buf_iter_))) {
          LOG_WARN("failed to rescan delta buf table rowkey table", K(ret), K_(delta_buf_tablet_id), K(scan_tablet_id));
        }
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_ROWKEY_VEC: {
      ObObj *vids = nullptr;
      ObSEArray<uint64_t, 1> vector_column_ids;
      int64_t dim = ada_ctx.get_dim();
      int64_t vec_cnt = ada_ctx.get_vec_cnt();

      if (OB_ISNULL(vids = ada_ctx.get_vids())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get vectors.", K(ret));
      }
      doc_id_scan_param_.key_ranges_.reuse();
      for (int i = 0; OB_SUCC(ret) && i < vec_cnt; i++) {
        ObRowkey vid_id_rowkey(&(vids[i + ada_ctx.get_curr_idx()]), 1);
        if (OB_FAIL(set_lookup_vid_key(vid_id_rowkey))) {
          LOG_WARN("failed to set vid rowkey id.", K(ret));
        } else if (OB_FAIL(get_cmpt_aux_table_rowkey())) {
          if (ret != OB_ITER_END) {
            LOG_WARN("do aux index lookup failed", K(ret));
          } else {
            ada_ctx.set_vector(i, nullptr, 0);
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(vector_do_index_lookup())) {
          LOG_WARN("failed to lookup.", K(ret));
        } else {
          ObNewRowIterator *storage_iter = com_aux_vec_iter_;
          storage::ObTableScanIterator *table_scan_iter = dynamic_cast<storage::ObTableScanIterator *>(storage_iter);
          ObString vector;
          blocksstable::ObDatumRow *datum_row = nullptr;
          if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next row from next table.", K(ret));
            } else {
              ada_ctx.set_vector(i, nullptr, 0);
              ret = OB_SUCCESS;
            }
          } else if (datum_row->get_column_count() != 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
          } else if (OB_FALSE_IT(vector = datum_row->storage_datums_[0].get_string())) {
            LOG_WARN("failed to get vid.", K(ret));
          } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&vec_op_alloc_,
                                                                            ObLongTextType,
                                                                            CS_TYPE_BINARY,
                                                                            com_aux_vec_ctdef_->result_output_.at(0)->obj_meta_.has_lob_header(),
                                                                            vector))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else {
            ada_ctx.set_vector(i, vector.ptr(), vector.length());
          }
        }
        reuse_scan_param_complete_data();
      }

      LOG_INFO("SYCN_DELTA_query_data", K(ada_ctx.get_vec_cnt()), K(ada_ctx.get_curr_idx()), K(ada_ctx.get_curr_idx()));

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }

      if (ada_ctx.is_query_end() || OB_FAIL(ret)) {
        // release iter for complete data, even OB_FAIL
        int tmp_ret = revert_iter_for_complete_data();
        if (tmp_ret != OB_SUCCESS) {
          LOG_WARN("failed to revert complete data iter.", K(ret));
          ret = ret == OB_SUCCESS ? tmp_ret : ret;
        }
        LOG_INFO("SYCN_DELTA_query_end_revert", K(ada_ctx.get_vec_cnt()), K(ada_ctx.get_curr_idx()), K(ada_ctx.get_curr_idx()));
      }
      break;
    }

    case ObVidAdaLookupStatus::QUERY_INDEX_ID_TBL: {
      if (nullptr == index_id_iter_) {
        index_id_scan_param_.need_switch_param_ = false;
        // init doc_id -> rowkey table iterator as rowkey iter
        if (OB_FAIL(init_index_id_scan_param())) {
          LOG_WARN("failed to init index id lookup scan param", K(ret));
        } else if (OB_FAIL(tsc_service.table_scan(index_id_scan_param_, index_id_iter_))) {
          if (OB_SNAPSHOT_DISCARDED == ret && index_id_scan_param_.fb_snapshot_.is_valid()) {
            ret = OB_INVALID_QUERY_TIMESTAMP;
          } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("fail to scan table", K(index_id_scan_param_), K(ret));
          }
        }
      } else {
        const ObTabletID &scan_tablet_id = index_id_scan_param_.tablet_id_;
        index_id_scan_param_.need_switch_param_ = scan_tablet_id.is_valid() && (index_id_tablet_id_ != scan_tablet_id);
        index_id_scan_param_.tablet_id_ = index_id_tablet_id_;
        index_id_scan_param_.ls_id_ = ls_id_;
        if (OB_FAIL(tsc_service.reuse_scan_iter(index_id_scan_param_.need_switch_param_, index_id_iter_))) {
          LOG_WARN("failed to reuse index id iter iterator", K(ret));
        } else if (OB_FAIL(tsc_service.table_rescan(index_id_scan_param_, index_id_iter_))) {
          LOG_WARN("failed to rescan index id iter rowkey table", K(ret), K_(index_id_tablet_id), K(scan_tablet_id));
        }
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL: {
      if (nullptr == snapshot_iter_) {
        snapshot_scan_param_.need_switch_param_ = false;
        // init doc_id -> rowkey table iterator as rowkey iter
        if (OB_FAIL(init_snapshot_scan_param())) {
          LOG_WARN("failed to init snapshot table lookup scan param", K(ret));
        } else if (OB_FAIL(tsc_service.table_scan(snapshot_scan_param_, snapshot_iter_))) {
          if (OB_SNAPSHOT_DISCARDED == ret && snapshot_scan_param_.fb_snapshot_.is_valid()) {
            ret = OB_INVALID_QUERY_TIMESTAMP;
          } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("fail to scan table", K(snapshot_scan_param_), K(ret));
          }
        }
      } else {
        const ObTabletID &scan_tablet_id = snapshot_scan_param_.tablet_id_;
        snapshot_scan_param_.need_switch_param_ = scan_tablet_id.is_valid() && (snapshot_tablet_id_ != scan_tablet_id);
        snapshot_scan_param_.tablet_id_ = snapshot_tablet_id_;
        snapshot_scan_param_.ls_id_ = ls_id_;
        if (OB_FAIL(tsc_service.reuse_scan_iter(snapshot_scan_param_.need_switch_param_, snapshot_iter_))) {
          LOG_WARN("failed to reuse sanpshot iterator", K(ret));
        } else if (OB_FAIL(tsc_service.table_rescan(snapshot_scan_param_, snapshot_iter_))) {
          LOG_WARN("failed to rescan snapshot rowkey table", K(ret), K_(snapshot_tablet_id), K(scan_tablet_id));
        }
      }
      break;
    }
    case ObVidAdaLookupStatus::STATES_END: {
      // do nothing
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status.", K(ret));
      break;
    }
  }
  return ret;
}
int ObVectorIndexLookupOp::call_pva_interface(const ObVidAdaLookupStatus& cur_state,
                                              ObVectorQueryAdaptorResultContext& ada_ctx,
                                              ObPluginVectorIndexAdaptor &adaptor)
{
  int ret = OB_SUCCESS;
  switch(cur_state) {
    case ObVidAdaLookupStatus::STATES_INIT: {
      if (OB_FAIL(adaptor.check_delta_buffer_table_readnext_status(&ada_ctx, delta_buf_iter_, delta_buf_scan_param_.snapshot_.core_.version_))) {
        LOG_WARN("fail to check_delta_buffer_table_readnext_status.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_ROWKEY_VEC: {
      if (OB_FAIL(adaptor.complete_delta_buffer_table_data(&ada_ctx))) {
        LOG_WARN("failed to complete_delta_buffer_table_data.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_INDEX_ID_TBL: {
      if (!index_id_scan_param_.snapshot_.is_valid() || !index_id_scan_param_.snapshot_.core_.version_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get index id scan param invalid.", K(ret));
      } else if (OB_FAIL(adaptor.check_index_id_table_readnext_status(&ada_ctx, index_id_iter_, index_id_scan_param_.snapshot_.core_.version_))) {
        LOG_WARN("fail to check_index_id_table_readnext_status.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL: {
      if (OB_FAIL(adaptor.check_snapshot_table_wait_status(&ada_ctx))) {
        LOG_WARN("fail to check_snapshot_table_wait_status.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::STATES_END: {
      ObVectorQueryConditions query_cond;
      if (OB_FAIL(set_vector_query_condition(query_cond))) {
        LOG_WARN("fail to set query condition.", K(ret));
      } else if (OB_FAIL(adaptor.query_result(&ada_ctx, &query_cond, adaptor_vid_iter_))) {
        LOG_WARN("fail to query result.", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status.", K(ret));
      break;
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::process_adaptor_state()
{
  int ret = OB_SUCCESS;
  bool is_continue = true;
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObVidAdaLookupStatus last_state = ObVidAdaLookupStatus::STATES_ERROR;
  ObVidAdaLookupStatus cur_state = ObVidAdaLookupStatus::STATES_INIT;
  ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()); // use for tmp query and data complement
  ObArenaAllocator batch_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()); // use for data complement for each batch
  ObVectorQueryAdaptorResultContext ada_ctx(MTL_ID(), &vec_op_alloc_, &tmp_allocator, &batch_allocator);
  share::ObVectorIndexAcquireCtx index_ctx;
  ObPluginVectorIndexAdapterGuard adaptor_guard;
  index_ctx.inc_tablet_id_ = delta_buf_tablet_id_;
  index_ctx.vbitmap_tablet_id_ = index_id_tablet_id_;
  index_ctx.snapshot_tablet_id_ = snapshot_tablet_id_;
  index_ctx.data_tablet_id_ = tablet_id_;
  if (OB_ISNULL(delta_buf_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be null", K(ret));
  }  else if (OB_FAIL(vec_index_service->acquire_adapter_guard(ls_id_, index_ctx, adaptor_guard, &vec_index_param_, dim_))) {
    LOG_WARN("fail to get ObMockPluginVectorIndexAdapter", K(ret));
  } else {
    share::ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    if (OB_ISNULL(adaptor)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("shouldn't be null.", K(ret));
    } else {
      while (OB_SUCC(ret) && is_continue) {
        if ((last_state != cur_state || cur_state == ObVidAdaLookupStatus::QUERY_ROWKEY_VEC) && OB_FAIL(prepare_state(cur_state, ada_ctx))) {
          LOG_WARN("failed to prepare state", K(ret));
        } else if (OB_FAIL(call_pva_interface(cur_state, ada_ctx, *adaptor))) {
          LOG_WARN("failed to call_pva_interface", K(ret));
        } else if (OB_FALSE_IT(last_state = cur_state)) {
        } else if (OB_FAIL(next_state(cur_state, ada_ctx, is_continue))) {
          LOG_WARN("fail to get next status.", K(cur_state), K(ada_ctx.get_status()), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::next_state(ObVidAdaLookupStatus& cur_state,
                                      ObVectorQueryAdaptorResultContext& ada_ctx,
                                      bool& is_continue)
{
  int ret = OB_SUCCESS;
  switch(cur_state) {
    case ObVidAdaLookupStatus::STATES_INIT: {
      if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_WAIT) {
        is_continue = true;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_LACK_SCN) {
        cur_state = ObVidAdaLookupStatus::QUERY_INDEX_ID_TBL;
        is_continue = true;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_OK) {
        cur_state = ObVidAdaLookupStatus::STATES_END;
        is_continue = true;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_INVALID_SCN) {
        cur_state = ObVidAdaLookupStatus::STATES_ERROR;
        is_continue = false;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status.", K(ada_ctx.get_status()), K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_ROWKEY_VEC: {
      if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_LACK_SCN) {
        cur_state = ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL;
        is_continue = true;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_INVALID_SCN) {
        cur_state = ObVidAdaLookupStatus::STATES_ERROR;
        is_continue = false;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_COM_DATA) {
        cur_state = ObVidAdaLookupStatus::QUERY_ROWKEY_VEC;
        is_continue = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status.", K(ada_ctx.get_status()), K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_INDEX_ID_TBL: {
      if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_WAIT) {
        is_continue = true;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_COM_DATA) {
        cur_state = ObVidAdaLookupStatus::QUERY_ROWKEY_VEC;
        is_continue = true;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_LACK_SCN) {
        cur_state = ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL;
        is_continue = true;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_OK) {
        cur_state = ObVidAdaLookupStatus::STATES_END;
        is_continue = true;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_INVALID_SCN) {
        cur_state = ObVidAdaLookupStatus::STATES_ERROR;
        is_continue = false;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status.", K(ada_ctx.get_status()), K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL: {
      if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_WAIT) {
        is_continue = true;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_OK) {
        cur_state = ObVidAdaLookupStatus::STATES_END;
        is_continue = true;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_INVALID_SCN) {
        cur_state = ObVidAdaLookupStatus::STATES_ERROR;
        is_continue = false;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status.", K(ada_ctx.get_status()), K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::STATES_END: {
      is_continue = false;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status.", K(ada_ctx.get_status()), K(ret));
      is_continue = false;
      break;
    }
  }
  return ret;
}
int ObVectorIndexLookupOp::get_aux_table_rowkey()
{
  int ret = OB_SUCCESS;
  doc_id_lookup_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
  if (index_end_ && doc_id_scan_param_.key_ranges_.empty()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(do_aux_table_lookup())) {
    LOG_WARN("failed to do aux table lookup", K(ret));
  } else if (OB_FAIL(rowkey_iter_->get_next_row())) {
    LOG_WARN("failed to get rowkey by vid", K(ret));
  } else if (OB_FAIL(set_main_table_lookup_key())) {
    LOG_WARN("failed to set main table lookup key", K(ret));
  }
  return ret;
}

int ObVectorIndexLookupOp::get_cmpt_aux_table_rowkey()
{
  int ret = OB_SUCCESS;
  doc_id_lookup_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
  if (index_end_ && doc_id_scan_param_.key_ranges_.empty()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(do_aux_table_lookup())) {
    LOG_WARN("failed to do aux table lookup", K(ret));
  } else if (OB_FAIL(rowkey_iter_->get_next_row())) {
    LOG_WARN("failed to get rowkey by vid", K(ret));
  } else if (OB_FAIL(set_com_main_table_lookup_key())) {
    LOG_WARN("failed to set main table lookup key", K(ret));
  }
  return ret;
}

int ObVectorIndexLookupOp::set_com_main_table_lookup_key()
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = doc_id_lookup_ctdef_->result_output_.count();
  void *buf = nullptr;
  ObObj *obj_ptr = nullptr;
  common::ObArenaAllocator &lookup_alloc = lookup_memctx_->get_arena_allocator();
  ObNewRange lookup_range;
  if (nullptr != doc_id_lookup_ctdef_->trans_info_expr_) {
    rowkey_cnt = rowkey_cnt - 1;
  }

  if (OB_UNLIKELY(rowkey_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret), KPC(doc_id_lookup_ctdef_));
  } else if (OB_ISNULL(buf = lookup_alloc.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new (buf) ObObj[rowkey_cnt];
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    ObObj tmp_obj;
    ObExpr *expr = doc_id_lookup_ctdef_->result_output_.at(i);
    if (T_PSEUDO_GROUP_ID == expr->type_) {
      // do nothing
    } else {
      ObDatum &col_datum = expr->locate_expr_datum(*doc_id_lookup_rtdef_->eval_ctx_);
      if (OB_ISNULL(col_datum.ptr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get col datum null", K(ret));
      } else if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      } else if (OB_FAIL(ob_write_obj(lookup_alloc, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObRowkey table_rowkey(obj_ptr, rowkey_cnt);
    if (OB_FAIL(lookup_range.build_range(com_aux_vec_ctdef_->ref_table_id_, table_rowkey))) {
      LOG_WARN("failed to build lookup range", K(ret), K(table_rowkey));
    } else if (OB_FAIL(com_aux_vec_scan_param_.key_ranges_.push_back(lookup_range))) {
      LOG_WARN("store lookup key range failed", K(ret), K(scan_param_));
    } else {
      LOG_DEBUG("get rowkey from docid rowkey table", K(ret), K(table_rowkey), K(lookup_range));
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::set_main_table_lookup_key()
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = doc_id_lookup_ctdef_->result_output_.count();
  void *buf = nullptr;
  ObObj *obj_ptr = nullptr;
  common::ObArenaAllocator &lookup_alloc = lookup_memctx_->get_arena_allocator();
  ObNewRange lookup_range;
  if (nullptr != doc_id_lookup_ctdef_->trans_info_expr_) {
    rowkey_cnt = rowkey_cnt - 1;
  }

  if (OB_UNLIKELY(rowkey_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret), KPC(doc_id_lookup_ctdef_));
  } else if (OB_ISNULL(buf = lookup_alloc.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new (buf) ObObj[rowkey_cnt];
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
    ObObj tmp_obj;
    ObExpr *expr = doc_id_lookup_ctdef_->result_output_.at(i);
    if (T_PSEUDO_GROUP_ID == expr->type_) {
      // do nothing
    } else {
      ObDatum &col_datum = expr->locate_expr_datum(*doc_id_lookup_rtdef_->eval_ctx_);
      if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      } else if (OB_FAIL(ob_write_obj(lookup_alloc, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObRowkey table_rowkey(obj_ptr, rowkey_cnt);
    if (OB_FAIL(lookup_range.build_range(lookup_ctdef_->ref_table_id_, table_rowkey))) {
      LOG_WARN("failed to build lookup range", K(ret), K(table_rowkey));
    } else if (OB_FAIL(scan_param_.key_ranges_.push_back(lookup_range))) {
      LOG_WARN("store lookup key range failed", K(ret), K(scan_param_));
    } else {
      LOG_DEBUG("get rowkey from docid rowkey table", K(ret), K(table_rowkey), K(lookup_range));
    }
  }
  return ret;
}

int ObVectorIndexLookupOp::do_aux_table_lookup()
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  if (nullptr == rowkey_iter_) {
    doc_id_scan_param_.need_switch_param_ = false;
    // init doc_id -> rowkey table iterator as rowkey iter
    if (OB_FAIL(set_doc_id_idx_lookup_param(
        doc_id_lookup_ctdef_, doc_id_lookup_rtdef_, doc_id_scan_param_, doc_id_idx_tablet_id_, ls_id_))) {
      LOG_WARN("failed to init vid lookup scan param", K(ret));
    } else if (OB_FAIL(tsc_service.table_scan(doc_id_scan_param_, rowkey_iter_))) {
      if (OB_SNAPSHOT_DISCARDED == ret && scan_param_.fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to scan table", K(scan_param_), K(ret));
      }
    }
  } else {
    const ObTabletID &scan_tablet_id = doc_id_scan_param_.tablet_id_;
    doc_id_scan_param_.need_switch_param_ = scan_tablet_id.is_valid() && (doc_id_idx_tablet_id_ != scan_tablet_id);
    doc_id_scan_param_.tablet_id_ = doc_id_idx_tablet_id_;
    doc_id_scan_param_.ls_id_ = ls_id_;
    if (OB_FAIL(tsc_service.reuse_scan_iter(doc_id_scan_param_.need_switch_param_, rowkey_iter_))) {
      LOG_WARN("failed to reuse vid iterator", K(ret));
    } else if (OB_FAIL(tsc_service.table_rescan(doc_id_scan_param_, rowkey_iter_))) {
      LOG_WARN("failed to rescan vid rowkey table", K(ret), K_(doc_id_idx_tablet_id), K(scan_tablet_id));
    }
  }
  return ret;
}

void ObVectorIndexLookupOp::do_clear_evaluated_flag()
{
  if OB_NOT_NULL(delta_buf_rtdef_) {
    delta_buf_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
  }
  if OB_NOT_NULL(index_id_rtdef_) {
    index_id_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
  }
  if OB_NOT_NULL(snapshot_rtdef_) {
    snapshot_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
  }
  return ObDomainIndexLookupOp::do_clear_evaluated_flag();
}

int ObVectorIndexLookupOp::revert_iter()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  if (nullptr != adaptor_vid_iter_) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }

  if (OB_NOT_NULL(delta_buf_rtdef_)) {
    delta_buf_scan_param_.need_switch_param_ = false;
    delta_buf_scan_param_.destroy_schema_guard();
    delta_buf_scan_param_.~ObTableScanParam();
  }
  if (OB_NOT_NULL(index_id_rtdef_)) {
    index_id_scan_param_.need_switch_param_ = false;
    index_id_scan_param_.destroy_schema_guard();
    index_id_scan_param_.~ObTableScanParam();
  }
  if (OB_NOT_NULL(snapshot_rtdef_)) {
    snapshot_scan_param_.need_switch_param_ = false;
    snapshot_scan_param_.destroy_schema_guard();
    snapshot_scan_param_.~ObTableScanParam();
  }

  if (OB_NOT_NULL(com_aux_vec_rtdef_)) {
    com_aux_vec_scan_param_.need_switch_param_ = false;
    com_aux_vec_scan_param_.destroy_schema_guard();
    com_aux_vec_scan_param_.~ObTableScanParam();
  }

  if (OB_FAIL(tsc_service.revert_scan_iter(delta_buf_iter_))) {
    LOG_WARN("revert scan iterator failed", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  // revert all scan iter anyway
  if (OB_FAIL(tsc_service.revert_scan_iter(index_id_iter_))) {
    LOG_WARN("revert scan iterator failed", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(tsc_service.revert_scan_iter(snapshot_iter_))) {
    LOG_WARN("revert scan iterator failed", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(tsc_service.revert_scan_iter(aux_lookup_iter_))) {
    LOG_WARN("revert index table scan iterator (opened by dasop) failed", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(revert_iter_for_complete_data())) {
    LOG_WARN("failed to revert iter for complete data.", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }

  delta_buf_iter_ = nullptr;
  index_id_iter_ = nullptr;
  snapshot_iter_ = nullptr;
  aux_lookup_iter_ = nullptr;
  if (OB_FAIL(ObDomainIndexLookupOp::revert_iter())) {
    LOG_WARN("failed to revert local index lookup op iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }
  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }
  vec_op_alloc_.reset();
  return ret;
}

int ObVectorIndexLookupOp::set_vector_query_condition(ObVectorQueryConditions &query_cond)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_vec_) || OB_ISNULL(sort_rtdef_) || OB_ISNULL(sort_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null.", K(ret));
  } else {
    query_cond.query_limit_ = limit_param_.limit_ + limit_param_.offset_;
    query_cond.query_order_ = true;
    query_cond.row_iter_ = snapshot_iter_;
    query_cond.query_scn_ = snapshot_scan_param_.snapshot_.core_.version_;
    ObSQLSessionInfo *session = nullptr;
    uint64_t ob_hnsw_ef_search = 0;
    ObDatum *vec_datum = NULL;
    if (OB_FALSE_IT(session = sort_rtdef_->eval_ctx_->exec_ctx_.get_my_session())) {
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get table schema", K(ret), KPC(session));
    } else if (OB_FAIL(session->get_ob_hnsw_ef_search(ob_hnsw_ef_search))) {
      LOG_WARN("fail to get ob_hnsw_ef_search", K(ret));
    } else if (OB_FALSE_IT(query_cond.ef_search_ = ob_hnsw_ef_search)) {
    } else if (OB_UNLIKELY(OB_FAIL(search_vec_->eval(*(sort_rtdef_->eval_ctx_), vec_datum)))) {
      LOG_WARN("eval vec arg failed", K(ret));
    } else if (OB_FALSE_IT(query_cond.query_vector_ = vec_datum->get_string())) {
    } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&vec_op_alloc_,
                                                                      ObLongTextType,
                                                                      CS_TYPE_BINARY,
                                                                      search_vec_->obj_meta_.has_lob_header(),
                                                                      query_cond.query_vector_))) {
      LOG_WARN("failed to get real data.", K(ret));
    }
  }
  return ret;
}


}  // namespace sql
}  // namespace oceanbase
