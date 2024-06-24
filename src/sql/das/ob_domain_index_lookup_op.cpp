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
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/ob_domain_index_lookup_op.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/access/ob_dml_param.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace transaction;
namespace sql
{

int ObDomainIndexLookupOp::init(
    const ObDASScanCtDef *lookup_ctdef,
    ObDASScanRtDef *lookup_rtdef,
    const ObDASScanCtDef *index_ctdef,
    ObDASScanRtDef *index_rtdef,
    const ObDASScanCtDef *doc_id_lookup_ctdef,
    ObDASScanRtDef *doc_id_lookup_rtdef,
    ObTxDesc *tx_desc,
    ObTxReadSnapshot *snapshot,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLocalIndexLookupOp::init(
      lookup_ctdef, lookup_rtdef, index_ctdef, index_rtdef, tx_desc, snapshot))) {
    LOG_WARN("ObLocalIndexLookupOp init failed", K(ret));
  } else {
    doc_id_lookup_ctdef_ = doc_id_lookup_ctdef;
    doc_id_lookup_rtdef_ = doc_id_lookup_rtdef;
    need_scan_aux_ = (doc_id_lookup_ctdef_ != nullptr);
  }
  return ret;
}

int ObDomainIndexLookupOp::reset_lookup_state()
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObLocalIndexLookupOp::reset_lookup_state())) {
    LOG_WARN("reset domain lookup state failed", K(ret));
  } else if (nullptr != lookup_iter_) {
    doc_id_scan_param_.key_ranges_.reuse();
    doc_id_scan_param_.ss_key_ranges_.reuse();
  }
  return ret;
}

int ObDomainIndexLookupOp::next_state()
{
  INIT_SUCC(ret);
  if (state_ == INDEX_SCAN) {
    if (0 == lookup_rowkey_cnt_) {
      state_ = LookupState::FINISHED;
    } else if (need_scan_aux_) {
      state_ = LookupState::AUX_LOOKUP;
    } else {
      state_ = LookupState::DO_LOOKUP;
    }
  } else if (state_ == LookupState::AUX_LOOKUP) {
    state_ = LookupState::DO_LOOKUP;
  } else if (state_ == LookupState::DO_LOOKUP) {
    state_ = LookupState::OUTPUT_ROWS;
  } else if (state_ == LookupState::OUTPUT_ROWS) {
    state_ = LookupState::INDEX_SCAN;
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  LOG_DEBUG("domain index to next state", K(ret), K(state_));
  return ret;
}

int ObDomainIndexLookupOp::get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  while (OB_SUCC(ret) && !got_next_row) {
    switch (state_) {
      case INDEX_SCAN: {
        reset_lookup_state();
        if (OB_FAIL(fetch_index_table_rowkey())) {
          if (OB_UNLIKELY(ret != OB_ITER_END)) {
            LOG_WARN("failed get index table rowkey", K(ret));
          } else {
            index_end_ = true;
            ret = OB_SUCCESS;
          }
        } else {
          ++lookup_rowkey_cnt_;
        }

        if (FAILEDx(next_state())) {
          LOG_WARN("failed to switch to next lookup state", K(ret), K(state_));
        }
        break;
      }
      case AUX_LOOKUP: {
        if (OB_FAIL(get_aux_table_rowkey())) {
          if (ret != OB_ITER_END) {
            LOG_WARN("do aux index lookup failed", K(ret));
          }
        } else {
          // ++lookup_rowkey_cnt_;
          if (OB_FAIL(next_state())) {
            LOG_WARN("failed to switch to next lookup state", K(ret), K(state_));
          }
        }
        break;
      }
      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("do index lookup failed", K(ret));
        } else if (OB_FAIL(next_state())) {
          LOG_WARN("failed to switch to next lookup state", K(ret), K(state_));
        }
        break;
      }
      case OUTPUT_ROWS: {
        if (OB_FAIL(get_next_row_from_data_table())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            if (OB_FAIL(check_lookup_row_cnt())) {
              LOG_WARN("failed to check lookup row cnt", K(ret));
            } else if (OB_FAIL(next_state())) {
              LOG_WARN("failed to switch to next lookup state", K(ret));
            }
          } else {
            LOG_WARN("look up get next row failed", K(ret));
          }
        } else {
          got_next_row = true;
          ++lookup_row_cnt_;
          LOG_DEBUG("got next row from table lookup",  K(ret), K(lookup_row_cnt_), K(lookup_rowkey_cnt_), "main table output", ROWEXPR2STR(get_eval_ctx(), get_output_expr()) );
        }
        break;
      }
      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(state_));
      }
    }
  }

  return ret;
}

int ObDomainIndexLookupOp::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  while (OB_SUCC(ret) && !got_next_row) {
    switch (state_) {
      case INDEX_SCAN: {
        reset_lookup_state();
        int64_t rowkey_count = 0;
        lookup_row_cnt_ = 0;
        lookup_row_cnt_ = 0;
        if (OB_FAIL(fetch_index_table_rowkeys(rowkey_count, capacity))) {
          LOG_WARN("failed get rowkeys from index table", K(ret));
        } else if (0 == rowkey_count) {
          index_end_ = true;
        }
        if (OB_SUCC(ret)) {
          if (rowkey_count > 0) {
            lookup_rowkey_cnt_ += rowkey_count;
          }
          if (OB_FAIL(next_state())) {
            LOG_WARN("failed to switch to next lookup state", K(ret), K(state_));
          }
        }
        break;
      }
      case AUX_LOOKUP: {
        if (OB_FAIL(get_aux_table_rowkeys(lookup_rowkey_cnt_))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("do aux index lookup failed", K(ret));
          }
        } else if (OB_FAIL(next_state())) {
          LOG_WARN("failed to switch to next lookup state", K(ret), K(state_));
        }
        break;
      }
      case DO_LOOKUP: {
        lookup_row_cnt_ = 0;
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("do index lookup failed", K(ret));
        } else if (OB_FAIL(next_state())) {
          LOG_WARN("failed to switch to next lookup state", K(ret), K(state_));
        }
        break;
      }
      case OUTPUT_ROWS: {
        if (OB_FAIL(get_next_rows_from_data_table(count, capacity))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            if (count > 0) {
              lookup_row_cnt_ += count;
              got_next_row = true;
            } else if (OB_FAIL(check_lookup_row_cnt())) {
              LOG_WARN("failed to check table lookup", K(ret));
            } else if (OB_FAIL(next_state())) {
              LOG_WARN("failed to switch to next lookup state", K(ret), K_(state));
            }
          } else {
            LOG_WARN("look up get next row failed", K(ret));
          }
        } else {
          got_next_row = true;
          lookup_row_cnt_ += count;
          const ObBitVector *skip = nullptr;
          PRINT_VECTORIZED_ROWS(SQL, DEBUG, get_eval_ctx(), get_output_expr(), count, skip,
                                K(ret), K(lookup_row_cnt_), K(lookup_rowkey_cnt_));
        }
        break;
      }
      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(state_));
      }
    }
  }

  return ret;
}

int ObDomainIndexLookupOp::set_lookup_doc_id_key(ObExpr *doc_id_expr, ObEvalCtx *eval_ctx_)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator &lookup_alloc = lookup_memctx_->get_arena_allocator();
  ObNewRange doc_id_range;
  ObDatum &doc_id_datum = doc_id_expr->locate_expr_datum(*eval_ctx_);
  if (OB_FAIL(doc_id_datum.to_obj(doc_id_key_obj_, doc_id_expr->obj_meta_, doc_id_expr->obj_datum_map_))) {
    LOG_WARN("failed to cast datum to obj", K(ret), K(doc_id_key_obj_));
  } else {
    ObRowkey doc_id_rowkey(&doc_id_key_obj_, 1);
    uint64_t ref_table_id = doc_id_lookup_ctdef_->ref_table_id_;
    if (OB_FAIL(doc_id_range.build_range(ref_table_id, doc_id_rowkey))) {
      LOG_WARN("build doc id lookup range failed", K(ret));
    } else if (OB_FAIL(doc_id_scan_param_.key_ranges_.push_back(doc_id_range))) {
      LOG_WARN("store lookup key range failed", K(ret));
    } else {
      LOG_DEBUG("generate doc id scan range", K(ret), K(doc_id_range));
    }
  }
  return ret;
}

int ObDomainIndexLookupOp::set_doc_id_idx_lookup_param(
  const ObDASScanCtDef *aux_lookup_ctdef,
  ObDASScanRtDef *aux_lookup_rtdef,
  storage::ObTableScanParam& aux_scan_param,
  common::ObTabletID tablet_id,
  share::ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(aux_lookup_ctdef)
      || OB_ISNULL(aux_lookup_rtdef)
      || OB_UNLIKELY(!tablet_id.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(aux_lookup_ctdef), KP(aux_lookup_rtdef), K(tablet_id), K(ls_id));
  } else {
    aux_scan_param.tenant_id_ = MTL_ID();
    aux_scan_param.tx_lock_timeout_ = aux_lookup_rtdef->tx_lock_timeout_;
    aux_scan_param.index_id_ = aux_lookup_ctdef->ref_table_id_;
    aux_scan_param.is_get_ = aux_lookup_ctdef->is_get_;
    aux_scan_param.is_for_foreign_check_ = aux_lookup_rtdef->is_for_foreign_check_;
    aux_scan_param.timeout_ = aux_lookup_rtdef->timeout_ts_;
    aux_scan_param.scan_flag_ = aux_lookup_rtdef->scan_flag_;
    aux_scan_param.reserved_cell_count_ = aux_lookup_ctdef->access_column_ids_.count();
    aux_scan_param.allocator_ = &aux_lookup_rtdef->stmt_allocator_;
    aux_scan_param.scan_allocator_ = &aux_lookup_rtdef->scan_allocator_;
    aux_scan_param.sql_mode_ = aux_lookup_rtdef->sql_mode_;
    aux_scan_param.frozen_version_ = aux_lookup_rtdef->frozen_version_;
    aux_scan_param.force_refresh_lc_ = aux_lookup_rtdef->force_refresh_lc_;
    aux_scan_param.output_exprs_ = &(aux_lookup_ctdef->pd_expr_spec_.access_exprs_);
    aux_scan_param.ext_file_column_exprs_ = &(aux_lookup_ctdef->pd_expr_spec_.ext_file_column_exprs_);
    aux_scan_param.ext_column_convert_exprs_ = &(aux_lookup_ctdef->pd_expr_spec_.ext_column_convert_exprs_);
    aux_scan_param.calc_exprs_ = &(aux_lookup_ctdef->pd_expr_spec_.calc_exprs_);
    aux_scan_param.aggregate_exprs_ = &(aux_lookup_ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    aux_scan_param.table_param_ = &(aux_lookup_ctdef->table_param_);
    aux_scan_param.op_ = aux_lookup_rtdef->p_pd_expr_op_;
    aux_scan_param.row2exprs_projector_ = aux_lookup_rtdef->p_row2exprs_projector_;
    aux_scan_param.schema_version_ = aux_lookup_ctdef->schema_version_;
    aux_scan_param.tenant_schema_version_ = aux_lookup_rtdef->tenant_schema_version_;
    aux_scan_param.limit_param_ = aux_lookup_rtdef->limit_param_;
    aux_scan_param.need_scn_ = aux_lookup_rtdef->need_scn_;
    aux_scan_param.pd_storage_flag_ = aux_lookup_ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    aux_scan_param.fb_snapshot_ = aux_lookup_rtdef->fb_snapshot_;
    aux_scan_param.fb_read_tx_uncommitted_ = aux_lookup_rtdef->fb_read_tx_uncommitted_;
    if (aux_lookup_rtdef->is_for_foreign_check_) {
      aux_scan_param.trans_desc_ = tx_desc_;
    }
    aux_scan_param.ls_id_ = ls_id;
    aux_scan_param.tablet_id_ = tablet_id;
    if (aux_lookup_rtdef->sample_info_ != nullptr) {
      aux_scan_param.sample_info_ = *aux_lookup_rtdef->sample_info_;
    }
    if (OB_NOT_NULL(snapshot_)) {
      aux_scan_param.snapshot_ = *snapshot_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("snapshot is null", K(ret), KPC(this));
    }
    if (OB_NOT_NULL(tx_desc_)) {
      aux_scan_param.tx_id_ = tx_desc_->get_tx_id();
    } else {
      aux_scan_param.tx_id_.reset();
    }
    if (!aux_lookup_ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      aux_scan_param.op_filters_ = &aux_lookup_ctdef->pd_expr_spec_.pushdown_filters_;
    }
    aux_scan_param.pd_storage_filters_ = aux_lookup_rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(aux_scan_param.column_ids_.assign(aux_lookup_ctdef->access_column_ids_))) {
      LOG_WARN("init column ids failed", K(ret));
    }
    //external table scan params
    if (OB_SUCC(ret) && aux_lookup_ctdef->is_external_table_) {
      aux_scan_param.external_file_access_info_ = aux_lookup_ctdef->external_file_access_info_.str_;
      aux_scan_param.external_file_location_ = aux_lookup_ctdef->external_file_location_.str_;
      if (OB_FAIL(aux_scan_param.external_file_format_.load_from_string(aux_lookup_ctdef->external_file_format_str_.str_, *aux_scan_param.allocator_))) {
        LOG_WARN("fail to load from string", K(ret));
      } else {
        uint64_t max_idx = 0;
        for (int i = 0; i < aux_scan_param.ext_file_column_exprs_->count(); i++) {
          max_idx = std::max(max_idx, aux_scan_param.ext_file_column_exprs_->at(i)->extra_);
        }
        aux_scan_param.external_file_format_.csv_format_.file_column_nums_ = static_cast<int64_t>(max_idx);
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("init scan param", K(aux_scan_param));
    }
  }
  return ret;
}

void ObDomainIndexLookupOp::do_clear_evaluated_flag()
{
  ObLocalIndexLookupOp::do_clear_evaluated_flag();
  if (OB_NOT_NULL(doc_id_lookup_rtdef_)) {
    doc_id_lookup_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
  }
}

int ObDomainIndexLookupOp::revert_iter()
{
  int ret = OB_SUCCESS;
  // rowkey_iter is reverted at ObLocalIndexLookupOp
  if (OB_NOT_NULL(doc_id_lookup_rtdef_)) {
    doc_id_scan_param_.need_switch_param_ = false;
    doc_id_scan_param_.destroy_schema_guard();
  }

  if (OB_FAIL(ObLocalIndexLookupOp::revert_iter())) {
    LOG_WARN("failed to revert local index lookup op iter", K(ret));
  }
  return ret;
}

int ObDomainIndexLookupOp::reuse_scan_iter()
{
  reset_lookup_state();
  return OB_SUCCESS;
}

int ObMulValueIndexLookupOp::revert_iter()
{
  int ret = OB_SUCCESS;
  if (nullptr != aux_lookup_iter_) {
    aux_lookup_iter_->reset();
    aux_lookup_iter_->~ObNewRowIterator();
    if (nullptr != allocator_) {
      allocator_->free(aux_lookup_iter_);
    }
    aux_lookup_iter_ = nullptr;
  }

  sorter_.clean_up();
  sorter_.~ObExternalSort();

  aux_sorter_.clean_up();
  aux_sorter_.~ObExternalSort();

  if (OB_FAIL(ObDomainIndexLookupOp::revert_iter())) {
    LOG_WARN("failed to revert multivalue index lookup op iter", K(ret));
  }
  return ret;
}

void ObMulValueIndexLookupOp::do_clear_evaluated_flag()
{
  lookup_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
  return ObDomainIndexLookupOp::do_clear_evaluated_flag();
}

int ObMulValueIndexLookupOp::init_scan_param()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObDomainIndexLookupOp::init_scan_param())) {
    LOG_WARN("failed to init scan param", K(ret));
  }

  return ret;
}

int ObMulValueIndexLookupOp::init(const ObDASBaseCtDef *table_lookup_ctdef,
                                  ObDASBaseRtDef *table_lookup_rtdef,
                                  ObTxDesc *tx_desc,
                                  ObTxReadSnapshot *snapshot,
                                  ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  const ObDASTableLookupCtDef *tbl_lookup_ctdef = nullptr;
  ObDASTableLookupRtDef *tbl_lookup_rtdef = nullptr;
  const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
  ObDASIRAuxLookupRtDef *aux_lookup_rtdef = nullptr;

  if (OB_ISNULL(table_lookup_ctdef) || OB_ISNULL(table_lookup_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table lookup param is nullptr", KP(table_lookup_ctdef), KP(table_lookup_rtdef));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(table_lookup_ctdef,
                                                     table_lookup_rtdef,
                                                     DAS_OP_TABLE_LOOKUP,
                                                     tbl_lookup_ctdef,
                                                     tbl_lookup_rtdef))) {
    LOG_WARN("find data table lookup def failed", K(ret));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(table_lookup_ctdef,
                                                     table_lookup_rtdef,
                                                     DAS_OP_IR_AUX_LOOKUP,
                                                     aux_lookup_ctdef,
                                                     aux_lookup_rtdef))) {
    LOG_WARN("find ir aux lookup def failed", K(ret));
  } else if (aux_lookup_ctdef->children_cnt_ != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find index def failed", K(ret), K(aux_lookup_ctdef->children_cnt_));
  } else {
    const ObDASScanCtDef* index_ctdef = static_cast<const ObDASScanCtDef*>(aux_lookup_ctdef->children_[0]);
    ObDASScanRtDef * index_rtdef = static_cast<ObDASScanRtDef *>(aux_lookup_rtdef->children_[0]);

    if (OB_FAIL(ObDomainIndexLookupOp::init(tbl_lookup_ctdef->get_lookup_scan_ctdef(),
                                            tbl_lookup_rtdef->get_lookup_scan_rtdef(),
                                            index_ctdef,
                                            index_rtdef,
                                            aux_lookup_ctdef->get_lookup_scan_ctdef(),
                                            aux_lookup_rtdef->get_lookup_scan_rtdef(),
                                            tx_desc, snapshot, scan_param))) {
      LOG_WARN("ObLocalIndexLookupOp init failed", K(ret));
    }
  }
  return ret;
}

int ObMulValueIndexLookupOp::reset_lookup_state()
{
  is_inited_ = false;
  return ObDomainIndexLookupOp::reset_lookup_state();
}

int ObMulValueIndexLookupOp::init_sort()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    cmp_ret_ = OB_SUCCESS;
    aux_cmp_ret_ = OB_SUCCESS;
    new (&comparer_) ObDomainRowkeyComp(cmp_ret_);
    new (&aux_comparer_) ObDomainRowkeyComp(aux_cmp_ret_);
    const int64_t file_buf_size = ObExternalSortConstant::DEFAULT_FILE_READ_WRITE_BUFFER;
    const int64_t expire_timestamp = 0;
    const int64_t buf_limit = SORT_MEMORY_LIMIT;
    const uint64_t tenant_id = MTL_ID();
    sorter_.clean_up();
    aux_sorter_.clean_up();
    if (OB_FAIL(sorter_.init(buf_limit, file_buf_size, expire_timestamp, tenant_id, &comparer_))) {
      LOG_WARN("fail to init sorter", K(ret));
    } else if (OB_FAIL(aux_sorter_.init(buf_limit, file_buf_size, expire_timestamp, tenant_id, &aux_comparer_))) {
      LOG_WARN("fail to init aux sorter", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObMulValueIndexLookupOp::get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;

  if (!is_inited_) {
    if (OB_FAIL(fetch_index_table_rowkey())) {
      if (OB_UNLIKELY(ret != OB_ITER_END)) {
        LOG_WARN("failed get index table rowkey", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (FAILEDx(fetch_rowkey_from_aux())) {
      LOG_WARN("fetch rowkey from doc-rowkey table failed", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  while (OB_SUCC(ret) && !got_next_row) {
    switch (state_) {
      case INDEX_SCAN: {
        if (OB_FAIL(save_rowkeys())) {
          if (OB_UNLIKELY(ret != OB_ITER_END)) {
            LOG_WARN("failed get index table rowkey", K(ret));
          }
        }

        if (OB_SUCC(ret) || OB_ITER_END == ret) {
          if (OB_ITER_END == ret) {
            state_ = FINISHED;
            index_end_ = true;
          } else {
            state_ = DO_LOOKUP;
            ret = OB_SUCCESS;
          }
        }
        break;
      }
      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("do index lookup failed", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }
      case OUTPUT_ROWS: {
        if (OB_FAIL(get_next_row_from_data_table())) {
          if (OB_ITER_END == ret) {
            if (!index_end_) {
              ret = OB_SUCCESS;
              state_ = INDEX_SCAN;
              ObLocalIndexLookupOp::reset_lookup_state();
            } else {
              state_ = FINISHED;
            }
          } else {
            LOG_WARN("look up get next row failed", K(ret));
          }
        } else {
          got_next_row = true;
          ++lookup_row_cnt_;
          LOG_DEBUG("got next row from table lookup",  K(ret), K(lookup_row_cnt_), K(lookup_rowkey_cnt_), "main table output", ROWEXPR2STR(get_eval_ctx(), get_output_expr()) );
        }
        break;
      }
      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(state_));
      }
    }
  }

  return ret;
}

int ObMulValueIndexLookupOp::save_doc_id_and_rowkey()
{
  int ret = OB_SUCCESS;

  // index_column_cnt : |multivalue column| rowkey column | doc-id column |
  int64_t index_column_cnt = index_ctdef_->result_output_.count();
  const storage::ObTableReadInfo& read_info = lookup_ctdef_->table_param_.get_read_info();
  int64_t main_rowkey_column_cnt = read_info.get_schema_rowkey_count();
  ObObj *obj_ptr = nullptr;

  ObArenaAllocator allocator("MulvalLookup");
  if (OB_ISNULL(obj_ptr = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * index_column_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(index_column_cnt));
  } else {
    obj_ptr = new(obj_ptr) ObObj[index_column_cnt];
  }

  int64_t rowkey_null_count = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < main_rowkey_column_cnt; ++i) {
    ObExpr *expr = index_ctdef_->result_output_.at(i);
    if (T_PSEUDO_GROUP_ID == expr->type_) {
      // do nothing
    } else {
      ObDatum &col_datum = expr->locate_expr_datum(*lookup_rtdef_->eval_ctx_);
      if (OB_FAIL(col_datum.to_obj(obj_ptr[i], expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      } else if (col_datum.is_null()) {
        rowkey_null_count++;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (rowkey_null_count != main_rowkey_column_cnt) {
    ++index_rowkey_cnt_;
    ++lookup_rowkey_cnt_;
    ObRowkey main_rowkey(obj_ptr, main_rowkey_column_cnt);
    if (OB_FAIL(sorter_.add_item(main_rowkey))) {
      LOG_WARN("filter mbr failed", K(ret));
    }
  } else {
    ++aux_key_count_;
    ++lookup_rowkey_cnt_;
    // last column is doc-id
    int64_t doc_id_idx = main_rowkey_column_cnt;
    ObExpr* doc_id_expr = index_ctdef_->result_output_.at(doc_id_idx);
    ObDatum& doc_id_datum = doc_id_expr->locate_expr_datum(*lookup_rtdef_->eval_ctx_);
    if (OB_FAIL(doc_id_datum.to_obj(obj_ptr[doc_id_idx], doc_id_expr->obj_meta_, doc_id_expr->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    } else if (doc_id_datum.is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("docid and rowkey can't both be null", K(ret));
    } else {
      ObRowkey table_rowkey(&obj_ptr[doc_id_idx], 1);
      if (OB_FAIL(aux_sorter_.add_item(table_rowkey))) {
        LOG_WARN("filter mbr failed", K(ret));
      }
    }
  }

  return ret;
}

int ObMulValueIndexLookupOp::fetch_index_table_rowkey()
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();

  if (is_inited_) {
  } else if (OB_FAIL(init_sort())) {
    LOG_WARN("fail to init sorter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      index_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
      if (OB_FAIL(rowkey_iter_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from index scan failed", K(ret));
        }
      } else if (OB_FAIL(save_doc_id_and_rowkey())) {
        LOG_WARN("process data table rowkey with das failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMulValueIndexLookupOp::save_aux_rowkeys()
{
  INIT_SUCC(ret);

  doc_id_scan_param_.key_ranges_.reset();
  const ObRowkey *idx_row = nullptr;

  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  int64_t default_row_batch_cnt  = simulate_batch_row_cnt > 0 ? simulate_batch_row_cnt : MAX_NUM_PER_BATCH;

  if (OB_FAIL(aux_sorter_.do_sort(true))) {
    LOG_WARN("do docid sort failed", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < default_row_batch_cnt; ++i) {
    if (OB_FAIL(aux_sorter_.get_next_item(idx_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next sorted item", K(ret), K(i));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (aux_last_rowkey_ != *idx_row) {
      ObNewRange lookup_range;
      uint64_t ref_table_id = doc_id_lookup_ctdef_->ref_table_id_;
      if (OB_FAIL(lookup_range.build_range(ref_table_id, *idx_row))) {
        LOG_WARN("build lookup range failed", K(ret), K(ref_table_id), K(*idx_row));
      } else if (OB_FAIL(doc_id_scan_param_.key_ranges_.push_back(lookup_range))) {
        LOG_WARN("store lookup key range failed", K(ret), K(doc_id_scan_param_));
      }
      aux_last_rowkey_ = *idx_row;
      LOG_DEBUG("build data table range", K(ret), K(*idx_row), K(lookup_range), K(doc_id_scan_param_.key_ranges_.count()));
    }
  }
  return ret;
}

int ObMulValueIndexLookupOp::save_rowkeys()
{
  int ret = OB_SUCCESS;
  ObStoreRowkey src_key;
  const ObRowkey *idx_row = NULL;
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  int64_t default_row_batch_cnt  = simulate_batch_row_cnt > 0 ? simulate_batch_row_cnt : MAX_NUM_PER_BATCH;
  for (int64_t i = 0; OB_SUCC(ret) && i < default_row_batch_cnt; ++i) {
    if (OB_FAIL(sorter_.get_next_item(idx_row))) {
      if (ret == OB_ITER_END) {
        ret = i > 0 ? OB_SUCCESS : ret;
      } else if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next sorted item", K(ret), K(i));
      }
    } else if (last_rowkey_ != *idx_row) {
      int64_t group_idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < index_ctdef_->result_output_.count(); ++i) {
        ObObj tmp_obj;
        ObExpr *expr = index_ctdef_->result_output_.at(i);
        if (T_PSEUDO_GROUP_ID == expr->type_) {
          group_idx = expr->locate_expr_datum(*lookup_rtdef_->eval_ctx_).get_int();
        }
      }

      ObNewRange lookup_range;
      uint64_t ref_table_id = lookup_ctdef_->ref_table_id_;
      if (OB_FAIL(lookup_range.build_range(ref_table_id, *idx_row))) {
        LOG_WARN("build lookup range failed", K(ret), K(ref_table_id), K(*idx_row));
      } else if (FALSE_IT(lookup_range.group_idx_ = group_idx)) {
      } else if (OB_FAIL(scan_param_.key_ranges_.push_back(lookup_range))) {
        LOG_WARN("store lookup key range failed", K(ret), K(scan_param_));
      }
      last_rowkey_ = *idx_row;
      LOG_DEBUG("build data table range", K(ret), K(*idx_row), K(lookup_range), K(scan_param_.key_ranges_.count()));
    }
  }
  return ret;
}

int ObMulValueIndexLookupOp::get_aux_table_rowkey()
{
  INIT_SUCC(ret);

  if (OB_FAIL(fetch_rowkey_from_aux())) {
    LOG_WARN("fetch rowkey from doc-rowkey table failed", K(ret));
  } else if (OB_FAIL(save_rowkeys())) {
    LOG_WARN("store rowkeys failed", K(ret));
  }

  return ret;
}


int ObMulValueIndexLookupOp::fetch_rowkey_from_aux()
{
  INIT_SUCC(ret);

  ObITabletScan &tsc_service = get_tsc_service();
  ObNewRowIterator *&storage_iter = get_aux_lookup_iter();

  if (aux_key_count_ == 0) {
    //do nothing
  } else if (storage_iter == nullptr) {
    //first index lookup, init scan param and do table scan
    if (OB_FAIL(set_doc_id_idx_lookup_param(
        doc_id_lookup_ctdef_, doc_id_lookup_rtdef_, doc_id_scan_param_, doc_id_idx_tablet_id_, ls_id_))) {
      LOG_WARN("failed to init doc id lookup scan param", K(ret));
    } else if (OB_FAIL(save_aux_rowkeys())) {
      LOG_WARN("failed to save aux keys failed", K(ret));
    } else if (OB_FAIL(tsc_service.table_scan(doc_id_scan_param_,
                       storage_iter))) {
      if (OB_SNAPSHOT_DISCARDED == ret && doc_id_scan_param_.fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to scan table", K(doc_id_scan_param_), K(ret));
      }
    }
  } else {
    const ObTabletID &storage_tablet_id = doc_id_scan_param_.tablet_id_;
    doc_id_scan_param_.need_switch_param_ = (storage_tablet_id.is_valid() && storage_tablet_id != tablet_id_ ? true : false);
    doc_id_scan_param_.tablet_id_ = tablet_id_;
    doc_id_scan_param_.ls_id_ = ls_id_;
    if (OB_FAIL(save_aux_rowkeys())) {
      LOG_WARN("failed to save aux keys failed", K(ret));
    } else if (OB_FAIL(tsc_service.table_rescan(doc_id_scan_param_, storage_iter))) {
      LOG_WARN("table_rescan scan iter failed", K(ret));
    }
  }

  if (aux_key_count_ > 0) {
    while (OB_SUCC(ret)) {
      doc_id_lookup_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
      if (OB_FAIL(storage_iter->get_next_row())) {
        if (OB_ITER_END != ret) {
            LOG_WARN("get next row from index scan failed", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ObObj *obj_ptr = nullptr;
        ObArenaAllocator allocator("MulvalLookup");

        const storage::ObTableReadInfo& read_info = lookup_ctdef_->table_param_.get_read_info();
        int64_t main_rowkey_column_cnt = read_info.get_schema_rowkey_count();

        if (OB_ISNULL(obj_ptr = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * main_rowkey_column_cnt)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate buffer failed", K(ret), K(main_rowkey_column_cnt));
        } else {
          obj_ptr = new(obj_ptr) ObObj[main_rowkey_column_cnt];
        }

        for (int64_t i = 0; OB_SUCC(ret) && i < main_rowkey_column_cnt; ++i) {
          ObExpr *expr = doc_id_lookup_ctdef_->result_output_.at(i);
          if (T_PSEUDO_GROUP_ID == expr->type_) {
            // do nothing
          } else {
            ObDatum &rowkey_datum = expr->locate_expr_datum(*doc_id_lookup_rtdef_->eval_ctx_);
            if (OB_FAIL(rowkey_datum.to_obj(obj_ptr[i], expr->obj_meta_, expr->obj_datum_map_))) {
              LOG_WARN("convert datum to obj failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObRowkey table_rowkey(obj_ptr, main_rowkey_column_cnt);
          if (OB_FAIL(sorter_.add_item(table_rowkey))) {
            LOG_WARN("filter mbr failed", K(ret));
          } else {
            LOG_TRACE("add rowkey success", K(table_rowkey), K(obj_ptr), K(obj_ptr[0]), K(main_rowkey_column_cnt));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sorter_.do_sort(true))) {
    LOG_WARN("do rowkey sort failed", K(ret));
  } else {
    lookup_rowkey_cnt_ = 0;
  }

  return ret;
}

int ObMulValueIndexLookupOp::reuse_scan_iter(bool need_switch_param)
{
  int ret = OB_SUCCESS;

  ObITabletScan &tsc_service = get_tsc_service();
  doc_id_scan_param_.need_switch_param_ = need_switch_param;

  // reset var for multi value
  aux_last_rowkey_.reset();
  last_rowkey_.reset();

  if (OB_FAIL(ObDomainIndexLookupOp::reuse_scan_iter())) {
    LOG_WARN("failed to reuse scan iter", K(ret));
  } else if (OB_FAIL(tsc_service.reuse_scan_iter(doc_id_scan_param_.need_switch_param_, rowkey_iter_))) {
    LOG_WARN("failed to reuse scan iter", K(ret));
  } else if (nullptr != get_aux_lookup_iter()) {
    doc_id_scan_param_.key_ranges_.reuse();
    doc_id_scan_param_.ss_key_ranges_.reuse();
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
