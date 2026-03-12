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
#include "ob_das_search_context.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase
{
namespace sql
{

void ObDASSearchCtx::reset()
{
  for (int64_t i = 0; i < op_store_.count(); ++i) {
    if (OB_NOT_NULL(op_store_.at(i))) {
      op_store_.at(i)->close();
      op_store_.at(i)->~ObIDASSearchOp();
      op_store_.at(i) = nullptr;
    }
  }
  op_store_.reset();
  op_count_ = 0;
  rowid_meta_.reset();
  bm25_param_cache_.reset();
}

int ObDASSearchCtx::init(
  const sql::ExprFixedArray &rowid_exprs,
  const sql::ExprFixedArray &output,
  bool use_dynamic_pruning)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  int64_t logical_row_cnt = -1;
  int64_t physical_row_cnt = -1;
  const ObAccessService *access_service = nullptr;
  const ObDASScanCtDef *scan_ctdef = nullptr;
  ObDASScanRtDef *scan_rtdef = nullptr;
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  const common::ObTabletID &tablet_id = root_das_task_.get_scan_param().tablet_id_;
  common::ObNewRange scan_range;
  ObSimpleBatch batch;
  ObTableScanParam est_param;
  ObSEArray<ObEstRowCountRecord, 1> est_records;
  ObTableScanRange table_scan_range;
  if (OB_ISNULL(scan_rtdef = static_cast<ObDASScanRtDef *>(root_das_task_.get_rtdef())) ||
      OB_ISNULL(scan_ctdef = static_cast<const ObDASScanCtDef *>(root_das_task_.get_ctdef())) ||
      OB_ISNULL(eval_ctx_ = root_das_task_.get_rtdef()->eval_ctx_) ||
      OB_ISNULL(access_service = MTL(ObAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(rowid_meta_.init(rowid_exprs, 0))) {
    LOG_WARN("failed to init rowid meta", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(ObBitVector::memory_size(eval_ctx_->max_batch_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mock skip", K(ret), K(eval_ctx_->max_batch_size_));
  } else {
    mock_skip_ = to_bit_vector(buf);
    mock_skip_->init(eval_ctx_->max_batch_size_);
    rowid_exprs_ = &rowid_exprs;
    output_ = &output;
    use_dynamic_pruning_ = use_dynamic_pruning;
    rowid_type_ = DAS_ROWID_TYPE_UINT64; // TODO: determine by actual rowid type

    // tablet full row count estimation
    est_param.index_id_ = scan_ctdef->ref_table_id_;
    est_param.scan_flag_ = scan_rtdef->scan_flag_;
    est_param.tablet_id_ = tablet_id;
    est_param.ls_id_ = ls_id_;
    est_param.schema_version_ = scan_ctdef->schema_version_;
    est_param.frozen_version_ = GET_BATCH_ROWS_READ_SNAPSHOT_VERSION;
    scan_range.table_id_ = scan_ctdef->ref_table_id_;
    scan_range.set_whole_range();
    batch.type_ = ObSimpleBatch::T_SCAN;
    batch.range_ = &scan_range;

    if (OB_FAIL(table_scan_range.init(est_param, batch, allocator_))) {
      LOG_WARN("failed to init table scan range", K(ret));
    } else if (OB_FAIL(access_service->estimate_row_count(est_param, table_scan_range, timeout_us, est_records, logical_row_cnt, physical_row_cnt))) {
      LOG_WARN("failed to estimate row count", K(ret));
    } else if (FALSE_IT(table_row_count_ = ObDASSearchCost(logical_row_cnt))) {
    } else if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid search ctx", K(ret));
    }
  }
  return ret;
}

int ObDASSearchCtx::get_related_tablet_id(const ObDASScalarScanCtDef *scalar_ctdef, common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(root_das_task_.get_related_tablet_id(scalar_ctdef, tablet_id))) {
    LOG_WARN("failed to get tablet id", K(scalar_ctdef));
  }
  return ret;
}

int ObDASSearchCtx::init_scan_param(
    common::ObTabletID &tablet_id,
    const ObDASScalarScanCtDef *ctdef,
    ObDASScalarScanRtDef *rtdef,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef) || OB_ISNULL(rtdef->p_pd_expr_op_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else {
    uint64_t tenant_id = MTL_ID();
    scan_param.tenant_id_ = tenant_id;
    scan_param.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
    scan_param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
    scan_param.scan_tasks_.set_attr(ObMemAttr(tenant_id, "ScanParamET"));
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = ctdef->is_get_;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &rtdef->scan_allocator_;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.ls_id_ = ls_id_;
    scan_param.tablet_id_ = tablet_id;
    scan_param.enable_new_false_range_ = ctdef->enable_new_false_range_;

    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_NOT_NULL(tx_desc_)) {
      scan_param.tx_id_ = tx_desc_->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }

    if (OB_NOT_NULL(snapshot_)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot_))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null snapshot", K(ret));
    }

    if (FAILEDx(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to init column ids", K(ret));
    } else if (OB_FAIL(scan_param.key_ranges_.assign(rtdef->key_ranges_))) {
      LOG_WARN("failed to assign key ranges", K(ret));
    } else {
      LOG_TRACE("init scan param key ranges", K(scan_param.key_ranges_));
    }
  }
  return ret;
}

int ObDASSearchCtx::lower_bound_in_frame(const ObDASRowID &target, const ObIArray<ObExpr *> *rowid_exprs,
                                         const int64_t count, int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowid_exprs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr rowid exprs", K(ret));
  } else if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr eval ctx", K(ret));
  } else {
    idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && idx < count && i < rowid_exprs->count(); ++i) {
      ObDatum target_datum;
      ObIVector *vec = nullptr;
      ObExpr *expr = rowid_exprs->at(i);

      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr expr", K(ret));
      } else if (OB_ISNULL(vec = expr->get_vector(*eval_ctx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr vector", K(ret));
      } else if (OB_FAIL(get_datum_from_rowid(target, target_datum, i))) {
        LOG_WARN("failed to get datum from rowid", K(ret), K(i));
      } else {
        if (OB_FAIL(ObDASSearchUtils::binary_search_lower_bound(*eval_ctx_, count, *expr, target_datum, idx))) {
          LOG_WARN("failed to binary search", K(ret));
        } else if (idx == count) {
          ret = OB_ITER_END;
        } else {
          // check if we found the lower bound
          // binary_search_lower_bound ensures vec[idx] >= target
          // if vec[idx] > target, then row[idx] > target, we found the lower bound
          // if vec[idx] == target, we need to check next column
          int cmp = 0;
          // we need to get datum again to check equality
          ObDatum datum;
          if (OB_FAIL(ObDASSearchUtils::get_datum(*expr, *eval_ctx_, idx, datum))) {
            LOG_WARN("failed to get datum", K(ret));
          } else if (OB_FAIL(expr->basic_funcs_->null_first_cmp_(datum, target_datum, cmp))) {
            LOG_WARN("failed to compare datums", K(ret));
          } else if (cmp > 0) {
            i = rowid_exprs->count();
          }
        }
      }
    }
  }
  return ret;
}

int ObDASSearchCtx::estimate_row_count(const ObDASScalarScanCtDef *ctdef, const ObDASScalarScanRtDef *rtdef, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  int64_t logical_row_cnt = 0;
  int64_t physical_row_cnt = 0;
  common::ObTabletID tablet_id;
  const ObAccessService *access_service = nullptr;
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  ObSimpleBatch batch;
  ObTableScanParam est_param;
  ObSEArray<ObEstRowCountRecord, 1> est_records;
  ObTableScanRange table_scan_range;
  row_count = 0;
  if (OB_ISNULL(ctdef) ||
      OB_ISNULL(rtdef) ||
      OB_ISNULL(access_service = MTL(ObAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (ctdef->enable_new_false_range_ && rtdef->key_ranges_.empty()) {
    // false range
    row_count = 0;
  } else if (rtdef->key_ranges_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty key ranges", K(ret));
  } else if (OB_FAIL(get_related_tablet_id(ctdef, tablet_id))) {
    LOG_WARN("failed to get related tablet id", K(ret));
  } else {
    est_param.index_id_ = ctdef->ref_table_id_;
    est_param.scan_flag_ = rtdef->scan_flag_;
    est_param.tablet_id_ = tablet_id;
    est_param.ls_id_ = ls_id_;
    est_param.schema_version_ = ctdef->schema_version_;
    est_param.frozen_version_ = GET_BATCH_ROWS_READ_SNAPSHOT_VERSION;
    batch.type_ = ObSimpleBatch::T_SCAN;
    batch.range_ = &rtdef->key_ranges_.at(0);

    if (OB_FAIL(table_scan_range.init(est_param, batch, allocator_))) {
      LOG_WARN("failed to init table scan range", K(ret));
    } else if (OB_FAIL(access_service->estimate_row_count(est_param, table_scan_range, timeout_us, est_records, logical_row_cnt, physical_row_cnt))) {
      LOG_WARN("failed to estimate row count", K(ret));
    } else {
      row_count = logical_row_cnt;
    }
  }
  return ret;
}

int ObDASSearchCtx::compare_uint64_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2,
                                         const RowMeta &rowid_meta, const ExprFixedArray &rowid_exprs,
                                         int &cmp)
{
  ObDASRowIDCmp<DAS_ROWID_TYPE_UINT64, true> cmp_func(rowid_meta, rowid_exprs);
  return cmp_func.cmp(rowid1, rowid2, cmp);
}

int ObDASSearchCtx::compare_compact_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2,
                                          const RowMeta &rowid_meta, const ExprFixedArray &rowid_exprs,
                                          int &cmp)
{
  ObDASRowIDCmp<DAS_ROWID_TYPE_COMPACT, true> cmp_func(rowid_meta, rowid_exprs);
  return cmp_func.cmp(rowid1, rowid2, cmp);
}

int ObDASSearchCtx::deep_copy_uint64_rowid(const ObDASRowID &src, ObDASRowID &dst,
                                           common::ObIAllocator &alloc)
{
  UNUSED(alloc);
  int ret = OB_SUCCESS;
  dst = src;
  return ret;
}

int ObDASSearchCtx::deep_copy_compact_rowid(const ObDASRowID &src, ObDASRowID &dst,
                                            common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  dst = src;
  if (OB_LIKELY(src.is_normal())) {
    const ObCompactRow *src_row = src.get_compact_row();
    if (OB_ISNULL(src_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src compact row is null", K(ret));
    } else {
      // TODO: optimize by reusing dst's underlying buffer
      const int64_t row_size = src_row->get_row_size();
      void *buf = nullptr;
      if (OB_ISNULL(buf = alloc.alloc(row_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(row_size));
      } else {
        MEMCPY(buf, src_row, row_size);
        dst.set_compact_row(reinterpret_cast<const ObCompactRow *>(buf));
      }
    }
  }
  return ret;
}

template<typename StoreType>
int ObDASSearchCtx::create_rowid_store(ObDASSearchCtx &ctx, int64_t capacity, RowIDStore *&store,
                                       common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store = OB_NEWx(StoreType, &alloc, ctx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create rowid store", K(ret));
  } else if (OB_FAIL(store->init(capacity))) {
    LOG_WARN("failed to init rowid store", K(ret), K(capacity));
    store = nullptr;
  }
  return ret;
}

int ObDASSearchCtx::uint64_rowid_to_expr(const ObDASRowID &rowid, const ExprFixedArray &rowid_exprs,
                                         const RowMeta &rowid_meta, ObEvalCtx &eval_ctx, int64_t batch_idx)
{
  UNUSED(rowid_meta);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowid_exprs.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uint64 rowid should have exactly one expr", K(ret), K(rowid_exprs.count()));
  } else {
    ObExpr *expr = rowid_exprs.at(0);
    ObIVector *vec = nullptr;
    if (OB_ISNULL(vec = expr->get_vector(eval_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector is null", K(ret));
    } else {
      vec->set_uint(batch_idx, rowid.get_uint64());
    }
  }
  return ret;
}

int ObDASSearchCtx::compact_rowid_to_expr(const ObDASRowID &rowid, const ExprFixedArray &rowid_exprs,
                                          const RowMeta &rowid_meta, ObEvalCtx &eval_ctx, int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  const ObCompactRow *compact_row = rowid.get_compact_row();
  if (OB_ISNULL(compact_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compact row is null", K(ret));
  } else {
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < rowid_exprs.count(); ++col_idx) {
      ObExpr *expr = rowid_exprs.at(col_idx);
      ObIVector *vec = nullptr;
      if (OB_ISNULL(vec = expr->get_vector(eval_ctx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vector is null", K(ret), K(col_idx));
      } else if (OB_FAIL(vec->from_row(rowid_meta, compact_row, batch_idx, col_idx))) {
        LOG_WARN("failed to set vector from compact row", K(ret), K(col_idx), K(batch_idx));
      }
    }
  }
  return ret;
}

int ObDASSearchCtx::write_datum_to_uint64_rowid(const ObDatum &datum, const RowMeta &meta,
                                                const ExprFixedArray &rowid_exprs,
                                                ObDASRowID &rowid, common::ObIAllocator &alloc)
{
  UNUSED(meta);
  UNUSED(rowid_exprs);
  UNUSED(alloc);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowid_exprs.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowid to write to should have exactly one expr", K(ret), K(rowid_exprs.count()));
  } else {
    rowid.set_uint64(datum.get_uint());
  }
  return ret;
}

int ObDASSearchCtx::write_datum_to_compact_rowid(const ObDatum &datum, const RowMeta &meta,
                                                 const ExprFixedArray &rowid_exprs,
                                                 ObDASRowID &rowid, common::ObIAllocator &alloc)
{
  // TODO: optimize by reusing rowid's underlying buffer
  int ret = OB_SUCCESS;
  int64_t buf_size = ObCompactRow::calc_max_row_size(rowid_exprs, 0);
  void *buffer = nullptr;
  if (OB_UNLIKELY(rowid_exprs.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowid to write to should have exactly one expr", K(ret), K(rowid_exprs.count()));
  } else if (OB_ISNULL(buffer = alloc.alloc(buf_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for rowid buffer", K(ret), K(buf_size));
  } else {
    ObCompactRow *row = new (buffer) ObCompactRow;
    row->init(meta);
    row->set_cell_payload(meta, 0, datum.ptr_, datum.len_);
    rowid.set_compact_row(row);
  }
  return ret;
}

int ObDASSearchCtx::get_datum_from_uint64_rowid(const ObDASRowID &rowid, const RowMeta &meta,
                                                ObDatum &datum, int64_t col_idx)
{
  UNUSED(meta);
  UNUSED(col_idx);
  int ret = OB_SUCCESS;
  datum.ptr_ = reinterpret_cast<const char *>(&rowid.uint64_rowid_);
  datum.pack_ = sizeof(uint64_t);
  return ret;
}

int ObDASSearchCtx::get_datum_from_compact_rowid(const ObDASRowID &rowid, const RowMeta &meta,
                                                 ObDatum &datum, int64_t col_idx)
{
  int ret = OB_SUCCESS;
  const ObCompactRow *row = rowid.get_compact_row();
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compact row is null", K(ret));
  } else if (OB_UNLIKELY(col_idx < 0 || col_idx >= meta.col_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col idx out of range", K(ret), K(col_idx), K(meta.col_cnt_));
  } else {
    datum = row->get_datum(meta, col_idx);
  }
  return ret;
}

const ObDASSearchCtx::RowIDFuncs ObDASSearchCtx::all_rowid_funcs_[DAS_ROWID_TYPE_MAX] = {
  {
    compare_uint64_rowid,
    deep_copy_uint64_rowid,
    create_rowid_store<UInt64RowIDStore>,
    uint64_rowid_to_expr,
    write_datum_to_uint64_rowid,
    get_datum_from_uint64_rowid,
  },
  {
    compare_compact_rowid,
    deep_copy_compact_rowid,
    create_rowid_store<CompactRowIDStore>,
    compact_rowid_to_expr,
    write_datum_to_compact_rowid,
    get_datum_from_compact_rowid,
  },
};

} // namespace sql
} // namespace oceanbase
