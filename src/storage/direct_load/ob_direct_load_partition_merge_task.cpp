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
#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_partition_merge_task.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/direct_load/ob_direct_load_conflict_check.h"
#include "storage/direct_load/ob_direct_load_data_insert.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace table;
using namespace observer;

/**
 * ObDirectLoadPartitionMergeTask
 */

ObDirectLoadPartitionMergeTask::ObDirectLoadPartitionMergeTask()
  : ctx_(nullptr),
    merge_param_(nullptr),
    merge_ctx_(nullptr),
    parallel_idx_(-1),
    affected_rows_(0),
    allocator_("TLD_ParMT"),
    insert_tablet_ctx_(nullptr),
    sql_statistics_(nullptr),
    is_stop_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadPartitionMergeTask::~ObDirectLoadPartitionMergeTask()
{
}

class ObStoreRowIteratorWrapper : public ObIStoreRowIterator
{
public:
  ObStoreRowIteratorWrapper(observer::ObTableLoadTableCtx *ctx, ObIStoreRowIterator *inner_iter) :
    ctx_(ctx), inner_iter_(inner_iter) {
  }

  int get_next_row(const blocksstable::ObDatumRow *&row)
  {
    int ret = inner_iter_->get_next_row(row);
    if (ret == OB_SUCCESS) {
      ATOMIC_AAF(&ctx_->job_stat_->store_.merge_stage_write_rows_, 1);
    }
    return ret;
  }

private:
  observer::ObTableLoadTableCtx *ctx_;
  ObIStoreRowIterator *inner_iter_;
};

int ObDirectLoadPartitionMergeTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionMergeTask not init", KR(ret), KP(this));
  } else {
    int64_t slice_id = 0;
    const ObTabletID &tablet_id = merge_ctx_->get_tablet_id();
    ObIStoreRowIterator *row_iter = nullptr;
    ObMacroDataSeq block_start_seq;
    block_start_seq.set_parallel_degree(parallel_idx_);
    if (OB_FAIL(
          merge_param_->insert_table_ctx_->get_tablet_context(tablet_id, insert_tablet_ctx_))) {
      LOG_WARN("fail to get tablet context ", KR(ret), K(tablet_id), K(block_start_seq));
    } else if (insert_tablet_ctx_->get_online_opt_stat_gather() &&
               OB_FAIL(merge_param_->insert_table_ctx_->get_sql_statistics(sql_statistics_))) {
      LOG_WARN("fail to get sql statistics", KR(ret));
    } else if (insert_tablet_ctx_->has_lob_storage() && OB_FAIL(lob_builder_.init(insert_tablet_ctx_))) {
      LOG_WARN("fail to inner init lob builder", KR(ret));
    } else if (OB_FAIL(construct_row_iter(allocator_, row_iter))) {
      LOG_WARN("fail to construct row iter", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx_->open_sstable_slice(block_start_seq, slice_id))) {
      LOG_WARN("fail to construct sstable slice ", KR(ret), K(slice_id), K(block_start_seq));
    } else {
      LOG_INFO("add sstable slice begin", K(tablet_id), K(parallel_idx_), K(slice_id));
      ObStoreRowIteratorWrapper row_iter_wrapper(ctx_, row_iter);
      if (OB_UNLIKELY(is_stop_)) {
        ret = OB_CANCELED;
        LOG_WARN("merge task canceled", KR(ret));
      } else if (OB_FAIL(insert_tablet_ctx_->fill_sstable_slice(slice_id, row_iter_wrapper, affected_rows_))) {
         LOG_WARN("fail to fill sstable slice", KR(ret));
      } else if (OB_FAIL(insert_tablet_ctx_->close_sstable_slice(slice_id))) {
        LOG_WARN("fail to close writer", KR(ret));
      } else if (insert_tablet_ctx_->has_lob_storage() && OB_FAIL(lob_builder_.close())) {
        LOG_WARN("fail to close lob_builder", KR(ret));
      } else {
        insert_tablet_ctx_->inc_row_count(affected_rows_);
      }
      LOG_INFO("add sstable slice end", KR(ret), K(tablet_id), K(parallel_idx_), K(affected_rows_));
    }
    if (OB_NOT_NULL(row_iter)) {
      row_iter->~ObIStoreRowIterator();
      allocator_.free(row_iter);
      row_iter = nullptr;
    }
    if (OB_SUCC(ret)) {
      bool is_ready = false;
      if (OB_FAIL(merge_ctx_->inc_finish_count(is_ready))) {
        LOG_WARN("fail to inc finish count", KR(ret));
      } else if (is_ready) {
        if (insert_tablet_ctx_->need_rescan()) {
          if (OB_FAIL(insert_tablet_ctx_->calc_range(merge_param_->fill_cg_thread_cnt_))) {
            LOG_WARN("fail to calc range", KR(ret));
          }
        } else if (insert_tablet_ctx_->need_del_lob()) {
          // do nothing
        } else if (OB_FAIL(insert_tablet_ctx_->close())) {
          LOG_WARN("fail to close", KR(ret));
        }
      }
    }
  }
  return ret;
}

void ObDirectLoadPartitionMergeTask::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRescanTask not init", KR(ret), KP(this));
  } else {
    ObDirectLoadInsertTabletContext *tablet_ctx = nullptr;
    const ObTabletID &tablet_id = merge_ctx_->get_tablet_id();
    if (OB_FAIL(merge_param_->insert_table_ctx_->get_tablet_context(tablet_id, tablet_ctx))) {
      LOG_WARN("fail to get tablet context ", KR(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_ctx->cancel())) {
      LOG_WARN("fail to cancel fill task", K(ret));
    } else {
      is_stop_ = true;
    }
  }
  // ignore ret
}

/**
 * ObDirectLoadPartitionRangeMergeTask
 */

ObDirectLoadPartitionRangeMergeTask::RowIterator::RowIterator()
  : data_iter_(nullptr), rowkey_column_num_(0)
{
}

ObDirectLoadPartitionRangeMergeTask::RowIterator::~RowIterator()
{
  if (data_iter_ != nullptr) {
    ob_delete(data_iter_);
  }
}

int ObDirectLoadPartitionRangeMergeTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param,
  ObDirectLoadTabletMergeCtx *merge_ctx,
  ObDirectLoadOriginTable *origin_table,
  ObTableLoadSqlStatistics *sql_statistics,
  ObDirectLoadLobBuilder &lob_builder,
  const ObIArray<ObDirectLoadSSTable *> &sstable_array,
  const ObDatumRange &range,
  ObDirectLoadInsertTabletContext *insert_tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionRangeMergeTask::RowIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || !range.is_valid() ||
                         nullptr == insert_tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), K(sstable_array), K(range),
             KP(insert_tablet_ctx));
  } else {
    if (OB_FAIL(inner_init(insert_tablet_ctx, sql_statistics, lob_builder))) {
      LOG_WARN("fail to inner init", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx->init_datum_row(datum_row_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      if (merge_ctx->merge_with_origin_data()) {
        // init data_fuse_
        ObDirectLoadDataFuseParam data_fuse_param;
        data_fuse_param.tablet_id_ = merge_ctx->get_tablet_id();
        data_fuse_param.store_column_count_ = merge_param.store_column_count_;
        data_fuse_param.table_data_desc_ = merge_param.table_data_desc_;
        data_fuse_param.datum_utils_ = merge_param.datum_utils_;
        data_fuse_param.dml_row_handler_ = merge_param.dml_row_handler_;
        ObDirectLoadSSTableDataFuse *data_fuse = nullptr;
        if (OB_ISNULL(data_fuse = OB_NEW(ObDirectLoadSSTableDataFuse, ObMemAttr(MTL_ID(), "TLD_PRMT")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else if (FALSE_IT(data_iter_ = data_fuse)) {
        } else if (OB_FAIL(data_fuse->init(data_fuse_param, origin_table, sstable_array, range))) {
          LOG_WARN("fail to init ObDirectLoadSSTableDataFuse", K(ret));
        }
      } else if (merge_ctx->merge_with_conflict_check()) {
        ObDirectLoadConflictCheckParam conflict_check_param;
        conflict_check_param.tablet_id_ = merge_ctx->get_tablet_id();
        conflict_check_param.table_data_desc_ = merge_param.table_data_desc_;
        conflict_check_param.store_column_count_ = merge_param.store_column_count_;
        conflict_check_param.origin_table_ = origin_table;
        conflict_check_param.range_ = &range;
        conflict_check_param.col_descs_ = merge_param.col_descs_;
        conflict_check_param.lob_column_idxs_ = merge_param.lob_column_idxs_;
        conflict_check_param.datum_utils_ = merge_param.datum_utils_;
        conflict_check_param.dml_row_handler_ = merge_param.dml_row_handler_;
        ObDirectLoadSSTableConflictCheck *conflict_check = nullptr;
        if (OB_FAIL(merge_ctx->get_table_builder(conflict_check_param.builder_))) {
          LOG_WARN("fail to get table builder", K(ret));
        } else if (OB_ISNULL(conflict_check = OB_NEW(ObDirectLoadSSTableConflictCheck, ObMemAttr(MTL_ID(), "TLD_PRMT")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        } else if (FALSE_IT(data_iter_ = conflict_check)) {
        } else if (OB_FAIL(conflict_check->init(conflict_check_param, sstable_array))) {
          LOG_WARN("fail to init ObDirectLoadSSTableConflictCheck", K(ret));
        }
      } else {
        ObDirectLoadDataInsertParam data_insert_param;
        data_insert_param.tablet_id_ = merge_ctx->get_tablet_id();
        data_insert_param.store_column_count_ = merge_param.store_column_count_;
        data_insert_param.table_data_desc_ = merge_param.table_data_desc_;
        data_insert_param.datum_utils_ = merge_param.datum_utils_;
        data_insert_param.dml_row_handler_ = merge_param.dml_row_handler_;
        ObDirectLoadSSTableDataInsert *data_insert = nullptr;
        if (OB_ISNULL(data_insert = OB_NEW(ObDirectLoadSSTableDataInsert, ObMemAttr(MTL_ID(), "TLD_PRMT")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else if (FALSE_IT(data_iter_ = data_insert)) {
        } else if (OB_FAIL(data_insert->init(data_insert_param, sstable_array, range))) {
          LOG_WARN("fail to init ObDirectLoadSSTableDataInsert", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      rowkey_column_num_ = merge_param.rowkey_column_num_;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObDirectLoadPartitionRangeMergeTask::RowIterator::inner_get_next_row(ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRangeMergeTask::RowIterator not init", KR(ret), KP(this));
  } else {
    const ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(data_iter_->get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else {
      // copy rowkey columns
      for (int64_t i = 0; i < rowkey_column_num_; ++i) {
        datum_row_.storage_datums_[i] = datum_row->storage_datums_[i];
      }
      // copy normal columns
      for (int64_t i = rowkey_column_num_,
                   j = rowkey_column_num_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
           i < datum_row->count_; ++i, ++j) {
        datum_row_.storage_datums_[j] = datum_row->storage_datums_[i];
      }
      result_row = &datum_row_;
    }
  }
  return ret;
}

ObDirectLoadPartitionRangeMergeTask::ObDirectLoadPartitionRangeMergeTask()
  : origin_table_(nullptr), sstable_array_(nullptr), range_(nullptr)
{
}

ObDirectLoadPartitionRangeMergeTask::~ObDirectLoadPartitionRangeMergeTask() {}

int ObDirectLoadPartitionRangeMergeTask::init(ObTableLoadTableCtx *ctx,
                                              const ObDirectLoadMergeParam &merge_param,
                                              ObDirectLoadTabletMergeCtx *merge_ctx,
                                              ObDirectLoadOriginTable *origin_table,
                                              const ObIArray<ObDirectLoadSSTable *> &sstable_array,
                                              const ObDatumRange &range,
                                              int64_t parallel_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionRangeMergeTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx || !merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || !range.is_valid() || parallel_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), K(sstable_array), K(range),
             K(parallel_idx));
  } else {
    ctx_ = ctx;
    merge_param_ = &merge_param;
    merge_ctx_ = merge_ctx;
    parallel_idx_ = parallel_idx;
    origin_table_ = origin_table;
    sstable_array_ = &sstable_array;
    range_ = &range;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionRangeMergeTask::construct_row_iter(ObIAllocator &allocator,
                                                            ObIStoreRowIterator *&result_row_iter)
{
  int ret = OB_SUCCESS;
  result_row_iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRangeMergeTask not init", KR(ret), KP(this));
  } else {
    RowIterator *row_iter = nullptr;
    if (OB_ISNULL(row_iter = OB_NEWx(RowIterator, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new RowIterator", KR(ret));
    } else if (OB_FAIL(row_iter->init(*merge_param_, merge_ctx_, origin_table_,
                                      sql_statistics_, lob_builder_, *sstable_array_, *range_,
                                      insert_tablet_ctx_))) {
      LOG_WARN("fail to init row iter", KR(ret));
    } else {
      result_row_iter = row_iter;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != row_iter) {
        row_iter->~RowIterator();
        allocator.free(row_iter);
        row_iter = nullptr;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadPartitionRangeMultipleMergeTask
 */

ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator::RowIterator()
  : data_iter_(nullptr), rowkey_column_num_(0)
{
}

ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator::~RowIterator()
{
  if (data_iter_ != nullptr) {
    ob_delete(data_iter_);
  }
}

int ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param,
  ObDirectLoadTabletMergeCtx *merge_ctx,
  ObDirectLoadOriginTable *origin_table,
  ObTableLoadSqlStatistics *sql_statistics,
  ObDirectLoadLobBuilder &lob_builder,
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
  const ObDatumRange &range,
  ObDirectLoadInsertTabletContext *insert_tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator init twice", KR(ret),
             KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || !range.is_valid() ||
                         nullptr == insert_tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), K(sstable_array), K(range),
             KP(insert_tablet_ctx));
  } else {
    if (OB_FAIL(inner_init(insert_tablet_ctx, sql_statistics, lob_builder))) {
      LOG_WARN("fail to inner init", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx->init_datum_row(datum_row_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      if (merge_ctx->merge_with_origin_data()) {
        // init data_fuse_
        ObDirectLoadDataFuseParam data_fuse_param;
        data_fuse_param.tablet_id_ = merge_ctx->get_tablet_id();
        data_fuse_param.store_column_count_ = merge_param.store_column_count_;
        data_fuse_param.table_data_desc_ = merge_param.table_data_desc_;
        data_fuse_param.datum_utils_ = merge_param.datum_utils_;
        data_fuse_param.dml_row_handler_ = merge_param.dml_row_handler_;
        ObDirectLoadMultipleSSTableDataFuse *data_fuse = nullptr;
        if (OB_ISNULL(data_fuse = OB_NEW(ObDirectLoadMultipleSSTableDataFuse, ObMemAttr(MTL_ID(), "TLD_PRMMT")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else if (FALSE_IT(data_iter_ = data_fuse)) {
        } else if (OB_FAIL(data_fuse->init(data_fuse_param, origin_table, sstable_array, range))) {
          LOG_WARN("fail to init ObDirectLoadMultipleSSTableDataFuse", K(ret));
        }
      } else if (merge_ctx->merge_with_conflict_check()) {
        ObDirectLoadConflictCheckParam conflict_check_param;
        conflict_check_param.tablet_id_ = merge_ctx->get_tablet_id();
        conflict_check_param.table_data_desc_ = merge_param.table_data_desc_;
        conflict_check_param.store_column_count_ = merge_param.store_column_count_;
        conflict_check_param.origin_table_ = origin_table;
        conflict_check_param.range_ = &range;
        conflict_check_param.col_descs_ = merge_param.col_descs_;
        conflict_check_param.lob_column_idxs_ = merge_param.lob_column_idxs_;
        conflict_check_param.datum_utils_ = merge_param.datum_utils_;
        conflict_check_param.dml_row_handler_ = merge_param.dml_row_handler_;
        ObDirectLoadMultipleSSTableConflictCheck *conflict_check = nullptr;
        if (OB_FAIL(merge_ctx->get_table_builder(conflict_check_param.builder_))) {
          LOG_WARN("fail to get table builder", K(ret));
        } else if (OB_ISNULL(conflict_check = OB_NEW(ObDirectLoadMultipleSSTableConflictCheck, ObMemAttr(MTL_ID(), "TLD_PRMMT")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        } else if (FALSE_IT(data_iter_ = conflict_check)) {
        } else if (OB_FAIL(conflict_check->init(conflict_check_param, sstable_array))) {
          LOG_WARN("fail to init ObDirectLoadMultipleSSTableConflictCheck", K(ret));
        }
      } else {
        ObDirectLoadDataInsertParam data_insert_param;
        data_insert_param.tablet_id_ = merge_ctx->get_tablet_id();
        data_insert_param.store_column_count_ = merge_param.store_column_count_;
        data_insert_param.table_data_desc_ = merge_param.table_data_desc_;
        data_insert_param.datum_utils_ = merge_param.datum_utils_;
        data_insert_param.dml_row_handler_ = merge_param.dml_row_handler_;
        ObDirectLoadMultipleSSTableDataInsert *data_insert = nullptr;
        if (OB_ISNULL(data_insert = OB_NEW(ObDirectLoadMultipleSSTableDataInsert, ObMemAttr(MTL_ID(), "TLD_PRMT")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else if (FALSE_IT(data_iter_ = data_insert)) {
        } else if (OB_FAIL(data_insert->init(data_insert_param, sstable_array, range))) {
          LOG_WARN("fail to init ObDirectLoadMultipleSSTableDataInsert", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      rowkey_column_num_ = merge_param.rowkey_column_num_;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator::inner_get_next_row(
  ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator not init", KR(ret),
             KP(this));
  } else {
    const ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(data_iter_->get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else {
      // copy rowkey columns
      for (int64_t i = 0; i < rowkey_column_num_; ++i) {
        datum_row_.storage_datums_[i] = datum_row->storage_datums_[i];
      }
      // copy normal columns
      for (int64_t i = rowkey_column_num_,
                   j = rowkey_column_num_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
           i < datum_row->count_; ++i, ++j) {
        datum_row_.storage_datums_[j] = datum_row->storage_datums_[i];
      }
      result_row = &datum_row_;
    }
  }
  return ret;
}

ObDirectLoadPartitionRangeMultipleMergeTask::ObDirectLoadPartitionRangeMultipleMergeTask()
  : origin_table_(nullptr), sstable_array_(nullptr), range_(nullptr)
{
}

ObDirectLoadPartitionRangeMultipleMergeTask::~ObDirectLoadPartitionRangeMultipleMergeTask() {}

int ObDirectLoadPartitionRangeMultipleMergeTask::init(
  ObTableLoadTableCtx *ctx,
  const ObDirectLoadMergeParam &merge_param,
  ObDirectLoadTabletMergeCtx *merge_ctx,
  ObDirectLoadOriginTable *origin_table,
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
  const ObDatumRange &range,
  int64_t parallel_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionRangeMultipleMergeTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx || !merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || !range.is_valid() || parallel_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), K(sstable_array), K(range),
             K(parallel_idx));
  } else {
    ctx_ = ctx;
    merge_param_ = &merge_param;
    merge_ctx_ = merge_ctx;
    parallel_idx_ = parallel_idx;
    origin_table_ = origin_table;
    sstable_array_ = &sstable_array;
    range_ = &range;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionRangeMultipleMergeTask::construct_row_iter(
  ObIAllocator &allocator, ObIStoreRowIterator *&result_row_iter)
{
  int ret = OB_SUCCESS;
  result_row_iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRangeMultipleMergeTask not init", KR(ret), KP(this));
  } else {
    RowIterator *row_iter = nullptr;
    if (OB_ISNULL(row_iter = OB_NEWx(RowIterator, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new RowIterator", KR(ret));
    } else if (OB_FAIL(row_iter->init(*merge_param_, merge_ctx_, origin_table_,
                                      sql_statistics_, lob_builder_, *sstable_array_, *range_,
                                      insert_tablet_ctx_))) {
      LOG_WARN("fail to init row iter", KR(ret));
    } else {
      result_row_iter = row_iter;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != row_iter) {
        row_iter->~RowIterator();
        allocator.free(row_iter);
        row_iter = nullptr;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadPartitionHeapTableMergeTask
 */

ObDirectLoadPartitionHeapTableMergeTask::RowIterator::RowIterator()
  : deserialize_datums_(nullptr),
    deserialize_datum_cnt_(0),
    dml_row_handler_(nullptr)
{
}

ObDirectLoadPartitionHeapTableMergeTask::RowIterator::~RowIterator() {}

int ObDirectLoadPartitionHeapTableMergeTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param,
  const ObTabletID &tablet_id,
  ObDirectLoadExternalTable *external_table,
  ObTableLoadSqlStatistics *sql_statistics,
  ObDirectLoadLobBuilder &lob_builder,
  const ObTabletCacheInterval &pk_interval,
  ObDirectLoadInsertTabletContext *insert_tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMergeTask::RowIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || !tablet_id.is_valid() ||
                         nullptr == external_table || 0 == pk_interval.count() ||
                         nullptr == insert_tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), K(tablet_id), KP(external_table),
             K(pk_interval), KP(insert_tablet_ctx));
  } else {
    // init row iterator
    if (OB_FAIL(inner_init(insert_tablet_ctx, sql_statistics, lob_builder))) {
      LOG_WARN("fail to inner init", KR(ret));
    }
    // init scanner_
    else if (OB_FAIL(scanner_.init(merge_param.table_data_desc_.external_data_block_size_,
                                   merge_param.table_data_desc_.compressor_type_,
                                   external_table->get_fragments()))) {
      LOG_WARN("fail to init fragment scanner", KR(ret));
    }
    // init datum_row_
    else if (OB_FAIL(insert_tablet_ctx->init_datum_row(datum_row_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      deserialize_datums_ = datum_row_.storage_datums_ + merge_param.rowkey_column_num_ +
                            ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      deserialize_datum_cnt_ = merge_param.store_column_count_ - merge_param.rowkey_column_num_;
      pk_interval_ = pk_interval;
      dml_row_handler_ = merge_param.dml_row_handler_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMergeTask::RowIterator::inner_get_next_row(
  ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRangeMergeTask::RowIterator not init", KR(ret), KP(this));
  } else {
    const ObDirectLoadExternalRow *external_row = nullptr;
    uint64_t pk_seq = OB_INVALID_ID;
    if (OB_FAIL(scanner_.get_next_item(external_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row from merge_sort", KR(ret));
      }
    } else if (OB_FAIL(external_row->to_datums(deserialize_datums_, deserialize_datum_cnt_))) {
      LOG_WARN("fail to transfer datum row", KR(ret));
    } else if (OB_FAIL(pk_interval_.next_value(pk_seq))) {
      LOG_WARN("fail to get next pk seq", KR(ret));
    } else {
      // fill hide pk
      datum_row_.storage_datums_[0].set_int(pk_seq);
      result_row = &datum_row_;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml_row_handler_->handle_insert_row(*result_row))) {
        LOG_WARN("fail to handle insert row", KR(ret), KPC(result_row));
      }
    }
  }
  return ret;
}

ObDirectLoadPartitionHeapTableMergeTask::ObDirectLoadPartitionHeapTableMergeTask()
  : external_table_(nullptr)
{
}

ObDirectLoadPartitionHeapTableMergeTask::~ObDirectLoadPartitionHeapTableMergeTask() {}

int ObDirectLoadPartitionHeapTableMergeTask::init(ObTableLoadTableCtx *ctx,
                                                  const ObDirectLoadMergeParam &merge_param,
                                                  ObDirectLoadTabletMergeCtx *merge_ctx,
                                                  ObDirectLoadExternalTable *external_table,
                                                  const ObTabletCacheInterval &pk_interval,
                                                  int64_t parallel_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMergeTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx || !merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == external_table || parallel_idx < 0 ||
                         0 == pk_interval.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), KP(external_table),
             K(parallel_idx), K(pk_interval));
  } else {
    ctx_ = ctx;
    merge_param_ = &merge_param;
    merge_ctx_ = merge_ctx;
    parallel_idx_ = parallel_idx;
    external_table_ = external_table;
    pk_interval_ = pk_interval;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMergeTask::construct_row_iter(
  ObIAllocator &allocator, ObIStoreRowIterator *&result_row_iter)
{
  int ret = OB_SUCCESS;
  result_row_iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionHeapTableMergeTask not init", KR(ret), KP(this));
  } else {
    RowIterator *row_iter = nullptr;
    if (OB_ISNULL(row_iter = OB_NEWx(RowIterator, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new RowIterator", KR(ret));
    } else if (OB_FAIL(row_iter->init(*merge_param_, merge_ctx_->get_tablet_id(), external_table_,
                                      sql_statistics_, lob_builder_, pk_interval_,
                                      insert_tablet_ctx_))) {
      LOG_WARN("fail to init row iter", KR(ret));
    } else {
      result_row_iter = row_iter;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != row_iter) {
        row_iter->~RowIterator();
        allocator.free(row_iter);
        row_iter = nullptr;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadPartitionHeapTableMultipleMergeTask
 */

ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::RowIterator()
  : deserialize_datums_(nullptr),
    deserialize_datum_cnt_(0),
    dml_row_handler_(nullptr)
{
}

ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::~RowIterator() {}

int ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param,
  const ObTabletID &tablet_id,
  ObDirectLoadMultipleHeapTable *heap_table,
  ObTableLoadSqlStatistics *sql_statistics,
  ObDirectLoadLobBuilder &lob_builder,
  const ObTabletCacheInterval &pk_interval,
  ObDirectLoadInsertTabletContext *insert_tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator init twice", KR(ret),
             KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || !tablet_id.is_valid() ||
                         nullptr == heap_table || 0 == pk_interval.count() ||
                         nullptr == insert_tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), K(tablet_id), KPC(heap_table), K(pk_interval),
             KP(insert_tablet_ctx));
  } else {
    // init row iterator
    if (OB_FAIL(inner_init(insert_tablet_ctx, sql_statistics, lob_builder))) {
      LOG_WARN("fail to inner init", KR(ret));
    }
    // init scanner_
    else if (OB_FAIL(scanner_.init(heap_table, tablet_id, merge_param.table_data_desc_))) {
      LOG_WARN("fail to init tablet whole scanner", KR(ret));
    }
    // init datum_row_
    else if (OB_FAIL(insert_tablet_ctx->init_datum_row(datum_row_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      deserialize_datums_ = datum_row_.storage_datums_ + merge_param.rowkey_column_num_ +
                            ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      deserialize_datum_cnt_ = merge_param.store_column_count_ - merge_param.rowkey_column_num_;
      pk_interval_ = pk_interval;
      dml_row_handler_ = merge_param.dml_row_handler_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::inner_get_next_row(
  ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator not init", KR(ret),
             KP(this));
  } else {
    const ObDirectLoadMultipleExternalRow *external_row = nullptr;
    uint64_t pk_seq = OB_INVALID_ID;
    if (OB_FAIL(scanner_.get_next_row(external_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else if (OB_FAIL(external_row->to_datums(deserialize_datums_, deserialize_datum_cnt_))) {
      LOG_WARN("fail to transfer datum row", KR(ret));
    } else if (OB_FAIL(pk_interval_.next_value(pk_seq))) {
      LOG_WARN("fail to get next pk seq", KR(ret));
    } else {
      // fill hide pk
      datum_row_.storage_datums_[0].set_int(pk_seq);
      result_row = &datum_row_;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml_row_handler_->handle_insert_row(*result_row))) {
        LOG_WARN("fail to handle insert row", KR(ret), KPC(result_row));
      }
    }
  }
  return ret;
}

ObDirectLoadPartitionHeapTableMultipleMergeTask::ObDirectLoadPartitionHeapTableMultipleMergeTask()
{
}

ObDirectLoadPartitionHeapTableMultipleMergeTask::~ObDirectLoadPartitionHeapTableMultipleMergeTask()
{
}

int ObDirectLoadPartitionHeapTableMultipleMergeTask::init(
  ObTableLoadTableCtx *ctx,
  const ObDirectLoadMergeParam &merge_param,
  ObDirectLoadTabletMergeCtx *merge_ctx,
  ObDirectLoadMultipleHeapTable *heap_table,
  const ObTabletCacheInterval &pk_interval,
  int64_t parallel_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleMergeTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx || !merge_param.is_valid() ||
                         nullptr == merge_ctx || nullptr == heap_table ||
                         parallel_idx < 0 || 0 == pk_interval.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), KPC(heap_table),
             K(parallel_idx), K(pk_interval));
  } else {
    ctx_ = ctx;
    merge_param_ = &merge_param;
    merge_ctx_ = merge_ctx;
    parallel_idx_ = parallel_idx;
    heap_table_ = heap_table;
    pk_interval_ = pk_interval;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleMergeTask::construct_row_iter(
  ObIAllocator &allocator, ObIStoreRowIterator *&result_row_iter)
{
  int ret = OB_SUCCESS;
  result_row_iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleMergeTask not init", KR(ret), KP(this));
  } else {
    RowIterator *row_iter = nullptr;
    if (OB_ISNULL(row_iter = OB_NEWx(RowIterator, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new RowIterator", KR(ret));
    } else if (OB_FAIL(row_iter->init(*merge_param_, merge_ctx_->get_tablet_id(), heap_table_,
                                      sql_statistics_, lob_builder_, pk_interval_,
                                      insert_tablet_ctx_))) {
      LOG_WARN("fail to init row iter", KR(ret));
    } else {
      result_row_iter = row_iter;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != row_iter) {
        row_iter->~RowIterator();
        allocator.free(row_iter);
        row_iter = nullptr;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask
 */

ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::RowIterator()
  : allocator_("TLD_RowIter"),
    origin_iter_(nullptr),
    rowkey_column_num_(0),
    store_column_count_(0),
    heap_table_array_(nullptr),
    pos_(0),
    deserialize_datums_(nullptr),
    deserialize_datum_cnt_(0),
    dml_row_handler_(nullptr)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::~RowIterator()
{
  if (nullptr != origin_iter_) {
    origin_iter_->~ObIStoreRowIterator();
    origin_iter_ = nullptr;
  }
}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param,
  ObDirectLoadTabletMergeCtx *merge_ctx,
  ObDirectLoadOriginTable *origin_table,
  ObTableLoadSqlStatistics *sql_statistics,
  ObDirectLoadLobBuilder &lob_builder,
  const ObIArray<ObDirectLoadMultipleHeapTable *> *heap_table_array,
  const ObTabletCacheInterval &pk_interval,
  ObDirectLoadInsertTabletContext *insert_tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator init twice", KR(ret),
             KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || nullptr == heap_table_array ||
                         nullptr == insert_tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), KP(origin_table),
             KP(heap_table_array), KP(insert_tablet_ctx));
  } else {
    range_.set_whole_range();
    // init row iterator
    if (OB_FAIL(inner_init(insert_tablet_ctx, sql_statistics, lob_builder))) {
      LOG_WARN("fail to inner init", KR(ret));
    }
    // init datum_row_
    else if (OB_FAIL(insert_tablet_ctx->init_datum_row(datum_row_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else if (merge_ctx->merge_with_origin_data()) {
      if (OB_FAIL(origin_table->scan(range_, allocator_, origin_iter_, false/*skip_read_lob*/))) {
        LOG_WARN("fail to scan origin table", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      deserialize_datums_ = datum_row_.storage_datums_ + merge_param.rowkey_column_num_ +
                            ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      deserialize_datum_cnt_ = merge_param.store_column_count_ - merge_param.rowkey_column_num_;
      rowkey_column_num_ = merge_param.rowkey_column_num_;
      store_column_count_ = merge_param.store_column_count_;
      tablet_id_ = merge_ctx->get_tablet_id();
      table_data_desc_ = merge_param.table_data_desc_;
      heap_table_array_ = heap_table_array;
      pk_interval_ = pk_interval;
      dml_row_handler_ = merge_param.dml_row_handler_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::inner_get_next_row(
  ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator not init",
             KR(ret), KP(this));
  } else {
    if (pos_ == 0) {
      if (origin_iter_ == nullptr) {
        if (OB_FAIL(switch_next_heap_table())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to switch next heap table", KR(ret));
          }
        }
      } else {
        // get row from origin table
        const ObDatumRow *datum_row = nullptr;
        if (OB_FAIL(origin_iter_->get_next_row(datum_row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row", KR(ret));
          } else {
            // switch heap table
            ret = OB_SUCCESS;
            if (OB_FAIL(switch_next_heap_table())) {
              if (OB_UNLIKELY(OB_ITER_END != ret)) {
                LOG_WARN("fail to switch next heap table", KR(ret));
              }
            }
          }
        } else if (OB_UNLIKELY(datum_row->count_ != store_column_count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column count", KR(ret), K(store_column_count_), KPC(datum_row));
        } else {
          // copy rowkey columns
          for (int64_t i = 0; i < rowkey_column_num_; ++i) {
            datum_row_.storage_datums_[i] = datum_row->storage_datums_[i];
          }
          // copy normal columns
          for (int64_t
                i = rowkey_column_num_,
                j = rowkey_column_num_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
                i < datum_row->count_; ++i, ++j) {
            datum_row_.storage_datums_[j] = datum_row->storage_datums_[i];
          }
          result_row = &datum_row_;
        }
      }
    }
    // get row from load data
    while (OB_SUCC(ret) && result_row == nullptr) {
      const ObDirectLoadMultipleExternalRow *external_row = nullptr;
      uint64_t pk_seq = OB_INVALID_ID;
      if (OB_FAIL(scanner_.get_next_row(external_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", KR(ret));
        } else {
          // switch next heap table
          ret = OB_SUCCESS;
          if (OB_FAIL(switch_next_heap_table())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to switch next heap table", KR(ret));
            }
          }
        }
      } else if (OB_FAIL(external_row->to_datums(deserialize_datums_, deserialize_datum_cnt_))) {
        LOG_WARN("fail to transfer datum row", KR(ret));
      } else if (OB_FAIL(pk_interval_.next_value(pk_seq))) {
        LOG_WARN("fail to get next pk seq", KR(ret));
      } else {
        // fill hide pk
        datum_row_.storage_datums_[0].set_int(pk_seq);
        result_row = &datum_row_;
      }
      if (OB_SUCC(ret) && nullptr != result_row) {
        if (OB_FAIL(dml_row_handler_->handle_insert_row(*result_row))) {
          LOG_WARN("fail to handle insert row", KR(ret), KPC(result_row));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::switch_next_heap_table()
{
  int ret = OB_SUCCESS;
  if (pos_ >= heap_table_array_->count()) {
    ret = OB_ITER_END;
  } else {
    ObDirectLoadMultipleHeapTable *heap_table = heap_table_array_->at(pos_);
    // restructure scanner
    scanner_.~ObDirectLoadMultipleHeapTableTabletWholeScanner();
    new (&scanner_) ObDirectLoadMultipleHeapTableTabletWholeScanner();
    if (OB_FAIL(scanner_.init(heap_table, tablet_id_, table_data_desc_))) {
      LOG_WARN("fail to init scanner", KR(ret));
    } else {
      ++pos_;
    }
  }
  return ret;
}

ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::
  ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask()
  : origin_table_(nullptr), heap_table_array_(nullptr)
{
}

ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::
  ~ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask()
{
}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::init(
  ObTableLoadTableCtx *ctx,
  const ObDirectLoadMergeParam &merge_param,
  ObDirectLoadTabletMergeCtx *merge_ctx,
  ObDirectLoadOriginTable *origin_table,
  const ObIArray<ObDirectLoadMultipleHeapTable *> &heap_table_array,
  const ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleMergeTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx || !merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || heap_table_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), KP(origin_table),
             K(heap_table_array));
  } else {
    ctx_ = ctx;
    merge_param_ = &merge_param;
    merge_ctx_ = merge_ctx;
    parallel_idx_ = 0;
    origin_table_ = origin_table;
    heap_table_array_ = &heap_table_array;
    pk_interval_ = pk_interval;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::construct_row_iter(
  ObIAllocator &allocator, ObIStoreRowIterator *&result_row_iter)
{
  int ret = OB_SUCCESS;
  result_row_iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask not init", KR(ret),
             KP(this));
  } else {
    RowIterator *row_iter = nullptr;
    if (OB_ISNULL(row_iter = OB_NEWx(RowIterator, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new RowIterator", KR(ret));
    } else if (OB_FAIL(row_iter->init(*merge_param_, merge_ctx_, origin_table_,
                                      sql_statistics_, lob_builder_, heap_table_array_,
                                      pk_interval_, insert_tablet_ctx_))) {
      LOG_WARN("fail to init row iter", KR(ret));
    } else {
      result_row_iter = row_iter;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != row_iter) {
        row_iter->~RowIterator();
        allocator.free(row_iter);
        row_iter = nullptr;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
