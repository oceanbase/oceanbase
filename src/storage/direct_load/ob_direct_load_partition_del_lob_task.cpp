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

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_partition_del_lob_task.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_lob_meta_row_iter.h"
#include "storage/lob/ob_lob_meta.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;

ObDirectLoadPartitionDelLobTask::RowIterator::RowIterator() : is_inited_(false) {}

ObDirectLoadPartitionDelLobTask::RowIterator::~RowIterator() {}

int ObDirectLoadPartitionDelLobTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param, const ObDirectLoadTabletMergeCtx *merge_ctx,
  ObDirectLoadOriginTable *origin_table,
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array, const ObDatumRange &range,
  int64_t parallel_idx, ObDirectLoadInsertTabletContext *insert_tablet_ctx)
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
    ObDirectLoadLobMetaIterParam iter_param;
    iter_param.tablet_id_ = merge_ctx->get_tablet_id();
    iter_param.table_data_desc_ = merge_param.lob_id_table_data_desc_;
    iter_param.datum_utils_ = merge_param.lob_meta_datum_utils_;
    iter_param.col_descs_ = merge_param.lob_meta_col_descs_;
    if (OB_FAIL(lob_iter_.init(iter_param, origin_table, sstable_array, range))) {
      LOG_WARN("fail to init lob meta row iter", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx->init_lob_datum_row(datum_row_))) {
      LOG_WARN("fail to init lob datum row", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadPartitionDelLobTask::RowIterator::get_next_row(const ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator not init", KR(ret),
             KP(this));
  } else {
    const ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(lob_iter_.get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else if (OB_UNLIKELY(datum_row->count_ < ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected datum row count", KR(ret), KPC(datum_row));
    } else {
      for (int64_t i = 0; i < ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT; ++i) {
        datum_row_.storage_datums_[i] = datum_row->storage_datums_[i];
      }
      result_row = &datum_row_;
    }
  }
  return ret;
}

ObDirectLoadPartitionDelLobTask::ObDirectLoadPartitionDelLobTask()
  : merge_param_(nullptr),
    merge_ctx_(nullptr),
    origin_table_(nullptr),
    sstable_array_(nullptr),
    range_(nullptr),
    parallel_idx_(-1),
    is_stop_(false),
    is_inited_(false)
{
}

ObDirectLoadPartitionDelLobTask::~ObDirectLoadPartitionDelLobTask() {}

int ObDirectLoadPartitionDelLobTask::init(
  const ObDirectLoadMergeParam &merge_param, ObDirectLoadTabletMergeCtx *merge_ctx,
  ObDirectLoadOriginTable *origin_table,
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array, const ObDatumRange &range,
  int64_t parallel_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionDelLobTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || !range.is_valid() || parallel_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), K(sstable_array), K(range),
             K(parallel_idx));
  } else {
    merge_param_ = &merge_param;
    merge_ctx_ = merge_ctx;
    origin_table_ = origin_table;
    sstable_array_ = &sstable_array;
    range_ = &range;
    parallel_idx_ = parallel_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionDelLobTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionDelLobTask not init", KR(ret), KP(this));
  } else {
    int64_t slice_id = 0;
    const ObTabletID &tablet_id = merge_ctx_->get_tablet_id();
    ObDirectLoadInsertTabletContext *insert_tablet_ctx = nullptr;
    RowIterator row_iter;
    ObMacroDataSeq block_start_seq;
    int64_t affected_rows = 0;
    block_start_seq.set_parallel_degree(parallel_idx_);
    if (OB_UNLIKELY(is_stop_)) {
      ret = OB_CANCELED;
      LOG_WARN("merge task canceled", KR(ret));
    } else if (OB_FAIL(merge_param_->insert_table_ctx_->get_tablet_context(tablet_id,
                                                                           insert_tablet_ctx))) {
      LOG_WARN("fail to get tablet context ", KR(ret), K(tablet_id));
    } else if (OB_FAIL(row_iter.init(*merge_param_, merge_ctx_, origin_table_, *sstable_array_,
                                     *range_, parallel_idx_, insert_tablet_ctx))) {
      LOG_WARN("fail to init row iter", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx->open_lob_sstable_slice(block_start_seq, slice_id))) {
      LOG_WARN("fail to construct lob sstable slice ", KR(ret), K(slice_id), K(block_start_seq));
    } else {
      LOG_INFO("add lob meta sstable slice begin", K(tablet_id), K(parallel_idx_), K(slice_id));
      if (OB_FAIL(
            insert_tablet_ctx->fill_lob_meta_sstable_slice(slice_id, row_iter, affected_rows))) {
        LOG_WARN("fail to fill lob meta sstable slice", KR(ret));
      } else if (OB_FAIL(insert_tablet_ctx->close_lob_sstable_slice(slice_id))) {
        LOG_WARN("fail to close writer", KR(ret));
      }
      LOG_INFO("add lob meta sstable slice end", KR(ret), K(tablet_id), K(parallel_idx_),
               K(affected_rows));
    }
    if (OB_SUCC(ret)) {
      bool is_ready = false;
      if (OB_FAIL(merge_ctx_->inc_del_lob_finish_count(is_ready))) {
        LOG_WARN("fail to inc del lob finish count", KR(ret));
      } else if (is_ready) {
        if (OB_FAIL(insert_tablet_ctx->close())) {
          LOG_WARN("fail to close", KR(ret));
        }
      }
    }
  }
  return ret;
}

void ObDirectLoadPartitionDelLobTask::stop() { is_stop_ = true; }

ObDirectLoadDelLobTaskIterator::ObDirectLoadDelLobTaskIterator()
  : merge_ctx_(nullptr), tablet_merge_ctx_(nullptr), tablet_pos_(0), task_pos_(0), is_inited_(false)
{
}

ObDirectLoadDelLobTaskIterator::~ObDirectLoadDelLobTaskIterator() {}

int ObDirectLoadDelLobTaskIterator::init(ObDirectLoadMergeCtx *merge_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDelLobTaskIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == merge_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(merge_ctx));
  } else {
    merge_ctx_ = merge_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadDelLobTaskIterator::get_next_task(ObDirectLoadPartitionDelLobTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDelLobTaskIterator not init", KR(ret), KP(this));
  } else {
    while (OB_SUCC(ret) && nullptr == task) {
      if (nullptr == tablet_merge_ctx_) {
        // get next partition merge ctx
        const ObIArray<ObDirectLoadTabletMergeCtx *> &tablet_merge_ctxs =
          merge_ctx_->get_tablet_merge_ctxs();
        if (tablet_pos_ >= tablet_merge_ctxs.count()) {
          ret = OB_ITER_END;
        } else {
          tablet_merge_ctx_ = tablet_merge_ctxs.at(tablet_pos_++);
          task_pos_ = 0;
        }
      }
      if (OB_SUCC(ret)) {
        const ObIArray<ObDirectLoadPartitionDelLobTask *> &tasks =
          tablet_merge_ctx_->get_del_lob_tasks();
        if (task_pos_ >= tasks.count()) {
          // try next partition
          tablet_merge_ctx_ = nullptr;
        } else {
          task = tasks.at(task_pos_++);
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
