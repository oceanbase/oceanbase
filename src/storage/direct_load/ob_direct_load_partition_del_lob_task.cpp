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
#include "storage/direct_load/ob_direct_load_merge_ctx.h"

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
  ObDirectLoadTabletMergeCtx *merge_ctx, ObDirectLoadOriginTable &origin_table,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObDirectLoadTableHandleArray &sstable_array, const ObDatumRange &range,
  int64_t parallel_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("RowIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == merge_ctx || !merge_ctx->is_valid() ||
                         !origin_table.is_valid() || !table_data_desc.is_valid() ||
                         !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(merge_ctx), K(origin_table), K(table_data_desc),
             K(range));
  } else {
    const ObDirectLoadMergeParam *merge_param = merge_ctx->get_param();
    ObDirectLoadInsertTabletContext *insert_tablet_ctx = merge_ctx->get_insert_tablet_ctx();
    ObDirectLoadLobMetaIterParam iter_param;
    iter_param.tablet_id_ = merge_ctx->get_tablet_id();
    iter_param.table_data_desc_ = table_data_desc;
    iter_param.datum_utils_ = merge_param->datum_utils_;
    iter_param.col_descs_ = merge_param->col_descs_;
    iter_param.dml_row_handler_ = merge_param->dml_row_handler_;
    if (OB_FAIL(lob_iter_.init(iter_param, origin_table, sstable_array, range))) {
      LOG_WARN("fail to init lob meta row iter", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx->init_datum_row(datum_row_, true/*is_delete*/))) {
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
    LOG_WARN("RowIterator not init", KR(ret), KP(this));
  } else {
    const ObDirectLoadDatumRow *datum_row = nullptr;
    if (OB_FAIL(lob_iter_.get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else if (OB_UNLIKELY(!datum_row->is_delete_ ||
                           datum_row->count_ < ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected datum row", KR(ret), KPC(datum_row));
    } else {
      // 删除lob只需要主键列
      for (int64_t i = 0; i < ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT; ++i) {
        datum_row_.storage_datums_[i] = datum_row->storage_datums_[i];
      }
      result_row = &datum_row_;
    }
  }
  return ret;
}

ObDirectLoadPartitionDelLobTask::ObDirectLoadPartitionDelLobTask()
  : merge_ctx_(nullptr),
    origin_table_(nullptr),
    range_(nullptr),
    parallel_idx_(-1),
    is_inited_(false)
{
}

ObDirectLoadPartitionDelLobTask::~ObDirectLoadPartitionDelLobTask() {}

int ObDirectLoadPartitionDelLobTask::init(ObDirectLoadTabletMergeCtx *merge_ctx,
                                          ObDirectLoadOriginTable &origin_table,
                                          const ObDirectLoadTableDataDesc &table_data_desc,
                                          const ObDirectLoadTableHandleArray &sstable_array,
                                          const ObDatumRange &range, const ObMacroDataSeq &data_seq,
                                          const int64_t parallel_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionDelLobTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == merge_ctx || !merge_ctx->is_valid() ||
                         !origin_table.is_valid() || !table_data_desc.is_valid() ||
                         !range.is_valid() || !data_seq.is_valid() || parallel_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(merge_ctx), K(origin_table), K(table_data_desc), K(range),
             K(data_seq), K(parallel_idx));
  } else if (OB_FAIL(sstable_array_.assign(sstable_array))) {
    LOG_WARN("fail to assign sstable array", KR(ret));
  } else {
    merge_ctx_ = merge_ctx;
    origin_table_ = &origin_table;
    table_data_desc_ = table_data_desc;
    range_ = &range;
    data_seq_ = data_seq;
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
    ObDirectLoadInsertTabletContext *insert_tablet_ctx = merge_ctx_->get_insert_tablet_ctx();
    RowIterator row_iter;
    int64_t affected_rows = 0;
    if (OB_FAIL(row_iter.init(merge_ctx_, *origin_table_, table_data_desc_, sstable_array_, *range_,
                              parallel_idx_))) {
      LOG_WARN("fail to init row iter", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx->open_sstable_slice(data_seq_, 0/*slice_idx*/, slice_id))) {
      LOG_WARN("fail to open lob sstable slice ", KR(ret), K(slice_id), K(data_seq_));
    } else {
      LOG_INFO("add lob meta sstable slice begin", K(tablet_id), K(parallel_idx_), K(data_seq_),
               K(slice_id), KPC(range_));
      if (OB_FAIL(insert_tablet_ctx->fill_sstable_slice(slice_id, row_iter, affected_rows))) {
        LOG_WARN("fail to fill lob sstable slice", KR(ret));
      } else if (OB_FAIL(insert_tablet_ctx->close_sstable_slice(slice_id, 0/*slice_idx*/))) {
        LOG_WARN("fail to close sstable slice", KR(ret));
      }
      LOG_INFO("add lob meta sstable slice end", KR(ret), K(tablet_id), K(parallel_idx_),
               K(affected_rows));
    }
  }
  return ret;
}

void ObDirectLoadPartitionDelLobTask::stop()
{
  if (nullptr != merge_ctx_) {
    ObDirectLoadInsertTabletContext *insert_tablet_ctx = merge_ctx_->get_insert_tablet_ctx();
    insert_tablet_ctx->cancel();
  }
}

} // namespace storage
} // namespace oceanbase
