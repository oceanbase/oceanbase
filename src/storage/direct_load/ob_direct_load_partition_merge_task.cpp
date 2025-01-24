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
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "storage/direct_load/ob_direct_load_compare.h"
#include "storage/direct_load/ob_direct_load_conflict_check.h"
#include "storage/direct_load/ob_direct_load_data_insert.h"
#include "storage/direct_load/ob_direct_load_data_fuse.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_iterator.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_writer.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"

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
    insert_tablet_ctx_(nullptr),
    need_handle_dml_row_(false),
    is_stop_(false),
    is_inited_(false)
{
}

ObDirectLoadPartitionMergeTask::~ObDirectLoadPartitionMergeTask()
{
}

int ObDirectLoadPartitionMergeTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionMergeTask not init", KR(ret), KP(this));
  } else {
    const ObTabletID &tablet_id = merge_ctx_->get_tablet_id();
    int64_t slice_id = 0;
    ObMacroDataSeq block_start_seq;
    ObArenaAllocator allocator("TLD_MergeExec");
    ObArray<ObDirectLoadIStoreRowIterator *> row_iters;
    allocator.set_tenant_id(MTL_ID());
    row_iters.set_block_allocator(ModulePageAllocator(allocator));
    if (OB_FAIL(
          merge_param_->insert_table_ctx_->get_tablet_context(tablet_id, insert_tablet_ctx_))) {
      LOG_WARN("fail to get tablet context ", KR(ret), K(tablet_id));
    } else if (OB_FAIL(construct_row_iters(row_iters, allocator))) {
      LOG_WARN("fail to construct row iters", KR(ret));
    }
    // 对于全量导入, 无论一个分区有没有数据, 都需要创建ddl对象来为该分区创建major sstable
    else if (OB_FAIL(ObDDLUtil::init_macro_block_seq(parallel_idx_, block_start_seq))) {
      LOG_WARN("fail to set parallel degree", KR(ret), K(parallel_idx_));
    } else if (OB_FAIL(insert_tablet_ctx_->open_sstable_slice(block_start_seq, slice_id))) {
      LOG_WARN("fail to open sstable slice ", KR(ret), K(block_start_seq));
    } else if (row_iters.empty()) {
      // do nothing
      LOG_INFO("skip empty sstable slice", K(tablet_id), K(parallel_idx_), K(block_start_seq),
               K(slice_id));
    } else {
      const bool use_batch_mode = insert_tablet_ctx_->need_rescan();
      LOG_INFO("add sstable slice begin", K(tablet_id), K(parallel_idx_), K(block_start_seq),
               K(slice_id), K(row_iters.count()), K(use_batch_mode));
      if (OB_UNLIKELY(is_stop_)) {
        ret = OB_CANCELED;
        LOG_WARN("merge task canceled", KR(ret));
      } else {
        // batch mode不支持同时写insert和delete行
        if (use_batch_mode) {
          if (OB_FAIL(fill_sstable_slice_batch(slice_id, row_iters))) {
            LOG_WARN("fail to fill sstable slice batch", KR(ret), K(slice_id));
          }
        } else {
          if (OB_FAIL(fill_sstable_slice(slice_id, row_iters))) {
            LOG_WARN("fail to fill sstable slice", KR(ret), K(slice_id));
          }
        }
      }
      LOG_INFO("add sstable slice end", KR(ret), K(tablet_id), K(parallel_idx_), K(affected_rows_));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(insert_tablet_ctx_->close_sstable_slice(slice_id))) {
        LOG_WARN("fail to close writer", KR(ret));
      } else if (OB_FAIL(finish_check())) {
        LOG_WARN("fail to do finish check", KR(ret));
      }
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
    // release row iters
    for (int64_t i = 0; i < row_iters.count(); ++i) {
      ObDirectLoadIStoreRowIterator *row_iter = row_iters.at(i);
      row_iter->~ObDirectLoadIStoreRowIterator();
      allocator.free(row_iter);
    }
    row_iters.reset();
    allocator.reset();
  }
  return ret;
}

int ObDirectLoadPartitionMergeTask::fill_sstable_slice(
  const int64_t slice_id,
  const ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters)
{
  int ret = OB_SUCCESS;
  ObDirectLoadInsertTableRowIterator insert_table_row_iter;
  if (OB_FAIL(insert_table_row_iter.init(insert_tablet_ctx_,
                                         row_iters,
                                         need_handle_dml_row_ ? merge_param_->dml_row_handler_ : nullptr,
                                         ctx_->job_stat_))) {
    LOG_WARN("fail to init insert table row iter", KR(ret));
  } else if (OB_FAIL(insert_tablet_ctx_->fill_sstable_slice(slice_id,
                                                            insert_table_row_iter,
                                                            affected_rows_))) {
    LOG_WARN("fail to fill sstable slice", KR(ret));
  } else if (OB_FAIL(insert_table_row_iter.close())) {
    LOG_WARN("fail to close insert table row iter", KR(ret));
  }
  return ret;
}

int ObDirectLoadPartitionMergeTask::fill_sstable_slice_batch(
  const int64_t slice_id,
  const ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters)
{
  int ret = OB_SUCCESS;
  ObDirectLoadInsertTableBatchRowStoreWriter batch_writer;
  ObDirectLoadInsertTableRowInfo row_info;
  if (OB_FAIL(insert_tablet_ctx_->get_row_info(row_info))) {
    LOG_WARN("fail to get row info", KR(ret));
  } else if (OB_FAIL(batch_writer.init(insert_tablet_ctx_,
                                       row_info,
                                       slice_id,
                                       need_handle_dml_row_ ? merge_param_->dml_row_handler_ : nullptr,
                                       ctx_->job_stat_))) {
    LOG_WARN("fail to init buffer writer", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_iters.count(); ++i) {
    ObDirectLoadIStoreRowIterator *row_iter = row_iters.at(i);
    if (OB_FAIL(batch_writer.write(row_iter))) {
      LOG_WARN("fail to write", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(batch_writer.close())) {
    LOG_WARN("fail to close writer", KR(ret));
  } else {
    affected_rows_ = batch_writer.get_row_count();
  }
  return ret;
}

void ObDirectLoadPartitionMergeTask::stop()
{
  is_stop_ = true;
  if (OB_NOT_NULL(insert_tablet_ctx_)) {
    insert_tablet_ctx_->cancel();
  }
}

/**
 * ObDirectLoadPartitionEmptyMergeTask
 */

int ObDirectLoadPartitionEmptyMergeTask::init(ObTableLoadTableCtx *ctx,
                                              const ObDirectLoadMergeParam &merge_param,
                                              ObDirectLoadTabletMergeCtx *merge_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionEmptyMergeTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx || !merge_param.is_valid() || nullptr == merge_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx));
  } else {
    ctx_ = ctx;
    merge_param_ = &merge_param;
    merge_ctx_ = merge_ctx;
    parallel_idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

/**
 * ObDirectLoadPartitionOriginDataMergeTask
 */

ObDirectLoadPartitionOriginDataMergeTask::ObDirectLoadPartitionOriginDataMergeTask()
  : origin_table_(nullptr), range_(nullptr)
{
}

ObDirectLoadPartitionOriginDataMergeTask::~ObDirectLoadPartitionOriginDataMergeTask() {}

int ObDirectLoadPartitionOriginDataMergeTask::init(ObTableLoadTableCtx *ctx,
                                                   const ObDirectLoadMergeParam &merge_param,
                                                   ObDirectLoadTabletMergeCtx *merge_ctx,
                                                   ObDirectLoadOriginTable *origin_table,
                                                   const ObDatumRange &range,
                                                   int64_t parallel_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionOriginDataMergeTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx || !merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || !range.is_valid() || parallel_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), K(range), K(parallel_idx));
  } else {
    ctx_ = ctx;
    merge_param_ = &merge_param;
    merge_ctx_ = merge_ctx;
    parallel_idx_ = parallel_idx;
    origin_table_ = origin_table;
    range_ = &range;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionOriginDataMergeTask::construct_row_iters(
  ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  row_iters.reset();
  ObDirectLoadIStoreRowIterator *data_iter = nullptr;
  if (OB_FAIL(origin_table_->scan(*range_, allocator, data_iter, false /*skip_read_lob*/))) {
    LOG_WARN("fail to scan origin table", KR(ret));
  } else if (OB_FAIL(row_iters.push_back(data_iter))) {
    LOG_WARN("fail to push back", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != data_iter) {
      data_iter->~ObDirectLoadIStoreRowIterator();
      allocator.free(data_iter);
      data_iter = nullptr;
    }
  }
  return ret;
}

/**
 * ObDirectLoadPartitionRangeMergeTask
 */

ObDirectLoadPartitionRangeMergeTask::ObDirectLoadPartitionRangeMergeTask()
  : origin_table_(nullptr), sstable_array_(nullptr), range_(nullptr), conflict_check_(nullptr)
{
}

ObDirectLoadPartitionRangeMergeTask::~ObDirectLoadPartitionRangeMergeTask() {}

int ObDirectLoadPartitionRangeMergeTask::init(ObTableLoadTableCtx *ctx,
                                              const ObDirectLoadMergeParam &merge_param,
                                              ObDirectLoadTabletMergeCtx *merge_ctx,
                                              ObDirectLoadOriginTable *origin_table,
                                              const ObIArray<ObDirectLoadSSTable *> &sstable_array,
                                              const ObDatumRange &range, int64_t parallel_idx)
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

int ObDirectLoadPartitionRangeMergeTask::construct_row_iters(
  ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  row_iters.reset();
  ObDirectLoadIStoreRowIterator *data_iter = nullptr;
  if (merge_ctx_->merge_with_origin_data()) {
    ObDirectLoadDataFuseParam data_fuse_param;
    data_fuse_param.tablet_id_ = merge_ctx_->get_tablet_id();
    data_fuse_param.store_column_count_ = merge_param_->store_column_count_;
    data_fuse_param.table_data_desc_ = merge_param_->table_data_desc_;
    data_fuse_param.datum_utils_ = merge_param_->datum_utils_;
    data_fuse_param.dml_row_handler_ = merge_param_->dml_row_handler_;
    ObDirectLoadSSTableDataFuse *data_fuse = nullptr;
    if (OB_ISNULL(data_iter = data_fuse = OB_NEWx(ObDirectLoadSSTableDataFuse, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadSSTableDataFuse", KR(ret));
    } else if (OB_FAIL(data_fuse->init(data_fuse_param, origin_table_, *sstable_array_, *range_))) {
      LOG_WARN("fail to init data fuse", KR(ret));
    }
  } else if (merge_ctx_->merge_with_conflict_check()) {
    ObDirectLoadConflictCheckParam conflict_check_param;
    conflict_check_param.tablet_id_ = merge_ctx_->get_tablet_id();
    conflict_check_param.tablet_id_in_lob_id_ = insert_tablet_ctx_->get_tablet_id_in_lob_id();
    conflict_check_param.store_column_count_ = merge_param_->store_column_count_;
    conflict_check_param.table_data_desc_ = merge_param_->table_data_desc_;
    conflict_check_param.origin_table_ = origin_table_;
    conflict_check_param.range_ = range_;
    conflict_check_param.col_descs_ = merge_param_->col_descs_;
    conflict_check_param.lob_column_idxs_ = merge_param_->lob_column_idxs_;
    conflict_check_param.datum_utils_ = merge_param_->datum_utils_;
    conflict_check_param.lob_meta_datum_utils_ = merge_param_->lob_meta_datum_utils_;
    conflict_check_param.dml_row_handler_ = merge_param_->dml_row_handler_;
    ObDirectLoadSSTableConflictCheck *conflict_check = nullptr;
    if (OB_FAIL(merge_ctx_->get_table_builder(conflict_check_param.builder_))) {
      LOG_WARN("fail to get table builder", K(ret));
    } else if (OB_ISNULL(data_iter = conflict_check =
                           OB_NEWx(ObDirectLoadSSTableConflictCheck, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadSSTableConflictCheck", K(ret));
    } else if (OB_FAIL(conflict_check->init(conflict_check_param, *sstable_array_))) {
      LOG_WARN("fail to init conflict check", K(ret));
    } else {
      conflict_check_ = conflict_check; // save conflict_check_
    }
  } else {
    ObDirectLoadDataInsertParam data_insert_param;
    data_insert_param.tablet_id_ = merge_ctx_->get_tablet_id();
    data_insert_param.store_column_count_ = merge_param_->store_column_count_;
    data_insert_param.table_data_desc_ = merge_param_->table_data_desc_;
    data_insert_param.datum_utils_ = merge_param_->datum_utils_;
    data_insert_param.dml_row_handler_ = merge_param_->dml_row_handler_;
    ObDirectLoadSSTableDataInsert *data_insert = nullptr;
    if (OB_ISNULL(data_iter = data_insert = OB_NEWx(ObDirectLoadSSTableDataInsert, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new ObDirectLoadSSTableDataInsert", K(ret));
    } else if (OB_FAIL(data_insert->init(data_insert_param, *sstable_array_, *range_))) {
      LOG_WARN("fail to init data insert", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_iters.push_back(data_iter))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != data_iter) {
      data_iter->~ObDirectLoadIStoreRowIterator();
      allocator.free(data_iter);
      data_iter = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadPartitionRangeMergeTask::finish_check()
{
  int ret = OB_SUCCESS;
  if (nullptr != conflict_check_) {
    const ObLobId max_del_lob_id = conflict_check_->get_max_del_lob_id();
    const ObLobId &min_insert_lob_id = insert_tablet_ctx_->get_min_insert_lob_id();
    if (max_del_lob_id.is_valid() && min_insert_lob_id.is_valid()) {
      ObStorageDatum max_del_lob_id_datum, min_insert_lob_id_datum;
      ObDirectLoadSingleDatumCompare compare;
      int cmp_ret = 0;
      max_del_lob_id_datum.set_string(reinterpret_cast<const char *>(&max_del_lob_id),
                                      sizeof(ObLobId));
      min_insert_lob_id_datum.set_string(reinterpret_cast<const char *>(&min_insert_lob_id),
                                         sizeof(ObLobId));
      if (OB_FAIL(compare.init(*merge_param_->lob_meta_datum_utils_))) {
        LOG_WARN("fail to init compare", KR(ret));
      } else if (OB_FAIL(
                   compare.compare(&max_del_lob_id_datum, &min_insert_lob_id_datum, cmp_ret))) {
        LOG_WARN("fail to compare lob id", KR(ret), K(max_del_lob_id_datum),
                 K(min_insert_lob_id_datum));
      } else if (OB_UNLIKELY(cmp_ret >= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected lob id", KR(ret), K(max_del_lob_id), K(max_del_lob_id_datum),
                 K(min_insert_lob_id), K(min_insert_lob_id_datum));
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadPartitionRangeMultipleMergeTask
 */

ObDirectLoadPartitionRangeMultipleMergeTask::ObDirectLoadPartitionRangeMultipleMergeTask()
  : origin_table_(nullptr), sstable_array_(nullptr), range_(nullptr), conflict_check_(nullptr)
{
}

ObDirectLoadPartitionRangeMultipleMergeTask::~ObDirectLoadPartitionRangeMultipleMergeTask() {}

int ObDirectLoadPartitionRangeMultipleMergeTask::init(
  ObTableLoadTableCtx *ctx, const ObDirectLoadMergeParam &merge_param,
  ObDirectLoadTabletMergeCtx *merge_ctx, ObDirectLoadOriginTable *origin_table,
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array, const ObDatumRange &range,
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

int ObDirectLoadPartitionRangeMultipleMergeTask::construct_row_iters(
  ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  row_iters.reset();
  ObDirectLoadIStoreRowIterator *data_iter = nullptr;
  if (merge_ctx_->merge_with_origin_data()) {
    ObDirectLoadDataFuseParam data_fuse_param;
    data_fuse_param.tablet_id_ = merge_ctx_->get_tablet_id();
    data_fuse_param.store_column_count_ = merge_param_->store_column_count_;
    data_fuse_param.table_data_desc_ = merge_param_->table_data_desc_;
    data_fuse_param.datum_utils_ = merge_param_->datum_utils_;
    data_fuse_param.dml_row_handler_ = merge_param_->dml_row_handler_;
    ObDirectLoadMultipleSSTableDataFuse *data_fuse = nullptr;
    if (OB_ISNULL(data_iter = data_fuse =
                    OB_NEWx(ObDirectLoadMultipleSSTableDataFuse, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new ObDirectLoadMultipleSSTableDataFuse", KR(ret));
    } else if (OB_FAIL(data_fuse->init(data_fuse_param, origin_table_, *sstable_array_, *range_))) {
      LOG_WARN("fail to init data fuse", KR(ret));
    }
  } else if (merge_ctx_->merge_with_conflict_check()) {
    ObDirectLoadConflictCheckParam conflict_check_param;
    conflict_check_param.tablet_id_ = merge_ctx_->get_tablet_id();
    conflict_check_param.tablet_id_in_lob_id_ = insert_tablet_ctx_->get_tablet_id_in_lob_id();
    conflict_check_param.store_column_count_ = merge_param_->store_column_count_;
    conflict_check_param.table_data_desc_ = merge_param_->table_data_desc_;
    conflict_check_param.origin_table_ = origin_table_;
    conflict_check_param.range_ = range_;
    conflict_check_param.col_descs_ = merge_param_->col_descs_;
    conflict_check_param.lob_column_idxs_ = merge_param_->lob_column_idxs_;
    conflict_check_param.datum_utils_ = merge_param_->datum_utils_;
    conflict_check_param.lob_meta_datum_utils_ = merge_param_->lob_meta_datum_utils_;
    conflict_check_param.dml_row_handler_ = merge_param_->dml_row_handler_;
    ObDirectLoadMultipleSSTableConflictCheck *conflict_check = nullptr;
    if (OB_FAIL(merge_ctx_->get_table_builder(conflict_check_param.builder_))) {
      LOG_WARN("fail to get table builder", KR(ret));
    } else if (OB_ISNULL(data_iter = conflict_check =
                           OB_NEWx(ObDirectLoadMultipleSSTableConflictCheck, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMultipleSSTableConflictCheck", KR(ret));
    } else if (OB_FAIL(conflict_check->init(conflict_check_param, *sstable_array_))) {
      LOG_WARN("fail to init conflict check", KR(ret));
    } else {
      conflict_check_ = conflict_check; // save conflict_check_
    }
  } else {
    ObDirectLoadDataInsertParam data_insert_param;
    data_insert_param.tablet_id_ = merge_ctx_->get_tablet_id();
    data_insert_param.store_column_count_ = merge_param_->store_column_count_;
    data_insert_param.table_data_desc_ = merge_param_->table_data_desc_;
    data_insert_param.datum_utils_ = merge_param_->datum_utils_;
    data_insert_param.dml_row_handler_ = merge_param_->dml_row_handler_;
    ObDirectLoadMultipleSSTableDataInsert *data_insert = nullptr;
    if (OB_ISNULL(data_iter = data_insert =
                    OB_NEWx(ObDirectLoadMultipleSSTableDataInsert, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new ObDirectLoadMultipleSSTableDataInsert", KR(ret));
    } else if (OB_FAIL(data_insert->init(data_insert_param, *sstable_array_, *range_))) {
      LOG_WARN("fail to init data insert", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_iters.push_back(data_iter))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != data_iter) {
      data_iter->~ObDirectLoadIStoreRowIterator();
      allocator.free(data_iter);
      data_iter = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadPartitionRangeMultipleMergeTask::finish_check()
{
  int ret = OB_SUCCESS;
  if (nullptr != conflict_check_) {
    const ObLobId max_del_lob_id = conflict_check_->get_max_del_lob_id();
    const ObLobId &min_insert_lob_id = insert_tablet_ctx_->get_min_insert_lob_id();
    if (max_del_lob_id.is_valid() && min_insert_lob_id.is_valid()) {
      ObStorageDatum max_del_lob_id_datum, min_insert_lob_id_datum;
      ObDirectLoadSingleDatumCompare compare;
      int cmp_ret = 0;
      max_del_lob_id_datum.set_string(reinterpret_cast<const char *>(&max_del_lob_id),
                                      sizeof(ObLobId));
      min_insert_lob_id_datum.set_string(reinterpret_cast<const char *>(&min_insert_lob_id),
                                         sizeof(ObLobId));
      if (OB_FAIL(compare.init(*merge_param_->lob_meta_datum_utils_))) {
        LOG_WARN("fail to init compare", KR(ret));
      } else if (OB_FAIL(
                   compare.compare(&max_del_lob_id_datum, &min_insert_lob_id_datum, cmp_ret))) {
        LOG_WARN("fail to compare lob id", KR(ret), K(max_del_lob_id_datum),
                 K(min_insert_lob_id_datum));
      } else if (OB_UNLIKELY(cmp_ret >= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected lob id", KR(ret), K(max_del_lob_id), K(max_del_lob_id_datum),
                 K(min_insert_lob_id), K(min_insert_lob_id_datum));
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadPartitionHeapTableMergeTask
 */

ObDirectLoadPartitionHeapTableMergeTask::RowIterator::RowIterator()
  : pk_interval_(nullptr), is_inited_(false)
{
}

ObDirectLoadPartitionHeapTableMergeTask::RowIterator::~RowIterator() {}

int ObDirectLoadPartitionHeapTableMergeTask::RowIterator::init(
  ObDirectLoadExternalTable *external_table, const ObDirectLoadTableDataDesc &table_data_desc,
  ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("RowIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == external_table || !table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(external_table), K(table_data_desc));
  } else {
    if (OB_FAIL(scanner_.init(table_data_desc.external_data_block_size_,
                              table_data_desc.compressor_type_, external_table->get_fragments()))) {
      LOG_WARN("fail to init fragment scanner", KR(ret));
    } else if (OB_FAIL(datum_row_.init(table_data_desc.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      pk_interval_ = &pk_interval;
      row_flag_.uncontain_hidden_pk_ = true;
      row_flag_.has_multi_version_cols_ = false;
      row_flag_.has_delete_row_ = false;
      column_count_ = table_data_desc.column_count_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMergeTask::RowIterator::get_next_row(const ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  datum_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("RowIterator not init", KR(ret), KP(this));
  } else {
    const ObDirectLoadExternalRow *external_row = nullptr;
    if (OB_FAIL(scanner_.get_next_item(external_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next item", KR(ret));
      }
    } else if (OB_FAIL(external_row->to_datums(datum_row_.storage_datums_, datum_row_.count_))) {
      LOG_WARN("fail to transfer datum row", KR(ret));
    } else {
      datum_row = &datum_row_;
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
    need_handle_dml_row_ = true;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMergeTask::construct_row_iters(
  ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  row_iters.reset();
  RowIterator *row_iter = nullptr;
  if (OB_ISNULL(row_iter = OB_NEWx(RowIterator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new RowIterator", KR(ret));
  } else if (OB_FAIL(
               row_iter->init(external_table_, merge_param_->table_data_desc_, pk_interval_))) {
    LOG_WARN("fail to init row iter", KR(ret));
  } else if (OB_FAIL(row_iters.push_back(row_iter))) {
    LOG_WARN("fail to push back", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != row_iter) {
      row_iter->~RowIterator();
      allocator.free(row_iter);
      row_iter = nullptr;
    }
  }
  return ret;
}

/**
 * ObDirectLoadPartitionHeapTableMultipleMergeTask
 */

ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::RowIterator()
  : pk_interval_(nullptr), is_inited_(false)
{
}

ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::~RowIterator() {}

int ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::init(
  ObDirectLoadMultipleHeapTable *heap_table, const ObTabletID &tablet_id,
  const ObDirectLoadTableDataDesc &table_data_desc, ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("RowIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == heap_table || !tablet_id.is_valid() ||
                         !table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(heap_table), K(tablet_id), K(table_data_desc));
  } else {
    if (OB_FAIL(scanner_.init(heap_table, tablet_id, table_data_desc))) {
      LOG_WARN("fail to init tablet whole scanner", KR(ret));
    } else if (OB_FAIL(datum_row_.init(table_data_desc.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      pk_interval_ = &pk_interval;
      row_flag_.uncontain_hidden_pk_ = true;
      row_flag_.has_multi_version_cols_ = false;
      row_flag_.has_delete_row_ = false;
      column_count_ = table_data_desc.column_count_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::get_next_row(
  const ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  datum_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("RowIterator not init", KR(ret), KP(this));
  } else {
    const ObDirectLoadMultipleExternalRow *external_row = nullptr;
    if (OB_FAIL(scanner_.get_next_row(external_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else if (OB_FAIL(external_row->to_datums(datum_row_.storage_datums_, datum_row_.count_))) {
      LOG_WARN("fail to transfer datum row", KR(ret));
    } else {
      datum_row = &datum_row_;
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

int ObDirectLoadPartitionHeapTableMultipleMergeTask::init(ObTableLoadTableCtx *ctx,
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
  } else if (OB_UNLIKELY(nullptr == ctx || !merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == heap_table || parallel_idx < 0 || 0 == pk_interval.count())) {
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
    need_handle_dml_row_ = true;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleMergeTask::construct_row_iters(
  ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  row_iters.reset();
  RowIterator *row_iter = nullptr;
  if (OB_ISNULL(row_iter = OB_NEWx(RowIterator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new RowIterator", KR(ret));
  } else if (OB_FAIL(row_iter->init(heap_table_, merge_ctx_->get_tablet_id(),
                                    merge_param_->table_data_desc_, pk_interval_))) {
    LOG_WARN("fail to init row iter", KR(ret));
  } else if (OB_FAIL(row_iters.push_back(row_iter))) {
    LOG_WARN("fail to push back", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != row_iter) {
      row_iter->~RowIterator();
      allocator.free(row_iter);
      row_iter = nullptr;
    }
  }
  return ret;
}

/**
 * ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask
 */

ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::RowIterator()
  : heap_table_array_(nullptr), pk_interval_(nullptr), pos_(-1), is_inited_(false)
{
}

ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::~RowIterator() {}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::init(
  const ObIArray<ObDirectLoadMultipleHeapTable *> *heap_table_array, const ObTabletID &tablet_id,
  const ObDirectLoadTableDataDesc &table_data_desc, ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("RowIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == heap_table_array || !tablet_id.is_valid() ||
                         !table_data_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(heap_table_array), K(tablet_id), K(table_data_desc));
  } else {
    if (OB_FAIL(datum_row_.init(table_data_desc.column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      pk_interval_ = &pk_interval;
      heap_table_array_ = heap_table_array;
      tablet_id_ = tablet_id;
      table_data_desc_ = table_data_desc;
      row_flag_.uncontain_hidden_pk_ = true;
      row_flag_.has_multi_version_cols_ = false;
      row_flag_.has_delete_row_ = false;
      column_count_ = table_data_desc.column_count_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::get_next_row(
  const ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  datum_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("RowIterator not init", KR(ret), KP(this));
  } else {
    const ObDirectLoadMultipleExternalRow *external_row = nullptr;
    if (pos_ < 0 && OB_FAIL(switch_next_heap_table())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to switch next heap table", KR(ret));
      }
    }
    while (OB_SUCC(ret) && datum_row == nullptr) {
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
      } else if (OB_FAIL(external_row->to_datums(datum_row_.storage_datums_, datum_row_.count_))) {
        LOG_WARN("fail to transfer datum row", KR(ret));
      } else {
        datum_row = &datum_row_;
      }
    }
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::switch_next_heap_table()
{
  int ret = OB_SUCCESS;
  pos_ = (pos_ < 0 ? 0 : pos_ + 1);
  if (pos_ >= heap_table_array_->count()) {
    ret = OB_ITER_END;
  } else {
    ObDirectLoadMultipleHeapTable *heap_table = heap_table_array_->at(pos_);
    // restructure scanner
    scanner_.~ObDirectLoadMultipleHeapTableTabletWholeScanner();
    new (&scanner_) ObDirectLoadMultipleHeapTableTabletWholeScanner();
    if (OB_FAIL(scanner_.init(heap_table, tablet_id_, table_data_desc_))) {
      LOG_WARN("fail to init scanner", KR(ret));
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
  ObTableLoadTableCtx *ctx, const ObDirectLoadMergeParam &merge_param,
  ObDirectLoadTabletMergeCtx *merge_ctx, ObDirectLoadOriginTable *origin_table,
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
    need_handle_dml_row_ = true;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::construct_row_iters(
  ObIArray<ObDirectLoadIStoreRowIterator *> &row_iters,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  row_iters.reset();
  if (merge_ctx_->merge_with_origin_data()) {
    ObDirectLoadIStoreRowIterator *data_iter = nullptr;
    range_.set_whole_range();
    if (OB_FAIL(origin_table_->scan(range_, allocator, data_iter, false /*skip_read_lob*/))) {
      LOG_WARN("fail to scan origin table", KR(ret));
    } else if (OB_FAIL(row_iters.push_back(data_iter))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != data_iter) {
        data_iter->~ObDirectLoadIStoreRowIterator();
        allocator.free(data_iter);
        data_iter = nullptr;
      }
    }
  }
  if (OB_SUCC(ret)) {
    RowIterator *row_iter = nullptr;
    if (OB_ISNULL(row_iter = OB_NEWx(RowIterator, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new RowIterator", KR(ret));
    } else if (OB_FAIL(row_iter->init(heap_table_array_, merge_ctx_->get_tablet_id(),
                                      merge_param_->table_data_desc_, pk_interval_))) {
      LOG_WARN("fail to init row iter", KR(ret));
    } else if (OB_FAIL(row_iters.push_back(row_iter))) {
      LOG_WARN("fail to push back", KR(ret));
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
