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
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_stat_define.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"


namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace share;

/**
 * ObDirectLoadPartitionMergeTask
 */

ObDirectLoadPartitionMergeTask::ObDirectLoadPartitionMergeTask()
  : merge_param_(nullptr),
    merge_ctx_(nullptr),
    parallel_idx_(-1),
    affected_rows_(0),
    allocator_("TLD_ParMT"),
    is_stop_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadPartitionMergeTask::~ObDirectLoadPartitionMergeTask()
{
  for (int64_t i = 0; i < column_stat_array_.count(); ++i) {
    ObOptOSGColumnStat *col_stat = column_stat_array_.at(i);
    if (col_stat != nullptr) {
       col_stat->~ObOptOSGColumnStat();
       col_stat = nullptr;
    }
  }
}

int ObDirectLoadPartitionMergeTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionMergeTask not init", KR(ret), KP(this));
  } else {
    const ObTabletID &tablet_id = merge_ctx_->get_tablet_id();
    const ObTabletID &target_tablet_id = merge_ctx_->get_target_tablet_id();
    ObIStoreRowIterator *row_iter = nullptr;
    ObSSTableInsertSliceWriter *writer = nullptr;
    ObMacroDataSeq block_start_seq;
    block_start_seq.set_parallel_degree(parallel_idx_);
    if (merge_param_->online_opt_stat_gather_ && OB_FAIL(init_sql_statistics())) {
      LOG_WARN("fail to init sql statistics", KR(ret));
    } else if (OB_FAIL(construct_row_iter(allocator_, row_iter))) {
      LOG_WARN("fail to construct row iter", KR(ret));
    } else if (OB_FAIL(merge_param_->insert_table_ctx_->construct_sstable_slice_writer(
                 target_tablet_id, block_start_seq, writer, allocator_))) {
      LOG_WARN("fail to construct sstable slice writer", KR(ret), K(target_tablet_id),
               K(block_start_seq));
    } else {
      LOG_INFO("add sstable slice begin", K(target_tablet_id), K(parallel_idx_));
      const ObDatumRow *datum_row = nullptr;
      while (OB_SUCC(ret)) {
        if (OB_UNLIKELY(is_stop_)) {
          ret = OB_CANCELED;
          LOG_WARN("merge task canceled", KR(ret));
        } else if (OB_FAIL(row_iter->get_next_row(datum_row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(writer->append_row(*const_cast<ObDatumRow *>(datum_row)))) {
          LOG_WARN("fail to append row", KR(ret), KPC(datum_row));
        } else if (merge_param_->online_opt_stat_gather_ && OB_FAIL(collect_obj(*datum_row))) {
          LOG_WARN("fail to collect statistics", KR(ret));
        } else {
          ++affected_rows_;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(writer->close())) {
          LOG_WARN("fail to close writer", KR(ret));
        }
      }
      LOG_INFO("add sstable slice end", KR(ret), K(target_tablet_id), K(tablet_id),
               K(parallel_idx_), K(affected_rows_));
    }
    if (OB_NOT_NULL(row_iter)) {
      row_iter->~ObIStoreRowIterator();
      allocator_.free(row_iter);
      row_iter = nullptr;
    }
    if (OB_NOT_NULL(writer)) {
      writer->~ObSSTableInsertSliceWriter();
      allocator_.free(writer);
      writer = nullptr;
    }
    if (OB_SUCC(ret)) {
      bool is_ready = false;
      if (OB_FAIL(merge_ctx_->inc_finish_count(is_ready))) {
        LOG_WARN("fail to inc finish count", KR(ret));
      } else if (is_ready &&
                 OB_FAIL(merge_param_->insert_table_ctx_->notify_tablet_finish(target_tablet_id))) {
        LOG_WARN("fail to notify tablet finish", KR(ret), K(target_tablet_id));
      }
    }
  }
  return ret;
}

int ObDirectLoadPartitionMergeTask::init_sql_statistics()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret)&& i < merge_param_->table_data_desc_.column_count_; ++i) {
    ObOptOSGColumnStat *col_stat = ObOptOSGColumnStat::create_new_osg_col_stat(allocator_);
    if (OB_ISNULL(col_stat)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate buffer", KR(ret));
    } else if (OB_FAIL(column_stat_array_.push_back(col_stat))){
      LOG_WARN("fail to push back", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (col_stat != nullptr) {
        col_stat->~ObOptOSGColumnStat();
        allocator_.free(col_stat);
        col_stat = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadPartitionMergeTask::collect_obj(const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (merge_param_->is_heap_table_ ) {
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_param_->table_data_desc_.column_count_; i++) {
      const ObStorageDatum &datum = datum_row.storage_datums_[i + extra_rowkey_cnt + 1];
      const ObColDesc &col_desc = merge_param_->col_descs_->at(i + 1);
      const ObCmpFunc &cmp_func = merge_param_->cmp_funcs_->at(i + 1).get_cmp_func();
      ObOptOSGColumnStat *col_stat = column_stat_array_.at(i);
      bool is_valid = ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type());
      if (col_stat != nullptr && is_valid) {
        if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_, cmp_func.cmp_func_))) {
          LOG_WARN("Failed to merge obj", K(ret), KP(col_stat));
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_param_->rowkey_column_num_; i++) {
      const ObStorageDatum &datum = datum_row.storage_datums_[i];
      const ObColDesc &col_desc = merge_param_->col_descs_->at(i);
      const ObCmpFunc &cmp_func = merge_param_->cmp_funcs_->at(i).get_cmp_func();
      ObOptOSGColumnStat *col_stat = column_stat_array_.at(i);
      bool is_valid = ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type());
      if (col_stat != nullptr && is_valid) {
        if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_, cmp_func.cmp_func_))) {
          LOG_WARN("Failed to merge obj", K(ret), KP(col_stat));
        }
      }
    }
    for (int64_t i = merge_param_->rowkey_column_num_; OB_SUCC(ret) && i < merge_param_->table_data_desc_.column_count_; i++) {
      const ObStorageDatum &datum = datum_row.storage_datums_[i + extra_rowkey_cnt];
      const ObColDesc &col_desc = merge_param_->col_descs_->at(i);
      const ObCmpFunc &cmp_func = merge_param_->cmp_funcs_->at(i).get_cmp_func();
      ObOptOSGColumnStat *col_stat = column_stat_array_.at(i);
      bool is_valid = ObColumnStatParam::is_valid_opt_col_type(col_desc.col_type_.get_type());
      if (col_stat != nullptr && is_valid) {
        if (OB_FAIL(col_stat->update_column_stat_info(&datum, col_desc.col_type_, cmp_func.cmp_func_))) {
          LOG_WARN("Failed to merge obj", K(ret), KP(col_stat));
        }
      }
    }
  }
  return ret;
}

void ObDirectLoadPartitionMergeTask::stop()
{
  is_stop_ = true;
}

/**
 * ObDirectLoadPartitionRangeMergeTask
 */

ObDirectLoadPartitionRangeMergeTask::RowIterator::RowIterator()
  : rowkey_column_num_(0), is_inited_(false)
{
}

ObDirectLoadPartitionRangeMergeTask::RowIterator::~RowIterator()
{
}

int ObDirectLoadPartitionRangeMergeTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param,
  const ObTabletID &tablet_id,
  ObDirectLoadOriginTable *origin_table,
  const ObIArray<ObDirectLoadSSTable *> &sstable_array,
  const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionRangeMergeTask::RowIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || !tablet_id.is_valid() ||
                         nullptr == origin_table || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), K(tablet_id), K(sstable_array), K(range));
  } else {
    // init data_fuse_
    ObDirectLoadDataFuseParam data_fuse_param;
    data_fuse_param.tablet_id_ = tablet_id;
    data_fuse_param.store_column_count_ = merge_param.store_column_count_;
    data_fuse_param.table_data_desc_ = merge_param.table_data_desc_;
    data_fuse_param.datum_utils_ = merge_param.datum_utils_;
    data_fuse_param.dml_row_handler_ = merge_param.dml_row_handler_;
    if (OB_FAIL(data_fuse_.init(data_fuse_param, origin_table, sstable_array, range))) {
      LOG_WARN("fail to init data fuse", KR(ret));
    }
    // init datum_row_
    else if (OB_FAIL(datum_row_.init(merge_param.store_column_count_ +
                                     ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      datum_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row_.mvcc_row_flag_.set_last_multi_version_row(true);
      datum_row_.storage_datums_[merge_param.rowkey_column_num_].set_int(
        -merge_param.snapshot_version_); // fill trans_version
      datum_row_.storage_datums_[merge_param.rowkey_column_num_ + 1].set_int(0); // fill sql_no
      rowkey_column_num_ = merge_param.rowkey_column_num_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadPartitionRangeMergeTask::RowIterator::get_next_row(const ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRangeMergeTask::RowIterator not init", KR(ret), KP(this));
  } else {
    const ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(data_fuse_.get_next_row(datum_row))) {
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

ObDirectLoadPartitionRangeMergeTask::~ObDirectLoadPartitionRangeMergeTask()
{
}

int ObDirectLoadPartitionRangeMergeTask::init(const ObDirectLoadMergeParam &merge_param,
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
  } else if (OB_UNLIKELY(!merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || !range.is_valid() || parallel_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), K(sstable_array), K(range),
             K(parallel_idx));
  } else {
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
    } else if (OB_FAIL(row_iter->init(*merge_param_, merge_ctx_->get_tablet_id(), origin_table_,
                                      *sstable_array_, *range_))) {
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
  : rowkey_column_num_(0), is_inited_(false)
{
}

ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator::~RowIterator()
{
}

int ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param,
  const ObTabletID &tablet_id,
  ObDirectLoadOriginTable *origin_table,
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
  const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator init twice", KR(ret),
             KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || !tablet_id.is_valid() ||
                         nullptr == origin_table || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), K(tablet_id), K(sstable_array), K(range));
  } else {
    // init data_fuse_
    ObDirectLoadDataFuseParam data_fuse_param;
    data_fuse_param.tablet_id_ = tablet_id;
    data_fuse_param.store_column_count_ = merge_param.store_column_count_;
    data_fuse_param.table_data_desc_ = merge_param.table_data_desc_;
    data_fuse_param.datum_utils_ = merge_param.datum_utils_;
    data_fuse_param.dml_row_handler_ = merge_param.dml_row_handler_;
    if (OB_FAIL(data_fuse_.init(data_fuse_param, origin_table, sstable_array, range))) {
      LOG_WARN("fail to init data fuse", KR(ret));
    }
    // init datum_row_
    else if (OB_FAIL(datum_row_.init(merge_param.store_column_count_ +
                                     ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      datum_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row_.mvcc_row_flag_.set_last_multi_version_row(true);
      datum_row_.storage_datums_[merge_param.rowkey_column_num_].set_int(
        -merge_param.snapshot_version_); // fill trans_version
      datum_row_.storage_datums_[merge_param.rowkey_column_num_ + 1].set_int(0); // fill sql_no
      rowkey_column_num_ = merge_param.rowkey_column_num_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator::get_next_row(
  const ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionRangeMultipleMergeTask::RowIterator not init", KR(ret),
             KP(this));
  } else {
    const ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(data_fuse_.get_next_row(datum_row))) {
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

ObDirectLoadPartitionRangeMultipleMergeTask::~ObDirectLoadPartitionRangeMultipleMergeTask()
{
}

int ObDirectLoadPartitionRangeMultipleMergeTask::init(
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
  } else if (OB_UNLIKELY(!merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || !range.is_valid() || parallel_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), K(sstable_array), K(range),
             K(parallel_idx));
  } else {
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
    } else if (OB_FAIL(row_iter->init(*merge_param_, merge_ctx_->get_tablet_id(), origin_table_,
                                      *sstable_array_, *range_))) {
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
    dml_row_handler_(nullptr),
    is_inited_(false)
{
}

ObDirectLoadPartitionHeapTableMergeTask::RowIterator::~RowIterator()
{
}

int ObDirectLoadPartitionHeapTableMergeTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param,
  const ObTabletID &tablet_id,
  ObDirectLoadExternalTable *external_table,
  const ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMergeTask::RowIterator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || !tablet_id.is_valid() ||
                         nullptr == external_table || 0 == pk_interval.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), K(tablet_id), KP(external_table),
             K(pk_interval));
  } else {
    // init scanner_
    if (OB_FAIL(scanner_.init(merge_param.table_data_desc_.external_data_block_size_,
                              external_table->get_meta().max_data_block_size_,
                              merge_param.table_data_desc_.compressor_type_,
                              external_table->get_fragments()))) {
      LOG_WARN("fail to init fragment scanner", KR(ret));
    }
    // init datum_row_
    else if (OB_FAIL(datum_row_.init(merge_param.store_column_count_ +
                                     ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      datum_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row_.mvcc_row_flag_.set_last_multi_version_row(true);
      datum_row_.storage_datums_[merge_param.rowkey_column_num_].set_int(
        -merge_param.snapshot_version_); // fill trans_version
      datum_row_.storage_datums_[merge_param.rowkey_column_num_ + 1].set_int(0); // fill sql_no
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

int ObDirectLoadPartitionHeapTableMergeTask::RowIterator::get_next_row(
  const ObDatumRow *&result_row)
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

ObDirectLoadPartitionHeapTableMergeTask::~ObDirectLoadPartitionHeapTableMergeTask()
{
}

int ObDirectLoadPartitionHeapTableMergeTask::init(const ObDirectLoadMergeParam &merge_param,
                                                  ObDirectLoadTabletMergeCtx *merge_ctx,
                                                  ObDirectLoadExternalTable *external_table,
                                                  const ObTabletCacheInterval &pk_interval,
                                                  int64_t parallel_idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMergeTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == external_table || parallel_idx < 0 ||
                         0 == pk_interval.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), KP(external_table),
             K(parallel_idx), K(pk_interval));
  } else {
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
                                      pk_interval_))) {
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
    dml_row_handler_(nullptr),
    is_inited_(false)
{
}

ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::~RowIterator()
{
}

int ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param,
  const ObTabletID &tablet_id,
  ObDirectLoadMultipleHeapTable *heap_table,
  const ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator init twice", KR(ret),
             KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || !tablet_id.is_valid() ||
                         nullptr == heap_table || 0 == pk_interval.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), K(tablet_id), KPC(heap_table),
             K(pk_interval));
  } else {
    // init scanner_
    if (OB_FAIL(scanner_.init(heap_table, tablet_id, merge_param.table_data_desc_))) {
      LOG_WARN("fail to init tablet whole scanner", KR(ret));
    }
    // init datum_row_
    else if (OB_FAIL(datum_row_.init(merge_param.store_column_count_ +
                                     ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      datum_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row_.mvcc_row_flag_.set_last_multi_version_row(true);
      datum_row_.storage_datums_[merge_param.rowkey_column_num_].set_int(
        -merge_param.snapshot_version_); // fill trans_version
      datum_row_.storage_datums_[merge_param.rowkey_column_num_ + 1].set_int(0); // fill sql_no
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

int ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator::get_next_row(
  const ObDatumRow *&result_row)
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
  } else if (OB_UNLIKELY(!merge_param.is_valid() || nullptr == merge_ctx || nullptr == heap_table ||
                         parallel_idx < 0 || 0 == pk_interval.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), KPC(heap_table),
             K(parallel_idx), K(pk_interval));
  } else {
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
                                      pk_interval_))) {
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
    dml_row_handler_(nullptr),
    is_inited_(false)
{
}

ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::~RowIterator()
{
  if (nullptr != origin_iter_) {
    origin_iter_->~ObIStoreRowIterator();
    origin_iter_ = nullptr;
  }
}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::init(
  const ObDirectLoadMergeParam &merge_param, const ObTabletID &tablet_id,
  ObDirectLoadOriginTable *origin_table,
  const ObIArray<ObDirectLoadMultipleHeapTable *> *heap_table_array,
  const ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleMergeTask::RowIterator init twice", KR(ret),
             KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || !tablet_id.is_valid() ||
                         nullptr == origin_table || nullptr == heap_table_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), K(tablet_id), KP(origin_table),
             KP(heap_table_array));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    range_.set_whole_range();
    if (OB_FAIL(origin_table->scan(range_, allocator_, origin_iter_))) {
      LOG_WARN("fail to scan origin table", KR(ret));
    }
    // init datum_row_
    else if (OB_FAIL(datum_row_.init(merge_param.store_column_count_ +
                                     ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      datum_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row_.mvcc_row_flag_.set_last_multi_version_row(true);
      datum_row_.storage_datums_[merge_param.rowkey_column_num_].set_int(
        -merge_param.snapshot_version_); // fill trans_version
      datum_row_.storage_datums_[merge_param.rowkey_column_num_ + 1].set_int(0); // fill sql_no
      deserialize_datums_ = datum_row_.storage_datums_ + merge_param.rowkey_column_num_ +
                            ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      deserialize_datum_cnt_ = merge_param.store_column_count_ - merge_param.rowkey_column_num_;
      rowkey_column_num_ = merge_param.rowkey_column_num_;
      store_column_count_ = merge_param.store_column_count_;
      tablet_id_ = tablet_id;
      table_data_desc_ = merge_param.table_data_desc_;
      heap_table_array_ = heap_table_array;
      pk_interval_ = pk_interval;
      dml_row_handler_ = merge_param.dml_row_handler_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator::get_next_row(
  const ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleAggregateMergeTask::RowIterator not init",
             KR(ret), KP(this));
  } else {
    // get row from origin table
    if (pos_ == 0) {
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
  const ObDirectLoadMergeParam &merge_param, ObDirectLoadTabletMergeCtx *merge_ctx,
  ObDirectLoadOriginTable *origin_table,
  const ObIArray<ObDirectLoadMultipleHeapTable *> &heap_table_array,
  const ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadPartitionHeapTableMultipleMergeTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!merge_param.is_valid() || nullptr == merge_ctx ||
                         nullptr == origin_table || heap_table_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(merge_param), KP(merge_ctx), KP(origin_table),
             K(heap_table_array));
  } else {
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
    } else if (OB_FAIL(row_iter->init(*merge_param_, merge_ctx_->get_tablet_id(), origin_table_,
                                      heap_table_array_, pk_interval_))) {
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
