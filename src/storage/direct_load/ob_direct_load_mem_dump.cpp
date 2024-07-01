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

#include "storage/direct_load/ob_direct_load_mem_dump.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_external_table_builder.h"
#include "storage/direct_load/ob_direct_load_external_table_compactor.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_builder.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_compactor.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace table;
using namespace sql;
using namespace observer;

/**
 * Context
 */

ObDirectLoadMemDump::Context::Context()
  : allocator_("TLD_MemDumpCtx"),
    safe_allocator_(allocator_),
    finished_sub_dump_count_(0),
    sub_dump_count_(0)
{
  allocator_.set_tenant_id(MTL_ID());
  mem_chunk_array_.set_tenant_id(MTL_ID());
  all_tables_.set_tenant_id(MTL_ID());
}

ObDirectLoadMemDump::Context::~Context()
{
  for (int64_t i = 0; i < all_tables_.count(); i++) {
    ObIDirectLoadPartitionTable *table = all_tables_.at(i);
    if (table != nullptr) {
      table->~ObIDirectLoadPartitionTable();
    }
  }
  for (int64_t i = 0; i < mem_chunk_array_.count(); i++) {
    ChunkType *chunk = mem_chunk_array_.at(i);
    if (chunk != nullptr) {
      chunk->~ChunkType();
      ob_free(chunk);
    }
  }
}

int ObDirectLoadMemDump::Context::add_table(const ObTabletID &tablet_id, int64_t range_idx,
                                            ObIDirectLoadPartitionTable *table)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_FAIL(all_tables_.push_back(table))) {
    LOG_WARN("fail to push table", KR(ret));
  } else if (OB_FAIL(tables_.add(tablet_id, std::make_pair(range_idx, table)))) {
    LOG_WARN("fail to add table", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadMemDump
 */

ObDirectLoadMemDump::ObDirectLoadMemDump(ObTableLoadTableCtx *ctx,
                                         ObDirectLoadMemContext *mem_ctx,
                                         const RangeType &range,
                                         ObTableLoadHandle<Context> context_ptr, int64_t range_idx)
  : allocator_("TLD_MemDump"),
    ctx_(ctx),
    mem_ctx_(mem_ctx),
    range_(range),
    context_ptr_(context_ptr),
    range_idx_(range_idx),
    extra_buf_(nullptr),
    extra_buf_size_(0)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadMemDump::~ObDirectLoadMemDump() {}

int ObDirectLoadMemDump::new_table_builder(const ObTabletID &tablet_id,
                                           ObIDirectLoadPartitionTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  if (mem_ctx_->table_data_desc_.is_heap_table_) {
    ret = new_external_table_builder(tablet_id, table_builder);
  } else {
    ret = new_sstable_builder(tablet_id, table_builder);
  }
  return ret;
}

int ObDirectLoadMemDump::new_sstable_builder(const ObTabletID &tablet_id,
                                             ObIDirectLoadPartitionTableBuilder *&table_builder)
{
  UNUSED(tablet_id);
  int ret = OB_SUCCESS;
  if (nullptr == table_builder) {
    ObDirectLoadMultipleSSTableBuilder *sstable_builder = nullptr;
    ObDirectLoadMultipleSSTableBuildParam sstable_build_param;
    sstable_build_param.table_data_desc_ = mem_ctx_->table_data_desc_;
    sstable_build_param.file_mgr_ = mem_ctx_->file_mgr_;
    sstable_build_param.datum_utils_ = mem_ctx_->datum_utils_;
    sstable_build_param.extra_buf_ = extra_buf_;
    sstable_build_param.extra_buf_size_ = extra_buf_size_;
    if (OB_ISNULL(sstable_builder = OB_NEWx(ObDirectLoadMultipleSSTableBuilder, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate sstable builder", KR(ret));
    } else if (OB_FAIL(sstable_builder->init(sstable_build_param))) {
      LOG_WARN("fail to init sstable builder", KR(ret));
    } else {
      table_builder = sstable_builder;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != sstable_builder) {
        sstable_builder->~ObDirectLoadMultipleSSTableBuilder();
        allocator_.free(sstable_builder);
        sstable_builder = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadMemDump::new_external_table_builder(
  const ObTabletID &tablet_id, ObIDirectLoadPartitionTableBuilder *&table_builder)
{
  int ret = OB_SUCCESS;
  ObDirectLoadExternalTableBuilder *external_table_builder = nullptr;
  ObDirectLoadExternalTableBuildParam external_build_param;
  external_build_param.table_data_desc_ = mem_ctx_->table_data_desc_;
  external_build_param.file_mgr_ = mem_ctx_->file_mgr_;
  external_build_param.tablet_id_ = tablet_id;
  external_build_param.datum_utils_ = mem_ctx_->datum_utils_;
  external_build_param.extra_buf_ = extra_buf_;
  external_build_param.extra_buf_size_ = extra_buf_size_;
  if (OB_ISNULL(external_table_builder =
                  OB_NEWx(ObDirectLoadExternalTableBuilder, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate external table builder", KR(ret));
  } else if (OB_FAIL(external_table_builder->init(external_build_param))) {
    LOG_WARN("fail to init external table builder", KR(ret));
  } else {
    table_builder = external_table_builder;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != external_table_builder) {
      external_table_builder->~ObDirectLoadExternalTableBuilder();
      allocator_.free(external_table_builder);
      external_table_builder = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadMemDump::close_table_builder(ObIDirectLoadPartitionTableBuilder *table_builder,
                                             ObTabletID tablet_id, bool is_final)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_builder)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_builder));
  } else {
    const bool need_close = (mem_ctx_->table_data_desc_.is_heap_table_ || is_final);
    if (need_close && table_builder->get_row_count() > 0) { //暂时因为simple file有问题
      ObSEArray<ObIDirectLoadPartitionTable *, 1> table_array;
      if (OB_FAIL(table_builder->close())) {
        LOG_WARN("fail to close sstable builder", KR(ret));
      } else if (OB_FAIL(table_builder->get_tables(table_array, context_ptr_->safe_allocator_))) {
        LOG_WARN("fail to get tables", KR(ret));
      } else if (OB_UNLIKELY(1 != table_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table count not equal 1", KR(ret));
      } else {
        ObIDirectLoadPartitionTable *table = table_array.at(0);
        if (OB_FAIL(context_ptr_->add_table(table->get_tablet_id(), range_idx_, table))) {
          LOG_WARN("fail to add table", K(tablet_id), K(range_idx_), KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
        for (int64_t i = 0; i < table_array.count(); ++i) {
          ObIDirectLoadPartitionTable *table = table_array.at(i);
          table->~ObIDirectLoadPartitionTable();
        }
      }
    }
    if (OB_SUCC(ret) && need_close) {
      table_builder->~ObIDirectLoadPartitionTableBuilder();
      table_builder = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadMemDump::dump_tables()
{
  typedef ObDirectLoadExternalIterator<RowType> ExternalIterator;
  int ret = OB_SUCCESS;
  ObArray<ExternalIterator *> iters;
  ObArray<ObDirectLoadMemChunkIter<RowType, CompareType>> chunk_iters; //用于暂存iters
  ObDirectLoadExternalMerger<RowType, CompareType> merger;
  CompareType compare;
  CompareType compare1;  //不带上seq_no的排序

  const RowType *external_row = nullptr;
  ObDatumRow datum_row;

  ObIDirectLoadPartitionTableBuilder *table_builder = nullptr;

  iters.set_tenant_id(MTL_ID());
  chunk_iters.set_tenant_id(MTL_ID());
  extra_buf_size_ = mem_ctx_->table_data_desc_.extra_buf_size_;
  if (OB_ISNULL(extra_buf_ = static_cast<char *>(allocator_.alloc(extra_buf_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else if (OB_FAIL(compare.init(*(mem_ctx_->datum_utils_), mem_ctx_->dup_action_))) {
    LOG_WARN("fail to init compare", KR(ret));
  } else if (OB_FAIL(compare1.init(*(mem_ctx_->datum_utils_), mem_ctx_->dup_action_, true))) {
    LOG_WARN("fail to init compare1", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < context_ptr_->mem_chunk_array_.count(); i++) {
    ChunkType *chunk = context_ptr_->mem_chunk_array_[i];
    auto iter = chunk->scan(range_.start_, range_.end_, compare1);
    if (OB_FAIL(chunk_iters.push_back(iter))) {
      LOG_WARN("fail to push iter", KR(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < chunk_iters.size(); i++) {
    if (OB_FAIL(iters.push_back(&(chunk_iters[i])))) {
      LOG_WARN("fail to push iters", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(merger.init(iters, &compare))) {
      LOG_WARN("fail to init merger", KR(ret));
    } else if (OB_FAIL(datum_row.init(mem_ctx_->column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      datum_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row.mvcc_row_flag_.set_last_multi_version_row(true);
    }
  }
  ObTabletID last_tablet_id;
  while (OB_SUCC(ret) && !(mem_ctx_->has_error_)) {
    if (OB_FAIL(merger.get_next_item(external_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row");
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (external_row->tablet_id_ != last_tablet_id) {
      if (nullptr != table_builder &&
          OB_FAIL(close_table_builder(table_builder, last_tablet_id, false /*is_final*/))) {
        LOG_WARN("fail to close table builder", KR(ret));
      } else if (OB_FAIL(new_table_builder(external_row->tablet_id_, table_builder))) {
        LOG_WARN("fail to new table builder", KR(ret));
      } else {
        last_tablet_id = external_row->tablet_id_;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(external_row->to_datums(datum_row.storage_datums_, datum_row.count_))) {
        LOG_WARN("fail to transfer dataum row", KR(ret));
      } else if (OB_FAIL(table_builder->append_row(external_row->tablet_id_, external_row->seq_no_, datum_row))) {
        if (OB_LIKELY(OB_ERR_PRIMARY_KEY_DUPLICATE == ret)) {
          if (OB_FAIL(mem_ctx_->dml_row_handler_->handle_update_row(datum_row))) {
            LOG_WARN("fail to handle update row", KR(ret), K(datum_row));
          }
        } else {
          LOG_WARN("fail to append row", KR(ret), K(datum_row));
        }
      } else {
        ATOMIC_AAF(&ctx_->job_stat_->store_.compact_stage_dump_rows_, 1);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (nullptr != table_builder &&
        OB_FAIL(close_table_builder(table_builder, last_tablet_id, true /*is_final*/))) {
      LOG_WARN("fail to close table builder", KR(ret));
    } else {
      table_builder = nullptr;
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != table_builder) {
      table_builder->~ObIDirectLoadPartitionTableBuilder();
      table_builder = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadMemDump::new_table_compactor(const ObTabletID &tablet_id,
                                             ObIDirectLoadTabletTableCompactor *&compactor)
{
  int ret = OB_SUCCESS;
  if (mem_ctx_->table_data_desc_.is_heap_table_) {
    ret = new_external_table_compactor(tablet_id, compactor);
  } else {
    ret = new_sstable_compactor(tablet_id, compactor);
  }
  return ret;
}

int ObDirectLoadMemDump::new_sstable_compactor(const ObTabletID &tablet_id,
                                               ObIDirectLoadTabletTableCompactor *&compactor)
{
  UNUSED(tablet_id);
  int ret = OB_SUCCESS;
  ObDirectLoadMultipleSSTableCompactor *sstable_compactor = nullptr;
  ObDirectLoadMultipleSSTableCompactParam param;
  param.table_data_desc_ = mem_ctx_->table_data_desc_;
  param.datum_utils_ = mem_ctx_->datum_utils_;
  if (OB_ISNULL(sstable_compactor = OB_NEWx(ObDirectLoadMultipleSSTableCompactor, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadMultipleSSTableCompactor", KR(ret));
  } else if (OB_FAIL(sstable_compactor->init(param))) {
    LOG_WARN("fail to init compactor", KR(ret));
  } else {
    compactor = sstable_compactor;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != sstable_compactor) {
      sstable_compactor->~ObDirectLoadMultipleSSTableCompactor();
      sstable_compactor = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadMemDump::new_external_table_compactor(const ObTabletID &tablet_id,
                                                      ObIDirectLoadTabletTableCompactor *&compactor)
{
  int ret = OB_SUCCESS;
  ObDirectLoadExternalTableCompactor *external_table_compactor = nullptr;
  ObDirectLoadExternalTableCompactParam param;
  param.tablet_id_ = tablet_id;
  param.table_data_desc_ = mem_ctx_->table_data_desc_;
  if (OB_ISNULL(external_table_compactor =
                  OB_NEWx(ObDirectLoadExternalTableCompactor, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadExternalTableCompactor", KR(ret));
  } else if (OB_FAIL(external_table_compactor->init(param))) {
    LOG_WARN("fail to init compactor", KR(ret));
  } else {
    compactor = external_table_compactor;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != external_table_compactor) {
      external_table_compactor->~ObDirectLoadExternalTableCompactor();
      external_table_compactor = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadMemDump::compact_tables()
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> keys;
  keys.set_tenant_id(MTL_ID());
  if (OB_FAIL(context_ptr_->tables_.get_all_key(keys))) {
    LOG_WARN("fail to get all keys", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); i++) {
    if (OB_FAIL(compact_tablet_tables(keys.at(i)))) {
      LOG_WARN("fail to compact tablet tables", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ATOMIC_AAF(&ctx_->job_stat_->store_.compact_stage_product_tmp_files_, keys.count());
  }
  return ret;
}

int ObDirectLoadMemDump::compact_tablet_tables(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObIDirectLoadTabletTableCompactor *compactor = nullptr;

  ObArray<std::pair<int64_t, ObIDirectLoadPartitionTable *>> table_array;
  table_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(context_ptr_->tables_.get(tablet_id, table_array))) {
    LOG_WARN("fail to get table array", K(tablet_id), KR(ret));
  } else {
    lib::ob_sort(
      table_array.begin(), table_array.end(),
      [](const std::pair<int64_t, ObIDirectLoadPartitionTable *> &a,
         const std::pair<int64_t, ObIDirectLoadPartitionTable *> &b) { return a.first < b.first; });
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(new_table_compactor(tablet_id, compactor))) {
      LOG_WARN("fail to new table compactor", KR(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); i++) {
    if (OB_FAIL(compactor->add_table(table_array[i].second))) {
      LOG_WARN("fail to add table", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObIDirectLoadPartitionTable *table = nullptr;
    if (OB_FAIL(compactor->compact())) {
      LOG_WARN("fail to compact tables", KR(ret));
    } else if (OB_FAIL(compactor->get_table(table, mem_ctx_->allocator_))) {
      LOG_WARN("fail to get table", KR(ret));
    } else if (OB_FAIL(mem_ctx_->tables_.push_back(table))) {
      LOG_WARN("fail to add table", KR(ret));
    }
  }

  if (nullptr != compactor) {
    compactor->~ObIDirectLoadTabletTableCompactor();
    compactor = nullptr;
  }
  return ret;
}

int ObDirectLoadMemDump::do_dump()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dump_tables())) {
    LOG_WARN("fail to dump tables", KR(ret));
  } else {
    int64_t finished = ATOMIC_AAF(&(context_ptr_->finished_sub_dump_count_), 1);
    if (finished == context_ptr_->sub_dump_count_) {
      if (OB_FAIL(compact_tables())) {
        LOG_WARN("fail to compact tables", KR(ret));
      }
      ATOMIC_AAF(&(mem_ctx_->fly_mem_chunk_count_), -context_ptr_->mem_chunk_array_.count());
    }
  }
  ATOMIC_AAF(&(mem_ctx_->running_dump_count_), -1);
  return ret;
}

} // namespace storage
} // namespace oceanbase
