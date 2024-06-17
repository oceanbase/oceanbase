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

#include "storage/direct_load/ob_direct_load_multiple_heap_table_sorter.h"
#include "storage/direct_load/ob_direct_load_external_block_reader.h"
#include "storage/direct_load/ob_direct_load_external_table.h"
#include "storage/direct_load/ob_direct_load_mem_sample.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_map.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_builder.h"
#include "observer/table_load/ob_table_load_service.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace observer;

ObDirectLoadMultipleHeapTableSorter::ObDirectLoadMultipleHeapTableSorter(
  ObDirectLoadMemContext *mem_ctx)
  : ctx_(nullptr),
    mem_ctx_(mem_ctx),
    allocator_("TLD_Sorter"),
    extra_buf_(nullptr),
    index_dir_id_(-1),
    data_dir_id_(-1),
    heap_table_array_(nullptr),
    heap_table_allocator_(nullptr)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadMultipleHeapTableSorter::~ObDirectLoadMultipleHeapTableSorter()
{
}

int ObDirectLoadMultipleHeapTableSorter::init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(extra_buf_ = static_cast<char *>(allocator_.alloc(mem_ctx_->table_data_desc_.extra_buf_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate extra buf", KR(ret));
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableSorter::add_table(ObIDirectLoadPartitionTable *table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table));
  } else {
    ObDirectLoadExternalTable *external_table = nullptr;
    if (OB_ISNULL(external_table = dynamic_cast<ObDirectLoadExternalTable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), KPC(table));
    } else if (OB_FAIL(fragments_.push_back(external_table->get_fragments()))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableSorter::close_chunk(ObDirectLoadMultipleHeapTableMap *&chunk)
{
  int ret = OB_SUCCESS;
  ObArray<common::ObTabletID> keys;
  ObDirectLoadMultipleHeapTableBuilder table_builder;
  ObDirectLoadMultipleHeapTableBuildParam table_builder_param;
  keys.set_tenant_id(MTL_ID());
  table_builder_param.table_data_desc_ = mem_ctx_->table_data_desc_;
  table_builder_param.file_mgr_ = mem_ctx_->file_mgr_;
  table_builder_param.extra_buf_size_ = mem_ctx_->table_data_desc_.extra_buf_size_;
  table_builder_param.extra_buf_ = extra_buf_;
  table_builder_param.index_dir_id_ = index_dir_id_;
  table_builder_param.data_dir_id_ = data_dir_id_;
  if (chunk == nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("chunk is null", KR(ret));
  } else if (OB_FAIL(table_builder.init(table_builder_param))) {
    LOG_WARN("fail to init table builder", KR(ret));
  } else if (OB_FAIL(chunk->get_all_key_sorted(keys))) {
    LOG_WARN("fail to get keys", KR(ret));
  }
  ObDatumRow datum_row;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum_row.init(mem_ctx_->column_count_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      datum_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row.mvcc_row_flag_.set_last_multi_version_row(true);
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); i ++) {
    ObArray<const ObDirectLoadConstExternalMultiPartitionRow *> bag;
    bag.set_tenant_id(MTL_ID());
    if (OB_FAIL(chunk->get(keys.at(i), bag))) {
      LOG_WARN("fail to get bag", KR(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < bag.count(); j ++) {
      if (OB_FAIL(bag.at(j)->to_datums(datum_row.storage_datums_, datum_row.count_))) {
        LOG_WARN("fail to transfer dataum row", KR(ret));
      } else if (OB_FAIL(table_builder.append_row(keys.at(i), bag.at(j)->seq_no_, datum_row))) {
        LOG_WARN("fail to append row", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_builder.close())) {
      LOG_WARN("fail to close table builder", KR(ret));
    } else if (OB_FAIL(get_tables(table_builder))) {
      LOG_WARN("fail to get tables", KR(ret));
    }
  }

  if (chunk != nullptr) {
    chunk->~ObDirectLoadMultipleHeapTableMap();
    ob_free(chunk);
    chunk = nullptr;
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableSorter::get_tables(
  ObIDirectLoadPartitionTableBuilder &table_builder)
{
  int ret = OB_SUCCESS;
  ObArray<ObIDirectLoadPartitionTable *> table_array;
  table_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(table_builder.get_tables(table_array, *heap_table_allocator_))) {
    LOG_WARN("fail to get tables", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
    ObIDirectLoadPartitionTable *table = table_array.at(i);
    ObDirectLoadMultipleHeapTable *heap_table = nullptr;
    if (OB_ISNULL(heap_table = dynamic_cast<ObDirectLoadMultipleHeapTable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), KPC(table));
    } else if (OB_FAIL(heap_table_array_->push_back(heap_table))) {
      LOG_WARN("fail to push back heap table", KR(ret));
    } else {
      ATOMIC_AAF(&ctx_->job_stat_->store_.compact_stage_product_tmp_files_, 1);
    }
  }
  return ret;
}

int ObDirectLoadMultipleHeapTableSorter::work()
{
  typedef ObDirectLoadExternalMultiPartitionRow RowType;
  typedef ObDirectLoadMultipleHeapTableMap ChunkType;
  typedef ObDirectLoadExternalBlockReader<RowType> ExternalReader;
  int ret = OB_SUCCESS;
  const RowType *row = nullptr;
  ObDirectLoadConstExternalMultiPartitionRow const_row;
  ChunkType *chunk = nullptr;

  if (OB_UNLIKELY(index_dir_id_ <= 0 || data_dir_id_ <= 0 || nullptr == heap_table_array_ ||
                  nullptr == heap_table_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K_(index_dir_id), K_(data_dir_id), KP_(heap_table_array),
             KP_(heap_table_allocator));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < fragments_.count(); i++) {
    const ObDirectLoadExternalFragment &fragment = fragments_.at(i);
    ExternalReader external_reader;
    if (OB_FAIL(external_reader.init(mem_ctx_->table_data_desc_.external_data_block_size_,
                                     mem_ctx_->table_data_desc_.compressor_type_))) {
      LOG_WARN("fail to init external reader", KR(ret));
    } else if (OB_FAIL(external_reader.open(fragment.file_handle_, 0, fragment.file_size_))) {
      LOG_WARN("fail to open file", KR(ret));
    }
    while (OB_SUCC(ret) && !(mem_ctx_->has_error_)) {
      if (row == nullptr) {
        if (OB_FAIL(external_reader.get_next_item(row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next item", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        }
      }
      if (OB_SUCC(ret) && chunk == nullptr) {
        if (mem_ctx_->has_error_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("some error ocurr", KR(ret));
        }
        if (OB_SUCC(ret)) {
          int64_t sort_memory = 0;
          if (mem_ctx_->table_data_desc_.exe_mode_ == observer::ObTableLoadExeMode::MAX_TYPE) {
            sort_memory = mem_ctx_->table_data_desc_.heap_table_mem_chunk_size_;
          } else if (OB_FAIL(ObTableLoadService::get_sort_memory(sort_memory))) {
            LOG_WARN("fail to get sort memory", KR(ret));
          }
          if (OB_SUCC(ret)) {
            chunk = OB_NEW(ChunkType, ObMemAttr(MTL_ID(), "TLD_MemChunkVal"), sort_memory);
            if (chunk == nullptr) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else if (OB_FAIL(chunk->init())) {
              LOG_WARN("fail to init external sort", KR(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        const_row = *row;
        ret = chunk->add_row(row->tablet_id_, const_row);
        if (ret == OB_BUF_NOT_ENOUGH) {
          ret = OB_SUCCESS;
          if (OB_FAIL(close_chunk(chunk))) {
            LOG_WARN("fail to close chunk", KR(ret));
          }
        } else if (ret != OB_SUCCESS) {
          LOG_WARN("fail to add item", KR(ret));
        } else {
          row = nullptr;
          ATOMIC_AAF(&ctx_->job_stat_->store_.compact_stage_load_rows_, 1);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (chunk != nullptr && OB_FAIL(close_chunk(chunk))) {
      LOG_WARN("fail to close chunk", KR(ret));
    }
  }

  if (chunk != nullptr) {
    chunk->~ObDirectLoadMultipleHeapTableMap();
    ob_free(chunk);
    chunk = nullptr;
  }

  return ret;
}


}
}
