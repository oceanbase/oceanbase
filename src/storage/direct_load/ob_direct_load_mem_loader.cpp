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

#include "storage/direct_load/ob_direct_load_mem_loader.h"
#include "storage/direct_load/ob_direct_load_external_table.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace observer;

/**
 * ObDirectLoadMemLoader
 */

ObDirectLoadMemLoader::ObDirectLoadMemLoader(ObTableLoadTableCtx *ctx,
                                             ObDirectLoadMemContext *mem_ctx)
  : ctx_(ctx), mem_ctx_(mem_ctx)
{
}

ObDirectLoadMemLoader::~ObDirectLoadMemLoader()
{
}

int ObDirectLoadMemLoader::add_table(const ObDirectLoadTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_handle.is_valid() || !table_handle.get_table()->is_external_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_handle));
  } else {
    ObDirectLoadExternalTable *external_table = static_cast<ObDirectLoadExternalTable *>(table_handle.get_table());
    if (OB_UNLIKELY(external_table->get_fragments().empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("external table should have at least one fragment", KR(ret), KPC(external_table));
    } else if (OB_FAIL(fragments_.push_back(external_table->get_fragments()))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadMemLoader::work()
{
  typedef ObDirectLoadExternalBlockReader<ObDirectLoadExternalMultiPartitionRow> ExternalReader;
  int ret = OB_SUCCESS;
  const ObDirectLoadExternalMultiPartitionRow *external_row = nullptr;
  ChunkType *chunk = nullptr;
  RowType row;
  for (int64_t i = 0; OB_SUCC(ret) && OB_LIKELY(!mem_ctx_->has_error_) && i < fragments_.count(); ++i) {
    ObDirectLoadExternalFragment &fragment = fragments_.at(i);
    ExternalReader external_reader;
    if (OB_FAIL(external_reader.init(mem_ctx_->table_data_desc_.external_data_block_size_,
                                     mem_ctx_->table_data_desc_.compressor_type_))) {
      LOG_WARN("fail to init external reader", KR(ret));
    } else if (OB_FAIL(external_reader.open(fragment.file_handle_, 0, fragment.file_size_))) {
      LOG_WARN("fail to open file", KR(ret));
    }
    while (OB_SUCC(ret) && OB_LIKELY(!mem_ctx_->has_error_)) {
      if (external_row == nullptr && OB_FAIL(external_reader.get_next_item(external_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next item", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (chunk == nullptr) {
        // 等待内存空出
        while (mem_ctx_->fly_mem_chunk_count_ >= mem_ctx_->max_mem_chunk_count_ &&
               OB_LIKELY(!mem_ctx_->has_error_)) {
          usleep(500000);
        }
        if (OB_UNLIKELY(mem_ctx_->has_error_)) {
          break;
        } else if (OB_FAIL(acquire_chunk(chunk))) {
          LOG_WARN("fail to acquire chunk", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        row = *external_row;
        if (OB_FAIL(chunk->add_item(row))) {
          if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
            LOG_WARN("fail to add item", KR(ret));
          } else {
            ret = OB_SUCCESS;
            if (OB_FAIL(close_chunk(chunk))) {
              LOG_WARN("fail to close chunk", KR(ret));
            }
          }
        } else {
          external_row = nullptr;
          ATOMIC_AAF(&ctx_->job_stat_->store_.compact_stage_load_rows_, 1);
        }
      }
    }
    if (OB_SUCC(ret)) {
      fragment.reset(); // 释放磁盘空间
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY(!mem_ctx_->has_error_)) {
    if (chunk != nullptr && OB_FAIL(close_chunk(chunk))) {
      LOG_WARN("fail to close chunk", KR(ret));
    }
  }

  if (chunk != nullptr) {
    chunk->~ChunkType();
    ob_free(chunk);
    chunk = nullptr;
  }

  return ret;
}

int ObDirectLoadMemLoader::acquire_chunk(ChunkType *&chunk)
{
  int ret = OB_SUCCESS;
  chunk = nullptr;
  ObMemAttr mem_attr(MTL_ID(), "TLD_MemChunk");
  int64_t sort_memory = 0;
  if (mem_ctx_->exe_mode_ == ObTableLoadExeMode::MAX_TYPE) {
    sort_memory = mem_ctx_->mem_chunk_size_;
  } else if (OB_FAIL(ObTableLoadService::get_sort_memory(sort_memory))) {
    LOG_WARN("fail to get sort memory", KR(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(chunk = OB_NEW(ChunkType, mem_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadExternalMultiPartitionRowChunk", KR(ret));
    } else if (OB_FAIL(chunk->init(MTL_ID(), sort_memory))) {
      LOG_WARN("fail to init external sort", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != chunk) {
      OB_DELETE(ChunkType, mem_attr, chunk);
      chunk = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadMemLoader::close_chunk(ChunkType *&chunk)
{
  int ret = OB_SUCCESS;
  CompareType compare;
  if (OB_FAIL(compare.init(*(mem_ctx_->datum_utils_), mem_ctx_->dup_action_))) {
    LOG_WARN("fail to init compare", KR(ret));
  } else if (OB_FAIL(chunk->sort(compare))) {
    LOG_WARN("fail to sort chunk", KR(ret));
  } else if (OB_FAIL(mem_ctx_->mem_chunk_queue_.push(chunk))) {
    LOG_WARN("fail to push", KR(ret));
  } else {
    chunk = nullptr;
  }

  if (chunk != nullptr) {
    chunk->~ChunkType();
    ob_free(chunk);
    chunk = nullptr;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
