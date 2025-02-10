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
#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_mem_chunk_manager.h"
#include "observer/table_load/ob_table_load_store_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace storage;

ObTableLoadChunkNode::ObTableLoadChunkNode() : chunk_(nullptr), is_used_(false) {}
ObTableLoadChunkNode::~ObTableLoadChunkNode()
{
  if (OB_NOT_NULL(chunk_)) {
    chunk_->~ChunkType();
    ob_free(chunk_);
    chunk_ = nullptr;
  }
}

ObTableLoadMemChunkManager::ObTableLoadMemChunkManager()
  : store_ctx_(nullptr), mem_ctx_(nullptr), is_inited_(false)
{
  chunk_nodes_.set_attr(ObMemAttr(MTL_ID(), "TLD_ChunkNode"));
}

ObTableLoadMemChunkManager::~ObTableLoadMemChunkManager() {}

int ObTableLoadMemChunkManager::init(ObTableLoadStoreCtx *store_ctx,
                                     ObDirectLoadMemContext *mem_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadMemChunkManager init twice", KR(ret), KP(this));
  } else {
    store_ctx_ = store_ctx;
    mem_ctx_ = mem_ctx;
    const int64_t chunk_count = MAX(mem_ctx->max_mem_chunk_count_ / 2, 1);
    if (OB_FAIL(chunk_nodes_.prepare_allocate(chunk_count))) {
      LOG_WARN("fail to prepare allocate chunk node", KR(ret), K(chunk_count));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::get_chunk(int64_t &chunk_node_id, ChunkType *&chunk)
{
  int ret = OB_SUCCESS;
  chunk_node_id = -1;
  chunk = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemChunkManager not init", KR(ret), KP(this));
  } else {
    int64_t cur_chunk_node_id = ObRandom::rand(0, chunk_nodes_.count() - 1);
    while (OB_SUCC(ret) && -1 == chunk_node_id) {
      ObTableLoadChunkNode &chunk_node = chunk_nodes_.at(cur_chunk_node_id);
      if (OB_UNLIKELY(mem_ctx_->has_error_)) {
        ret = store_ctx_->get_error_code();
        LOG_WARN("pre sort has error, task canceled", KR(ret));
      } else if (!chunk_node.is_used() && chunk_node.set_used()) {
        if (nullptr == chunk_node.chunk_) {
          // 等待内存空出
          while (mem_ctx_->fly_mem_chunk_count_ >= mem_ctx_->max_mem_chunk_count_ &&
                 OB_LIKELY(!mem_ctx_->has_error_)) {
            usleep(50000);
          }
          if (OB_UNLIKELY(mem_ctx_->has_error_)) {
            // do nothing
          } else if (OB_FAIL(acquire_chunk(chunk_node.chunk_))) {
            LOG_WARN("fail to acquire chunk", KR(ret));
          } else {
            chunk_node_id = cur_chunk_node_id;
            chunk = chunk_node.chunk_;
          }
        } else {
          chunk_node_id = cur_chunk_node_id;
          chunk = chunk_node.chunk_;
        }
      } else {
        // switch next chunk
        ++cur_chunk_node_id;
        cur_chunk_node_id %= chunk_nodes_.count();
      }
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::get_unclosed_chunks(ObIArray<int64_t> &chunk_node_ids)
{
  int ret = OB_SUCCESS;
  chunk_node_ids.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemChunkManager not init", KR(ret), KP(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < chunk_nodes_.count(); ++i) {
      ObTableLoadChunkNode &chunk_node = chunk_nodes_.at(i);
      if (OB_UNLIKELY(chunk_node.is_used())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected chunk already is used", KR(ret), K(i), K(chunk_node));
      } else if (nullptr == chunk_node.chunk_) {
        // do nothing
      } else if (OB_UNLIKELY(!chunk_node.set_used())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected set chunk node used failed", KR(ret), K(i), K(chunk_node));
      } else if (OB_FAIL(chunk_node_ids.push_back(i))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::push_chunk(int64_t chunk_node_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemChunkManager not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(chunk_node_id < 0 || chunk_node_id >= chunk_nodes_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(chunk_node_id));
  } else {
    ObTableLoadChunkNode &chunk_node = chunk_nodes_.at(chunk_node_id);
    if (OB_UNLIKELY(!chunk_node.is_used())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("chunk node should be used", KR(ret), K(chunk_node_id), K(chunk_node));
    } else if (OB_UNLIKELY(!chunk_node.set_unused())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected set chunk node unused failed", KR(ret), K(chunk_node_id),
               K(chunk_node));
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::close_chunk(int64_t chunk_node_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemChunkManager is not init", KR(ret));
  } else if (OB_UNLIKELY(chunk_node_id < 0 || chunk_node_id >= chunk_nodes_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(chunk_node_id));
  } else {
    ObTableLoadChunkNode &chunk_node = chunk_nodes_.at(chunk_node_id);
    CompareType compare;
    if (OB_UNLIKELY(!chunk_node.is_used() || nullptr == chunk_node.chunk_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("chunk node should be used and chunk not null", KR(ret), K(chunk_node_id),
               K(chunk_node));
    } else if (OB_FAIL(compare.init(*(mem_ctx_->datum_utils_), mem_ctx_->dup_action_))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(chunk_node.chunk_->sort(compare))) {
      LOG_WARN("fail to sort chunk", KR(ret));
    } else if (OB_FAIL(mem_ctx_->mem_chunk_queue_.push(chunk_node.chunk_))) {
      LOG_WARN("fail to push", KR(ret));
    } else if (FALSE_IT(chunk_node.chunk_ = nullptr)) {
    } else if (OB_UNLIKELY(!chunk_node.set_unused())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected set chunk node unused failed", KR(ret), K(chunk_node_id),
               K(chunk_node));
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::acquire_chunk(ChunkType *&chunk)
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

} // namespace observer
} // namespace oceanbase
