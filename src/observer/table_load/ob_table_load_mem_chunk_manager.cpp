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

#include "ob_table_load_mem_chunk_manager.h"
#include "observer/table_load/ob_table_load_task.h"
#include "src/observer/table_load/ob_table_load_task_scheduler.h"
#include "src/observer/table_load/ob_table_load_pre_sorter.h"
#include "src/observer/table_load/ob_table_load_pre_sorter.h"

namespace oceanbase {

namespace observer {

using namespace storage;

ObTableLoadChunkNode::ObTableLoadChunkNode() : chunk_(nullptr), chunk_node_mutex_(), is_used_(false) {}
ObTableLoadChunkNode::~ObTableLoadChunkNode()
{
  if (OB_NOT_NULL(chunk_)) {
    chunk_->~ChunkType();
    ob_free(chunk_);
    chunk_ = nullptr;
  }
}

ObTableLoadMemChunkManager::ObTableLoadMemChunkManager()
  :  store_ctx_(nullptr),
     mem_ctx_(nullptr),
     chunks_count_(0),
     max_chunks_count_(0),
     mem_chunk_size_(0),
     close_task_count_(0),
     finished_close_task_count_(0),
     is_inited_(false)
{
}

ObTableLoadMemChunkManager::~ObTableLoadMemChunkManager()
{
  for (int64_t i = 0; i < chunk_nodes_.count(); ++i) {
    if (OB_NOT_NULL(chunk_nodes_[i])) {
      chunk_nodes_[i]->~ObTableLoadChunkNode();
      ob_free(chunk_nodes_[i]);
      chunk_nodes_[i] = nullptr;
    }
  }
}

int ObTableLoadMemChunkManager::init(ObTableLoadStoreCtx *store_ctx, ObDirectLoadMemContext *mem_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadMemChunkManager init twice", KR(ret));
  } else {
    store_ctx_ = store_ctx;
    chunks_count_ = mem_ctx->table_data_desc_.max_mem_chunk_count_ / 2;
    max_chunks_count_ = mem_ctx->table_data_desc_.max_mem_chunk_count_;
    mem_chunk_size_ = mem_ctx->table_data_desc_.mem_chunk_size_;
    exe_mode_ = mem_ctx->table_data_desc_.exe_mode_;
    mem_ctx_ = mem_ctx;
    for (int64_t i = 0; OB_SUCC(ret) && i < chunks_count_; ++i) {
      ObTableLoadChunkNode *node = OB_NEW(ObTableLoadChunkNode, ObMemAttr(MTL_ID(), "TLD_ChunkNode"));
      if (OB_ISNULL(node)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate mem");
      } else if (OB_FAIL(chunk_nodes_.push_back(node))) {
        LOG_WARN("fail to push chunk node", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::alloc_chunk(ChunkType *&chunk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemChunkManager is not init", KR(ret));
  } else if (OB_NOT_NULL(chunk)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("chunk should be nullptr", KR(ret));
  } else {
    // wait available memory
    while (mem_ctx_->fly_mem_chunk_count_ >= max_chunks_count_ &&
            !(mem_ctx_->has_error_)) {
      usleep(50000);
    }
    if (OB_SUCC(ret)) {
      chunk = OB_NEW(ChunkType, ObMemAttr(MTL_ID(), "TLD_MemChunkVal"));
      if (OB_ISNULL(chunk)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate mem", KR(ret));
      } else {
        ATOMIC_AAF(&(mem_ctx_->fly_mem_chunk_count_), 1);
        int64_t sort_memory = 0;
        if (observer::ObTableLoadExeMode::MAX_TYPE == exe_mode_) {
          sort_memory = mem_chunk_size_;
        } else if (OB_FAIL(ObTableLoadService::get_sort_memory(sort_memory))) {
          LOG_WARN("fail to get sort memory", KR(ret));
        }
        if (OB_SUCC(ret) && OB_FAIL(chunk->init(MTL_ID(), sort_memory))) {
          LOG_WARN("fail to init chunk", KR(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(chunk)) {
        chunk->~ChunkType();
        ob_free(chunk);
        chunk = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::get_chunk_node_id(int64_t &chunk_node_id) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemChunkManager is not inited");
  } else if (-1 != chunk_node_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("chunk node id is invalid",K(chunk_node_id), KR(ret));
  } else {
    chunk_node_id = ObRandom::rand(0, chunks_count_ - 1);
  }
  return ret;
}

int ObTableLoadMemChunkManager::get_chunk(int64_t &chunk_node_id, ChunkType *&chunk) {
  int ret = OB_SUCCESS;
  bool get_chunk_success = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemChunkManager is not inited");
  } else if (OB_FAIL(get_chunk_node_id(chunk_node_id))) {
    LOG_WARN("fail to get chunk id", KR(ret));
  } else if (OB_NOT_NULL(chunk)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("chunk should be nullptr", KR(ret));
  } else {
    while(OB_SUCC(ret) && !get_chunk_success) {
      if (!chunk_nodes_[chunk_node_id]->is_used_) {
        ObMutexGuard guard(chunk_nodes_[chunk_node_id]->chunk_node_mutex_);
        if (!chunk_nodes_[chunk_node_id]->is_used_) {
          chunk_nodes_[chunk_node_id]->is_used_ = true;
          // guard.~ObMutexGuard();
          chunk = chunk_nodes_[chunk_node_id]->chunk_;
          if (OB_ISNULL(chunk)) {
            if (OB_FAIL(alloc_chunk(chunk))) {
              LOG_WARN("fail to alloc chunk", KR(ret));
            } else {
              chunk_nodes_[chunk_node_id]->chunk_ = chunk;
            }
          }
          if (OB_SUCC(ret)) {
            get_chunk_success = true;
          }
        }
      }
      if (!get_chunk_success) {
        ++chunk_node_id;
        chunk_node_id %= chunks_count_;
      }
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::push_chunk(int64_t chunk_node_id) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemChunkManager is not inited");
  } else if (chunk_node_id < 0 || chunk_node_id >= chunks_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("chunk node id is invalid", K(chunk_node_id), KR(ret));
  } else {
    if (!chunk_nodes_[chunk_node_id]->is_used_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("chunk node should be used", KR(ret));
    } else {
      chunk_nodes_[chunk_node_id]->is_used_ = false;
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::close_all_chunk()
{
  int ret = OB_SUCCESS;
  close_task_count_ = chunks_count_;
  for (int64_t i = 0; OB_SUCC(ret) && i < chunks_count_; ++i) {
    ObTableLoadTask *task = nullptr;
    int64_t thread_id = i % store_ctx_->pre_sorter_->other_task_count_;
    if (OB_FAIL(store_ctx_->ctx_->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(task->set_processor<CloseChunkTaskProcessor>(this, i))) {
      LOG_WARN("fail to set pre sort parallel merge task processor", KR(ret));
    } else if (OB_FAIL(task->set_callback<CloseChunkTaskCallback>(store_ctx_->ctx_, this))) {
      LOG_WARN("fail to set pre sort parallel merge task callback", KR(ret));
    } else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(thread_id, task))) {
      LOG_WARN("fail to add task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(task)) {
        store_ctx_->ctx_->free_task(task);
      }
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::close_chunk(int64_t chunk_node_id)
{
  int ret = OB_SUCCESS;
  ChunkType *chunk = chunk_nodes_[chunk_node_id]->chunk_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMemChunkManager is not init", KR(ret));
  } else if (OB_NOT_NULL(chunk)) {
    CompareType compare;
    if (OB_FAIL(compare.init(*(mem_ctx_->datum_utils_), mem_ctx_->dup_action_))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(chunk->sort(compare))) {
      LOG_WARN("fail to sort chunk", KR(ret));
    } else if (OB_FAIL(mem_ctx_->mem_chunk_queue_.push(chunk))) {
      LOG_WARN("fail to push", KR(ret));
    } else {
      chunk_nodes_[chunk_node_id]->chunk_ = nullptr;
      chunk = nullptr;
      ObMutexGuard guard(chunk_nodes_[chunk_node_id]->chunk_node_mutex_);
      chunk_nodes_[chunk_node_id]->is_used_ = false;
    }
  }
  return ret;
}

int ObTableLoadMemChunkManager::handle_close_task_finished()
{
  int ret = OB_SUCCESS;
  int tmp_finished_close_task_count = ATOMIC_AAF(&finished_close_task_count_, 1);
  if (tmp_finished_close_task_count == close_task_count_) {
    if (OB_FAIL(store_ctx_->pre_sorter_->set_all_trans_finished())) {
      LOG_WARN("fail to set all trans finished", KR(ret));
    }
  }
  return ret;
}

class ObTableLoadMemChunkManager::CloseChunkTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  CloseChunkTaskProcessor(ObTableLoadTask &task,
                          ObTableLoadMemChunkManager *chunk_manager,
                          int64_t chunk_node_id)
    : ObITableLoadTaskProcessor(task),
      chunk_manager_(chunk_manager),
      chunk_node_id_(chunk_node_id)
  {
  }
  virtual ~CloseChunkTaskProcessor()
  {
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(chunk_manager_->close_chunk(chunk_node_id_))) {
      LOG_WARN("fail to close chunk", K(chunk_node_id_), KR(ret));
    }
    return ret;
  }
private:
  ObTableLoadMemChunkManager *chunk_manager_;
  int64_t chunk_node_id_;
};

class ObTableLoadMemChunkManager::CloseChunkTaskCallback : public ObITableLoadTaskCallback
{
public:
  CloseChunkTaskCallback(ObTableLoadTableCtx * ctx, ObTableLoadMemChunkManager *chunk_manager)
    : ctx_(ctx), chunk_manager_(chunk_manager)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CloseChunkTaskCallback()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = ret_code;
    if (OB_SUCC(ret) && OB_FAIL(chunk_manager_->handle_close_task_finished())) {
      LOG_WARN("fail to handle close task finished", KR(ret));
    }
    if (OB_FAIL(ret)) {
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadMemChunkManager *chunk_manager_;
};

} /* namespace storage */
} /* namespace oceanbase */
