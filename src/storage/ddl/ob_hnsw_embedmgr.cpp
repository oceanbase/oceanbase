/**
 * Copyright (c) 2023 OceanBase
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
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log.h"
#include "storage/ddl/ob_ddl_pipeline.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "storage/ddl/ob_hnsw_embedmgr.h"

using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;

// -------------------------------- ObEmbeddingConfig --------------------------------
bool ObEmbeddingConfig::is_valid() const
{
  bool is_valid = true;
  if (OB_UNLIKELY(model_url_.empty() || model_name_.empty() || user_key_.empty() || provider_.empty())) {
    is_valid = false;
  }
  return is_valid;
}
void ObEmbeddingConfig::set_config(const ObString &model_url, const ObString &model_name,
                                    const ObString &user_key, const ObString &provider)
{
  model_url_ = model_url;
  model_name_ = model_name;
  user_key_ = user_key;
  provider_ = provider;
}

int ObEmbeddingConfig::assign(const ObEmbeddingConfig &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (!other.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(other));
    } else {
      model_url_ = other.model_url_;
      model_name_ = other.model_name_;
      user_key_ = other.user_key_;
      provider_ = other.provider_;
    }
  }
  return ret;
}

// -------------------------------- ObEmbeddingResult --------------------------------
int ObEmbeddingResult::set_extra_cols(const common::ObArray<blocksstable::ObStorageDatum> &src_extras, ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  extra_values_.reset();
  if (src_extras.count() == 0) {
    // nothing to do
  } else if (OB_FAIL(extra_values_.reserve(src_extras.count()))) {
    LOG_WARN("reserve extra array failed", K(ret), K(src_extras.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_extras.count(); ++i) {
      const blocksstable::ObStorageDatum &src_datum = src_extras.at(i);
      blocksstable::ObStorageDatum dst_datum;
      if (OB_FAIL(dst_datum.deep_copy(src_datum, allocator))) {
        LOG_WARN("deep copy extra datum failed", K(ret), K(i));
      } else if (OB_FAIL(extra_values_.push_back(dst_datum))) {
        LOG_WARN("push extra datum failed", K(ret), K(i));
      }
    }
  }
  return ret;
}
void ObEmbeddingResult::reset()
{
  vector_ = nullptr;
  vector_dim_ = 0;
  text_.reset();
  extra_values_.reset();
  status_ = NEED_EMBEDDING;
}

// -------------------------------- ObEmbeddingIOCallback --------------------------------
ObEmbeddingIOCallback::~ObEmbeddingIOCallback()
{
  is_inited_ = false;
  mgr_ = nullptr;
  task_ = nullptr;
  batch_info_ = nullptr;
}

int ObEmbeddingIOCallback::init(ObEmbeddingTaskMgr *mgr, const int64_t slot_idx,
                                 ObTaskBatchInfo *batch_info, share::ObEmbeddingTask *task, int64_t dim)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("embedding batch callback init twice", K(ret));
  } else if (OB_ISNULL(mgr) || slot_idx < 0 || OB_ISNULL(batch_info) || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args for embedding batch callback init", K(ret), KP(mgr), K(slot_idx), KP(batch_info), KP(task));
  } else {
    mgr_ = mgr;
    slot_idx_ = slot_idx;
    batch_info_ = batch_info;
    task_ = task;
    dim_ = dim;
    is_inited_ = true;
  }
  return ret;
}

int ObEmbeddingIOCallback::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("embedding callback not inited", K(ret));
  } else {
    common::ObArray<float*> vectors;
    if (OB_FAIL(task_->get_async_result(vectors))) {
      LOG_WARN("get async result failed", K(ret), K(slot_idx_), "vec_cnt", vectors.count());
    } else if (vectors.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty vectors", K(ret), K(slot_idx_), "vec_cnt", vectors.count());
    } else {
      int64_t embedding_count = batch_info_->get_need_embedding_count();
      const common::ObArray<ObEmbeddingResult*> &results = batch_info_->get_results();
      if (vectors.count() != embedding_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("count mismatch", K(ret), K(embedding_count), K(vectors.count()), K(slot_idx_));
      } else {
        int64_t vec_idx = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < batch_info_->get_count(); ++i) {
          ObEmbeddingResult *r = results.at(i);
          if (OB_ISNULL(r)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null result slot in batch", K(ret), K(i), K(slot_idx_));
          } else if (r->need_embedding()) {
            if (vec_idx >= vectors.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("vector index out of bounds", K(ret), K(vec_idx), K(vectors.count()));
            } else {
              //deep copy
              const int64_t bytes = dim_ * static_cast<int64_t>(sizeof(float));
              MEMCPY(r->get_vector(), vectors.at(vec_idx), bytes);
              vec_idx++;
            }
          } else {
            //do nothing
          }
        }
      }
    }
  }

  // Mark task as ready and return ret to embedding task mgr, regardless of success.
  int mark_ret = mgr_->mark_task_ready(slot_idx_, ret);
  if (OB_SUCCESS != mark_ret) {
    LOG_WARN("mark task ready failed", K(mark_ret), K(slot_idx_), "task_ret", ret);
  }

  return ret;
}

// -------------------------------- ObTaskBatchInfo --------------------------------
int ObTaskBatchInfo::init(const int64_t batch_size, const int64_t vec_dim)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(batch_size <= 0 || vec_dim <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(batch_size), K(vec_dim));
  } else if (OB_UNLIKELY(batch_size_ > 0)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTaskBatchInfo init twice", K(ret), K_(batch_size));
  } else {
    batch_size_ = batch_size;
    vec_dim_ = vec_dim;
    current_count_ = 0;

    if (OB_FAIL(results_.reserve(batch_size))) {
      LOG_WARN("reserve results array failed", K(ret), K(batch_size));
    } else {
      // Pre-allocate all result objects
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        void *buf = allocator_.alloc(sizeof(ObEmbeddingResult));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate result object failed", K(ret), K(i), K(batch_size));
        } else {
          ObEmbeddingResult *result = new (buf) ObEmbeddingResult();
          if (OB_FAIL(results_.push_back(result))) {
            LOG_WARN("push result to array failed", K(ret), K(i));
            result->~ObEmbeddingResult();
          }
        }
      }
    }
  }
  return ret;
}

int ObTaskBatchInfo::add_item(const common::ObString &text,
                              const common::ObArray<blocksstable::ObStorageDatum> &extras,
                              const ObEmbeddingResult::EmbeddingStatus status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(current_count_ >= batch_size_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("batch is full", K(ret), K_(current_count), K_(batch_size));
  } else {
    ObEmbeddingResult *result = results_.at(current_count_);
    result->set_status(status);
    if (status == ObEmbeddingResult::NEED_EMBEDDING) {
      need_embedding_count_++;
    }

    //deep copy
    if (text.length() > 0) {
      char *text_buf = static_cast<char*>(allocator_.alloc(text.length()));
      if (OB_ISNULL(text_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate text buffer failed", K(ret), K(text.length()));
      } else {
        MEMCPY(text_buf, text.ptr(), text.length());
        result->set_text(common::ObString(text.length(), text_buf));
      }
    }

    // deep copy extras
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result->set_extra_cols(extras, allocator_))) {
        LOG_WARN("deep copy extras failed", K(ret));
      }
    }

    // pre-allocate space (will be filled by embedding task)
    if (OB_SUCC(ret)) {
      float *vec_buf = static_cast<float*>(allocator_.alloc(vec_dim_ * sizeof(float)));
      if (OB_ISNULL(vec_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate vector buffer failed", K(ret), K_(vec_dim));
      } else {
        result->set_vector(vec_buf, vec_dim_);
      }
    }

    if (OB_SUCC(ret)) {
      current_count_++;
    }
  }
  return ret;
}

void ObTaskBatchInfo::reset()
{
  for (int64_t i = 0; i < results_.count(); ++i) {
    if (results_.at(i) != nullptr) {
      results_.at(i)->~ObEmbeddingResult();
    }
  }
  results_.reset();
  allocator_.reset();
  batch_size_ = 0;
  current_count_ = 0;
  vec_dim_ = 0;
  need_embedding_count_ = 0;
}

// -------------------------------- Slot --------------------------------
void Slot::reset()
{
  task_ = nullptr;
  if (batch_info_ != nullptr) {
    batch_info_->~ObTaskBatchInfo();
    ob_free(batch_info_);
    batch_info_ = nullptr;
  }
  ready_ = false;
  ret_code_ = 0;
}

// -------------------------------- ObTaskSlotRing --------------------------------
ObTaskSlotRing::~ObTaskSlotRing()
{
  reset();
}

int ObTaskSlotRing::init(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid capacity", K(ret), K(capacity));
  } else if (slots_.count() > 0) {
    ret = OB_INIT_TWICE;
    LOG_WARN("slot ring already initialized", K(ret));
  } else {
    capacity_ = capacity + 1; // +1 for the extra slot to differentiate between full and empty queue
    if (OB_FAIL(slots_.prepare_allocate(capacity_))) {
      LOG_WARN("prepare allocate slots failed", K(ret), K(capacity_));
    } else {
      next_idx_ = 0;
      head_idx_ = 0;
    }
  }
  return ret;
}

int ObTaskSlotRing::reserve_slot(int64_t &slot_idx)
{
  int ret = OB_SUCCESS;
  slot_idx = -1;
  ObSpinLockGuard guard(lock_);
  const int64_t cap = slots_.count();
  if (cap <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slots count is 0", K(ret), K(cap));
  } else {
    // Reserve one empty slot to differentiate between full and empty queue
    int64_t next = (next_idx_ + 1) % cap;
    if (next == head_idx_) {
      ret = OB_EAGAIN;
    } else if (slots_.at(next_idx_).task_ != nullptr || slots_.at(next_idx_).batch_info_ != nullptr || slots_.at(next_idx_).ready_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("next slot not empty", K(ret), K(next_idx_));
    } else {
      Slot &slot = slots_.at(next_idx_);
      slot.reset();
      slot_idx = next_idx_;
      next_idx_ = next;
    }
  }
  return ret;
}

int ObTaskSlotRing::mark_ready(const int64_t slot_idx, const int ret_code)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  if (slot_idx < 0 || slot_idx >= slots_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid slot idx", K(ret), K(slot_idx), K(slots_.count()));
  } else {
    slots_.at(slot_idx).ret_code_ = ret_code;
    slots_.at(slot_idx).ready_ = true;
  }
  return ret;
}

int ObTaskSlotRing::pop_ready_in_order(ObTaskBatchInfo *&batch_info, int &ret_code)
{
  int ret = OB_SUCCESS;
  batch_info = nullptr;
  ObSpinLockGuard guard(lock_);

  if (head_idx_ != next_idx_ && slots_.at(head_idx_).ready_) {
    Slot &slot = slots_.at(head_idx_);
    if (!slot.ready_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("slot not ready", K(ret));
    } else if (OB_UNLIKELY(slot.ret_code_ != OB_SUCCESS)) {
      ret_code = slot.ret_code_;
      // Even if failed, transfer ownership of batch_info for cleanup
      batch_info = slot.batch_info_;
      slot.batch_info_ = nullptr;
    } else {
      // Transfer ownership of batch_info
      batch_info = slot.batch_info_;
      slot.batch_info_ = nullptr;
    }

    if (OB_NOT_NULL(slot.task_)) {
      slot.task_->release_if_managed();
      slot.task_ = nullptr;
    }

    if (OB_SUCC(ret) || OB_NOT_NULL(batch_info)) {
      slot.ready_ = false;
      head_idx_ = (head_idx_ + 1) % slots_.count();
    }
  }
  return ret;
}

void ObTaskSlotRing::disable_all_callbacks()
{
  share::ObEmbeddingTask* tasks_to_cancel[slots_.count()];
  int64_t task_count = 0;

  {
    ObSpinLockGuard guard(lock_);
    // Collect tasks to cancel
    for (int64_t i = 0; i < slots_.count(); ++i) {
      Slot &slot = slots_.at(i);
      if (OB_NOT_NULL(slot.task_)) {
        tasks_to_cancel[task_count++] = slot.task_;
      }
    }
  }

  for (int64_t i = 0; i < task_count; ++i) {
    tasks_to_cancel[i]->disable_callback();
  }
}

void ObTaskSlotRing::clean_all_slots()
{
  ObSpinLockGuard guard(lock_);
  for (int64_t i = 0; i < slots_.count(); ++i) {
    Slot &slot = slots_.at(i);
    if (OB_NOT_NULL(slot.task_)) {
      slot.task_->release_if_managed();
      slot.task_ = nullptr;
    }
    if (OB_NOT_NULL(slot.batch_info_)) {
      slot.batch_info_->~ObTaskBatchInfo();
      ob_free(slot.batch_info_);
      slot.batch_info_ = nullptr;
    }
    slot.ready_ = false;
  }
}

int ObTaskSlotRing::wait_all_tasks_finished()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < slots_.count(); ++i) {
    Slot &slot = slots_.at(i);
    if (OB_NOT_NULL(slot.task_)) {
      if (OB_FAIL(slot.task_->wait_for_completion())) {
        LOG_WARN("wait for task completion failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTaskSlotRing::wait_for_head_completion()
{
  int ret = OB_SUCCESS;
  share::ObEmbeddingTask *task_to_wait = nullptr;

  {
    ObSpinLockGuard guard(lock_);
    if (head_idx_ != next_idx_) {
      Slot &slot = slots_.at(head_idx_);
      if (OB_NOT_NULL(slot.task_)) {
        task_to_wait = slot.task_;
      }
    }
  }

  if (OB_NOT_NULL(task_to_wait)) {
    if (OB_FAIL(task_to_wait->wait_for_completion())) {
      LOG_WARN("wait for head embedding task completion failed", K(ret));
    }
  }
  return ret;
}

void ObTaskSlotRing::set_task(const int64_t slot_idx, share::ObEmbeddingTask *task)
{
  slots_.at(slot_idx).task_ = task;
}

void ObTaskSlotRing::set_batch_info(const int64_t slot_idx, ObTaskBatchInfo *batch_info)
{
  slots_.at(slot_idx).batch_info_ = batch_info;
}

void ObTaskSlotRing::reset()
{
  slots_.reset();
  next_idx_ = 0;
  head_idx_ = 0;
  capacity_ = 0;
}

// -------------------------------- ObEmbeddingIOCallbackHandle --------------------------------
ObEmbeddingIOCallbackHandle *ObEmbeddingIOCallbackHandle::create(ObEmbeddingIOCallback *cb)
{
  void *mem = ob_malloc(sizeof(ObEmbeddingIOCallbackHandle), ObMemAttr(MTL_ID(), "EmbedCbHandle"));
  if (nullptr == mem) {
    return nullptr;
  } else {
    return new (mem) ObEmbeddingIOCallbackHandle(cb);
  }
}

void ObEmbeddingIOCallbackHandle::retain()
{
  ATOMIC_AAF(&ref_cnt_, 1);
}

void ObEmbeddingIOCallbackHandle::disable()
{
  common::ObSpinLockGuard guard(lock_);
  disabled_ = true;
}

int ObEmbeddingIOCallbackHandle::process()
{
  int ret = OB_SUCCESS;
  if (nullptr == cb_) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    common::ObSpinLockGuard guard(lock_);
    if (is_disabled()) {
      // callback disabled; skip
    } else {
      ret = cb_->process();
    }
  }
  return ret;
}

void ObEmbeddingIOCallbackHandle::release()
{
  int64_t v = ATOMIC_AAF(&ref_cnt_, -1);
  if (0 == v) {
    if (nullptr != cb_) {
      cb_->~ObEmbeddingIOCallback();
      ob_free(cb_);
      cb_ = nullptr;
    }
    this->~ObEmbeddingIOCallbackHandle();
    ob_free(this);
  }
}

// -------------------------------- ObEmbeddingTaskMgr --------------------------------
ObEmbeddingTaskMgr::~ObEmbeddingTaskMgr()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    slot_ring_.disable_all_callbacks();
    if (OB_FAIL(slot_ring_.wait_all_tasks_finished())) {
      LOG_WARN("failed to wait for all tasks to finish", K(ret));
    }
    slot_ring_.clean_all_slots();
    is_inited_ = false;
  }
}

int ObEmbeddingTaskMgr::init(const ObString &model_id, const ObCollationType col_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("embedding task mgr init twice", K(ret));
  } else if (OB_FAIL(get_ai_config(model_id))) {
    LOG_WARN("load cfg from sys table failed", K(ret));
  } else {
    ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
    if (OB_ISNULL(service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plugin vector index service is null", K(ret));
    } else if (OB_FAIL(service->get_embedding_task_handler(embedding_handler_))) {
      LOG_WARN("failed to get embedding task handler", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    cs_type_ = col_type;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      model_request_timeout_us_ = tenant_config->model_request_timeout;
      model_max_retries_ = tenant_config->model_max_retries;
    } else {
      SHARE_LOG_RET(WARN, OB_INVALID_CONFIG, "init model request timeout and max retries config with default value");
      model_request_timeout_us_ = 60 * 1000 * 1000; // 60 seconds
      model_max_retries_ = 2;
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t reserve_slots = ring_capacity_;
    if (OB_FAIL(slot_ring_.init(reserve_slots))) {
      LOG_WARN("init slot ring failed", K(ret), K(reserve_slots));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObEmbeddingTaskMgr::submit_batch_info(ObTaskBatchInfo *&batch_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("embedding task mgr not inited", K(ret));
  } else if (OB_ISNULL(batch_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch_info is null", K(ret), K(batch_info));
  } else if (OB_UNLIKELY(batch_info->get_count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch_info is empty", K(ret));
  } else {
    int64_t slot_idx = -1;
    if (OB_FAIL(slot_ring_.reserve_slot(slot_idx))) {
      if (OB_EAGAIN == ret) {
        LOG_DEBUG("slots is full", "batch_count", batch_info->get_count(), K(slot_idx));
      } else {
        LOG_WARN("reserve task slot failed", K(ret));
      }
    } else {
      common::ObArray<ObString> texts;
      const common::ObArray<ObEmbeddingResult*> &results = batch_info->get_results();
      const int64_t count = batch_info->get_count();
      int64_t embedding_count = batch_info->get_need_embedding_count();
      if (OB_FAIL(texts.reserve(embedding_count))) {
        LOG_WARN("reserve texts failed", K(ret), K(embedding_count));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          if (OB_NOT_NULL(results.at(i)) && results.at(i)->need_embedding()) {
            if (OB_FAIL(texts.push_back(results.at(i)->get_text()))) {
              LOG_WARN("push text failed", K(ret), K(i));
            }
          }
        }
      }

      if (OB_SUCC(ret) && embedding_count > 0) {
        // Only create embedding task if there are items to embed
        void *cb_buf = ob_malloc(sizeof(ObEmbeddingIOCallback), ObMemAttr(MTL_ID(), "EmbedCb"));
        if (OB_ISNULL(cb_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc embedding callback failed", K(ret));
        } else {
          ObEmbeddingIOCallback *cb = new (cb_buf) ObEmbeddingIOCallback();
          ObEmbeddingIOCallbackHandle *cb_handle = nullptr;
          share::ObEmbeddingTask *task = nullptr;

          if (OB_ISNULL(cb_handle = ObEmbeddingIOCallbackHandle::create(cb))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("create callback handle failed", K(ret));
          } else {
            void *task_mem = ob_malloc(sizeof(share::ObEmbeddingTask), ObMemAttr(MTL_ID(), "EmbeddingTask"));
            if (OB_ISNULL(task_mem)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate memory for EmbeddingTask", K(ret));
            } else {
              task = new (task_mem) share::ObEmbeddingTask();
              const int64_t vec_dim = results.at(0)->get_vector_dim();
              if (OB_FAIL(task->init(cfg_.model_url_, cfg_.model_name_, cfg_.provider_,
                                   cfg_.user_key_, texts, cs_type_, vec_dim, model_request_timeout_us_, model_max_retries_, cb_handle))) {
                LOG_WARN("failed to initialize EmbeddingTask", K(ret));
              }
            }
          }

          if (OB_SUCC(ret) && OB_NOT_NULL(task)) {
            if (OB_FAIL(cb->init(this, slot_idx, batch_info, task, results.at(0)->get_vector_dim()))) {
              LOG_WARN("init callback failed", K(ret));
            } else {
              task->retain_if_managed();
              if (OB_FAIL(embedding_handler_->push_task(*task))) {
                LOG_WARN("submit task failed", K(ret));
                task->release_if_managed();
              }
            }
          }

          if (OB_SUCC(ret)) {
            slot_ring_.set_task(slot_idx, task);
            slot_ring_.set_batch_info(slot_idx, batch_info); // Take ownership
            batch_info = nullptr;
          } else {
            if (OB_NOT_NULL(task)) {
              task->release_if_managed();
            }
            if (OB_NOT_NULL(cb_handle)) {
              cb_handle->release();
            } else if (OB_NOT_NULL(cb)) {
              cb->~ObEmbeddingIOCallback();
              ob_free(cb);
            }
          }
        }
      } else if (OB_SUCC(ret) && embedding_count == 0) {
        // Just mark this slot as ready immediately without embedding
        if (OB_FAIL(mark_task_ready(slot_idx, OB_SUCCESS))) {
          LOG_WARN("mark task ready failed", K(ret), K(slot_idx));
        } else {
          slot_ring_.set_batch_info(slot_idx, batch_info); // Take ownership
          batch_info = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObEmbeddingTaskMgr::get_ready_batch_info(ObTaskBatchInfo *&batch_info, int &error_ret_code)
{
  int ret = OB_SUCCESS;
  batch_info = nullptr;
  error_ret_code = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("embedding task mgr not inited", K(ret));
  } else if (OB_FAIL(slot_ring_.pop_ready_in_order(batch_info, error_ret_code))) {
    LOG_WARN("pop ready batch failed", K(ret));
  } else if (OB_UNLIKELY(OB_SUCCESS != error_ret_code)) {
    set_failed();
    LOG_WARN("error ret code", K(error_ret_code));
  }
  return ret;
}

//TODO(fanfangyao.ffy): Move this process to vectorindexctx
int ObEmbeddingTaskMgr::get_ai_config(const common::ObString &model_id)
{
  int ret = OB_SUCCESS;
  cfg_.model_url_.reset();
  cfg_.model_name_.reset();
  cfg_.user_key_.reset();
  cfg_.provider_.reset();

  if (model_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model_id is empty", K(ret), K(model_id));
  } else {
    ObAIFuncExprInfo *info = nullptr;
    const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
    omt::ObAiServiceGuard ai_service_guard;
    omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService*);
    if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator_, const_cast<common::ObString&>(model_id), info))) {
      LOG_WARN("failed to get ai func info", K(ret), K(model_id));
    } else if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai func info is null", K(ret));
    } else if (OB_ISNULL(ai_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai service is null", K(ret));
    } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
      LOG_WARN("failed to get ai service guard", K(ret));
    } else if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(model_id, endpoint_info))) {
      LOG_WARN("failed to get endpoint info", K(ret), K(model_id));
    } else if (OB_FAIL(ob_write_string(allocator_, endpoint_info->get_url(), cfg_.model_url_))) {
      LOG_WARN("failed to copy model_url", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, info->model_, cfg_.model_name_))) {
      LOG_WARN("failed to copy model_name", K(ret));
    } else if (OB_FAIL(endpoint_info->get_unencrypted_access_key(allocator_, cfg_.user_key_))) {
      LOG_WARN("failed to copy user_key", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, endpoint_info->get_provider(), cfg_.provider_))) {
      LOG_WARN("failed to copy provider", K(ret));
    }
  }
  return ret;
}

int ObEmbeddingTaskMgr::mark_task_ready(const int64_t slot_idx, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("embedding task mgr not inited", K(ret));
  } else if (OB_FAIL(slot_ring_.mark_ready(slot_idx, ret_code))) {
    LOG_WARN("mark task ready failed", K(ret));
  }
  return ret;
}

int ObEmbeddingTaskMgr::wait_for_completion()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("embedding task mgr not inited", K(ret));
  } else if (OB_FAIL(slot_ring_.wait_for_head_completion())) {
    LOG_WARN("wait for head completion failed", K(ret));
  }
  return ret;
}

void ObEmbeddingTaskMgr::set_failed()
{
  is_failed_ = true;
}