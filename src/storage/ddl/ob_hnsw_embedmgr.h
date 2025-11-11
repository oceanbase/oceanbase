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

#ifndef OB_OCEANBASE_STORAGE_DDL_HNSW_EMBEDMGR_H
#define OB_OCEANBASE_STORAGE_DDL_HNSW_EMBEDMGR_H

#include "lib/container/ob_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "share/vector_index/ob_vector_embedding_handler.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace storage
{

// Forward declarations
class ObHNSWEmbeddingOperator;
class ObEmbeddingTaskMgr;

// ==================== Embedding Types ====================

struct ObEmbeddingConfig
{
public:
  ObEmbeddingConfig() : model_url_(), model_name_(), user_key_(), provider_() {}
  ~ObEmbeddingConfig() = default;

  bool is_valid() const;
  void set_config(const ObString &model_url, const ObString &model_name, const ObString &user_key, const ObString &provider);
  int assign(const ObEmbeddingConfig &other);

public:
  ObString model_url_;
  ObString model_name_;
  ObString user_key_;
  ObString provider_;

  TO_STRING_KV(K_(model_url), K_(model_name), K_(user_key), K_(provider));
};

class ObEmbeddingResult
{
public:
  // Embedding status enum
  enum EmbeddingStatus {
    NEED_EMBEDDING = 0,
    SKIP_EMBEDDING = 1       // Empty/Null text case
  };

  ObEmbeddingResult()
    : extra_values_(), vector_(nullptr), vector_dim_(0), text_(), status_(NEED_EMBEDDING) {}

  ~ObEmbeddingResult() {
    reset();
  }

  common::ObString get_text() const { return text_; }
  void set_text(const common::ObString &text) { text_ = text; }

  // Deep copy extra non-embedding columns
  int set_extra_cols(const common::ObArray<blocksstable::ObStorageDatum> &src_extras, ObArenaAllocator &allocator);
  const common::ObArray<blocksstable::ObStorageDatum>& get_extra_cols() const { return extra_values_; }
  float *get_vector() const { return vector_; }
  int64_t get_vector_dim() const { return vector_dim_; }
  void set_vector(float *vector, const int64_t vector_dim) { vector_ = vector; vector_dim_ = vector_dim; }

  // Status management
  void set_status(EmbeddingStatus status) { status_ = status; }
  bool need_embedding() const { return status_ == NEED_EMBEDDING; }

  void reset();

  TO_STRING_KV(K_(vector_dim), K_(text), K_(status));

private:
  common::ObArray<blocksstable::ObStorageDatum> extra_values_;
  float* vector_;
  int64_t vector_dim_;
  common::ObString text_;
  EmbeddingStatus status_;
};

// ==================== Task Batch Info ====================
class ObTaskBatchInfo
{
public:
  ObTaskBatchInfo()
    : allocator_("TaskBatch", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      results_(),
      batch_size_(0),
      current_count_(0),
      vec_dim_(0),
      need_embedding_count_(0)
  {}

  ~ObTaskBatchInfo() {
    reset();
  }

  int init(const int64_t batch_size, const int64_t vec_dim);

  // Add an item during batching phase (deep copy to allocator)
  int add_item(const common::ObString &text,
               const common::ObArray<blocksstable::ObStorageDatum> &extras,
               const ObEmbeddingResult::EmbeddingStatus status);
  int64_t get_count() const { return current_count_; }
  int64_t get_need_embedding_count() const { return need_embedding_count_; }
  bool is_full() const { return current_count_ >= batch_size_; }
  common::ObArray<ObEmbeddingResult*>& get_results() { return results_; }
  void reset();

  TO_STRING_KV(K_(batch_size), K_(current_count), K_(need_embedding_count), K_(vec_dim), "results_count", results_.count());

private:
  ObArenaAllocator allocator_;
  common::ObArray<ObEmbeddingResult*> results_;
  int64_t batch_size_;
  int64_t current_count_;
  int64_t vec_dim_;
  int64_t need_embedding_count_;

  DISALLOW_COPY_AND_ASSIGN(ObTaskBatchInfo);
};

struct Slot
{
public:
  Slot() : task_(nullptr), batch_info_(nullptr), ready_(false), ret_code_(0) {}

  ~Slot() {
    reset();
  }

  void reset();

public:
  share::ObEmbeddingTask *task_;
  ObTaskBatchInfo *batch_info_;
  bool ready_;
  int ret_code_;
  TO_STRING_KV(K_(task), KP_(batch_info), K_(ready), K_(ret_code));
};

// Ring buffer for managing task slots in order
class ObTaskSlotRing
{
public:
  ObTaskSlotRing() : lock_(), capacity_(1), slots_(), next_idx_(0), head_idx_(0) {}
  ~ObTaskSlotRing();

  int init(const int64_t capacity);
  // Reserve a slot and store batch_info
  int reserve_slot(int64_t &slot_idx);
  // Mark a slot/task as ready
  int mark_ready(const int64_t slot_idx, const int ret_code);
  // Pop ready batch_info
  int pop_ready_in_order(ObTaskBatchInfo *&batch_info, int &ret_code);
  int wait_for_head_completion(const int64_t timeout_us);
  void set_task(const int64_t slot_idx, share::ObEmbeddingTask *task);
  void set_batch_info(const int64_t slot_idx, ObTaskBatchInfo *batch_info);

  // Cleanup operations
  void disable_all_callbacks();
  void clean_all_slots();
  int wait_all_tasks_finished(const int64_t timeout_us);

  TO_STRING_KV(K_(capacity), K_(next_idx), K_(head_idx));

private:
  void reset();

private:
  common::ObSpinLock lock_;
  int64_t capacity_;
  common::ObArray<Slot> slots_;
  int64_t next_idx_;  // Next slot to write
  int64_t head_idx_;  // Next slot to read

  DISALLOW_COPY_AND_ASSIGN(ObTaskSlotRing);
};

class ObEmbeddingIOCallback
{
public:
  ObEmbeddingIOCallback() : is_inited_(false), mgr_(nullptr), task_(nullptr), dim_(0), slot_idx_(-1), batch_info_(nullptr) {}
  ~ObEmbeddingIOCallback();
  int init(ObEmbeddingTaskMgr *mgr, const int64_t slot_idx,
           ObTaskBatchInfo *batch_info, share::ObEmbeddingTask *task, int64_t dim);
  int process();

  TO_STRING_KV(K_(is_inited), K_(mgr), K_(task), K_(dim), K_(slot_idx), KP_(batch_info));

private:
  bool is_inited_;
  ObEmbeddingTaskMgr *mgr_;
  share::ObEmbeddingTask *task_;
  int64_t dim_;
  int64_t slot_idx_;
  ObTaskBatchInfo *batch_info_;
  DISALLOW_COPY_AND_ASSIGN(ObEmbeddingIOCallback);
};

class ObEmbeddingIOCallbackHandle
{
public:
  ObEmbeddingIOCallbackHandle() : ref_cnt_(0), disabled_(false), cb_(nullptr) {}
  explicit ObEmbeddingIOCallbackHandle(ObEmbeddingIOCallback *cb) : ref_cnt_(0), disabled_(false), cb_(cb) {}
  static ObEmbeddingIOCallbackHandle *create(ObEmbeddingIOCallback *cb);
  void retain();
  void disable();
  bool is_disabled() const { return disabled_; }
  int process();
  void release();

  TO_STRING_KV(K_(ref_cnt), K_(disabled), K_(cb));

private:
  int64_t ref_cnt_;
  bool disabled_;
  ObEmbeddingIOCallback *cb_;
  common::ObSpinLock lock_;
};

class ObEmbeddingTaskMgr
{
public:
  ObEmbeddingTaskMgr() : allocator_("EmbedTaskMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
                         embedding_handler_(nullptr), slot_ring_(), ring_capacity_(9),
                         cfg_(), is_inited_(false), is_failed_(false), http_timeout_us_(0), cs_type_(CS_TYPE_INVALID) {}
  ~ObEmbeddingTaskMgr();
  int init(const common::ObString &model_id, const int64_t http_timeout_us, const ObCollationType cs_type);
  int submit_batch_info(ObTaskBatchInfo *&batch_info);
  int get_ready_batch_info(ObTaskBatchInfo *&batch_info, int &error_ret_code);
  int mark_task_ready(const int64_t slot_idx, const int ret_code);
  int wait_for_completion(const int64_t timeout_ms = 0);
  bool get_failed() const { return is_failed_; }

  TO_STRING_KV(K_(ring_capacity), K_(slot_ring), K_(cfg), K_(is_inited));

private:
  int get_ai_config(const common::ObString &model_id);
  void set_failed();

private:
  ObArenaAllocator allocator_;
  share::ObEmbeddingTaskHandler *embedding_handler_;
  ObTaskSlotRing slot_ring_;  // Ring buffer for task slots
  int64_t ring_capacity_;  // TODO(fanfangyao.ffy): 待调参
  ObEmbeddingConfig cfg_;
  bool is_inited_;
  bool is_failed_;
  int64_t http_timeout_us_;
  ObCollationType cs_type_;
  DISALLOW_COPY_AND_ASSIGN(ObEmbeddingTaskMgr);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_OCEANBASE_STORAGE_DDL_HNSW_EMBEDMGR_H
