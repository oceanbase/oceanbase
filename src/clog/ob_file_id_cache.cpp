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

#include "ob_file_id_cache.h"
#include "lib/allocator/ob_mod_define.h"
#include "clog/ob_ilog_storage.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
namespace clog {
// Not thread safe
template <typename T>
class ObLog2FileList : public ObISegArray<T> {
public:
  ObLog2FileList() : is_inited_(false), item_cnt_(0), allocator_(NULL), head_(NULL), tail_(NULL)
  {}

  virtual ~ObLog2FileList()
  {
    is_inited_ = false;
    item_cnt_ = 0;
    allocator_ = NULL;
    head_ = NULL;
    tail_ = NULL;
  }

public:
  virtual int init(ObSmallAllocator* allocator) override
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
    } else if (NULL == allocator) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      is_inited_ = true;
      item_cnt_ = 0;
      allocator_ = allocator;
      head_ = NULL;
      tail_ = NULL;
    }
    return ret;
  }

  virtual void destroy() override
  {
    clear();
  }

  virtual int clear() override
  {
    int ret = OB_SUCCESS;
    T item;
    while (OB_SUCC(ret)) {
      ret = pop_back_(item);
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
    return ret;
  }

  virtual int push_back(const T& item) override
  {
    int ret = OB_SUCCESS;
    char* buf = NULL;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (NULL == (buf = (char*)allocator_->alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      DListNode* node = new (buf) DListNode();
      node->value_ = item;

      if (is_empty_()) {
        head_ = node;
        tail_ = node;
      } else {
        tail_->next_ = node;
        node->prev_ = tail_;
        tail_ = node;
      }
      item_cnt_++;
    }
    return ret;
  }

  virtual int push_front(const T& item) override
  {
    int ret = OB_NOT_SUPPORTED;
    UNUSED(item);
    return ret;
  }

  virtual int pop_front(T& item) override
  {
    return pop_front_(item);
  }

  virtual int top_front(T& item) override
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (is_empty_()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      item = head_->value_;
    }
    return ret;
  }

  virtual int pop_back(T& item) override
  {
    return pop_back_(item);
  }

  virtual int top_back(T& item) override
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (is_empty_()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      item = tail_->value_;
    }
    return ret;
  }

  virtual bool is_empty() const override
  {
    return is_empty_();
  }

  virtual int search_boundary(const T& item, T& prev_item, T& next_item)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (is_empty_()) {
      ret = OB_EMPTY_RESULT;
    } else {
      DListNode* curr = head_;
      while (true) {
        if (curr->value_ <= item) {
          if (curr->next_ == NULL) {
            prev_item = curr->value_;
            ret = OB_ERR_OUT_OF_UPPER_BOUND;
            break;
          } else {
            curr = curr->next_;
          }
        } else if (curr->prev_ == NULL) {
          next_item = curr->value_;
          ret = OB_ERR_OUT_OF_LOWER_BOUND;
          break;
        } else {
          next_item = curr->value_;
          prev_item = curr->prev_->value_;
          break;
        }
      }
    }
    return ret;
  }

  int32_t get_item_cnt() const
  {
    return item_cnt_;
  }

  static uint64_t get_item_size()
  {
    return sizeof(DListNode);
  }

private:
  bool is_empty_() const
  {
    return head_ == NULL && tail_ == NULL;
  }

  int pop_front_(T& item)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (is_empty_()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      item = head_->value_;
      DListNode* tmp = head_;

      if (NULL == head_->next_) {
        head_ = NULL;
        tail_ = NULL;
      } else {
        head_->next_->prev_ = NULL;
        head_ = head_->next_;
      }

      allocator_->free(tmp);
      tmp = NULL;
      item_cnt_--;
    }
    return ret;
  }

  int pop_back_(T& item)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (is_empty_()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      item = tail_->value_;
      DListNode* tmp = tail_;

      if (NULL == tail_->prev_) {
        head_ = NULL;
        tail_ = NULL;
      } else {
        tail_->prev_->next_ = NULL;
        tail_ = tail_->prev_;
      }

      allocator_->free(tmp);
      tmp = NULL;
      item_cnt_--;
    }
    return ret;
  }

private:
  friend class ObFileIdList;
  class DListNode {
  public:
    DListNode() : value_(), next_(NULL), prev_(NULL)
    {}

  public:
    T value_;
    DListNode* next_;
    DListNode* prev_;
  };

private:
  bool is_inited_;
  int32_t item_cnt_;
  ObSmallAllocator* allocator_;
  DListNode* head_;
  DListNode* tail_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLog2FileList);
};

// ------------ ObFileIdList ---------------
int ObFileIdList::BackFillFunctor::init(const file_id_t file_id, const offset_t start_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || start_offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid argument", K(ret), K(file_id), K(start_offset));
  } else {
    file_id_ = file_id;
    start_offset_ = start_offset;
    err_ = OB_SUCCESS;
  }
  return ret;
}

void ObFileIdList::BackFillFunctor::operator()(Log2File& item)
{
  if (OB_LIKELY(file_id_ == item.get_file_id())) {
    err_ = item.backfill(start_offset_);
  } else {
    err_ = common::OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "log2file item not match", K(err_), K(file_id_), K(item));
  }
}

bool ObFileIdList::PurgeChecker::should_purge(const Log2File& log_2_file) const
{
  bool bret = false;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!purge_strategy_.is_valid()) || OB_UNLIKELY(!log_2_file.is_valid())) {
    tmp_ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "PurgeChecker error", K(tmp_ret), K(purge_strategy_), K(log_2_file));
  } else {
    bret = purge_strategy_.can_purge(log_2_file, partition_key_);
    CSR_LOG(TRACE, "should_purge", K(bret), K(log_2_file), K(partition_key_));
  }
  return bret;
}

bool ObFileIdList::ClearBrokenFunctor::should_purge(const Log2File& log_2_file) const
{
  bool bret = false;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!log_2_file.is_valid())) {
    tmp_ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "ClearBrokenFunctor error", K(tmp_ret));
  } else {
    bret = (log_2_file.get_file_id() == broken_file_id_);
  }
  return bret;
}

ObFileIdList::ObFileIdList()
    : is_inited_(false),
      use_seg_array_(false),
      min_continuous_log_id_(0),
      file_id_cache_(NULL),
      base_pos_(),
      container_ptr_(NULL),
      seg_array_allocator_(NULL),
      seg_item_allocator_(NULL),
      log2file_list_allocator_(NULL),
      list_item_allocator_(NULL)
{
  // do nothing
}

int ObFileIdList::init(ObSmallAllocator* seg_array_allocator, ObSmallAllocator* seg_item_allocator,
    ObSmallAllocator* log2file_list_allocator, ObSmallAllocator* list_item_allocator, ObFileIdCache* file_id_cache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(file_id_cache)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "ObFileIdList init error", K(ret), KP(file_id_cache));
  } else {
    use_seg_array_ = false;
    min_continuous_log_id_ = 0;
    file_id_cache_ = file_id_cache;
    container_ptr_ = NULL;
    seg_array_allocator_ = seg_array_allocator;
    seg_item_allocator_ = seg_item_allocator;
    log2file_list_allocator_ = log2file_list_allocator;
    list_item_allocator_ = list_item_allocator;

    if (OB_FAIL(prepare_container_())) {
      CSR_LOG(WARN, "prepare_container_ failed", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObFileIdList::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    min_continuous_log_id_ = 0;
    file_id_cache_ = NULL;
    base_pos_.reset();

    if (NULL != container_ptr_) {
      container_ptr_->destroy();

      if (use_seg_array_) {
        seg_array_allocator_->free(container_ptr_);
      } else {
        log2file_list_allocator_->free(container_ptr_);
      }
    }
    use_seg_array_ = false;
    container_ptr_ = NULL;
    seg_array_allocator_ = NULL;
    seg_item_allocator_ = NULL;
    log2file_list_allocator_ = NULL;
    list_item_allocator_ = NULL;
  }
}

int ObFileIdList::get_max_continuous_log_id(const common::ObPartitionKey& pkey, uint64_t& max_continuous_log_id)
{
  int ret = OB_SUCCESS;
  Log2File last_item;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (container_ptr_->is_empty()) {
    max_continuous_log_id = 0;
    // do nothing
  } else if (OB_FAIL(container_ptr_->top_back(last_item))) {
    CSR_LOG(WARN, "failed to top_back", KR(ret), K(pkey));
  } else {
    max_continuous_log_id = last_item.get_max_log_id();
  }
  return ret;
}

int ObFileIdList::get_front_log2file_max_timestamp(int64_t& front_log2file_max_timestamp) const
{
  int ret = OB_SUCCESS;
  Log2File log_2_file;
  if (OB_FAIL(container_ptr_->top_front(log_2_file)) && OB_ENTRY_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "container_ptr_ is empty", K(ret));
  } else if (OB_UNLIKELY(!log_2_file.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "container_ptr_ unexpected error", K(ret));
  } else {
    front_log2file_max_timestamp = log_2_file.get_max_log_timestamp();
  }
  return ret;
}

int ObFileIdList::locate(const ObPartitionKey& pkey, const int64_t target_value, const bool locate_by_log_id,
    Log2File& prev_item, Log2File& next_item)
{
  int ret = OB_SUCCESS;

  uint64_t target_log_id = (locate_by_log_id ? static_cast<uint64_t>(target_value) : OB_INVALID_ID);
  int64_t target_timestamp = (locate_by_log_id ? OB_INVALID_TIMESTAMP : target_value);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == target_log_id && OB_INVALID_TIMESTAMP == target_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    Log2File target_item;

    if (locate_by_log_id) {
      target_item.set_min_log_id(target_log_id);
    } else {
      target_item.set_min_log_timestamp(target_timestamp);
    }

    ret = container_ptr_->search_boundary(target_item, prev_item, next_item);

    if (OB_SUCCESS == ret || OB_ERR_OUT_OF_LOWER_BOUND == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      CSR_LOG(TRACE,
          "[FILE_ID_CACHE] ObFileIdList locate finish",
          K(ret),
          K(pkey),
          K(target_value),
          K(locate_by_log_id),
          K(prev_item),
          K(next_item),
          K(target_item));
    } else if (OB_EMPTY_RESULT == ret) {
      if (OB_INVALID_FILE_ID == base_pos_.file_id_) {
        ret = OB_ERR_UNEXPECTED;
        CSR_LOG(WARN,
            "ObFileIdList is empty when no purge before, locate fail",
            KR(ret),
            K(pkey),
            K(target_value),
            K(locate_by_log_id),
            K(target_item));
      }
    } else {
      CSR_LOG(WARN,
          "ObFileIdList locate from SegArray fail",
          K(ret),
          K(pkey),
          K(target_item),
          K(target_value),
          K(locate_by_log_id),
          K(prev_item),
          K(next_item));
    }
  }

  return ret;
}

// max_log_id,max_log_timestamp,start_offset may be invalid
int ObFileIdList::append(const ObPartitionKey& pkey, const file_id_t file_id, const offset_t start_offset,
    const uint64_t min_log_id, const uint64_t max_log_id, const int64_t min_log_timestamp,
    const int64_t max_log_timestamp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(OB_INVALID_ID == min_log_id) ||
             OB_UNLIKELY(OB_INVALID_TIMESTAMP == min_log_timestamp)) {
    CSR_LOG(WARN,
        "invalid argument",
        K(file_id),
        K(min_log_id),
        K(min_log_timestamp),
        K(max_log_id),
        K(max_log_timestamp),
        K(start_offset));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(prepare_container_())) {
    CSR_LOG(ERROR, "prepare_container_ failed", K(ret), K(pkey));
  } else {
    Log2File new_item;
    new_item.reset(file_id, start_offset, min_log_id, max_log_id, min_log_timestamp, max_log_timestamp);

    // clear preceding items when necessary
    Log2File last_item;
    if (container_ptr_->is_empty()) {
      // do nothing
    } else if (OB_FAIL(container_ptr_->top_back(last_item))) {
      CSR_LOG(WARN, "failed to top_back", KR(ret), K(pkey), K(new_item));
    } else if (OB_UNLIKELY(OB_INVALID_ID == last_item.get_max_log_id() || 0 == last_item.get_max_log_id())) {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(WARN, "invalid last_item", KR(ret), K(pkey), K(last_item));
    } else if (last_item.get_max_log_id() < new_item.get_min_log_id()) {
      // do nothing
    } else if (OB_FAIL(purge_preceding_items_(pkey, last_item))) {
      CSR_LOG(WARN, "failed to purge_preceding_items_", KR(ret), K(pkey), K(last_item), K(new_item));
    } else {
      CSR_LOG(INFO, "success to purge_preceding_items_", KR(ret), K(pkey), K(last_item), K(new_item));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(container_ptr_->push_back(new_item))) {
        CSR_LOG(WARN, "container push_back new item error", K(ret), K(pkey), K(new_item), K(last_item));
      } else {
        if (0 == min_continuous_log_id_) {
          // append first item
          min_continuous_log_id_ = new_item.get_min_log_id();
        } else if (OB_UNLIKELY(!last_item.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          CSR_LOG(WARN, "invalid last_item", KR(ret), K(pkey), K(new_item), K(last_item));
        } else if (last_item.get_max_log_id() + 1 != new_item.get_min_log_id()) {
          min_continuous_log_id_ = new_item.get_min_log_id();
        } else {
        }
      }
    }
  }
  return ret;
}

int ObFileIdList::purge_preceding_items(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  Log2File last_item;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "not inited", KR(ret), K(pkey));
  } else if (container_ptr_->is_empty()) {
    // do nothing
  } else if (OB_FAIL(container_ptr_->top_back(last_item))) {
    CSR_LOG(ERROR, "failed to get last_item", KR(ret), K(pkey));
  } else if (OB_FAIL(purge_preceding_items_(pkey, last_item))) {
    CSR_LOG(WARN, "failed to purge_preceding_items_", KR(ret), K(pkey));
  } else { /*do nothing*/
  }
  return ret;
}

int ObFileIdList::purge_preceding_items_(const ObPartitionKey& pkey, const Log2File& last_item)
{
  int ret = OB_SUCCESS;
  // clear all previous items
  ObLogCursorExt log_cursor;
  if (OB_FAIL(file_id_cache_->get_cursor_from_file(pkey, last_item.get_max_log_id(), last_item, log_cursor))) {
    CSR_LOG(WARN, "failed to get_cursor_from_file", KR(ret), K(pkey), K(last_item));
  } else if (OB_FAIL(container_ptr_->clear())) {
    CSR_LOG(WARN, "failed to clear container", KR(ret), K(pkey), K(log_cursor), K(last_item));
  } else {
    min_continuous_log_id_ = 0;
    base_pos_.file_id_ = log_cursor.get_file_id();
    base_pos_.base_offset_ = log_cursor.get_offset();
    CSR_LOG(INFO, "success to purge_preceding_items_", KR(ret), K(pkey), K(last_item), K(base_pos_), K(log_cursor));
  }
  return ret;
}

int ObFileIdList::undo_append(const file_id_t broken_file_id, bool& empty)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == broken_file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "undo append error, invalid broken_file_id", K(ret));
  } else {
    ClearBrokenFunctor clear_broken_functor(broken_file_id);
    const bool is_front_end = false;
    if (OB_FAIL(purge_(is_front_end, clear_broken_functor, empty))) {
      CSR_LOG(
          WARN, "clear broken error", K(ret), K(is_front_end), K(broken_file_id), K(clear_broken_functor), K(empty));
    } else {
      CSR_LOG(TRACE, "clear broken success");
    }
  }
  return ret;
}

int ObFileIdList::backfill(const uint64_t min_log_id, const file_id_t file_id, const offset_t start_offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == min_log_id) || OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) ||
             OB_UNLIKELY(start_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "backfill list error", K(ret), K(min_log_id), K(file_id), K(start_offset));
  } else if (use_seg_array_) {
    ObSegArray<Log2File, ObFileIdList::SEG_STEP, ObFileIdList::SEG_COUNT>* seg_array_ptr =
        dynamic_cast<ObSegArray<Log2File, ObFileIdList::SEG_STEP, ObFileIdList::SEG_COUNT>*>(container_ptr_);

    if (seg_array_ptr) {
      Log2File target_item;
      BackFillFunctor bff;

      target_item.set_min_log_id(min_log_id);

      if (OB_FAIL(bff.init(file_id, start_offset))) {
        CSR_LOG(WARN, "BackFillFunctor init error", K(ret), K(start_offset));
      } else if (OB_FAIL(seg_array_ptr->search_lower_bound_and_operate(target_item, bff))) {
        CSR_LOG(WARN, "seg_arr_ search_lower_bound_and_operate error", K(ret), K(target_item), K(bff));
      } else if (OB_FAIL(bff.get_err())) {
        CSR_LOG(WARN, "BackFillFunctor exec error", K(ret), K(bff), K(start_offset), K(file_id), K(min_log_id));
      } else {
        // do nothing
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(ERROR, "when use_seg_array_, dynamic_cast failed", K(ret));
    }
  } else {
    ObLog2FileList<Log2File>* list_ptr = dynamic_cast<ObLog2FileList<Log2File>*>(container_ptr_);
    if (list_ptr) {
      ObLog2FileList<Log2File>::DListNode* curr = list_ptr->head_;

      while (NULL != curr) {
        if (curr->value_.get_min_log_id() == min_log_id) {
          if (curr->value_.get_file_id() != file_id) {
            ret = OB_ERR_UNEXPECTED;
            CSR_LOG(ERROR,
                "Log2File file_id not match, unexpected",
                K(file_id),
                "Log2File_file_id",
                curr->value_.get_file_id(),
                K(min_log_id));
          } else {
            curr->value_.backfill(start_offset);
          }
          break;
        } else {
          curr = curr->next_;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(ERROR, "when no use_seg_array_, dynamic_cast failed", K(ret));
    }
  }
  return ret;
}

int ObFileIdList::purge(const common::ObPartitionKey& pkey, ObIFileIdCachePurgeStrategy& purge_strategy, bool& empty)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObFileIdList is not inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(!purge_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "purge error", K(ret), K(pkey), K(purge_strategy));
  } else {
    PurgeChecker purge_checker(pkey, purge_strategy);
    const bool is_front_end = true;
    if (OB_FAIL(purge_(is_front_end, purge_checker, empty))) {
      CSR_LOG(WARN, "purge error", K(ret), K(is_front_end), K(purge_checker), K(empty));
    } else {
      CSR_LOG(TRACE, "purge success", K(is_front_end), K(purge_checker), K(empty));
    }
  }
  return ret;
}

// The caller guarantees that the function will not be executed concurrently
int ObFileIdList::purge_(const bool is_front_end, IPurgeChecker& checker, bool& empty)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!checker.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "purge file error", K(ret), K(checker));
  } else {
    bool purge_finish = false;
    while (OB_SUCC(ret) && !purge_finish) {
      Log2File top_item;
      if (is_front_end) {
        ret = container_ptr_->top_front(top_item);
      } else {
        ret = container_ptr_->top_back(top_item);
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(!top_item.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        CSR_LOG(ERROR, "get invalid top item from seg_array", K(ret), K(top_item), K(is_front_end));
      }
      if (OB_SUCC(ret)) {
        if (checker.should_purge(top_item)) {
          if (is_front_end) {
            ret = container_ptr_->pop_front(top_item);
          } else {
            ret = container_ptr_->pop_back(top_item);
          }
          if (OB_FAIL(ret)) {
            CSR_LOG(ERROR, "seg_array pop error", K(ret), K(checker), K(is_front_end));
          } else {
            empty = container_ptr_->is_empty();
            // update min_continuous_log_id_
            if (empty) {
              min_continuous_log_id_ = 0;
            } else if (min_continuous_log_id_ == top_item.get_min_log_id()) {
              min_continuous_log_id_ = top_item.get_max_log_id() + 1;
            } else { /*do nothing*/
            }
            CSR_LOG(TRACE, "purge_file_id success", K(checker), K(is_front_end), K(top_item), K(empty));
          }
        } else {
          purge_finish = true;
          CSR_LOG(TRACE, "do not need purge", K(is_front_end), K(checker));
        }
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        CSR_LOG(TRACE, "empty seg_arr", K(ret));
        ret = OB_SUCCESS;
        purge_finish = true;
      }
    }
  }
  return ret;
}

int ObFileIdList::get_log_base_pos(ObLogBasePos& base_pos) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(WARN, "not inited", KR(ret));
  } else {
    base_pos = base_pos_;
  }
  return ret;
}

int ObFileIdList::prepare_container_()
{
  int ret = OB_SUCCESS;
  if (NULL == container_ptr_) {
    use_seg_array_ = false;
    char* buf = NULL;
    if (NULL == (buf = (char*)log2file_list_allocator_->alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CSR_LOG(ERROR, "log2file_list_allocator_ alloc failed", K(ret));
    } else {
      container_ptr_ = new (buf) ObLog2FileList<Log2File>();

      if (OB_FAIL(container_ptr_->init(list_item_allocator_))) {
        CSR_LOG(ERROR, "ObLog2FileList init failed", K(ret));
        log2file_list_allocator_->free(container_ptr_);
        container_ptr_ = NULL;
      }
    }
  } else if (!use_seg_array_ &&
             ((ObLog2FileList<Log2File>*)container_ptr_)->get_item_cnt() >= NEED_USE_SEG_ARRAY_THRESHOLD) {
    char* buf = NULL;
    if (NULL == (buf = (char*)seg_array_allocator_->alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CSR_LOG(ERROR, "seg_array_allocator_ alloc failed", K(ret));
    } else {
      common::ObISegArray<Log2File>* tmp_container_ptr =
          new (buf) ObSegArray<Log2File, ObFileIdList::SEG_STEP, ObFileIdList::SEG_COUNT>();
      if (OB_FAIL(tmp_container_ptr->init(seg_item_allocator_))) {
        CSR_LOG(ERROR, "ObSegArray init failed", K(ret));
        seg_array_allocator_->free(tmp_container_ptr);
        tmp_container_ptr = NULL;
      } else if (OB_FAIL(move_item_to_seg_array_(tmp_container_ptr))) {
        CSR_LOG(WARN, "move_item_to_seg_array_ failed", K(ret));
        tmp_container_ptr->destroy();
        seg_array_allocator_->free(tmp_container_ptr);
        tmp_container_ptr = NULL;
      } else {
        container_ptr_->destroy();
        log2file_list_allocator_->free(container_ptr_);
        container_ptr_ = tmp_container_ptr;
        use_seg_array_ = true;
        CSR_LOG(INFO, "use seg array in prepare_container_ function");
      }
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObFileIdList::move_item_to_seg_array_(ObISegArray<Log2File>* tmp_container_ptr) const
{
  int ret = OB_SUCCESS;
  ObLog2FileList<Log2File>* ptr = dynamic_cast<ObLog2FileList<Log2File>*>(container_ptr_);

  if (ptr) {
    ObLog2FileList<Log2File>::DListNode* curr = ptr->head_;

    while (NULL != curr && OB_SUCC(ret)) {
      if (OB_FAIL(tmp_container_ptr->push_back(curr->value_))) {
        CSR_LOG(ERROR, "move_item_to_seg_array_ push_back failed", K(ret));
      } else {
        curr = curr->next_;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "dynamic_cast failed", K(ret));
  }

  return ret;
}

ObFileIdCachePurgeByFileId::ObFileIdCachePurgeByFileId(const file_id_t min_file_id, ObFileIdCache& file_id_cache)
    : min_file_id_(min_file_id), file_id_cache_(file_id_cache)
{}

ObFileIdCachePurgeByFileId::~ObFileIdCachePurgeByFileId()
{
  min_file_id_ = OB_INVALID_FILE_ID;
}

bool ObFileIdCachePurgeByFileId::can_purge(const Log2File& log_2_file, const common::ObPartitionKey& unused_pkey) const
{
  UNUSED(unused_pkey);
  return log_2_file.get_file_id() <= min_file_id_;
}

bool ObFileIdCachePurgeByFileId::is_valid() const
{
  return OB_INVALID_FILE_ID != min_file_id_;
}

bool ObFileIdCachePurgeByFileId::need_wait(bool& need_print_error) const
{
  bool bool_ret = false;
  need_print_error = false;
  const file_id_t curr_max_file_id = file_id_cache_.get_curr_max_file_id();
  // This measn that ilog can be purged, but ilog index don't in file_id_cahce
  if (OB_INVALID_FILE_ID == curr_max_file_id || curr_max_file_id < min_file_id_) {
    bool_ret = true;
    need_print_error = true;
  }
  return bool_ret;
}

ObFileIdCachePurgeByTimestamp::ObFileIdCachePurgeByTimestamp(const int64_t max_decided_trans_version,
    const int64_t min_timestamp, ObIlogAccessor& ilog_accessor, ObFileIdCache& file_id_cache)
    : max_decided_trans_version_(max_decided_trans_version),
      min_timestamp_(min_timestamp),
      ilog_accessor_(ilog_accessor),
      file_id_cache_(file_id_cache)
{}

ObFileIdCachePurgeByTimestamp::~ObFileIdCachePurgeByTimestamp()
{
  max_decided_trans_version_ = OB_INVALID_TIMESTAMP;
  min_timestamp_ = OB_INVALID_TIMESTAMP;
}

bool ObFileIdCachePurgeByTimestamp::can_purge(const Log2File& log_2_file, const common::ObPartitionKey& pkey) const
{
  bool can_purge = false;
  int ret = OB_SUCCESS;
  can_purge = log_2_file.get_max_log_timestamp() <= min_timestamp_;
  if (can_purge) {
    if (OB_FAIL(ilog_accessor_.check_partition_ilog_can_be_purged(pkey,
            max_decided_trans_version_,
            log_2_file.get_max_log_id(),
            log_2_file.get_max_log_timestamp(),
            can_purge))) {
      CSR_LOG(ERROR,
          "partition_partition_ilog_can_be_purged failed",
          K(pkey),
          K(max_decided_trans_version_),
          K(log_2_file));
    }
  }
  CSR_LOG(TRACE, "ObFileIdCachePurgeByTimestamp can_purge", K(can_purge), K(log_2_file), K(min_timestamp_));
  return can_purge;
}

bool ObFileIdCachePurgeByTimestamp::is_valid() const
{
  return OB_INVALID_TIMESTAMP != max_decided_trans_version_ && OB_INVALID_TIMESTAMP != min_timestamp_;
}

bool ObFileIdCachePurgeByTimestamp::need_wait(bool& need_print_error) const
{
  bool bool_ret = false;
  need_print_error = false;
  const int64_t next_can_purge_log2file_timestamp = file_id_cache_.get_next_can_purge_log2file_timestamp();
  if (next_can_purge_log2file_timestamp > min_timestamp_) {
    bool_ret = true;
    CSR_LOG(TRACE, "ObFileIdCachePurgeByTimestamp need_wait", K(next_can_purge_log2file_timestamp), K(min_timestamp_));
  }
  return bool_ret;
}

ObFileIdCache::ObFileIdCache()
    : rwlock_(),
      is_inited_(false),
      need_filter_partition_(false),
      curr_max_file_id_(OB_INVALID_FILE_ID),
      next_can_purge_log2file_timestamp_(OB_INVALID_TIMESTAMP),
      server_seq_(-1),
      ilog_accessor_(NULL),
      addr_(),
      list_allocator_(),
      seg_array_allocator_(),
      seg_item_allocator_(),
      log2file_list_allocator_(),
      list_item_allocator_(),
      map_(),
      filter_map_()
{}

ObFileIdCache::~ObFileIdCache()
{
  destroy();
}

int ObFileIdCache::init(const int64_t server_seq, const common::ObAddr& addr, ObIlogAccessor* ilog_accessor)
{
  int ret = OB_SUCCESS;
  const int64_t LIST_ALLOCATOR_OBJ_SIZE = sizeof(ObFileIdList);
  constexpr const char* LIST_ALLOCATOR_LABEL = ObModIds::OB_CSR_FILE_ID_CACHE_FILE_LIST;
  const uint64_t LIST_ALLOCATOR_TENANT_ID = OB_SERVER_TENANT_ID;
  const int64_t LIST_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;  // 2M - 1K
  const int64_t LIST_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK = 128;
  const int64_t LIST_ALLOCATOR_LIMIT_NUM = 2L * 1024L * 1024L * 1024L;  // 2G

  const int64_t SEG_ARRAY_ALLOCATOR_OBJ_SIZE =
      sizeof(ObSegArray<Log2File, ObFileIdList::SEG_STEP, ObFileIdList::SEG_COUNT>);
  constexpr const char* SEG_ARRAY_ALLOCATOR_LABEL = "CsrFilIdCacSeAr";
  const uint64_t SEG_ARRAY_ALLOCATOR_TENANT_ID = OB_SERVER_TENANT_ID;
  const int64_t SEG_ARRAY_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;  // 2M - 1K
  const int64_t SEG_ARRAY_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK = 128;
  const int64_t SEG_ARRAY_ALLOCATOR_LIMIT_NUM = 4L * 1024L * 1024L * 1024L;  // 4G

  const int64_t SEG_ITEM_ALLOCATOR_OBJ_SIZE =
      ObSegArray<Log2File, ObFileIdList::SEG_STEP, ObFileIdList::SEG_COUNT>::get_seg_size();
  constexpr const char* SEG_ITEM_ALLOCATOR_LABEL = "CsrFilIdCacSeIt";
  const uint64_t SEG_ITEM_ALLOCATOR_TENANT_ID = OB_SERVER_TENANT_ID;
  const int64_t SEG_ITEM_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;  // 2M - 1K
  const int64_t SEG_ITEM_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK = 128;
  const int64_t SEG_ITEM_ALLOCATOR_LIMIT_NUM = 4L * 1024L * 1024L * 1024L;  // 4G

  const int64_t LOG2FILE_LIST_ALLOCATOR_OBJ_SIZE = sizeof(ObLog2FileList<Log2File>);
  constexpr const char* LOG2FILE_LIST_ALLOCATOR_LABEL = "CsrFilIdCacLfLi";
  const uint64_t LOG2FILE_LIST_ALLOCATOR_TENANT_ID = OB_SERVER_TENANT_ID;
  const int64_t LOG2FILE_LIST_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;  // 2M - 1K
  const int64_t LOG2FILE_LIST_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK = 128;
  const int64_t LOG2FILE_LIST_ALLOCATOR_LIMIT_NUM = 4L * 1024L * 1024L * 1024L;  // 4G

  const int64_t LIST_ITEM_ALLOCATOR_OBJ_SIZE = ObLog2FileList<Log2File>::get_item_size();
  constexpr const char* LIST_ITEM_ALLOCATOR_LABEL = "CsrFilIdCacLiIt";
  const uint64_t LIST_ITEM_ALLOCATOR_TENANT_ID = OB_SERVER_TENANT_ID;
  const int64_t LIST_ITEM_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;  // 2M - 1K
  const int64_t LIST_ITEM_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK = 128;
  const int64_t LIST_ITEM_ALLOCATOR_LIMIT_NUM = 4L * 1024L * 1024L * 1024L;  // 4G

  constexpr const char* MAP_LABEL = ObModIds::OB_CSR_FILE_ID_CACHE_PKEY_MAP;
  const uint64_t MAP_TENANT_ID = OB_SERVER_TENANT_ID;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(server_seq < 0 || !addr.is_valid()) || OB_ISNULL(ilog_accessor)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "invalid arguments", K(server_seq), K(addr), K(ret));
  } else if (OB_FAIL(list_allocator_.init(LIST_ALLOCATOR_OBJ_SIZE,
                 LIST_ALLOCATOR_LABEL,
                 LIST_ALLOCATOR_TENANT_ID,
                 LIST_BLOCK_SIZE,
                 LIST_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK,
                 LIST_ALLOCATOR_LIMIT_NUM))) {
    CSR_LOG(WARN, "list_allocator_ init error", K(ret));
  } else if (OB_FAIL(seg_array_allocator_.init(SEG_ARRAY_ALLOCATOR_OBJ_SIZE,
                 SEG_ARRAY_ALLOCATOR_LABEL,
                 SEG_ARRAY_ALLOCATOR_TENANT_ID,
                 SEG_ARRAY_BLOCK_SIZE,
                 SEG_ARRAY_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK,
                 SEG_ARRAY_ALLOCATOR_LIMIT_NUM))) {
    CSR_LOG(WARN, "seg_array_allocator_ init error", K(ret));
  } else if (OB_FAIL(seg_item_allocator_.init(SEG_ITEM_ALLOCATOR_OBJ_SIZE,
                 SEG_ITEM_ALLOCATOR_LABEL,
                 SEG_ITEM_ALLOCATOR_TENANT_ID,
                 SEG_ITEM_BLOCK_SIZE,
                 SEG_ITEM_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK,
                 SEG_ITEM_ALLOCATOR_LIMIT_NUM))) {
    CSR_LOG(WARN, "seg_item_allocator_ init error", K(ret));
  } else if (OB_FAIL(log2file_list_allocator_.init(LOG2FILE_LIST_ALLOCATOR_OBJ_SIZE,
                 LOG2FILE_LIST_ALLOCATOR_LABEL,
                 LOG2FILE_LIST_ALLOCATOR_TENANT_ID,
                 LOG2FILE_LIST_BLOCK_SIZE,
                 LOG2FILE_LIST_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK,
                 LOG2FILE_LIST_ALLOCATOR_LIMIT_NUM))) {
    CSR_LOG(WARN, "log2file_list_allocator_ init error", K(ret));
  } else if (OB_FAIL(list_item_allocator_.init(LIST_ITEM_ALLOCATOR_OBJ_SIZE,
                 LIST_ITEM_ALLOCATOR_LABEL,
                 LIST_ITEM_ALLOCATOR_TENANT_ID,
                 LIST_ITEM_BLOCK_SIZE,
                 LIST_ITEM_ALLOCATOR_MIN_OBJ_COUNT_ON_BLOCK,
                 LIST_ITEM_ALLOCATOR_LIMIT_NUM))) {
    CSR_LOG(WARN, "list_item_allocator_ init error", K(ret));
  } else if (OB_FAIL(map_.init(MAP_LABEL, MAP_TENANT_ID))) {
    CSR_LOG(WARN, "map_ init error", K(ret));
  } else if (OB_FAIL(filter_map_.init("FAST_PG_FILTER", MAP_TENANT_ID))) {
    CSR_LOG(WARN, "fliter_map_ init error", K(ret));
  } else {
    curr_max_file_id_ = OB_INVALID_FILE_ID;
    next_can_purge_log2file_timestamp_ = OB_INVALID_TIMESTAMP;
    server_seq_ = server_seq;
    ilog_accessor_ = ilog_accessor;
    addr_ = addr;
    is_inited_ = true;
    CSR_LOG(INFO, "ObFileIdCache init success", K(server_seq), K(addr));
  }
  return ret;
}

void ObFileIdCache::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    curr_max_file_id_ = OB_INVALID_FILE_ID;
    next_can_purge_log2file_timestamp_ = OB_INVALID_TIMESTAMP;
    DestroyListFunctor destroy_list_functor(list_allocator_);
    int tmp_ret = OB_SUCCESS;
    ;
    if (OB_SUCCESS != (tmp_ret = map_.for_each(destroy_list_functor))) {
      CSR_LOG(WARN, "map_ destory list", K(tmp_ret));
    }
    map_.destroy();
    filter_map_.destroy();
    (void)list_allocator_.destroy();
    (void)seg_array_allocator_.destroy();
    (void)seg_item_allocator_.destroy();
    (void)log2file_list_allocator_.destroy();
    (void)list_item_allocator_.destroy();
  }
}

// Located based on the log id or timestamp, return the upper and lower bound Log2File element
//
// Only compare the min_log_id or min_log_timestamp of Log2File, ensure that the lower bound
// is less than or equal to the target element.
//
// prev_item, lower bound, which is the largest item whose min_log is less than or equal
// to the target element
// next_item, upper_bound, which is the smallest item whose min_log is greater than or equal
// to the target element
//
// Return value:
// 1. OB_SUCCESS                          prev_item and next_item are both valid
// 2. OB_ERR_OUT_OF_UPPER_BOUND           prev_item valid, next_item invalid
// 3. OB_ERR_OUT_OF_LOWER_BOUND           prev_item invalid, next_item valid
// 4. OB_PARTITION_NOT_EXIST              partiiton not exist, prev_item and next_item are both invalid
// 5. OB_NEED_RETRY                       need retrym prev_item and next_item are both invalid
// 6. Others
int ObFileIdCache::locate(const ObPartitionKey& pkey, const int64_t target_value, const bool locate_by_log_id,
    Log2File& prev_item, Log2File& next_item)
{
  int ret = OB_SUCCESS;
  ObFileIdList* list = NULL;
  RLockGuard rguard(rwlock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(map_.get(pkey, list))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_PARTITION_NOT_EXIST;
    } else {
      CSR_LOG(WARN, "map_ get error", K(ret), K(pkey), K(target_value));
    }
  } else if (OB_ISNULL(list)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "get null list", K(ret), K(pkey), K(target_value));
  } else {
    ret = list->locate(pkey, target_value, locate_by_log_id, prev_item, next_item);
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_PARTITION_NOT_EXIST;
    }
  }
  return ret;
}

int ObFileIdCache::append(const file_id_t file_id, IndexInfoBlockMap& index_info_block_map)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObFileIdCache is not inited", K(ret));
  } else if (OB_INVALID_FILE_ID == file_id) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(file_id));
  } else {
    static int32_t RETRY_SLEEP_TS_US = 1000;  // 1ms;
    while (OB_SUCCESS != (ret = append_(file_id, index_info_block_map))) {
      usleep(RETRY_SLEEP_TS_US);
    }
    ATOMIC_STORE(&curr_max_file_id_, file_id);
  }
  return ret;
}

int ObFileIdCache::backfill(
    const ObPartitionKey& pkey, const uint64_t min_log_id, const file_id_t file_id, const offset_t start_offset)
{
  int ret = OB_SUCCESS;
  ObFileIdList* list = NULL;
  RLockGuard rguard(rwlock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(map_.get(pkey, list))) {
    CSR_LOG(WARN, "get list error", K(ret), K(pkey), K(min_log_id), K(file_id), K(start_offset));
  } else if (OB_ISNULL(list)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "get null list", K(ret), K(pkey), K(min_log_id), K(file_id), K(start_offset));
  } else if (OB_FAIL(list->backfill(min_log_id, file_id, start_offset))) {
    CSR_LOG(WARN, "list backfill error", K(ret), K(pkey), K(min_log_id), K(file_id), K(start_offset));
  } else {
    CSR_LOG(TRACE, "[FILE_ID_CACHE] backfill success", K(pkey), K(min_log_id), K(file_id), K(start_offset));
  }
  return ret;
}

int ObFileIdCache::purge(ObIFileIdCachePurgeStrategy& purge_strategy)
{
  int ret = OB_SUCCESS;
  bool need_print_error = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObFileIdCache is not inited", K(ret));
  } else if (OB_UNLIKELY(!purge_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(purge_strategy));
  } else if (purge_strategy.need_wait(need_print_error)) {
    if (need_print_error) {
      CSR_LOG(ERROR, "no need to purge", K(ret), K(curr_max_file_id_));
    }
    ret = OB_NEED_WAIT;
    CSR_LOG(TRACE, "need wait", K(ret), K(curr_max_file_id_), K(purge_strategy));
  } else {
    WLockGuard guard(rwlock_);
    ObPurgeFunctor purge_functor(purge_strategy);
    if (OB_FAIL(map_.for_each(purge_functor))) {
      CSR_LOG(WARN, "map_ for_each error", K(ret), K(purge_strategy));
    } else {
      const ObPartitionArray& dead_pkeys = purge_functor.get_dead_pkeys();
      int64_t dead_pkey_count = dead_pkeys.count();
      for (int i = 0; OB_SUCC(ret) && i < dead_pkey_count; i++) {
        const ObPartitionKey& pkey = dead_pkeys[i];
        ObFileIdList* list = NULL;
        if (OB_FAIL(map_.get(pkey, list))) {
          CSR_LOG(WARN, "map_ get list error", K(ret), K(pkey), KP(list));
        } else if (OB_ISNULL(list)) {
          ret = OB_ERR_UNEXPECTED;
          CSR_LOG(ERROR, "list is null", K(ret), KP(list));
        } else if (OB_FAIL(map_.erase(pkey))) {
          CSR_LOG(WARN, "map_ erase error", K(ret), K(pkey));
        } else {
          CSR_LOG(TRACE, "purge, map_ erase success", K(pkey), KP(list));
          list->destroy();
          list_allocator_.free(list);
          list = NULL;
        }
        if (OB_SUCC(ret)) {
          CSR_LOG(INFO, "[FILE_ID_CACHE] purge erase dead pkey", K(pkey), K(dead_pkey_count), K(purge_strategy));
        }
      }
      const int64_t next_can_purge_log2file_timestamp = purge_functor.get_next_can_purge_log2file_timestamp();
      ATOMIC_STORE(&next_can_purge_log2file_timestamp_, next_can_purge_log2file_timestamp);
      CSR_LOG(TRACE, "[FILE_ID_CACHE]", K(next_can_purge_log2file_timestamp_));
    }
  }
  CSR_LOG(TRACE, "[FILE_ID_CACHE] purge", K(ret));
  return ret;
}

int ObFileIdCache::ensure_log_continuous(const common::ObPartitionKey& pkey, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  ObFileIdList* list = NULL;
  bool is_continuous = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObFileIdCache is not inited", K(ret));
  } else if (!pkey.is_valid() || OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(pkey), K(log_id));
  } else if (OB_FAIL(map_.get(pkey, list)) && OB_ENTRY_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "map_ get failed", K(ret), K(pkey));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    is_continuous = true;
  } else if (OB_ISNULL(list)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "list is null", K(ret), K(pkey), K(log_id));
  } else if (0 == list->get_min_continuous_log_id()) {
    // list is empty
    ret = OB_SUCCESS;
    is_continuous = true;
  } else if (list->get_min_continuous_log_id() <= (log_id + 1)) {
    is_continuous = true;
  } else {
    is_continuous = false;
  }

  if (OB_SUCC(ret) && (!is_continuous)) {
    if (OB_FAIL(list->purge_preceding_items(pkey))) {
      CSR_LOG(ERROR, "failed to purge_preceding_items", K(ret), K(pkey));
    } else {
      CSR_LOG(INFO, "[FILE_ID_CACHE] purge_partition erase done", K(pkey), KR(ret), K(log_id));
    }
  }
  return ret;
}

int ObFileIdCache::get_clog_base_pos(const ObPartitionKey& pkey, file_id_t& file_id, offset_t& offset) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  ObFileIdList* list = NULL;
  ObLogBasePos pos;
  if (OB_FAIL(map_.get(pkey, list))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      file_id = OB_INVALID_FILE_ID;
      offset = OB_INVALID_OFFSET;
    } else {
      CSR_LOG(WARN, "map_ get list error", K(ret), K(pkey), K(file_id), K(offset));
    }
  } else if (OB_ISNULL(list)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "list is null", K(ret), KP(list));
  } else if (OB_FAIL(list->get_log_base_pos(pos))) {
    CSR_LOG(WARN, "map_ erase error", K(ret), K(pkey));
  } else {
    file_id = pos.file_id_;
    offset = pos.base_offset_;
  }
  return ret;
}

int ObFileIdCache::add_partition_needed(const common::ObPartitionKey& pkey, const uint64_t last_replay_log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObFileIdCache is not inited", KR(ret), K(pkey));
  } else if (OB_FAIL(filter_map_.insert(pkey, last_replay_log_id))) {
    CSR_LOG(WARN, "map_ insert error", KR(ret), K(pkey), K(last_replay_log_id));
  } else {
    need_filter_partition_ = true;
    // do nothing
  }
  return ret;
}

int ObFileIdCache::AppendInfoFunctor::init(const file_id_t file_id, ObFileIdCache* cache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) || OB_UNLIKELY(NULL == cache)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "AppendInfoFunctor init error", K(ret), K(file_id), K(cache));
  } else {
    file_id_ = file_id;
    cache_ = cache;
    err_ = OB_SUCCESS;
  }
  return ret;
}

// For compatibility, allow max_log_id and max_log_timestamp to be invalid
bool ObFileIdCache::AppendInfoFunctor::operator()(const ObPartitionKey& pkey, const IndexInfoBlockEntry& entry)
{
  if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(!entry.is_valid())) {
    err_ = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "invalid arguments", K(err_), K(pkey), K(entry));
  } else if (OB_ISNULL(cache_)) {
    err_ = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "null cache_", K(err_), K(cache_), K(pkey), K(entry));
  } else {
    bool need_filter = false;
    if (OB_SUCCESS != (err_ = cache_->check_need_filter_partition_(pkey, entry.max_log_id_, need_filter))) {
      CSR_LOG(WARN, "failed to check need_filter_partition", K(err_), KP(cache_), K(pkey), K(entry));
    } else if (!need_filter) {
      err_ = cache_->append_(pkey,
          file_id_,
          entry.start_offset_,
          entry.min_log_id_,
          entry.max_log_id_,
          entry.min_log_timestamp_,
          entry.max_log_timestamp_);
    } else { /*just filter*/
    }
  }
  return OB_SUCCESS == err_;
}

bool ObFileIdCache::ObUndoAppendFunctor::operator()(const ObPartitionKey& pkey, ObFileIdList* list)
{
  int ret = OB_SUCCESS;
  bool empty = false;
  if (OB_UNLIKELY(!pkey.is_valid()) || OB_ISNULL(list)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "try apply ObUndoAppendFunctor to an invalid item", K(ret), K(pkey), KP(list));
  } else if (OB_FAIL(list->undo_append(broken_file_id_, empty))) {
    CSR_LOG(WARN, "list undo_append error", K(broken_file_id_), K(pkey), K(empty), KP(list));
  } else if (empty) {
    if (OB_FAIL(dead_pkeys_.push_back(pkey))) {
      CSR_LOG(WARN, "push pkey inito dead_pkeys error", K(ret), K(dead_pkeys_), K(pkey));
    } else {
      CSR_LOG(TRACE, "dead_pkey", K(pkey), K(broken_file_id_), KP(list));
    }
  }
  return OB_SUCCESS == ret;
}

bool ObFileIdCache::ObPurgeFunctor::operator()(const ObPartitionKey& pkey, ObFileIdList* list)
{
  int ret = OB_SUCCESS;
  bool empty = false;
  int64_t front_log2file_max_log_timestamp = OB_INVALID_TIMESTAMP;
  if (OB_ISNULL(list)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "get null list", K(ret), K(pkey), K(list));
  } else if (OB_FAIL(list->purge(pkey, purge_strategy_, empty))) {
    CSR_LOG(ERROR, "list purge error", K(ret), K(pkey), K(purge_strategy_), K(empty));
  } else if (empty) {
    if (OB_FAIL(dead_pkeys_.push_back(pkey))) {
      CSR_LOG(WARN, "append dead pkey error", K(ret), K(pkey), KP(list));
    } else {
      CSR_LOG(TRACE, "dead pkey", K(pkey), KP(list));
    }
  } else if (OB_FAIL(list->get_front_log2file_max_timestamp(front_log2file_max_log_timestamp))) {
    CSR_LOG(ERROR, "get_log2file_max_timestamp unexpected error", K(ret));
  } else if (OB_INVALID_TIMESTAMP == next_can_purge_log2file_timestamp_) {
    next_can_purge_log2file_timestamp_ = front_log2file_max_log_timestamp;
  } else {
    next_can_purge_log2file_timestamp_ = std::min(next_can_purge_log2file_timestamp_, front_log2file_max_log_timestamp);
  }
  CSR_LOG(TRACE, "purge on pkey", K(pkey), K(purge_strategy_), KP(list), K(next_can_purge_log2file_timestamp_));
  return OB_SUCCESS == ret;
}

int ObFileIdCache::LogContinuousFunctor::operator()(const Log2File& log2file_item)
{
  int ret = OB_SUCCESS;
  if (!next_item_.is_valid() && OB_INVALID_ID == memstore_min_log_id_) {
    next_item_ = log2file_item;
  } else if (!next_item_.is_valid() && OB_INVALID_ID != memstore_min_log_id_) {
    next_item_ = log2file_item;
    if (next_item_.get_max_log_id() + 1 != memstore_min_log_id_) {
      ret = OB_CANCELED;
    }
  } else if (!log2file_item.is_preceding_to(next_item_)) {
    ret = OB_CANCELED;
  }
  if (OB_SUCC(ret)) {
    if (log_id_ + 1 >= log2file_item.get_min_log_id()) {
      is_continuous_ = true;
      ret = OB_CANCELED;
    }
  }
  return ret;
}

int ObFileIdCache::append_(const file_id_t file_id, IndexInfoBlockMap& index_info_block_map)
{
  int ret = OB_SUCCESS;
  AppendInfoFunctor append_functor;
  if (OB_FAIL(append_functor.init(file_id, this))) {
    CSR_LOG(WARN, "append_functor init error", K(ret), K(file_id));
  } else {
    if (OB_FAIL(index_info_block_map.for_each(append_functor))) {
      CSR_LOG(WARN, "index_info_block_map apply append_functor error", K(ret));
    } else if (OB_FAIL(append_functor.get_err())) {
      CSR_LOG(WARN, "append_functor exec error", K(ret));
    } else {
      CSR_LOG(TRACE, "do_append success", K(ret), K(file_id));
    }
    if (OB_FAIL(ret)) {
      int undo_ret = undo_append_(file_id);
      if (OB_SUCCESS != undo_ret) {
        CSR_LOG(ERROR, "undo_append error", K(undo_ret), K(ret), K(file_id));
      }
    }
  }
  return ret;
}

int ObFileIdCache::append_(const ObPartitionKey& pkey, const file_id_t file_id, const offset_t start_offset,
    const uint64_t min_log_id, const uint64_t max_log_id, const int64_t min_log_timestamp,
    const int64_t max_log_timestamp)
{
  WLockGuard wguard(rwlock_);
  return do_append_(pkey, file_id, start_offset, min_log_id, max_log_id, min_log_timestamp, max_log_timestamp);
}

int ObFileIdCache::append_new_list_(const ObPartitionKey& pkey, const file_id_t file_id, const offset_t start_offset,
    const uint64_t min_log_id, const uint64_t max_log_id, const int64_t min_log_timestamp,
    const int64_t max_log_timestamp)
{
  int ret = OB_SUCCESS;
  ObFileIdList* list = NULL;
  char* buf = NULL;
  if (OB_UNLIKELY(NULL == (buf = (char*)list_allocator_.alloc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    list = new (buf) ObFileIdList();

    if (OB_FAIL(list->init(
            &seg_array_allocator_, &seg_item_allocator_, &log2file_list_allocator_, &list_item_allocator_, this))) {
      CSR_LOG(WARN, "file_id_list init error", K(ret), K(pkey), K(min_log_id), K(file_id), KP(list));
    } else if (OB_FAIL(list->append(
                   pkey, file_id, start_offset, min_log_id, max_log_id, min_log_timestamp, max_log_timestamp))) {
      CSR_LOG(WARN, "list append error", K(ret), K(pkey), K(min_log_id), K(file_id), KP(list));
    } else if (OB_FAIL(map_.insert(pkey, list))) {
      CSR_LOG(WARN, "map_ insert error", K(pkey), KP(list));
    } else {
      CSR_LOG(TRACE,
          "[FILE_ID_CACHE] append new file for pkey",
          K(pkey),
          K(file_id),
          K(start_offset),
          K(min_log_id),
          K(max_log_id),
          K(min_log_timestamp),
          K(max_log_timestamp));
    }

    if (OB_FAIL(ret) && NULL != list) {
      list->~ObFileIdList();
      list_allocator_.free(list);
      list = NULL;
    }
  }
  return ret;
}

// For compatibility, allow max_log_id and max_log_timestamp to be invalid
int ObFileIdCache::do_append_(const ObPartitionKey& pkey, const file_id_t file_id, const offset_t start_offset,
    const uint64_t min_log_id, const uint64_t max_log_id, const int64_t min_log_timestamp,
    const int64_t max_log_timestamp)
{
  int ret = OB_SUCCESS;
  ObFileIdList* list = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(OB_INVALID_FILE_ID == file_id) ||
             OB_UNLIKELY(OB_INVALID_ID == min_log_id) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == min_log_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN,
        "do_append error",
        K(ret),
        K(pkey),
        K(file_id),
        K(start_offset),
        K(min_log_id),
        K(min_log_timestamp),
        K(max_log_id),
        K(max_log_timestamp));
  } else if (OB_FAIL(map_.get(pkey, list))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(append_new_list_(
              pkey, file_id, start_offset, min_log_id, max_log_id, min_log_timestamp, max_log_timestamp))) {
        CSR_LOG(WARN,
            "append new list error",
            K(ret),
            K(pkey),
            K(file_id),
            K(start_offset),
            K(min_log_id),
            K(max_log_id),
            K(min_log_timestamp),
            K(max_log_timestamp));
      } else {
        CSR_LOG(TRACE, "append new list success", K(pkey), K(min_log_id), K(file_id));
      }
    } else {
      CSR_LOG(WARN, "get file id list from map fail", K(ret), K(pkey));
    }
  } else if (OB_ISNULL(list)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "get null list from map", K(ret), K(pkey), K(file_id));
  } else if (OB_FAIL(list->append(
                 pkey, file_id, start_offset, min_log_id, max_log_id, min_log_timestamp, max_log_timestamp))) {
    CSR_LOG(WARN,
        "file id list append log2file item error",
        K(ret),
        K(pkey),
        K(file_id),
        K(start_offset),
        K(min_log_id),
        K(max_log_id),
        K(min_log_timestamp),
        K(max_log_timestamp));
  }
  if (OB_SUCC(ret)) {
    CSR_LOG(TRACE,
        "[FILE_ID_CACHE] do_append success",
        K(pkey),
        K(file_id),
        K(start_offset),
        K(min_log_id),
        K(max_log_id),
        K(min_log_timestamp),
        K(max_log_timestamp));
  } else {
    CSR_LOG(WARN,
        "ObFileIdCache do_append error",
        K(ret),
        K(pkey),
        K(file_id),
        K(start_offset),
        K(min_log_id),
        K(max_log_id),
        K(min_log_timestamp),
        K(max_log_timestamp));
  }
  return ret;
}

int ObFileIdCache::undo_append_(const file_id_t broken_file_id)
{
  int ret = OB_SUCCESS;
  WLockGuard wguard(rwlock_);
  if (OB_UNLIKELY(OB_INVALID_FILE_ID == broken_file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "ObFileIdCachel undo_append error", K(ret), K(broken_file_id));
  } else {
    ObUndoAppendFunctor undo_functor(broken_file_id);
    if (OB_FAIL(map_.for_each(undo_functor))) {
      CSR_LOG(WARN, "map_ apply undo_functor error", K(ret), K(undo_functor));
    } else {
      const ObPartitionArray& dead_pkeys = undo_functor.get_dead_pkeys();
      int64_t dead_pkey_count = dead_pkeys.count();
      for (int i = 0; OB_SUCC(ret) && i < dead_pkey_count; i++) {
        const ObPartitionKey& pkey = dead_pkeys[i];
        ObFileIdList* list = NULL;
        if (OB_FAIL(map_.get(pkey, list))) {
          CSR_LOG(WARN, "map_ get list error", K(ret), K(pkey), KP(list));
        } else if (OB_ISNULL(list)) {
          ret = OB_ERR_UNEXPECTED;
          CSR_LOG(ERROR, "list is null", K(ret), KP(list));
        } else if (OB_FAIL(map_.erase(pkey))) {
          CSR_LOG(WARN, "map_ erase error", K(ret), K(pkey));
        } else {
          CSR_LOG(TRACE, "undo_append, map erase success", K(pkey), KP(list));
          list->destroy();
          list_allocator_.free(list);
          list = NULL;
        }
      }

      if (OB_SUCC(ret)) {
        CSR_LOG(INFO, "[FILE_ID_CACHE] undo_append success", K(broken_file_id), K(dead_pkey_count));
      }
    }
  }
  return ret;
}

int ObFileIdCache::check_need_filter_partition_(
    const common::ObPartitionKey& pkey, const uint64_t max_log_id, bool& need_filter)
{
  int ret = OB_SUCCESS;
  need_filter = false;
  if (need_filter_partition_) {
    uint64_t last_replay_log_id = OB_INVALID_ID;
    if (OB_FAIL(filter_map_.get(pkey, last_replay_log_id)) && OB_ENTRY_NOT_EXIST != ret) {
      CSR_LOG(WARN, "failed to get element from filter_map_", K(ret), K(pkey));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      need_filter = true;
      ret = OB_SUCCESS;
    } else if (OB_UNLIKELY(OB_INVALID_ID == last_replay_log_id)) {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(ERROR, "unexpected last_replay_log_id", K(ret), K(pkey), K(last_replay_log_id), K(max_log_id));
    } else if (max_log_id <= last_replay_log_id) {
      need_filter = true;
    } else {
      need_filter = false;
    }
  }
  return ret;
}

int ObFileIdCache::get_cursor_from_file(
    const ObPartitionKey& pkey, const uint64_t log_id, const Log2File& item, ObLogCursorExt& log_cursor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObFileIdCache is not inited", KR(ret), K(pkey));
  } else if (OB_FAIL(ilog_accessor_->get_cursor_from_ilog_file(addr_, server_seq_, pkey, log_id, item, log_cursor))) {
    CSR_LOG(WARN, "failed to get_cursor_from_file", KR(ret), K(pkey), K(log_id), K(addr_), K(server_seq_));
  } else { /*do nothing*/
  }
  return ret;
}

}  // namespace clog
}  // namespace oceanbase
