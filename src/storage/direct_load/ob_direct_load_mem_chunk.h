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
#pragma once

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_vector.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "storage/direct_load/ob_direct_load_external_scanner.h"

namespace oceanbase
{
namespace storage
{

template <typename T, typename Compare>
class ObDirectLoadMemChunk;

template <typename T, typename Compare>
class ObDirectLoadMemChunkIter : public ObDirectLoadExternalIterator<T>
{
public:
  ObDirectLoadMemChunkIter() : chunk_(nullptr), start_(-1), end_(-1) {}
  ObDirectLoadMemChunkIter(ObDirectLoadMemChunk<T, Compare> *chunk,
                               int64_t start,
                               int64_t end) :
    chunk_(chunk), start_(start), end_(end) {}

  int get_next_item(const T *&item);

private:
  ObDirectLoadMemChunk<T, Compare> *chunk_;
  int64_t start_;
  int64_t end_;
};

template <typename T, typename Compare>
class ObDirectLoadMemChunk
{
  friend class ObDirectLoadMemChunkIter<T, Compare>;
public:
  static const constexpr int64_t MIN_MEMORY_LIMIT = 8 * 1024LL * 1024LL; // min memory limit is 8M

  ObDirectLoadMemChunk();
  int init(uint64_t tenant_id, int64_t mem_limit);
  int add_item(const T &item);
  int64_t get_size() const {
    return item_list_.size();
  }

  T *get_item(int64_t idx) {
    return item_list_[idx];
  }

  // start如果是nullptr，表示min
  // end如果是nullptr，表示max
  ObDirectLoadMemChunkIter<T, Compare> scan(T *start, T *end, Compare &compare) { //左开右闭
    int64_t start_idx = 0;
    int64_t end_idx = 0;
    if (start != nullptr) {
      auto iter = std::upper_bound(item_list_.begin(), item_list_.end(), start, compare);
      start_idx = (iter - item_list_.begin());
    } else {
      start_idx = 0;
    }
    if (end != nullptr) {
      auto iter2 = std::upper_bound(item_list_.begin(), item_list_.end(), end, compare);
      end_idx = iter2 - item_list_.begin() - 1;
    } else {
      end_idx = item_list_.size() - 1;
    }
    return ObDirectLoadMemChunkIter<T, Compare>(this, start_idx, end_idx);
  }

  void reuse();
  void reset();
  int sort(Compare &compare);
  TO_STRING_KV(K(buf_mem_limit_), "size", item_list_.size());
private:
  int64_t buf_mem_limit_;
  common::ObArenaAllocator allocator_;
  common::ObArray<T *> item_list_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadMemChunk);
};

template <typename T, typename Compare>
int ObDirectLoadMemChunkIter<T, Compare>::get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  if (start_ > end_) {
    ret = common::OB_ITER_END;
  } else {
    item = chunk_->item_list_[start_ ++];
  }
  return ret;
}

template <typename T, typename Compare>
int ObDirectLoadMemChunk<T, Compare>::sort(Compare &compare)
{
  int ret = common::OB_SUCCESS;
  if (item_list_.size() > 1) {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, memory_sort_item_time_us);
    std::sort(item_list_.begin(), item_list_.end(), compare);
    if (OB_FAIL(compare.get_error_code())) {
      ret = compare.get_error_code();
      STORAGE_LOG(WARN, "fail to sort memory item list", KR(ret));
    }
  }
  return ret;
}

template <typename T, typename Compare>
ObDirectLoadMemChunk<T, Compare>::ObDirectLoadMemChunk()
  : buf_mem_limit_(0),
    allocator_("TLD_MemChunk"),
    is_inited_(false)
{
}

template <typename T, typename Compare>
int ObDirectLoadMemChunk<T, Compare>::init(uint64_t tenant_id, int64_t mem_limit)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDirectLoadMemChunk init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(mem_limit < MIN_MEMORY_LIMIT)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(mem_limit));
  } else {
    buf_mem_limit_ = mem_limit;
    allocator_.set_tenant_id(tenant_id);
    item_list_.set_attr(ObMemAttr(tenant_id, "TLD_MemChunk"));
    is_inited_ = true;
  }
  return ret;
}

template <typename T, typename Compare>
int ObDirectLoadMemChunk<T, Compare>::add_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadMemChunk not init", KR(ret), KP(this));
  } else {
    const int64_t item_size = sizeof(T) + item.get_deep_copy_size();
    if (item_size > buf_mem_limit_) {
      ret = common::OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "invalid item size, must not larger than buf memory limit", KR(ret),
                  K(item_size), K(buf_mem_limit_));
    } else if (allocator_.used() + item_size > buf_mem_limit_) {
      return OB_BUF_NOT_ENOUGH;
    } else {
      OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, memory_add_item_time_us);
      char *buf = nullptr;
      T *new_item = nullptr;
      if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(item_size)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to allocate memory", KR(ret), K(item_size));
      } else {
        new_item = new (buf) T();
        int64_t buf_pos = sizeof(T);
        if (OB_FAIL(new_item->deep_copy(item, buf, item_size, buf_pos))) {
          STORAGE_LOG(WARN, "fail to deep copy item", KR(ret));
        } else if (OB_FAIL(item_list_.push_back(new_item))) {
          STORAGE_LOG(WARN, "fail to push back new item", KR(ret));
        }
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
void ObDirectLoadMemChunk<T, Compare>::reuse()
{
  for (int64_t i = 0; i < item_list_.size(); ++i) {
    item_list_[i]->~T();
  }
  item_list_.reset();
  allocator_.reuse();
}

template <typename T, typename Compare>
void ObDirectLoadMemChunk<T, Compare>::reset()
{
  reuse();
  allocator_.reset();
  is_inited_ = false;
}


} // namespace storage
} // namespace oceanbase
