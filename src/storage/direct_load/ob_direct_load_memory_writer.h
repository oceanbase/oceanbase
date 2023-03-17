// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_vector.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace storage
{

template <typename T, typename Compare, typename FramgentBuilder>
class ObDirectLoadMemoryWriter
{
  static const int64_t MIN_MEMORY_LIMIT = 8 * 1024LL * 1024LL; // min memory limit is 8M
public:
  ObDirectLoadMemoryWriter();
  ~ObDirectLoadMemoryWriter();
  void reuse();
  void reset();
  int init(uint64_t tenant_id, int64_t mem_limit, Compare *compare,
           FramgentBuilder *fragment_builder);
  int add_item(const T &item);
  int close();
  TO_STRING_KV(K(is_inited_), K(buf_mem_limit_), KP(compare_), KP(fragment_builder_));
private:
  int build_fragment();
private:
  int64_t buf_mem_limit_;
  Compare *compare_;
  FramgentBuilder *fragment_builder_;
  common::ObArenaAllocator allocator_;
  common::ObVector<T *> item_list_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadMemoryWriter);
};

template <typename T, typename Compare, typename FramgentBuilder>
ObDirectLoadMemoryWriter<T, Compare, FramgentBuilder>::ObDirectLoadMemoryWriter()
  : buf_mem_limit_(0),
    compare_(nullptr),
    fragment_builder_(nullptr),
    allocator_("TLD_MemWriter"),
    is_inited_(false)
{
}

template <typename T, typename Compare, typename FramgentBuilder>
ObDirectLoadMemoryWriter<T, Compare, FramgentBuilder>::~ObDirectLoadMemoryWriter()
{
  reset();
}

template <typename T, typename Compare, typename FramgentBuilder>
void ObDirectLoadMemoryWriter<T, Compare, FramgentBuilder>::reuse()
{
  for (int64_t i = 0; i < item_list_.size(); ++i) {
    item_list_[i]->~T();
  }
  item_list_.reset();
  allocator_.reset();
}

template <typename T, typename Compare, typename FramgentBuilder>
void ObDirectLoadMemoryWriter<T, Compare, FramgentBuilder>::reset()
{
  buf_mem_limit_ = 0;
  compare_ = nullptr;
  fragment_builder_ = nullptr;
  for (int64_t i = 0; i < item_list_.size(); ++i) {
    item_list_[i]->~T();
  }
  item_list_.reset();
  allocator_.reset();
  is_inited_ = false;
}

template <typename T, typename Compare, typename FramgentBuilder>
int ObDirectLoadMemoryWriter<T, Compare, FramgentBuilder>::init(uint64_t tenant_id,
                                                                int64_t mem_limit, Compare *compare,
                                                                FramgentBuilder *fragment_builder)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDirectLoadMemoryWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(common::OB_INVALID_ID == tenant_id || mem_limit < MIN_MEMORY_LIMIT ||
                         nullptr == compare || nullptr == fragment_builder)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), K(mem_limit), KP(compare), KP(fragment_builder));
  } else {
    buf_mem_limit_ = mem_limit;
    compare_ = compare;
    fragment_builder_ = fragment_builder;
    allocator_.set_tenant_id(tenant_id);
    is_inited_ = true;
  }
  return ret;
}

template <typename T, typename Compare, typename FramgentBuilder>
int ObDirectLoadMemoryWriter<T, Compare, FramgentBuilder>::add_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadMemoryWriter not init", KR(ret), KP(this));
  } else {
    const int64_t item_size = sizeof(T) + item.get_deep_copy_size();
    if (item_size > buf_mem_limit_) {
      ret = common::OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "invalid item size, must not larger than buf memory limit", KR(ret),
                  K(item_size), K(buf_mem_limit_));
    } else if (allocator_.used() + item_size > buf_mem_limit_ && OB_FAIL(build_fragment())) {
      STORAGE_LOG(WARN, "fail to build fragment", KR(ret));
    } else {
      OB_TABLE_LOAD_STATISTICS_TIME_COST(memory_add_item_time_us);
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

template <typename T, typename Compare, typename FramgentBuilder>
int ObDirectLoadMemoryWriter<T, Compare, FramgentBuilder>::build_fragment()
{
  int ret = common::OB_SUCCESS;
  if (item_list_.size() > 1) {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(memory_sort_item_time_us);
    std::sort(item_list_.begin(), item_list_.end(), *compare_);
    if (OB_FAIL(compare_->get_error_code())) {
      ret = compare_->get_error_code();
      STORAGE_LOG(WARN, "fail to sort memory item list", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fragment_builder_->build_fragment(item_list_))) {
      STORAGE_LOG(WARN, "fail to build fragment", KR(ret));
    } else {
      reuse();
    }
  }
  return ret;
}

template <typename T, typename Compare, typename FramgentBuilder>
int ObDirectLoadMemoryWriter<T, Compare, FramgentBuilder>::close()
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadMemoryWriter not init", KR(ret), KP(this));
  } else if (item_list_.size() > 0 && OB_FAIL(build_fragment())) {
    STORAGE_LOG(WARN, "fail to build fragment", KR(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
