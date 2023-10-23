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

#ifndef SHARE_CACHE_OB_VTABLE_EVENT_RECYCLE_BUFFER_H
#define SHARE_CACHE_OB_VTABLE_EVENT_RECYCLE_BUFFER_H

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_recycle_multi_kvcache.h"
#include "share/ob_delegate.h"
#include "lib/literals/ob_literals.h"
#include <utility>

namespace oceanbase
{
namespace common
{
namespace cache
{

template <typename K, typename V>
struct ObRecycleMultiKVCacheAligedWrapper
{
  ObRecycleMultiKVCache<K, V> cache_;
  DELEGATE(cache_, init);
  DELEGATE(cache_, append);
  DELEGATE(cache_, for_each);
} CACHE_ALIGNED;

template <typename K, typename V>
class ObVtableEventRecycleBuffer {
public:
  ObVtableEventRecycleBuffer() : buffer_bkt_(nullptr), bkt_len_(0), alloc_(nullptr) {}
  ~ObVtableEventRecycleBuffer() {
    if (OB_NOT_NULL(alloc_)) {
      for (int64_t idx = 0; idx < bkt_len_; ++idx) {
        buffer_bkt_[idx].~ObRecycleMultiKVCacheAligedWrapper();
      }
      alloc_->free(buffer_bkt_);
      buffer_bkt_ = nullptr;
      bkt_len_ = 0;
      alloc_ = 0;
    }
  }
  ObVtableEventRecycleBuffer(const ObVtableEventRecycleBuffer &) = delete;
  template <int N>
  int init (const char (&mem_tag)[N],// tag used for alloc static memory
            ObIAllocator &alloc,// the real allocator to alloc memory
            const int64_t recycle_buffer_number,
            const int64_t recycle_buffer_size_each,
            const int64_t hash_idx_bkt_num_each) {
    #define PRINT_WRAPPER K(ret), K(mem_tag), K(recycle_buffer_number), K(recycle_buffer_number), K(hash_idx_bkt_num_each)
    int ret = OB_SUCCESS;
    if (OB_ISNULL(mem_tag) ||
        recycle_buffer_number < 1 ||
        recycle_buffer_size_each < 1 ||
        hash_idx_bkt_num_each < 1) {
      ret = OB_INVALID_ARGUMENT;
      OCCAM_LOG(WARN, "invalid argument", PRINT_WRAPPER);
    } else if (OB_NOT_NULL(alloc_)) {
      ret = OB_INIT_TWICE;
      OCCAM_LOG(WARN, "invalid argument", PRINT_WRAPPER);
    } else if (nullptr == (buffer_bkt_ = (ObRecycleMultiKVCacheAligedWrapper<K, V> *)alloc.alloc(sizeof(ObRecycleMultiKVCacheAligedWrapper<K, V>) * recycle_buffer_number))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OCCAM_LOG(WARN, "fail to allocate", PRINT_WRAPPER);
    } else {
      alloc_ = &alloc;
      for (int64_t idx = 0; idx < recycle_buffer_number && OB_SUCC(ret); ++idx) {
        new (&buffer_bkt_[idx]) ObRecycleMultiKVCacheAligedWrapper<K, V>();
        if (OB_FAIL(buffer_bkt_[idx].init(mem_tag,
                                          alloc,
                                          recycle_buffer_size_each,
                                          hash_idx_bkt_num_each,
                                          idx))) {
          OCCAM_LOG(WARN, "fail init cache", PRINT_WRAPPER, K(idx));
        } else {
          bkt_len_ = idx + 1;
        }
      }
    }
    if (OB_FAIL(ret)) {
      this->~ObVtableEventRecycleBuffer();
      new (this) ObVtableEventRecycleBuffer();
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  template <typename Value>
  int append(const K &key, Value &&event, const char *file = nullptr, const uint32_t line = 0, const char *func = nullptr) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buffer_bkt_)) {
      ret = OB_NOT_INIT;
      // OCCAM_LOG(WARN, "not init", K(ret), K(key), K(event));
    } else if (OB_FAIL(buffer_bkt_[key.hash() % bkt_len_].append(key, std::forward<Value>(event)))) {
      OCCAM_LOG(WARN, "fail to append event", K(ret), K(key), K(event));
    } else {
      OCCAM_LOG(TRACE, "succ to append event", K(ret), K(key), K(event));
    }
    return ret;
  }
  template <typename OP>// int OP(const V &);
  int for_each(const K &key, OP &&op) const {// optimized for point select
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buffer_bkt_)) {
      ret = OB_NOT_INIT;
      OCCAM_LOG(WARN, "not init", K(ret));
    } else {
      ret = buffer_bkt_[key.hash() % bkt_len_].for_each(key, std::forward<OP>(op));
    }
    return ret;
  }
  template <typename OP>// int OP(const K &, const V &);
  int for_each(OP &&op) const {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buffer_bkt_)) {
      ret = OB_NOT_INIT;
      OCCAM_LOG(WARN, "not init", K(ret));
    } else {
      for (int64_t idx = 0; idx < bkt_len_ && OB_SUCC(ret); ++idx) {
        ret = buffer_bkt_[idx].for_each(std::forward<OP>(op));
      }
    }
    return ret;
  }
  void dump_statistics() const {
    constexpr int64_t stack_buffer_size = 1_KB;
    int64_t buffer_size = 0;
    char info[stack_buffer_size] = { 0 };
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buffer_bkt_)) {
      ret = OB_NOT_INIT;
      OCCAM_LOG(WARN, "not init", K(ret));
    } else {
      for (int64_t idx = 0; idx < bkt_len_; ++idx) {
        if (idx != 0) {
          databuff_printf(info, stack_buffer_size, pos, ", ");
        }
        int64_t write_pos = 0;
        int64_t recycle_pos = 0;
        int64_t write_number = 0;
        int64_t recycle_number = 0;
        buffer_bkt_[idx].cache_.get_statistics(write_pos, recycle_pos, write_number, recycle_number, buffer_size);
        databuff_printf(info, stack_buffer_size, pos,
                        "[%ld: total_write_size:%s, total_recycle_size:%s, total_write_num:%ld, total_recycle_num:%ld]",
                        idx, to_cstring(ObSizeLiteralPrettyPrinter(write_pos)), to_cstring(ObSizeLiteralPrettyPrinter(recycle_pos)),
                        write_number, recycle_number);
      }
      databuff_printf(info, stack_buffer_size, pos, ", buffer_size_each:%s", to_cstring(ObSizeLiteralPrettyPrinter(buffer_size)));
      OCCAM_LOG(INFO, "DUMP VTableBuffer STATISTICS", K(info));
    }
  }
private:
  ObRecycleMultiKVCacheAligedWrapper<K, V> *buffer_bkt_;
  int64_t bkt_len_;
  ObIAllocator *alloc_;
};

}
}
}

#endif