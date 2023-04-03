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

#ifndef OCEANBASE_STORAGE_OB_HANDLE_MGR_H
#define OCEANBASE_STORAGE_OB_HANDLE_MGR_H

#include "storage/ob_handle_cache.h"

namespace oceanbase
{
namespace storage
{

template<typename Handle, typename Key, int64_t N>
class ObHandleMgr
{
public:
  ObHandleMgr()
    : is_inited_(false),
      is_multi_(false),
      is_ordered_(false),
      last_handle_(NULL),
      handle_cache_(NULL)
  {}
  virtual ~ObHandleMgr() { reset(); }
  void reset()
  {
    is_multi_ = false;
    is_ordered_ = false;
    if (NULL != last_handle_) {
      last_handle_->~Handle();
      last_handle_ = NULL;
    }
    if (NULL != handle_cache_) {
      handle_cache_->~ObHandleCache();
      handle_cache_ = NULL;
    }
    is_inited_ = false;
  }
  int init(const bool is_multi, const bool is_ordered, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    void *buf = NULL;
    if (OB_UNLIKELY(is_inited_)) {
      ret = common::OB_INIT_TWICE;
      STORAGE_LOG(WARN, "handle mgr is inited twice", K(ret));
    } else if (is_multi) {
      if (is_ordered) {
        if (OB_ISNULL(buf = allocator.alloc(sizeof(Handle)))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to allocate last handle");
        } else {
          last_handle_ = new (buf) Handle();
        }
      } else {
        if (OB_ISNULL(buf = allocator.alloc(sizeof(HandleCache)))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to allocate last handle");
        } else {
          handle_cache_ = new (buf) HandleCache();
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_multi_ = is_multi;
      is_ordered_ = is_ordered;
      is_inited_ = true;
    }
    return ret;
  }
  inline bool is_inited() { return is_inited_; }
  TO_STRING_KV(KP_(last_handle), KP_(handle_cache));
protected:
  typedef ObHandleCache<Key, Handle, N> HandleCache;
  bool is_inited_;
  bool is_multi_;
  bool is_ordered_;
  Handle *last_handle_;
  HandleCache *handle_cache_;
};

}
}
#endif /* OCEANBASE_STORAGE_OB_HANDLE_MGR_H */
