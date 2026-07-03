/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_CONTAINER_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_CONTAINER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/fts/dict/ob_ft_cache.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
namespace oceanbase
{
namespace storage
{
// type
struct ObFTCacheRangeHandle
{
  // kvcache
  ObKVCacheHandle handle_;
  ObDictCacheKey key_;
  const ObDictCacheValue *value_;

public:
  ObFTCacheRangeHandle()
      : handle_(), key_(), value_(nullptr)
  {
  }
  ~ObFTCacheRangeHandle() {}
  void move_from(ObFTCacheRangeHandle &other)
  {
    this->handle_.move_from(other.handle_);
    this->key_ = other.key_;
    this->value_ = other.value_;
    other.value_ = nullptr;
  }
};

class ObFTCacheRangeContainer
{
public:
  ObFTCacheRangeContainer(ObIAllocator &alloc) : alloc_(alloc), handles_(alloc) {}
  ~ObFTCacheRangeContainer() { reset(); }

public:
  int fetch_info_for_dict(ObFTCacheRangeHandle *&info)
  {
    int ret = OB_SUCCESS;
    ObFTCacheRangeHandle *handle = nullptr;
    if (OB_ISNULL(handle = OB_NEWx(ObFTCacheRangeHandle, &alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(handles_.push_back(handle))) {
      OB_DELETEx(ObFTCacheRangeHandle, &alloc_, handle);
      STORAGE_FTS_LOG(WARN, "Failed to push back handle", K(ret));
    } else {
      info = handle;
    }
    return ret;
  }

  const ObList<ObFTCacheRangeHandle *, ObIAllocator> &get_handles() const { return handles_; }

  void reset()
  {
    // clear all info
    for (ObList<ObFTCacheRangeHandle *, ObIAllocator>::iterator iter = handles_.begin();
         iter != handles_.end();
         ++iter) {
      if (OB_NOT_NULL(*iter)) {
        (*iter)->~ObFTCacheRangeHandle();
        alloc_.free(*iter);
        *iter = nullptr;
      }
    }
    handles_.reset();
  }

private:
  ObIAllocator &alloc_;
  ObList<ObFTCacheRangeHandle *, ObIAllocator> handles_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFTCacheRangeContainer);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_CONTAINER_H_
