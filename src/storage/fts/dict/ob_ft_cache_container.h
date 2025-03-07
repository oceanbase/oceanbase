/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
  ObFTDictType type_;
  // kvcache
  ObKVCacheHandle handle_;
  const ObDictCacheKey *key_;
  const ObDictCacheValue *value_;

public:
  ObFTCacheRangeHandle()
      : type_(ObFTDictType::DICT_TYPE_INVALID), handle_(), key_(nullptr), value_(nullptr)
  {
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
    if (OB_ISNULL(handle = static_cast<ObFTCacheRangeHandle *>(
                      alloc_.alloc(sizeof(ObFTCacheRangeHandle))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (FALSE_IT(new (handle) ObFTCacheRangeHandle())) {
      // never reach
    } else if (OB_FAIL(handles_.push_back(handle))) {
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
