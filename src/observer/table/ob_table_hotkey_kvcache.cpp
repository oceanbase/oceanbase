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
#include "ob_table_hotkey_kvcache.h"
#include "ob_table_throttle.h"
#include "ob_table_hotkey.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{

/**
 * -----------------------------------ObTableHotKeyCacheKey-----------------------------------
 */

inline uint64_t ObTableHotKeyCacheKey::get_tenant_id() const
{
  return tenant_id_ ? tenant_id_ : common::OB_SYS_TENANT_ID;
}

/**
  * @param [in]   allocator allocator for malloc
  * @param [out]  key copy to
  */
int ObTableHotKeyCacheKey::deep_copy(ObFIFOAllocator &allocator, ObTableHotKeyCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  char* buf = nullptr;
  ObIKVCacheKey *new_key = nullptr;
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(size()));
  } else if(OB_FAIL(deep_copy(buf, size(), new_key))) {
    LOG_WARN("fail to deep copy", K(ret));
    allocator.free(buf);
  } else {
    key = static_cast<ObTableHotKeyCacheKey*>(new_key);
  }
  return ret;
}

/**
  * @param [in]   buf buffer for copy
  * @param [in]   buf_len length of buffer
  * @param [out]  key copy to
  * for kvcache to deep copy
  */
int ObTableHotKeyCacheKey::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer is invalid for deep copy", K(buf), K(buf_len), K(size()), K(ret));
  } else {
    ObTableHotKeyCacheKey* key_pointer = new (buf) ObTableHotKeyCacheKey();
    *key_pointer = *this;
    // deep copy rowkey
    memcpy(buf + sizeof(ObTableHotKeyCacheKey), rowkey_.ptr(), rowkey_.length());
    key_pointer->rowkey_ = ObString(rowkey_.size(), rowkey_.length(), (buf + sizeof(ObTableHotKeyCacheKey)));
    key = key_pointer;
  }
  return ret;
}


/**
 * -----------------------------------ObTableSlideWindowHotkey-----------------------------------
 */

inline uint64_t ObTableSlideWindowHotkey::get_tenant_id() const
{
  return tenant_id_ ? tenant_id_ : common::OB_SYS_TENANT_ID;
}

/**
  * @param [in]   allocator allocator for malloc
  * @param [out]  key copy to
  */
int ObTableSlideWindowHotkey::deep_copy(ObFIFOAllocator &allocator, ObTableSlideWindowHotkey *&key) const
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  ObIKVCacheKey *new_key = nullptr;
  if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(size()));
  } else if(OB_FAIL(deep_copy(buf, size(), new_key))) {
    LOG_WARN("fail to deep copy", K(ret));
    allocator.free(buf);
  } else {
    key = static_cast<ObTableSlideWindowHotkey*>(new_key);
  }
  return ret;
}

/**
  * @param [in]   buf buffer for copy
  * @param [in]   buf_len length of buffer
  * @param [out]  key copy to
  * for kvcache to deep copy
  */
int ObTableSlideWindowHotkey::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer is invalid for deep copy", K(buf), K(buf_len), K(size()), K(ret));
  } else {
    ObTableSlideWindowHotkey* key_pointer = new (buf) ObTableSlideWindowHotkey();
    *key_pointer = *this;
    // deep copy rowkey
    memcpy(buf + sizeof(ObTableSlideWindowHotkey), rowkey_.ptr(), rowkey_.length());
    key_pointer->rowkey_ = ObString(rowkey_.size(), rowkey_.length(), (buf + sizeof(ObTableSlideWindowHotkey)));
    key = key_pointer;
  }
  return ret;
}


/**
 * -----------------------------------ObTableHotKeyCacheValue-----------------------------------
 */

/**
  * @param [in]   buf buffer for copy
  * @param [in]   buf_len length of buffer
  * @param [out]  key copy to
  */
int ObTableHotKeyCacheValue::deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer is invalid for deep copy", K(buf), K(buf_len), K(size()), K(ret));
  } else {
    ObTableHotKeyCacheValue* key_pointer = new (buf) ObTableHotKeyCacheValue();
    *key_pointer = *this;
    key = key_pointer;
  }
  return ret;
}

/**
 * -----------------------------------ObTableHotKeyCache-----------------------------------
 */
int ObTableHotKeyCache::init(const char* cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (hotkey_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet location cache has already inited", K(ret));
  } else if (OB_FAIL(ObKVCache::init(cache_name, priority))) {
    LOG_WARN("hotkey cache init failed", K(ret));
  } else {
    hotkey_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

/**
  * @param [in]   key a key that has been get
  * @param [in]   value useless
  * @param [in]   get_cnt_inc increament of key ( > 1 )
  * @param [out]  get_cnt result get_cnt of the key
  */
int ObTableHotKeyCache::get(const ObTableHotKeyCacheKey &key, const ObTableHotKeyCacheValue &value, int64_t get_cnt_inc, int64_t &get_cnt)
{
  int ret = OB_SUCCESS;
  if (get_cnt_inc < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid count in hotkey kvcache", K(get_cnt_inc), K(ret));
  } else {
    ObKVCacheHandle handle;
    const struct ObTableHotKeyCacheValue *pvalue = &value;
    for (int64_t i = 0; i < get_cnt_inc && OB_SUCC(ret); ++i) {
      if (OB_FAIL(ObKVCache::get(key, pvalue, handle, get_cnt))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get get_cnt from key", K(ret), K(key));
        } else {
          // key not in kvcache -> put first then get
          if (OB_FAIL(put(key, value))) {
            LOG_WARN("fail to put key first (in get)", K(ret));
          } else {
            if (1 == get_cnt_inc) {
              // new hotkey and just put
              get_cnt = 1;
            } else if (OB_FAIL(get(key, value, get_cnt_inc-1, get_cnt))) {
              LOG_WARN("fail get get_cnt from key", K(ret));
            }
          }
          break;
        }
      }
    }
  }
  return ret;
}

/**
  * @param [in]   key a key that has been get
  * @param [in]   value useless
  * @param [in]   get_cnt_inc increament of key ( > 1 )
  * @param [out]  get_cnt result get_cnt of the key
  * get result from kvcache without put, if a key is not in kvcache then it will not add to kvcache
  */
int ObTableHotKeyCache::get_without_put(const ObTableHotKeyCacheKey &key, const ObTableHotKeyCacheValue *&value, int64_t get_cnt_inc, int64_t &get_cnt)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;
  for (int64_t i = 0; i < get_cnt_inc && OB_SUCC(ret); ++i) {
    if (OB_FAIL(ObKVCache::get(key, value, handle, get_cnt))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get get_cnt from key", K(ret), K(key));
      }
    }
  }
  return ret;
}

/**
  * @param [in]   key a key that has been get
  * @param [in]   value useless
  * Always set the new key's cnt to 1
  */
int ObTableHotKeyCache::put(const ObTableHotKeyCacheKey& key, const ObTableHotKeyCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObKVCache::put(key, value))) {
    LOG_WARN("fail to put key into kvcache", K(ret), K(key));
  }
  return ret;
}

} /* namespace table */
} /* namespace oceanbase */