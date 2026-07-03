/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_cache.h"

#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace storage
{
int ObDictCache::get_dict(const ObDictCacheKey &key,
                          const ObDictCacheValue *&value,
                          common::ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_FAIL(get(key, value, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get dict from cache failed", K(ret));
    }
  }
  return ret;
}

int ObDictCache::put_dict(const ObDictCacheKey &key, const ObDictCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(put(key, value))) {
    LOG_WARN("put dict to cache failed", K(ret));
  }
  return ret;
}

int ObDictCache::put_and_fetch_dict(const ObDictCacheKey &key,
                                    const ObDictCacheValue &value,
                                    const ObDictCacheValue *&pvalue,
                                    common::ObKVCacheHandle &handle,
                                    bool overwrite)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_FAIL(put_and_fetch(key, value, pvalue, handle, overwrite))) {
    LOG_WARN("put dict to cache failed", K(ret));
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
