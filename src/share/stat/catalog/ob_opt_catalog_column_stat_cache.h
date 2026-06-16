/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_STAT_EXTERNAL_OB_OPT_EXTERNAL_COLUMN_STAT_CACHE_
#define OCEANBASE_SHARE_STAT_EXTERNAL_OB_OPT_EXTERNAL_COLUMN_STAT_CACHE_

#include "lib/allocator/ob_allocator.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat.h"

namespace oceanbase
{
namespace share
{
class ObOptCatalogColumnStatHandle;
class ObOptCatalogColumnStatCache
    : public common::ObKVCache<ObOptCatalogColumnStat::Key, ObOptCatalogColumnStat>
{
public:
  ObOptCatalogColumnStatCache()
  {
  }
  ~ObOptCatalogColumnStatCache()
  {
  }

  /**
   * @param[out] handle  A handle object holding the column statistics.
   *                     the real data is stored in handle.stat_
   */
  int get_row(const ObOptCatalogColumnStat::Key &key, ObOptCatalogColumnStatHandle &handle);
  int put_row(const ObOptCatalogColumnStat::Key &key, const ObOptCatalogColumnStat &value);
  int put_and_fetch_row(const ObOptCatalogColumnStat::Key &key,
                        const ObOptCatalogColumnStat &value,
                        ObOptCatalogColumnStatHandle &handle);
};

/**
 * This class is used to hold a External Column Statistics
 * object(ObOptCatalogColumnStat).
 *
 * An instance of this class keeps a pointer to a External Column Statistics
 * object stored in Statistics Cache and prevent it from being released. As long
 * as the instance is alive, the pointer is always valid (even if the object is
 * removed from the cache).
 *
 */
class ObOptCatalogColumnStatHandle
{
public:
  friend class ObOptCatalogColumnStatCache;
  ObOptCatalogColumnStatHandle() : stat_(nullptr), cache_(nullptr)
  {
  }
  ~ObOptCatalogColumnStatHandle()
  {
    stat_ = nullptr;
    cache_ = nullptr;
  }
  int assign(const ObOptCatalogColumnStatHandle &other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(handle_.assign(other.handle_))) {
      COMMON_LOG(WARN, "fail to assign handle");
      this->stat_ = nullptr;
      this->cache_ = nullptr;
    } else {
      this->stat_ = other.stat_;
      this->cache_ = other.cache_;
    }
    return ret;
  }
  void move_from(ObOptCatalogColumnStatHandle &other)
  {
    this->stat_ = other.stat_;
    this->cache_ = other.cache_;
    this->handle_.move_from(other.handle_);
    other.reset();
  }
  void reset()
  {
    stat_ = nullptr;
    cache_ = nullptr;
    handle_.reset();
  }
  const ObOptCatalogColumnStat *stat_;
  TO_STRING_KV(K(stat_));

private:
  ObOptCatalogColumnStatCache *cache_;
  common::ObKVCacheHandle handle_;
};

} // end of namespace share
} // end of namespace oceanbase

#endif // OCEANBASE_SHARE_STAT_EXTERNAL_OB_OPT_EXTERNAL_COLUMN_STAT_CACHE_