/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_STAT_EXTERNAL_OB_OPT_EXTERNAL_TABLE_STAT_CACHE_
#define OCEANBASE_SHARE_STAT_EXTERNAL_OB_OPT_EXTERNAL_TABLE_STAT_CACHE_

#include "share/cache/ob_kv_storecache.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"

namespace oceanbase
{
namespace share
{

struct ObOptCatalogTableStatHandle;

class ObOptCatalogTableStatCache
    : public common::ObKVCache<ObOptCatalogTableStat::Key, ObOptCatalogTableStat>
{
public:
  int get_value(const ObOptCatalogTableStat::Key &key, ObOptCatalogTableStatHandle &handle);
  int put_value(const ObOptCatalogTableStat::Key &key, const ObOptCatalogTableStat &value);
  int put_and_fetch_value(const ObOptCatalogTableStat::Key &key,
                          const ObOptCatalogTableStat &value,
                          ObOptCatalogTableStatHandle &handle);
};

struct ObOptCatalogTableStatHandle
{
  const ObOptCatalogTableStat *stat_;
  ObOptCatalogTableStatCache *cache_;
  common::ObKVCacheHandle handle_;

  ObOptCatalogTableStatHandle() : stat_(nullptr), cache_(nullptr), handle_()
  {
  }
  ~ObOptCatalogTableStatHandle()
  {
    stat_ = nullptr;
    cache_ = nullptr;
  }
  void move_from(ObOptCatalogTableStatHandle &other)
  {
    stat_ = other.stat_;
    cache_ = other.cache_;
    handle_.move_from(other.handle_);
    other.reset();
  }
  int assign(const ObOptCatalogTableStatHandle &other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(handle_.assign(other.handle_))) {
      COMMON_LOG(WARN, "fail to assign kv cache handle", K(ret));
      reset();
    } else {
      this->stat_ = other.stat_;
      this->cache_ = other.cache_;
    }
    return ret;
  }
  void reset()
  {
    stat_ = nullptr;
    cache_ = nullptr;
    handle_.reset();
  }
  TO_STRING_KV(K(stat_));
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_STAT_EXTERNAL_OB_OPT_EXTERNAL_TABLE_STAT_CACHE_