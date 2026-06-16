/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_opt_catalog_column_stat_cache.h"

namespace oceanbase
{
namespace share
{

int ObOptCatalogColumnStatCache::get_row(const ObOptCatalogColumnStat::Key &key,
                                          ObOptCatalogColumnStatHandle &handle)
{
  int ret = OB_SUCCESS;

  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid external column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(get(key, handle.stat_, handle.handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "Fail to get key from row cache. ", K(key), K(ret));
    }
    EVENT_INC(ObStatEventIds::OPT_EXTERNAL_COLUMN_STAT_CACHE_MISS);
  } else {
    handle.cache_ = this;
    EVENT_INC(ObStatEventIds::OPT_EXTERNAL_COLUMN_STAT_CACHE_HIT);
  }
  return ret;
}

int ObOptCatalogColumnStatCache::put_row(const ObOptCatalogColumnStat::Key &key,
                                          const ObOptCatalogColumnStat &value)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid external column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(put(key, value, true /*overwrite*/))) {
    COMMON_LOG(WARN, "put value in cache failed.", K(key), K(value), K(ret));
  }
  return ret;
}

int ObOptCatalogColumnStatCache::put_and_fetch_row(const ObOptCatalogColumnStat::Key &key,
                                                    const ObOptCatalogColumnStat &value,
                                                    ObOptCatalogColumnStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid external column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(put_and_fetch(key, value, handle.stat_, handle.handle_, true /*overwrite*/))) {
    COMMON_LOG(WARN, "Fail to put kvpair to cache.", K(ret));
  }
  return ret;
}

} // end of namespace share
} // end of namespace oceanbase