/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/stat/ob_opt_column_stat_cache.h"

namespace oceanbase
{
namespace common
{

int ObOptColumnStatCache::get_row(const ObOptColumnStat::Key &key, ObOptColumnStatHandle &handle)
{
  int ret = OB_SUCCESS;

  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(get(key, handle.stat_, handle.handle_))){
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "Fail to get key from row cache. ", K(key), K(ret));
    }
    EVENT_INC(ObStatEventIds::OPT_COLUMN_STAT_CACHE_MISS);
  } else {
    handle.cache_ = this;
    EVENT_INC(ObStatEventIds::OPT_COLUMN_STAT_CACHE_HIT);
  }
  return ret;
}

int ObOptColumnStatCache::put_row(const ObOptColumnStat::Key &key, const ObOptColumnStat &value)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(put(key, value, true/*overwrite*/))) {
    COMMON_LOG(WARN, "put value in cache failed.", K(key), K(value), K(ret));
  }
  return ret;
}

int ObOptColumnStatCache::put_and_fetch_row(const ObOptColumnStat::Key &key,
                                            const ObOptColumnStat &value,
                                            ObOptColumnStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
     ret = OB_INVALID_ARGUMENT;
     COMMON_LOG(WARN, "invalid column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(put_and_fetch(key, value, handle.stat_, handle.handle_, true /*overwrite*/))) {
     COMMON_LOG(WARN, "Fail to put kvpair to cache.", K(ret));
  }
  return ret;
}

} // end of namespace common
} // end of namespace oceanbase


