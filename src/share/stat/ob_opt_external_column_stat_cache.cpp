/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/stat/ob_opt_external_column_stat_cache.h"

namespace oceanbase {
namespace share {

int ObOptExternalColumnStatCache::get_row(
    const ObOptExternalColumnStat::Key &key,
    ObOptExternalColumnStatHandle &handle) {
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

int ObOptExternalColumnStatCache::put_row(
    const ObOptExternalColumnStat::Key &key,
    const ObOptExternalColumnStat &value) {
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid external column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(put(key, value, true /*overwrite*/))) {
    COMMON_LOG(WARN, "put value in cache failed.", K(key), K(value), K(ret));
  }
  return ret;
}

int ObOptExternalColumnStatCache::put_and_fetch_row(
    const ObOptExternalColumnStat::Key &key,
    const ObOptExternalColumnStat &value,
    ObOptExternalColumnStatHandle &handle) {
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid external column stat cache key.", K(key), K(ret));
  } else if (OB_FAIL(put_and_fetch(key, value, handle.stat_, handle.handle_,
                                   true /*overwrite*/))) {
    COMMON_LOG(WARN, "Fail to put kvpair to cache.", K(ret));
  }
  return ret;
}

} // end of namespace share
} // end of namespace oceanbase