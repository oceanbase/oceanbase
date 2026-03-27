/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/stat/ob_opt_ds_stat_cache.h"

namespace oceanbase {
namespace common {

/**
 * @return OB_SUCCESS         if value corresponding to the key is successfully fetched
 *         OB_ENTRY_NOT_EXIST if values is not available from the cache
 *         other error codes  if unexpected errors occurred
 */
int ObOptDSStatCache::get_value(const ObOptDSStat::Key &key, ObOptDSStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get(key, handle.stat_, handle.handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "fail to get value from cache", K(ret), K(key));
    }
    EVENT_INC(ObStatEventIds::OPT_DS_STAT_CACHE_MISS);
  } else {
    handle.cache_ = this;
    EVENT_INC(ObStatEventIds::OPT_DS_STAT_CACHE_HIT);
  }
  return ret;
}

int ObOptDSStatCache::put_value(const ObOptDSStat::Key &key, const ObOptDSStat &value)
{
  return put(key, value, true /* overwrite */);
}

int ObOptDSStatCache::put_and_fetch_value(const ObOptDSStat::Key &key,
                                          const ObOptDSStat &value,
                                          ObOptDSStatHandle &handle)
{
  return put_and_fetch(key, value, handle.stat_, handle.handle_, true /* overwrite */ );
}

}
}
