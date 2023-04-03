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

#include "share/stat/ob_opt_table_stat_cache.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase {
namespace common {

/**
 * @return OB_SUCCESS         if value corresponding to the key is successfully fetched
 *         OB_ENTRY_NOT_EXIST if values is not available from the cache
 *         other error codes  if unexpected errors occurred
 */
int ObOptTableStatCache::get_value(const ObOptTableStat::Key &key, ObOptTableStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get(key, handle.stat_, handle.handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "fail to get value from cache", K(ret), K(key));
    }
    EVENT_INC(ObStatEventIds::OPT_TABLE_STAT_CACHE_MISS);
  } else {
    handle.cache_ = this;
    EVENT_INC(ObStatEventIds::OPT_TABLE_STAT_CACHE_HIT);
  }
  return ret;
}

int ObOptTableStatCache::put_value(const ObOptTableStat::Key &key, const ObOptTableStat &value)
{
  return put(key, value, true /* overwrite */);
}

int ObOptTableStatCache::put_and_fetch_value(const ObOptTableStat::Key &key,
                                             const ObOptTableStat &value,
                                             ObOptTableStatHandle &handle)
{
  return put_and_fetch(key, value, handle.stat_, handle.handle_, true /* overwrite */ );
}

}
}
