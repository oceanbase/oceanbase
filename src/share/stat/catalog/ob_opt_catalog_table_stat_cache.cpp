/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "share/stat/catalog/ob_opt_catalog_table_stat_cache.h"

namespace oceanbase
{
namespace share
{

int ObOptCatalogTableStatCache::get_value(const ObOptCatalogTableStat::Key &key,
                                           ObOptCatalogTableStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get(key, handle.stat_, handle.handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "fail to get value from cache", K(ret), K(key));
    }
    EVENT_INC(ObStatEventIds::OPT_EXTERNAL_TABLE_STAT_CACHE_MISS);
  } else {
    handle.cache_ = this;
    EVENT_INC(ObStatEventIds::OPT_EXTERNAL_TABLE_STAT_CACHE_HIT);
  }
  return ret;
}

int ObOptCatalogTableStatCache::put_value(const ObOptCatalogTableStat::Key &key,
                                           const ObOptCatalogTableStat &value)
{
  return put(key, value, true /* overwrite */);
}

int ObOptCatalogTableStatCache::put_and_fetch_value(const ObOptCatalogTableStat::Key &key,
                                                     const ObOptCatalogTableStat &value,
                                                     ObOptCatalogTableStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(put_and_fetch(key, value, handle.stat_, handle.handle_, true /* overwrite */))) {
    COMMON_LOG(WARN, "failed to put and fetch value", K(ret), K(key));
  } else if (OB_ISNULL(handle.stat_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "catalog table stat is null", K(ret));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase