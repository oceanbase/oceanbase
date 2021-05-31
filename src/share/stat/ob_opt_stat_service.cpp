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

#define USING_LOG_PREFIX COMMON
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

#include "ob_opt_stat_service.h"

namespace oceanbase {
namespace common {

using namespace share::schema;
static bool is_non_stat_table(const int64_t table_id)
{
  const uint64_t id = common::extract_pure_id(table_id);
  return (id < share::OB_ALL_META_TABLE_TID || id == share::OB_ALL_COLUMN_STATISTIC_TID ||
          id == share::OB_ALL_TABLE_STAT_TID || id == share::OB_ALL_COLUMN_STAT_TID ||
          id == share::OB_ALL_HISTOGRAM_STAT_TID);
}

int ObOptStatService::get_table_stat(const ObOptTableStat::Key& key, ObOptTableStat& tstat)
{
  int ret = OB_SUCCESS;
  ObOptTableStatHandle handle;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(key));
  } else if (OB_FAIL(table_stat_cache_.get_value(key, handle))) {
    // we need to fetch statistics from inner table if it is not yet available from cache
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get table stat from cache failed", K(ret), K(key));
    } else if (OB_FAIL(load_table_stat_and_put_cache(key, handle))) {
      LOG_WARN("load and put cache table stat failed.", K(ret), K(key));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(handle.stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(key));
  } else {
    tstat = *handle.stat_;
  }
  return ret;
}

int ObOptStatService::get_column_stat(const ObOptColumnStat::Key& key, ObOptColumnStatHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(key));
  } else if (OB_FAIL(column_stat_cache_.get_row(key, handle))) {
    // we need to fetch statistics from inner table if it is not yet available from cache
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get table stat from cache failed", K(ret), K(key));
    } else if (OB_FAIL(load_column_stat_and_put_cache(key, handle))) {
      LOG_WARN("failed to load column stat and put cache", K(ret), K(key));
    }
  }

  if (OB_SUCC(ret) && NULL == handle.stat_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(key));
  }
  return ret;
}

int ObOptStatService::load_table_stat_and_put_cache(const ObOptTableStat::Key& key, ObOptTableStatHandle& handle)
{
  int ret = OB_SUCCESS;
  ObOptTableStat tstat;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table statistics service is not initialized. ", K(ret), K(key));
  } else if (common::is_non_stat_table(key.table_id_)) {
    tstat.reset();
  } else if (OB_FAIL(sql_service_.fetch_table_stat(key, tstat))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fetch table stat failed. ", K(ret), K(key));
    } else {
      // it's not guaranteed that table stat exists.
      tstat.reset();
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(table_stat_cache_.put_and_fetch_value(key, tstat, handle))) {
    LOG_WARN("put and fetch table stat failed.", K(ret), K(key));
  }
  return ret;
}

int ObOptStatService::load_column_stat_and_put_cache(const ObOptColumnStat::Key& key, ObOptColumnStatHandle& handle)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena(ObModIds::OB_BUFFER);
  // generate new entry and load from global statistics table and store it in cache.
  common::ObOptColumnStat new_entry(arena);
  new_entry.set_table_id(key.table_id_);
  new_entry.set_partition_id(key.partition_id_);
  new_entry.set_column_id(key.column_id_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("col stat service has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(key), K(ret));
  } else if (!new_entry.is_writable()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("ObOptColumnStat Object new_entry is invalid.", K(ret));
  } else if (common::is_non_stat_table(key.table_id_)) {
    if (OB_FAIL(column_stat_cache_.put_and_fetch_row(key, new_entry, handle))) {
      LOG_WARN("puts column stat into cache failed.", K(ret));
    }
  } else if (OB_FAIL(load_column_stat_and_put_cache(key, new_entry, handle))) {
    LOG_WARN("load from internal table and put in cache failed.", K(ret));
  }
  return ret;
}

int ObOptStatService::load_column_stat_and_put_cache(
    const ObOptColumnStat::Key& key, ObOptColumnStat& new_entry, ObOptColumnStatHandle& handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("col stat service has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(key), K(ret));
  } else if (OB_FAIL(sql_service_.fetch_column_stat(key, new_entry))) {
    if (OB_ENTRY_NOT_EXIST != ret && OB_TABLE_NOT_EXIST != ret) {
      LOG_WARN("fetch data from __all_column_stat failed.", K(ret));
    } else if (OB_FAIL(column_stat_cache_.put_and_fetch_row(key, new_entry, handle))) {
      LOG_WARN("puts column stat into cache failed.", K(ret));
    }
  } else if (OB_FAIL(column_stat_cache_.put_and_fetch_row(key, new_entry, handle))) {
    LOG_WARN("puts column stat into cache failed.", K(ret));
  }
  return ret;
}

int ObOptStatService::init(common::ObMySQLProxy* proxy, ObServerConfig* config)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("col stat service has been initialized.", K(ret));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid proxy object.", KP(proxy), K(ret));
  } else if (OB_FAIL(sql_service_.init(proxy, config))) {
    LOG_WARN("fail to init sql_service_.", K(ret));
  } else if (OB_FAIL(table_stat_cache_.init("opt_table_stat_cache", DEFAULT_TAB_STAT_CACHE_PRIORITY))) {
    LOG_WARN("fail to init table cache.", K(ret));
  } else if (OB_FAIL(column_stat_cache_.init("opt_column_stat_cache", DEFAULT_COL_STAT_CACHE_PRIORITY))) {
    LOG_WARN("fail to init table cache.", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
