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

#define USING_LOG_PREFIX SQL_OPT
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

#include "ob_opt_stat_service.h"

namespace oceanbase {
namespace common {

using namespace share::schema;

int ObOptStatService::get_table_stat(const uint64_t tenant_id,
                                     const ObOptTableStat::Key &key,
                                     ObOptTableStat &tstat)
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
    } else if (OB_FAIL(load_table_stat_and_put_cache(tenant_id, key, handle))) {
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

int ObOptStatService::get_table_stat(const uint64_t tenant_id,
                                     const ObOptTableStat::Key &key,
                                     ObOptTableStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(key));
  } else if (OB_FAIL(table_stat_cache_.get_value(key, handle))) {
    // we need to fetch statistics from inner table if it is not yet available from cache
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get table stat from cache failed", K(ret), K(key));
    } else if (OB_FAIL(load_table_stat_and_put_cache(tenant_id, key, handle))) {
      LOG_WARN("load and put cache table stat failed.", K(ret), K(key));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(handle.stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(key));
  }
  return ret;
}

int ObOptStatService::get_column_stat(const uint64_t tenant_id,
                                      const ObOptColumnStat::Key &key,
                                      ObOptColumnStatHandle &handle)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObOptColumnStat::Key*, 1> keys;
  ObSEArray<ObOptColumnStatHandle, 1> handles;
  if (OB_FAIL(keys.push_back(&key))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(get_column_stat(tenant_id, keys, handles))) {
    LOG_WARN("failed to get column stat", K(ret));
  } else if (OB_UNLIKELY(handles.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(handles.count()));
  } else {
    handle = handles.at(0);
  }
  return ret;
}

int ObOptStatService::get_column_stat(const uint64_t tenant_id,
                                      ObIArray<const ObOptColumnStat::Key*> &keys,
                                      ObIArray<ObOptColumnStatHandle> &handles)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObOptColumnStat::Key*, 4> regather_keys;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(keys));
  } else {
    LOG_TRACE("begin get column stat", K(keys));
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      ObOptColumnStatHandle handle;
      if (OB_ISNULL(keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(keys.at(i)));
      } else if (OB_FAIL(column_stat_cache_.get_row(*keys.at(i), handle))) {
        // we need to fetch statistics from inner table if it is not yet available from cache
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get table stat from cache failed", K(ret), KPC(keys.at(i)));
        } else if (OB_FAIL(regather_keys.push_back(keys.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      } else if (NULL == handle.stat_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), KPC(keys.at(i)));
      } else if (OB_FAIL(handles.push_back(handle))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && !regather_keys.empty()) {
      if (OB_FAIL(load_column_stat_and_put_cache(tenant_id, regather_keys, handles))) {
        LOG_WARN("failed to load column stat and put cache", K(ret), K(regather_keys));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObOptStatService::load_table_stat_and_put_cache(const uint64_t tenant_id,
                                                    const ObOptTableStat::Key &key,
                                                    ObOptTableStatHandle &handle)
{
  int ret = OB_SUCCESS;
  ObOptTableStat tstat;
  ObSEArray<ObOptTableStat, 4> all_part_stats;
  bool added = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table statistics service is not initialized. ", K(ret), K(key));
  } else if (OB_FAIL(sql_service_.fetch_table_stat(tenant_id, key, all_part_stats))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fetch table stat failed. ", K(ret), K(key));
    } else {
      // it's not guaranteed that table stat exists.
      tstat.reset();
      ret = OB_SUCCESS;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_part_stats.count(); ++i) {
      ObOptTableStat::Key tmp_key(tenant_id,
                                  all_part_stats.at(i).get_table_id(),
                                  all_part_stats.at(i).get_partition_id());
      ObOptTableStatHandle hd;
      if (OB_FAIL(table_stat_cache_.put_and_fetch_value(tmp_key, all_part_stats.at(i), hd))) {
        LOG_WARN("failed to put and fetch table stat", K(ret));
      } else if (tmp_key == key) {
        handle = hd;
        added = true;
      }
    }
  }

  if (OB_SUCC(ret) && !added) {
    tstat.set_table_id(key.table_id_);
    tstat.set_partition_id(key.partition_id_);
    if (OB_FAIL(table_stat_cache_.put_and_fetch_value(key, tstat, handle))) {
      LOG_WARN("put and fetch table stat failed.", K(ret), K(key));
    }
  }
  return ret;
}

int ObOptStatService::load_column_stat_and_put_cache(const uint64_t tenant_id,
                                                     ObIArray<const ObOptColumnStat::Key*> &keys,
                                                     ObIArray<ObOptColumnStatHandle> &handles)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena(ObModIds::OB_SQL_PARSER);
  LOG_TRACE("begin load column stat and put cache", K(keys));
  ObSEArray<ObOptKeyColumnStat, 4> key_column_stats;
  // generate new entrys and load from global statistics table and store it in cache.
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("col stat service has not been initialized.", K(ret));
  } else if (keys.empty()) {
    //do nothing
  } else if (init_key_column_stats(arena, keys, key_column_stats)) {
    LOG_WARN("failed to init key column stats", K(ret));
  } else if (OB_FAIL(sql_service_.fetch_column_stat(tenant_id, arena, key_column_stats))) {
    LOG_WARN("failed to fetch column stat", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < key_column_stats.count(); ++i) {
      ObOptColumnStatHandle handle;
      if (OB_ISNULL(key_column_stats.at(i).key_) ||
          OB_ISNULL(key_column_stats.at(i).stat_) ||
          OB_UNLIKELY(key_column_stats.at(i).key_->table_id_ != key_column_stats.at(i).stat_->get_table_id() ||
                      key_column_stats.at(i).key_->partition_id_ != key_column_stats.at(i).stat_->get_partition_id() ||
                      key_column_stats.at(i).key_->column_id_ != key_column_stats.at(i).stat_->get_column_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(key_column_stats), K(i), K(ret));
      } else if (OB_FAIL(column_stat_cache_.put_and_fetch_row(*key_column_stats.at(i).key_,
                                                              *key_column_stats.at(i).stat_,
                                                              handle))) {
        LOG_WARN("puts column stat into cache failed.", K(ret));
      } else if (OB_FAIL(handles.push_back(handle))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        key_column_stats.at(i).stat_->~ObOptColumnStat();
        key_column_stats.at(i).stat_ = NULL;
      }
    }
  }
  return ret;
}

int ObOptStatService::init(common::ObMySQLProxy *proxy, ObServerConfig *config)
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
  } else if (OB_FAIL(table_stat_cache_.init(
      "opt_table_stat_cache", DEFAULT_TAB_STAT_CACHE_PRIORITY))) {
    LOG_WARN("fail to init table cache.", K(ret));
  } else if (OB_FAIL(column_stat_cache_.init("opt_column_stat_cache",
                                             DEFAULT_COL_STAT_CACHE_PRIORITY))) {
    LOG_WARN("fail to init table cache.", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObOptStatService::erase_table_stat(const ObOptTableStat::Key &key)
{
  return table_stat_cache_.erase(key);
}

int ObOptStatService::erase_column_stat(const ObOptColumnStat::Key &key)
{
  return column_stat_cache_.erase(key);
}

int ObOptStatService::init_key_column_stats(ObIAllocator &allocator,
                                            ObIArray<const ObOptColumnStat::Key*> &keys,
                                            ObIArray<ObOptKeyColumnStat> &key_column_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    void *ptr = NULL;
    ObOptKeyColumnStat tmp_key_col_stat;
    if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptColumnStat)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(ptr));
    } else {
      ObOptColumnStat *tmp_col_stat = new (ptr) ObOptColumnStat();
      tmp_col_stat->set_table_id(keys.at(i)->table_id_);
      tmp_col_stat->set_partition_id(keys.at(i)->partition_id_);
      tmp_col_stat->set_column_id(keys.at(i)->column_id_);
      tmp_key_col_stat.key_ = keys.at(i);
      tmp_key_col_stat.stat_ = tmp_col_stat;
      if (OB_FAIL(key_column_stats.push_back(tmp_key_col_stat))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

}
}
