/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "src/share/interrupt/ob_interrupt_rpc_proxy.h"
#include "ob_opt_stat_service.h"
#include "sql/ob_sql_context.h"
#include "share/catalog/ob_cached_catalog_meta_getter.h"
#include "share/stat/ob_opt_external_table_stat_builder.h"
#include "share/stat/ob_opt_external_column_stat_builder.h"
#include "lib/time/ob_time_utility.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "storage/ob_locality_manager.h"
#include "share/ob_srv_rpc_proxy.h"
#include "src/pl/sys_package/ob_dbms_stats.h"

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

int ObOptStatService::batch_get_table_stats(const uint64_t tenant_id,
                                            ObIArray<const ObOptTableStat::Key *> &keys,
                                            ObIArray<ObOptTableStatHandle> &handles)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObOptTableStat::Key*, 4> regather_keys;
  ObSEArray<int64_t, 4> regather_handles_indices;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret));
  } else {
    LOG_TRACE("begin get table stat", K(keys));
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    ObOptTableStatHandle handle;
      if (OB_ISNULL(keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(keys.at(i)));
      } else if (OB_FAIL(table_stat_cache_.get_value(*keys.at(i), handle))) {
        // we need to fetch statistics from inner table if it is not yet available from cache
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get table stat from cache failed", K(ret), KPC(keys.at(i)));
        } else if (OB_FAIL(regather_keys.push_back(keys.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(handles.push_back(handle))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(regather_handles_indices.push_back(i))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else if (NULL == handle.stat_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), KPC(keys.at(i)));
      } else if (OB_FAIL(handles.push_back(handle))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    }

    if (OB_SUCC(ret) && !regather_keys.empty()) {
      if (OB_FAIL(batch_load_table_stats_and_put_cache(tenant_id, regather_keys, handles, regather_handles_indices))) {
        LOG_WARN("failed to load column stat and put cache", K(ret), K(regather_keys));
      } else {/*do nothing*/}
    }
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
    handle.move_from(handles.at(0));
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
    if (OB_ENTRY_NOT_EXIST != ret &&
        !is_sys_table(key.table_id_) &&
        !is_virtual_table(key.table_id_)) {//sys table and virtual table failed, use default
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
        handle.move_from(hd);
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

int ObOptStatService::batch_load_table_stats_and_put_cache(const uint64_t tenant_id,
                                                           ObIArray<const ObOptTableStat::Key *> &keys,
                                                           ObIArray<ObOptTableStatHandle> &handles,
                                                           ObIArray<int64_t> &regather_handles_indices)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptTableStat*, 4> all_part_stats;
  ObArenaAllocator arena("ObOptTblStatGet", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);

  bool added = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table statistics service is not initialized. ", K(ret));
  } else if (keys.empty()) {
    //do nothing
  } else if (OB_FAIL(init_table_stats(arena, keys, all_part_stats))) {
    LOG_WARN("failed to init table stats", K(ret));
  } else if (OB_FAIL(sql_service_.fetch_table_stat(tenant_id, keys, all_part_stats))) {
    if (OB_ENTRY_NOT_EXIST != ret &&
        !is_sys_table(keys.at(0)->table_id_) &&
        !is_virtual_table(keys.at(0)->table_id_)) {//sys table and virtual table failed, use default
      LOG_WARN("fetch table stat failed. ", K(ret));
    } else {
      // it's not guaranteed that table stat exists.
      LOG_WARN("table statistics not existed, use default stats instead. ", K(ret));
      ret = OB_SUCCESS;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_part_stats.count(); ++i) {
    int64_t handle_index = regather_handles_indices.at(i);
    if (OB_ISNULL(keys.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(all_part_stats.at(i)) ||
               OB_UNLIKELY(all_part_stats.at(i)->get_table_id() != keys.at(i)->table_id_ ||
                           all_part_stats.at(i)->get_partition_id() != keys.at(i)->partition_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(all_part_stats), K(i), K(ret));
    } else if (OB_FAIL(table_stat_cache_.put_and_fetch_value(*keys.at(i), *all_part_stats.at(i), handles.at(handle_index)))) {
      LOG_WARN("failed to put and fetch table stat", K(ret));
    }
  }

  return ret;
}

int ObOptStatService::load_column_stat_and_put_cache(const uint64_t tenant_id,
                                                     ObIArray<const ObOptColumnStat::Key*> &keys,
                                                     ObIArray<ObOptColumnStatHandle> &handles)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena("ObOptColStatGet", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  LOG_TRACE("begin load column stat and put cache", K(keys));
  ObSEArray<ObOptKeyColumnStat, 4> key_column_stats;
  // generate new entrys and load from global statistics table and store it in cache.
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("col stat service has not been initialized.", K(ret));
  } else if (keys.empty()) {
    //do nothing
  } else if (OB_FAIL(init_key_column_stats(arena, keys, key_column_stats))) {
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
  } else if (OB_FAIL(ds_stat_cache_.init("opt_ds_stat_cache",
                                          DEFAULT_DS_STAT_CACHE_PRIORITY))) {
    LOG_WARN("fail to init table cache.", K(ret));
  } else if (OB_FAIL(system_stat_cache_.init("opt_system_stat_cache",
                                             DEFAULT_SYSTEM_STAT_CACHE_PRIORITY))) {
    LOG_WARN("fail to init system stat cache.", K(ret));
  } else if (OB_FAIL(external_table_stat_cache_.init("opt_external_table_stat_cache",
                                                     DEFAULT_TAB_STAT_CACHE_PRIORITY))) {
    LOG_WARN("fail to init external table stat cache.", K(ret));
  } else if (OB_FAIL(external_column_stat_cache_.init("opt_external_column_stat_cache",
                                                      DEFAULT_COL_STAT_CACHE_PRIORITY))) {
    LOG_WARN("fail to init external column stat cache.", K(ret));
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

int ObOptStatService::init_table_stats(ObIAllocator &allocator,
                                       const ObIArray<const ObOptTableStat::Key *> &keys,
                                       ObIArray<ObOptTableStat *> &table_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    void *ptr = NULL;
    if (OB_ISNULL(keys.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexepected null", K(ret), K(keys), K(i));
    } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptTableStat)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(ptr));
    } else {
      ObOptTableStat *tmp_table_stat = new (ptr) ObOptTableStat();
      tmp_table_stat->set_table_id(keys.at(i)->table_id_);
      tmp_table_stat->set_partition_id(keys.at(i)->partition_id_);
      if (OB_FAIL(table_stats.push_back(tmp_table_stat))) {
        LOG_WARN("failed to push back", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObOptStatService::get_table_rowcnt(const uint64_t tenant_id,
                                       const uint64_t table_id,
                                       const ObIArray<ObTabletID> &all_tablet_ids,
                                       const ObIArray<share::ObLSID> &all_ls_ids,
                                       int64_t &table_rowcnt)
{
  int ret = OB_SUCCESS;
  table_rowcnt = 0;
  if (OB_UNLIKELY(all_tablet_ids.count() != all_ls_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error");
  } else {
    ObSEArray<ObTabletID, 16> reload_tablet_ids;
    ObSEArray<share::ObLSID, 16> reload_ls_ids;
    storage::ObTabletStat unused_tablet_stat;
    share::schema::ObTableModeFlag unused_mode;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_tablet_ids.count(); ++i) {
      ObOptTableStat::Key key(tenant_id, table_id, all_tablet_ids.at(i).id());
      ObOptTableStatHandle handle;
      if (OB_FAIL(table_stat_cache_.get_value(key, handle))) {
        // we need to fetch statistics from inner table if it is not yet available from cache
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get table stat from cache failed", K(ret), K(key));
        } else if (OB_FAIL(reload_tablet_ids.push_back(all_tablet_ids.at(i))) ||
                   OB_FAIL(reload_ls_ids.push_back(all_ls_ids.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
         }
      } else if (OB_ISNULL(handle.stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(key));
       //check is stale
      } else if (handle.stat_->is_arrived_expired_time()) {
        if (OB_FAIL(reload_tablet_ids.push_back(all_tablet_ids.at(i))) ||
            OB_FAIL(reload_ls_ids.push_back(all_ls_ids.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      } else {
        storage::ObTenantTabletStatMgr *stat_mgr = MTL(storage::ObTenantTabletStatMgr *);
        storage::ObTabletStat tablet_stat;
        int64_t tablet_row_cnt = 0;
        //try check the latest tablet stat from stroage
        if (stat_mgr != NULL) {
          if (OB_FAIL(stat_mgr->get_latest_tablet_stat(all_ls_ids.at(i), all_tablet_ids.at(i), tablet_stat, unused_tablet_stat, unused_mode))) {
            if (OB_HASH_NOT_EXIST != ret) {
              LOG_WARN("failed to get latest tablet stat", K(ret), K(all_ls_ids.at(i)), K(all_tablet_ids.at(i)));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }
        LOG_TRACE("cache stat compare", KPC(handle.stat_), K(tablet_stat));
        tablet_row_cnt = tablet_stat.insert_row_cnt_ > tablet_stat.delete_row_cnt_ ?
                          tablet_stat.insert_row_cnt_ - tablet_stat.delete_row_cnt_ : 0;
        if (handle.stat_->get_row_count() < tablet_row_cnt) {
          if (OB_FAIL(reload_tablet_ids.push_back(all_tablet_ids.at(i))) ||
              OB_FAIL(reload_ls_ids.push_back(all_ls_ids.at(i)))) {
            LOG_WARN("failed to push back", K(ret));
          } else {/*do nothing*/}
        } else {
          table_rowcnt += handle.stat_->get_row_count();
        }
      }
    }
    if (OB_SUCC(ret) && !reload_tablet_ids.empty()) {
      int64_t reload_row_cnt = 0;
      if (OB_FAIL(load_table_rowcnt_and_put_cache(tenant_id, table_id, reload_tablet_ids,
                                                  reload_ls_ids, reload_row_cnt))) {
        LOG_WARN("load and put cache table stat failed.", K(ret));
      } else {
        table_rowcnt += reload_row_cnt;
      }
    }
    LOG_TRACE("Succeed to get table rowcnt", K(table_id), K(table_rowcnt),
                                             K(all_tablet_ids), K(reload_tablet_ids));
  }
  return ret;
}

int ObOptStatService::load_table_rowcnt_and_put_cache(const uint64_t tenant_id,
                                                      const uint64_t table_id,
                                                      const ObIArray<ObTabletID> &all_tablet_ids,
                                                      const ObIArray<share::ObLSID> &all_ls_ids,
                                                      int64_t &table_rowcnt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptTableStat, 4> tstats;
  table_rowcnt = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table statistics service is not initialized. ", K(ret));
  } else if (OB_FAIL(sql_service_.fetch_table_rowcnt(tenant_id, table_id,
                                                     all_tablet_ids, all_ls_ids,
                                                     tstats))) {
    if (!is_sys_table(table_id) && !is_virtual_table(table_id)) {//sys table and virtual table failed, use default
      LOG_WARN("failed to fetch table rowcnt ", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tstats.count(); ++i) {
      ObOptTableStat::Key key(tenant_id, table_id, tstats.at(i).get_tablet_id());
      ObOptTableStatHandle handle;
      if (OB_FAIL(table_stat_cache_.put_and_fetch_value(key, tstats.at(i), handle))) {
        LOG_WARN("put and fetch table stat failed.", K(ret), K(key));
      } else {
        table_rowcnt += tstats.at(i).get_row_count();
      }
    }
  }
  return ret;
}

int ObOptStatService::get_ds_stat(const ObOptDSStat::Key &key, ObOptDSStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(key));
  } else if (OB_FAIL(ds_stat_cache_.get_value(key, handle))) {
    // we need to fetch statistics from inner table if it is not yet available from cache
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get ds stat from cache failed", K(ret), K(key));
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

int ObOptStatService::add_ds_stat_cache(const ObOptDSStat::Key &key,
                                        const ObOptDSStat &value,
                                        ObOptDSStatHandle &ds_stat_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(ds_stat_cache_.put_and_fetch_value(key, value, ds_stat_handle))) {
    LOG_WARN("failed to put value", K(ret));
  }
  return ret;
}

int ObOptStatService::erase_ds_stat(const ObOptDSStat::Key &key)
{
  return ds_stat_cache_.erase(key);
}

int ObOptStatService::get_system_stat(const uint64_t tenant_id,
                                      const ObOptSystemStat::Key &key,
                                      ObOptSystemStat &stat)
{
  int ret = OB_SUCCESS;
  ObOptSystemStatHandle handle;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(key));
  } else if (OB_FAIL(system_stat_cache_.get_value(key, handle))) {
    // we need to fetch statistics from inner table if it is not yet available from cache
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get system stat from cache failed", K(ret), K(key));
    } else if (OB_FAIL(load_system_stat_and_put_cache(tenant_id, key, handle))) {
      LOG_WARN("load and put cache system stat failed.", K(ret), K(key));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(handle.stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(key));
  } else {
    stat = *handle.stat_;
  }
  return ret;
}

int ObOptStatService::load_system_stat_and_put_cache(const uint64_t tenant_id,
                                                    const ObOptSystemStat::Key &key,
                                                    ObOptSystemStatHandle &handle)
{
  int ret = OB_SUCCESS;
  ObOptSystemStat stat;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(key));
  } else if (OB_FAIL(system_stat_cache_.put_and_fetch_value(key, stat, handle))) {
    LOG_WARN("put and fetch table stat failed.", K(ret), K(key));
  } else if (OB_FAIL(sql_service_.fetch_system_stat(tenant_id, key, stat))) {
    system_stat_cache_.erase(key);
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fetch system stat failed. ", K(ret), K(key));
    } else {
      // it's not guaranteed that system stat exists.
      stat.reset();
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(system_stat_cache_.put_and_fetch_value(key, stat, handle))) {
    LOG_WARN("put and fetch table stat failed.", K(ret), K(key));
  }
  return ret;
}

int ObOptStatService::erase_system_stat(const ObOptSystemStat::Key &key)
{
  return system_stat_cache_.erase(key);
}

int ObOptStatService::load_external_table_stat_and_put_cache(const uint64_t tenant_id,
  const uint64_t catalog_id,
  const uint64_t table_id,
  sql::ObSqlSchemaGuard &schema_guard,
  ObIArray<ObString> &key_partition_names,
  const ObIArray<ObString> &all_partition_names,
  ObOptExternalTableStatHandle &handle,
  ObIArray<ObOptExternalColumnStatHandle> &column_handles)
{
  int ret = OB_SUCCESS;
  const share::ObILakeTableMetadata *lake_table_metadata = nullptr;
  const share::schema::ObTableSchema *table_schema = nullptr;
  ObSEArray<ObString, 16> column_names;
  ObOptExternalTableStat *external_table_stat = nullptr;
  ObSEArray<ObOptExternalColumnStat *, 16> external_column_stats;
  ObArenaAllocator stat_allocator("ExtStatFetch", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  if (OB_FAIL(schema_guard.get_lake_table_metadata(tenant_id, table_id, lake_table_metadata))) {
    LOG_WARN("failed to get lake table metadata", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(extract_column_names_from_table_schema(*table_schema, column_names))) {
    LOG_WARN("failed to extract column names from table schema", K(ret));
  } else {
    if (lake_table_metadata->get_format_type() != ObLakeTableFormat::ODPS) {
      if (OB_FAIL(fetch_external_table_statistics_from_catalog(stat_allocator,
                                                                      schema_guard,
                                                                      lake_table_metadata,
                                                                      key_partition_names,
                                                                      column_names,
                                                                      external_table_stat,
                                                                      external_column_stats))) {
        LOG_WARN("failed to fetch external table statistics from catalog", K(ret));
      }
    } else {
      // odps tunnel 比较特殊不能获得所有分区rowcount api限制 需要通过选中分区缩放，其他通过全局分区缩放
      // 如果只有一个分区那么all_partition_names 就是key_partition_names
      if (OB_FAIL(fetch_external_table_statistics_from_catalog(stat_allocator,
                                                                schema_guard,
                                                                lake_table_metadata,
                                                                all_partition_names,
                                                                column_names,
                                                                external_table_stat,
                                                                external_column_stats))) {
        LOG_WARN("failed to fetch external table statistics from catalog", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(put_external_table_stats_to_cache(tenant_id,
                                                        catalog_id,
                                                        table_id,
                                                        key_partition_names,
                                                        external_table_stat,
                                                        external_column_stats,
                                                        handle,
                                                        column_handles))) {
      LOG_WARN("failed to put external table stats to cache", K(ret));
    }
  }
  if (OB_NOT_NULL(external_table_stat)) {
    external_table_stat->~ObOptExternalTableStat();
  }
  if (!external_column_stats.empty()) {
    for (int64_t i = 0; i < external_column_stats.count(); ++i) {
      external_column_stats.at(i)->~ObOptExternalColumnStat();
    }
  }
  return ret;
}

static int broadcast_evict_opt_stat_kvcache(const uint64_t tenant_id,
                                            ObIArray<share::ObServerLocality> & all_server_arr,
                                            int64_t timeout,
                                            obrpc::ObUpdateStatCacheArg &arg)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < all_server_arr.count(); i++) {
    if (!all_server_arr.at(i).is_active()
        || share::ObServerStatus::OB_SERVER_ACTIVE != all_server_arr.at(i).get_server_status()
        || 0 != all_server_arr.at(i).get_server_stop_time()) {
    //server may not serving
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = GCTX.srv_rpc_proxy_->to(all_server_arr.at(i).get_addr())
                                                        .timeout(timeout)
                                                        .by(tenant_id)
                                                        .update_local_stat_cache(arg))) {
        LOG_WARN("failed to evict table stat cache caused by unknow error",
                 K(tmp_ret), K(all_server_arr.at(i).get_addr()), K(arg));
      }
    }
  }
  return ret;
}

int ObOptStatService::extract_column_names_from_table_schema(const share::schema::ObTableSchema &table_schema,
                                                             ObIArray<ObString> &column_names)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = table_schema.get_column_count();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    const share::schema::ObColumnSchemaV2 *column_schema = table_schema.get_column_schema_by_idx(i);
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(ret), K(i));
    } else if (column_schema->is_hidden() || column_schema->is_unused()) {
      // Skip hidden and unused columns
    } else if (OB_FAIL(column_names.push_back(column_schema->get_column_name_str()))) {
      LOG_WARN("failed to add column name", K(ret), K(column_schema->get_column_name_str()));
    }
  }
  LOG_TRACE("extracted column names from table schema", K(column_count), K(column_names.count()));
  return ret;
}

int ObOptStatService::fetch_external_table_statistics_from_catalog(ObIAllocator &allocator,
                                                                   sql::ObSqlSchemaGuard &schema_guard,
                                                                   const share::ObILakeTableMetadata *lake_table_metadata,
                                                                   const ObIArray<ObString> &partition_names,
                                                                   const ObIArray<ObString> &column_names,
                                                                   ObOptExternalTableStat *&external_table_stat,
                                                                   ObIArray<ObOptExternalColumnStat *> &external_column_stats)
{
  int ret = OB_SUCCESS;
  external_table_stat = nullptr;

  if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret));
  } else if (column_names.empty()) {
    LOG_TRACE("no columns to fetch statistics for");
  } else {
    // Construct cached catalog meta getter
    share::schema::ObSchemaGetterGuard *schema_getter_guard = schema_guard.get_schema_guard();
    if (OB_ISNULL(schema_getter_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema getter guard is null", K(ret));
    } else {
      // Use the passed-in allocator instead of creating a local one
      share::ObCachedCatalogMetaGetter catalog_getter(*schema_getter_guard, allocator);
      // Fetch table and column statistics via catalog
      if (OB_FAIL(catalog_getter.fetch_table_statistics(allocator,
                                                        schema_guard,
                                                        lake_table_metadata,
                                                        partition_names,
                                                        column_names,
                                                        external_table_stat,
                                                        external_column_stats))) {
        LOG_WARN("failed to fetch table statistics via catalog", K(ret));
      } else if (OB_ISNULL(external_table_stat)) {
        // No table stat found, generate complete default statistics
        if (OB_FAIL(generate_default_external_table_statistics(allocator,
                                                               lake_table_metadata,
                                                               partition_names,
                                                               column_names,
                                                               external_table_stat,
                                                               external_column_stats))) {
          LOG_WARN("failed to generate default external table statistics", K(ret));
        } else {
          LOG_TRACE("successfully generated default external table statistics",
                    KP(external_table_stat), K(external_column_stats.count()));
        }
      } else if (OB_UNLIKELY(external_column_stats.count() != column_names.count())) {
        if (OB_FAIL(generate_missing_external_column_statistics(allocator,
                                                                lake_table_metadata,
                                                                partition_names,
                                                                column_names,
                                                                external_column_stats))) {
          LOG_WARN("failed to generate missing external column statistics", K(ret));
        } else {
          LOG_TRACE("successfully generated missing external column statistics",
                    K(external_column_stats.count()), K(column_names.count()));
        }
      }
    }
  }

  return ret;
}

int ObOptStatService::put_external_table_stats_to_cache(const uint64_t tenant_id,
                                                       const uint64_t catalog_id,
                                                       const uint64_t table_id,
                                                       const ObIArray<ObString> &partition_names,
                                                       ObOptExternalTableStat *external_table_stat,
                                                       const ObIArray<ObOptExternalColumnStat *> &external_column_stats,
                                                       ObOptExternalTableStatHandle &handle,
                                                       ObIArray<ObOptExternalColumnStatHandle> &column_handles)
{
  int ret = OB_SUCCESS;
  ObString partition_value = partition_names.empty() ? ObString("") : partition_names.at(0);

  // Put table statistics to cache
  if (OB_ISNULL(external_table_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("external table stat is null", K(ret));
  } else {
    ObOptExternalTableStat::Key table_key;
    table_key.tenant_id_ = tenant_id;
    table_key.catalog_id_ = catalog_id;
    table_key.database_name_ = external_table_stat->get_database_name();
    table_key.table_name_ = external_table_stat->get_table_name();
    table_key.partition_value_ = partition_value;

    if (OB_FAIL(external_table_stat_cache_.put_and_fetch_value(table_key, *external_table_stat, handle))) {
      LOG_WARN("failed to put and fetch external table stat", K(ret), K(table_key));
    } else {
      LOG_TRACE("successfully put external table stat to cache", K(table_key));
    }
  }

  // Put column statistics to cache
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < external_column_stats.count(); ++i) {
      const ObOptExternalColumnStat *column_stat = external_column_stats.at(i);
      if (OB_ISNULL(column_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column stat is null", K(ret), K(i));
      } else {
        ObOptExternalColumnStat::Key column_key;
        ObOptExternalColumnStatHandle *column_handle = column_handles.alloc_place_holder();
        column_key.tenant_id_ = tenant_id;
        column_key.catalog_id_ = catalog_id;
        column_key.database_name_ = column_stat->get_database_name();
        column_key.table_name_ = column_stat->get_table_name();
        column_key.partition_value_ = partition_value;
        column_key.column_name_ = column_stat->get_column_name();

        if (OB_ISNULL(column_handle)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc place holder", K(ret));
        } else if (OB_FAIL(external_column_stat_cache_.put_and_fetch_row(column_key, *column_stat, *column_handle))) {
          LOG_WARN("failed to put and fetch external column stat", K(ret), K(column_key));
        } else {
          LOG_TRACE("successfully put external column stat to cache", K(column_key));
        }
      }
    }
  }

  return ret;
}

int ObOptStatService::get_external_table_stat(const uint64_t tenant_id,
                                              const ObOptExternalTableStat::Key &key,
                                              ObOptExternalTableStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(key));
  } else if (OB_FAIL(external_table_stat_cache_.get_value(key, handle))) {
    // For external table stats, we need to fetch from catalog if not in cache
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get external table stat from cache failed", K(ret), K(key));
    } else {
      LOG_TRACE("external table stat not found in cache", K(key));
    }
  } else if (OB_ISNULL(handle.stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(key));
  }
  return ret;
}

int ObOptStatService::get_external_column_stat(const uint64_t tenant_id,
                                                const ObOptExternalColumnStat::Key &key,
                                                ObOptExternalColumnStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(key));
  } else if (OB_FAIL(external_column_stat_cache_.get_row(key, handle))) {
    // For external column stats, we don't auto-load - just return cache miss
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get external column stat from cache failed", K(ret), K(key));
    } else {
      LOG_TRACE("external column stat not found in cache", K(key));
    }
  } else if (OB_ISNULL(handle.stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(key));
  }
  return ret;
}

int ObOptStatService::get_external_column_stats(const uint64_t tenant_id,
                                                ObIArray<ObOptExternalColumnStat::Key> &keys,
                                                ObIArray<ObOptExternalColumnStatHandle> &handles)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(keys));
  } else {
    LOG_TRACE("begin get external column stats", K(keys));
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      ObOptExternalColumnStatHandle handle;
      if (OB_FAIL(external_column_stat_cache_.get_row(keys.at(i), handle))) {
        // For external column stats, we don't auto-load - just return cache miss
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get external column stat from cache failed", K(ret), K(keys.at(i)));
        } else {
          LOG_TRACE("external column stat not found in cache", K(keys.at(i)));
          if (OB_FAIL(handles.push_back(handle))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      } else if (OB_ISNULL(handle.stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cache hit but value is NULL. BUG here.", K(ret), K(keys.at(i)));
      } else if (OB_FAIL(handles.push_back(handle))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObOptStatService::generate_missing_external_column_statistics(ObIAllocator &allocator,
                                                                  const share::ObILakeTableMetadata *lake_table_metadata,
                                                                  const ObIArray<ObString> &partition_names,
                                                                  const ObIArray<ObString> &column_names,
                                                                  ObIArray<ObOptExternalColumnStat *> &external_column_stats)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret));
  } else {
    ObString partition_value = partition_names.empty() ? ObString("") : partition_names.at(0);
    // Use hash map to build column name -> column stat mapping for O(1) lookup
    hash::ObHashMap<ObString, ObOptExternalColumnStat *> existing_column_stats_map;
    if (OB_FAIL(existing_column_stats_map.create(external_column_stats.count() * 2 + 512,
                                                 ObModIds::OB_HASH_BUCKET))) {
      LOG_WARN("failed to create existing column stats hash map", K(ret), K(external_column_stats.count()));
    } else {
      // Build hash map with existing column stats
      for (int64_t j = 0; OB_SUCC(ret) && j < external_column_stats.count(); ++j) {
        if (OB_NOT_NULL(external_column_stats.at(j))) {
          const ObString &existing_column_name = external_column_stats.at(j)->get_column_name();
          if (OB_FAIL(existing_column_stats_map.set_refactored(existing_column_name,
                                                               external_column_stats.at(j)))) {
            LOG_WARN("failed to insert column stat to hash map", K(ret), K(j), K(existing_column_name));
          }
        }
      }

      // Clear the external_column_stats array and rebuild it in column_names order
      external_column_stats.reuse();

      // Build ordered column stats array according to column_names
      for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
        const ObString &column_name = column_names.at(i);
        ObOptExternalColumnStat *column_stat = nullptr;

        // Try to find existing column stat in hash map
        int hash_ret = existing_column_stats_map.get_refactored(column_name, column_stat);
        if (OB_SUCCESS == hash_ret && OB_NOT_NULL(column_stat)) {
          // Use existing column stat
          if (OB_FAIL(external_column_stats.push_back(column_stat))) {
            LOG_WARN("failed to add existing column stat to ordered array", K(ret), K(i), K(column_name));
          }
        } else if (OB_HASH_NOT_EXIST == hash_ret ||
                   OB_ISNULL(column_stat)) {
          // Column not found, generate default column stat
          ObOptExternalColumnStatBuilder column_stat_builder(allocator);

          if (OB_FAIL(column_stat_builder.set_basic_info(lake_table_metadata->tenant_id_,
                                                         lake_table_metadata->catalog_id_,
                                                         lake_table_metadata->namespace_name_,
                                                         lake_table_metadata->table_name_,
                                                         partition_value,
                                                         column_name))) {
            LOG_WARN("failed to set basic info for missing column stat", K(ret), K(i), K(column_name));
          } else if (OB_FAIL(column_stat_builder.set_stat_info(0,     // num_null
                                                               0,     // num_not_null
                                                               0,     // num_distinct
                                                               0,     // avg_length
                                                               0,     // last_analyzed = 0
                                                               CS_TYPE_UTF8MB4_GENERAL_CI))) { // default collation
            LOG_WARN("failed to set stat info for missing column stat", K(ret), K(i), K(column_name));
          } else if (OB_FAIL(column_stat_builder.build(allocator, column_stat))) {
            LOG_WARN("failed to build missing external column stat", K(ret), K(i), K(column_name));
          } else if (OB_ISNULL(column_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("external column stat is null", K(ret), K(i), K(column_name));
          } else if (OB_FAIL(external_column_stats.push_back(column_stat))) {
            LOG_WARN("failed to add generated column stat to ordered array", K(ret), K(i), K(column_name));
          } else {
            LOG_TRACE("generated default stat for missing column", K(column_name));
          }
        } else {
          // Hash map error
          ret = hash_ret;
          LOG_WARN("failed to get column stat from hash map", K(ret), K(i), K(column_name));
        }
      }

      // Clean up hash map
      existing_column_stats_map.destroy();
    }
  }

  return ret;
}

int ObOptStatService::generate_default_external_table_statistics(ObIAllocator &allocator,
                                                                 const share::ObILakeTableMetadata *lake_table_metadata,
                                                                 const ObIArray<ObString> &partition_names,
                                                                 const ObIArray<ObString> &column_names,
                                                                 ObOptExternalTableStat *&external_table_stat,
                                                                 ObIArray<ObOptExternalColumnStat *> &external_column_stats)
{
  int ret = OB_SUCCESS;
  external_table_stat = nullptr;

  if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret));
  } else {
    // Generate default table statistics
    ObOptExternalTableStatBuilder table_stat_builder;
    ObString partition_value = partition_names.empty() ? ObString("") : partition_names.at(0);

    if (OB_FAIL(table_stat_builder.set_basic_info(lake_table_metadata->tenant_id_,
                                                   lake_table_metadata->catalog_id_,
                                                   lake_table_metadata->namespace_name_,
                                                   lake_table_metadata->table_name_,
                                                   partition_value))) {
      LOG_WARN("failed to set basic info for default table stat", K(ret));
    } else if (OB_FAIL(table_stat_builder.set_stat_info(0,
                                                        0,
                                                        0,
                                                        0))) {
      LOG_WARN("failed to set stat info for default table stat", K(ret));
    } else if (OB_FAIL(table_stat_builder.build(allocator, external_table_stat))) {
      LOG_WARN("failed to build default external table stat", K(ret));
    } else if (OB_ISNULL(external_table_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("external table stat is null", K(ret));
    } else if (OB_FAIL(generate_missing_external_column_statistics(allocator,
                                                                   lake_table_metadata,
                                                                   partition_names,
                                                                   column_names,
                                                                   external_column_stats))) {
      LOG_WARN("failed to generate missing external column statistics", K(ret));
    }
  }
  return ret;
}

static int get_dbms_stats_column_ids(const share::schema::ObTableSchema &table_schema,
                                     ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); ++i) {
    const share::schema::ObColumnSchemaV2 *col = table_schema.get_column_schema_by_idx(i);
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is null", K(ret), K(col));
    //here add extra column id condition, because func index in oracle mode, the column will mark is
    //hidden, that's will cause the fewer columns.
    } else if (!pl::ObDbmsStats::check_column_validity(table_schema, *col)){
      continue;
    } else if (OB_FAIL(column_ids.push_back(col->get_column_id()))) {
      LOG_WARN("failed to push column id", K(ret));
    }
  }
  return ret;
}

static int evict_one_table_opt_stat_kvcache(const uint64_t tenant_id,
                                            const ObEvictTableKey &key,
                                            ObSchemaGetterGuard &schema_guard,
                                            ObIArray<share::ObServerLocality> &all_server_arr,
                                            int64_t timeout,
                                            int64_t &last_gmt_modified)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = NULL;
  const int64_t NO_EXPIRED_AT = -1;
  obrpc::ObUpdateStatCacheArg update_stat_cache_arg;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, key.table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(key.table_id_));
  } else if (OB_ISNULL(table_schema)) {
    LOG_INFO("get null table schema, skip evict", K(key.table_id_));
    last_gmt_modified = key.gmt_modified_;
  } else if (OB_FAIL(get_dbms_stats_column_ids(*table_schema, update_stat_cache_arg.column_ids_))) {
    LOG_WARN("failed to get dbms stats column ids", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_all_part_ids(*table_schema,
                                                        update_stat_cache_arg.partition_ids_))) {
    LOG_WARN("failed to push back partition ids", K(ret));
  } else {
    update_stat_cache_arg.tenant_id_ = tenant_id;
    update_stat_cache_arg.table_id_ = key.table_id_;
    update_stat_cache_arg.no_invalidate_ = (key.plan_expired_before_ == NO_EXPIRED_AT);
    update_stat_cache_arg.plan_expired_before_ =
        (key.plan_expired_before_ == NO_EXPIRED_AT) ? 0 : key.plan_expired_before_;
    update_stat_cache_arg.standby_last_evict_time_ = key.gmt_modified_;
    if (OB_FAIL(broadcast_evict_opt_stat_kvcache(tenant_id, all_server_arr,
                                                  timeout, update_stat_cache_arg))) {
      LOG_WARN("failed to broadcast evict opt stat kvcache", K(ret));
    } else {
      last_gmt_modified = key.gmt_modified_;
      LOG_INFO("successfully evict standby opt stat kvcache",
               K(key.table_id_), K(key.plan_expired_before_));
    }
  }
  return ret;
}

int ObOptStatService::evict_all_opt_stat_kvcache(const uint64_t tenant_id,
                                                 int64_t &last_gmt_modified)
{
  int ret = OB_SUCCESS;
  const static int64_t max_table_cnt = 2000;
  ObSEArray<ObEvictTableKey, 32> keys;
  ObSEArray<share::ObServerLocality, 4> all_server_arr;
  bool has_read_only_zone = false; // UNUSED;
  int64_t timeout = MAX_OPT_STATS_PROCESS_RPC_TIMEOUT;
  ObSchemaGetterGuard schema_guard;
  hash::ObHashSet<uint64_t> table_ids_set;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("statistics service is not initialized. ", K(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.locality_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc_proxy or session is null", K(ret), K(GCTX.srv_rpc_proxy_), K(GCTX.locality_manager_));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(GCTX.locality_manager_->get_server_locality_array(all_server_arr,
                                                                       has_read_only_zone))) {
    LOG_WARN("fail to get server locality", K(ret));
  } else if (OB_FAIL(table_ids_set.create(max_table_cnt, "TableIdsSet", "TableIdsSetNode", tenant_id))) {
    LOG_WARN("failed to create table ids set", K(ret));
  } else {
    LOG_INFO("begin evict standby opt stat kvcache", K(tenant_id), K(last_gmt_modified));
    do {
      keys.reuse();
      if (OB_FAIL(sql_service_.get_recent_updated_stats(tenant_id,
                                                        last_gmt_modified,
                                                        max_table_cnt,
                                                        keys))) {
        LOG_WARN("failed to get recent updated stats", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
          const ObEvictTableKey &key = keys.at(i);
          if (OB_FAIL(THIS_WORKER.check_status())) {
            LOG_WARN("failed to check status", K(ret));
          } else if (OB_FAIL(table_ids_set.set_refactored(key.table_id_))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
              last_gmt_modified = key.gmt_modified_;
              LOG_TRACE("table id already exists, skip evict", K(key.table_id_));
            } else {
              LOG_WARN("failed to set refactored", K(ret), K(key.table_id_));
            }
          } else {
            timeout = std::min(MAX_OPT_STATS_PROCESS_RPC_TIMEOUT, THIS_WORKER.get_timeout_remain());
            int tmp_ret = evict_one_table_opt_stat_kvcache(tenant_id, key, schema_guard,
                                                           all_server_arr, timeout,
                                                           last_gmt_modified);
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("failed to evict one table opt stat kvcache, skip",
                       K(tmp_ret), K(key.table_id_));
              last_gmt_modified = key.gmt_modified_;
            }
          }
        }
      }
    } while (OB_SUCC(ret) && max_table_cnt == keys.count());
    LOG_INFO("end evict standby opt stat kvcache", K(tenant_id), K(last_gmt_modified), K(table_ids_set.size()));
  }
  return ret;
}

}
}
