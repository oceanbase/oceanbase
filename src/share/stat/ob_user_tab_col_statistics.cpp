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

#include "share/stat/ob_user_tab_col_statistics.h"
#include "share/stat/ob_column_stat.h"
#include "common/ob_partition_key.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace share::schema;
namespace common {

bool my_is_non_stat_table(const int64_t table_id)
{
  const uint64_t id = common::extract_pure_id(table_id);
  return (id < share::OB_ALL_META_TABLE_TID || id == share::OB_ALL_COLUMN_STATISTIC_TID);
}

ObColStatService::ObColStatService() : sql_service_(), column_stat_cache_(), inited_(false)
{}

ObColStatService::~ObColStatService()
{}

int ObColStatService::init(common::ObMySQLProxy* proxy, ObServerConfig* config)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "col stat service has been initialized.", K(ret));
  } else if (NULL == proxy) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid proxy object.", KP(proxy), K(ret));
  } else if (OB_FAIL(sql_service_.init(proxy, config))) {
    COMMON_LOG(WARN, "fail to init sql_service_.", K(ret));
  } else if (OB_FAIL(column_stat_cache_.init(
                 "user_tab_col_stat_cache", common::ObServerConfig::get_instance().user_tab_col_stat_cache_priority))) {
    COMMON_LOG(WARN, "fail to init table cache.", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObColStatService::get_column_stat(
    const ObColumnStat::Key& key, const bool force_new, ObColumnStatValueHandle& handle)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "col stat service has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(key), K(ret));
  } else if (force_new) {
    if (OB_FAIL(load_and_put_cache(key, handle))) {
      COMMON_LOG(WARN, "load_and_put_cache column stat failed.", K(key), K(ret));
    }
  } else if (OB_FAIL(column_stat_cache_.get_row(key, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "get column stat from cache failed.", K(ret));
    } else if (OB_FAIL(load_and_put_cache(key, handle))) {
      COMMON_LOG(WARN, "load_and_put_cache column stat failed.", K(key), K(ret));
    }
  } else if (my_is_non_stat_table(key.table_id_)) {
    // skip non stat table
  } else if (OB_ISNULL(handle.cache_value_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "cache hit but value is NULL, BUG here.", K(key), K(ret));
  } else if (OB_UNLIKELY((version = handle.cache_value_->get_version()) < *GCTX.merged_version_)) {
    handle.reset();
    if (OB_FAIL(load_and_put_cache(key, handle))) {
      COMMON_LOG(WARN,
          "failed to load_and_put_cache column stat",
          K(ret),
          K(key),
          "colum stat version",
          version,
          "merged version",
          *GCTX.merged_version_);
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(handle.cache_value_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "cache hit but value is NULL, BUG here.", K(key), K(ret));
  }

  return ret;
}

int ObColStatService::get_column_stat(
    const ObColumnStat::Key& key, const bool force_new, ObColumnStat& stat, ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  ObColumnStatValueHandle handle;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "col stat service has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(key), K(ret));
  } else if (OB_FAIL(get_column_stat(key, force_new, handle))) {
    COMMON_LOG(WARN, "get_column_stat failed.", K(ret));
  } else if (OB_FAIL(deep_copy(alloc, *handle.cache_value_, stat))) {
    COMMON_LOG(WARN, "copy cache value to result failed.", K(ret));
  }
  return ret;
}

int ObColStatService::get_batch_stat(const uint64_t table_id, const ObIArray<uint64_t>& partition_ids,
    const ObIArray<uint64_t>& column_ids, ObIArray<ObColumnStatValueHandle>& stat_values)
{
  int ret = OB_SUCCESS;
  // column stat not existed in the cache
  ObSEArray<uint64_t, 4> new_partition_ids;
  ObColumnStatValueHandle handler;
  ObSEArray<ObColumnStatValueHandle, 8> part_stat_values;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
    const uint64_t part_id = partition_ids.at(i);
    part_stat_values.reuse();
    bool need_fetch = false;
    for (int64_t j = 0; OB_SUCC(ret) && !need_fetch && j < column_ids.count(); ++j) {
      ObColumnStat::Key col_key(table_id, part_id, column_ids.at(j));
      bool found = false;
      if (OB_FAIL(column_stat_cache_.get_row(col_key, handler))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          COMMON_LOG(WARN, "failed to get row from cache", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(handler.cache_value_)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "cache hit but value is null", K(ret), K(col_key));
      } else if (handler.cache_value_->get_version() < *GCTX.merged_version_) {
        // the cached value is stale
      } else if (OB_FAIL(part_stat_values.push_back(handler))) {
        COMMON_LOG(WARN, "failed to push back stat value handler", K(ret));
      } else {
        found = true;
      }
      need_fetch = !found;
    }
    if (OB_SUCC(ret)) {
      if (need_fetch) {
        if (OB_FAIL(new_partition_ids.push_back(part_id))) {
          COMMON_LOG(WARN, "failed to push back partition id", K(ret));
        } else if (new_partition_ids.count() >= 100) {
          ObArray<ObColumnStatValueHandle> new_stats;
          if (OB_FAIL(batch_load_and_put_cache(table_id, new_partition_ids, column_ids, new_stats))) {
            COMMON_LOG(WARN, "failed to batch load and put cache", K(ret));
          } else if (OB_FAIL(append(stat_values, new_stats))) {
            COMMON_LOG(WARN, "failed to append new stat values", K(ret));
          } else {
            new_partition_ids.reset();
          }
        }
      } else if (OB_FAIL(append(stat_values, part_stat_values))) {
        COMMON_LOG(WARN, "failed to append stat values", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !new_partition_ids.empty()) {
    ObSEArray<ObColumnStatValueHandle, 8> new_stats;
    if (OB_FAIL(batch_load_and_put_cache(table_id, new_partition_ids, column_ids, new_stats))) {
      COMMON_LOG(WARN, "failed to batch load and put cache", K(ret));
    } else if (OB_FAIL(append(stat_values, new_stats))) {
      COMMON_LOG(WARN, "failed to append new stat values", K(ret));
    }
  }
  return ret;
}

int ObColStatService::get_batch_stat(const ObTableSchema& table_schema, const uint64_t partition_id,
    ObIArray<ObColumnStat*>& stats, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObArray<ObColumnStatValueHandle> handles;
  ObArenaAllocator arena(ObModIds::OB_BUFFER);
  uint64_t table_id = table_schema.get_table_id();
  int64_t pos = 0;
  bool find = false;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "col stat service has not been initialized. ", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), K(table_id), K(partition_id), "column_stat_cnt", stats.count());
  } else if (OB_FAIL(batch_load_and_put_cache(table_schema, partition_id, handles))) {
    COMMON_LOG(WARN, "fail to batch load_and_put_cache. ", K(ret), K(table_id), K(partition_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stats.count(); ++i) {
      find = false;
      for (int64_t j = 0; j < handles.count(); ++j) {
        if (stats.at(i)->get_column_id() == handles.at(j).cache_value_->get_column_id()) {
          pos = j;
          find = true;
          break;
        }
      }

      if (find) {
        if (OB_FAIL(deep_copy(allocator, *handles.at(pos).cache_value_, *stats.at(i)))) {
          COMMON_LOG(WARN, "copy cache value to result failed. ", K(ret), K(table_id), K(partition_id), K(i));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "fail to find column_stat from handles.", K(ret), K(table_id), K(partition_id), K(i));
      }
    }
  }

  return ret;
}

int ObColStatService::update_column_stats(const ObIArray<ObColumnStat*>& stats)
{
  int ret = OB_SUCCESS;
  ObColumnStat::Key key;
  ObColumnStat* stat = NULL;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "col stat service has not been initialized.", K(ret));
  } else if (OB_FAIL(sql_service_.insert_or_update_column_stats(stats))) {
    COMMON_LOG(WARN, "fail to update user_tab_col_statistics table.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stats.count(); ++i) {
      if (NULL == (stat = stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "item in stats must not NULL, ", K(ret), K(i));
      } else {
        key.table_id_ = stat->get_table_id();
        key.partition_id_ = stat->get_partition_id();
        key.column_id_ = stat->get_column_id();
        if (OB_FAIL(column_stat_cache_.put_row(key, *stat))) {
          COMMON_LOG(WARN, "put column stat in cache failed.", K(ret), K(i), K(*stat));
        }
      }
    }
  }

  return ret;
}

int ObColStatService::update_column_stat(const ObColumnStat& stat)
{
  int ret = OB_SUCCESS;
  ObColumnStat::Key key(stat.get_table_id(), stat.get_partition_id(), stat.get_column_id());
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "col stat service has not been initialized.", K(ret));
  } else if (!key.is_valid() || !stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(key), K(stat), K(ret));
  } else if (OB_FAIL(sql_service_.insert_or_update_column_stat(stat))) {
    COMMON_LOG(WARN, "update user_tab_col_statistics table failed.", K(ret), K(stat));
  } else if (OB_FAIL(column_stat_cache_.put_row(key, stat))) {
    COMMON_LOG(WARN, "put column stat in cache failed.", K(ret));
  }
  return ret;
}

int ObColStatService::load_and_put_cache(const ObColumnStat::Key& key, ObColumnStatValueHandle& handle)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena(ObModIds::OB_BUFFER);
  // generate new entry and load from table to put into cache.
  common::ObColumnStat new_entry(arena);
  new_entry.set_table_id(key.table_id_);
  new_entry.set_partition_id(key.partition_id_);
  new_entry.set_column_id(key.column_id_);
  new_entry.set_version(*GCTX.merged_version_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "col stat service has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(key), K(ret));
  } else if (!new_entry.is_writable()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "ObColumnStat Object new_entry is invalid.", K(ret));
  } else if (common::my_is_non_stat_table(key.table_id_)) {
    if (OB_FAIL(column_stat_cache_.put_and_fetch_row(key, new_entry, handle))) {
      COMMON_LOG(WARN, "puts column stat into cache failed.", K(ret));
    }
  } else if (OB_FAIL(load_and_put_cache(key, new_entry, handle))) {
    COMMON_LOG(WARN, "load from internal table and put in cache failed.", K(ret));
  }
  return ret;
}

int ObColStatService::load_and_put_cache(
    const ObColumnStat::Key& key, ObColumnStat& new_entry, ObColumnStatValueHandle& handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "col stat service has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(key), K(ret));
  } else if (*GCTX.merged_version_ > 1 && OB_FAIL(sql_service_.fetch_column_stat(key, new_entry))) {
    if (OB_ENTRY_NOT_EXIST != ret && OB_TABLE_NOT_EXIST != ret && OB_TRANS_STMT_TIMEOUT != ret) {
      COMMON_LOG(WARN, "fetch data from user_tab_col_statistics failed.", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (new_entry.get_version() < *GCTX.merged_version_) {
    // stat stored in the inner table is not updated by major freeze
    COMMON_LOG(INFO, "read stale stat from inner table", K(*GCTX.merged_version_), K(new_entry.get_version()), K(key));
    new_entry.set_version(*GCTX.merged_version_);
  }
  if (OB_SUCC(ret) && OB_FAIL(column_stat_cache_.put_and_fetch_row(key, new_entry, handle))) {
    COMMON_LOG(WARN, "puts column stat into cache failed.", K(ret));
  }
  return ret;
}

int ObColStatService::batch_load_and_put_cache(
    const ObTableSchema& table_schema, const uint64_t partition_id, ObIArray<ObColumnStatValueHandle>& handles)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> column_ids;
  ObTableSchema::const_column_iterator it_begin = table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = table_schema.column_end();
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "col stat service has not been initialized.", K(ret));
  }
  for (; OB_SUCC(ret) && it_begin != it_end; ++it_begin) {
    if (OB_ISNULL(it_begin)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "it_begin is NULL. ", K(ret));
    } else if (OB_ISNULL(*it_begin)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "*it_begin is NULL. ", K(ret));
    } else if (OB_FAIL(column_ids.push_back((*it_begin)->get_column_id()))) {
      COMMON_LOG(WARN, "failed to push back column id", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<uint64_t, 1> partition_list;
    if (OB_FAIL(partition_list.push_back(partition_id))) {
      COMMON_LOG(WARN, "failed to push back partition id", K(ret));
    } else if (OB_FAIL(batch_load_and_put_cache(table_schema.get_table_id(), partition_list, column_ids, handles))) {
      COMMON_LOG(WARN, "failed to batch load and put cache", K(ret));
    }
  }
  return ret;
}

int ObColStatService::batch_load_and_put_cache(const uint64_t table_id, const ObIArray<uint64_t>& partition_ids,
    const ObIArray<uint64_t>& column_ids, ObIArray<ObColumnStatValueHandle>& handles)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena(ObModIds::OB_BUFFER);
  ObArray<ObColumnStat*> column_stats;
  ObArray<ObColumnStat*> all_column_stats;
  ObColumnStat* col_stat = NULL;
  void* ptr = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "col stat service has not been initialized.", K(ret));
  } else if (!common::my_is_non_stat_table(table_id) && *GCTX.merged_version_ > 1) {
    // 1. try to fetch column stat from inner table and put them into column stat cache
    if (OB_FAIL(sql_service_.fetch_batch_stat(table_id, partition_ids, column_ids, arena, column_stats))) {
      if (OB_TABLE_NOT_EXIST == ret || OB_TRANS_STMT_TIMEOUT == ret) {
        ret = OB_SUCCESS;
      } else if (OB_ENTRY_NOT_EXIST != ret) {
        COMMON_LOG(
            WARN, "batch fetch data from user_tab_col_statistics failed.", K(ret), K(table_id), K(partition_ids));
      } else if (OB_FAIL(sql_service_.fetch_batch_stat(
                     table_id, partition_ids, column_ids, arena, column_stats, false /*use_pure_id=false*/))) {
        if (OB_ENTRY_NOT_EXIST == ret || OB_TRANS_STMT_TIMEOUT == ret) {
          ret = OB_SUCCESS;
        } else {
          COMMON_LOG(
              WARN, "batch fetch data from user_tab_col_statistics failed.", K(ret), K(table_id), K(partition_ids));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // check stat version
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); ++i) {
      ObColumnStat* col_stat = NULL;
      if (OB_ISNULL(col_stat = column_stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "column stats is null", K(ret));
      } else if (col_stat->get_version() < *GCTX.merged_version_) {
        // stat stored in the inner table is not updated by major freeze
        COMMON_LOG(INFO,
            "read stale stat from inner table",
            K(*GCTX.merged_version_),
            K(col_stat->get_version()),
            K(col_stat->get_key()));
        col_stat->set_version(*GCTX.merged_version_);
      }
    }
    if (column_stats.count() >= partition_ids.count() * column_ids.count()) {
      // all column stats are returned by inner-sql
      if (OB_FAIL(all_column_stats.assign(column_stats))) {
        COMMON_LOG(WARN, "failed to assign column stats", K(ret));
      }
    } else {
      // 2. seems that some column stat is not returned by inner-sql, fill them with default stat entry
      // e.g. non stat table, merged version <= 1, or generated column does not have statistics
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); ++j) {
          const uint64_t part_id = partition_ids.at(i);
          const uint64_t col_id = column_ids.at(j);
          col_stat = NULL;
          ptr = NULL;
          for (int64_t k = 0; OB_SUCC(ret) && k < column_stats.count(); ++k) {
            if (OB_ISNULL(column_stats.at(k))) {
              ret = OB_ERR_UNEXPECTED;
              COMMON_LOG(WARN, "column stats is null", K(ret));
            } else if (column_stats.at(k)->get_partition_id() == part_id &&
                       column_stats.at(k)->get_column_id() == col_id) {
              col_stat = column_stats.at(k);
              break;
            }
          }
          if (OB_SUCC(ret) && OB_ISNULL(col_stat)) {
            if (OB_ISNULL(ptr = arena.alloc(sizeof(ObColumnStat))) ||
                OB_ISNULL(col_stat = new (ptr) ObColumnStat(arena)) || OB_UNLIKELY(!col_stat->is_writable())) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              COMMON_LOG(ERROR, "failed to allocate memory for ObColumnStat", K(ret), "size", sizeof(ObColumnStat));
            } else {
              col_stat->set_table_id(table_id);
              col_stat->set_partition_id(part_id);
              col_stat->set_column_id(col_id);
              col_stat->set_version(*GCTX.merged_version_);
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(all_column_stats.push_back(col_stat))) {
              COMMON_LOG(WARN, "failed to push back col stat", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(batch_put_and_fetch_row(all_column_stats, handles))) {
      COMMON_LOG(WARN, "failed to batch put and fetch row", K(ret));
    }
  }
  return ret;
}

int ObColStatService::batch_put_and_fetch_row(
    ObIArray<ObColumnStat*>& col_stats, ObIArray<ObColumnStatValueHandle>& handles)
{
  int ret = OB_SUCCESS;
  ObColumnStat::Key key;
  ObColumnStatValueHandle handle;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "col stat service has not been initialized.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_stats.count(); ++i) {
      key = col_stats.at(i)->get_key();
      if (OB_FAIL(column_stat_cache_.put_and_fetch_row(key, *col_stats.at(i), handle))) {
        COMMON_LOG(WARN, "put column stat into cache failed.", K(ret), K(key));
      } else if (OB_FAIL(handles.push_back(handle))) {
        COMMON_LOG(WARN, "fail to push back ObColumnStatValueHandle. ", K(ret));
      } else {
        handle.reset();
      }
    }
  }

  return ret;
}
int ObColStatService::deep_copy(ObIAllocator& alloc, const ObColumnStat& src, ObColumnStat& dst)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t size = src.get_deep_copy_size();
  char* buf = NULL;
  if (NULL == (buf = (char*)alloc.alloc(size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "allocate memory for deep copy column stat failed.", K(size), K(ret));
  } else if (OB_FAIL(dst.deep_copy(src, buf, size, pos))) {
    COMMON_LOG(WARN, "deep copy column stat failed.", K(ret));
  }
  return ret;
}

int ObColStatService::erase_column_stat(const ObPartitionKey& pkey, const int64_t column_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObColStatService is not inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || 0 >= column_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(pkey), K(column_id));
  } else {
    ObColumnStat::Key key;
    key.table_id_ = pkey.get_table_id();
    key.partition_id_ = pkey.get_partition_id();
    key.column_id_ = column_id;
    if (OB_FAIL(column_stat_cache_.erase(key))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        COMMON_LOG(WARN, "failed to erase column stat cache", K(ret), K(key));
      }
    }
  }
  return ret;
}

ObTableStatService::ObTableStatService() : service_cache_(NULL), inited_(false)
{}

ObTableStatService::~ObTableStatService()
{}

int ObTableStatService::init(common::ObMySQLProxy* proxy, ObTableStatDataService* cache, ObServerConfig* config)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "col stat service has been initialized.", K(ret));
  } else if (NULL == proxy || NULL == cache) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid proxy object.", KP(proxy), KP(cache), K(ret));
  } else if (OB_FAIL(sql_service_.init(proxy, config))) {
    COMMON_LOG(WARN, "fail to init sql_service_.", K(ret));
  } else if (OB_FAIL(table_stat_cache_.init("user_table_stat_cache", DEFAULT_USER_TAB_STAT_CACHE_PRIORITY))) {
    COMMON_LOG(WARN, "fail to init table stat cache.", K(ret));
  } else {
    service_cache_ = cache;
    inited_ = true;
  }
  return ret;
}

int ObTableStatService::get_table_stat(const ObPartitionKey& pkey, ObTableStat& tstat)
{
  int ret = OB_SUCCESS;
  ObTableStatValueHandle handle;
  ObTableStat::Key key(pkey);
  int64_t version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "TableStat service is not inited.", K(ret), K(key));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(ret), K(key));
  } else if (OB_FAIL(table_stat_cache_.get_row(key, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "get table stat from cache failed.", K(ret), K(key));
    } else if (OB_FAIL(load_and_put_cache(key, handle))) {
      COMMON_LOG(WARN, "load_and_put_cache table stat failed.", K(ret), K(key));
    }
  } else if (my_is_non_stat_table(pkey.get_table_id())) {
    // skip non stat table
  } else if (OB_ISNULL(handle.cache_value_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "cache hit but value is NULL, BUG here.", K(ret), K(key));
  } else if (OB_UNLIKELY((version = handle.cache_value_->get_data_version()) < *GCTX.merged_version_)) {
    handle.reset();
    if (OB_FAIL(load_and_put_cache(key, handle))) {
      COMMON_LOG(WARN,
          "failed to load_and_put_cache table stat",
          K(ret),
          K(key),
          "table stat version",
          version,
          "merged version",
          *GCTX.merged_version_);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(handle.cache_value_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "cache hit but value is NULL, BUG here.", K(ret), K(key));
    } else {
      tstat = *handle.cache_value_;
    }
  }

  return ret;
}

int ObTableStatService::erase_table_stat(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTableStatService has not been initialized.", K(ret), K(pkey));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(ret), K(pkey));
  } else {
    ObTableStat::Key key(pkey);
    if (OB_FAIL(table_stat_cache_.erase(key))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        COMMON_LOG(WARN, "failed to erase table stat", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObTableStatService::load_and_put_cache(ObTableStat::Key& key, ObTableStatValueHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStat tstat;
  ObSEArray<std::pair<ObPartitionKey, ObTableStat>, 4> all_part_stats;
  tstat.set_data_version(*GCTX.merged_version_);
  bool is_added = false;  // whether the required table stat is added into cache
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTableStatService has not been initialized.", K(ret), K(key));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(ret), K(key));
  } else if (OB_FAIL(service_cache_->get_table_stat(key.pkey_, tstat))) {
    if (OB_PARTITION_NOT_EXIST != ret && OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "get table stat on service_cache failed.", K(ret), K(key));
    } else if (common::my_is_non_stat_table(key.pkey_.get_table_id()) || *GCTX.merged_version_ <= 1) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(sql_service_.fetch_table_stat(key.pkey_, all_part_stats))) {
      if (OB_ENTRY_NOT_EXIST != ret && OB_TRANS_STMT_TIMEOUT != ret) {
        COMMON_LOG(WARN, "get table stat through sql query failed.", K(ret), K(key));
      } else {
        ret = OB_SUCCESS;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_part_stats.count(); ++i) {
      ObTableStat::Key tkey(all_part_stats.at(i).first);
      ObTableStat& tvalue = all_part_stats.at(i).second;
      ObTableStatValueHandle tmp;
      tvalue.set_data_version(*GCTX.merged_version_);
      COMMON_LOG(DEBUG, "put cache", K(tkey), K(tvalue));
      if (OB_FAIL(table_stat_cache_.put_and_fetch_row(tkey, tvalue, tmp))) {
        COMMON_LOG(WARN, "failed to put and fetch row", K(ret));
      } else if (tkey == key) {
        handle = tmp;
        is_added = true;
      }
    }
  }

  if (OB_SUCC(ret) && !is_added) {
    // case 1. the table stat is returned by ObPartitionService
    // case 2. the table stat is not found in ObPartitionService or meta table
    //         it happends after a partition is splitted
    if (OB_FAIL(table_stat_cache_.put_and_fetch_row(key, tstat, handle))) {
      COMMON_LOG(WARN, "puts table stat into cache failed.", K(ret), K(key));
    }
  }
  return ret;
}

}  // namespace common
}  // end of namespace oceanbase
