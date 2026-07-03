/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_dict_bg_task_mgr.h"
#include "storage/fts/dict/ob_ft_dict_mgr.h"
#include "storage/fts/dict/ob_ft_dict_cache_loader.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/ob_server_struct.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace storage
{

ObFTDictRefreshTask::ObFTDictRefreshTask()
    : tenant_id_(OB_INVALID_TENANT_ID),
      sql_proxy_(nullptr),
      is_inited_(false)
{
}

int ObFTDictRefreshTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObFTDictRefreshTask init twice", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret));
  } else {
    tenant_id_ = MTL_ID();
    sql_proxy_ = GCTX.sql_proxy_;
    is_inited_ = true;
  }
  return ret;
}

void ObFTDictRefreshTask::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_proxy_ = nullptr;
  is_inited_ = false;
}

void ObFTDictRefreshTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTDictRefreshTask not init", K(ret));
  } else {
    common::ObArenaAllocator allocator(common::ObMemAttr(tenant_id_, "FTDictRefresh"));
    common::ObArray<DictTableInfo> dict_tables;
    if (OB_FAIL(get_all_dict_tables(allocator, dict_tables))) {
      LOG_WARN("fail to get all dict tables", K(ret), K_(tenant_id));
    } else if (dict_tables.count() > 0) {
      for (int64_t i = 0; i < dict_tables.count(); ++i) {
        const DictTableInfo &table_info = dict_tables.at(i);
        if (OB_FAIL(refresh_dict_cache(table_info))) {
          LOG_WARN("fail to refresh dict cache", K(ret), K(table_info));
        }
      }
      LOG_INFO("finish refresh dict cache", K_(tenant_id), K(dict_tables.count()));
    }
  }
}

int ObFTDictRefreshTask::get_sql_statement(common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  uint64_t cached_ids[ObFTDictMgr::ROW_SCN_CACHE_SIZE];
  int64_t cached_id_cnt = 0;
  ObFTDictMgr *dict_mgr = MTL(ObFTDictMgr *);
  if (OB_ISNULL(dict_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dict_mgr is null", K(ret));
  } else if (OB_FAIL(dict_mgr->collect_table_ids(cached_ids, cached_id_cnt))) {
    LOG_WARN("fail to collect row_scn table ids", K(ret));
  } else if (0 < cached_id_cnt) {
    if (OB_FAIL(sql.assign_fmt(
            "SELECT DISTINCT t.table_id AS table_id, "
            "CONCAT(db.database_name, '.', t.table_name) AS table_name, "
            "t.collation_type AS collation_type "
            "FROM %s t "
            "INNER JOIN %s db ON t.tenant_id = db.tenant_id AND t.database_id = db.database_id "
            "AND db.in_recyclebin = 0 AND db.database_name != '__recyclebin' "
            "WHERE t.tenant_id = %lu AND t.table_id IN (",
            share::OB_ALL_TABLE_TNAME,
            share::OB_ALL_DATABASE_TNAME,
            OB_INVALID_TENANT_ID))) {
      LOG_WARN("fail to assign sql head", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cached_id_cnt; ++i) {
        if (OB_FAIL(sql.append_fmt(0 == i ? "%lu" : ", %lu", cached_ids[i]))) {
          LOG_WARN("fail to append table_id", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append_fmt(")"))) {
          LOG_WARN("fail to assign sql tail", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObFTDictRefreshTask::get_all_dict_tables(common::ObIAllocator &allocator,
                                             common::ObIArray<DictTableInfo> &dict_tables)
{
  int ret = OB_SUCCESS;
  dict_tables.reset();
  common::ObSqlString sql;
  if (OB_FAIL(get_sql_statement(sql))) {
    LOG_WARN("fail to get sql statement", K(ret));
  } else if (!sql.empty()) {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(ret), K_(tenant_id));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret));
        } else {
          bool iter_end = false;
          while (OB_SUCC(ret) && !iter_end) {
            if (OB_FAIL(result->next())) {
              if (OB_UNLIKELY(OB_ITER_END != ret)) {
                LOG_WARN("fail to get next row", K(ret));
              } else {
                ret = OB_SUCCESS;
                iter_end = true;
              }
            } else {
              DictTableInfo table_info;
              common::ObString tmp_table_name;
              EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_info.table_id_, int64_t);
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "table_name", tmp_table_name);
              EXTRACT_INT_FIELD_MYSQL(*result, "collation_type", table_info.collation_type_, int64_t);
              if (OB_FAIL(ret)) {
                LOG_WARN("fail to extract table info", K(ret));
              } else if (OB_INVALID_ID == table_info.table_id_) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("table id is invalid", K(ret), K(table_info.table_id_));
              } else if (OB_FAIL(ob_write_string(allocator, tmp_table_name, table_info.table_name_))) {
                LOG_WARN("fail to copy table name", K(ret));
              } else if (OB_FAIL(dict_tables.push_back(table_info))) {
                LOG_WARN("fail to push dict table", K(ret));
              }
            }
          }
        }
    }
  }

  return ret;
}

int ObFTDictRefreshTask::refresh_dict_cache(const DictTableInfo &table_info)
{
  int ret = OB_SUCCESS;
  const ObCollationType collation = static_cast<ObCollationType>(table_info.collation_type_);
  const ObCharsetType charset = ObCharset::charset_type_by_coll(collation);
  ObFTDictDesc desc(charset, collation, table_info.table_id_, table_info.table_name_);
  ObFTDictCacheLoaderRefresh loader;
  if (OB_FAIL(loader.load_cache(desc))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to load cache", K(ret), K(table_info));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    LOG_INFO("refresh single dict table cache success", K(table_info));
  }
  return ret;
}

int ObFTDictRefreshTaskMgr::init()
{
  return ObFTBackgroundTaskMgrBase<ObFTDictRefreshTask>::init(lib::TGDefIDs::FTDictRefresh);
}

ObFTAccessRowScnCacheTask::ObFTAccessRowScnCacheTask()
    : head_(0),
      tail_(0),
      cooldown_cnt_(0),
      is_inited_(false)
{
  memset(queue_, 0, sizeof(queue_));
  memset(cooldown_, 0, sizeof(cooldown_));
}

int ObFTAccessRowScnCacheTask::init()
{
  is_inited_ = true;
  return OB_SUCCESS;
}

void ObFTAccessRowScnCacheTask::destroy()
{
  head_ = 0;
  tail_ = 0;
  cooldown_cnt_ = 0;
  is_inited_ = false;
  memset(queue_, 0, sizeof(queue_));
  memset(cooldown_, 0, sizeof(cooldown_));
}

void ObFTAccessRowScnCacheTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  ObFTDictMgr *dict_mgr = MTL(ObFTDictMgr *);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTAccessRowScnCacheTask not init", K(ret), K_(is_inited));
  } else if (OB_ISNULL(dict_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dict_mgr is null", K(ret), K_(is_inited));
  } else {
    ObFTDictMgr::DictTableSnapshot snapshot;
    uint64_t table_id = 0;
    int64_t now_us = ObTimeUtility::current_time();
    bool has_more = true;
    int64_t pop_cnt = 0;
    int64_t skip_cooldown_cnt = 0;
    int64_t access_ok_cnt = 0;
    while (has_more) {
      has_more = pop(table_id);
      if (has_more) {
        pop_cnt++;
        if (OB_INVALID_ID != table_id) {
          if (should_skip_by_cooldown(table_id, now_us)) {
            skip_cooldown_cnt++;
          } else if (OB_FAIL(dict_mgr->get_table_snapshot(table_id, snapshot))) {
            if (OB_ENTRY_NOT_EXIST != ret) {
              LOG_WARN("fail to get table snapshot", K(ret), K(table_id));
            }
          } else {
            access_ok_cnt++;
            update_cooldown(table_id, now_us);
          }
        }
      }
    }
    if (pop_cnt > 0) {
      LOG_INFO("ft access row scn cache task drain finished",
               K(pop_cnt),
               K(skip_cooldown_cnt),
               K(access_ok_cnt));
    }
  }
}

// Lock-free for performance; multi-producer may race (overflow/drop or overwrite), acceptable.
void ObFTAccessRowScnCacheTask::push(const uint64_t table_id)
{
  const uint64_t new_tail = ATOMIC_AAF(&tail_, 1);
  const uint64_t cur_head = ATOMIC_LOAD(&head_);
  if (OB_UNLIKELY(new_tail - cur_head > FT_TABLE_ID_QUEUE_CAP)) {
    ATOMIC_SAF(&tail_, 1);
  } else {
    queue_[(new_tail - 1) % FT_TABLE_ID_QUEUE_CAP] = table_id;
  }
}

bool ObFTAccessRowScnCacheTask::pop(uint64_t &table_id)
{
  bool ret = false;
  uint64_t cur_head = ATOMIC_LOAD(&head_);
  if (cur_head < ATOMIC_LOAD(&tail_)) {
    table_id = queue_[cur_head % FT_TABLE_ID_QUEUE_CAP];
    ATOMIC_AAF(&head_, 1);
    ret = true;
  }
  return ret;
}

bool ObFTAccessRowScnCacheTask::should_skip_by_cooldown(const uint64_t table_id, const int64_t now_us)
{
  bool skip = false;
  for (int64_t i = 0; i < cooldown_cnt_ && !skip; ++i) {
    if (cooldown_[i].table_id_ == table_id) {
      skip = (now_us - cooldown_[i].last_access_us_) < FT_ACCESS_COOLDOWN_US;
    }
  }
  return skip;
}

void ObFTAccessRowScnCacheTask::update_cooldown(const uint64_t table_id, const int64_t now_us)
{
  int64_t found_idx = -1;
  int64_t oldest_idx = 0;
  for (int64_t i = 0; i < cooldown_cnt_; ++i) {
    if (cooldown_[i].table_id_ == table_id) {
      found_idx = i;
    }
    if (cooldown_[i].last_access_us_ < cooldown_[oldest_idx].last_access_us_) {
      oldest_idx = i;
    }
  }
  if (found_idx >= 0) {
    // update existing entry
    cooldown_[found_idx].last_access_us_ = now_us;
  } else if (cooldown_cnt_ < FT_ACCESS_COOLDOWN_CAP) {
    // add new entry
    cooldown_[cooldown_cnt_].table_id_ = table_id;
    cooldown_[cooldown_cnt_].last_access_us_ = now_us;
    ++cooldown_cnt_;
  } else {
    // evict oldest entry
    cooldown_[oldest_idx].table_id_ = table_id;
    cooldown_[oldest_idx].last_access_us_ = now_us;
  }
}

int ObFTAccessRowScnCacheTaskMgr::init()
{
  return ObFTBackgroundTaskMgrBase<ObFTAccessRowScnCacheTask>::init(lib::TGDefIDs::FTDictAccessRowScnCache);
}

void ObFTAccessRowScnCacheTaskMgr::push(const uint64_t table_id)
{
  if (is_inited()) {
    task_.push(table_id);
  }
}

}  // namespace storage
}  // namespace oceanbase
