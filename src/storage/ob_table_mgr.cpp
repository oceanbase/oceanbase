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

#define USING_LOG_PREFIX STORAGE
#include "lib/oblog/ob_log_module.h"
#include "ob_table_mgr.h"
#include "ob_sstable.h"
#include "ob_old_sstable.h"
#include "memtable/ob_memtable.h"
#include "blocksstable/slog/ob_storage_log_struct.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_partition_service.h"
#include "storage/transaction/ob_trans_service.h"
#include "blocksstable/ob_store_file.h"
#include "share/ob_task_define.h"

using namespace oceanbase;
using namespace storage;
using namespace blocksstable;
using namespace common;
using namespace share;
using namespace lib;
using namespace memtable;

ObTableMgr::ObTableMgr()
    : is_inited_(false),
      completed_table_map_(),
      all_table_map_(),
      sstable_bucket_lock_(),
      gc_task_(),
      fast_gc_task_(),
      is_gc_started_(false)
{}

ObTableMgr::~ObTableMgr()
{
  destroy();
}

ObTableMgr& ObTableMgr::get_instance()
{
  static ObTableMgr instance;
  return instance;
}

int ObTableMgr::enable_write_log()
{
  int ret = OB_SUCCESS;
  ObBucketWLockAllGuard guard(sstable_bucket_lock_);
  LOG_INFO("enable write table mgr log");
  if (OB_FAIL(ObIRedoModule::enable_write_log())) {
    LOG_WARN("Failed to enable write log", K(ret));
  } else if (OB_FAIL(check_sstables())) {
    LOG_WARN("failed to check_sstables", K(ret));
  }
  return ret;
}

int ObTableMgr::init()
{
  int ret = OB_SUCCESS;
  const int64_t sstable_obj_size = sizeof(ObSSTable);
  const int64_t memtable_obj_size = sizeof(memtable::ObMemtable);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_FAIL(SLOGGER.register_redo_module(OB_REDO_LOG_TABLE_MGR, this))) {
    LOG_WARN("failed to register redo module", K(ret));
  } else if (OB_FAIL(completed_table_map_.init(
                 DEFAULT_HASH_MAP_BUCKETS_COUNT, ObLatchIds::TABLE_MGR_MAP, ObModIds::OB_TABLE_MGR_MAP))) {
    STORAGE_LOG(WARN, "failed to init completed_table_map_", K(ret));
  } else if (OB_FAIL(all_table_map_.init(
                 DEFAULT_HASH_MAP_BUCKETS_COUNT, ObLatchIds::TABLE_MGR_MAP, ObModIds::OB_TABLE_MGR_MAP))) {
    STORAGE_LOG(WARN, "failed to init all_table_map_", K(ret));
  } else if (OB_FAIL(sstable_bucket_lock_.init(
                 DEFAULT_HASH_MAP_BUCKETS_COUNT, ObLatchIds::TABLE_MGR_MAP, ObModIds::OB_TABLE_MGR_MAP))) {
    STORAGE_LOG(WARN, "failed to init sstable_bucket_lock", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::TableMgrGC))) {
    LOG_WARN("failed to init timer", K(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("succeed to init table mgr", K(sstable_obj_size), K(memtable_obj_size));
  }

  return ret;
}

int ObTableMgr::schedule_gc_task()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TableMgrGC, gc_task_, GC_INTERVAL_US, true /*repeat*/))) {
    LOG_WARN("failed to schedule gc task", K(ret));
  } else {
    is_gc_started_ = true;
    LOG_INFO("succeed to schedule gc task");
  }
  return ret;
}

void ObTableMgr::stop()
{
  LOG_INFO("ObTableMgr::stop");
  TG_STOP(lib::TGDefIDs::TableMgrGC);
  TG_WAIT(lib::TGDefIDs::TableMgrGC);
  LOG_INFO("timer stopped");
  ObBucketWLockAllGuard guard(sstable_bucket_lock_);
  enable_write_log_ = false;
  LOG_INFO("set disable write log");
}

void ObTableMgr::destroy()
{
  LOG_INFO("ObTableMgr::destroy", K_(is_inited));
  if (is_inited_) {
    {
      ObBucketWLockAllGuard guard(sstable_bucket_lock_);
      free_all_tables();
      is_inited_ = false;
      TG_STOP(lib::TGDefIDs::TableMgrGC);
      TG_WAIT(lib::TGDefIDs::TableMgrGC);
      completed_table_map_.destroy();
      all_table_map_.destroy();
      enable_write_log_ = false;
    }
    sstable_bucket_lock_.destroy();
  }
}

void ObTableMgr::free_all_tables()
{
  int tmp_ret = OB_SUCCESS;
  UnneedAllTableFinder finder;
  if (OB_SUCCESS != (tmp_ret = all_table_map_.foreach (finder))) {
    LOG_WARN("failed to get all tables", K(tmp_ret));
  } else {
    ObIArray<ObITable*>& tables = finder.get_tables();
    for (int64_t i = 0; i < tables.count(); ++i) {
      free_table(tables.at(i));
    }
  }
}

int ObTableMgr::free_all_sstables()
{
  int ret = OB_SUCCESS;
  UnneedAllTableFinder finder;
  if (OB_FAIL(all_table_map_.foreach (finder))) {
    LOG_WARN("fail to get all completed tables", K(ret));
  } else {
    ObIArray<ObITable*>& tables = finder.get_tables();
    int64_t free_sstable_cnt = 0;
    for (int64_t i = 0; i < tables.count(); ++i) {
      if (tables.at(i)->is_sstable()) {
        if (OB_FAIL(all_table_map_.erase(reinterpret_cast<uint64_t>(tables.at(i))))) {
          LOG_WARN("fail to erase from all table", K(ret));
        } else if (OB_FAIL(completed_table_map_.erase(tables.at(i)->get_key()))) {
          LOG_WARN("fail to erase from completed sstable", K(ret));
        } else {
          free_table(tables.at(i));
          ++free_sstable_cnt;
        }
      }
    }
    LOG_INFO("free all sstables", K(free_sstable_cnt));
  }
  return ret;
}

int ObTableMgr::create_memtable(ObITable::TableKey& table_key, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  memtable::ObMemtable* table = NULL;
  const bool skip_version_range = true;
  const bool skip_log_ts_range = true;
  ObTimeGuard timeguard("create_memtable", 1000L * 1000L);
  AllTableNode* node = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_key.is_valid(skip_version_range, skip_log_ts_range) ||
             ObITable::MEMTABLE != table_key.table_type_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(table_key));
  } else {
    hash_ret = completed_table_map_.exist(table_key);
    if (OB_HASH_EXIST == hash_ret) {
      ret = OB_ENTRY_EXIST;
      STORAGE_LOG(WARN, "table has exist, cannot create twice", K(ret), K(table_key));
    } else if (OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret;
      STORAGE_LOG(WARN, "failed to get table", K(ret), K(table_key));
    } else if (OB_FAIL(alloc_table(table))) {
      STORAGE_LOG(WARN, "failed to alloc table", K(ret));
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret));
    } else if (OB_FAIL(table->init(table_key))) {
      STORAGE_LOG(WARN, "failed to init memtable", K(ret), K(table_key));
    } else if (!table->is_memtable()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table type not match", K(ret), K(table_key), K(*table));
    } else if (OB_FAIL(handle.set_table(table))) {
      STORAGE_LOG(WARN, "failed to set table to handle", K(ret));
    } else if (OB_ISNULL(node = all_table_map_.alloc_node(*table))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to new all table node", K(ret));
    } else if (OB_FAIL(all_table_map_.put(*node))) {
      LOG_WARN("Failed to set sstable", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO,
          "succeed to create memtable",
          "total_count",
          all_table_map_.get_count(),
          K(table_key),
          K(*table),
          K(lbt()));
      table = NULL;
      node = NULL;
    }
  }

  if (NULL != node) {
    all_table_map_.free_node(node);
  }

  if (NULL != table) {
    handle.reset();
    free_table(table);
    table = NULL;
  }

  return ret;
}

int ObTableMgr::acquire_old_table(const ObITable::TableKey& table_key, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  CompletedTableGetFunctor get_functor(handle);

  ObBucketHashRLockGuard bucket_guard(sstable_bucket_lock_, table_key.hash());
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(table_key));
  } else if (OB_SUCCESS != (hash_ret = completed_table_map_.get(table_key, get_functor))) {
    if (OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to inner get table", K(ret), K(table_key));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  } else if (OB_ISNULL(handle.get_table())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("node or item must not null", K(ret), K(table_key), K(handle));
  }

  return ret;
}

int ObTableMgr::CompletedTableGetFunctor::operator()(ObITable& table)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(handle_.set_table(&table))) {
    LOG_WARN("failed to set table", K(ret));
  }
  return ret;
}

int ObTableMgr::release_table(ObITable* table)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(table));
  } else {
    int64_t ref = table->dec_ref();
    if (0 == ref) {
      if (table->is_memtable()) {
        const int64_t minor_merged_time = static_cast<ObMemtable*>(table)->get_minor_merged_time();
        if (0 < minor_merged_time) {
          const int64_t SLOW_RELEASE_THRESHOLD = 10L * 60L * 1000L * 1000L;  // 10 min
          const int64_t release_time = ObTimeUtility::current_time();
          const int64_t warmup_time = GCONF.minor_warm_up_duration_time;
          if (release_time - minor_merged_time > std::max(SLOW_RELEASE_THRESHOLD, warmup_time * 5)) {
            ObTaskController::get().allow_next_syslog();
            LOG_WARN("memtable last ref release too late",
                K(table->get_key()),
                K(minor_merged_time),
                K(release_time),
                K(lbt()));
          }
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = schedule_release_task())) {
        LOG_WARN("failed to schedule_release_task", K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObTableMgr::clear_unneed_tables()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!enable_write_log_) {
    LOG_INFO("write log is disable, no need clear unneed tables");
  } else if (OB_FAIL(clear_unneed_completed_tables())) {
    LOG_WARN("failed to clear_unneed_completed_tables", K(ret));
  } else if (OB_FAIL(clear_unneed_all_tables())) {
    LOG_WARN("failed to clear unneed tables", K(ret));
  }
  return ret;
}

// caller hold lock_
int ObTableMgr::remove_completed_sstables(common::ObIArray<ObITable*>& del_tables)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (del_tables.empty()) {
    // do nothing
  } else if (OB_FAIL(SLOGGER.begin(OB_LOG_TABLE_MGR))) {
    STORAGE_LOG(WARN, "Fail to begin daily merge log, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < del_tables.count(); ++i) {
      if (!del_tables.at(i)->is_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("not sstables", K(ret), K(i), "table", *del_tables.at(i));
      } else if (OB_FAIL(write_delete_sstable_log(del_tables.at(i)->get_key()))) {
        LOG_WARN("failed to write delete sstable log", K(ret), "table_key", del_tables.at(i)->get_key());
      }
    }
    if (OB_SUCC(ret)) {
      int64_t lsn = 0;
      if (OB_FAIL(SLOGGER.commit(lsn))) {
        STORAGE_LOG(ERROR, "Fail to commit logger, ", K(ret), K(lsn));
      } else {
        LOG_INFO("succeed to commit delete sstables", K(ret), K(lsn), "count", del_tables.count());
        for (int64_t i = 0; OB_SUCC(ret) && i < del_tables.count(); ++i) {
          const ObITable* table = del_tables.at(i);
          if (OB_FAIL(completed_table_map_.erase(table->get_key()))) {
            LOG_ERROR("failed to erase table from table map, fatal error, abort now", K(ret), K(*table));
            ob_abort();
          }
        }
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        STORAGE_LOG(ERROR, "delete sstable logger abort error", K(tmp_ret), K(del_tables));
      }
    }
  }
  cost_ts = ObTimeUtility::current_time() - cost_ts;
  LOG_INFO("remove_completed_sstables", K(ret), "count", del_tables.count(), K(cost_ts));
  return ret;
}

int ObTableMgr::remove_unused_callback_for_uncommited_txn_(ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService* trans_service = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(trans_service = ObPartitionService::get_instance().get_trans_service())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "can't get trans service", K(ret));
  } else if (OB_ISNULL(mt)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "can't get memtable", K(ret));
  } else if (OB_FAIL(trans_service->remove_callback_for_uncommited_txn(mt))) {
    TRANS_LOG(WARN, "remove callback for uncommited txn failed", K(ret), KP(mt));
  }

  return ret;
}

int ObTableMgr::remove_completed_memtables(common::ObIArray<ObITable*>& del_memtables)
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < del_memtables.count(); ++i) {
      ObITable* memtable = del_memtables.at(i);
      if (!memtable->is_memtable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("not memtable", K(ret), K(*memtable));
      } else if (OB_FAIL(completed_table_map_.erase(memtable->get_key()))) {
        LOG_WARN("failed to del completed memtable", K(ret), K(*memtable));
      } else {
        LOG_INFO("delete completed memtable", K(*memtable));
      }
    }
  }

  cost_ts = ObTimeUtility::current_time() - cost_ts;
  LOG_INFO("remove_completed_memtables", K(ret), "count", del_memtables.count(), K(cost_ts));
  return ret;
}

int ObTableMgr::clear_unneed_completed_tables()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_DEL_COUNT = 1024;
  bool need_next = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }

  while (OB_SUCC(ret) && need_next) {
    if (OB_FAIL(clear_unneed_completed_tables(MAX_DEL_COUNT, need_next))) {
      LOG_WARN("failed to clear_unneed_completed_tables", K(ret));
    }
  }
  return ret;
}

int ObTableMgr::clear_unneed_completed_tables(const int64_t max_del_count, bool& need_next)
{
  int ret = OB_SUCCESS;
  UnneedCompletedTableFinder finder;
  ObArray<ObITable*> del_sstables;
  ObArray<ObITable*> del_memtables;
  need_next = false;
  int64_t total_cost_ts = ObTimeUtility::current_time();
  int64_t cost_ts = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!enable_write_log_) {
    LOG_INFO("write log is disable, no need clear unneed completed tables");
  } else if (OB_FAIL(finder.init(max_del_count))) {
    LOG_WARN("failed to init finder", K(ret), K(max_del_count));
  } else if (OB_FAIL(completed_table_map_.foreach (finder))) {
    LOG_WARN("failed to foreach find unneed completed table", K(ret));
  }

  if (OB_SUCC(ret)) {
    const bool is_write_lock = true;
    ObMultiBucketLockGuard lock_guard(sstable_bucket_lock_, is_write_lock);

    if (OB_FAIL(lock_multi_bucket_lock(lock_guard, finder.get_all_tables()))) {
      LOG_WARN("failed to lock delete locks", K(ret));
    } else if (OB_FAIL(finder.get_tables(del_sstables, del_memtables))) {
      LOG_WARN("failed to get finder tables", K(ret));
    } else if (OB_FAIL(remove_completed_memtables(del_memtables))) {
      LOG_WARN("Failed to remove completed memtables", K(ret));
    } else if (OB_FAIL(remove_completed_sstables(del_sstables))) {
      LOG_ERROR("failed to remove_completed_tables", K(ret));
    } else {
      need_next = finder.is_full();
    }
  }
  total_cost_ts = ObTimeUtility::current_time() - total_cost_ts;
  cost_ts = ObTimeUtility::current_time() - cost_ts;
  LOG_INFO("finish clear_unneed_completed_tables",
      K(total_cost_ts),
      K(cost_ts),
      K(need_next),
      "del_memtables",
      del_memtables.count(),
      "del_sstables",
      del_sstables.count());
  return ret;
}

int ObTableMgr::clear_unneed_all_tables()
{
  int ret = OB_SUCCESS;
  UnneedAllTableFinder finder;
  AllTableEraseChecker checker;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!enable_write_log_) {
    LOG_INFO("write log is disable, no need clear unneed completed tables");
  } else if (OB_FAIL(all_table_map_.foreach (finder))) {
    LOG_WARN("failed to foreach find unneed table", K(ret));
  } else {
    common::ObIArray<ObITable*>& tables = finder.get_tables();
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      ObITable* table = tables.at(i);

      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret));
      } else {
        ObITable* del_table = NULL;
        if (!table->is_memtable()) {
          ret = OB_ERR_SYS;
          LOG_ERROR("not memtable", K(ret), K(*table));
        } else if (OB_FAIL(remove_unused_callback_for_uncommited_txn_(static_cast<ObMemtable*>(table)))) {
          LOG_WARN("failed to remove callback for uncommited txn", K(ret), K(*table));
        } else if (OB_FAIL(all_table_map_.erase(reinterpret_cast<uint64_t>(table), del_table, &checker))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("Failed to del from all_table_map", K(ret), K(*table));
          } else {
            ret = OB_SUCCESS;
            continue;
          }
        } else {
          LOG_INFO("del unneed table", K(*del_table));
          free_table(del_table);
          del_table = NULL;
        }
      }
    }
  }

  return ret;
}

int ObTableMgr::get_all_tables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  int64_t total_cost_ts = ObTimeUtility::current_time();
  ObITable* table = NULL;
  AllTableMap::Iterator iter(all_table_map_);

  int64_t cost_ts = ObTimeUtility::current_time();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(handle.reserve(all_table_map_.get_count()))) {
    LOG_WARN("failed to reserve handle", K(ret), "count", all_table_map_.get_count());
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next(table))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
      break;
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("node must not null", K(ret), KP(table));
    } else if (OB_FAIL(handle.add_table(table))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }

  total_cost_ts = ObTimeUtility::current_time() - total_cost_ts;
  cost_ts = ObTimeUtility::current_time() - cost_ts;
  LOG_INFO("get all tables", K(total_cost_ts), K(cost_ts), "count", handle.get_count());
  return ret;
}

int ObTableMgr::get_completed_sstables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObITable* table = NULL;
  handle.reset();

  ObBucketTryRLockAllGuard guard(sstable_bucket_lock_);
  CompletedTableMap::Iterator iter(completed_table_map_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(guard.get_ret())) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("failed to get_completed_sstables lock", K(ret));
    } else {
      LOG_INFO("failed to get_completed_sstables lock", K(ret));
    }
  } else if (OB_FAIL(handle.reserve(completed_table_map_.get_count()))) {
    LOG_WARN("failed to reserve handle", K(ret), "count", completed_table_map_.get_count());
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next(table))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
      break;
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("node must not null", K(ret), KP(table));
    } else if (!table->is_sstable()) {
      // do nothing
    } else if (OB_FAIL(handle.add_table(table))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }
  return ret;
}

int ObTableMgr::load_sstable(const char* buf, int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObOldSSTable tmp_sstable;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(tmp_sstable.deserialize(buf, buf_len, pos))) {
    LOG_ERROR("failed to deserialize sstable", K(ret));
  } else if (OB_FAIL(add_replay_sstable_to_map(false /*is_replay*/, tmp_sstable))) {
    LOG_WARN("failed to add_replay_sstable_to_map", K(ret), K(tmp_sstable));
  }
  return ret;
}

int ObTableMgr::complete_sstables(ObTablesHandle& handle, const bool use_inc_macro_block_slog)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObTableMgr::CompletedTableNode*> node_array;
  ObTableMgr::CompletedTableNode* completed_table_node = NULL;
  int64_t cost_ts = ObTimeUtility::current_time();
  const bool is_write_lock = true;
  ObMultiBucketLockGuard lock_guard(sstable_bucket_lock_, is_write_lock);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(lock_multi_bucket_lock(lock_guard, handle.get_tables()))) {
    LOG_WARN("failed to lock_multi_bucket_lock", K(ret));
  } else if (OB_FAIL(check_can_complete_sstables(handle))) {
    LOG_WARN("failed to check can complete sstables", K(ret));
  } else if (OB_FAIL(SLOGGER.begin(OB_LOG_TABLE_MGR))) {
    STORAGE_LOG(WARN, "Fail to begin daily merge log, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
      ObSSTable* sstable = static_cast<ObSSTable*>(handle.get_table(i));
      if (OB_FAIL(write_complete_sstable_log(*sstable, use_inc_macro_block_slog))) {
        LOG_WARN("failed to write complete sstable log", K(ret), K(*sstable));
      } else if (OB_ISNULL(completed_table_node = completed_table_map_.alloc_node(*sstable))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to new completed_table_node", K(ret));
      } else if (OB_FAIL(node_array.push_back(completed_table_node))) {
        LOG_WARN("failed to add node array", K(ret));
        completed_table_map_.free_node(completed_table_node);
      }
    }

    if (OB_SUCC(ret)) {
      int64_t lsn = 0;
      if (OB_FAIL(SLOGGER.commit(lsn))) {
        STORAGE_LOG(ERROR, "Fail to commit logger, ", K(ret), K(lsn));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < node_array.count(); ++i) {
          if (OB_FAIL(completed_table_map_.put(*node_array.at(i)))) {
            LOG_ERROR(
                "failed to set table map, fatal error. abort now.", K(ret), K(i), "table", node_array.at(i)->item_);
            ob_abort();
          } else {
            node_array.at(i) = NULL;
          }
        }
        cost_ts = ObTimeUtility::current_time() - cost_ts;
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("succeed to complete sstables", K(cost_ts), K(lsn), K(handle));
      }
    } else if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        LOG_WARN("create sstable logger abort error", K(tmp_ret));
      } else {
        LOG_WARN("failed to complete sstables", K(ret), K(handle));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < node_array.count(); ++i) {
    if (NULL != node_array.at(i)) {
      completed_table_map_.free_node(node_array.at(i));
    }
  }
  return ret;
}

int ObTableMgr::complete_sstable(ObTableHandle& handle, const bool use_inc_macro_block_slog)
{
  int ret = OB_SUCCESS;
  ObTablesHandle batch_handle;

  if (OB_FAIL(OB_STORE_FILE.fsync())) {
    LOG_WARN("failed to fsync store file", K(ret));
  } else if (OB_FAIL(batch_handle.add_table(handle))) {
    LOG_WARN("Failed to add table", K(ret));
  } else if (OB_FAIL(complete_sstables(batch_handle, use_inc_macro_block_slog))) {
    LOG_WARN("Failed to complete sstables", K(ret), K(handle), K(batch_handle));
  }
  return ret;
}

// caller own the sstable_lock_
int ObTableMgr::check_can_complete_sstables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
      ObITable* table = handle.get_table(i);
      hash_ret = completed_table_map_.exist(table->get_key());

      if (OB_HASH_EXIST == hash_ret) {
        ret = OB_ENTRY_EXIST;
        LOG_WARN("table has exist in table_map, cannot complete twice", K(ret), KP(table), K(*table), K(handle));
      } else if (OB_HASH_NOT_EXIST != hash_ret) {
        ret = hash_ret;
        LOG_WARN("failed to get table map", K(ret), K(*table));
      } else if (!table->is_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("cannot complete sstable with not sstable", K(ret), K(*table));
      }
    }
  }
  return ret;
}

int ObTableMgr::complete_memtable(
    memtable::ObMemtable* memtable, const int64_t snapshot_version, const int64_t freeze_log_ts)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObTableMgr::CompletedTableNode* new_node = NULL;
  int64_t cost_ts = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(memtable) || ObVersionRange::MAX_VERSION == snapshot_version ||
             snapshot_version <= ObVersionRange::MIN_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(memtable), K(snapshot_version));
  } else if (memtable->get_base_version() >= snapshot_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("snapshot_version is too small", K(ret), K(snapshot_version), K(*memtable));
  } else if (ObVersionRange::MAX_VERSION != memtable->get_snapshot_version()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("memtable cannot frozen twice", K(ret), K(snapshot_version), K(*memtable));
  } else {
    ObITable::TableKey table_key = memtable->get_key();
    table_key.trans_version_range_.snapshot_version_ = snapshot_version;
    hash_ret = completed_table_map_.exist(table_key);
    if (OB_HASH_EXIST == hash_ret) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("same memtable exists, cannot complete twice", K(ret), K(table_key));
    } else if (OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret;
      LOG_WARN("Failed to get memtable", K(ret), K(table_key));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(new_node = completed_table_map_.alloc_node(*memtable))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc table node", K(ret));
      } else if (OB_FAIL(memtable->set_snapshot_version(snapshot_version))) {
        LOG_ERROR("failed to set snapshot version", K(ret), K(*memtable));
      } else if (OB_FAIL(memtable->set_end_log_ts(freeze_log_ts))) {
        LOG_ERROR("failed to set snapshot version", K(ret), K(*memtable));
      } else if (OB_FAIL(completed_table_map_.put(*new_node))) {
        LOG_WARN("failed to put memtable node, fatal error. abort now", K(ret), K(table_key));
        ob_abort();
      } else {
        new_node = NULL;
        cost_ts = ObTimeUtility::current_time() - cost_ts;
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("succeed to complete memtable", K(cost_ts), K(table_key), K(*memtable));
      }
    }
  }

  if (NULL != new_node) {
    completed_table_map_.free_node(new_node);
  }
  return ret;
}

int ObTableMgr::write_delete_sstable_log(const ObITable::TableKey& table_key)
{
  UNUSEDx(table_key);
  return OB_NOT_SUPPORTED;
}

int ObTableMgr::write_complete_sstable_log(ObSSTable& sstable, const bool use_inc_macro_block_slog)
{
  UNUSEDx(sstable, use_inc_macro_block_slog);
  return OB_NOT_SUPPORTED;
}

int ObTableMgr::replay(const ObRedoModuleReplayParam& param)
{
  int ret = OB_SUCCESS;
  const int64_t subcmd = param.subcmd_;
  const char* buf = param.buf_;
  const int64_t len = param.buf_len_;
  ObRedoLogMainType main_type = OB_REDO_LOG_MAX;
  int32_t sub_type = 0;
  ObIRedoModule::parse_subcmd(param.subcmd_, main_type, sub_type);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(param));
  } else if (OB_REDO_LOG_TABLE_MGR != main_type) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "wrong redo log type.", K(ret), K(main_type), K(sub_type));
  } else {
    switch (sub_type) {
      case REDO_LOG_CREATE_SSTABLE: {
        if (OB_FAIL(replay_create_sstable(buf, len))) {
          LOG_WARN("failed to replay create sstable", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_COMPELTE_SSTABLE: {
        if (OB_FAIL(replay_complete_sstable(buf, len))) {
          LOG_WARN("failed to replay complete sstable", K(ret), K(param));
        }
        break;
      }
      case REDO_LOG_DELETE_SSTABLE: {
        if (OB_FAIL(replay_delete_sstable(buf, len))) {
          LOG_WARN("failed to replay delete sstable", K(ret), K(param));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown subtype", K(ret), K(sub_type), K(param));
      }
    }
  }

  return ret;
}

int ObTableMgr::replay_create_sstable(const char* buf, const int64_t buf_len)
{
  UNUSEDx(buf, buf_len);
  int ret = OB_NOT_SUPPORTED;
  STORAGE_LOG(ERROR, "create sstable slog is not supported now", K(ret));
  return ret;
}

int ObTableMgr::replay_complete_sstable(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObOldSSTable tmp_sstable;
  int64_t pos = 0;
  ObCompleteSSTableLogEntry log_entry(tmp_sstable);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to decode log entry", K(ret));
  } else if (OB_FAIL(add_replay_sstable_to_map(true /*is_replay*/, tmp_sstable))) {
    LOG_WARN("failed to replay sstable", K(ret), K(tmp_sstable));
  } else {
    LOG_INFO("succeed to replay complete sstable", "table_key", tmp_sstable.get_key());
  }
  return ret;
}

int ObTableMgr::add_replay_sstable_to_map(const bool is_replay, ObOldSSTable& tmp_sstable)
{
  int ret = OB_SUCCESS;
  const ObITable::TableKey& table_key = tmp_sstable.get_key();
  ObOldSSTable* sstable = NULL;
  ObTableMgr::CompletedTableNode* completed_table_node = NULL;
  ObTableMgr::AllTableNode* all_table_node = NULL;
  uint64_t tenant_id = OB_INVALID_ID;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (!tmp_sstable.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tmp_sstable));
  } else if (OB_INVALID_ID == (tenant_id = tmp_sstable.get_key().get_tenant_id())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid tenant_id", K(ret), K(table_key));
  } else if (OB_FAIL(alloc_table(sstable))) {
    LOG_ERROR("failed to new sstable", K(ret));
  } else if (OB_FAIL(sstable->set_sstable(tmp_sstable))) {
    LOG_WARN("failed to set_sstable", K(ret));
  } else if (!sstable->is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable not valid", K(ret), K(*sstable));
  } else if (OB_ISNULL(completed_table_node = completed_table_map_.alloc_node(*sstable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to new completed_table_node", K(ret));
  } else if (OB_ISNULL(all_table_node = all_table_map_.alloc_node(*sstable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to new all_table_node", K(ret));
  } else {
    ret = completed_table_map_.exist(table_key);
    if (OB_HASH_NOT_EXIST != ret && OB_HASH_EXIST != ret) {
      LOG_ERROR("failed to check table", K(ret), K(table_key));
    } else if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(all_table_map_.put(*all_table_node))) {
        LOG_ERROR("put all table node fail", K(ret));
      } else if (FALSE_IT(all_table_node = nullptr)) {
      } else if (FALSE_IT(sstable = nullptr)) {
      } else if (OB_FAIL(completed_table_map_.put(*completed_table_node))) {
        LOG_ERROR("put completed table node fail", K(ret));
      } else {
        completed_table_node = nullptr;
      }
    } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000 || !is_replay) {
      LOG_ERROR("meet duplicate sstable when upgrade, need check", K(ret), K(*sstable), K(is_replay));
    } else {
      FLOG_INFO("meet duplicate sstable when replay", K(table_key));
      ObITable* del_table = nullptr;
      AllTableEraseChecker checker;

      if (OB_FAIL(completed_table_map_.erase(table_key, del_table))) {
        LOG_WARN("failed to erase table_map", K(ret), K(table_key));
      } else if (OB_FAIL(all_table_map_.erase(reinterpret_cast<uint64_t>(del_table), del_table, &checker))) {
        LOG_WARN("failed to erase table", K(ret), K(table_key));
      } else if (OB_FAIL(all_table_map_.put(*all_table_node))) {
        LOG_ERROR("put all table node fail", K(ret));
      } else if (FALSE_IT(all_table_node = nullptr)) {
      } else if (FALSE_IT(sstable = nullptr)) {
      } else if (OB_FAIL(completed_table_map_.put(*completed_table_node))) {
        LOG_ERROR("put completed table node fail", K(ret));
      } else {
        completed_table_node = nullptr;
      }
    }
  }

  if (NULL != sstable) {
    free_table(sstable);
    sstable = NULL;
  }
  if (NULL != all_table_node) {
    all_table_map_.free_node(all_table_node);
  }
  if (NULL != completed_table_node) {
    completed_table_map_.free_node(completed_table_node);
  }
  return ret;
}

int ObTableMgr::replay_delete_sstable(const char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObDeleteSSTableLogEntry log_entry;
  int64_t pos = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(log_entry.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to decode log entry", K(ret));
  } else if (OB_FAIL(replay_delete_sstable(log_entry.table_key_))) {
    LOG_WARN("failed to replay_delete_sstable", K(ret), K(log_entry));
  } else {
    STORAGE_LOG(INFO, "succeed to replay delete sstable", K(log_entry));
  }

  return ret;
}

int ObTableMgr::replay_delete_sstable(const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  ObITable* del_table = NULL;
  AllTableEraseChecker checker;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition service is not initialized", K(ret));
  } else if (OB_FAIL(completed_table_map_.erase(table_key, del_table))) {
    STORAGE_LOG(WARN, "failed to erase table_map", K(ret), K(table_key));
  } else if (OB_FAIL(all_table_map_.erase(reinterpret_cast<uint64_t>(del_table), del_table, &checker)) &&
             OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("failed to erase table", K(ret), K(table_key));
  } else if (OB_HASH_NOT_EXIST == ret) {
    FLOG_INFO("table not exist when replay delete sstable", K(ret), K(table_key));
    ret = OB_SUCCESS;
  } else {
    STORAGE_LOG(INFO, "succeed to replay delete sstable", K(table_key));
    free_table(del_table);
    del_table = NULL;
  }
  return ret;
}

int ObTableMgr::parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObRedoLogMainType main_type = OB_REDO_LOG_TABLE_MGR;
  int32_t sub_type = 0;

  ObIRedoModule::parse_subcmd(subcmd, main_type, sub_type);  // this func has no ret
  if (NULL == buf || len <= 0 || NULL == stream) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is invalid", K(ret), K(buf), K(len), K(stream));
  } else if (OB_REDO_LOG_TABLE_MGR != main_type) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "wrong redo log type.", K(ret), K(main_type), K(sub_type));
  } else {
    switch (sub_type) {
      case REDO_LOG_CREATE_SSTABLE: {
        ObSSTable sstable;
        ObCreateSSTableLogEntry entry(sstable);
        if (OB_FAIL(entry.deserialize(buf, len, pos))) {
          STORAGE_LOG(WARN, "Fail to deserialize create sstable log entry, ", K(ret));
        } else if (0 > fprintf(stream, "create sstable.\n%s\n", to_cstring(entry))) {
          ret = OB_IO_ERROR;
          STORAGE_LOG(WARN, "failed to create sstable log", K(ret), K(entry));
        }
        break;
      }
      case REDO_LOG_COMPELTE_SSTABLE: {
        ObOldSSTable sstable;
        ObCompleteSSTableLogEntry entry(sstable);
        if (OB_FAIL(entry.deserialize(buf, len, pos))) {
          STORAGE_LOG(WARN, "Fail to deserialize complete sstable log entry, ", K(ret));
        } else if (0 > fprintf(stream, "complete sstable.\n%s\n", to_cstring(entry))) {
          ret = OB_IO_ERROR;
          STORAGE_LOG(WARN, "failed to set complete sstable log", K(ret), K(entry));
        }
        break;
      }
      case REDO_LOG_DELETE_SSTABLE: {
        PrintSLogEntry(ObDeleteSSTableLogEntry);
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown subtype", K(ret), K(sub_type));
      }
    }
  }

  return OB_SUCCESS;
}

int ObTableMgr::replay_add_old_sstable(ObSSTable* sstable)
{
  UNUSED(sstable);
  return OB_NOT_SUPPORTED;
}

int ObTableMgr::replay_del_old_sstable(const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (OB_FAIL(replay_delete_sstable(table_key))) {
    LOG_WARN("failed to replay_delete_sstable", K(ret), K(table_key));
  } else {
    LOG_INFO("succeed to replay_del_old_sstable", K(table_key));
  }
  return ret;
}

int ObTableMgr::schedule_release_task()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t delay = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition service is not initialized", K(ret));
  } else if (is_gc_started_) {
    if (OB_SUCCESS != (tmp_ret = TG_CANCEL_R(lib::TGDefIDs::TableMgrGC, fast_gc_task_))) {
      LOG_WARN("failed to cancel fast_gc_task", K(tmp_ret));
    }
    if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TableMgrGC, fast_gc_task_, delay))) {
      LOG_WARN("failed to schedule fast gc task", K(ret));
    }
  }
  return ret;
}

ObTableMgrGCTask::ObTableMgrGCTask()
{}

ObTableMgrGCTask::~ObTableMgrGCTask()
{}

void ObTableMgrGCTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = ObTableMgr::get_instance().clear_unneed_tables())) {
    LOG_INFO("failed to clear_unneed_tables", K(tmp_ret));
  }
}

int ObTableMgr::check_sstables()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObITable* table = NULL;
  AllTableMap::Iterator iter(all_table_map_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table mgr is not initialized", K(ret));
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next(table))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
      break;
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("node must not null", K(ret), KP(table));
    } else if (!table->is_sstable()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("no memtable should exists before enable replay", K(ret), K(*table));
    }
    ++count;
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("check_sstables", K(ret), K(count), K(cost_ts));
  return ret;
}

ObTableMgr::UnneedCompletedTableFinder::UnneedCompletedTableFinder()
    : is_inited_(false), max_batch_count_(0), is_full_(false), del_tables_()
{}

ObTableMgr::UnneedCompletedTableFinder::~UnneedCompletedTableFinder()
{}

int ObTableMgr::UnneedCompletedTableFinder::init(const int64_t max_batch_count)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (max_batch_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(max_batch_count));
  } else {
    is_inited_ = true;
    max_batch_count_ = max_batch_count;
    is_full_ = false;
  }
  return ret;
}

int ObTableMgr::UnneedCompletedTableFinder::operator()(ObITable& table, bool& is_full)
{
  int ret = OB_SUCCESS;
  is_full = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (table.get_ref() == 0) {
    if (OB_FAIL(del_tables_.push_back(&table))) {
      LOG_WARN("failed to push back table", K(ret));
    } else if (del_tables_.count() >= max_batch_count_) {
      is_full = true;
    }
  }
  return ret;
}

int ObTableMgr::UnneedCompletedTableFinder::get_tables(
    common::ObIArray<ObITable*>& del_sstables, common::ObIArray<ObITable*>& del_memtables)
{
  int ret = OB_SUCCESS;
  del_sstables.reset();
  del_memtables.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < del_tables_.count(); ++i) {
      ObITable* table = del_tables_.at(i);
      if (table->get_ref() != 0) {
        continue;
      } else if (table->is_sstable()) {
        if (OB_FAIL(del_sstables.push_back(table))) {
          LOG_WARN("failed to push del sstable", K(ret));
        }
      } else if (OB_FAIL(del_memtables.push_back(table))) {
        LOG_WARN("failed to push del memtables", K(ret));
      }
    }
  }
  return ret;
}

int ObTableMgr::UnneedAllTableFinder::operator()(ObITable& table, bool& is_full)
{
  int ret = OB_SUCCESS;
  is_full = false;

  if (table.get_ref() == 0) {
    if (OB_FAIL(del_tables_.push_back(&table))) {
      LOG_WARN("failed to push back table", K(ret));
    }
  } else if (table.get_ref() < 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("ref count must not less than 0", K(ret), K(table));
  }
  return ret;
}

int ObTableMgr::AllTableEraseChecker::operator()(ObITable& table)
{
  int ret = OB_SUCCESS;

  if (table.get_ref() != 0) {
    ret = OB_EAGAIN;
  }
  return ret;
}

template <class T>
int ObTableMgr::alloc_table(T*& table, const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  table = NULL;

  ObMemAttr memattr(tenant_id);
  if (typeid(T) == typeid(ObOldSSTable)) {
    memattr.label_ = ObModIds::OB_SSTABLE;
    memattr.ctx_id_ = ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID;
  } else if (typeid(T) == typeid(ObSSTable)) {
    // TODO(): will be removed when create sstable is disabled
    memattr.label_ = ObModIds::OB_SSTABLE;
    memattr.ctx_id_ = ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID;
  } else if (typeid(T) == typeid(ObMemtable)) {
    memattr.label_ = ObModIds::OB_MEMTABLE_OBJECT;
    memattr.ctx_id_ = ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID;
  } else {
    ret = OB_ERR_ILLEGAL_TYPE;
    LOG_WARN("cannot alloc non table type", K(ret), "type name", typeid(T).name());
  }

  if (OB_SUCC(ret) && OB_ISNULL(table = OB_NEW_ALIGN32(T, memattr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc table", K(ret), K(memattr), K(tenant_id));
  }

  return ret;
}

void ObTableMgr::free_table(ObITable* table)
{
  if (NULL != table) {
    LOG_INFO("free table", K(*table));
    if (table->is_sstable()) {
      ObOldSSTable* sstable = static_cast<ObOldSSTable*>(table);
      OB_DELETE_ALIGN32(ObOldSSTable, unused, sstable);
    } else {
      ObMemtable* memtable = static_cast<ObMemtable*>(table);
      OB_DELETE_ALIGN32(ObMemtable, unused, memtable);
    }
    table = NULL;
  }
}

int ObTableMgr::lock_multi_bucket_lock(common::ObMultiBucketLockGuard& guard, common::ObIArray<ObITable*>& tables)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> hash_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tables.count() == 0) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      if (OB_ISNULL(tables.at(i))) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret), K(i), K(tables));
      } else {
        const ObITable::TableKey& table_key = tables.at(i)->get_key();
        if (OB_FAIL(hash_array.push_back(table_key.hash()))) {
          LOG_WARN("failed to add hash array", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(guard.lock_multi_buckets(hash_array))) {
        LOG_WARN("failed to lock multi buckets", K(ret), K(hash_array));
      }
    }
  }
  return ret;
}

int ObTableMgr::check_tenant_sstable_exist(const uint64_t tenant_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  ObITable* table = NULL;
  is_exist = false;

  CompletedTableMap::Iterator iter(completed_table_map_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_next(table))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
      break;
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("node must not null", K(ret), KP(table));
    } else if (!table->is_sstable()) {
      // do nothing
    } else if (table->get_partition_key().get_tenant_id() == tenant_id) {
      is_exist = true;
      break;
    }
  }
  return ret;
}
