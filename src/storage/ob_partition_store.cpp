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
#include "ob_partition_store.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_sstable_checksum_operator.h"
#include "share/ob_debug_sync.h"
#include "share/ob_force_print_log.h"
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_storage.h"
#include "ob_partition_log.h"
#include "ob_table_mgr.h"
#include "ob_partition_split.h"
#include "storage/ob_freeze_info_snapshot_mgr.h"
#include "ob_storage_struct.h"
#include "ob_pg_memtable_mgr.h"
#include "storage/ob_partition_scheduler.h"

using namespace oceanbase;
using namespace storage;
using namespace common;
using namespace lib;
using namespace share;
using namespace blocksstable;
using namespace memtable;

void ObPartitionStoreInfo::reset()
{
  is_restore_ = -1;
  migrate_status_ = -1;
  migrate_timestamp_ = -1;
  replica_type_ = -1;
  split_state_ = -1;
  multi_version_start_ = -1;
  report_version_ = -1;
  report_row_count_ = -1;
  report_data_checksum_ = -1;
  report_data_size_ = -1;
  report_required_size_ = -1;
  readable_ts_ = -1;
}

TableStoreStat::TableStoreStat() : index_id_(0), has_dropped_flag_(false)
{}

ObPartitionStore::ObPartitionStore()
    : is_inited_(false),
      is_removed_(false),
      meta_buf_(),
      meta_(NULL),
      log_seq_num_(0),
      store_map_(nullptr),
      is_freezing_(false),
      pg_memtable_mgr_(NULL),
      lock_(ObLatchIds::PARTITION_STORE_LOCK),
      freeze_info_mgr_(NULL),
      pkey_(),
      pg_(nullptr),
      cached_data_table_store_(nullptr),
      allocator_()
{
  meta_buf_[0].reset();
  meta_buf_[1].reset();
  meta_ = &meta_buf_[0];
}

ObPGPartitionStoreMeta& ObPartitionStore::get_next_meta()
{
  ObPGPartitionStoreMeta* meta = &meta_buf_[0];
  if (meta == meta_) {
    meta = &meta_buf_[1];
  }
  return *meta;
}

void ObPartitionStore::switch_meta()
{
  if (meta_ == &meta_buf_[0]) {
    meta_ = &meta_buf_[1];
  } else {
    meta_ = &meta_buf_[0];
  }
  LOG_INFO("switch meta", K(*meta_));
}

ObPartitionStore::~ObPartitionStore()
{
  destroy();
}

int ObPartitionStore::create_partition_store(const ObPGPartitionStoreMeta& meta, const bool write_slog,
    ObIPartitionGroup* pg, ObFreezeInfoSnapshotMgr& freeze_info_mgr, ObPGMemtableMgr* pg_memtable_mgr)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("create_partition_store", 1000L * 1000L);

  if (is_inited_) {
    LOG_INFO("partition store has been created, skip it", K(meta));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("the partition has been removed, ", K(ret));
  } else if (!meta.is_valid() || OB_ISNULL(pg_memtable_mgr) || OB_ISNULL(pg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(meta), KP(pg_memtable_mgr), KP(pg));
  } else {
    TCWLockGuard lock_guard(lock_);
    pg_memtable_mgr_ = pg_memtable_mgr;
    if (OB_FAIL(init(meta, pg, freeze_info_mgr))) {
      LOG_WARN("failed to init meta", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (write_slog && OB_FAIL(write_create_partition_store_log(meta))) {
      LOG_WARN("failed to write_create_partition_store_log", K(ret), K(meta));
    }
  }

  return ret;
}

int ObPartitionStore::init(
    const ObPGPartitionStoreMeta& meta, ObIPartitionGroup* pg, ObFreezeInfoSnapshotMgr& freeze_info_mgr)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = nullptr;

  freeze_info_mgr_ = &freeze_info_mgr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!meta.is_valid() || OB_ISNULL(pg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(meta), KP(pg));
  } else if (OB_FAIL(create_new_store_map_(store_map_))) {
    LOG_WARN("failed to create table_store_map", K(ret));
  } else {
    if (OB_FAIL(meta_->deep_copy(meta))) {
      LOG_WARN("failed to copy meta", K(ret));
    } else if (ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_) &&
               OB_FAIL(create_multi_version_store(meta.pkey_.get_table_id(), table_store))) {
      LOG_WARN("Failed to create table store", K(ret));
    } else {
      is_inited_ = true;
      is_removed_ = false;
      pkey_ = meta.pkey_;
      pg_ = pg;
      FLOG_INFO("succeed to create partition store", K(ret), K(is_removed_), KP(this), K(*meta_));
    }
    if (OB_FAIL(ret)) {
      destroy_store_map(store_map_);
      store_map_ = nullptr;
    }
  }
  return ret;
}

void ObPartitionStore::destroy_store_map(TableStoreMap* store_map)
{
  if (OB_NOT_NULL(store_map)) {
    for (TableStoreMap::iterator it = store_map->begin(); it != store_map->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_ISNULL(table_store)) {
        LOG_ERROR("table store must not null");
      } else {
        free_multi_version_table_store(table_store);
      }
    }
    ob_delete(store_map);
  }
}

void ObPartitionStore::destroy()
{
  TCWLockGuard guard(lock_);

  if (is_inited_) {
    LOG_INFO("destroy partition store", K(*this));
    destroy_store_map(store_map_);
    store_map_ = nullptr;
  }
  cached_data_table_store_ = nullptr;
  is_inited_ = false;
  is_removed_ = false;
  meta_buf_[0].reset();
  meta_buf_[1].reset();
  meta_ = &meta_buf_[0];
  pg_ = nullptr;
}

int ObPartitionStore::set_removed()
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);

  is_removed_ = true;
  FLOG_INFO("partition store set removed", KP(this), K(ret), K(*meta_), K(lbt()));
  return ret;
}

void ObPartitionStore::set_replica_type(const common::ObReplicaType& replica_type)
{
  const bool need_clean_sstable = !ObReplicaTypeCheck::is_replica_with_ssstore(replica_type);

  {
    TCWLockGuard guard(lock_);
    meta_->replica_type_ = replica_type;
    if (need_clean_sstable) {
      meta_->report_status_.reset();
    }
    LOG_INFO("succeed to set replica type", K(*meta_));

    if (need_clean_sstable) {
      clear_sstores_no_lock();
    }
  }

  if (ObReplicaTypeCheck::is_replica_with_ssstore(replica_type)) {
    // create_table_store_if_need will acquire lock inside
    bool is_created = false;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = create_table_store_if_need(meta_->pkey_.get_table_id(), is_created))) {
      LOG_WARN("failed to create_table_store_if_need", K(tmp_ret), "pkey", meta_->pkey_);
    }
  }
}

void ObPartitionStore::clear_sstores_no_lock()
{
  FLOG_INFO("clean sstables", "pkey", meta_->pkey_);
  cached_data_table_store_ = nullptr;
  for (TableStoreMap::iterator it = store_map_->begin(); it != store_map_->end(); ++it) {
    ObMultiVersionTableStore* table_store = it->second;
    free_multi_version_table_store(table_store);
  }
  store_map_->clear();

  if (NULL != meta_) {
    LOG_INFO("clean report status", "report_status", meta_->report_status_);
    meta_->report_status_.reset();
  }
}

common::ObReplicaType ObPartitionStore::get_replica_type() const
{
  TCRLockGuard guard(lock_);
  return meta_->replica_type_;
}

int ObPartitionStore::create_index_table_store(const uint64_t table_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;

  TCWLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (OB_FAIL(create_multi_version_store(table_id, schema_version, table_store))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("failed to create table store", K(ret), K(table_id));
    }
  } else {
    LOG_INFO("succeed to create table store", K(table_id));
  }
  return ret;
}

int ObPartitionStore::check_table_store_exist(const uint64_t index_id, bool& exist)
{
  ObMultiVersionTableStore* table_store = nullptr;
  TCRLockGuard lock_guard(lock_);
  return check_table_store_exist_nolock(index_id, exist, table_store);
}

int ObPartitionStore::check_table_store_exist_with_lock(
    const uint64_t index_id, bool& exist, ObMultiVersionTableStore*& got_table_store)
{
  TCRLockGuard lock_guard(lock_);
  return check_table_store_exist_nolock(index_id, exist, got_table_store);
}

int ObPartitionStore::check_table_store_exist_nolock(
    const uint64_t index_id, bool& exist, ObMultiVersionTableStore*& got_table_store)
{
  int ret = OB_SUCCESS;
  got_table_store = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionStore has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == index_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_id));
  } else if (OB_FAIL(store_map_->get(index_id, got_table_store))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("Failed to erase table store", K(ret), K(index_id));
    } else {
      ret = OB_SUCCESS;
      exist = false;
    }
  } else {
    exist = true;
  }
  return ret;
}

int ObPartitionStore::drop_index(const uint64_t table_id, const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* got_table_store = NULL;
  bool exist = false;

  {
    TCRLockGuard lock_guard(lock_);
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret));
    } else if (is_removed_) {
      ret = OB_PARTITION_IS_REMOVED;
      LOG_WARN("partition is removed", K(ret), K(meta_));
    } else if (table_id == meta_->pkey_.get_table_id()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cannot drop main table", K(ret), K(table_id), KPC_(meta));
    } else if (OB_FAIL(check_table_store_exist_nolock(table_id, exist, got_table_store))) {
      LOG_WARN("fail to check table store exist", K(ret), K(table_id));
    }
  }

  if (OB_SUCC(ret) && exist) {
    if (OB_ISNULL(got_table_store)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("got_table_store must not null", K(ret));
    } else if (write_slog && OB_FAIL(write_drop_index_trans(meta_->pkey_, table_id))) {
      LOG_WARN("Failed to write drop index log", K(ret));
    } else {
      {
        TCWLockGuard lock_guard(lock_);
        if (OB_FAIL(store_map_->erase(table_id))) {
          LOG_ERROR("failed to erase index, abort now", K(ret), K(table_id));
          ob_abort();
        } else {
          LOG_INFO("drop index", KP(got_table_store), "pkey", meta_->pkey_, K(table_id));
        }
      }
      if (OB_SUCC(ret)) {
        free_multi_version_table_store(got_table_store);
      }
    }
  }

  return ret;
}

int ObPartitionStore::set_dropped_flag(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* got_table_store = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(store_map_->get(table_id, got_table_store))) {
    LOG_WARN("Failed to get table store", K(ret), K(table_id));
  } else if (OB_ISNULL(got_table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("got_table_store must not null", K(ret), K(table_id), K(*this));
  } else {
    TCWLockGuard lock_guard(lock_);
    got_table_store->set_is_dropped_schema();
  }

  return ret;
}
// add_sstable won't update partition meta
// in_slog_trans == false, slog controlled by caller, drop partiton when failed
// in_slog_trans == true, slog controlled by self
int ObPartitionStore::add_sstable(storage::ObSSTable* table, const int64_t max_kept_major_version_number,
    const bool in_slog_trans, const ObMigrateStatus& migrate_status, const bool is_in_dest_split,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to add sstable", K(ret), K(table));
  } else {
    AddTableParam param;
    if (table->is_complement_minor_sstable()) {
      param.complement_minor_sstable_ = table;
    } else {
      param.table_ = table;
    }
    param.max_kept_major_version_number_ = max_kept_major_version_number;
    param.in_slog_trans_ = in_slog_trans;
    param.need_prewarm_ = false;
    param.is_daily_merge_ = false;
    param.multi_version_start_ = meta_->multi_version_start_;
    param.schema_version_ = schema_version;

    if (!param.in_slog_trans_) {
      if (OB_FAIL(get_kept_multi_version_start(
              is_in_dest_split, param.multi_version_start_, param.backup_snapshot_version_))) {
        LOG_WARN("failed to get_kept_multi_version_start", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_add_sstable(param, migrate_status))) {
        LOG_WARN("Failed to do add sstable", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionStore::do_add_sstable(
    AddTableParam& param, const ObMigrateStatus& migrate_status, const bool is_in_source_split)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObMultiVersionTableStore* multi_version_store = NULL;
  ObTableStore* new_table_store = NULL;
  ObTableHandle handle;
  ObITable::TableKey table_key;
  bool need_abort_slog = false;
  bool need_update = false;
  bool exist = false;
  int64_t lsn = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("replica type is without sstable, cannot add it", K(ret), K(*meta_));
  } else if (param.multi_version_start_ < meta_->multi_version_start_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("input multi version start is smaller than local",
        K(ret),
        K(param.multi_version_start_),
        K(meta_->multi_version_start_));
  } else {
    uint64_t table_id =
        OB_NOT_NULL(param.table_) ? param.table_->get_table_id() : param.complement_minor_sstable_->get_table_id();
    const ObITable::TableKey table_key =
        OB_NOT_NULL(param.table_) ? param.table_->get_key() : param.complement_minor_sstable_->get_key();
    if (OB_FAIL(check_table_store_exist_with_lock(table_id, exist, multi_version_store))) {
      LOG_WARN("failed to check table store exist with lock", K(ret), K(param));
    } else {
      if (!exist) {
        if (param.is_daily_merge_) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("table store not exists, cannot add table for merge", K(ret), K(table_id));
        } else {
          TCWLockGuard guard(lock_);
          if (OB_FAIL(create_multi_version_store(table_id, param.schema_version_, multi_version_store))) {
            LOG_WARN("failed to create table store", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(multi_version_store)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("table store must not null", K(ret));
        } else if (OB_FAIL(multi_version_store->prepare_add_sstable(param, new_table_store, need_update))) {
          LOG_WARN("failed to prepare add table", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && pg_memtable_mgr_->get_memtable_count() > 0 && NULL != new_table_store &&
      OB_NOT_NULL(param.table_) && !param.table_->is_trans_sstable()) {
    if (OB_FAIL(check_new_table_store(*new_table_store, migrate_status, is_in_source_split))) {
      LOG_WARN("failed to check new table store", K(ret));
    }
  }

  if (OB_SUCC(ret) && need_update) {
    if (!param.in_slog_trans_) {
      if (OB_FAIL(SLOGGER.begin(OB_LOG_ADD_SSTABLE))) {
        STORAGE_LOG(WARN, "fail to begin add sstable log.", K(ret));
      } else {
        need_abort_slog = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(write_modify_table_store_log(*new_table_store, param.multi_version_start_))) {
      LOG_WARN("failed to write_add_sstable_log", K(ret), K(new_table_store));
    } else if (!param.in_slog_trans_ && OB_FAIL(SLOGGER.commit(lsn))) {
      STORAGE_LOG(WARN, "fail to commit add sstable log.", K(ret));
    } else {
      TCWLockGuard guard(lock_);
      need_abort_slog = false;
      if (OB_FAIL(multi_version_store->enable_table_store(param.need_prewarm_, *new_table_store))) {
        if (!param.in_slog_trans_) {
          ObTaskController::get().allow_next_syslog();
          LOG_ERROR("failed to enable table store, abort now", K(ret), K(table_key));
          ob_abort();
        } else {
          LOG_WARN("failed to enable table store", K(ret), K(table_key));
        }
      } else {
        meta_->multi_version_start_ = param.multi_version_start_;
        new_table_store = NULL;
        FLOG_INFO("enable add table store",
            K(lsn),
            "memtable_cnt",
            pg_memtable_mgr_->get_memtable_count(),
            K(*pg_memtable_mgr_),
            K(param));
      }
    }
  }

  if (NULL != multi_version_store && NULL != new_table_store) {
    multi_version_store->free_table_store(new_table_store);
  }
  if (need_abort_slog) {
    if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
      STORAGE_LOG(ERROR, "logger abort error, ", K(tmp_ret));
    }
  }

  return ret;
}

int ObPartitionStore::check_new_table_store(
    ObTableStore& new_table_store, const ObMigrateStatus migrate_status, const bool is_in_source_split)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable* active_memtable = NULL;
  ObTablesHandle tmp_handle;
  ObTableHandle memtable_handle;
  ObITable* last_table = nullptr;
  if (OB_FAIL(pg_memtable_mgr_->get_active_memtable(memtable_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret || is_in_source_split) {
      ret = OB_SUCCESS;
      LOG_INFO("active_memtable not exist, skip check it", K(ret));
    } else {
      ret = OB_ACTIVE_MEMTBALE_NOT_EXSIT;
      LOG_ERROR("active memtable not exist", K(ret), K(*this));
    }
  } else if (OB_FAIL(memtable_handle.get_memtable(active_memtable))) {
    LOG_WARN("failed to get active memtable", K(ret));
  } else if (OB_FAIL(new_table_store.get_all_tables(false, /*include_active_memtable*/
                 false,                                    /*include_complement*/
                 tmp_handle))) {
    LOG_ERROR("failed to get all tables", K(ret));
  } else if (OB_ISNULL(last_table = tmp_handle.get_last_table())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("new table store must not empty", K(ret), K(tmp_handle), K(new_table_store));
  } else if (!last_table->is_major_sstable() && last_table->get_end_log_ts() < active_memtable->get_start_log_ts()) {
    if (migrate_status != OB_MIGRATE_STATUS_NONE) {
      LOG_INFO("partition is migrating, last table not continue with active memtable",
          K(ret),
          K(tmp_handle),
          K(*active_memtable),
          K(new_table_store));
    } else {
      ret = OB_ERR_SYS;
      LOG_ERROR("last table not continue with active memtable",
          K(ret),
          K(tmp_handle),
          K(*active_memtable),
          K(new_table_store),
          K(*meta_));
    }
  }
  return ret;
}

// max_kept_major_version_number means no need remove old versions
int ObPartitionStore::add_sstable_for_merge(storage::ObSSTable* table, const int64_t max_kept_major_version_number,
    const ObMigrateStatus& migrate_status, const bool is_in_restore, const bool is_in_source_split,
    const bool is_in_dest_split, ObSSTable* complement_minor_sstable)
{
  int ret = OB_SUCCESS;
  AddTableParam param;
  param.multi_version_start_ = meta_->multi_version_start_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (max_kept_major_version_number < 0 || (OB_NOT_NULL(table) && !table->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(max_kept_major_version_number), KPC(table));
  } else if (is_in_restore) {
    // do nothing
  } else if (OB_FAIL(get_kept_multi_version_start(
                 is_in_dest_split, param.multi_version_start_, param.backup_snapshot_version_))) {
    LOG_WARN("failed to get_kept_multi_version_start", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else {
    param.table_ = table;
    param.max_kept_major_version_number_ = max_kept_major_version_number;
    param.in_slog_trans_ = false;
    param.is_daily_merge_ = true;
    param.complement_minor_sstable_ = complement_minor_sstable;
    if (OB_NOT_NULL(table) && table->is_major_sstable()) {
      param.need_prewarm_ = false;
    } else if (is_in_restore) {
      param.need_prewarm_ = false;
    } else {
      param.need_prewarm_ = 0 < GCONF.minor_warm_up_duration_time;
    }
    if (is_in_dest_split) {
      param.max_kept_major_version_number_ = 1;
    }
    if (OB_FAIL(do_add_sstable(param, migrate_status, is_in_source_split))) {
      LOG_WARN("failed to do_add_sstable", K(ret));
    }
  }

  return ret;
}

int ObPartitionStore::set_reference_tables(const uint64_t table_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  bool tmp_ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObMultiVersionTableStore* multi_version_store = NULL;
  ObTableStore* new_table_store = NULL;
  bool need_update = false;
  memtable::ObMemtable* active_memtable = NULL;
  ObTableHandle memtable_handle;
  TCWLockGuard lock_guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (table_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args, ", K(table_id));
  } else if (OB_FAIL(pg_memtable_mgr_->get_active_memtable(memtable_handle))) {
    LOG_WARN("failed to get active memtable", K(ret));
  } else if (OB_FAIL(memtable_handle.get_memtable(active_memtable))) {
    STORAGE_LOG(WARN, "get memtable error", K(ret), K(memtable_handle), K_(meta));
  } else {
    hash_ret = store_map_->get(table_id, multi_version_store);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      if (OB_FAIL(create_multi_version_store(table_id, multi_version_store))) {
        LOG_WARN("failed to create table store", K(ret));
      }
    } else if (OB_SUCCESS != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to get table store", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(multi_version_store->set_reference_tables(
            handle, new_table_store, active_memtable->get_base_version(), need_update))) {
      LOG_WARN("failed to set new tables", K(ret), K(table_id), K(meta_->pkey_));
    } else if (need_update) {
      int64_t lsn = 0;
      if (OB_FAIL(SLOGGER.begin(OB_LOG_SET_REFERENCE_TABLES))) {
        LOG_WARN("failed to begin slog", K(ret), K(meta_->pkey_));
      } else if (OB_FAIL(write_modify_table_store_log(*new_table_store, meta_->multi_version_start_))) {
        LOG_WARN("failed to write modify table store log", K(ret), K(meta_->pkey_));
      } else if (OB_FAIL(SLOGGER.commit(lsn))) {
        LOG_WARN("failed to commit slog", K(ret), K(meta_->pkey_), K(lsn));
      } else if (OB_FAIL(multi_version_store->enable_table_store(false /*need_prewarm*/, *new_table_store))) {
        LOG_ERROR("failed to enable table store, abort now", K(ret), K(table_id));
        ob_abort();
      } else {
        new_table_store = NULL;
        LOG_INFO("succeed to set reference tables", K(table_id), K(lsn));
      }

      if (OB_FAIL(ret)) {
        if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
          STORAGE_LOG(ERROR, "logger abort error, ", K(tmp_ret));
        }
      }
    }
  }

  if (NULL != multi_version_store && NULL != new_table_store) {
    multi_version_store->free_table_store(new_table_store);
  }

  return ret;
}

// caller must help lock_
int ObPartitionStore::create_multi_version_store(const uint64_t table_id, ObMultiVersionTableStore*& out_table_store)
{
  return create_multi_version_store_(table_id, 0, out_table_store);
}

int ObPartitionStore::create_multi_version_store(
    const uint64_t table_id, const int64_t schema_version, ObMultiVersionTableStore*& out_table_store)
{
  return create_multi_version_store_(table_id, schema_version, out_table_store);
}

int ObPartitionStore::create_multi_version_store_(
    const uint64_t table_id, const int64_t schema_version, ObMultiVersionTableStore*& out_table_store)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* tmp_table_store = NULL;
  out_table_store = NULL;

  if (OB_UNLIKELY(!meta_->is_valid())) {  // maybe called in init(), so only check meta
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("replica type without ssstore cannot create table store", K(ret), K(*meta_));
  } else if (OB_ISNULL(tmp_table_store = alloc_multi_version_table_store(extract_tenant_id(table_id)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to new ObMultiVersionTableStore", K(ret));
  } else if (OB_FAIL(
                 tmp_table_store->init(meta_->pkey_, table_id, freeze_info_mgr_, pg_memtable_mgr_, schema_version))) {
    LOG_WARN("failed to init table_store", K(ret), K(table_id));
  } else if (OB_FAIL(store_map_->set(table_id, tmp_table_store))) {
    if (OB_HASH_EXIST != ret) {
      LOG_WARN("failed to set table store to map", K(ret), K(table_id));
    } else {
      ret = OB_ENTRY_EXIST;
    }
  } else {
    LOG_INFO("succeed to create multi version table store", KPC(tmp_table_store), KP(tmp_table_store), K(table_id));
    if (table_id == meta_->pkey_.get_table_id()) {
      cached_data_table_store_ = tmp_table_store;
    }
    out_table_store = tmp_table_store;
    tmp_table_store = NULL;
  }

  if (NULL != tmp_table_store) {
    free_multi_version_table_store(tmp_table_store);
  }
  return ret;
}

int ObPartitionStore::get_index_status(const int64_t schema_version,
    common::ObIArray<share::schema::ObIndexTableStat>& index_status,
    common::ObIArray<uint64_t>& deleted_and_error_index_ids)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  share::schema::ObMultiVersionSchemaService& schema_service =
      share::schema::ObMultiVersionSchemaService::get_instance();
  const bool with_global_index = false;
  int64_t save_schema_version;
  int64_t check_schema_version = schema_version;
  int64_t latest_schema_version = 0;
  const uint64_t fetch_tenant_id = is_inner_table(pkey_.get_table_id()) ? OB_SYS_TENANT_ID : pkey_.get_tenant_id();
  index_status.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_inner_table(meta_->pkey_.table_id_) &&
             OB_FAIL(schema_service.get_tenant_schema_version(OB_SYS_TENANT_ID, save_schema_version)) &&
             FALSE_IT(check_schema_version = std::max(save_schema_version, check_schema_version))) {
    LOG_WARN("failed to get sys tenant schema version", K(ret), K(meta_->pkey_));
  } else if (OB_FAIL(schema_service.retry_get_schema_guard(
                 check_schema_version, meta_->pkey_.table_id_, schema_guard, save_schema_version))) {
    if (OB_TABLE_IS_DELETED != ret) {
      LOG_WARN("failed to get schema guard", K(ret), "pkey", meta_->pkey_);
    } else {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        LOG_INFO("table is deleted, cannot get index status", K(ret), "pkey", meta_->pkey_);
      } else {
        LOG_DEBUG("table is deleted, cannot get index status", K(ret), "pkey", meta_->pkey_);
      }
    }
  } else if (OB_FAIL(schema_guard.get_all_aux_table_status(meta_->pkey_.table_id_, with_global_index, index_status))) {
    LOG_WARN("Failed to get index status", K(ret), K(*meta_));
  } else if (OB_FAIL(schema_guard.get_schema_version(fetch_tenant_id, save_schema_version))) {
    LOG_WARN("failed to get schema version", K(ret), K(fetch_tenant_id), K(pkey_));
  } else if (OB_FAIL(schema_service.get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("failed to get full tenant schema guard", K(ret), K(fetch_tenant_id), K(pkey_));
  } else if (OB_FAIL(schema_guard.get_schema_version(fetch_tenant_id, latest_schema_version))) {
    LOG_WARN("failed to get schema version", K(ret), K(fetch_tenant_id), K(pkey_));
  } else if (latest_schema_version > save_schema_version) {
    // befor check the delete status of index, we should make sure the schema guard is refreshed
    for (int64_t i = 0; OB_SUCC(ret) && i < index_status.count(); ++i) {
      const share::schema::ObTableSchema* table_schema = NULL;
      share::schema::ObIndexTableStat& index_stat = index_status.at(i);
      if (OB_FAIL(schema_guard.get_table_schema(index_stat.index_id_, table_schema))) {
        LOG_WARN("Failed to get table schema", K(ret), K(index_status.at(i)));
      } else if (NULL == table_schema || share::schema::INDEX_STATUS_INDEX_ERROR == table_schema->get_index_status()) {
        if (OB_FAIL(deleted_and_error_index_ids.push_back(index_stat.index_id_))) {
          LOG_WARN("failed to push back deleted_and_error_index_ids", K(ret), "index id", index_status.at(i).index_id_);
        } else {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_INFO("table is deleted or error,", K(ret), K(index_stat), "pkey", meta_->pkey_);
          } else {
            LOG_DEBUG("table is deleted or error,", K(ret), K(index_stat), "pkey", meta_->pkey_);
          }
        }
      }
    }
  }

  return ret;
}

int ObPartitionStore::check_all_tables_merged(const memtable::ObMemtable& memtable,
    const common::ObIArray<share::schema::ObIndexTableStat>& index_status,
    const common::ObIArray<uint64_t>& deleted_index_ids, bool& all_merged, bool& can_release)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  int64_t least_merged_snapshot_version = -1;
  int64_t latest_merged_snapshot_version = -1;
  int64_t least_merged_log_ts = -1;
  int64_t latest_merged_log_ts = -1;
  bool is_created = false;
  all_merged = true;
  can_release = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(pg_memtable_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pg memtable mgr is null", K(ret));
  } else if (OB_FAIL(create_table_store_if_need(meta_->pkey_.get_table_id(), is_created))) {
    LOG_WARN("faield to create_table_store_if_need", K(ret), "pkey", meta_->pkey_);
  } else if (is_created) {
    all_merged = false;
    can_release = false;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < index_status.count(); ++i) {
    const share::schema::ObIndexTableStat& status = index_status.at(i);
    bool can_skip = false;
    if (OB_FAIL(check_skip_check_index_merge_status(status, deleted_index_ids, can_skip))) {
      LOG_WARN("failed to check_skip_check_index_merge_status", K(ret), K(status));
    } else if (can_skip) {
      // can skip, do nothing
    } else if (OB_FAIL(create_table_store_if_need(status.index_id_, is_created))) {
      LOG_WARN("failed to create_table_store_if_need", K(ret), K(status));
    } else if (is_created) {
      all_merged = false;
      can_release = false;
    }
  }

  {
    TCRLockGuard lock_guard(lock_);
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end() && all_merged;
         ++it) {
      table_store = it->second;
      const int64_t index_id = table_store->get_table_id();
      bool need_skip = false;
      bool tmp_all_merged = false;
      bool tmp_can_release = false;
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store is null", K(ret));
      } else if (OB_FAIL(table_store->check_memtable_merged(memtable, tmp_all_merged, tmp_can_release))) {
        LOG_WARN("failed to check memtable merged", K(ret));
      } else {
        all_merged = all_merged && tmp_all_merged;
        can_release = can_release && tmp_can_release;
        if (!tmp_all_merged || !tmp_can_release) {
          bool found = false;
          share::schema::ObIndexTableStat status(
              index_id, share::schema::INDEX_STATUS_UNAVAILABLE, false /*assume not dropped*/);
          for (int64_t i = 0; i < index_status.count(); ++i) {
            if (index_status.at(i).index_id_ == index_id) {
              status = index_status.at(i);
              found = true;
              break;
            }
          }
          if (found && OB_FAIL(check_skip_check_index_merge_status(status, deleted_index_ids, need_skip))) {
            LOG_WARN("failed to check_skip_check_index_merge_status", K(ret), K(index_id));
          } else if (!need_skip && REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
            LOG_INFO("sstable not merged, not update partition meta now",
                "end_log_ts",
                memtable.get_end_log_ts(),
                "snapshot_version",
                memtable.get_snapshot_version(),
                "timestamp",
                memtable.get_timestamp(),
                "pkey",
                meta_->pkey_,
                "table_id",
                table_store->get_table_id(),
                K(tmp_can_release),
                K(tmp_all_merged));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionStore::create_table_store_if_need(const uint64_t table_id, bool& is_created)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    {
      TCRLockGuard lock_guard(lock_);
      if (OB_FAIL(store_map_->get(table_id, table_store))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to get table store", K(ret), K(table_id));
        }
      }
    }
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      TCWLockGuard lock_guard(lock_);
      ObMultiVersionTableStore* tmp_table_store = NULL;
      LOG_INFO("create index table store", K(table_id));
      is_created = true;
      if (OB_FAIL(create_multi_version_store(table_id, tmp_table_store))) {
        LOG_WARN("Failed to create_multi_version_store", K(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObPartitionStore::check_skip_check_index_merge_status(const share::schema::ObIndexTableStat& index_stat,
    const common::ObIArray<uint64_t>& deleted_index_ids, bool& need_skip)
{
  int ret = OB_SUCCESS;
  const uint64_t index_id = index_stat.index_id_;
  need_skip = false;
  for (int64_t i = 0; !need_skip && i < deleted_index_ids.count(); ++i) {
    if (deleted_index_ids.at(i) == index_id) {
      need_skip = true;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_INFO("skip check deleted index merge status", K(index_id), K(deleted_index_ids));
      }
    }
  }
  return ret;
}

// ObTableHandle *main_table_handle is for index back
int ObPartitionStore::get_read_tables(const uint64_t table_id, const int64_t snapshot_version, ObTablesHandle& handle,
    const bool allow_not_ready, const bool need_safety_check, const bool reset_handle, const bool print_dropped_alert)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  if (reset_handle) {
    handle.reset();
  }

  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
    // read requests may fail in some concurrent situations, suck as slave read, transformation of replica type (F->L /
    // F->D)
  } else if (!ObReplicaTypeCheck::is_readable_replica(meta_->replica_type_)) {
    ret = OB_REPLICA_NOT_READABLE;
    STORAGE_LOG(WARN, "replica is not readable", K(ret), "this", *this);
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_id));
  } else if (table_id == meta_->pkey_.get_table_id() && OB_NOT_NULL(cached_data_table_store_)) {
    // always try to guess data table store first
    table_store = cached_data_table_store_;
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    if (OB_HASH_NOT_EXIST != ret || table_id == meta_->pkey_.get_table_id() || !allow_not_ready) {
      LOG_WARN("failed to get table store", K(ret), K(table_id), K(meta_->pkey_.get_table_id()));
    } else if (OB_FAIL(pg_memtable_mgr_->get_memtables(handle, reset_handle))) {
      LOG_WARN("Failed to get memtable", K(ret));
    }
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table store must not null", K(ret), K(table_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(table_store)) {
    // only index memtables and allow_not_ready
  } else if (OB_FAIL(table_store->get_read_tables(snapshot_version, handle, allow_not_ready, print_dropped_alert))) {
    LOG_WARN("failed to get read tables", K(ret), K(handle), KP(table_store));
  } else if (need_safety_check && snapshot_version > get_readable_ts_()) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_INFO("replica not ready yet", K(ret), K(table_id));
  } else {
    LOG_DEBUG("get read tables", K(snapshot_version), K(handle));
  }
  if (OB_SNAPSHOT_DISCARDED == ret) {
    handle.reset();
    ObIPartitionGroupGuard guard;
    if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey_, guard))) {
      LOG_WARN("failed to get partition", K(pkey_), K(table_id), K(snapshot_version));
    } else if (OB_FAIL(
                   guard.get_partition_group()->get_pg_storage().get_recovery_data_mgr().get_restore_point_read_tables(
                       table_id, snapshot_version, handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get restore point read tables", K(ret), K(table_id), K(snapshot_version));
      } else {
        ret = OB_SNAPSHOT_DISCARDED;
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_read_frozen_tables(
    const uint64_t table_id, const common::ObVersion& frozen_version, ObTablesHandle& handle, const bool reset_handle)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  if (reset_handle) {
    handle.reset();
  }

  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == table_id || !frozen_version.is_valid() || -1 == frozen_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_id), K(frozen_version));
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    LOG_WARN("failed to get table store", K(ret), K(table_id));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table store must not null", K(ret), K(table_id));
  } else if (OB_FAIL(table_store->get_major_sstable(frozen_version, handle))) {
    LOG_WARN("failed to get read tables", K(ret), K(handle));
  }

  return ret;
}

int ObPartitionStore::get_sample_read_tables(
    const common::SampleInfo& sample_info, const uint64_t table_id, ObTablesHandle& handle, const bool reset_handle)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  ObTableHandle memtable_handle;
  if (reset_handle) {
    handle.reset();
  }
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_id));
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    LOG_WARN("table not exist", K(ret), K(table_id));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table store must not null", K(ret), K(table_id));
  } else if (OB_FAIL(table_store->get_sample_read_tables(sample_info, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret && SampleInfo::SAMPLE_INCR_DATA == sample_info.scope_) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get read tables", K(ret), K(handle));
    }
  }

  return ret;
}

int ObPartitionStore::get_active_protection_clock(int64_t& active_protection_clock)
{
  int ret = OB_SUCCESS;
  memtable::ObMemtable* memtable = NULL;
  ObTableHandle handle;

  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_->get_active_memtable(handle))) {
    LOG_WARN("failed to get active memtable", K(ret));
  } else if (OB_FAIL(handle.get_memtable(memtable))) {
    STORAGE_LOG(WARN, "get memtable error", K(ret), K(handle), K_(meta));
  } else {
    active_protection_clock = memtable->get_protection_clock();
  }

  return ret;
}

int ObPartitionStore::get_active_memtable(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();

  if (OB_FAIL(pg_memtable_mgr_->get_active_memtable(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get_latest_memtable_nolock", K(ret));
    }
  }
  return ret;
}

int ObPartitionStore::get_memtables(ObTablesHandle& handle, const bool reset_handle)
{
  int ret = OB_SUCCESS;
  if (reset_handle) {
    handle.reset();
  }
  ObTimeGuard timeguard("get_memtables", 1000L * 1000L);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_->get_memtables(handle))) {
    LOG_WARN("failed to get working memtable", K(ret));
  }
  return ret;
}

bool ObPartitionStore::has_memtable()
{
  bool has_memtable = false;

  has_memtable = pg_memtable_mgr_->get_memtable_count() > 0;
  return has_memtable;
}

bool ObPartitionStore::has_active_memtable()
{
  return pg_memtable_mgr_->has_active_memtable();
}

int ObPartitionStore::has_major_sstable(const uint64_t index_id, bool& has_major)
{
  TCRLockGuard guard(lock_);
  return has_major_sstable_nolock(index_id, has_major);
}

int ObPartitionStore::has_major_sstable_nolock(const uint64_t index_id, bool& has_major)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(store_map_->get(index_id, table_store))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get table store", K(ret), K(index_id), K(meta_));
    }
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K(index_id), K(meta_));
  } else if (OB_FAIL(table_store->latest_has_major_sstable(has_major))) {
    LOG_WARN("failed to get merge tables", K(ret), K(index_id), K(meta_));
  }
  return ret;
}

int ObPartitionStore::get_all_tables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  TCRLockGuard guard(lock_);

  if (OB_FAIL(get_all_tables_unlock(handle))) {
    LOG_WARN("failed to get_all_tables_unlock", K(ret));
  }
  return ret;
}

int ObPartitionStore::get_all_tables_unlock(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore::TableSet table_set;
  const int64_t set_bucket_num = ObMultiVersionTableStore::MAX_KETP_VERSION_NUM * common::OB_MAX_SSTABLE_PER_TABLE;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(table_set.create(set_bucket_num))) {
    LOG_WARN("failed to create table set", K(ret), K(set_bucket_num));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store is null", K(ret));
      } else if (OB_FAIL(table_store->get_all_tables(table_set, handle))) {
        LOG_WARN("failed to get all tables", K(ret), "table_id", it->first, K(pkey_));
      }
    }
  }
  return ret;
}

int ObPartitionStore::halt_prewarm()
{
  int ret = OB_SUCCESS;
  const int64_t duration = 0;
  int64_t minor_deferred_gc_time = 0;

  TCWLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("the partition has been removed, ", K(ret));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store is null", K(ret));
      } else if (OB_FAIL(table_store->retire_prewarm_store(duration, minor_deferred_gc_time))) {
        LOG_WARN("failed to halt_prewarm", K(ret));
      }
    }
  }

  return ret;
}

int ObPartitionStore::fill_checksum(
    const uint64_t index_id, const int sstable_type, share::ObSSTableChecksumItem& checksum)
{
  int ret = OB_SUCCESS;
  ObSSTable* major_sstable = NULL;
  bool has_major = false;
  ObMultiVersionTableStore* table_store = NULL;
  ObTablesHandle table_handle;
  ObSSTableDataChecksumItem& data_checksum = checksum.data_checksum_;
  data_checksum.sstable_id_ = index_id;
  data_checksum.sstable_type_ = sstable_type;
  data_checksum.replica_type_ = get_replica_type();
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionStore has not been inited", K(ret));
  } else if (OB_FAIL(has_major_sstable_nolock(index_id, has_major))) {
    LOG_WARN("fail to check has major sstable", K(ret), K(index_id));
  } else if (!has_major) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(store_map_->get(index_id, table_store))) {
    LOG_WARN("fail to get table store", K(ret));
  } else if (OB_FAIL(table_store->get_effective_tables(false /*include active memtable*/, table_handle))) {
    LOG_WARN("fail to get latest tables", K(ret));
  } else if (OB_FAIL(table_handle.get_last_major_sstable(major_sstable))) {
    LOG_WARN("fail to get last major sstable", K(ret));
  } else if (major_sstable->get_key().get_partition_key() != pkey_) {
    // skip splitting partition
  } else {
    const int64_t snapshot_version = major_sstable->get_snapshot_version();
    bool found = false;
    if (ObITable::is_major_sstable(static_cast<ObITable::TableType>(sstable_type))) {
      if (major_sstable->get_key().table_type_ == sstable_type) {
        data_checksum.row_checksum_ = major_sstable->get_meta().row_checksum_;
        data_checksum.data_checksum_ = major_sstable->get_meta().data_checksum_;
        data_checksum.row_count_ = major_sstable->get_meta().row_count_;
        data_checksum.snapshot_version_ = snapshot_version;
        const ObIArray<ObSSTableColumnMeta>& column_metas = major_sstable->get_meta().column_metas_;
        for (int64_t i = 0; OB_SUCC(ret) && i < column_metas.count(); ++i) {
          const ObSSTableColumnMeta& column_meta = column_metas.at(i);
          ObSSTableColumnChecksumItem column_checksum;
          column_checksum.tenant_id_ = extract_tenant_id(index_id);
          column_checksum.data_table_id_ = meta_->data_table_id_;
          if (share::schema::ObTableType::AUX_VERTIAL_PARTITION_TABLE == major_sstable->get_meta().table_type_) {
            column_checksum.index_id_ = meta_->data_table_id_;
          } else {
            column_checksum.index_id_ = index_id;
          }
          column_checksum.partition_id_ = meta_->pkey_.get_partition_id();
          column_checksum.sstable_type_ = sstable_type;
          column_checksum.column_id_ = column_meta.column_id_;
          column_checksum.column_checksum_ = column_meta.column_checksum_;
          column_checksum.checksum_method_ = major_sstable->get_meta().checksum_method_;
          column_checksum.snapshot_version_ = major_sstable->get_snapshot_version();
          column_checksum.replica_type_ = data_checksum.replica_type_;
          column_checksum.major_version_ = major_sstable->get_meta().data_version_;
          if (OB_FAIL(checksum.column_checksum_.push_back(column_checksum))) {
            LOG_WARN("fail to push back column checksum", K(ret));
          }
        }
        found = true;
      }
    } else if (ObITable::is_minor_sstable(static_cast<ObITable::TableType>(sstable_type))) {
      // obsoleted
      ObTablesHandle tables_handle;
      if (OB_FAIL(table_store->get_effective_tables(false /*include active memtable*/, tables_handle))) {
        LOG_WARN("fail to get latest tables", K(ret));
      } else {
        for (int64_t i = tables_handle.get_count() - 1; OB_SUCC(ret) && i >= 0; --i) {
          ObITable* table = tables_handle.get_tables().at(i);
          if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, table must not be NULL", K(ret));
          } else if (table->is_minor_sstable() && table->get_snapshot_version() == snapshot_version) {
            ObSSTable* minor_sstable = static_cast<ObSSTable*>(table);
            if (minor_sstable->get_key().table_type_ == sstable_type) {
              data_checksum.row_checksum_ = minor_sstable->get_meta().row_checksum_;
              data_checksum.data_checksum_ = minor_sstable->get_meta().data_checksum_;
              data_checksum.row_count_ = minor_sstable->get_meta().row_count_;
              data_checksum.snapshot_version_ = snapshot_version;
              found = true;
              break;
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, invalid sstable type", K(ret), K(sstable_type));
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      data_checksum.data_table_id_ = meta_->data_table_id_;
    }
  }
  return ret;
}

int ObPartitionStore::calc_report_status(ObReportStatus& status)
{
  int ret = OB_SUCCESS;
  ObTableHandle main_handle;
  ObSSTable* main_sstable = NULL;
  ObTablesHandle all_tables_handle;
  const int64_t macro_block_size = OB_FILE_SYSTEM.get_macro_block_size();
  ObMultiVersionTableStore* data_table_store = NULL;
  int64_t last_index_id = 0;
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    if (!ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_)) {
      status.row_count_ = 0;
      status.row_checksum_ = 0;
      status.data_checksum_ = 0;
      status.data_size_ = 0;
      status.required_size_ = 0;
      status.snapshot_version_ = 0;
    } else {
      bool has_major = false;

      if (OB_FAIL(has_major_sstable_nolock(meta_->pkey_.get_table_id(), has_major))) {
        LOG_WARN("fail to check has major sstable", K(ret));
      } else if (!has_major) {
        status.data_version_ = 0;
        status.row_count_ = 0;
        status.row_checksum_ = 0;
        status.data_checksum_ = 0;
        status.data_size_ = 0;
        status.required_size_ = 0;
        status.snapshot_version_ = 0;
      } else if (OB_ISNULL(data_table_store = get_data_table_store())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("data_table_store_ must not null", K(ret), K(meta_));
      } else if (OB_FAIL(data_table_store->get_latest_major_sstable(main_handle))) {
        LOG_WARN("failed to get last major main_sstable", K(ret));
      } else if (OB_FAIL(main_handle.get_sstable(main_sstable))) {
        LOG_WARN("failed to get main_sstable", K(ret));
      } else if (OB_ISNULL(main_sstable)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("main_sstable must not null", K(ret));
      } else {
        const blocksstable::ObSSTableMeta& meta = main_sstable->get_meta();
        status.data_version_ = main_sstable->get_version();
        status.row_count_ = meta.row_count_;
        status.row_checksum_ = meta.row_checksum_;
        status.data_checksum_ = meta.data_checksum_;
        status.snapshot_version_ = main_sstable->get_key().trans_version_range_.snapshot_version_;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(get_all_tables_unlock(all_tables_handle))) {
          LOG_WARN("failed to get all tables", K(ret));
        } else {
          status.data_size_ = 0;
          status.required_size_ = 0;
          const int64_t table_cnt = all_tables_handle.get_count();
          for (int64_t i = 0; OB_SUCC(ret) && i < table_cnt; ++i) {
            ObITable* table = all_tables_handle.get_tables().at(i);
            if (OB_ISNULL(table)) {
              ret = OB_ERR_SYS;
              LOG_ERROR("table must not null", K(ret));
            } else if (table->is_major_sstable()) {
              const blocksstable::ObSSTableMeta& meta = static_cast<ObSSTable*>(table)->get_meta();
              if (meta.data_version_ == status.data_version_) {
                status.data_size_ += meta.occupy_size_;
              }
              if (meta.index_id_ != last_index_id) {
                status.required_size_ += meta.get_total_macro_block_count() * macro_block_size;
                last_index_id = meta.index_id_;
              } else {
                status.required_size_ +=
                    (meta.get_total_macro_block_count() - meta.get_total_use_old_macro_block_count()) *
                    macro_block_size;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_all_table_ids(common::ObIArray<uint64_t>& index_tables)
{
  int ret = OB_SUCCESS;
  index_tables.reset();
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_FAIL(index_tables.push_back(table_store->get_table_id()))) {
        LOG_WARN("failed to add merge index table ids", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_all_table_stats(common::ObIArray<TableStoreStat>& index_tables)
{
  int ret = OB_SUCCESS;
  index_tables.reset();
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      TableStoreStat stat;
      ObMultiVersionTableStore* table_store = it->second;
      stat.index_id_ = table_store->get_table_id();
      stat.has_dropped_flag_ = table_store->is_dropped_schema();
      if (OB_FAIL(index_tables.push_back(stat))) {
        LOG_WARN("failed to add merge index table ids", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_reference_tables(int64_t table_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    LOG_WARN("failed to get table store", K(ret), K(table_id));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K(meta_));
  } else if (OB_FAIL(table_store->get_reference_tables(handle))) {
    LOG_WARN("failed to get reference tables", K(ret), K(table_id));
  }
  return ret;
}

int ObPartitionStore::get_merge_table_ids(const ObMergeType merge_type, const bool using_remote_memstore,
    const bool is_in_dest_split, const int64_t trans_table_end_log_ts, const int64_t trans_table_timestamp,
    common::ObVersion& frozen_version, common::ObIArray<uint64_t>& index_tables, bool& need_merge)
{
  int ret = OB_SUCCESS;
  bool is_complete = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else if (OB_UNLIKELY((merge_type <= INVALID_MERGE_TYPE || merge_type >= MERGE_TYPE_MAX))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get merge table ids", K(merge_type), K(ret));
  } else if (MAJOR_MERGE == merge_type) {
    DEBUG_SYNC(BEFORE_GET_MAJOR_MERGE_TABLE_IDS);
  } else {
    DEBUG_SYNC(BEFORE_GET_MINOR_MERGE_TABLE_IDS);
  }

  if (OB_SUCC(ret)) {
    TCRLockGuard lock_guard(lock_);

    int64_t save_slave_read_version = get_readable_ts_();

    if (!ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_)) {
      need_merge = false;
    } else if (save_slave_read_version < 0) {
      need_merge = false;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_WARN("save_slave_read_version is invalid, cannot schedule merge",
            "pkey",
            meta_->pkey_,
            K(save_slave_read_version),
            "readable info",
            pg_memtable_mgr_->get_readable_info());
      }
    } else if (OB_FAIL(check_store_complete_(is_complete))) {
      LOG_WARN("failed to check store complete", K(ret), K_(pkey));
    } else if (is_in_dest_split && !is_complete) {
      need_merge = true;
      LOG_INFO("store is not complete, needs to wait", K_(pkey));
    } else if (MAJOR_MERGE == merge_type) {
      if (OB_FAIL(get_major_merge_table_ids(save_slave_read_version, frozen_version, index_tables, need_merge))) {
        LOG_WARN("failed to get major merge table ids", K(ret));
      }
    } else if (MINI_MINOR_MERGE == merge_type || HISTORY_MINI_MINOR_MERGE == merge_type) {
      if (OB_FAIL(get_minor_merge_table_ids(using_remote_memstore, merge_type, index_tables))) {
        LOG_WARN("failed to get mini minor merge table ids", K(ret));
      }
    } else {
      if (OB_FAIL(get_mini_merge_table_ids(save_slave_read_version,
              merge_type,
              trans_table_end_log_ts,
              trans_table_timestamp,
              index_tables,
              need_merge))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("failed to get minor merge table ids", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObPartitionStore::get_major_merge_table_ids(const int64_t save_slave_read_version, common::ObVersion& merge_version,
    common::ObIArray<uint64_t>& index_tables, bool& need_merge)
{
  int ret = OB_SUCCESS;
  bool can_merge = false;
  need_merge = false;
  common::ObVersion saved_merge_version = merge_version;
  index_tables.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (save_slave_read_version < 0 || !merge_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(save_slave_read_version), K(merge_version));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      bool tmp_need_merge = false;
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_FAIL(
              check_need_major_merge(table_store, save_slave_read_version, merge_version, tmp_need_merge, can_merge))) {
        LOG_WARN("failed to check_need_major_merge", K(ret));
      } else if (tmp_need_merge) {
        if (can_merge) {
          if (saved_merge_version > merge_version) {
            saved_merge_version = merge_version;
            index_tables.reset();
          }
          if (OB_FAIL(index_tables.push_back(table_store->get_table_id()))) {
            LOG_WARN("failed to add merge index table ids", K(ret), K(meta_->pkey_));
          }
        }
        need_merge = true;
      }
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret) && !GCONF.allow_major_sstable_merge) {
    index_tables.reset();
  }
#endif

  return ret;
}

int ObPartitionStore::get_minor_merge_table_ids(
    const bool using_remote_memstore, const ObMergeType merge_type, common::ObIArray<uint64_t>& index_tables)
{
  int ret = OB_SUCCESS;
  index_tables.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    bool need_merge = false;

    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_FAIL(check_need_minor_merge(table_store->get_table_id(), need_merge))) {
        LOG_WARN("Failed to check need mini minor merge", K(ret), KPC(table_store));
      } else if (!need_merge) {
      } else if (OB_FAIL(table_store->check_need_minor_merge(using_remote_memstore, merge_type, need_merge))) {
        LOG_WARN("failed to check need mini minor merge", K(ret));
      } else if (need_merge) {
        if (OB_FAIL(index_tables.push_back(table_store->get_table_id()))) {
          LOG_WARN("failed to add merge index table ids", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_mini_merge_table_ids(const int64_t save_slave_read_version, const ObMergeType merge_type,
    const int64_t trans_table_end_log_ts, const int64_t trans_table_timestamp, common::ObIArray<uint64_t>& index_tables,
    bool& need_merge)
{
  int ret = OB_SUCCESS;
  bool can_merge = false;
  need_merge = false;
  index_tables.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (pg_memtable_mgr_->get_memtable_count() < 1) {
    LOG_DEBUG("has no memtable, cannot get minor merge version", K(ret));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_FAIL(check_need_mini_merge(table_store,
              merge_type,
              save_slave_read_version,
              trans_table_end_log_ts,
              trans_table_timestamp,
              need_merge,
              can_merge))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("failed to check_need_mini_merge", K(ret));
        }
      } else if (need_merge) {
        if (can_merge) {
          if (OB_FAIL(index_tables.push_back(table_store->get_table_id()))) {
            LOG_WARN("failed to add merge index table ids", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObPartitionStore::get_migrate_table_ids(common::ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;

  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table id should not be null here", K(ret));
        // TODO check index valid here
      } else if (OB_FAIL(table_ids.push_back(table_store->get_table_id()))) {
        LOG_WARN("failed to add merge index table ids", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionStore::is_physical_split_finished(bool& is_physical_split_finish)
{
  int ret = OB_SUCCESS;
  is_physical_split_finish = true;
  bool temp_is_finish = true;

  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* multi_version_table_store = it->second;

      if (OB_ISNULL(multi_version_table_store)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store must not null", K(ret));
      } else {
        const int64_t table_id = multi_version_table_store->get_table_id();
        if (OB_FAIL(multi_version_table_store->is_physical_split_finished(temp_is_finish))) {
          LOG_WARN("failed to check physical split finished", K(ret), K(meta_->pkey_), K(table_id));
        }
        if (OB_SUCC(ret)) {
          if (!temp_is_finish) {
            is_physical_split_finish = false;
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_physical_split_info(ObVirtualPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  ObMultiVersionTableStore* data_table_store = get_data_table_store();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(data_table_store)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data table store must not null", K(ret));
  } else if (OB_FAIL(data_table_store->get_physical_split_info(split_info))) {
    LOG_WARN("failed to get physical split info", K(ret), K(meta_->pkey_));
  } else {
    split_info.merge_version_ = meta_->report_status_.data_version_;
    split_info.table_id_ = data_table_store->get_table_id();
    split_info.partition_id_ = pkey_.get_partition_id();
  }

  return ret;
}

int ObPartitionStore::check_need_split(
    const int64_t& index_id, common::ObVersion& split_version, bool& need_split, bool& need_minor_split)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (OB_FAIL(store_map_->get(index_id, table_store))) {
    LOG_WARN("failed to get table store", K(ret), K(index_id), K(*this));
  } else if (OB_FAIL(table_store->check_need_split(split_version, need_split, need_minor_split))) {
    LOG_WARN("failed to check need split", K(index_id), K(split_version), K(*this));
  }
  return ret;
}

int ObPartitionStore::check_can_migrate(bool& can_migrate)
{
  int ret = OB_SUCCESS;
  can_migrate = true;

  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* multi_version_table_store = it->second;

      if (OB_ISNULL(multi_version_table_store)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store must not null", K(ret));
      } else {
        const int64_t table_id = multi_version_table_store->get_table_id();
        if (OB_FAIL(multi_version_table_store->check_can_migrate(can_migrate))) {
          LOG_WARN("failed to check can split", K(ret), K(meta_->pkey_), K(table_id));
        } else if (!can_migrate) {
          break;
        }
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_split_table_ids(
    common::ObVersion& split_version, bool is_major_split, common::ObIArray<uint64_t>& index_tables)
{
  int ret = OB_SUCCESS;
  bool need_split = false;
  bool need_minor_split = false;
  bool is_complete = true;
  bool is_minor_split = !is_major_split;
  index_tables.reset();

  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_store_complete_(is_complete))) {
    LOG_WARN("failed to check store complete", K(ret), K_(pkey));
  } else if (!is_complete) {
    LOG_INFO("store is not complete, needs to wait", K_(pkey));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(table_store->check_need_split(split_version, need_split, need_minor_split))) {
        LOG_WARN("failed to check_need split", K(ret));
      } else if (need_split) {
        if (is_minor_split && !need_minor_split) {
          // do nothing
        } else {
          if (OB_FAIL(index_tables.push_back(table_store->get_table_id()))) {
            LOG_WARN("failed to add split table key", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObPartitionStore::check_need_major_merge(ObMultiVersionTableStore* table_store,
    const int64_t save_slave_read_version, ObVersion& merge_version, bool& need_merge, bool& can_merge)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  ObITable* latest_major_sstable = NULL;
  ObITable* latest_minor_sstable = NULL;
  ObFreezeInfoSnapshotMgr::FreezeInfoLite freeze_info;
  common::ObVersion major_sstable_version;
  common::ObVersion split_version;
  bool need_split = false;
  bool need_minor_split = false;
  need_merge = false;
  can_merge = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret));
  } else if (OB_FAIL(table_store->get_effective_tables(false /*include active memtable*/, handle))) {
    LOG_WARN("failed to get_latest_tables", K(ret));
  }

  // find max major sstable
  for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
    ObITable* tmp = handle.get_table(i);
    if (OB_ISNULL(tmp)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret));
    } else if (tmp->is_major_sstable()) {
      latest_major_sstable = tmp;
    } else if (tmp->is_minor_sstable()) {
      latest_minor_sstable = tmp;
    }
  }

  if (OB_SUCC(ret) && NULL != latest_major_sstable) {
    major_sstable_version = latest_major_sstable->get_version();
    if (major_sstable_version.major_ + 1 <= merge_version.major_) {
      merge_version.major_ = major_sstable_version.major_ + 1;
      need_merge = true;
    }
  }

  if (OB_SUCC(ret) && need_merge) {
    if (OB_FAIL(freeze_info_mgr_->get_freeze_info_by_major_version(merge_version.major_, freeze_info))) {
      LOG_WARN("failed to get_freeze_info_by_major_version", K(ret), K(merge_version));
    } else if (save_slave_read_version < freeze_info.freeze_ts) {
      FLOG_INFO("slave_read_version is less than freeze_ts, can not merge",
          K(freeze_info),
          K(merge_version),
          K(save_slave_read_version));
      can_merge = false;
    } else {
      if (OB_NOT_NULL(latest_minor_sstable)) {
        can_merge = latest_minor_sstable->get_snapshot_version() >= freeze_info.freeze_ts;
      } else {
        can_merge = latest_major_sstable->get_snapshot_version() >= freeze_info.freeze_ts;
      }
    }
  }

  if (OB_SUCC(ret) && can_merge) {
    if (OB_FAIL(table_store->check_need_split(split_version, need_split, need_minor_split))) {
      LOG_WARN("failed to check need split", K(ret), "data_table_id", meta_->pkey_.table_id_);
    } else {
      if (need_split) {
        can_merge = false;
      }
    }
  }

  if (need_merge && !can_merge && REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
    LOG_INFO(
        "check_need_major_merge", K_(pkey), K(need_merge), K(can_merge), K(merge_version), K(freeze_info), K(handle));
  }

  return ret;
}

int ObPartitionStore::check_need_mini_merge(ObMultiVersionTableStore* table_store, const ObMergeType merge_type,
    const int64_t safe_slave_read_version, const int64_t trans_table_end_log_ts, const int64_t trans_table_timestamp,
    bool& need_merge, bool& can_merge)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  ObITable* latest_sstable = nullptr;
  ObMemtable* first_frozen_memtable = nullptr;
  ObMemtable* last_frozen_memtable = nullptr;
  ObGetMergeTablesParam param;
  ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite freeze_info;

  bool is_source_minor_merge_finish = true;
  need_merge = false;
  can_merge = false;

  if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret));
  } else if (OB_ISNULL(meta_) || OB_ISNULL(pg_memtable_mgr_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("meta and pg memtable mgr must not null", K(ret));
  } else if (OB_UNLIKELY(MINI_MERGE != merge_type)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Unexpected minor merge type", K(merge_type), K(ret));
  } else if (OB_FAIL(table_store->get_effective_tables(true /*include active memtable*/, handle))) {
    LOG_WARN("failed to get_latest_tables", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
      ObITable* tmp = handle.get_table(i);
      if (OB_ISNULL(tmp)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret));
      } else if (tmp->is_memtable() && tmp->get_partition_key() != pg_memtable_mgr_->get_pkey()) {
        is_source_minor_merge_finish = false;
        break;
      } else if (tmp->is_sstable()) {
        latest_sstable = tmp;
      } else if (tmp->is_frozen_memtable()) {
        if (OB_ISNULL(first_frozen_memtable)) {
          first_frozen_memtable = static_cast<ObMemtable*>(tmp);
        }
        last_frozen_memtable = static_cast<ObMemtable*>(tmp);
      }
    }

    if (OB_SUCC(ret) && !need_merge && OB_NOT_NULL(first_frozen_memtable)) {
      need_merge = true;
      if (first_frozen_memtable->get_end_log_ts() > trans_table_end_log_ts ||
          first_frozen_memtable->get_timestamp() > trans_table_timestamp) {
        // we have to merge trans table before other tables
        can_merge = false;
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          LOG_WARN("trans table has not merged, can not schedule minor merge",
              KPC(first_frozen_memtable),
              K_(pkey),
              K(trans_table_end_log_ts),
              K(trans_table_timestamp),
              "memtable end_log_ts",
              first_frozen_memtable->get_end_log_ts(),
              "memtable timestamp",
              first_frozen_memtable->get_timestamp());
        }
      } else {
        if (first_frozen_memtable->can_be_minor_merged()) {
          can_merge = true;
          if (OB_NOT_NULL(latest_sstable) &&
              (latest_sstable->get_end_log_ts() >= last_frozen_memtable->get_end_log_ts() &&
                  latest_sstable->get_snapshot_version() >= last_frozen_memtable->get_snapshot_version())) {
            need_merge = false;
          }
        }
      }
    }

    // only check sstable count when mini merge
    if (OB_SUCC(ret) && need_merge && MINI_MERGE == merge_type) {
      bool is_merge_safe = false;
      if (OB_FAIL(table_store->check_latest_table_count_safe(is_merge_safe))) {
        LOG_WARN("Failed to check table store table count safe with minor merge", K(merge_type), K(ret));
      } else if (!is_merge_safe) {
        need_merge = false;
        LOG_ERROR("Too many sstables, minor merge is not safe now", K(merge_type));
      }
    }
  }

  if (!is_source_minor_merge_finish) {
    can_merge = false;
    if (EXECUTE_COUNT_PER_SEC(16)) {
      LOG_INFO("source partition has not merged, cannot merge now", K(ret), "pkey", meta_->pkey_);
    }
  }

  if (true == need_merge && false == can_merge) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_INFO("check_need_minor_merge which cannot merge",
          K_(pkey),
          K(pg_memtable_mgr_->get_pkey()),
          K(need_merge),
          K(can_merge),
          K(safe_slave_read_version),
          K(freeze_info),
          K(latest_sstable),
          K(first_frozen_memtable),
          K(handle));
    }
  }

  return ret;
}

int ObPartitionStore::check_need_minor_merge(const uint64_t table_id, bool& need_merge)
{
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService& schema_service =
      share::schema::ObMultiVersionSchemaService::get_instance();
  ObTablesHandle handle;
  bool is_exist = true;
  int64_t table_schema_version = OB_INVALID_VERSION;
  need_merge = true;

  if (OB_FAIL(schema_service.check_table_exist(table_id, table_schema_version, is_exist))) {
    LOG_WARN("failed to check table exist", K(ret), K(table_id), K(table_schema_version));
  } else if (!is_exist) {
    need_merge = false;
    LOG_INFO("index is deleted, will del it, no need merge", K(ret), "pkey", meta_->pkey_, K(table_id));
  }

  return ret;
}

int ObPartitionStore::get_merge_tables(const ObGetMergeTablesParam& param, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  int64_t min_reserved_snapshot = 0;
  int64_t min_merged_version = INT64_MAX;
  ObTableHandle handle;
  int64_t backup_snapshot_version = 0;
  TCRLockGuard guard(lock_);
  ObTableHandle memtable_handle;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(store_map_->get(param.index_id_, table_store))) {
    LOG_WARN("failed to get table store", K(ret), K(param), K(*this));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K(param), K(*this));
  } else if (OB_FAIL(table_store->get_latest_major_sstable(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get latest major sstable", K(ret), KPC(table_store));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_min_merged_version_(min_merged_version))) {
      LOG_WARN("failed to get_min_merged_version_", K(ret), K(pkey_));
    } else if (OB_FAIL(ObFreezeInfoMgrWrapper::get_instance().get_min_reserved_snapshot(pkey_,
                   min_merged_version,
                   meta_->create_schema_version_,
                   min_reserved_snapshot,
                   backup_snapshot_version))) {
      LOG_WARN("failed to get min reserved snapshot", K(ret), K(pkey_));
    } else if (OB_FAIL(table_store->get_merge_tables(param, min_reserved_snapshot, result))) {
      LOG_WARN("failed to get merge tables", K(ret), K(param), K(*this));
    } else {
      LOG_DEBUG("succeed to get merge tables", K(param), K(result));
    }
  }

  return ret;
}

int ObPartitionStore::get_split_tables(const bool is_major_split, const uint64_t index_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(store_map_->get(index_id, table_store))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get table store", K(ret), K(index_id), K(meta_));
    }
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K(index_id), K(meta_));
  } else if (OB_FAIL(table_store->get_split_tables(is_major_split, handle))) {
    LOG_WARN("failed to get split tables", K(ret), K(index_id), K(meta_));
  }

  return ret;
}

// for merge/backup/migrate
int ObPartitionStore::get_major_sstable(
    const uint64_t index_id, const common::ObVersion& merge_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(store_map_->get(index_id, table_store))) {
    LOG_WARN("failed to get table store", K(ret), K(index_id), K(meta_));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K(index_id), K(meta_));
  } else if (OB_FAIL(table_store->get_major_sstable(merge_version, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get merge tables", K(ret), K(index_id), K(meta_));
    }
  }

  return ret;
}

int ObPartitionStore::get_last_major_sstable(const uint64_t index_id, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(store_map_->get(index_id, table_store))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get table store", K(ret), K(index_id), K(meta_));
    }
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K(index_id), K(meta_));
  } else if (OB_FAIL(table_store->get_latest_major_sstable(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get merge tables", K(ret), K(index_id), K(meta_));
    }
  }

  return ret;
}

int ObPartitionStore::get_last_all_major_sstable(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionStore has not been inited", K(ret));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObTableHandle tmp_handle;
      bool is_ready_for_read = false;
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store must not null", K(ret));
      } else if (OB_FAIL(table_store->check_ready_for_read(is_ready_for_read))) {
        LOG_WARN("fail to check ready for read", K(ret));
      } else if (is_ready_for_read) {
        if (OB_FAIL(table_store->get_latest_major_sstable(tmp_handle))) {
          LOG_WARN("fail to get lastest major sstables", K(ret));
        } else {
          if (OB_FAIL(handle.add_table(tmp_handle.get_table()))) {
            LOG_WARN("fail to add table", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionStore::remove_old_table(const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  int64_t multi_version_start = 0;
  const bool is_in_dest_split = false;
  int64_t backup_snapshot_verison = 0;
  ObSEArray<ObMultiVersionTableStore*, OB_MAX_INDEX_PER_TABLE> stores;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("the partition has been removed, ", K(ret));
  } else if (OB_FAIL(get_kept_multi_version_start(is_in_dest_split, multi_version_start, backup_snapshot_verison))) {
    LOG_WARN("failed to get_kept_multi_version_start", K(ret));
  } else {
    int64_t kept_major_num = GCONF.max_kept_major_version_number;
    if (kept_major_num < 1) {
      kept_major_num = 1;
    }
    const ObVersion kept_min_version = ObVersion(frozen_version - (kept_major_num - 1));
    {
      TCRLockGuard lock_guard(lock_);
      for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
        ObMultiVersionTableStore* table_store = it->second;
        if (OB_ISNULL(table_store)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("table_store must not null", K(ret));
        } else if (OB_FAIL(stores.push_back(table_store))) {
          LOG_WARN("failed to push back table store", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stores.count(); ++i) {
      if (OB_FAIL(remove_old_table_(
              kept_min_version, multi_version_start, kept_major_num, backup_snapshot_verison, *stores.at(i)))) {
        LOG_WARN("faile to remove_old_table_", K(ret), K(pkey_));
      }
    }
  }

  return ret;
}

int ObPartitionStore::remove_old_table_(const common::ObVersion& kept_min_version, const int64_t multi_version_start,
    const int64_t kept_major_num, const int64_t backup_snapshot_version, ObMultiVersionTableStore& table_store)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_remove = true;
  bool need_update = false;
  int64_t real_kept_major_num = kept_major_num;
  int64_t lsn = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(table_store.need_remove_old_table(
                 kept_min_version, multi_version_start, backup_snapshot_version, real_kept_major_num, need_remove))) {
    LOG_WARN("failed to need_remove_old_table", K(ret));
  }

  if (OB_SUCC(ret) && need_remove) {
    ObTableStore* new_table_store = NULL;
    AddTableParam param;

    param.table_ = NULL;
    param.max_kept_major_version_number_ = real_kept_major_num;
    param.multi_version_start_ = multi_version_start;
    param.in_slog_trans_ = false;
    param.need_prewarm_ = true;
    param.is_daily_merge_ = false;
    param.backup_snapshot_version_ = backup_snapshot_version;

    if (param.multi_version_start_ < meta_->multi_version_start_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("input multi version start is smaller than local",
          K(ret),
          K(param.multi_version_start_),
          K(meta_->multi_version_start_));
    } else if (OB_FAIL(table_store.prepare_add_sstable(param, new_table_store, need_update))) {
      LOG_WARN("failed to prepare add table", K(ret));
    } else if (!need_update) {
      LOG_INFO(
          "no old table is deleted", K(kept_min_version), "pkey", meta_->pkey_, "table_id", table_store.get_table_id());
    } else if (OB_FAIL(SLOGGER.begin(OB_LOG_REMOVE_OLD_TABLE))) {
      STORAGE_LOG(WARN, "fail to begin minor merge log.", K(ret));
    } else {
      if (OB_FAIL(write_modify_table_store_log(*new_table_store, param.multi_version_start_))) {
        LOG_WARN("failed to write_modify_table_store_log", K(ret), K(pkey_));
      } else if (OB_FAIL(SLOGGER.commit(lsn))) {
        STORAGE_LOG(WARN, "fail to begin minor merge log.", K(ret));
      } else {
        TCWLockGuard guard(lock_);
        if (OB_FAIL(table_store.enable_table_store(param.need_prewarm_, *new_table_store))) {
          LOG_ERROR("failed to enable table store, abort now", K(ret), K(*new_table_store));
          ob_abort();
        } else {
          meta_->multi_version_start_ = param.multi_version_start_;
          LOG_INFO("enable remove old table store", K(lsn), K(kept_min_version), K(*new_table_store));
          new_table_store = NULL;
        }
      }

      if (OB_FAIL(ret)) {
        if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
          STORAGE_LOG(ERROR, "logger abort error, ", K(tmp_ret));
        }
      }
    }

    if (NULL != new_table_store) {
      table_store.free_table_store(new_table_store);
    }
  }
  return ret;
}

int ObPartitionStore::retire_prewarm_store(const bool is_disk_full)
{
  int ret = OB_SUCCESS;
  int64_t duration = GCONF.minor_warm_up_duration_time;
  int64_t minor_deferred_gc_time = GCONF.minor_deferred_gc_time;

  if (is_disk_full) {
    duration = 0;
    minor_deferred_gc_time = 0;
  }
  TCWLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("the partition has been removed, ", K(ret));
  }

  for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
    ObMultiVersionTableStore* table_store = it->second;
    if (OB_ISNULL(table_store)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table_store must not null", K(ret));
    } else if (OB_FAIL(table_store->retire_prewarm_store(duration, minor_deferred_gc_time))) {
      LOG_WARN("failed to remove unneed tables", K(ret), K(is_disk_full));
    }
  }

  return ret;
}

// caller need has lock
int64_t ObPartitionStore::get_serialize_size()
{
  int tmp_ret = OB_SUCCESS;
  int64_t serialize_size = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    tmp_ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(tmp_ret));
  } else {
    const int64_t table_store_count = store_map_->size();
    serialize_size += serialization::encoded_length_i64(MAGIC_NUM_2);
    serialize_size += serialization::encoded_length_i64(log_seq_num_);
    serialize_size += meta_->get_serialize_size();
    serialize_size += serialization::encoded_length_i64(table_store_count);
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCCESS == tmp_ret && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* multi_table_store = it->second;
      ObTableStore* table_store = NULL;

      if (OB_ISNULL(multi_table_store)) {
        tmp_ret = OB_ERR_SYS;
        LOG_ERROR("multi version table store is null", K(tmp_ret));
      } else if (OB_ISNULL(table_store = multi_table_store->get_latest_store())) {
        tmp_ret = OB_ERR_SYS;
        LOG_ERROR("table store must not null", K(tmp_ret), K(*multi_table_store));
      } else {
        serialize_size += table_store->get_serialize_size();
      }
    }
  }

  if (OB_SUCCESS != tmp_ret) {
    serialize_size = 0;
  }

  return serialize_size;
}

int ObPartitionStore::serialize(common::ObIAllocator& allocator, char*& new_buf, int64_t& serialize_size)
{
  int ret = OB_SUCCESS;
  int64_t need_size = 0;
  serialize_size = 0;
  new_buf = NULL;

  TCRLockGuard lock_guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (0 >= (need_size = get_serialize_size())) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to get_serialize_need_size", K(ret));
  } else if (OB_ISNULL(new_buf = static_cast<char*>(allocator.alloc(need_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(need_size));
  } else if (OB_FAIL(serialization::encode_i64(new_buf, need_size, serialize_size, MAGIC_NUM_2))) {
    STORAGE_LOG(WARN, "serialize OB_PARTITION_STORE_VERSION failed.", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(new_buf, need_size, serialize_size, log_seq_num_))) {
    STORAGE_LOG(WARN, "serialize log_seq_num_ failed.", K(ret));
  } else if (OB_FAIL(meta_->serialize(new_buf, need_size, serialize_size))) {
    LOG_WARN("failed to serialize meta", K(ret));
  } else {
    const int64_t total_count = store_map_->size();
    int64_t cur_count = 0;

    if (OB_FAIL(serialization::encode_i64(new_buf, need_size, serialize_size, total_count))) {
      LOG_WARN("failed to encode table count", K(ret));
    }
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* multi_table_store = it->second;
      ObTableStore* table_store = NULL;

      if (OB_ISNULL(multi_table_store)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("multi version table store is null", K(ret));
      } else if (OB_ISNULL(table_store = multi_table_store->get_latest_store())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store must not null", K(ret), K(*multi_table_store));
      } else if (OB_FAIL(table_store->serialize(new_buf, need_size, serialize_size))) {
        LOG_WARN("failed to serialize table_store", K(ret), K(*table_store));
      } else {
        ++cur_count;
        LOG_INFO("succeed to serialize table store",
            K(cur_count),
            K(total_count),
            K(need_size),
            K(serialize_size),
            K(*table_store));
      }
    }

    if (OB_SUCC(ret)) {
      if (cur_count != total_count) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store count not match", K(ret), K(cur_count), K(total_count), K(*this));
      } else {
        LOG_INFO("succeed to serialize partition store", "table_store_count", store_map_->size(), K(*this));
      }
    }
  }

  if (OB_SUCC(ret) && need_size != serialize_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("serialize size not match", K(ret), K(need_size), K(serialize_size));
  }

  return ret;
}

int ObPartitionStore::deserialize(ObIPartitionGroup* pg, ObPGMemtableMgr* pg_memtable_mgr,
    const ObReplicaType replica_type, const char* buf, const int64_t buf_len, int64_t& pos, bool& is_old_meta,
    ObPartitionStoreMeta& old_meta)
{
  int ret = OB_SUCCESS;
  int64_t magic_num = 0;
  int64_t cur_count = 0;
  int64_t total_count = 0;
  ObPGPartitionStoreMeta meta;
  ObTableStore table_store;
  TCWLockGuard guard(lock_);

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited store cannot deserialize again", K(ret), K(meta_));
  } else if (OB_ISNULL(pg_memtable_mgr) || OB_ISNULL(buf) || OB_ISNULL(pg)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(pg_memtable_mgr), KP(pg));
  } else {
    pg_memtable_mgr_ = pg_memtable_mgr;
    if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &magic_num))) {
      STORAGE_LOG(WARN, "deserialize magic_num failed.", K(ret));
    } else if (magic_num != MAGIC_NUM_2) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid magic num", K(ret), K(magic_num), LITERAL_K(MAGIC_NUM_2));
    } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &log_seq_num_))) {
      LOG_WARN("failed to decode log_seq_num_", K(ret));
    } else {
      int64_t tmp_pos = pos;
      int64_t meta_version = 0;
      if (OB_FAIL(serialization::decode(buf, buf_len, tmp_pos, meta_version))) {
        LOG_WARN("failed to decode meta version", K(ret));
      } else if (ObPGPartitionStoreMeta::PARTITION_STORE_META_VERSION_V2 != meta_version) {
        if (OB_FAIL(old_meta.deserialize(buf, buf_len, pos))) {
          LOG_WARN("failed to deserialize store meta v1", K(ret));
        } else if (OB_FAIL(meta.copy_from_old_meta(old_meta))) {
          LOG_WARN("failed to copy_from_old_meta", K(ret), K(meta));
        } else {
          is_old_meta = true;
        }
      } else if (OB_FAIL(meta.deserialize(buf, buf_len, pos))) {
        LOG_WARN("failed to deserialize meta", K(ret));
      } else {
        meta.replica_type_ = replica_type;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &total_count))) {
          LOG_WARN("failed to decode table count", K(ret));
        } else if (OB_FAIL(init(meta, pg, ObFreezeInfoMgrWrapper::get_instance()))) {
          LOG_WARN("failed to init", K(ret));
        }
      }
    }
  }

  while (OB_SUCC(ret) && cur_count < total_count) {
    table_store.reset();
    if (OB_FAIL(table_store.deserialize(buf, buf_len, pos))) {
      LOG_WARN("failed to deserialize table_store", K(ret), K(buf_len), K(pos), K(meta));
    } else if (OB_FAIL(set_table_store(table_store))) {
      LOG_WARN("failed to replay_modify_table_store", K(ret), K(PRETTY_TS(table_store)));
    } else {
      ++cur_count;
    }
  }

  if (OB_SUCC(ret)) {
    if (cur_count != total_count) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table store count not match", K(ret), K(cur_count), K(total_count));
    } else {
      LOG_INFO("succeed to deserialize partition store", K(total_count), K(*this));
    }
  }
  return ret;
}

int ObPartitionStore::write_create_partition_store_log(const ObPGPartitionStoreMeta& meta)
{
  int ret = OB_SUCCESS;
  ObCreatePGPartitionStoreLogEntry log_entry;
  int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_CREATE_PG_PARTITION_STORE);
  log_entry.pg_key_ = pg_memtable_mgr_->get_pkey();
  const ObStorageLogAttribute log_attr(
      pg_memtable_mgr_->get_pkey().get_tenant_id(), pg_->get_pg_storage().get_storage_file()->get_file_id());

  if (!meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(meta));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta));
  } else if (OB_FAIL(log_entry.meta_.deep_copy(meta))) {
    LOG_WARN("Failed to deep copy meta", K(ret));
  } else if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, log_entry))) {
    STORAGE_LOG(WARN, "Failed to write create partition store log", K(ret), K(meta));
  }
  return ret;
}

int ObPartitionStore::write_update_partition_meta_trans(const ObPGPartitionStoreMeta& meta, const LogCommand& cmd)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObUpdatePGPartitionMetaLogEntry log_entry;
  log_entry.pg_key_ = pg_memtable_mgr_->get_pkey();

  if (!meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(meta));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta));
  } else if (OB_FAIL(log_entry.meta_.deep_copy(meta))) {
    LOG_WARN("failed to deep copy meta", K(ret));
  } else if (OB_FAIL(SLOGGER.begin(cmd))) {
    STORAGE_LOG(WARN, "Fail to begin daily merge log, ", K(ret));
  } else {
    const ObStorageLogAttribute log_attr(
        pg_memtable_mgr_->get_pkey().get_tenant_id(), pg_->get_pg_storage().get_storage_file()->get_file_id());
    int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_UPDATE_PG_PARTITION_META);
    int64_t lsn = 0;
    if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, log_entry))) {
      STORAGE_LOG(WARN, "Failed to write_update_partition_meta_trans", K(ret));
    } else if (OB_FAIL(SLOGGER.commit(lsn))) {
      STORAGE_LOG(ERROR, "Fail to commit logger, ", K(ret));
    } else {
      FLOG_INFO("succeed to write update partition meta trans", K(lsn), K(meta));
    }

    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        STORAGE_LOG(ERROR, "write_update_partition_meta_trans logger abort error", K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObPartitionStore::write_modify_table_store_log(ObTableStore& table_store, const int64_t multi_version_start)
{
  int ret = OB_SUCCESS;
  ObModifyTableStoreLogEntry log_entry(table_store);
  int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_MODIFY_TABLE_STORE);

  log_entry.kept_multi_version_start_ = multi_version_start;
  log_entry.pg_key_ = pg_memtable_mgr_->get_pkey();

  const ObStorageLogAttribute log_attr(
      pg_memtable_mgr_->get_pkey().get_tenant_id(), pg_->get_pg_storage().get_storage_file()->get_file_id());

  if (!log_entry.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(multi_version_start), K(PRETTY_TS(table_store)));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret));
  } else if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, log_entry))) {
    STORAGE_LOG(WARN, "Failed to write modify table store log", K(ret), K(log_entry));
  } else {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("succeed to write modify table store log", K(log_entry));
  }
  return ret;
}

int ObPartitionStore::write_drop_index_trans(const common::ObPartitionKey& pkey, const uint64_t index_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDropIndexSSTableLogEntry log_entry;
  log_entry.pkey_ = pkey;
  log_entry.index_id_ = index_id;
  log_entry.pg_key_ = pg_memtable_mgr_->get_pkey();

  if (!pkey.is_valid() || index_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(pkey), K(index_id));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret));
  } else if (OB_FAIL(SLOGGER.begin(OB_LOG_PARTITION_DROP_INDEX))) {
    STORAGE_LOG(WARN, "Fail to begin daily merge log, ", K(ret));
  } else {
    int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_DROP_INDEX_SSTABLE_OF_STORE);
    const ObStorageLogAttribute log_attr(
        pg_memtable_mgr_->get_pkey().get_tenant_id(), pg_->get_pg_storage().get_storage_file()->get_file_id());
    int64_t lsn = 0;
    if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, log_entry))) {
      STORAGE_LOG(WARN, "Failed to write_drop_index_trans", K(ret));
    } else if (OB_FAIL(SLOGGER.commit(lsn))) {
      STORAGE_LOG(ERROR, "Fail to commit logger, ", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("succeed to wrtite drop index trans log", K(lsn), K(log_entry), K(common::lbt()));
    }

    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        STORAGE_LOG(ERROR, "write_drop_index_trans logger abort error", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_effective_tables(const uint64_t table_id, ObTablesHandle& handle, bool& is_ready_for_read)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  handle.reset();
  ObTableHandle memtable_handle;
  memtable::ObMemtable* memtable = NULL;
  ObITable* last_table = NULL;
  is_ready_for_read = false;

  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_id));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get table store", K(ret), K(table_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table store must not null", K(ret), K(table_id));
  } else if (OB_FAIL(table_store->get_effective_tables(false /*include active memtable*/, handle))) {
    LOG_WARN("failed to get read tables", K(ret), K(handle));
  } else if (OB_FAIL(table_store->check_ready_for_read(is_ready_for_read))) {
    LOG_WARN("failed to get read tables", K(ret), K(handle));
  } else if (OB_FAIL(get_active_memtable(memtable_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get active memtable", K(ret), K(table_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(memtable_handle.get_memtable(memtable))) {
    STORAGE_LOG(WARN, "get active memtable error", K(ret), K(memtable_handle), K(handle));
  } else if (OB_ISNULL(memtable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null memtable", K(ret), K(memtable_handle));
  } else {
    if (is_ready_for_read) {
      if (OB_ISNULL(last_table = handle.get_last_table())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("handle should not be empty", K(ret), K(*table_store));
      } else if (last_table->get_snapshot_version() < memtable->get_base_version() ||
                 (last_table->is_table_with_log_ts_range() &&
                     last_table->get_end_log_ts() < memtable->get_start_log_ts())) {
        is_ready_for_read = false;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(handle.add_table(memtable))) {
      STORAGE_LOG(WARN, "set table error", K(ret), K(*memtable), K(handle));
    }
  }
  return ret;
}

int ObPartitionStore::get_gc_sstables(const uint64_t table_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  handle.reset();
  ObITable* last_table = NULL;

  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_id));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get table store", K(ret), K(table_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table store must not null", K(ret), K(table_id));
  } else if (OB_FAIL(table_store->get_gc_sstables(handle))) {
    LOG_WARN("failed to get_gc_sstables", K(ret), K(handle));
  }
  return ret;
}

int ObPartitionStore::get_replay_tables(const uint64_t table_id, ObIArray<ObITable::TableKey>& replay_tables)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  replay_tables.reset();

  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_id));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get table store", K(ret), K(table_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table store must not null", K(ret), K(table_id));
  } else if (OB_FAIL(table_store->get_replay_tables(replay_tables))) {
    LOG_WARN("failed to get replay tables", K(ret), K(table_id));
  }
  return ret;
}

// used for replay, no log
int ObPartitionStore::replay_partition_meta(const ObPGPartitionStoreMeta& meta)
{
  int ret = OB_SUCCESS;

  TCWLockGuard guard(lock_);
  meta_->reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_ERROR("the partition has been removed, ", K(ret));
  } else if (pg_memtable_mgr_->get_memtable_count() > 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("has memtable now, not in replay", K(ret), K(meta), K(*meta_));
  } else if (OB_FAIL(meta_->deep_copy(meta))) {
    LOG_WARN("failed to copy info", K(ret));
  } else {
    FLOG_INFO("succeed to replay partition meta", K(*meta_));
  }

  return ret;
}

int ObPartitionStore::finish_replay()
{
  int ret = OB_SUCCESS;
  uint64_t data_table_id = OB_INVALID_ID;

  {
    TCWLockGuard guard(lock_);

    if (!is_inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret));
    } else if (is_removed_) {
      ret = OB_PARTITION_IS_REMOVED;
      LOG_ERROR("the partition has been removed", K(ret), K(*this));
    } else if (!meta_->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid meta", K(ret), K(*meta_));
    } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_)) {
      clear_sstores_no_lock();
    }

    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store is null", K(ret));
      } else if (OB_FAIL(table_store->finish_replay(meta_->multi_version_start_))) {
        LOG_WARN("failed to finish_replay", K(ret), K(*this));
      }
    }

    if (OB_SUCC(ret)) {
      data_table_id = meta_->data_table_id_;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_update_report_status(data_table_id, false /*write slog*/))) {
      LOG_WARN("fail to try update report status", K(ret));
    }
  }
  return ret;
}

int ObPartitionStore::get_meta(ObPGPartitionStoreMeta& meta)
{
  int ret = OB_SUCCESS;

  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(meta.deep_copy(*meta_))) {
    LOG_WARN("failed to copy meta", K(ret));
  }
  return ret;
}

int ObPartitionStore::check_need_report(const common::ObVersion& version, bool& need_report)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStore has not been inited", K(ret));
  } else {
    need_report = version > meta_->report_status_.data_version_;
  }
  return ret;
}

int ObPartitionStore::check_major_merge_finished(const common::ObVersion& version, bool& finished)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStore has not been inited", K(ret));
  } else if (OB_FAIL(get_last_all_major_sstable(tables_handle))) {
    STORAGE_LOG(WARN, "fail to get last all major sstable", K(ret));
  } else if (0 == tables_handle.get_count()) {
    finished = false;
  } else {
    finished = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count() && finished; ++i) {
      ObITable* table = tables_handle.get_tables().at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, table must not be NULL", K(ret));
      } else {
        finished = table->get_version() >= version;
      }
    }
  }
  return ret;
}

int ObPartitionStore::write_report_status(
    const ObReportStatus& status, const uint64_t data_table_id, const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObPGPartitionStoreMeta& meta = get_next_meta();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStore has not been inited", K(ret));
  } else if (FALSE_IT(meta.reset())) {
    // can no reach here
  } else if (OB_FAIL(meta.deep_copy(*meta_))) {
    LOG_WARN("failed to get next meta", K(ret));
  } else {
    meta.report_status_ = status;
    meta.data_table_id_ = data_table_id;
    if (write_slog && OB_FAIL(write_update_partition_meta_trans(meta, OB_LOG_WRITE_REPORT_STATUS))) {
      LOG_WARN("failed to write update partition meta log", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("succeed to update report status", "pkey", meta_->pkey_, K(status));
      TCWLockGuard guard(lock_);
      switch_meta();
    }
  }
  return ret;
}

int ObPartitionStore::update_multi_version_start(const int64_t multi_version_start)
{
  int ret = OB_SUCCESS;
  ObPGPartitionStoreMeta& meta = get_next_meta();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStore has not been inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (multi_version_start <= meta_->multi_version_start_) {
    LOG_INFO("no need to update multi_version_start", K(multi_version_start), K(*meta_));
  } else if (FALSE_IT(meta.reset())) {
    // can no reach here
  } else if (OB_FAIL(meta.deep_copy(*meta_))) {
    LOG_WARN("failed to get next meta", K(ret));
  } else {
    meta.multi_version_start_ = multi_version_start;
    if (OB_FAIL(write_update_partition_meta_trans(meta, OB_LOG_UPDATE_MULTI_VERSION_START))) {
      LOG_WARN("failed to write update partition meta log", K(ret));
    } else {
      LOG_INFO("succeed to update multi_version_start", "pkey", meta_->pkey_, K(multi_version_start));
      TCWLockGuard guard(lock_);
      switch_meta();
    }
  }
  return ret;
}

int ObPartitionStore::try_update_report_status(const uint64_t data_table_id, const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObReportStatus report_status;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStore has not been inited", K(ret));
  } else if (OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(data_table_id));
  } else if (OB_FAIL(calc_report_status(report_status))) {
    STORAGE_LOG(WARN, "fail to calc report status", K(ret));
  } else if (OB_FAIL(write_report_status(report_status, data_table_id, write_slog))) {
    STORAGE_LOG(WARN, "fail to write report status", K(ret));
  }
  return ret;
}

int ObPartitionStore::check_ready_for_read(const uint64_t table_id, bool& is_ready)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  TCRLockGuard guard(lock_);
  is_ready = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    LOG_WARN("Failed to get table store", K(ret), K(table_id));
  } else if (OB_FAIL(table_store->check_ready_for_read(is_ready))) {
    LOG_WARN("failed to check ready for read", K(ret), K(table_id), K(*table_store));
  }

  return ret;
}

int ObPartitionStore::replay_modify_table_store(const ObModifyTableStoreLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_ERROR("the partition has been removed, ", K(ret));
  } else if (log_entry.kept_multi_version_start_ < meta_->multi_version_start_) {
    FLOG_INFO("multi version start smaller than local, skip", K(log_entry), K(*meta_));
  } else if (OB_FAIL(set_table_store(log_entry.table_store_))) {
    LOG_WARN("failed to set table store", K(ret));
  } else {
    meta_->multi_version_start_ = log_entry.kept_multi_version_start_;
  }

  return ret;
}

int ObPartitionStore::set_table_store(const ObTableStore& table_store)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* multi_table_store = NULL;
  const uint64_t table_id = table_store.get_table_id();
  int64_t table_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_ERROR("the partition has been removed, ", K(ret));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_)) {
    if (OB_FAIL(table_store.get_table_count(table_count))) {
      LOG_WARN("failed to get table count", K(ret));
    } else if (table_count > 0) {
      ret = OB_ERR_SYS;
      LOG_ERROR("replica without ssstore should not has table", K(ret), K(*meta_), K(PRETTY_TS(table_store)));
    } else {
      LOG_INFO("skip set tables for replica without ssstore", K(*meta_), K(PRETTY_TS(table_store)));
    }
  } else {
    if (OB_FAIL(store_map_->get(table_id, multi_table_store))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get table store", K(ret));
      } else {
        if (OB_FAIL(create_multi_version_store(table_id, multi_table_store))) {
          LOG_WARN("failed to create table store", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(multi_table_store->set_table_store(table_store))) {
        LOG_WARN("failed to replay_modify_table_store", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObPartitionStore::get_multi_version_start()
{
  TCWLockGuard guard(lock_);
  return meta_->multi_version_start_;
}

int ObPartitionStore::get_partition_store_info(ObPartitionStoreInfo& info)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);

  info.replica_type_ = meta_->replica_type_;
  info.multi_version_start_ = meta_->multi_version_start_;
  info.report_version_ = meta_->report_status_.data_version_;
  info.report_row_count_ = meta_->report_status_.row_count_;
  info.report_data_checksum_ = meta_->report_status_.data_checksum_;
  info.report_data_size_ = meta_->report_status_.data_size_;
  info.report_required_size_ = meta_->report_status_.required_size_;
  info.readable_ts_ = this->get_readable_ts_();
  return ret;
}

int ObPartitionStore::get_first_frozen_memtable(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (OB_FAIL(pg_memtable_mgr_->get_first_frozen_memtable(handle))) {
    STORAGE_LOG(WARN, "get first frozen memtable error", K(ret), K(handle));
  } else {
    // do nothing
  }
  return ret;
}

ObMultiVersionTableStore* ObPartitionStore::alloc_multi_version_table_store(const uint64_t tenant_id)
{
  ObMemAttr attr(tenant_id, ObModIds::OB_PARTITION_STORE, ObCtxIds::STORAGE_LONG_TERM_META_CTX_ID);
  ObMultiVersionTableStore* store = OB_NEW_ALIGN32(ObMultiVersionTableStore, attr);
  LOG_INFO("alloc_multi_version_table_store", KP(store), K(lbt()));
  return store;
}

void ObPartitionStore::free_multi_version_table_store(ObMultiVersionTableStore*& table_store)
{
  if (NULL != table_store) {
    LOG_INFO("free_multi_version_table_store", KP(table_store), K(lbt()));
    OB_DELETE_ALIGN32(ObMultiVersionTableStore, unused, table_store);
    table_store = NULL;
  }
}

int ObPartitionStore::get_kept_multi_version_start(
    const bool is_in_dest_split, int64_t& multi_version_start, int64_t& backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  multi_version_start = 0;
  int64_t min_merged_version = 0;
  int64_t min_reserved_snapshot = 0;
  ObTableHandle handle;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(get_last_major_sstable(pkey_.get_table_id(), handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get last major sstable", K(ret), K_(pkey));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    {
      TCRLockGuard lock_guard(lock_);
      if (OB_FAIL(get_min_merged_version_(min_merged_version))) {
        LOG_WARN("failed to get_min_merged_version_", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObFreezeInfoMgrWrapper::get_instance().get_min_reserved_snapshot(pkey_,
              min_merged_version,
              meta_->create_schema_version_,
              min_reserved_snapshot,
              backup_snapshot_version))) {
        LOG_WARN("failed to get_kept_multi_version_start", K(ret), K(pkey_));
      }
    }
  }

  multi_version_start = min_reserved_snapshot;

  if (OB_SUCC(ret) && is_in_dest_split) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      LOG_INFO("partition in logical splitting cannot keep multi version start", K_(pkey));
    }
    multi_version_start = std::max(ObTimeUtility::current_time(), multi_version_start);
  }

  multi_version_start = std::max(multi_version_start, meta_->multi_version_start_);
  return ret;
}

int ObPartitionStore::check_all_merged(
    memtable::ObMemtable& memtable, const int64_t schema_version, bool& is_all_merged, bool& can_release)
{
  int ret = OB_SUCCESS;
  is_all_merged = false;
  can_release = false;
  bool is_merged = false;
  bool tmp_can_release = false;
  common::ObSEArray<share::schema::ObIndexTableStat, OB_MAX_INDEX_PER_TABLE> index_status;
  common::ObSEArray<uint64_t, OB_MAX_INDEX_PER_TABLE> deleted_and_error_index_ids;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_index_status(schema_version, index_status, deleted_and_error_index_ids))) {
    if (OB_EAGAIN != ret && OB_TABLE_IS_DELETED != ret) {
      LOG_WARN("failed to get index ids", K(ret));
    } else if (OB_TABLE_IS_DELETED == ret) {
      // the memtable of the deleted partiton should be released
      ret = OB_SUCCESS;
      is_all_merged = true;
      can_release = true;
    }
  } else if (OB_FAIL(check_all_tables_merged(
                 memtable, index_status, deleted_and_error_index_ids, is_merged, tmp_can_release))) {
    LOG_WARN("failed to check_all_tables_merged", K(ret));
  } else {
    is_all_merged = is_merged;
    can_release = tmp_can_release;
  }

  return ret;
}

void ObPartitionStore::clear_ssstores()
{
  TCWLockGuard lock_guard(lock_);
  clear_sstores_no_lock();
}

bool ObPartitionStore::is_removed() const
{
  TCRLockGuard lock_guard(lock_);
  return is_removed_;
}

int ObPartitionStore::remove_uncontinues_inc_tables(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  ObMultiVersionTableStore* multi_table_store = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (0 == table_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "remove inc sstables get invalid argument", K(ret), K(table_id));
  } else if (OB_FAIL(store_map_->get(table_id, multi_table_store))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to get multi table store", K(ret), K(table_id), KP(multi_table_store));
    } else {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "no multi table store created, skip remove_uncontinues_inc_tables", K(table_id));
    }
  } else if (OB_ISNULL(multi_table_store)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "multi table store should not be NULL", K(ret), KP(multi_table_store));
  } else if (OB_FAIL(inner_remove_uncontinues_inc_tables(multi_table_store))) {
    STORAGE_LOG(WARN, "failed to inner remove uncontinues inc tables", K(ret), KP(multi_table_store));
  }
  return ret;
}

int ObPartitionStore::update_split_table_store(int64_t table_id, bool is_major_split, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMultiVersionTableStore* multi_version_store = NULL;
  bool need_update = false;
  ObTableStore* new_table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (OB_FAIL(store_map_->get(table_id, multi_version_store))) {
    LOG_WARN("failed to get table store", K(ret), K(table_id));
  } else if (OB_FAIL(multi_version_store->prepare_update_split_table_store(
                 is_major_split, handle, need_update, new_table_store))) {
    LOG_WARN("failed to set new tables", K(ret), K(table_id), K(meta_->pkey_));
  } else if (need_update) {
    int64_t lsn = 0;
    if (OB_FAIL(SLOGGER.begin(OB_LOG_UPDATE_SPLIT_TABLE_STORE))) {
      LOG_WARN("failed to begin slog", K(ret), K(meta_->pkey_));
    } else if (OB_FAIL(write_modify_table_store_log(*new_table_store, meta_->multi_version_start_))) {
    } else if (OB_FAIL(SLOGGER.commit(lsn))) {
      LOG_WARN("failed to commit slog", K(ret), K(meta_->pkey_), K(lsn));
    } else {
      TCWLockGuard lock_guard(lock_);
      if (OB_FAIL(multi_version_store->enable_table_store(false /*need_prewarm*/, *new_table_store))) {
        LOG_ERROR("failed to enable table store, abort now", K(ret), K(table_id));
        ob_abort();
      } else {
        new_table_store = NULL;
        LOG_INFO("succeed to update split table store", K(table_id), K(lsn));
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        STORAGE_LOG(ERROR, "logger abort error, ", K(tmp_ret));
      }
    }
  }

  if (NULL != multi_version_store && NULL != new_table_store) {
    multi_version_store->free_table_store(new_table_store);
  }

  return ret;
}

int ObPartitionStore::get_create_schema_version(int64_t& create_schema_version)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else {
    create_schema_version = meta_->create_schema_version_;
  }
  return ret;
}

int ObPartitionStore::inner_remove_uncontinues_inc_tables(ObMultiVersionTableStore* multi_table_store)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t max_kept_major_version_number = 0;
  const bool in_slog_trans = false;
  ObTableStore* new_table_store = NULL;
  bool is_equal = true;
  bool need_abort_slog = false;
  int64_t lsn = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    AddTableParam param;
    param.table_ = NULL;
    param.max_kept_major_version_number_ = max_kept_major_version_number;
    param.in_slog_trans_ = in_slog_trans;
    param.need_prewarm_ = false;
    param.is_daily_merge_ = false;
    param.multi_version_start_ = meta_->multi_version_start_;
    ObTablesHandle continues_tables_handle;
    if (OB_FAIL(multi_table_store->get_latest_continue_tables(continues_tables_handle))) {
      LOG_WARN("failed to get latest continue tables", K(ret), K(param));
    } else if (OB_FAIL(multi_table_store->prepare_remove_sstable(
                   param, continues_tables_handle, new_table_store, is_equal))) {
      LOG_WARN("failed to prepare remove sstable", K(ret), K(param));
    } else {
      if (OB_SUCC(ret) && !is_equal) {
        if (!param.in_slog_trans_) {
          if (OB_FAIL(SLOGGER.begin(OB_LOG_ADD_SSTABLE))) {
            STORAGE_LOG(WARN, "fail to begin add sstable log.", K(ret));
          } else {
            need_abort_slog = true;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(write_modify_table_store_log(*new_table_store, param.multi_version_start_))) {
          LOG_WARN("failed to write_add_sstable_log", K(ret), K(new_table_store));
        } else if (!param.in_slog_trans_ && OB_FAIL(SLOGGER.commit(lsn))) {
          STORAGE_LOG(WARN, "fail to commit add sstable log.", K(ret));
        } else {
          TCWLockGuard guard(lock_);
          need_abort_slog = false;
          if (OB_FAIL(multi_table_store->enable_table_store(param.need_prewarm_, *new_table_store))) {
            if (!param.in_slog_trans_) {
              ObTaskController::get().allow_next_syslog();
              LOG_ERROR("failed to enable table store, abort now", K(ret), K(param));
              ob_abort();
            } else {
              LOG_WARN("failed to enable table store", K(ret), K(param));
            }
          } else {
            meta_->multi_version_start_ = param.multi_version_start_;
            ObTaskController::get().allow_next_syslog();
            LOG_INFO("enable remove uncontinues table store",
                K(lsn),
                K(*pg_memtable_mgr_),
                K(pg_memtable_mgr_->get_memtable_count()),
                K(param),
                K(*new_table_store),
                K(*param.table_));
            new_table_store = NULL;
          }
        }
      }

      if (NULL != multi_table_store && NULL != new_table_store) {
        multi_table_store->free_table_store(new_table_store);
      }
      if (need_abort_slog) {
        if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
          STORAGE_LOG(ERROR, "logger abort error, ", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_report_status(ObReportStatus& status)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else {
    status = meta_->report_status_;
  }
  return ret;
}

int64_t ObPartitionStore::get_readable_ts_() const
{
  return nullptr != pg_memtable_mgr_ ? pg_memtable_mgr_->get_readable_ts() : OB_INVALID_TIMESTAMP;
}

int ObPartitionStore::get_major_frozen_versions(
    const ObVersion& data_version, int64_t& publish_version, int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  schema_version = -1;
  publish_version = -1;
  ObITable* table = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else if (!data_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data version is invalid", K(ret), K(data_version));
  } else if (OB_FAIL(get_major_sstable(pkey_.get_table_id(), data_version, handle))) {
    LOG_WARN("failed to get major sstable", K(ret), K(pkey_.get_table_id()), K(data_version));
  } else if (1 != handle.get_count()) {
    ret = OB_ERR_SYS;
    LOG_WARN("handle should not be empty", K(ret), K(pkey_.get_table_id()), K(data_version));
  } else if (OB_ISNULL(table = handle.get_table(0))) {
    ret = OB_ERR_SYS;
    LOG_WARN("major sstable should not be NULL", K(ret), K(pkey_.get_table_id()), K(data_version));
  } else if (OB_FAIL(table->get_frozen_schema_version(schema_version))) {
    LOG_WARN("failed to get frozen schema version", K(ret), K(pkey_.get_table_id()), K(data_version));
  } else {
    publish_version = table->get_snapshot_version();
  }
  return ret;
}

int ObPartitionStore::get_min_sstable_version(int64_t& min_sstable_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  min_sstable_snapshot_version = INT64_MAX;
  TCRLockGuard guard(lock_);

  if (OB_FAIL(get_all_tables_unlock(handle))) {
    LOG_WARN("failed to get read tables", K(ret), K_(pkey));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); i++) {
      ObITable* table = handle.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_WARN("table should not be null", K(ret));
      } else if (table->is_major_sstable()) {
        if (table->get_snapshot_version() < min_sstable_snapshot_version) {
          min_sstable_snapshot_version = table->get_snapshot_version();
        }
      } else if (table->is_multi_version_table()) {
        if (table->get_base_version() < min_sstable_snapshot_version) {
          min_sstable_snapshot_version = table->get_base_version();
        }
      }
    }
  }

  return ret;
}

int64_t ObPartitionStore::get_table_store_cnt() const
{
  TCRLockGuard guard(lock_);
  return store_map_->size();
}

int ObPartitionStore::get_data_table_latest_sstables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  ObMultiVersionTableStore* data_table_store = nullptr;
  ObTableStore* latest_table_store = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else if (OB_ISNULL(data_table_store = get_data_table_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table store is null", K(ret));
  } else if (OB_ISNULL(latest_table_store = data_table_store->get_latest_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("latest table store is null", K(ret));
  } else if (OB_FAIL(latest_table_store->get_all_sstables(false /*need_complent*/, handle))) {
    LOG_WARN("failed to get all sstables", K(ret));
  }
  return ret;
}

int ObPartitionStore::set_replay_sstables(
    const uint64_t table_id, const bool is_replay_old, ObIArray<ObSSTable*>& sstables)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_id));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get table store", K(ret), K(table_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table store must not null", K(ret), K(table_id));
  } else if (OB_FAIL(table_store->set_replay_sstables(is_replay_old, sstables))) {
    LOG_WARN("failed to set replay tables", K(ret), K(table_id));
  }
  return ret;
}

int ObPartitionStore::get_max_major_sstable_snapshot(int64_t& max_snapshot_version)
{
  int ret = OB_SUCCESS;
  max_snapshot_version = 0;
  int64_t snapshot_version = 0;
  ObMultiVersionTableStore* table_store = NULL;

  TCRLockGuard lock_guard(lock_);
  for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
    table_store = it->second;

    if (OB_ISNULL(table_store)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table store is null", K(ret));
    } else if (OB_FAIL(table_store->get_max_major_sstable_snapshot(snapshot_version))) {
      LOG_WARN("failed to get_multi_version_start", K(ret));
    } else if (snapshot_version > max_snapshot_version) {
      max_snapshot_version = snapshot_version;
    }
  }
  return ret;
}

int ObPartitionStore::get_min_max_major_version(int64_t& min_version, int64_t& max_version)
{
  int ret = OB_SUCCESS;
  min_version = INT64_MAX;
  max_version = INT64_MIN;
  int64_t tmp_min_version = 0;
  int64_t tmp_max_version = 0;
  ObMultiVersionTableStore* table_store = NULL;
  TCRLockGuard lock_guard(lock_);
  for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
    table_store = it->second;
    if (OB_ISNULL(table_store)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table store is null", K(ret));
    } else if (OB_FAIL(table_store->get_min_max_major_version(tmp_min_version, tmp_max_version))) {
      LOG_WARN("failed to get_multi_version_start", K(ret));
    } else {
      min_version = min(min_version, tmp_min_version);
      max_version = max(max_version, tmp_max_version);
    }
  }
  return ret;
}

int ObPartitionStore::check_store_complete_(bool& is_complete)
{
  int ret = OB_SUCCESS;
  is_complete = true;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* multi_version_table_store = it->second;

      if (OB_ISNULL(multi_version_table_store)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store must not null", K(ret));
      } else {
        const int64_t table_id = multi_version_table_store->get_table_id();
        if (OB_FAIL(multi_version_table_store->check_complete(is_complete))) {
          LOG_WARN("failed to check can split", K(ret), K(meta_->pkey_), K(table_id));
        } else if (!is_complete) {
          break;
        }
      }
    }
  }
  return ret;
}

// lock_ must be held before calling this method
int ObPartitionStore::get_min_merged_version_(int64_t& min_merged_version)
{
  int ret = OB_SUCCESS;
  ObTableHandle tmp_handle;
  min_merged_version = -1;
  int64_t tmp_min_merged_version = INT64_MAX;
  for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
    ObMultiVersionTableStore* table_store = it->second;
    ObSSTable* sstable = nullptr;
    tmp_handle.reset();
    if (OB_ISNULL(table_store)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table store must not null", K(ret));
    } else if (OB_FAIL(table_store->get_latest_major_sstable(tmp_handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get lastest major sstables", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(tmp_handle.get_sstable(sstable))) {
      LOG_WARN("failed to get last major sstable");
    } else if (OB_ISNULL(sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable is null", K(ret));
    } else {
      tmp_min_merged_version = std::min(tmp_min_merged_version, int64_t(sstable->get_version().major_));
    }
  }
  if (OB_SUCC(ret) && tmp_min_merged_version != INT64_MAX) {
    min_merged_version = tmp_min_merged_version;
  }
  return ret;
}

int ObPartitionStore::get_latest_minor_sstables(const int64_t index_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = nullptr;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition_store is not inited", K(ret));
  } else if (index_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index_id));
  } else {
    TCRLockGuard lock_guard(lock_);
    if (OB_FAIL(store_map_->get(index_id, table_store))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get table store", K(ret), K(index_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(table_store)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "table store must not null", K(ret), K(index_id));
    } else if (OB_FAIL(table_store->get_latest_minor_sstables(handle))) {
      LOG_WARN("failed to get_latest_minor_sstables", K(ret), K(index_id), K(*table_store));
    }
  }
  return ret;
}

int ObPartitionStore::physical_flashback(const int64_t flashback_scn, ObVersion& data_version)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  data_version.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else if (flashback_scn <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("physical flashback get invalid argument", K(ret), K(flashback_scn));
  } else if (OB_FAIL(get_all_table_ids(table_ids))) {
    LOG_WARN("failed to get all table ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      const uint64_t table_id = table_ids.at(i);
      ObMultiVersionTableStore* multi_table_store = NULL;
      const bool is_data_table = table_id == pkey_.get_table_id();
      if (OB_FAIL(store_map_->get(table_id, multi_table_store))) {
        if (OB_HASH_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "failed to get multi table store", K(ret), K(table_id), KP(multi_table_store));
        } else {
          ret = OB_SUCCESS;
          STORAGE_LOG(INFO, "no multi table store created, skip remove_uncontinues_inc_tables", K(table_id));
        }
      } else if (OB_ISNULL(multi_table_store)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "multi table store should not be NULL", K(ret), KP(multi_table_store));
      } else if (OB_FAIL(inner_physical_flashback(is_data_table, flashback_scn, multi_table_store))) {
        STORAGE_LOG(WARN, "failed to inner physical flashback", K(ret), KP(multi_table_store));
      }

      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_WARN("can not find flash back major sstable", K(ret), K(table_id), K(pkey_));
        if (is_data_table) {
          LOG_ERROR("flash back data table unxpected", K(ret), K(table_id), K(pkey_));
        } else {
          const bool write_slog = false;
          // index table id
          // here need cover ret
          FLOG_INFO("physical flashback can not find index major sstables", K(ret), K(table_id), K(*multi_table_store));
          if (OB_FAIL(drop_index(table_id, write_slog))) {
            LOG_WARN("failed to drop index", K(ret), K(table_id));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(calc_report_status(meta_->report_status_))) {
        STORAGE_LOG(WARN, "fail to calc report status", K(ret));
      } else {
        data_version.version_ = meta_->report_status_.data_version_;
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_mark_deletion_tables(
    const uint64_t index_id, const int64_t end_log_ts, const int64_t snapshot_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob_partition_store is not inited", K(ret));
  } else {
    ObMultiVersionTableStore* table_store = nullptr;
    TCRLockGuard lock_guard(lock_);
    if (OB_FAIL(store_map_->get(index_id, table_store))) {
      LOG_WARN("failed to multi version table store", K(ret), K(index_id), K_(pkey));
    } else if (OB_FAIL(table_store->get_mark_deletion_tables(end_log_ts, snapshot_version, handle))) {
      LOG_WARN("failed to get_mark_deletion_tables", K(ret), K(index_id), K_(pkey));
    }
  }
  return ret;
}

int ObPartitionStore::get_all_latest_minor_sstables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition_store is not inited", K(ret));
  } else {
    TCRLockGuard lock_guard(lock_);
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      const int64_t index_id = it->first;
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_store should not be null", K(ret), K(index_id));
      } else if (OB_FAIL(table_store->get_latest_minor_sstables(handle))) {
        LOG_WARN("failed to get_latest_minor_sstables", K(ret), K(index_id));
      }
    }
  }
  return ret;
}

int ObPartitionStore::scan_trans_table(const ObTableIterParam& iter_param, ObTableAccessContext& access_context,
    const ObExtStoreRange& whole_range, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* data_table_store = nullptr;
  ObTableHandle handle;
  ObSSTable* sstable = NULL;
  row_iter = NULL;
  TCRLockGuard lock_guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else if (!pkey_.is_trans_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pkey is not trans table", K(ret), K_(pkey));
  } else if (OB_ISNULL(data_table_store = get_data_table_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table store is null", K(ret), K_(pkey));
  } else if (OB_FAIL(data_table_store->get_latest_major_sstable(handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get latest major sstable", K(ret), K_(pkey));
    }
  } else if (OB_FAIL(handle.get_sstable(sstable))) {
    LOG_WARN("failed to get last major sstable", K(ret), K_(pkey));
  } else if (OB_FAIL(sstable->scan(iter_param, access_context, whole_range, row_iter))) {
    LOG_WARN("failed to scan trans sstable", K(ret));
  }
  return ret;
}

int ObPartitionStore::check_store_complete(bool& is_complete)
{
  TCRLockGuard lock_guard(lock_);
  return check_store_complete_(is_complete);
}

int ObPartitionStore::inner_physical_flashback(
    const bool is_data_table, const int64_t flashback_scn, ObMultiVersionTableStore* multi_table_store)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t max_kept_major_version_number = 0;
  const bool in_slog_trans = false;
  ObTableStore* new_table_store = NULL;
  bool is_equal = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    AddTableParam param;
    param.table_ = NULL;
    param.max_kept_major_version_number_ = max_kept_major_version_number;
    param.in_slog_trans_ = in_slog_trans;
    param.need_prewarm_ = false;
    param.is_daily_merge_ = false;
    param.multi_version_start_ = meta_->multi_version_start_;
    ObTablesHandle major_sstables_handle;
    if (OB_FAIL(multi_table_store->get_flashback_major_tables(flashback_scn, major_sstables_handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get latest continue tables", K(ret), K(param));
      } else if (ObMultiClusterUtil::is_cluster_private_table(multi_table_store->get_table_id())) {
        LOG_ERROR("table is cluster private table, flash back failed", K(ret), K(*multi_table_store));
      } else if (is_data_table) {
        // index table major not exist, drop index
        // data table major not exist, create empty partition
        ret = OB_SUCCESS;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(multi_table_store->prepare_remove_sstable(
                   param, major_sstables_handle, new_table_store, is_equal))) {
      LOG_WARN("failed to prepare remove sstable", K(ret), K(param));
    } else {
      if (major_sstables_handle.empty()) {
        meta_->multi_version_start_ = 0;
      } else if (!is_equal) {
        TCWLockGuard guard(lock_);
        ObSSTable* sstable = NULL;
        if (OB_FAIL(multi_table_store->enable_table_store(param.need_prewarm_, *new_table_store))) {
          LOG_WARN("failed to enable table store", K(ret), K(param));
        } else if (OB_FAIL(major_sstables_handle.get_first_sstable(sstable))) {
          LOG_WARN("failed to get first sstable", K(ret), K(param));
        } else {
          meta_->multi_version_start_ = sstable->get_key().trans_version_range_.multi_version_start_;
          ObTaskController::get().allow_next_syslog();
          LOG_INFO("enable remove inc table store",
              K(*pg_memtable_mgr_),
              K(pg_memtable_mgr_->get_memtable_count()),
              K(param),
              K(*new_table_store),
              K(*param.table_));
          new_table_store = NULL;
        }
      }

      if (NULL != multi_table_store && NULL != new_table_store) {
        multi_table_store->free_table_store(new_table_store);
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_trans_table_end_log_ts_and_timestamp(int64_t& end_log_ts, int64_t& timestamp)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = nullptr;
  ObTableHandle handle;
  ObSSTable* table = nullptr;
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition_store is not inited", K(ret));
  } else if (!is_trans_table_id(pkey_.get_table_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("this is not trans table partition", K(ret), K_(pkey));
  } else if (OB_ISNULL(table_store = get_data_table_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table store should be null", K(ret), K_(pkey));
  } else if (OB_FAIL(table_store->get_latest_major_sstable(handle))) {
    LOG_WARN("failed to get_latest_trans_sstable", K(ret));
  } else if (OB_FAIL(handle.get_sstable(table))) {
    LOG_WARN("failed to get last major sstable", K(ret), K_(pkey));
  } else {
    end_log_ts = table->get_end_log_ts();
    timestamp = table->get_snapshot_version();
  }
  return ret;
}

int ObPartitionStore::clear_complement_minor_sstable()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition_store is not inited", K(ret));
  } else {
    TCWLockGuard lock_guard(lock_);
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      const int64_t index_id = it->first;
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_store should not be null", K(ret), K(index_id));
      } else if (OB_FAIL(table_store->clear_complement_minor_sstable())) {
        LOG_WARN("failed to get_latest_minor_sstables", K(ret), K(index_id));
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_min_schema_version(int64_t& min_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t tmp_schema_version = 0;
  min_schema_version = INT64_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition_store is not inited", K(ret));
  } else {
    TCRLockGuard lock_guard(lock_);
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      const int64_t index_id = it->first;
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_store should not be null", K(ret), K(index_id));
      } else if (OB_FAIL(table_store->get_min_schema_version(tmp_schema_version))) {
        LOG_WARN("failed to get_latest_minor_sstables", K(ret), K(index_id));
      } else if (tmp_schema_version < min_schema_version) {
        min_schema_version = tmp_schema_version;
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_physical_flashback_publish_version(const int64_t flashback_scn, int64_t& publish_version)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* multi_table_store = NULL;
  ObTablesHandle table_handle;
  ObSSTable* sstable = NULL;
  publish_version = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else if (flashback_scn <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("physical flashback get invalid argument", K(ret), K(flashback_scn));
  } else {
    TCRLockGuard lock_guard(lock_);
    // just use major table
    if (OB_FAIL(store_map_->get(pkey_.get_table_id(), multi_table_store))) {
      STORAGE_LOG(WARN, "failed to get multi table store", K(ret), K(pkey_), KP(multi_table_store));
    } else if (OB_ISNULL(multi_table_store)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "multi table store should not be NULL", K(ret), KP(multi_table_store));
    } else if (OB_FAIL(multi_table_store->get_flashback_major_tables(flashback_scn, table_handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fialed to get flashback major tables", K(ret), K(*multi_table_store));
      } else {
        publish_version = 0;
      }
    } else if (OB_FAIL(table_handle.get_last_major_sstable(sstable))) {
      STORAGE_LOG(WARN, "failed to get last major sstable", K(ret), K(*multi_table_store));
    } else {
      publish_version = sstable->get_snapshot_version();
    }
  }
  return ret;
}

void ObPartitionStore::replace_store_map(TableStoreMap& store_map)
{
  bool found = false;
  TableStoreMap* cur_store_map = nullptr;
  {
    TCWLockGuard lock_guard(lock_);
    cur_store_map = store_map_;
    store_map_ = &store_map;
    cached_data_table_store_ = nullptr;
  }
  FLOG_INFO("Succ to replace store map", KP_(store_map), KP(cur_store_map));
  destroy_store_map(cur_store_map);
}

int ObPartitionStore::create_new_store_map_(TableStoreMap*& new_store_map)
{
  int ret = OB_SUCCESS;
  new_store_map = nullptr;
  ObMemAttr attr(pkey_.get_tenant_id(), ObModIds::OB_PARTITION_STORE, ObCtxIds::STORAGE_LONG_TERM_META_CTX_ID);
  const int64_t page_size = 1024L;
  if (OB_FAIL(allocator_.init(page_size, lib::ObLabel("TableStoreMap")))) {
    LOG_WARN("fail to init allocator", K(ret));
  } else if (OB_ISNULL(new_store_map = OB_NEW(TableStoreMap, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table store map", K(ret));
  } else if (OB_FAIL(new_store_map->create(TABLE_STORE_BUCKET_NUM, &allocator_))) {
    LOG_WARN("failed to create table store map", K(ret));
    ob_delete(new_store_map);
    new_store_map = nullptr;
  }
  return ret;
}

// !!!Attention!!! this function only used in migration
int ObPartitionStore::prepare_new_store_map(const ObTablesHandle& sstables, const int64_t max_kept_major_version_number,
    const bool need_reuse_local_minor, TableStoreMap*& new_store_map)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tmp_handle;
  new_store_map = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition_store is not inited", K(ret));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(pkey_));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(meta_->replica_type_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("replica type is without sstable, cannot add it", K(ret), K(*meta_));
  } else if (OB_FAIL(tmp_handle.assign(sstables))) {
    LOG_WARN("failed to assign tables", K(ret), K_(pkey));
  } else if (OB_FAIL(create_new_store_map_(new_store_map))) {
    LOG_WARN("failed to create_table_store_map_", K(ret), K_(pkey));
  } else {
    const bool compare_table_id = true;
    ObTableStore::ObITableIDCompare comp(ret);
    std::sort(tmp_handle.table_begin(), tmp_handle.table_end(), comp);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to sort tables", K(ret), K_(pkey));
    } else {
      ObTablesHandle index_handle;
      for (int64_t i = 0; OB_SUCC(ret) && i <= tmp_handle.get_count(); ++i) {
        ObMultiVersionTableStore* multi_version_table_store = nullptr;
        ObITable* last_table = 0 == i ? nullptr : tmp_handle.get_table(i - 1);
        ObITable* cur_table = i < tmp_handle.get_count() ? tmp_handle.get_table(i) : nullptr;
        bool added = false;
        if (i > 0 && OB_ISNULL(last_table)) {
          ret = OB_ERR_SYS;
          LOG_WARN("last_table is null", K(ret));
        } else if (i < tmp_handle.get_count() && OB_ISNULL(cur_table)) {
          ret = OB_ERR_SYS;
          LOG_WARN("cur_table is null", K(ret));
        } else if (nullptr != cur_table &&
                   (nullptr == last_table || last_table->get_key().table_id_ == cur_table->get_key().table_id_)) {
          if (OB_FAIL(index_handle.add_table(cur_table))) {
            LOG_WARN("failed to add table", K(ret), KPC(cur_table));
          }
        } else if (OB_FAIL(prepare_new_table_store_(index_handle,
                       max_kept_major_version_number,
                       need_reuse_local_minor,
                       multi_version_table_store))) {
          LOG_WARN("failed to prepare new table store", K(ret), K(index_handle));
        } else if (OB_ISNULL(multi_version_table_store)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("multi_version_table_store should not be null", K(ret));
        } else if (OB_FAIL(new_store_map->set(multi_version_table_store->get_table_id(), multi_version_table_store))) {
          LOG_WARN("failed to set multi_version_table_store", K(ret), K(index_handle));
          free_multi_version_table_store(multi_version_table_store);
        } else {
          index_handle.reset();
          if (OB_NOT_NULL(cur_table) && OB_FAIL(index_handle.add_table(cur_table))) {
            LOG_WARN("failed to add table", K(ret), KPC(cur_table));
          }
        }
      }
      if (OB_SUCC(ret) && !index_handle.empty()) {
        ObMultiVersionTableStore* multi_version_table_store = nullptr;
        if (OB_FAIL(prepare_new_table_store_(
                index_handle, max_kept_major_version_number, need_reuse_local_minor, multi_version_table_store))) {
          LOG_WARN("failed to prepare new table store", K(ret), K(index_handle));
        } else if (OB_ISNULL(multi_version_table_store)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("multi_version_table_store should not be null", K(ret));
        } else if (OB_FAIL(new_store_map->set(multi_version_table_store->get_table_id(), multi_version_table_store))) {
          LOG_WARN("failed to set multi_version_table_store", K(ret), K(index_handle));
          free_multi_version_table_store(multi_version_table_store);
        }
      }
      if (OB_SUCC(ret)) {
        FLOG_INFO("succeed to prepare_new_store_map", K_(pkey), KP(new_store_map), K(sstables));
      }
    }
    if (OB_FAIL(ret)) {
      destroy_store_map(new_store_map);
      new_store_map = nullptr;
    }
  }
  return ret;
}

int ObPartitionStore::get_table_schema_version(const uint64_t table_id, int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = nullptr;
  TCRLockGuard lock_guard(lock_);
  if (OB_FAIL(store_map_->get(table_id, table_store))) {
    LOG_WARN("failed to get table store", K(ret), K(table_id));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_store should not be null", K(ret), K(table_id));
  } else if (OB_FAIL(table_store->get_schema_version(schema_version))) {
    LOG_WARN("failed to get_schema_version", K(ret), K(table_id));
  }
  return ret;
}

int ObPartitionStore::get_remote_table_info_(
    const ObTablesHandle& index_handle, const bool need_reuse_local_minor, ObMigrateRemoteTableInfo& remote_table_info)
{
  int ret = OB_SUCCESS;
  if (need_reuse_local_minor) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Using local minor sstable during migrate is not supported", K(ret), K(remote_table_info));
  } else {
    remote_table_info.reset();
    remote_table_info.need_reuse_local_minor_ = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_handle.get_count(); ++i) {
      const ObITable* table = index_handle.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_WARN("table should not be null", K(ret));
      } else if (table->is_major_sstable()) {
        if (table->get_key().version_.major_ < remote_table_info.remote_min_major_version_) {
          remote_table_info.remote_min_major_version_ = table->get_key().version_.major_;
        }
      }
    }
  }
  return ret;
}

// !!!Attention!!! this function only used in migration
int ObPartitionStore::prepare_new_table_store_(ObTablesHandle& index_handle,
    const int64_t max_kept_major_version_number, const bool need_reuse_local_minor,
    ObMultiVersionTableStore*& multi_version_table_store)
{
  int ret = OB_SUCCESS;
  multi_version_table_store = nullptr;
  AddTableParam param;
  param.table_ = NULL;
  param.max_kept_major_version_number_ = max_kept_major_version_number;
  param.in_slog_trans_ = true;
  param.need_prewarm_ = false;
  param.is_daily_merge_ = false;
  param.multi_version_start_ = meta_->multi_version_start_;
  ObTablesHandle tables_handle;
  ObMigrateRemoteTableInfo remote_table_info;
  if (index_handle.get_count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index handle should not be empty", K(ret));
  } else if (OB_FAIL(get_remote_table_info_(index_handle, need_reuse_local_minor, remote_table_info))) {
    LOG_WARN("failed to get_remote_table_info_", K(ret), K(index_handle));
  } else if (pg_->get_pg_storage().is_restore()) {
    // do nothing
    // no need get kept multi version start
  } else if (OB_FAIL(get_kept_multi_version_start(false, /*is_split*/
                 param.multi_version_start_,
                 param.backup_snapshot_version_))) {
    LOG_WARN("failed to get_kept_multi_version_start", K(ret), K(param));
  }

  if (OB_SUCC(ret)) {
    const int64_t index_id = index_handle.get_table(0)->get_key().table_id_;
    bool is_created = false;
    if (OB_ISNULL(multi_version_table_store = alloc_multi_version_table_store(extract_tenant_id(index_id)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate multi version table store", K(ret));
    } else if (OB_FAIL(multi_version_table_store->init(pkey_, index_id, freeze_info_mgr_, pg_memtable_mgr_))) {
      LOG_WARN("failed to init multi_version_table_store", K(ret), K_(pkey), K(index_id));
    } else if (OB_FAIL(create_table_store_if_need(index_id, is_created))) {
      LOG_WARN("failed to create table store if need", K(ret), K(index_id));
    } else {
      bool is_equal = false;
      const bool need_prewarm = false;
      ObMultiVersionTableStore* cur_mv_table_store = nullptr;
      ObTableStore* new_table_store = nullptr;
      {
        TCRLockGuard lock_guard(lock_);
        if (OB_FAIL(store_map_->get(index_id, cur_mv_table_store))) {
          LOG_WARN("failed to get table store", K(ret), K(index_id), K_(pkey));
        } else if (OB_ISNULL(cur_mv_table_store)) {
          ret = OB_ERR_SYS;
          LOG_WARN("current multi_version_table_store is null", K(ret), K(index_id));
        } else if (OB_FAIL(cur_mv_table_store->get_needed_local_tables_for_migrate(remote_table_info, tables_handle))) {
          LOG_WARN("failed to get_major_sstables_after_start", K(ret), K(index_id), K(pkey_));
        } else {
          ObITable* table = nullptr;
          for (int64_t i = 0; OB_SUCC(ret) && i < index_handle.get_count(); i++) {
            if (OB_ISNULL(table = index_handle.get_table(i))) {
              ret = OB_ERR_SYS;
              LOG_ERROR("Unexpected null table", K(ret), K(i), K(index_handle));
            } else if (table->is_complement_minor_sstable()) {
              // table ref is holded by index_handle
              // migrate complement_minor_sstable should ignore data
              param.complement_minor_sstable_ = static_cast<ObSSTable*>(table);
            } else if (OB_FAIL(tables_handle.add_table(table))) {
              LOG_WARN("Failed to add table to new table handle", K(ret), KPC(table), K(tables_handle));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(
                       cur_mv_table_store->prepare_remove_sstable(param, tables_handle, new_table_store, is_equal))) {
          LOG_WARN("failed to prepare_remove_sstable", K(ret), K(tables_handle));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(write_modify_table_store_log(*new_table_store, param.multi_version_start_))) {
          LOG_WARN("failed to write_modify_table_store_log", K(ret), KPC(new_table_store));
        } else if (OB_FAIL(multi_version_table_store->enable_table_store(need_prewarm, *new_table_store))) {
          LOG_WARN("failed to enable table store", K(ret), K(index_id), K_(pkey));
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(new_table_store)) {
          cur_mv_table_store->free_table_store(new_table_store);
          new_table_store = nullptr;
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("Succ to prepare new table store",
          KP(multi_version_table_store),
          KPC(multi_version_table_store),
          K(index_handle));
    } else if (OB_NOT_NULL(multi_version_table_store)) {
      free_multi_version_table_store(multi_version_table_store);
      multi_version_table_store = nullptr;
    }
  }
  return ret;
}

int ObPartitionStore::get_trans_table_status(ObTransTableStatus& status)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* data_table_store = nullptr;
  ObTableHandle handle;
  ObSSTable* sstable = NULL;
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else if (!pkey_.is_trans_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pkey is not trans table", K(ret), K_(pkey));
  } else if (OB_ISNULL(data_table_store = get_data_table_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table store is null", K(ret), K_(pkey));
  } else if (OB_FAIL(data_table_store->get_latest_major_sstable(handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get latest major sstable", K(ret), K_(pkey));
    }
  } else if (OB_FAIL(handle.get_sstable(sstable))) {
    LOG_WARN("failed to get last major sstable", K(ret), K_(pkey));
  } else {
    status.end_log_ts_ = sstable->get_end_log_ts();
    status.row_count_ = sstable->get_total_row_count();
  }
  return ret;
}

int ObPartitionStore::get_latest_table_count(const int64_t table_id, int64_t& table_count)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    LOG_WARN("failed to get table store", K(ret), K(table_id));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K(meta_));
  } else if (OB_FAIL(table_store->get_latest_table_count(table_count))) {
    LOG_WARN("failed to get reference tables", K(ret), K(table_id));
  }
  return ret;
}

int ObPartitionStore::get_migrate_tables(const uint64_t table_id, ObTablesHandle& handle, bool& is_ready_for_read)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = NULL;
  handle.reset();
  is_ready_for_read = false;

  TCRLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_id));
  } else if (is_removed_) {
    ret = OB_PARTITION_IS_REMOVED;
    LOG_WARN("partition is removed", K(ret), K(meta_));
  } else if (OB_FAIL(store_map_->get(table_id, table_store))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get table store", K(ret), K(table_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table store must not null", K(ret), K(table_id));
  } else if (OB_FAIL(table_store->get_migrate_tables(handle, is_ready_for_read))) {
    STORAGE_LOG(WARN, "failed to get migrate kept max snapshot", K(ret), K(table_id));
  }

  return ret;
}

int ObPartitionStore::get_restore_point_tables(const int64_t snapshot_version, const int64_t publish_version,
    ObRecoveryPointSchemaFilter& schema_filter, ObTablesHandle& handle, bool& is_ready)
{
  int ret = OB_SUCCESS;
  is_ready = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else if (pkey_.is_trans_table()) {
    if (OB_FAIL(get_restore_point_trans_tables_(snapshot_version, handle, is_ready))) {
      LOG_WARN("failed to get restore point trans tables", K(ret));
    }
  } else {
    if (OB_FAIL(get_restore_point_normal_tables_(snapshot_version, publish_version, schema_filter, handle, is_ready))) {
      LOG_WARN("failed to get restore point normal tables", K(ret));
    }
  }
  return ret;
}

int ObPartitionStore::filter_read_sstables(
    const ObTablesHandle& tmp_handle, const int64_t snapshot_version, ObTablesHandle& handle, bool& is_ready)
{
  int ret = OB_SUCCESS;
  handle.reset();
  is_ready = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_handle.get_count(); i++) {
    if (tmp_handle.get_table(i)->is_memtable()) {
      is_ready = false;
      break;
    }
    if (OB_FAIL(handle.add_table(tmp_handle.get_table(i)))) {
      LOG_WARN("failed to add table", K(ret));
    } else if (tmp_handle.get_table(i)->get_snapshot_version() >= snapshot_version) {
      break;
    }
  }
  return ret;
}

int ObPartitionStore::set_drop_schema_info(const int64_t index_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = nullptr;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionStore has not been inited", K(ret));
  } else if (OB_FAIL(store_map_->get(index_id, table_store))) {
    LOG_WARN("fail to get table store", K(ret), K(index_id));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K(meta_));
  } else if (OB_FAIL(table_store->set_drop_schema_info(schema_version))) {
    LOG_WARN("fail to get reference tables", K(ret), K(index_id));
  }
  return ret;
}

int ObPartitionStore::get_drop_schema_info(
    const int64_t index_id, int64_t& drop_schema_version, int64_t& drop_schema_refreshed_ts)
{
  int ret = OB_SUCCESS;
  ObMultiVersionTableStore* table_store = nullptr;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionStore has not been inited", K(ret));
  } else if (OB_FAIL(store_map_->get(index_id, table_store))) {
    LOG_WARN("fail to get table store", K(ret), K(index_id));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K(meta_));
  } else if (OB_FAIL(table_store->get_drop_schema_info(drop_schema_version, drop_schema_refreshed_ts))) {
    LOG_WARN("fail to get reference tables", K(ret), K(index_id));
  }
  return ret;
}

int ObPartitionStore::get_restore_point_trans_tables_(
    const int64_t snapshot_version, ObTablesHandle& handle, bool& is_ready)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tmp_handles;
  is_ready = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else if (OB_FAIL(get_all_tables(tmp_handles))) {
    LOG_WARN("failed to get all tables", K(ret), K(snapshot_version));
  } else if (tmp_handles.empty()) {
    is_ready = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_handles.get_count(); ++i) {
      ObITable* table = tmp_handles.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KP(table));
      } else if (table->is_memtable()) {
        // do nothing
      } else if (OB_FAIL(handle.add_table(table))) {
        LOG_WARN("failed to push table into handles", K(ret), KPC(table));
      } else {
        is_ready = true;
      }
    }
  }
  return ret;
}

int ObPartitionStore::get_restore_point_normal_tables_(const int64_t snapshot_version, const int64_t publish_version,
    ObRecoveryPointSchemaFilter& schema_filter, ObTablesHandle& handle, bool& is_ready)
{
  int ret = OB_SUCCESS;
  is_ready = true;
  int64_t multi_version_start = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition store is not inited", K(ret));
  } else {
    const bool allow_not_ready = false;
    const bool print_dropped_alert = false;
    ObArray<uint64_t> index_tables;
    TCRLockGuard lock_guard(lock_);
    for (TableStoreMap::iterator it = store_map_->begin(); OB_SUCC(ret) && it != store_map_->end(); ++it) {
      ObMultiVersionTableStore* table_store = it->second;
      const uint64_t index_id = table_store->get_table_id();
      if (OB_FAIL(index_tables.push_back(table_store->get_table_id()))) {
        LOG_WARN("failed to add merge index table ids", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_filter.do_filter_tables(index_tables))) {
        LOG_WARN("failed to filter tables", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < index_tables.count() && is_ready; ++i) {
      const uint64_t index_id = index_tables.at(i);
      ObTablesHandle tmp_handle;
      ObTablesHandle new_handle;
      ObMultiVersionTableStore* table_store = NULL;
      if (OB_FAIL(store_map_->get(index_id, table_store))) {
        LOG_WARN("failed to get table store", K(ret), K(index_id));
      } else if (OB_ISNULL(table_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table store should not be null", K(ret), "index_id", index_id);
      } else if (OB_FAIL(table_store->get_multi_version_start(multi_version_start))) {
        LOG_WARN("failed to get multi_version_start", K(ret), K(pkey_), K(index_id));
      } else if (meta_->create_timestamp_ < snapshot_version && multi_version_start > snapshot_version) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table store has not kept multi version",
            K(ret),
            K(snapshot_version),
            K(multi_version_start),
            K_(pkey),
            K(index_id));
      } else if (OB_FAIL(table_store->get_recovery_point_tables(snapshot_version, tmp_handle))) {
        LOG_WARN("failed to get recovery point tables", K(ret), K(snapshot_version), K(publish_version));
      } else if (OB_FAIL(filter_read_sstables(tmp_handle, publish_version, new_handle, is_ready))) {
        LOG_WARN("failed to filter read tables", K(ret), K_(pkey), K(publish_version));
      } else if (!is_ready) {
        // do nothing
        LOG_INFO(
            "restore point sstable not ready", K(index_id), K(tmp_handle), K(snapshot_version), K(publish_version));
      } else if (OB_FAIL(handle.add_tables(new_handle))) {
        LOG_WARN("failed to add table", K(ret), K_(pkey), K(snapshot_version));
      }
    }
  }
  return ret;
}
