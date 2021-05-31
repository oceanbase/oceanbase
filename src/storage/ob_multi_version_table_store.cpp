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
#include "ob_multi_version_table_store.h"
#include "storage/ob_table_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"

using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace memtable;

ObMultiVersionTableStore::ObMultiVersionTableStore()
    : is_inited_(false),
      pkey_(),
      table_id_(OB_INVALID_ID),
      head_(0),
      tail_(0),
      rand_(),
      freeze_info_mgr_(nullptr),
      pg_memtable_mgr_(nullptr),
      gc_sstable_count_(0),
      create_schema_version_(0),
      is_dropped_schema_(false),
      drop_schema_version_(0),
      drop_schema_refreshed_ts_(OB_INVALID_TIMESTAMP)
{
  MEMSET(table_stores_, 0, sizeof(table_stores_));
}

ObMultiVersionTableStore::~ObMultiVersionTableStore()
{
  ObTableStore* table_store = nullptr;

  if (is_inited_) {
    while (head_ < tail_) {
      if (OB_ISNULL(table_store = get_store(head_))) {
        LOG_ERROR("table store must not null", K(pkey_), K(table_id_));
      } else {
        free_table_store(table_store);
        table_stores_[get_idx(head_)] = nullptr;
        ++head_;
      }
    }
    for (int64_t i = 0; i < gc_sstable_count_; ++i) {
      gc_sstable_infos_[i].sstable_->dec_ref();
      gc_sstable_infos_[i].reset();
    }
    gc_sstable_count_ = 0;
  }
  is_inited_ = false;
}

int ObMultiVersionTableStore::init(const common::ObPartitionKey& pkey, const uint64_t table_id,
    ObFreezeInfoSnapshotMgr* freeze_info_mgr, ObPGMemtableMgr* pg_memtable_mgr, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObTableStore* new_store = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(pkey), K(table_id));
  } else if (!pkey.is_valid() || OB_INVALID_ID == table_id || OB_ISNULL(freeze_info_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(pkey), K(table_id), KP(freeze_info_mgr));
  } else {
    pkey_ = pkey;
    table_id_ = table_id;
    MEMSET(table_stores_, 0, sizeof(table_stores_));
    head_ = 0;
    tail_ = 0;
    freeze_info_mgr_ = freeze_info_mgr;
    pg_memtable_mgr_ = pg_memtable_mgr;
    gc_sstable_count_ = 0;
    create_schema_version_ = schema_version;

    if (OB_FAIL(alloc_table_store(new_store))) {
      LOG_WARN("failed to alloc_table_store", K(ret), K(pkey_), K(table_id_));
    } else if (OB_FAIL(new_store->init(pkey_, table_id_, freeze_info_mgr, pg_memtable_mgr))) {
      LOG_WARN("failed to init new store", K(ret), K(pkey_), K(table_id_));
    } else {
      table_stores_[get_idx(tail_)] = new_store;
      ++tail_;
      new_store = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  if (NULL != new_store) {
    free_table_store(new_store);
  }
  return ret;
}

// used in logic split, reference tables from parent partition
int ObMultiVersionTableStore::set_reference_tables(
    ObTablesHandle& handle, ObTableStore*& new_table_store, const int64_t memtable_base_version, bool& need_update)
{
  int ret = OB_SUCCESS;
  ObTablesHandle latest_tables_handle;
  ObTableStore* latest_store = NULL;
  new_table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(latest_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("latest store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(alloc_table_store(new_table_store))) {
    LOG_WARN("failed to alloc_table_store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(new_table_store->init(pkey_, table_id_, freeze_info_mgr_, pg_memtable_mgr_))) {
    LOG_WARN("failed to init new store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->get_all_sstables(true /*need_complent*/, latest_tables_handle))) {
    LOG_WARN("failed to get_all_sstables", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(new_table_store->set_reference_tables(
                 handle, latest_tables_handle, memtable_base_version, need_update))) {
    LOG_WARN("failed to set reference tables", K(ret), K(pkey_), K(table_id_));
  }

  if (OB_FAIL(ret)) {
    if (NULL != new_table_store) {
      free_table_store(new_table_store);
      new_table_store = NULL;
    }
  }
  return ret;
}

// p0 Minor SSTable + memtable split and assign to p1
int ObMultiVersionTableStore::prepare_update_split_table_store(
    const bool is_major_split, ObTablesHandle& handle, bool& need_update, ObTableStore*& new_table_store)
{
  int ret = OB_SUCCESS;
  ObTableStore* latest_store = NULL;
  ObTablesHandle old_handle;
  bool is_complete = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(latest_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("latest store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->check_complete(is_complete))) {
    LOG_WARN("failed to check complete", K(ret), K(pkey_), K(table_id_));
  } else if (!is_complete) {
    need_update = false;
    LOG_INFO("table store is not complete, no need to update", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(alloc_table_store(new_table_store))) {
    LOG_WARN("failed to alloc_table_store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(new_table_store->init(pkey_, table_id_, freeze_info_mgr_, pg_memtable_mgr_))) {
    LOG_WARN("failed to init new store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->get_all_tables(false, /*include_active_memtable*/
                 false,                                  /*include_complement*/
                 old_handle))) {
    LOG_WARN("failed to get_all_tables", K(ret), K(pkey_), K(table_id_));
  } else if (is_major_split && OB_FAIL(new_table_store->build_major_split_store(old_handle, handle, need_update))) {
    LOG_WARN("failed to build split store", K(ret), K(pkey_), K(table_id_));
  } else if (!is_major_split && OB_FAIL(new_table_store->build_minor_split_store(old_handle, handle, need_update))) {
    LOG_WARN("failed to build split store", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::check_need_split(
    common::ObVersion& split_version, bool& need_split, bool& need_minor_split)
{
  int ret = OB_SUCCESS;
  ObTableStore* latest_store = NULL;
  ObTablesHandle old_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(latest_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("latest store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->get_all_tables(false, /*include_active_memtable*/
                 false,                                  /*include_complement*/
                 old_handle))) {
    LOG_WARN("failed to get_all_tables", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->check_need_split(old_handle, split_version, need_split, need_minor_split))) {
    LOG_WARN("failed to check if need split", K(ret), K(pkey_), K(table_id_));
  }

  return ret;
}

int ObMultiVersionTableStore::check_can_migrate(bool& can_migrate)
{
  int ret = OB_SUCCESS;
  ObTableStore* latest_store = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(latest_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("latest store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->check_can_migrate(can_migrate))) {
    LOG_WARN("failed to check physical split finish", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::check_complete(bool& is_complete)
{
  int ret = OB_SUCCESS;
  ObTableStore* latest_store = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(latest_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("latest store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->check_complete(is_complete))) {
    LOG_WARN("failed to check physical split finish", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::is_physical_split_finished(bool& is_physical_split_finish)
{
  int ret = OB_SUCCESS;
  ObTableStore* latest_store = NULL;
  ObTablesHandle old_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(latest_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("latest store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->get_all_tables(false, /*include_active_memtable*/
                 false,                                  /*include_complement*/
                 old_handle))) {
    LOG_WARN("failed to get_all_tables", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->is_physical_split_finished(old_handle, is_physical_split_finish))) {
    LOG_WARN("failed to check physical split finish", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::get_physical_split_info(ObVirtualPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  ObTableStore* latest_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(latest_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("latest store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->get_physical_split_info(split_info))) {
    LOG_WARN("Failed to get physical split info", K(ret), K(pkey_), K(table_id_));
  }

  return ret;
}

// add_sstable_with_prewarm is  used after merge
int ObMultiVersionTableStore::prepare_add_sstable(
    AddTableParam& param, ObTableStore*& new_table_store, bool& need_update)
{
  int ret = OB_SUCCESS;
  need_update = true;
  bool is_equal = false;
  bool can_split = false;
  ObTableStore* latest_store = NULL;
  ObTablesHandle latest_tables_handle;
  new_table_store = NULL;
  bool need_check_split = (NULL != param.table_ && param.table_->get_partition_key() != pkey_) ? true : false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_ISNULL(latest_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("latest store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (need_check_split && OB_FAIL(latest_store->check_can_split(can_split))) {
    LOG_WARN("failed to check can split", K(ret), K(pkey_));
  } else if (can_split) {
    need_update = false;
    LOG_INFO("split dest store has complete tables, no need to update", K(pkey_), K(table_id_));
  } else if (OB_FAIL(alloc_table_store(new_table_store))) {
    LOG_WARN("failed to alloc_table_store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(new_table_store->init(pkey_, table_id_, freeze_info_mgr_, pg_memtable_mgr_))) {
    LOG_WARN("failed to init new store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->get_all_tables(false, /*include_active_memtable*/
                 false,                                  /*include_complement*/
                 latest_tables_handle))) {
    LOG_WARN("failed to get_all_tables", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(add_complement_minor_sstable_if_needed_(param, *latest_store))) {
    LOG_WARN("failed to add_complement_minor_sstable_if_needed", K(ret), K_(pkey), K(table_id_), K(param));
  } else if (OB_FAIL(new_table_store->build_new_merge_store(param, latest_tables_handle))) {
    LOG_WARN("failed to build new merge store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(new_table_store->equals(*latest_store, is_equal))) {
    LOG_WARN("failed to check table store is equal", K(ret), K(pkey_), K(table_id_));
  }

  if (OB_SUCC(ret)) {
    if (!need_update) {
      // do nothing
    } else if (!is_equal) {
      need_update = true;
      LOG_INFO("prepare new merge store",
          K(head_),
          K(tail_),
          "multi_version_start",
          param.multi_version_start_,
          K(is_equal),
          K(latest_tables_handle));
    } else {
      need_update = false;
      LOG_INFO("prepare merge store, but new and old one are same",
          K(head_),
          K(tail_),
          "multi_version_start",
          param.multi_version_start_,
          K(is_equal),
          K(*new_table_store),
          K(latest_tables_handle),
          K(*latest_store));
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != new_table_store) {
      free_table_store(new_table_store);
    }
  }
  return ret;
}

int ObMultiVersionTableStore::prepare_remove_sstable(
    AddTableParam& param, ObTablesHandle& handle, ObTableStore*& new_table_store, bool& is_equal)
{
  int ret = OB_SUCCESS;
  ObTableStore* latest_store = NULL;
  new_table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_ISNULL(latest_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("latest store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(alloc_table_store(new_table_store))) {
    LOG_WARN("failed to alloc_table_store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(new_table_store->init(pkey_, table_id_, freeze_info_mgr_, pg_memtable_mgr_))) {
    LOG_WARN("failed to init new store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(new_table_store->build_new_merge_store(param, handle))) {
    LOG_WARN("failed to build new merge store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(new_table_store->equals(*latest_store, is_equal))) {
    LOG_WARN("failed to check table store is equal", K(ret), K(pkey_), K(table_id_));
  }

  if (OB_SUCC(ret)) {
    if (!is_equal) {
      LOG_INFO("prepare remove store",
          K(head_),
          K(tail_),
          K(param),
          K(is_equal),
          K(*new_table_store),
          K(handle),
          K(*latest_store));
    } else {
      LOG_INFO("prepare remove store, but new and old one are same",
          K(head_),
          K(tail_),
          K(param),
          K(is_equal),
          K(*new_table_store),
          K(handle),
          K(*latest_store));
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != new_table_store) {
      free_table_store(new_table_store);
    }
  }
  return ret;
}

int ObMultiVersionTableStore::alloc_table_store(ObTableStore*& new_table_store)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(new_table_store = op_alloc(ObTableStore))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to new store", K(ret), K(pkey_), K(table_id_));
  } else {
    LOG_DEBUG("succeed to alloc_table_store", KP(new_table_store));
  }
  return ret;
}

void ObMultiVersionTableStore::free_table_store(ObTableStore*& new_table_store)
{
  if (NULL != new_table_store) {
    LOG_INFO("succeed to free_table_store", KP(new_table_store));
    op_free(new_table_store);
    new_table_store = NULL;
  }
}

int ObMultiVersionTableStore::enable_table_store(const bool need_prewarm, ObTableStore& new_table_store)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (get_idx(tail_ + 1) == get_idx(head_) && OB_FAIL(retire_head_store())) {
    LOG_WARN("failed to retire_store", K(ret), K(pkey_), K(table_id_));
  } else if (get_idx(tail_ + 1) == get_idx(head_) || NULL != get_store(tail_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid head and tail", K(ret), K(tail_), K(head_), KP(get_store(tail_)));
  } else if (OB_SUCCESS != (tmp_ret = record_gc_minor_sstable(new_table_store))) {
    LOG_WARN("failed to record gc minor sstable", K(tmp_ret));
  }

  if (OB_SUCC(ret) && !need_prewarm) {
    while (OB_SUCC(ret) && head_ < tail_) {
      ObTableStore* table_store = get_store(head_);
      if (NULL != table_store) {
        LOG_INFO("retire table store",
            K(pkey_),
            K(table_id_),
            "left_count",
            tail_ - head_ - 1,
            K(tail_),
            K(head_),
            K(*table_store));
        free_table_store(table_store);
        table_stores_[get_idx(head_)] = NULL;
        ++head_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    table_stores_[get_idx(tail_)] = &new_table_store;
    ++tail_;
    LOG_DEBUG("enable new sstable store", "count", tail_ - head_, K(tail_), K(head_), K(new_table_store));
  }
  return ret;
}

int ObMultiVersionTableStore::get_read_tables(
    const int64_t snapshot_version, ObTablesHandle& handle, const bool allow_not_ready, const bool print_dropped_alert)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (is_dropped_schema_) {
    if (print_dropped_alert) {
      ret = OB_ERR_SYS;
      LOG_ERROR("should not read dropped table store", K(ret), K(pkey_), K(table_id_), K(is_dropped_schema_));
    } else {
      ret = OB_PARTITION_IS_REMOVED;
      LOG_WARN("cannot read dropped table store", K(ret), K(pkey_), K(table_id_), K(is_dropped_schema_));
    }
  } else if (OB_ISNULL(table_store = get_read_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("no table store exists", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_read_tables(snapshot_version, handle, allow_not_ready))) {
    LOG_WARN("failed to get read tables", K(ret));
  } else {
    handle.set_retire_check();
  }

  return ret;
}

int ObMultiVersionTableStore::get_reference_tables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("no table store exists", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_reference_tables(handle))) {
    LOG_WARN("failed to get reference tables", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::get_sample_read_tables(const common::SampleInfo& sample_info, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("no table store exists", K(ret), K(pkey_), K(table_id_));
    LOG_WARN("failed to get sample read tables", K(ret), K(sample_info));
  } else if (OB_FAIL(table_store->get_sample_read_tables(sample_info, handle))) {
  }

  return ret;
}

int ObMultiVersionTableStore::get_major_sstable(const ObVersion& version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("no table store exists", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_major_sstable(version, handle))) {
    LOG_WARN("failed to get_major_sstable", K(ret), K(version));
  }

  return ret;
}

int ObMultiVersionTableStore::get_all_tables(TableSet& table_set, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    const bool is_single_table_store = (1 == (tail_ - head_));
    for (int64_t pos = head_; OB_SUCC(ret) && pos < tail_; ++pos) {
      ObTablesHandle tmp_handle;
      if (OB_ISNULL(table_store = get_store(pos))) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store must not null", K(ret), K(pkey_), K(table_id_));
      } else if (OB_FAIL(table_store->get_all_tables(true, /*include_active_memtable*/
                     true,                                 /*include_complement_minor_sstable*/
                     tmp_handle))) {
        LOG_WARN("failed to get all tables", K(ret), K(pkey_), K(table_id_));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_handle.get_count(); ++i) {
          ObITable* table = tmp_handle.get_table(i);
          int64_t table_addr = reinterpret_cast<int64_t>(table);
          if (OB_ISNULL(table)) {
            ret = OB_ERR_SYS;
            LOG_ERROR("table must not null", K(ret), K(pkey_), K(table_id_));
          } else if (!is_single_table_store || table->is_memtable()) {
            if (OB_HASH_EXIST == (hash_ret = table_set.exist_refactored(table_addr))) {
              // pass
            } else if (OB_HASH_NOT_EXIST != hash_ret) {
              ret = hash_ret;
              LOG_WARN("failed to check if table exists", K(ret), K(pkey_), K(table_id_));
            } else if (OB_FAIL(table_set.set_refactored(table_addr))) {
              LOG_WARN("failed to set table addr", K(ret));
            } else if (OB_FAIL(handle.add_table(table))) {
              LOG_WARN("failed to add table", K(ret), K(pkey_), K(table_id_));
            }
          } else {
            // table_set is passed from caller due to performance consideration.
            // Different ObMultiVersionTableStore won't have same sstable, so we can skip
            // check table_set when there's only one table store.
            if (OB_FAIL(handle.add_table(table))) {
              LOG_WARN("failed to add table", K(ret), K(pkey_), K(table_id_));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObMultiVersionTableStore::check_memtable_merged(const ObMemtable& memtable, bool& all_merged, bool& can_release)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    all_merged = false;
    can_release = true;
    for (int64_t pos = head_; OB_SUCC(ret) && pos < tail_; ++pos) {
      bool need_merge = false;
      if (OB_ISNULL(table_store = get_store(pos))) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store must not null", K(ret), K(pkey_), K(table_id_));
      } else if (OB_FAIL(table_store->is_memtable_need_merge(memtable, need_merge))) {
        LOG_WARN("failed to check memtable need merge", K(ret), K(*table_store), K(memtable), K(need_merge));
      } else if (need_merge) {
        can_release = false;
      } else {
        all_merged = true;
      }
    }
    if (OB_SUCC(ret)) {
      can_release = can_release && all_merged;
    }
  }

  return ret;
}

int ObMultiVersionTableStore::get_effective_tables(const bool include_active_memtable, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
    // get_effective_tables no need to get buffer minor sstable now
    // TODO  remove this interface
  } else if (OB_FAIL(table_store->get_all_tables(include_active_memtable,
                 false, /*include_complement*/
                 handle))) {
    LOG_WARN("failed to get latest sstable", K(ret), K(pkey_), K(table_id_));
  }

  return ret;
}

int ObMultiVersionTableStore::get_latest_major_sstable(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_latest_major_sstable(handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get latest sstable", K(ret), K(pkey_), K(table_id_));
    }
  }

  return ret;
}

int ObMultiVersionTableStore::get_sstable_schema_version(int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  ObTableHandle handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_latest_major_sstable(handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get table handle", K(ret));
    }
  } else if (OB_FAIL(handle.get_sstable_schema_version(schema_version))) {
    LOG_WARN("fail to get sstable schema version", K(ret));
  }

  return ret;
}

int ObMultiVersionTableStore::get_replay_tables(ObIArray<ObITable::TableKey>& replay_tables)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_replay_tables(replay_tables))) {
    LOG_WARN("failed to get_replay_tables", K(ret), K(pkey_), K(table_id_));
  }

  return ret;
}

int ObMultiVersionTableStore::get_oldest_read_tables(const int64_t snapshot_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_oldest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("no table store exists", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_read_tables(snapshot_version, handle))) {
    LOG_WARN("failed to get read tables", K(ret));
  } else {
    handle.set_retire_check();
  }

  return ret;
}

int ObMultiVersionTableStore::get_merge_tables(
    const ObGetMergeTablesParam& param, const int64_t multi_version_start, ObGetMergeTablesResult& result)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!param.is_valid() || table_id_ != param.index_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), K(table_id_));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (MAJOR_MERGE == param.merge_type_) {
    if (OB_FAIL(table_store->get_major_merge_tables(param, result))) {
      LOG_WARN("failed to get major merge tables", K(ret), K(param), K(*this));
    }
  } else if (MINI_MINOR_MERGE == param.merge_type_) {
    if (OB_FAIL(table_store->get_mini_minor_merge_tables(param, multi_version_start, result))) {
      LOG_WARN("failed to get minor merge tables", K(ret), K(param), K(*this));
    }
  } else if (MINI_MERGE == param.merge_type_) {
    if (OB_FAIL(table_store->get_mini_merge_tables(param, multi_version_start, result))) {
      LOG_WARN("failed to get minor merge tables", K(ret), K(param), K(*this));
    }
  } else if (HISTORY_MINI_MINOR_MERGE == param.merge_type_) {
    if (OB_FAIL(table_store->get_hist_minor_merge_tables(param, multi_version_start, result))) {
      LOG_WARN("failed to get history mini minor merge tables", K(ret), K(param), K(*this));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support merge type", K(ret), K(param), K(*this));
  }
  return ret;
}

int ObMultiVersionTableStore::get_split_tables(const bool is_major_split, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (is_major_split) {
    if (OB_FAIL(table_store->get_major_split_tables(handle))) {
      LOG_WARN("failed to get major split tables", K(ret), K(*this));
    }
  } else {
    if (OB_FAIL(table_store->get_minor_split_tables(handle))) {
      LOG_WARN("failed to get minor split tables", K(ret), K(*this));
    }
  }
  return ret;
}

int ObMultiVersionTableStore::retire_prewarm_store(const int64_t duration, const int64_t minor_deferred_gc_time)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;
  const int64_t retire_ts = duration <= 0 ? INT64_MAX : ObTimeUtility::current_time() - duration;
  const int64_t retire_gc_sstbale_ts =
      minor_deferred_gc_time <= 0 ? INT64_MAX : ObTimeUtility::current_time() - minor_deferred_gc_time;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    while (OB_SUCC(ret) && head_ < tail_ - 1) {
      if (OB_ISNULL(table_store = get_store(head_))) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table store must not null", K(ret), K(pkey_), K(table_id_));
      } else if (table_store->get_uptime() < retire_ts) {
        LOG_INFO("retire table store",
            K(pkey_),
            K(table_id_),
            K(duration),
            K(retire_ts),
            "left_count",
            tail_ - head_ - 1,
            K(*table_store));
        free_table_store(table_store);
        table_stores_[get_idx(head_)] = NULL;
        ++head_;
      } else {
        break;
      }
    }

    for (int64_t i = gc_sstable_count_ - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (gc_sstable_infos_[i].retired_ts_ < retire_gc_sstbale_ts) {
        ObGCSSTableInfo& gc_sstable_info = gc_sstable_infos_[i];
        LOG_INFO("retire gc sstable", K(gc_sstable_count_), K(minor_deferred_gc_time), K(gc_sstable_info));
        gc_sstable_info.sstable_->dec_ref();
        gc_sstable_info.reset();
        --gc_sstable_count_;
      } else {
        break;
      }
    }
  }

  return ret;
}

int ObMultiVersionTableStore::retire_head_store()
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tail_ - head_ <= 1) {
    ret = OB_ERR_SYS;
    LOG_ERROR("only one table store exist, cannot retire", K(ret), K(*this));
  } else if (OB_ISNULL(table_store = get_store(head_))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K(pkey_), K(table_id_));
  } else {
    LOG_INFO("retire_head_store", K(pkey_), K(table_id_), K(head_), K(tail_), K(*table_store));
    free_table_store(table_store);
    table_stores_[get_idx(head_)] = NULL;
    ++head_;
  }

  return ret;
}

int ObMultiVersionTableStore::get_latest_table_count(int64_t& latest_table_count)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_table_count(latest_table_count))) {
    LOG_WARN("failed to get table count", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::check_latest_table_count_safe(bool& is_safe)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->check_table_count_safe(is_safe))) {
    LOG_WARN("failed to check table count safe", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::latest_has_major_sstable(bool& has_major)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->has_major_sstable(has_major))) {
    LOG_WARN("fail to check has major sstable", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int64_t ObMultiVersionTableStore::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV(K_(pkey),
        K_(table_id),
        K_(create_schema_version),
        K_(is_dropped_schema),
        "count",
        tail_ - head_,
        K_(tail),
        K_(head));
    J_ARRAY_START();
    for (int64_t tmp_pos = head_; tmp_pos < tail_; ++tmp_pos) {
      const ObTableStore* table_store = table_stores_[get_idx(tmp_pos)];
      J_KV(K(tmp_pos), K(table_store));
    }
    J_ARRAY_END();
    J_ARRAY_START();
    for (int64_t i = 0; i < gc_sstable_count_; ++i) {
      const ObGCSSTableInfo& gc_info = gc_sstable_infos_[i];
      if (gc_info.sstable_ != NULL) {
        J_KV("retired_ts", gc_info.retired_ts_, "table_key", gc_info.sstable_->get_key());
      } else {
        J_KV("retired_ts", gc_info.retired_ts_, "table_key", "null");
      }
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

int ObMultiVersionTableStore::finish_replay(const int64_t multi_version_start)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tail_ - head_ > 1 || OB_ISNULL(get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid store status", K(ret), K(*this));
  } else if (OB_FAIL(get_latest_store()->finish_replay(multi_version_start))) {
    LOG_WARN("failed to finish_replay", K(ret));
  }
  return ret;
}

int ObMultiVersionTableStore::check_ready_for_read(bool& is_ready)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else {
    is_ready = table_store->is_ready_for_read();
  }

  return ret;
}

int ObMultiVersionTableStore::need_remove_old_table(const common::ObVersion& kept_min_version,
    const int64_t multi_version_start, const int64_t backup_snapshot_verison, int64_t& real_kept_major_num,
    bool& need_remove)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->need_remove_old_table(
                 kept_min_version, multi_version_start, backup_snapshot_verison, real_kept_major_num, need_remove))) {
    LOG_WARN("fail to check need remove old table", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::set_table_store(const ObTableStore& table_store)
{
  int ret = OB_SUCCESS;
  ObTableStore* new_table_store = NULL;
  const bool need_prewarm = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (tail_ - head_ > 1) {
    ret = OB_ERR_SYS;
    LOG_ERROR("during replay, table store must not more than one", K(ret), K(*this));
  } else if (OB_FAIL(alloc_table_store(new_table_store))) {
    LOG_WARN("failed to alloc_table_store", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(new_table_store->init(table_store, freeze_info_mgr_, pg_memtable_mgr_))) {
    LOG_WARN("failed to init new store", K(ret), K(table_store));
  } else if (OB_FAIL(enable_table_store(need_prewarm, *new_table_store))) {
    LOG_WARN("Failed to enable table store", K(ret));
  } else {
    new_table_store = NULL;
  }

  if (NULL != new_table_store) {
    free_table_store(new_table_store);
  }
  return ret;
}

int ObMultiVersionTableStore::check_need_minor_merge(
    const bool is_follower_data_rep, const ObMergeType merge_type, bool& need_merge)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (MINI_MINOR_MERGE == merge_type) {
    if (OB_FAIL(table_store->check_need_mini_minor_merge(is_follower_data_rep, need_merge))) {
      LOG_WARN("failed to check_need_mini_minor_merge", K(ret), K(pkey_), K(table_id_));
    }
  } else if (HISTORY_MINI_MINOR_MERGE == merge_type) {
    if (OB_FAIL(table_store->check_need_hist_minor_merge(need_merge))) {
      LOG_WARN("failed to check_need_hist_minor_merge", K(ret), K(pkey_), K(table_id_));
    }
  } else {
    ret = OB_ERR_SYS;
    LOG_ERROR("Unsupported merge type", K(ret), K(merge_type));
  }
  return ret;
}

ObMultiVersionTableStore::ObGCSSTableInfo::ObGCSSTableInfo() : sstable_(nullptr), retired_ts_(0)
{}

ObMultiVersionTableStore::ObGCSSTableInfo::~ObGCSSTableInfo()
{
  sstable_ = nullptr;
  retired_ts_ = 0;
}

void ObMultiVersionTableStore::ObGCSSTableInfo::reset()
{
  sstable_ = nullptr;
  retired_ts_ = 0;
}

int64_t ObMultiVersionTableStore::ObGCSSTableInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    if (sstable_ != NULL) {
      J_KV("retired_ts", retired_ts_, "table_key", sstable_->get_key());
    } else {
      J_KV("retired_ts", retired_ts_, "table_key", "null");
    }
    J_OBJ_END();
  }
  return pos;
}

int ObMultiVersionTableStore::record_gc_minor_sstable(ObTableStore& new_table_store)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  const int64_t set_bucket_num = common::OB_MAX_VERSION_COUNT * 2;
  const int64_t minor_deferred_gc_level = GCONF._minor_deferred_gc_level;
  const int64_t minor_deferred_gc_time = GCONF.minor_deferred_gc_time;
  TableSet new_table_set;
  ObTablesHandle new_tables_handle;
  ObTablesHandle old_tables_handle;
  int64_t cur_time = ObTimeUtility::current_time();

  if (OB_FAIL(new_table_set.create(set_bucket_num))) {
    LOG_WARN("failed to create new table set", K(ret));
  } else if (OB_FAIL(new_table_store.get_all_sstables(false /*need_complent*/, new_tables_handle))) {
    LOG_WARN("failed to get_all_sstables", K(ret));
  } else if (NULL != get_latest_store()) {
    if (OB_FAIL(get_latest_store()->get_all_sstables(false /*need_complent*/, old_tables_handle))) {
      LOG_WARN("Failed to get old sstables", K(ret));
    }
  }

  if (OB_SUCC(ret) && old_tables_handle.get_count() > 0 && minor_deferred_gc_time > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_tables_handle.get_count(); ++i) {
      ObITable* table = new_tables_handle.get_table(i);
      int64_t table_addr = reinterpret_cast<int64_t>(table);
      if (OB_FAIL(new_table_set.set_refactored(table_addr))) {
        LOG_WARN("failed to set table addr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < old_tables_handle.get_count(); ++i) {
      ObITable* table = old_tables_handle.get_table(i);
      int64_t table_addr = reinterpret_cast<int64_t>(table);
      hash_ret = new_table_set.exist_refactored(table_addr);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret));
      } else if (table->is_mini_minor_sstable() || (table->is_minor_sstable() && 1 == minor_deferred_gc_level)) {
        if (OB_HASH_NOT_EXIST == hash_ret) {
          if (OB_FAIL(add_gc_minor_sstable(
                  cur_time, static_cast<ObSSTable*>(table), MAX_DEFERRED_GC_MINOR_SSTABLE_COUNT))) {
            LOG_WARN("failed to add gc minor sstable", K(ret));
          }
        } else if (OB_HASH_EXIST == hash_ret) {
          // do nothing;
        } else {
          ret = OB_ERR_SYS;
          LOG_WARN("failed to check hash_set exist", K(ret), K(hash_ret));
        }
      }
    }
  }
  return ret;
}

int ObMultiVersionTableStore::add_gc_minor_sstable(
    const int64_t gc_ts, ObSSTable* table, const int64_t max_deferred_gc_count)
{
  int ret = OB_SUCCESS;
  int64_t find_pos = -1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table)) {
    ret = OB_ERR_SYS;
    LOG_WARN("table must not null", K(ret));
  } else {
    for (int64_t i = 0; i < gc_sstable_count_; ++i) {
      if (gc_sstable_infos_[i].sstable_ == table) {
        find_pos = i;
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (-1 == find_pos) {  // add new
      if (gc_sstable_count_ == max_deferred_gc_count) {
        ObGCSSTableInfo& last_info = gc_sstable_infos_[gc_sstable_count_ - 1];
        last_info.sstable_->dec_ref();
        last_info.reset();
        --gc_sstable_count_;
      }

      for (int64_t i = gc_sstable_count_; i >= 1; --i) {
        gc_sstable_infos_[i] = gc_sstable_infos_[i - 1];
      }
      gc_sstable_infos_[0].sstable_ = table;
      gc_sstable_infos_[0].retired_ts_ = gc_ts;
      table->inc_ref();
      ++gc_sstable_count_;
      ObArrayWrap<ObGCSSTableInfo> new_gc_sstables(gc_sstable_infos_, gc_sstable_count_);
      LOG_INFO("add_gc_minor_sstable", K_(gc_sstable_count), K(new_gc_sstables));
    } else {  // update ts
      ObGCSSTableInfo tmp_info = gc_sstable_infos_[find_pos];
      tmp_info.retired_ts_ = gc_ts;
      for (int64_t i = find_pos; i >= 1; --i) {
        gc_sstable_infos_[i] = gc_sstable_infos_[i - 1];
      }
      gc_sstable_infos_[0] = tmp_info;
      ObArrayWrap<ObGCSSTableInfo> new_gc_sstables(gc_sstable_infos_, gc_sstable_count_);
      LOG_INFO("update add_gc_minor_sstable", K_(gc_sstable_count), K(new_gc_sstables));
    }
  }

  return ret;
}

int ObMultiVersionTableStore::get_gc_sstables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < gc_sstable_count_; ++i) {
      if (OB_FAIL(handle.add_table(gc_sstable_infos_[i].sstable_))) {
        LOG_WARN("Failed to get gc sstable", K(ret), K(i));
      }
    }
    ObArrayWrap<ObGCSSTableInfo> gc_sstables(gc_sstable_infos_, gc_sstable_count_);
    LOG_INFO("get_gc_sstables", K_(gc_sstable_count), K(gc_sstables), K(handle));
  }
  return ret;
}

int ObMultiVersionTableStore::set_replay_sstables(
    const bool is_replay_old, const common::ObIArray<ObSSTable*>& sstables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiVersionTableStore has not been inited", K(ret));
  } else if (tail_ - head_ > 1 || OB_ISNULL(get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid store status", K(ret), K(*this));
  } else if (OB_FAIL(get_latest_store()->set_replay_sstables(is_replay_old, sstables))) {
    LOG_WARN("failed to finish_replay", K(ret));
  }
  return ret;
}

int ObMultiVersionTableStore::get_multi_version_start(int64_t& multi_version_start)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_multi_version_start(multi_version_start))) {
    LOG_WARN("failed to get multi_version_start", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::get_max_major_sstable_snapshot(int64_t& max_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;
  ObTableHandle handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_latest_major_sstable(handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      max_snapshot_version = INT64_MIN;
    } else {
      LOG_WARN("failed to get multi_version_start", K(ret), K(pkey_), K(table_id_));
    }
  } else {
    max_snapshot_version = handle.get_table()->get_snapshot_version();
  }
  return ret;
}

int ObMultiVersionTableStore::get_min_max_major_version(int64_t& min_version, int64_t& max_version)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_min_max_major_version(min_version, max_version))) {
    LOG_WARN("failed to get multi_version_start", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::get_latest_minor_sstables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("latest_store can not be null", K(ret), K(*this));
  } else if (OB_FAIL(table_store->get_minor_sstables(handle))) {
    LOG_WARN("failed to get_minor_sstables", K(ret), K(*this));
  }
  return ret;
}

int ObMultiVersionTableStore::add_complement_minor_sstable_if_needed_(AddTableParam& param, ObTableStore& store)
{
  int ret = OB_SUCCESS;
  if (nullptr == param.complement_minor_sstable_) {
    param.complement_minor_sstable_ = store.get_complement_minor_sstable();
  }
  return ret;
}

int ObMultiVersionTableStore::get_latest_continue_tables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* latest_store = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(latest_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("latest store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(latest_store->get_continue_tables(handle))) {
    LOG_WARN("failed to get continue tables", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::get_min_schema_version(int64_t& min_schema_version)
{
  int ret = OB_SUCCESS;
  min_schema_version = 0;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_min_schema_version(min_schema_version))) {
    LOG_WARN("failed to get schema version", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::get_flashback_major_tables(const int64_t flashback_scn, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_flashback_major_sstable(flashback_scn, handle))) {
    LOG_WARN("failed to get latest sstable", K(ret), K(pkey_), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::get_mark_deletion_tables(
    const int64_t end_log_ts, const int64_t snapshot_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiVersionTableStore is not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_mark_deletion_tables(end_log_ts, snapshot_version, handle))) {
    LOG_WARN("failed to get_mark_deletion_tables", K(ret), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::clear_complement_minor_sstable()
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiVersionTableStore is not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->clear_complement_minor_sstable())) {
    LOG_WARN("failed to clear_complement_minor_sstable", K(ret), K(table_id_));
  }
  return ret;
}

int ObMultiVersionTableStore::get_schema_version(int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = nullptr;
  int64_t table_count = 0;
  schema_version = INT64_MAX;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMultiVersionTableStore is not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("table store must not null", K(ret), K_(pkey), K_(table_id));
  } else if (OB_FAIL(table_store->get_table_count(table_count))) {
    LOG_WARN("failed to get_table_count", K(ret));
  } else if (table_count > 0) {
    if (OB_FAIL(table_store->get_schema_version(schema_version))) {
      LOG_WARN("failed to get_schema_version", K(ret));
    }
  } else if (create_schema_version_ <= 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("create_schema_version should be valid when no table exist", K(ret));
  } else {
    schema_version = create_schema_version_;
  }
  return ret;
}

int ObMultiVersionTableStore::get_needed_local_tables_for_migrate(
    const ObMigrateRemoteTableInfo& remote_table_info, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("multi version table store is inited", K(ret));
  } else if (pkey_.is_trans_table()) {
    handle.reset();
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store should not be null", K(ret));
  } else if (OB_FAIL(table_store->get_needed_local_tables_for_migrate(remote_table_info, handle))) {
    LOG_WARN("failed to get_needed_local_tables_for_migrate", K(ret), K(remote_table_info));
  }
  return ret;
}

int ObMultiVersionTableStore::get_migrate_tables(ObTablesHandle& handle, bool& is_ready_for_read)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;
  is_ready_for_read = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_all_sstables(false /*need_complent*/, handle))) {
    LOG_WARN("failed to get latest sstable", K(ret), K(pkey_), K(table_id_));
  } else {
    is_ready_for_read = table_store->is_ready_for_read();
  }

  return ret;
}

int ObMultiVersionTableStore::set_drop_schema_info(const int64_t drop_schema_version)
{
  int ret = OB_SUCCESS;
  drop_schema_version_ = drop_schema_version;
  drop_schema_refreshed_ts_ = ObTimeUtility::current_time();
  return ret;
}

int ObMultiVersionTableStore::get_drop_schema_info(int64_t& drop_schema_version, int64_t& drop_schema_refreshed_ts)
{
  int ret = OB_SUCCESS;
  drop_schema_version = drop_schema_version_;
  drop_schema_refreshed_ts = drop_schema_refreshed_ts_;
  return ret;
}

int ObMultiVersionTableStore::get_recovery_point_tables(const int64_t snapshot_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTableStore* table_store = NULL;
  int64_t multi_version_start = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (OB_ISNULL(table_store = get_latest_store())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("store must not null", K(ret), K(pkey_), K(table_id_));
  } else if (OB_FAIL(table_store->get_recovery_point_tables(snapshot_version, handle))) {
    LOG_WARN("failed to get recovery point tables", K(ret), K(snapshot_version));
  }
  return ret;
}
