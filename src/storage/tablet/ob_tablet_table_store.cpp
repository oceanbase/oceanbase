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
#define PRINT_TS(x) (ObPrintTableStore(x))

#include "lib/oblog/ob_log_module.h"
#include "share/ob_force_print_log.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/ob_i_memtable_mgr.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "share/scn.h"
#include "lib/container/ob_array_iterator.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"

using namespace oceanbase;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::memtable;
using namespace oceanbase::share::schema;
using compaction::ObPartitionMergePolicy;
using compaction::ObAdaptiveMergePolicy;


/* ObTabletTableStore Section */
ObTabletTableStore::ObTabletTableStore()
  : tablet_ptr_(nullptr),
    major_tables_(),
    minor_tables_(),
    ddl_sstables_(),
    extend_tables_(),
    memtables_(),
    read_cache_(),
    version_(TABLE_STORE_VERSION),
    is_ready_for_read_(false),
    is_inited_(false)
{
}

ObTabletTableStore::~ObTabletTableStore()
{
  reset();
}

void ObTabletTableStore::reset()
{
  major_tables_.destroy();
  minor_tables_.destroy();
  ddl_sstables_.destroy();
  extend_tables_.destroy();
  memtables_.destroy();
  ddl_mem_sstables_.destroy();
  read_cache_.reset();
  is_ready_for_read_ = false;
  tablet_ptr_ = nullptr;
  is_inited_ = false;
}

int ObTabletTableStore::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t serialized_length = get_serialize_size();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("table store not init", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(NULL == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, version_))) {
    LOG_WARN("failed to serialize table_store_version", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, serialized_length))) {
    LOG_WARN("failed to seriazlie serialized_length", K(ret));
  } else if (OB_FAIL(major_tables_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize major_tables", K(ret));
  } else if (OB_FAIL(minor_tables_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize minor_tables", K(ret));
  } else if (OB_FAIL(ddl_sstables_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize ddl_sstables_", K(ret));
  } else if (OB_FAIL(extend_tables_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize extend_tables", K(ret));
  }
  return ret;
}

int ObTabletTableStore::deserialize(
    common::ObIAllocator &allocator,
    ObTablet *tablet,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t serialized_length = 0;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot deserialize inited ObTabletTableStore", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(NULL == tablet || NULL == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), KP(tablet), KP(buf), K(data_len));
  } else if (OB_FAIL(init(allocator, tablet))) {
    LOG_WARN("failed to init an empty Table Store before deserializing", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &version_))) {
    LOG_WARN("failed to deserialize table_store_version", K(ret));
  } else if (version_ != TABLE_STORE_VERSION) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("version_ must be equal to TABLE_STORE_VERSION", K(ret), K(version_));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &serialized_length))) {
    LOG_WARN("failed to deserialize serialized_length", K(ret));
  } else if (OB_FAIL(major_tables_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize major_tables", K(ret));
  } else if (OB_FAIL(minor_tables_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize minor_tables", K(ret));
  } else if (OB_FAIL(ddl_sstables_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize ddl_sstables_", K(ret));
  } else if (OB_FAIL(extend_tables_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize extend_tables", K(ret));
  } else if (OB_FAIL(pull_memtables())) {
    LOG_WARN("failed to pull memtable from memtable_mgr", K(ret));
  } else if (OB_FAIL(pull_ddl_memtables(allocator))) {
    LOG_WARN("pull_ddl_memtables failed", K(ret));
  } else if (OB_FAIL(check_ready_for_read())) {
    LOG_WARN("failed to check ready for read", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = init_read_cache())) {
      LOG_WARN("failed to init read cache iterator", K(tmp_ret));
    }
    FLOG_INFO("succeed to deserialize table store", K(major_tables_), K(minor_tables_), K(memtables_), K(PRINT_TS(*this)));
  }

  return ret;
}

int64_t ObTabletTableStore::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(version_);
  len += serialization::encoded_length_i64(len);
  len += major_tables_.get_serialize_size();
  len += minor_tables_.get_serialize_size();
  len += ddl_sstables_.get_serialize_size();
  len += extend_tables_.get_serialize_size();
  return len;
}

int ObTabletTableStore::init(
    ObIAllocator &allocator,
    ObTablet *tablet,
    ObTableHandleV2 * const handle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletTableStore init twice", K(ret), K(*this));
  } else if (OB_ISNULL(tablet) || OB_ISNULL(tablet->get_memtable_mgr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to init ObTabletTableStore", K(ret), KP(tablet));
  } else if (OB_FAIL(extend_tables_.init(allocator, EXTEND_CNT))) {
    LOG_WARN("Failed to init extend tables array", K(ret));
  } else if (OB_FAIL(memtables_.init(&allocator))) {
    LOG_WARN("Failed to init memtable array", K(ret));
  } else {
    tablet_ptr_ = tablet;
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(handle) && handle->is_valid()) { // init sstables when first creating tablet
    ObSEArray<ObTableHandleV2, MAX_MEMSTORE_CNT> memtable_handles;
    if (tablet->get_memtable_mgr()->has_memtable()) {
      if (OB_FAIL(tablet->get_memtable_mgr()->get_all_memtables(memtable_handles))) {
        LOG_WARN("failed to get all memtables from memtable_mgr", K(ret));
      } else if (OB_FAIL(memtables_.build(memtable_handles))) {
        LOG_WARN("failed to build memtable array", K(ret));
      } else {
        LOG_INFO("success to pull memtables when first creating tablet", K(ret), K(*this));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(handle->get_table()) || !handle->get_table()->is_major_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected table handle", K(ret), KPC(handle->get_table()));
    } else if (OB_FAIL(major_tables_.init(allocator, 1))) {
      LOG_WARN("Failed to init major tables", K(ret));
    } else if (OB_FAIL(major_tables_.assign(0, handle->get_table()))) {
      LOG_WARN("Failed to add table to major_tables", K(ret));
    } else {
      is_ready_for_read_ = true; // exist major sstable and no minor sstable, must be ready for read
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = init_read_cache())) {
        LOG_WARN("failed to init read cache, just skip", K(tmp_ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTabletTableStore::init(
    ObIAllocator &allocator,
    ObTablet *tablet,
    const ObUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid() || !old_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(param), K(old_store));
  } else if (OB_FAIL(init(allocator, tablet))) {
    LOG_WARN("failed to init a new empty table store", K(ret), K(old_store));
  } else if (OB_FAIL(build_new_table_store(allocator, param, old_store))) {
    LOG_WARN("failed to build new table store with old store", K(ret), K(old_store), K(*this));
  }
  return ret;
}

int ObTabletTableStore::build_ha_new_table_store(
    common::ObIAllocator &allocator,
    ObTablet *tablet,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet) || !param.is_valid() || !old_store.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table store get invalid argument", K(ret), KP(tablet), K(param), K(old_store));
  } else if (OB_FAIL(init(allocator, tablet))) {
    LOG_WARN("failed to init a new empty table store", K(ret));
  } else if (OB_FAIL(build_ha_new_table_store_(allocator, param, old_store))) {
    LOG_WARN("failed to build new table store with old store", K(ret));
  }
  return ret;
}

int ObTabletTableStore::batch_replace_sstables(
    common::ObIAllocator &allocator,
    ObTablet *tablet,
    const ObIArray<ObTableHandleV2> &table_handles,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet) || !old_store.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table store get invalid argument", K(ret), KP(tablet), K(old_store));
  } else if (OB_FAIL(init(allocator, tablet))) {
    LOG_WARN("failed to init a new empty table store", K(ret));
  } else if (OB_FAIL(inner_replace_sstables(allocator, table_handles, old_store))) {
    LOG_WARN("failed to build new table store with old store", K(ret));
  }
  return ret;
}

int ObTabletTableStore::inner_replace_sstables(
    common::ObIAllocator &allocator,
    const ObIArray<ObTableHandleV2> &table_handles,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  const ObITableArray &old_extend = old_store.extend_tables_;
  // check table key first
  ObTableHandleV2 tmp_handle;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_handles.count(); ++i) {
    const ObITable *table = table_handles.at(i).get_table();
    if (OB_UNLIKELY(nullptr == table || !table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must be sstable", K(ret), KPC(table));
    } else if (OB_FAIL(old_store.get_table(table->get_key(), tmp_handle))) {
      LOG_WARN("failed to get the same key sstable in old store", K(ret), KPC(table), K(old_store));
    }
  }
  ObSEArray<ObITable *, 8> major_tables;
  ObSEArray<ObITable *, 8> minor_tables;
  ObSEArray<ObITable *, 8> ddl_sstables;
  ObSEArray<ObITable *, 8> ddl_mem_sstables;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_replaced_tables(table_handles, old_store.major_tables_, major_tables))) {
    LOG_WARN("failed to get replaced major tables", K(ret));
  } else if (OB_FAIL(get_replaced_tables(table_handles, old_store.minor_tables_, minor_tables))) {
    LOG_WARN("failed to get replaced minor tables", K(ret));
  } else if (OB_FAIL(get_replaced_tables(table_handles, old_store.ddl_sstables_, ddl_sstables))) {
    LOG_WARN("failed to get replaced ddl sstables", K(ret));
  } else if (OB_FAIL(get_replaced_tables(table_handles, old_store.ddl_mem_sstables_, ddl_mem_sstables))) {
    LOG_WARN("failed to get replaced ddl memtables", K(ret));
  } else if (!major_tables.empty() && OB_FAIL(major_tables_.init_and_copy(allocator, major_tables))) {
    LOG_WARN("failed to init major tables", K(ret));
  } else if (!minor_tables.empty() && OB_FAIL(minor_tables_.init_and_copy(allocator, minor_tables))) {
    LOG_WARN("failed to init minor tables", K(ret));
  } else if (!ddl_sstables.empty() && OB_FAIL(ddl_sstables_.init_and_copy(allocator, ddl_sstables))) {
    LOG_WARN("failed to init ddl tables", K(ret));
  } else if (nullptr != old_extend[META_MAJOR] && OB_FAIL(extend_tables_.assign(META_MAJOR, old_extend[META_MAJOR]))) {
    LOG_WARN("failed to build buf minor table", K(ret), K(old_extend));
  } else if (OB_FAIL(pull_memtables())) {
    LOG_WARN("failed to pull memtable from memtable_mgr", K(ret));
  } else if (OB_FAIL(pull_ddl_memtables(allocator))) {
    LOG_WARN("pull_ddl_memtables failed", K(ret));
  } else if (OB_FAIL(check_ready_for_read())) {
    LOG_WARN("failed to check ready for read", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = init_read_cache())) {
      if (OB_SNAPSHOT_DISCARDED != tmp_ret) {
        LOG_WARN("failed to cache read iterator", K(tmp_ret));
      }
    }
    FLOG_INFO("succeed to batch replace table store", K(major_tables_), K(minor_tables_), K(memtables_), K(PRINT_TS(*this)));
  }
  return ret;
}

int ObTabletTableStore::get_replaced_tables(
    const ObIArray<ObTableHandleV2> &table_handles,
    const ObITableArray &old_tables,
    ObSEArray<ObITable *, 8> &replaced_tables) const
{
  int ret = OB_SUCCESS;
  replaced_tables.reset();
  if (OB_FAIL(old_tables.get_all_tables(replaced_tables))) {
    LOG_WARN("failed to get all table from old tables", K(ret), K(old_tables));
  }
  ObITable *table = nullptr;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < replaced_tables.count(); ++idx) {
    if (OB_ISNULL(table = replaced_tables.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), K(replaced_tables));
    }
    for (int64_t pos = 0; OB_SUCC(ret) && pos < table_handles.count(); ++pos) {
      if (table->get_key() == table_handles.at(pos).get_table()->get_key()) {
        replaced_tables[idx] = const_cast<ObITable *>(table_handles.at(pos).get_table());
        break;
      }
    }
  }
  return ret;
}

int ObTabletTableStore::get_table(const ObITable::TableKey &table_key, ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is unexpected invalid", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key));
  } else {
    handle.reset();
    if (OB_FAIL(major_tables_.get_table(table_key, handle))) {
      LOG_WARN("failed to get table", K(ret), K(table_key));
    } else if (!handle.is_valid() && OB_FAIL(minor_tables_.get_table(table_key, handle))) {
      LOG_WARN("failed to get table", K(ret), K(table_key));
    } else if (!handle.is_valid() && OB_FAIL(ddl_sstables_.get_table(table_key, handle))) {
      LOG_WARN("failed to get table", K(ret), K(table_key));
    } else if (!handle.is_valid() && OB_FAIL(extend_tables_.get_table(table_key, handle))) {
      LOG_WARN("failed to get table", K(ret), K(table_key));
    } else if (!handle.is_valid() && OB_FAIL(memtables_.find(table_key, handle))) {
      LOG_WARN("failed to get table", K(ret), K(table_key));
    } else if (!handle.is_valid() && OB_FAIL(ddl_mem_sstables_.get_table(table_key, handle))) {
      LOG_WARN("failed to get table", K(ret), K(table_key));
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!handle.is_valid())) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("not found table", K(ret), K(table_key), K(handle));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::get_first_frozen_memtable(ObITable *&table)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < memtables_.count(); ++i) {
    if (OB_ISNULL(memtables_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memtable must not null", K(ret), K(memtables_));
    } else if (memtables_[i]->is_frozen_memtable()) {
      table = memtables_[i];
      break;
    }
  }
  return ret;
}

int ObTabletTableStore::get_memtables(ObIArray<storage::ObITable *> &memtables, const bool need_active) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < memtables_.count(); ++i) {
    if (OB_ISNULL(memtables_.get_table(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memtable must not null", K(ret), K(memtables_));
    } else if (!need_active && memtables_.get_table(i)->is_active_memtable()) {
      continue;
    } else if (OB_FAIL(memtables.push_back(memtables_.get_table(i)))) {
      LOG_WARN("failed to add memtables", K(ret), K(*this));
    }
  }
  return ret;
}

int ObTabletTableStore::prepare_memtables()
{
  return memtables_.prepare_allocate();
}

int ObTabletTableStore::update_memtables()
{
  int ret = OB_SUCCESS;
  ObTableHandleArray inc_memtables;
  ObTimeGuard time_guard("ObTabletTableStore::update_memtables", 5 * 1000);

  if (OB_ISNULL(tablet_ptr_) || OB_ISNULL(tablet_ptr_->get_memtable_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected tablet_ptr or memtable_mgr", K(ret), KP(tablet_ptr_));
  } else if (!tablet_ptr_->get_memtable_mgr()->has_memtable()) {
    LOG_INFO("no memtable in memtable mgr", K(ret));
  } else if (FALSE_IT(time_guard.click("has_memtable"))) {
  } else if (OB_FAIL(tablet_ptr_->get_memtable_mgr()->get_all_memtables(inc_memtables))) {
    LOG_WARN("failed to get all memtables from memtable_mgr", K(ret));
  } else if (FALSE_IT(time_guard.click("get_all_memtable"))) {
  } else if (OB_FAIL(memtables_.rebuild(inc_memtables))) {
    LOG_ERROR("failed to rebuild table store memtables", K(ret), K(inc_memtables), KPC(this));
  } else if (FALSE_IT(time_guard.click("memtables_.rebuild"))) {
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = init_read_cache())) {
      LOG_WARN("failed to rebuild read cache", K(tmp_ret));
    }
    time_guard.click("init_read_cache");
  }
  return ret;
}

int ObTabletTableStore::clear_memtables()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is unexpected invalid", K(ret), KPC(this));
  } else {
    memtables_.destroy();
    read_cache_.reset();
  }
  return ret;
}

int ObTabletTableStore::init_read_cache()
{
  int ret = OB_SUCCESS;
  read_cache_.reset();

  if (OB_UNLIKELY(is_major_sstable_empty() && minor_tables_.empty() && memtables_.empty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no table in table store, skip init read cache", K(ret), KPC(this));
  } else if (OB_FAIL(calculate_read_tables(read_cache_, INT64_MAX))) {
    if (OB_SNAPSHOT_DISCARDED != ret) {
      LOG_WARN("failed to init read_cache", K(ret));
    }
  } else if (OB_FAIL(read_cache_.set_retire_check())) {
      LOG_WARN("failed to set retire check to iterator", K(ret));
  }
  if (OB_FAIL(ret)) {
    read_cache_.reset();
  }
  return ret;
}

int ObTabletTableStore::get_read_tables(
    const int64_t snapshot_version,
    ObTableStoreIterator &iterator,
    const bool allow_no_ready_read)
{
  int ret = OB_SUCCESS;
  iterator.reset();

  if (OB_UNLIKELY(snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(snapshot_version));
  } else if (OB_UNLIKELY(is_major_sstable_empty() && minor_tables_.empty() && allow_no_ready_read)) {
    if (memtables_.empty()) {
      LOG_INFO("no table in table store, cannot read", K(ret), K(*this));
    } else if (OB_FAIL(iterator.add_tables(memtables_))) {
      LOG_WARN("failed to add tables to iterator", K(ret));
    }
  } else if (OB_UNLIKELY(!allow_no_ready_read && !is_ready_for_read_)) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("table store not ready for read", K(ret), K(allow_no_ready_read), K(PRINT_TS(*this)));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(snapshot_version), K(*this));
  } else if (check_read_cache(snapshot_version)) { // use read_cache
    if (OB_FAIL(iterator.copy(read_cache_))) {
      LOG_WARN("failed to copy read tables from cache", K(ret));
    }
  } else { // don't use cache
    if (OB_FAIL(calculate_read_tables(iterator, snapshot_version, allow_no_ready_read))) {
      LOG_WARN("failed to get read tables", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(iterator.set_retire_check())) {
      LOG_WARN("failed to set retire check to iterator", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::get_read_major_sstable(
    const int64_t &major_snapshot_version,
    ObTableStoreIterator &iterator)
{
  int ret = OB_SUCCESS;
  iterator.reset();
  if (OB_UNLIKELY(major_snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(major_snapshot_version));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i <major_tables_.count_; ++i) {
      if (OB_ISNULL(major_tables_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table", K(ret), K(i), K(major_tables_));
      } else if (major_snapshot_version < major_tables_[i]->get_snapshot_version()) {
        break;
      } else if (major_snapshot_version == major_tables_[i]->get_snapshot_version()) {
        if (OB_FAIL(iterator.add_tables(major_tables_.array_ + i))) {
          LOG_WARN("failed to add major table to iterator", K(ret));
        }
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(iterator.set_retire_check())) {
      LOG_WARN("failed to set retire check to iterator", K(ret));
    }
  }
  return ret;
}

bool ObTabletTableStore::check_read_cache(const int64_t snapshot_version)
{
  bool use_cache = false;
  if (!read_cache_.is_valid()) {
  } else if (INT64_MAX != snapshot_version) {
  } else {
    use_cache = true;
  }
  return use_cache;
}

bool ObTabletTableStore::check_read_tables(
     ObTableStoreIterator &iterator,
     const int64_t snapshot_version)
{
  bool contain_snapshot_version = false;
  if (!iterator.is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "iterator invalid, must not contain snapshot_version", K(iterator));
  } else if (OB_ISNULL(tablet_ptr_)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "Unexpected null tablet ptr, must not contain snapshot version");
  } else {
    if (iterator.get_boundary_table(false)->is_major_sstable()) {
      contain_snapshot_version = iterator.get_boundary_table(false)->get_snapshot_version() == snapshot_version;
    }
    if (!contain_snapshot_version &&
        snapshot_version >= tablet_ptr_->get_multi_version_start()) {
      contain_snapshot_version = true;
    }
  }
  if (!contain_snapshot_version) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "table store has no contain snapshot version", K(snapshot_version), KPC_(tablet_ptr), K(iterator), K(*this));
  }
  return contain_snapshot_version;
}

int ObTabletTableStore::calculate_read_tables(
    ObTableStoreIterator &iterator,
    const int64_t snapshot_version,
    const bool allow_no_ready_read)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;

  if (OB_NOT_NULL(extend_tables_[META_MAJOR])
      && extend_tables_[META_MAJOR]->get_snapshot_version() <= snapshot_version) {
    base_table = static_cast<ObSSTable *>(extend_tables_[META_MAJOR]);
    if (OB_FAIL(iterator.add_tables(extend_tables_.array_ + META_MAJOR))) {
      LOG_WARN("failed to add meta sstable to iterator", K(ret));
    }
  } else if (!is_major_sstable_empty()) {
    if (!major_tables_.empty()) {
      for (int64_t i = major_tables_.count_ - 1; OB_SUCC(ret) && i >= 0; --i) {
        if (major_tables_[i]->get_snapshot_version() <= snapshot_version) {
          base_table = static_cast<ObSSTable *>(major_tables_[i]);
          if (OB_FAIL(iterator.add_tables(major_tables_.array_ + i))) {
            LOG_WARN("failed to add major table to iterator", K(ret));
          }
          break;
        }
      }
    } else {
      ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> ddl_major_sstables;
      if (OB_FAIL(get_ddl_major_sstables(ddl_major_sstables))) {
        LOG_WARN("get ddl major sstable failed", K(ret));
      } else if (OB_UNLIKELY(ddl_major_sstables.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl major sstable is empty", K(ret));
      } else {
        ObSSTable *first_ddl_sstable = static_cast<ObSSTable *>(ddl_major_sstables.at(0));
        if (first_ddl_sstable->get_data_version() <= snapshot_version) {
          if (OB_FAIL(iterator.add_tables(ddl_major_sstables.get_data(), ddl_major_sstables.count()))) {
            LOG_WARN("add ddl major sstable failec", K(ret));
          } else {
            base_table = first_ddl_sstable;
          }
        } else {
          LOG_DEBUG("the snapshot_version of ddl major sstable is not match",
              "ddl_major_sstable_version", first_ddl_sstable->get_data_version(), K(snapshot_version));
        }
      }
    }
  } else { // no major table, not ready for reading
    LOG_INFO("no base tables in table store, no ready for reading", K(ret), K(snapshot_version), K(*this));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tablet_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null tablet", K(ret));
  } else if (OB_NOT_NULL(base_table)) { // find read minor tables
    ObITable *table = nullptr;
    int64_t inc_pos = -1;
    // TODO@wenqu: better abstract to calculate read tables in ddl path
    SCN last_scn = base_table->is_major_sstable() || base_table->is_ddl_sstable() ? SCN::max_scn() : base_table->get_end_scn();
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables_.count_; ++i) {
      table = minor_tables_[i];
      if (((base_table->is_major_sstable() || base_table->is_ddl_sstable()) && table->get_upper_trans_version() >= base_table->get_data_version())
          || table->get_end_scn() >= last_scn) {
        inc_pos = i;
        break;
      }
    }
    if (OB_SUCC(ret) && inc_pos >= 0
        && OB_FAIL(iterator.add_tables(minor_tables_.array_ + inc_pos, minor_tables_.count_ - inc_pos))) {
      LOG_WARN("failed add table to iterator", K(ret));
    } else { // try to add memtables for reading
      if (OB_FAIL(calculate_read_memtables(*tablet_ptr_, iterator))) {
        LOG_WARN("failed to calculate read memtables", K(ret), K(snapshot_version), K(iterator), KPC(this));
      }
    }
    if (OB_SUCC(ret) && !check_read_tables(iterator, snapshot_version)) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("exist base table, but no read table found for specific version", K(ret), K(snapshot_version), K(iterator), K(PRINT_TS(*this)));
    }
  } else { // not find base table
    if (!allow_no_ready_read) {
      if (is_major_sstable_empty()) {
        ret = OB_REPLICA_NOT_READABLE;
        LOG_WARN("no base table, not allow no ready read, tablet is not readable",
                 K(ret), K(snapshot_version), K(allow_no_ready_read), K(PRINT_TS(*this)));
      } else {
        ret = OB_SNAPSHOT_DISCARDED;
        LOG_WARN("no base table found for specific version",
                 K(ret), K(snapshot_version), K(allow_no_ready_read), K(PRINT_TS(*this)));
      }
    } else if (!major_tables_.empty()) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("exist major table, allow_no_ready_read can not be true",
               K(ret), K(snapshot_version), K(allow_no_ready_read), K(PRINT_TS(*this)));
    } else if (!minor_tables_.empty() && OB_FAIL(iterator.add_tables(minor_tables_.array_, minor_tables_.count_))) {
      LOG_WARN("failed to add all minor tables to iterator", K(ret));
    } else {
      if (OB_FAIL(calculate_read_memtables(*tablet_ptr_, iterator))) {
        LOG_WARN("no base table, but allow no ready read, failed to calculate read memtables",
                 K(ret), K(snapshot_version), K(memtables_), K(PRINT_TS(*this)));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::calculate_read_memtables(const ObTablet &tablet, ObTableStoreIterator &iterator)
{
  int ret = OB_SUCCESS;
  int64_t start_snapshot_version = tablet.get_snapshot_version();
  SCN start_scn = tablet.get_clog_checkpoint_scn();
  int64_t mem_pos = -1;
  ObITable *memtable = nullptr;

  if (memtables_.empty()) {
  } else if (OB_FAIL(memtables_.find(start_scn, start_snapshot_version, memtable, mem_pos))) {
    LOG_WARN("failed to find memtable", K(ret), K(*this));
  } else if (-1 == mem_pos) {
  } else if (OB_FAIL(iterator.add_tables(memtables_, mem_pos))) {
    LOG_WARN("failed to add memtable to iterator", K(ret));
  }
  return ret;
}

int ObTabletTableStore::pull_memtables()
{
  int ret = OB_SUCCESS;
  ObTableHandleArray memtable_handles;

  if (OB_ISNULL(tablet_ptr_) || OB_ISNULL(tablet_ptr_->get_memtable_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected tablet_ptr or memtable_mgr", K(ret), KP(tablet_ptr_));
  } else if (!tablet_ptr_->get_memtable_mgr()->has_memtable()) {
    LOG_TRACE("no memtable in memtable mgr", K(ret));
  } else if (OB_FAIL(tablet_ptr_->get_memtable_mgr()->get_all_memtables(memtable_handles))) {
    LOG_WARN("failed to get all memtables from memtable_mgr", K(ret));
  } else {
    int64_t start_snapshot_version = tablet_ptr_->get_snapshot_version();
    SCN clog_checkpoint_scn = tablet_ptr_->get_clog_checkpoint_scn();
    int64_t start_pos = -1;

    for (int64_t i = 0; OB_SUCC(ret) && i < memtable_handles.count(); ++i) {
      ObIMemtable *table = static_cast<ObIMemtable*>(memtable_handles.at(i).get_table());
      if (OB_ISNULL(table) || !table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must not null and must be memtable", K(ret), K(table));
      } else if (table->is_resident_memtable()) { // Single full resident memtable will be available always
        LOG_INFO("is_resident_memtable will be pulled always", K(table->get_key().tablet_id_.id()));
        start_pos = i;
        break;
      } else if (table->get_end_scn() == clog_checkpoint_scn) {
        if (table->get_snapshot_version() > start_snapshot_version) {
          start_pos = i;
          break;
        }
      } else if (table->get_end_scn() > clog_checkpoint_scn) {
        start_pos = i;
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (start_pos < 0 || start_pos >= memtable_handles.count()) {
      // all memtables need to be released
    } else if (OB_FAIL(memtables_.build(memtable_handles, start_pos))) {
      LOG_WARN("failed to build table store memtables", K(ret), K(memtable_handles), K(start_pos), K(*this));
    }
  }
  return ret;
}

int ObTabletTableStore::assign(
    ObIAllocator &allocator,
    const ObTabletTableStore &other, ObTablet *new_tablet)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletTableStore has been inited, cannot assign", K(ret), K(*this));
  } else if (OB_UNLIKELY(!other.is_valid() || NULL == (tablet_ptr_ = new_tablet))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(other), K(new_tablet));
  } else if (other.major_tables_.count_ > 0 && OB_FAIL(major_tables_.copy(allocator, other.major_tables_))) {
    LOG_WARN("failed to copy major tables", K(ret));
  } else if (other.minor_tables_.count_ > 0 && OB_FAIL(minor_tables_.copy(allocator, other.minor_tables_))) {
    LOG_WARN("failed to copy minor tables", K(ret));
  } else if (other.ddl_sstables_.count_ > 0 && OB_FAIL(ddl_sstables_.copy(allocator, other.ddl_sstables_))) {
    LOG_WARN("failed to copy ddl sstables", K(ret));
  } else if (other.ddl_mem_sstables_.count_ > 0 && OB_FAIL(ddl_mem_sstables_.copy(allocator, other.ddl_mem_sstables_))) {
    LOG_WARN("failed to copy ddl memtables", K(ret));
  } else if (OB_FAIL(extend_tables_.copy(allocator, other.extend_tables_, true/*allow_empty_table*/))) {
    LOG_WARN("failed to copy extend tables", K(ret));
  } else if (OB_FAIL(memtables_.init(&allocator, other.memtables_))) {
    LOG_WARN("failed to copy memtables", K(ret));
  } else {
    is_ready_for_read_ = other.is_ready_for_read_;
    is_inited_ = true;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = init_read_cache())) {
      LOG_WARN("failed to init read cache iterator", K(tmp_ret));
    }
    FLOG_INFO("success to assign table store", K(ret), K(PRINT_TS(*this)));
  }
  return ret;
}

int ObTabletTableStore::build_new_table_store(
    ObIAllocator &allocator,
    const ObUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  ObITable *new_table = const_cast<ObITable *>(param.table_handle_.get_table()); //table can be null

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletTableStore is not inited", K(ret));
  } else if (!major_tables_.empty() || !minor_tables_.empty()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("already exists sstable, cannot build new table store", K(ret), KPC(this));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(new_table)) {
    if (new_table->is_meta_major_sstable()) {
      ObITable *meta_major_table = old_store.extend_tables_[META_MAJOR];
      if (OB_NOT_NULL(meta_major_table) && new_table->get_end_scn() <= meta_major_table->get_end_scn()) {
        ret= OB_MINOR_SSTABLE_RANGE_CROSS;
        LOG_WARN("new meta major table is covered by old one", K(ret), KPC(new_table), KPC(meta_major_table));
      }
    } else if (new_table->is_ddl_sstable() || new_table->is_major_sstable() || new_table->is_minor_sstable()) {
      // ddl will deal in build_ddl_table later; all major/minor sstables always need rebuild
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected table type", K(ret), KPC(new_table));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t inc_base_snapshot_version = -1;
    if (OB_FAIL(build_major_tables(allocator, param, old_store, inc_base_snapshot_version))) {
      LOG_WARN("failed to build major_tables", K(ret));
    } else if (OB_FAIL(build_minor_tables(allocator, param, old_store, inc_base_snapshot_version))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("failed to build minor_tables", K(ret));
      }
    } else if (OB_FAIL(build_ddl_sstables(allocator, param, old_store))) {
      LOG_WARN("Failed to add ddl minor sstable", K(ret));
    } else if (OB_FAIL(build_meta_major_table(param.table_handle_, old_store))) {
      LOG_WARN("Failed to add meta sstable", K(ret));
    } else if (OB_FAIL(pull_memtables())) {
      LOG_WARN("failed to pull memtable from memtable_mgr", K(ret));
    } else if (OB_FAIL(pull_ddl_memtables(allocator))) {
      LOG_WARN("pull_ddl_memtables failed", K(ret));
    } else if (OB_FAIL(check_ready_for_read())) {
      LOG_WARN("failed to check ready for read", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = init_read_cache())) {
        LOG_WARN("failed to cache read iterator", K(tmp_ret));
      }
      FLOG_INFO("succeed to build new table store", K(major_tables_), K(minor_tables_), K(memtables_), K(PRINT_TS(*this)));
    }
  }
  return ret;
}

int ObTabletTableStore::build_major_tables(
    ObIAllocator &allocator,
    const ObUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store,
    int64_t &inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  inc_base_snapshot_version = -1;
  ObITable *new_table = const_cast<ObITable *>(param.table_handle_.get_table()); //table can be null
  ObTablesHandleArray tables_handle;
  if (OB_NOT_NULL(new_table) && OB_FAIL(tables_handle.add_table(new_table))) {
    LOG_WARN("failed to add table into tables handle", K(ret), K(param));
  } else if (OB_FAIL(inner_build_major_tables_(allocator, old_store, tables_handle,
      param.multi_version_start_, param.allow_duplicate_sstable_, inc_base_snapshot_version))) {
    LOG_WARN("failed to inner build major tables", K(ret), K(param), K(tables_handle));
  }
  return ret;
}

int ObTabletTableStore::inner_build_major_tables_(
    common::ObIAllocator &allocator,
    const ObTabletTableStore &old_store,
    const ObTablesHandleArray &tables_handle,
    const int64_t multi_version_start,
    const bool allow_duplicate_sstable,
    int64_t &inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  inc_base_snapshot_version = -1;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> major_tables;
  bool need_add = true;

  if (!old_store.major_tables_.empty() && OB_FAIL(old_store.major_tables_.get_all_tables(major_tables))) {
    LOG_WARN("failed to get major tables from old store", K(ret), K(old_store));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
    ObITable *new_table = tables_handle.get_table(i);
    need_add = true;
    if (OB_NOT_NULL(new_table) && new_table->is_major_sstable()) {
      for (int64_t j = 0; OB_SUCC(ret) && j < major_tables.count(); ++j) {
        ObITable *table = major_tables.at(j);
        if (!table->is_major_sstable()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table type is unexpected", K(ret), KPC(table));
        } else if (new_table->get_key() == table->get_key()) {
          if (!allow_duplicate_sstable) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected new major table which has same range with old major sstable", K(ret), KPC(new_table), KPC(table));
          } else {
            ObSSTable *new_sstable = static_cast<ObSSTable *>(new_table);
            ObSSTable *old_sstable = static_cast<ObSSTable *>(table);
            if (OB_FAIL(ObSSTableMetaChecker::check_sstable_meta(old_sstable->get_meta(), new_sstable->get_meta()))) {
              LOG_WARN("failed to check sstable meta", K(ret), KPC(new_sstable), KPC(old_sstable));
            } else {
              need_add = false;
            }
          }
        }
      }

      if (OB_SUCC(ret) && need_add && OB_FAIL(major_tables.push_back(new_table))) {
        LOG_WARN("failed to push new table into array", K(ret), KPC(new_table), K(major_tables));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTableStoreUtil::sort_major_tables(major_tables))) {
    LOG_WARN("failed to sort major tables", K(ret));
  } else {
    int64_t start_pos = 0;
    for (int64_t i = major_tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (major_tables.at(i)->get_snapshot_version() <= multi_version_start) {
        start_pos = i;
        inc_base_snapshot_version = major_tables.at(i)->get_snapshot_version();
        break;
      }
    }
    if (-1 == inc_base_snapshot_version && major_tables.count() > 0) {
      inc_base_snapshot_version = major_tables.at(0)->get_snapshot_version();
      LOG_WARN("not found inc base snapshot version, use the oldest major table", K(ret));
    }

    if (major_tables.empty()) {
      LOG_INFO("major tables is empty", K(major_tables));
    } else if (OB_FAIL(major_tables_.init_and_copy(allocator, major_tables, start_pos))) {
      LOG_WARN("failed to init major_tables", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::build_minor_tables(
    ObIAllocator &allocator,
    const ObUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store,
    const int64_t inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObITable *new_table = const_cast<ObITable *>(param.table_handle_.get_table()); //table can be null
  ObArray<ObITable *> minor_tables;

  if (NULL == new_table || !new_table->is_minor_sstable()) {
  } else if (!new_table->get_key().scn_range_.is_valid() ||
             new_table->get_key().scn_range_.is_empty()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("meet invalid or empty scn_range sstable", KPC(new_table));
  } else if (param.need_check_sstable_) { // fix issue 45431762
    const ObSSTableArray &old_minor_tables = old_store.minor_tables_;
    if (old_minor_tables.empty()) {
      // no minor tables to override new_table, skip to add new_table
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No minor tables in old store, cannot add a minor sstable", K(ret), K(param), KPC(new_table), K(old_store));
    } else if (new_table->get_end_scn() < old_minor_tables.get_boundary_table(false/*first*/)->get_start_scn()
            || new_table->get_start_scn() > old_minor_tables.get_boundary_table(true/*last*/)->get_end_scn()) {
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No minor tables covered by new minor table in old store, cannot add the new minor table",
          K(ret), K(param), KPC(new_table), K(old_store));
    }
  }

  if (OB_SUCC(ret)) {
    ObITable *table = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < old_store.minor_tables_.count_; ++i) {
      table = old_store.minor_tables_[i];
      bool need_add = true;
      if (OB_UNLIKELY(NULL == table || !table->is_minor_sstable())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must be minor sstable", K(ret), KPC(table));
      } else if (OB_NOT_NULL(new_table) && new_table->is_minor_sstable()) {
        if (new_table->get_key() == table->get_key()) {
          ObSSTable *sstable = static_cast<ObSSTable *>(table);
          ObSSTable *new_sstable = static_cast<ObSSTable *>(new_table);
          if (sstable->get_meta().get_basic_meta().max_merged_trans_version_
              <= new_sstable->get_meta().get_basic_meta().max_merged_trans_version_) {
            need_add = false;
            LOG_INFO("new table's max merge trans version is not less than the old table, "
                "add new table when table key is same", KPC(sstable), KPC(new_sstable));
          } else {
            ret = OB_NO_NEED_MERGE;
            LOG_WARN("new table with old max merged trans version, no need to merge", K(ret), KPC(sstable), KPC(new_sstable));
          }
        } else if (ObTableStoreUtil::check_include_by_scn_range(*new_table, *table)) {
          LOG_DEBUG("table purged", K(*new_table), K(*table));
          continue;
        } else if (ObTableStoreUtil::check_include_by_scn_range(*table, *new_table)) {
          ret = OB_MINOR_SSTABLE_RANGE_CROSS;
          LOG_WARN("new_table is contained by existing table", K(ret), KPC(new_table), KPC(table));
        } else if (ObTableStoreUtil::check_intersect_by_scn_range(*table, *new_table)) {
          ret = OB_MINOR_SSTABLE_RANGE_CROSS;
          LOG_WARN("new table's range is crossed with existing table", K(ret), K(*new_table), K(*table));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (need_add && OB_FAIL(minor_tables.push_back(table))) {
        LOG_WARN("failed to add table", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(new_table) && new_table->is_minor_sstable()
        && (OB_FAIL(minor_tables.push_back(new_table)))) {
      LOG_WARN("failed to add new minor table", K(ret), KPC(new_table));
    } else if (minor_tables.empty()) { // no minor tables
    } else if (minor_tables.count() == old_store.minor_tables_.count() && minor_tables.count() >= MAX_SSTABLE_CNT) {
      ret = OB_MINOR_MERGE_NOT_ALLOW;
      LOG_WARN("too many sstables, cannot add new minor sstable", K(ret), K(new_table));
    } else if (OB_FAIL(ObTableStoreUtil::sort_minor_tables(minor_tables))) {
      LOG_WARN("failed to sort minor tables", K(ret));
    } else {
      int64_t inc_pos = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
        if (minor_tables.at(i)->get_upper_trans_version() > inc_base_snapshot_version) {
          inc_pos = i;
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (inc_pos >= 0 && OB_FAIL(minor_tables_.init_and_copy(allocator, minor_tables, inc_pos))) {
        LOG_WARN("failed to init minor_tables", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::build_meta_major_table(
    const ObTableHandleV2 &new_handle,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  ObITable *new_table = const_cast<ObITable *>(new_handle.get_table());
  ObITable *old_meta_major = old_store.extend_tables_[META_MAJOR];
  ObITable *last_major = nullptr;
  extend_tables_.reset_table(META_MAJOR);

  if (OB_ISNULL(last_major = major_tables_.get_boundary_table(true))) {
    LOG_INFO("no major sstable exists, skip to try to build meta sstable", K(*this));
  } else if (OB_NOT_NULL(new_table) && new_table->is_meta_major_sstable()) { // new meta sstable must be newer than old meta
    if (new_table->get_max_merged_trans_version() <= last_major->get_snapshot_version()) {
      ret= OB_MINOR_SSTABLE_RANGE_CROSS;
      LOG_WARN("the new meta merge sstable is covered by major", K(ret), KPC(new_table), KPC(last_major));
    } else if (OB_FAIL(extend_tables_.assign(META_MAJOR, new_table))) {
      LOG_WARN("failed to add new meta merge sstable", K(ret));
    }
  } else if (OB_NOT_NULL(new_table) && new_table->is_major_sstable()) {
    // new table is major sstable, retire old meta sstable.
  } else if (OB_NOT_NULL(old_meta_major)) {
    if (old_meta_major->get_snapshot_version() <= last_major->get_snapshot_version()) { // new table is not meta sstable
      FLOG_INFO("meta sstable is covered by major sstable", KPC(last_major), KPC(old_meta_major));
    } else if (OB_FAIL(extend_tables_.assign(META_MAJOR, old_meta_major))) {
      LOG_WARN("failed to add new meta sstable", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::get_ddl_major_sstables(ObIArray<ObITable *> &ddl_major_sstables)
{
  int ret = OB_SUCCESS;
  ddl_major_sstables.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t ddl_sstable_count = ddl_sstables_.count() + ddl_mem_sstables_.count();
    if (ddl_sstable_count > 0 && OB_FAIL(ddl_major_sstables.reserve(ddl_sstable_count))) {
      LOG_WARN("reserve ddl sstable array failed", K(ret), K(ddl_sstable_count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_sstables_.count(); ++i) {
      if (OB_FAIL(ddl_major_sstables.push_back(ddl_sstables_.get_table(i)))) {
        LOG_WARN("push back ddl dump sstable failed", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_mem_sstables_.count(); ++i) {
      if (OB_FAIL(ddl_major_sstables.push_back(ddl_mem_sstables_.get_table(i)))) {
        LOG_WARN("push back old ddl sstable failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::pull_ddl_memtables(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  ObArray<ObITable *> ddl_memtables;
  ObDDLKvMgrHandle kv_mgr_handle;
  bool has_ddl_kv = false;
  ObTablesHandleArray ddl_kvs_handle;
  if (OB_ISNULL(tablet_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected tablet_ptr", K(ret), KP(tablet_ptr_));
  } else if (OB_FAIL(tablet_ptr_->get_ddl_kv_mgr(kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get ddl kv mgr failed", K(ret), KPC(tablet_ptr_));
    } else {
      LOG_TRACE("there is no ddl kv mgr in this tablet", K(ret));
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(kv_mgr_handle.get_obj()->get_ddl_kvs_for_query(*tablet_ptr_, ddl_kvs_handle))) {
    LOG_WARN("failed to get all ddl freeze kvs", K(ret));
  } else {
    SCN ddl_checkpoint_scn = tablet_ptr_->get_tablet_meta().ddl_checkpoint_scn_;
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_kvs_handle.get_count(); ++i) {
      ObDDLKV *ddl_kv = static_cast<ObDDLKV *>(ddl_kvs_handle.get_table(i));
      if (OB_ISNULL(ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get ddl kv failed", K(ret), K(i));
      } else if (ddl_kv->is_closed()) {
        // skip, because closed meanns ddl dump sstable created
      } else if (ddl_kv->get_freeze_scn() > ddl_checkpoint_scn) {
        if (OB_FAIL(ddl_kv->prepare_sstable())) {
          LOG_WARN("prepare sstable failed", K(ret));
        } else if (OB_FAIL(ddl_memtables.push_back(ddl_kv))) {
          LOG_WARN("push back ddl kv failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!ddl_memtables.empty() && OB_FAIL(ddl_mem_sstables_.init_and_copy(allocator, ddl_memtables))) {
        LOG_WARN("assign ddl memtables failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::build_ddl_sstables(
    ObIAllocator &allocator,
    const ObUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  ObITable *new_table = const_cast<ObITable *>(param.table_handle_.get_table());
  ObArray<ObITable *> ddl_dump_sstables;
  ObArray<ObITable *> ddl_mem_sstables;

  const bool is_new_table_valid_ddl_sstable = nullptr != new_table && new_table->is_ddl_dump_sstable()
    && static_cast<ObSSTable *>(new_table)->get_meta().get_basic_meta().ddl_scn_ >= tablet_ptr_->get_tablet_meta().ddl_start_scn_;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (param.ddl_info_.keep_old_ddl_sstable_) {
    if (!is_new_table_valid_ddl_sstable) { // not valid ddl sstable, simple keep old ddl sstable
      if (OB_FAIL(old_store.ddl_sstables_.get_all_tables(ddl_dump_sstables))) {
        LOG_WARN("get ddl dump sstables failed", K(ret));
      }
    } else { // new_table is valid, calculate new ddl sstables
      // since the scn is continuous in each table, so we can make it like this:
      // collect all ddl sstable and sort by:
      //   start_scn asc, means smaller start_scn first
      //   end_scn desc, means larger scn range first
      //   prefer ddl dump sstable
      // compare end_scn of this table with max end_scn, filter out the table whose end_scn is smaller
      ObArray<ObITable *> ddl_sstables;
      const int64_t ddl_sstable_count = old_store.ddl_sstables_.count() + 1;
      if (OB_FAIL(ddl_sstables.reserve(ddl_sstable_count))) {
        LOG_WARN("reserve ddl sstable array failed", K(ret), K(ddl_sstable_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < old_store.ddl_sstables_.count(); ++i) {
        if (OB_FAIL(ddl_sstables.push_back(old_store.ddl_sstables_.get_table(i)))) {
          LOG_WARN("push back old ddl sstable failed", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_sstables.push_back(new_table))) {
          LOG_WARN("push back new ddl sstable failed", K(ret), KPC(new_table));
        } else {
          std::sort(ddl_sstables.begin(), ddl_sstables.end(), [](ObITable *left, ObITable *right) {
              return left->get_start_scn() != right->get_start_scn() ?
              left->get_start_scn() < right->get_start_scn() : (left->get_end_scn() != right->get_end_scn() ?
               left->get_end_scn() > right->get_end_scn() : left->get_key().table_type_ < right->get_key().table_type_);
              });
        }
      }
      SCN max_end_scn = SCN::min_scn();
      for (int64_t i = 0; OB_SUCC(ret) && i < ddl_sstables.count(); ++i) {
        ObITable *cur_table = ddl_sstables.at(i);
        if (cur_table->get_end_scn() <= max_end_scn) {
          // drop it
          LOG_INFO("drop ddl sstable", K(i), K(max_end_scn), KPC(cur_table));
        } else if (OB_FAIL(ddl_dump_sstables.push_back(cur_table))) {
          LOG_WARN("push back ddl dump sstable failed", K(ret), K(i));
        } else {
          max_end_scn = cur_table->get_end_scn();
        }
      }
    }
  } else { // remove old ddl sstable
    if (is_new_table_valid_ddl_sstable) {
      if (OB_FAIL(ddl_dump_sstables.push_back(new_table))) {
        LOG_WARN("push back ddl dump table failed", K(ret), KPC(new_table));
      } else {
        LOG_INFO("push back ddl dump sstable success after clean up", KPC(new_table));
      }
    }
  }

  if (OB_SUCC(ret) && !ddl_dump_sstables.empty()) {
    if (OB_FAIL(ddl_sstables_.init_and_copy(allocator, ddl_dump_sstables))) {
      LOG_WARN("failed to init ddl_sstables", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::check_ready_for_read()
{
  int ret = OB_SUCCESS;
  is_ready_for_read_ = false;

  if (OB_UNLIKELY(!is_inited_ || nullptr == tablet_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("table store not init", K(ret), KPC(this), KPC(tablet_ptr_));
  } else if (is_major_sstable_empty()) {
    LOG_INFO("no valid major sstable, not ready for read", K(*this));
  } else if (OB_FAIL(check_continuous())) {
    LOG_WARN("failed to check continuous of tables", K(ret));
  } else if (minor_tables_.count() + 1 > MAX_SSTABLE_CNT_IN_STORAGE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Too Many sstables in table store", K(ret), KPC(this), KPC(tablet_ptr_));
    MTL(concurrency_control::ObMultiVersionGarbageCollector *)->report_sstable_overflow();
  } else if (get_table_count() > ObTabletTableStore::MAX_SSTABLE_CNT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Too Many sstables, cannot add another sstable any more", K(ret), KPC(this), KPC(tablet_ptr_));
    MTL(concurrency_control::ObMultiVersionGarbageCollector *)->report_sstable_overflow();
    ObPartitionMergePolicy::diagnose_table_count_unsafe(MAJOR_MERGE, *tablet_ptr_);
  } else if (minor_tables_.empty()) {
    is_ready_for_read_ = true;
  } else {
    const SCN &clog_checkpoint_scn = tablet_ptr_->get_clog_checkpoint_scn();
    const SCN &last_minor_end_scn = minor_tables_.get_boundary_table(true/*last*/)->get_end_scn();
    if (OB_UNLIKELY(clog_checkpoint_scn != last_minor_end_scn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("last minor table's end_scn must be equal to clog_checkpoint_scn",
          K(ret), K(last_minor_end_scn), K(clog_checkpoint_scn), KPC(this), KPC(tablet_ptr_));
    } else {
      is_ready_for_read_ = true;
    }
  }

  if (OB_SUCC(ret) && get_table_count() > EMERGENCY_SSTABLE_CNT) {
    int tmp_ret = OB_TOO_MANY_SSTABLE;
    LOG_WARN("Emergency SSTable count, maybe frequency freeze occurs, or maybe multi_version_start not adavanced.",
             K(tmp_ret),
             "major table count: ", major_tables_.count(),
             "minor table count: ", minor_tables_.count());
  }
  return ret;
}

int ObTabletTableStore::check_continuous() const
{
  int ret = OB_SUCCESS;
  ObITable *prev_table = nullptr;
  ObITable *table = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < major_tables_.count_; ++i) {
      if (OB_UNLIKELY(NULL == (table = major_tables_[i]) || !table->is_major_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must be major table", K(ret), K(i), KPC(table));
      } else if (OB_ISNULL(prev_table)) {
      } else if (table->get_snapshot_version() < prev_table->get_snapshot_version()) {
        // recover ddl task may create new sstable with same major version and table key.
        ret = OB_ERR_UNEXPECTED;
        LOG_INFO("table version is invalid", K(ret), K(i), KPC(table), KPC(prev_table));
      }
      prev_table = table;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_minor_tables_continue_(minor_tables_.count(), minor_tables_.array_))) {
        LOG_WARN("failed to check minor tables continue", K(ret), K(minor_tables_));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::need_remove_old_table(
    const int64_t multi_version_start,
    bool &need_remove) const
{
  int ret = OB_SUCCESS;
  need_remove = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletTableStore not init", K(ret), K(*this));
  } else if (major_tables_.empty()) {
    // do nothing
  } else if (multi_version_start <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(multi_version_start));
  } else if (minor_tables_.empty() || INT64_MAX == minor_tables_[0]->get_upper_trans_version()) {
    // do nothing
  } else if (minor_tables_[0]->get_upper_trans_version() <= major_tables_[0]->get_snapshot_version()) {
    // at least one minor sstable is coverd by major sstable
    // don't need to care about kept_multi_version_start here
    // becase major_tables_[0]::snapshot_version must <= kept_multi_version_start
    need_remove = true;
    FLOG_INFO("need recycle unused minor table", K(ret), KPC(minor_tables_[0]), KPC(major_tables_[0]));
  } else if (major_tables_.count() > 1 && major_tables_[1]->get_snapshot_version() <= multi_version_start) {
    need_remove = true;
    FLOG_INFO("need recycle oldest major sstable", K(ret), K(multi_version_start), KPC(major_tables_[1]));
  }
  return ret;
}

int64_t ObTabletTableStore::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObTabletTableStore");
    J_COLON();
    J_KV(KP(this), KP_(tablet_ptr), K_(major_tables), K_(minor_tables), K_(memtables), K_(is_ready_for_read));
    J_COMMA();
    J_ARRAY_START();
    if (major_tables_.is_valid()) {
      for (int64_t i = 0; i < major_tables_.count_; ++i) {
        ObITable *table = major_tables_[i];
        if (NULL != table && table->is_sstable()) {
          J_OBJ_START();
          J_KV(K(i), "ptr", table, "type", ObITable::get_table_type_name(table->get_key().table_type_),
              "tablet_id", table->get_key().tablet_id_,
              "scn_range", table->get_key().scn_range_,
              "ref", table->get_ref(),
              "snapshot_version", table->get_snapshot_version(),
              "max_merge_version", static_cast<ObSSTable *>(table)->get_max_merged_trans_version(),
              "table_mode_flag", static_cast<ObSSTable*>(table)->get_meta().get_basic_meta().table_mode_.mode_flag_);
          J_OBJ_END();
          J_COMMA();
        }
      }
    }
    if (minor_tables_.is_valid()) {
      for (int64_t i = 0; i < minor_tables_.count_; ++i) {
        ObITable *table = minor_tables_[i];
        if (NULL != table && table->is_sstable()) {
          J_OBJ_START();
          J_KV(K(i), "ptr", table, "type", ObITable::get_table_type_name(table->get_key().table_type_),
              "tablet_id", table->get_key().tablet_id_,
              "scn_range", table->get_key().scn_range_,
              "ref", table->get_ref(),
              "contain_uncommitted_row", static_cast<ObSSTable *>(table)->get_meta().contain_uncommitted_row() ? "yes" : "no",
              "max_merge_version", static_cast<ObSSTable *>(table)->get_max_merged_trans_version(),
              "upper_trans_version", static_cast<ObSSTable *>(table)->get_upper_trans_version(),
              "table_mode_flag", static_cast<ObSSTable*>(table)->get_meta().get_basic_meta().table_mode_.mode_flag_);
          J_OBJ_END();
          J_COMMA();
        }
      }
    }
    if (ddl_sstables_.is_valid()) {
      for (int64_t i = 0; i < ddl_sstables_.count_; ++i) {
        ObITable *table = ddl_sstables_[i];
        if (NULL != table && table->is_sstable()) {
          J_OBJ_START();
          J_KV(K(i), "ptr", table, "type", ObITable::get_table_type_name(table->get_key().table_type_),
              "tablet_id", table->get_key().tablet_id_,
              "scn_range", table->get_key().scn_range_,
              "ref", table->get_ref(),
              "max_merge_version", static_cast<ObSSTable *>(table)->get_max_merged_trans_version(),
              "table_mode_flag", static_cast<ObSSTable*>(table)->get_meta().get_basic_meta().table_mode_.mode_flag_);
          J_OBJ_END();
          J_COMMA();
        }
      }
    }
    if (ddl_mem_sstables_.is_valid()) {
      for (int64_t i = 0; i < ddl_mem_sstables_.count_; ++i) {
        ObITable *table = ddl_mem_sstables_[i];
        if (NULL != table && table->is_sstable()) {
          J_OBJ_START();
          J_KV(K(i), "ptr", table, "type", ObITable::get_table_type_name(table->get_key().table_type_),
              "tablet_id", table->get_key().tablet_id_,
              "scn_range", table->get_key().scn_range_,
              "ref", table->get_ref(),
              "max_merge_version", static_cast<ObSSTable *>(table)->get_max_merged_trans_version(),
              "table_mode_flag", static_cast<ObSSTable*>(table)->get_meta().get_basic_meta().table_mode_.mode_flag_);
          J_OBJ_END();
          J_COMMA();
        }
      }
    }
    if (extend_tables_.is_valid()) {
      for (int64_t i = 0; i < extend_tables_.count_; ++i) {
        ObITable *table = extend_tables_[i];
        if (NULL != table && table->is_sstable()) {
          J_OBJ_START();
          J_KV(K(i), "ptr", table, "type", ObITable::get_table_type_name(table->get_key().table_type_),
              "tablet_id", table->get_key().tablet_id_,
              "scn_range", table->get_key().scn_range_,
              "ref", table->get_ref(),
              "max_merge_version", static_cast<ObSSTable *>(table)->get_max_merged_trans_version(),
              "table_mode_flag", static_cast<ObSSTable*>(table)->get_meta().get_basic_meta().table_mode_.mode_flag_);
          J_OBJ_END();
          J_COMMA();
        }
      }
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

int ObTabletTableStore::get_ddl_sstable_handles(ObTablesHandleArray &ddl_sstable_handles) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store is not inited", K(ret));
  } else if (OB_FAIL(ddl_sstables_.get_all_tables(ddl_sstable_handles))) {
    LOG_WARN("failed to get ddl sstables handle", K(ret));
  }
  return ret;
}

int ObTabletTableStore::get_ha_tables(
    ObTableStoreIterator &iterator,
    bool &is_ready_for_read)
{
  int ret = OB_SUCCESS;
  iterator.reset();
  is_ready_for_read = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store is not inited", K(ret));
  } else if (FALSE_IT(is_ready_for_read = is_ready_for_read_)) {
  } else if (!major_tables_.empty() && OB_FAIL(iterator.add_tables(major_tables_.array_, major_tables_.count_))) {
    LOG_WARN("failed to add major table to iterator", K(ret));
  } else if (!minor_tables_.empty() && OB_FAIL(iterator.add_tables(minor_tables_.array_, minor_tables_.count_))) {
    LOG_WARN("failed to add minor table to iterator", K(ret));
  } else if (!ddl_sstables_.empty() && OB_FAIL(iterator.add_tables(ddl_sstables_.array_, ddl_sstables_.count_))) {
    LOG_WARN("failed to add ddl table to iterator", K(ret));
  } else if (OB_FAIL(iterator.set_retire_check())) {
    LOG_WARN("failed to set retire check to iterator", K(ret));
  } else {
    LOG_INFO("succeed get ha tables", K(major_tables_), K(minor_tables_), K(ddl_sstables_));
  }
  return ret;
}

int ObTabletTableStore::build_ha_new_table_store_(
    common::ObIAllocator &allocator,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  const ObExtendTableArray &meta_major_table = old_store.extend_tables_;
  int64_t inc_base_snapshot_version = 0;
  ObTableHandleV2 tmp_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store is not inited", K(ret));
  } else if (!param.is_valid() || !old_store.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("build ha new table store get invalid argument", K(ret), K(param), K(old_store));
  } else if (OB_FAIL(build_ha_major_tables_(allocator, param, old_store, inc_base_snapshot_version))) {
    LOG_WARN("failed to build ha major tables", K(ret), K(param), K(old_store));
  } else if (OB_FAIL(build_ha_minor_tables_(allocator, param, old_store, inc_base_snapshot_version))) {
    LOG_WARN("failed to build ha minor tables", K(ret), K(param), K(old_store));
  } else if (OB_FAIL(build_ha_ddl_tables_(allocator, param, old_store))) {
    LOG_WARN("failed to build ha ddl tables", K(ret), K(param), K(old_store));
  } else if (!meta_major_table.empty() && OB_FAIL(build_meta_major_table(tmp_handle, old_store))) {
    LOG_WARN("failed to build buf minor table", K(ret), K(old_store));
  } else if (OB_FAIL(pull_memtables())) {
    LOG_WARN("failed to pull memtable from memtable_mgr", K(ret));
  } else if (OB_FAIL(pull_ddl_memtables(allocator))) {
    LOG_WARN("pull_ddl_memtables failed", K(ret));
  } else if (OB_FAIL(check_ready_for_read())) {
    LOG_WARN("failed to check ready for read", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = init_read_cache())) {
      LOG_WARN("failed to cache read iterator", K(tmp_ret));
    }
    FLOG_INFO("succeed to build ha new table store", K(major_tables_), K(minor_tables_), K(memtables_), K(PRINT_TS(*this)));
  }

  return ret;
}

int ObTabletTableStore::build_ha_major_tables_(
    common::ObIAllocator &allocator,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store,
    int64_t &inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  inc_base_snapshot_version = -1;
  ObArray<ObITable *> major_tables;
  const bool allow_duplicate_sstable = true;
  const int64_t multi_version_start = 0;

  if (!old_store.major_tables_.empty() && OB_FAIL(old_store.major_tables_.get_all_tables(major_tables))) {
    LOG_WARN("failed to get all tables", K(ret), K(old_store));
  } else if (OB_FAIL(inner_build_major_tables_(allocator, old_store, param.tables_handle_,
      multi_version_start, allow_duplicate_sstable, inc_base_snapshot_version))) {
    LOG_WARN("failed to inner build major tables", K(ret), K(param));
  }
  return ret;
}

int ObTabletTableStore::replace_ha_minor_sstables_(
    common::ObIAllocator &allocator,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store,
    const int64_t inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> new_minor_tables;
  UNUSED(inc_base_snapshot_version);
  ObSEArray<ObITable *, MAX_SSTABLE_CNT> need_add_minor_tables;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT> old_minor_tables;
  const int64_t inc_pos = 0;
  ObTablesHandleArray tables_handle;
  ObArray<ObITable *> minor_tables;

  if (OB_FAIL(param.tables_handle_.get_all_minor_sstables(need_add_minor_tables))) {
    LOG_WARN("failed to add need add minor tables", K(ret), K(param));
  } else if (OB_FAIL(old_store.minor_tables_.get_all_tables(old_minor_tables))) {
    LOG_WARN("failed to get old minor tables", K(ret), K(old_store));
  } else if (OB_FAIL(check_old_store_minor_sstables_(old_minor_tables))) {
    LOG_WARN("failed to check old store minor sstables", K(ret), K(old_minor_tables));
  } else if (need_add_minor_tables.empty()) {
    if (old_minor_tables.empty()) {
    } else if (OB_FAIL(minor_tables_.init_and_copy(allocator, old_minor_tables, inc_pos))) {
      LOG_WARN("failed to init minor_tables", K(ret));
    }
  } else if (OB_FAIL(combin_ha_minor_sstables_(old_minor_tables, need_add_minor_tables, new_minor_tables))) {
    LOG_WARN("failed to combin ha minor sstables", K(ret), K(old_store), K(param));
  } else if (new_minor_tables.empty()) { // no minor tables
    if (tablet_ptr_->get_tablet_meta().start_scn_ != tablet_ptr_->get_tablet_meta().clog_checkpoint_scn_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet meta is not match with minor sstables", K(ret), K(new_minor_tables), K(param), K(old_store));
    } else {
      LOG_INFO("minor tables is empty, skip it", K(ret), K(new_minor_tables));
    }
  } else if (OB_FAIL(ObTableStoreUtil::sort_minor_tables(new_minor_tables))) {
    LOG_WARN("failed to sort minor tables", K(ret));
  } else if (OB_FAIL(cut_ha_sstable_scn_range_(new_minor_tables, tables_handle))) {
    LOG_WARN("failed to cut ha sstable log ts range", K(ret), K(old_store), K(param));
  } else if (OB_FAIL(tables_handle.get_tables(minor_tables))) {
    LOG_WARN("failed to get minor tables", K(ret), K(tables_handle));
  } else if (OB_FAIL(check_minor_tables_continue_(minor_tables.count(), minor_tables.get_data()))) {
    LOG_WARN("minor tables is not continue", K(ret), K(param), K(minor_tables), K(old_store));
  } else if (minor_tables.at(0)->get_start_scn() > tablet_ptr_->get_tablet_meta().start_scn_
      || minor_tables.at(minor_tables.count() - 1)->get_end_scn() != tablet_ptr_->get_tablet_meta().clog_checkpoint_scn_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta is not match with minor sstables", K(ret), K(minor_tables), K(param), K(old_store));
  } else if (OB_FAIL(minor_tables_.init_and_copy(allocator, minor_tables, inc_pos))) {
    LOG_WARN("failed to init minor_tables", K(ret));
  } else {
    LOG_INFO("succeed build ha minor sstables", K(old_store), K(minor_tables));
  }
  return ret;
}

int ObTabletTableStore::build_ha_ddl_tables_(
    common::ObIAllocator &allocator,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> ddl_tables;
  ObITable *new_table = nullptr;
  ObITable *last_ddl_table = nullptr;
  bool need_add_ddl_tables = true;

  if (!old_store.get_major_sstables().empty()) {
    need_add_ddl_tables = false;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < param.tables_handle_.get_count() && need_add_ddl_tables; ++i) {
    new_table = param.tables_handle_.get_table(i);
    if (OB_ISNULL(new_table) || !new_table->is_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new table is null or table type is unexpected", K(ret), KPC(new_table));
    } else if (new_table->is_major_sstable()) {
      need_add_ddl_tables = false;
      break;
    } else if (!new_table->is_ddl_sstable()) {
      //do nothing
    } else if (static_cast<ObSSTable *>(new_table)->get_meta().get_basic_meta().ddl_scn_ < tablet_ptr_->get_tablet_meta().ddl_start_scn_) {
      // the ddl start scn is old, drop it
    } else if (OB_NOT_NULL(last_ddl_table) && new_table->get_start_scn() != last_ddl_table->get_end_scn()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl table is not continue", K(ret), K(param), K(old_store));
    } else if (OB_FAIL(ddl_tables.push_back(new_table))) {
      LOG_WARN("failed to push new table into array", K(ret), KPC(new_table));
    } else {
      last_ddl_table = new_table;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < old_store.ddl_sstables_.count() && need_add_ddl_tables; ++i) {
    new_table = old_store.ddl_sstables_[i];
    if (OB_ISNULL(new_table) || !new_table->is_ddl_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new table is null or table type is unexpected", K(ret), KPC(new_table));
    } else if (OB_NOT_NULL(last_ddl_table) && new_table->get_start_scn() != last_ddl_table->get_end_scn()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl table is not continue", K(ret), K(param), K(old_store));
    } else if (OB_FAIL(ddl_tables.push_back(new_table))) {
      LOG_WARN("failed to push new table into array", K(ret), KPC(new_table));
    } else {
      last_ddl_table = new_table;
    }
  }

  if (OB_SUCC(ret)) {
    if (!need_add_ddl_tables) {
      LOG_INFO("has major sstable ,no need add ddl sstable", K(param), K(old_store));
    } else if (ddl_tables.empty()) { // no minor tables
      LOG_INFO("ddl tables is empty, skip it", K(ret), K(ddl_tables));
    } else if (OB_FAIL(ddl_sstables_.init_and_copy(allocator, ddl_tables))) {
      LOG_WARN("failed to init minor_tables", K(ret));
    }
  }
  return ret;
}

int ObTabletTableStore::cut_ha_sstable_scn_range_(
    common::ObIArray<ObITable *> &minor_sstables,
    ObTablesHandleArray &tables_handle)
{
  int ret = OB_SUCCESS;
  SCN last_end_scn = SCN::min_scn();
  for (int64_t i = 0; OB_SUCC(ret) && i < minor_sstables.count(); ++i) {
    ObITable *table = minor_sstables.at(i);
    ObTableHandleV2 new_table_handle;

    if (OB_ISNULL(table) || !table->is_multi_version_minor_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null or table type is unexpected", K(ret), KPC(table));
    } else if (0 == i) {
      last_end_scn = table->get_end_scn();
    } else if (last_end_scn < table->get_start_scn() || last_end_scn >= table->get_end_scn()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("minor sstable log ts is not continue or scn has overlap", K(ret), K(minor_sstables));
    } else if (last_end_scn == table->get_start_scn()) {
      last_end_scn = table->get_end_scn();
    } else {
      ObSSTable *sstable = static_cast<ObSSTable *>(table);
      if (OB_FAIL(deep_copy_sstable_(*sstable, new_table_handle))) {
        LOG_WARN("failed to deep copy sstable", KPC(sstable), K(new_table_handle));
      } else {
        table = new_table_handle.get_table();
        ObScnRange new_scn_range;
        ObScnRange original_scn_range = table->get_scn_range();
        new_scn_range.start_scn_ = last_end_scn;
        new_scn_range.end_scn_ = table->get_end_scn();

        table->set_scn_range(new_scn_range);
        last_end_scn = table->get_end_scn();
        LOG_INFO("cut ha sstable log ts range", KPC(sstable), K(new_scn_range), K(original_scn_range));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KP(table));
      } else if (OB_FAIL(tables_handle.add_table(table))) {
        LOG_WARN("failed to add table into array", K(ret), KPC(table));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::check_minor_tables_continue_(
    const int64_t count,
    ObITable **minor_sstables) const
{
  int ret = OB_SUCCESS;
  ObITable *prev_table = nullptr;
  ObITable *table = nullptr;
  prev_table = nullptr;

  if (count > 0 && OB_ISNULL(minor_sstables)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check minor tables continue minor sstables is unexpected", KP(minor_sstables), K(count));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_UNLIKELY(NULL == (table = minor_sstables[i]) || !table->is_multi_version_minor_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must be multi version minor table", K(ret), K(i), KPC(table));
    } else if (OB_ISNULL(prev_table)) {
    } else if (table->get_start_scn() > prev_table->get_end_scn()
        || table->get_end_scn() <= prev_table->get_end_scn()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table scn range not continuous or overlap", K(ret), KPC(table), KPC(prev_table));
    }
    prev_table = table;
  }
  return ret;
}

int ObTabletTableStore::combin_ha_minor_sstables_(
    common::ObIArray<ObITable *> &old_store_minor_sstables,
    common::ObIArray<ObITable *> &need_add_minor_sstables,
    common::ObIArray<ObITable *> &new_minor_sstables)
{
  int ret = OB_SUCCESS;
  SCN logical_start_scn = ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN;
  SCN logical_end_scn = ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN;
  SCN max_copy_end_scn = ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN;
  int64_t old_store_minor_tables_index = 0;

  //get remote logical minor sstable log ts
  for (int64_t i = 0; OB_SUCC(ret) && i < old_store_minor_sstables.count(); ++i) {
    ObITable *table = old_store_minor_sstables.at(i);
    old_store_minor_tables_index = i;
    if (table->is_remote_logical_minor_sstable()) {
      logical_start_scn = table->get_start_scn();
      logical_end_scn = table->get_end_scn();
      break;
    } else if (OB_FAIL(new_minor_sstables.push_back(table))) {
      LOG_WARN("failed to push minor table into array", K(ret), K(old_store_minor_sstables), KPC(table));
    } else {
      max_copy_end_scn = std::max(table->get_end_scn(), max_copy_end_scn);
    }
  }

  //push new sstable into array
  const SCN checkpoint_scn = tablet_ptr_->get_clog_checkpoint_scn();
  if (OB_SUCC(ret)) {
    if (need_add_minor_sstables.empty()) {
      //do nothing
    } else {
      bool found = false;
      int64_t param_table_index = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < need_add_minor_sstables.count() && !found; ++i) {
        ObITable *new_table = need_add_minor_sstables.at(i);
        if (OB_ISNULL(new_table) || !new_table->is_minor_sstable()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new table is null or table type is unexpected", K(ret), KPC(new_table));
        } else if (!found && new_table->get_start_scn() <= logical_start_scn
            && new_table->get_end_scn() >= logical_start_scn) {
          found = true;
          param_table_index = i;
        } else if (new_table->get_start_scn() >= checkpoint_scn) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("[MIGRATION ERROR] table's start log scn must be less than checkpoint scn",
              K(ret), KPC(this), KPC(tablet_ptr_));
        }
      }

      if (OB_SUCC(ret) && !found && ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN != logical_end_scn) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not find table continue with old table store", K(ret),
            K(old_store_minor_sstables), K(logical_start_scn), K(logical_end_scn), K(need_add_minor_sstables));
      }

      for (int64_t i = param_table_index; OB_SUCC(ret) && i < need_add_minor_sstables.count(); ++i) {
        ObITable *new_table = need_add_minor_sstables.at(i);
        if (OB_ISNULL(new_table) || !new_table->is_minor_sstable()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new table is null or table type is unexpected", K(ret), KPC(new_table));
        } else if (new_table->get_end_scn() <= max_copy_end_scn) {
          //do nothing
          //need_add_minor_sstables is copied from src.
          //Old table store table will be reused when new table end log ts is smaller than old table.
        } else if (OB_FAIL(new_minor_sstables.push_back(new_table))) {
          LOG_WARN("failed to push minor table into array", K(ret), K(new_minor_sstables), KPC(new_table));
        } else if (new_table->get_end_scn() > max_copy_end_scn) {
          max_copy_end_scn = new_table->get_end_scn();
        }
      }

      if (OB_SUCC(ret) && max_copy_end_scn < logical_end_scn) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("need add minor tables can not cover remote logical minor log ts", K(ret),
            K(need_add_minor_sstables), K(old_store_minor_sstables), K(max_copy_end_scn), K(logical_end_scn));
      }
    }
  }
  //push old table store left table into array
  if (OB_SUCC(ret)) {
    for (int64_t i  = old_store_minor_tables_index + 1; OB_SUCC(ret) && i < old_store_minor_sstables.count(); ++i) {
      ObITable *table = old_store_minor_sstables.at(i);
      if (table->get_end_scn() <= max_copy_end_scn) {
        //do nothing
      } else if (OB_FAIL(new_minor_sstables.push_back(table))) {
        LOG_WARN("failed to push table into array", K(ret), K(table));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::check_old_store_minor_sstables_(
    common::ObIArray<ObITable *> &old_store_minor_sstables)
{
  int ret = OB_SUCCESS;
  int64_t remote_logical_minor_sstable_count = 0;

  if (OB_FAIL(check_minor_tables_continue_(old_store_minor_sstables.count(), old_store_minor_sstables.get_data()))) {
    LOG_WARN("failed to check minor tables continue", K(ret), K(old_store_minor_sstables));
  }

  //check old store remote logical minor sstable count should be less than 1
  for (int64_t i = 0; OB_SUCC(ret) && i < old_store_minor_sstables.count(); ++i) {
    ObITable *table = old_store_minor_sstables.at(i);
    if (OB_ISNULL(table) || !table->is_multi_version_minor_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null or table type is unexpected", K(ret), KPC(table));
    } else if (table->is_remote_logical_minor_sstable()) {
      remote_logical_minor_sstable_count++;
    }
  }
  if (OB_SUCC(ret) && remote_logical_minor_sstable_count > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old table store remote logical minor sstable count more than 1", K(ret), K(old_store_minor_sstables));
  }
  return ret;
}

int ObTabletTableStore::get_mini_minor_sstables(ObTablesHandleArray &minor_sstables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table store is not inited", K(ret));
  } else if (tablet_ptr_->get_tablet_meta().ha_status_.is_data_status_complete()) {
    if (OB_FAIL(minor_tables_.get_all_tables(minor_sstables))) {
      LOG_WARN("failed to get all tables", K(ret), K(minor_tables_));
    }
  } else if (OB_FAIL(get_ha_mini_minor_sstables_(minor_sstables))) {
    LOG_WARN("failed to get ha mini minor sstables", K(ret), K(minor_tables_));
  }
  return ret;
}

int ObTabletTableStore::get_ha_mini_minor_sstables_(
    ObTablesHandleArray &minor_sstables) const
{
  int ret = OB_SUCCESS;
  int64_t index = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables_.count(); ++i) {
    ObITable *table = minor_tables_[i];
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table should not be NULL", K(ret), K(minor_tables_), KP(table));
    } else if (table->is_remote_logical_minor_sstable()) {
      index = i + 1;
      break;
    }
  }

  for (int64_t i = index; OB_SUCC(ret) && i < minor_tables_.count(); ++i) {
    ObITable *table = minor_tables_[i];
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table should not be NULL", K(ret), K(minor_tables_), KP(table));
    } else if (table->is_remote_logical_minor_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet table store has multi remote logical minor sstable, unexpected !!!", K(ret), K(minor_tables_));
    } else if (OB_FAIL(minor_sstables.add_table(table))) {
      LOG_WARN("failed to push table into minor sstables array", K(ret), KPC(table), K(minor_tables_));
    }
  }
  return ret;
}

int ObTabletTableStore::update_ha_minor_sstables_(
    common::ObIAllocator &allocator,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> new_minor_tables;
  const ObSSTableArray &old_minor_tables = old_store.get_minor_sstables();

  if (param.start_scn_ >= tablet_ptr_->get_clog_checkpoint_scn()) {
    //no need keep local minor sstable
    LOG_INFO("start scn is bigger than clog checkpoint ts, no need keep local minor sstable", K(old_store));
  } else {
    int64_t index = 0;
    bool has_remote_logical_sstable = false;
    for (int64_t i = 0; i < old_minor_tables.count_; ++i) {
      const ObITable *table = old_minor_tables[i];
      if (table->is_remote_logical_minor_sstable()) {
        has_remote_logical_sstable = true;
        index = i;
        break;
      }
    }

    if (has_remote_logical_sstable) {
      ObITable *table = old_minor_tables[index];
      if (!table->is_remote_logical_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table type is unexpected", K(ret), KPC(table), K(old_store), K(param));
      } else if (param.start_scn_ >= table->get_end_scn()) {
        //no need remote logical sstable
        index = index + 1;
      } else {
        ObSSTable *sstable = static_cast<ObSSTable *>(table);
        share::ObScnRange new_scn_range;
        share::ObScnRange original_scn_range = sstable->get_scn_range();
        new_scn_range.start_scn_ = param.start_scn_;
        new_scn_range.end_scn_ = table->get_end_scn();
        sstable->set_scn_range(new_scn_range);
        LOG_INFO("cut ha remote logical sstable log ts range", KPC(sstable), K(new_scn_range), K(original_scn_range));
      }
    } else {
      //local minor sstable contain param.start_scn, reuse local sstable
      //index = 0
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(old_minor_tables.get_all_tables(new_minor_tables))) {
        LOG_WARN("failed to get all minor tables", K(ret), K(old_minor_tables));
      } else if (index >= new_minor_tables.count()) {
        //reuse nothing, copy from src
      } else if (OB_FAIL(minor_tables_.init_and_copy(allocator, new_minor_tables, index))) {
        LOG_WARN("failed to init minor_tables", K(ret), K(new_minor_tables));
      }
    }
  }
  return ret;
}

int ObTabletTableStore::build_ha_minor_tables_(
    common::ObIAllocator &allocator,
    const ObBatchUpdateTableStoreParam &param,
    const ObTabletTableStore &old_store,
    const int64_t inc_base_snapshot_version)
{
  int ret = OB_SUCCESS;
  if (param.update_logical_minor_sstable_) {
    if (OB_FAIL(update_ha_minor_sstables_(allocator, param, old_store))) {
      LOG_WARN("failed to update ha minor sstables", K(ret), K(param), K(old_store));
    }
  } else {
    if (OB_FAIL(replace_ha_minor_sstables_(allocator, param, old_store, inc_base_snapshot_version))) {
      LOG_WARN("failed to replace ha minor tables", K(ret), K(param), K(old_store));
    }
  }
  return ret;
}

int ObTabletTableStore::get_recycle_version(
    const int64_t multi_version_start,
    int64_t &recycle_version) const
{
  int ret = OB_SUCCESS;
  recycle_version = 0;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> major_tables;
  if (major_tables_.empty()) {
  } else if (OB_FAIL(major_tables_.get_all_tables(major_tables))) {
    LOG_WARN("failed to get major tables from old store", K(ret), KPC(this));
  } else if (OB_FAIL(ObTableStoreUtil::sort_major_tables(major_tables))) {
    LOG_WARN("failed to sort major tables", K(ret));
  } else {
    for (int64_t i = major_tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (major_tables.at(i)->get_snapshot_version() <= multi_version_start) {
        recycle_version = major_tables.at(i)->get_snapshot_version();
        break;
      }
    }
    if (0 == recycle_version && major_tables.count() > 0) {
      recycle_version = major_tables.at(0)->get_snapshot_version();
      LOG_WARN("not found inc base snapshot version, use the oldest major table", K(ret));
    }
  }

  return ret;
}


int ObTabletTableStore::deep_copy_sstable_(
    const blocksstable::ObSSTable &old_sstable,
    ObTableHandleV2 &new_table_handle)
{
  int ret = OB_SUCCESS;
  new_table_handle.reset();
  ObArenaAllocator allocator;
  int64_t pos = 0;
  int64_t size = 0;
  char *buf = nullptr;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ObSSTable *new_sstable = nullptr;

  if (!old_sstable.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("deep copy sstable get invalid argument", K(ret), K(old_sstable));
  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("t3m should not be NULL", K(ret), KP(t3m));
  } else if (FALSE_IT(size = old_sstable.get_serialize_size())) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(size));
  } else if (OB_FAIL(old_sstable.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize sstable", K(ret), K(old_sstable), K(size));
  } else if (OB_FAIL(t3m->acquire_sstable(new_table_handle))) {
    LOG_WARN("failed to acquire sstable", K(ret));
  } else if (OB_ISNULL(new_sstable = static_cast<ObSSTable *>(new_table_handle.get_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table should not be NULL", K(ret), K(new_table_handle));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(new_sstable->deserialize(t3m->get_tenant_allocator(), buf, size, pos))) {
    LOG_WARN("failed to deserialize new sstable", K(ret), K(size), K(pos), K(old_sstable));
  } else if (OB_FAIL(new_sstable->deserialize_post_work())) {
    LOG_WARN("failed to deserialize post work", K(ret), KPC(new_sstable), K(old_sstable));
  }
  return ret;
}


ObPrintTableStore::ObPrintTableStore(const ObTabletTableStore &table_store)
  : major_tables_(table_store.major_tables_),
    minor_tables_(table_store.minor_tables_),
    memtables_(table_store.memtables_),
    ddl_mem_sstables_(table_store.ddl_mem_sstables_),
    ddl_sstables_(table_store.ddl_sstables_),
    extend_tables_(table_store.extend_tables_),
    is_ready_for_read_(table_store.is_ready_for_read_)
{
}

ObPrintTableStore::~ObPrintTableStore()
{
}

int64_t ObPrintTableStore::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObTabletTableStore_Pretty");
    J_COLON();
    J_KV(KP(this), K_(major_tables), K_(minor_tables), K_(memtables), K_(is_ready_for_read));
    J_COMMA();
    BUF_PRINTF("table_array");
    J_COLON();
    J_OBJ_START();
    if (!major_tables_.empty() || !minor_tables_.empty() || !memtables_.empty()) {
      ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
      J_NEWLINE();
      // table_type|max_merge_version
      //      |upper_trans_version|start_scn|end_scn|ref|buffer_minor
      BUF_PRINTF("[%ld] [ ", GETTID());
      BUF_PRINTO(PC(trace_id));
      BUF_PRINTF(" ] ");
      BUF_PRINTF(" %-10s %-10s %-19s %-19s %-19s %-19s %-4s %-16s \n",
          "table_arr", "table_type", "upper_trans_ver", "max_merge_ver",
          "start_scn", "end_scn", "ref", "uncommit_row");
      bool is_print = false;
      print_arr(major_tables_, "MAJOR", buf, buf_len, pos, is_print);
      print_arr(minor_tables_, "MINOR", buf, buf_len, pos, is_print);
      print_arr(ddl_sstables_, "DDL_DUMP", buf, buf_len, pos, is_print);
      print_arr(ddl_mem_sstables_, "DDL_MEM", buf, buf_len, pos, is_print);
      print_mem(memtables_, "MEM", buf, buf_len, pos, is_print);
      if (nullptr != extend_tables_[ObTabletTableStore::META_MAJOR]) {
        if (is_print) {
          J_NEWLINE();
        }
        table_to_string(extend_tables_[ObTabletTableStore::META_MAJOR], "BUF", buf, buf_len, pos);
      }
    } else {
      J_EMPTY_OBJ();
    }
    J_OBJ_END();
    J_OBJ_END();
  }
  return pos;
}

void ObPrintTableStore::print_arr(
    const ObITableArray &tables,
    const char* table_arr,
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    bool &is_print) const
{
  for (int64_t i = 0; i < tables.count_; ++i) {
    if (is_print && 0 == i) {
      J_NEWLINE();
    }
    table_to_string(tables[i], i == 0 ? table_arr : " ", buf, buf_len, pos);
    if (i < tables.count_ - 1) {
      J_NEWLINE();
    }
  }
  if (tables.count_ > 0) {
    is_print = true;
  }
}

void ObPrintTableStore::print_mem(
    const ObMemtableArray &tables,
    const char* table_arr,
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    bool &is_print) const
{
  for (int64_t i = 0; i < tables.count(); ++i) {
    if (is_print && 0 == i) {
      J_NEWLINE();
    }
    table_to_string(tables.get_table(i), i == 0 ? table_arr : " ", buf, buf_len, pos);
    if (i < tables.count() - 1) {
      J_NEWLINE();
    }
  }
  if (tables.count() > 0) {
    is_print = true;
  }
}

void ObPrintTableStore::table_to_string(
     ObITable *table,
     const char* table_arr,
     char *buf,
     const int64_t buf_len,
     int64_t &pos) const
{
  if (nullptr != table) {
    ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
    BUF_PRINTF("[%ld] [ ", GETTID());
    BUF_PRINTO(PC(trace_id));
    BUF_PRINTF(" ] ");
    const char* table_name = table->is_sstable()
      ? ObITable::get_table_type_name(table->get_key().table_type_)
      : (table->is_active_memtable() ? "ACTIVE" : "FROZEN");
    const char * uncommit_row = table->is_sstable()
      ? (static_cast<ObSSTable *>(table)->get_meta().get_basic_meta().contain_uncommitted_row_ ? "true" : "false")
      : "unused";

    BUF_PRINTF(" %-10s %-10s %-19lu %-19lu %-10s %-10s %-4ld %-16s ",
      table_arr,
      table_name,
      table->get_upper_trans_version(),
      table->get_max_merged_trans_version(),
      to_cstring(table->get_start_scn()),
      to_cstring(table->get_end_scn()),
      table->get_ref(),
      uncommit_row);
  }
}
