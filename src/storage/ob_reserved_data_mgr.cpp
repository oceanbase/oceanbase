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

#include "storage/ob_reserved_data_mgr.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_pg_sstable_mgr.h"
#include "share/ob_force_print_log.h"

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;

ObRecoveryTableData::ObRecoveryTableData() : is_inited_(false), tables_(allocator_)
{}

ObRecoveryTableData::~ObRecoveryTableData()
{
  reset();
}

void ObRecoveryTableData::reset()
{
  for (int64_t i = 0; i < tables_.count(); ++i) {
    if (NULL != tables_[i]) {
      tables_[i]->dec_ref();
    }
  }
  tables_.reset();
  is_inited_ = false;
}

int ObRecoveryTableData::init(const uint64_t tenant_id, const ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "TableData");
  allocator_.set_attr(attr);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("recovery table data has been inited already", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (tables_handle.get_count() == 0) {
    // do nothing
  } else if (tables_.init(tables_handle.get_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tables init failed.", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      ObITable* table = NULL;
      if (OB_ISNULL(table = tables_handle.get_table(i))) {
        ret = OB_ERR_SYS;
        LOG_WARN("table should not be null", K(ret), K(i));
      } else if (!table->get_key().is_valid() || !table->is_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("table key is not valid", K(ret), KPC(table));
      } else if (OB_FAIL(tables_.push_back(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("push into tables error.", K(ret), KPC(table));
      } else {
        table->inc_ref();
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    for (int64_t i = 0; i < tables_.count(); ++i) {
      if (NULL != tables_[i]) {
        tables_[i]->dec_ref();
      }
    }
    tables_.reset();
  }
  return ret;
}

int ObRecoveryTableData::get_tables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get recovery table sstables", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
      if (OB_FAIL(handle.add_table(tables_[i]))) {
        LOG_WARN("failed to add table", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObRecoveryTableData::get_tables(const ObPartitionKey& pkey, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get recovery table sstables", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get partition tables get invalid argument", K(ret), K(pkey));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
      ObITable* table = tables_[i];
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), K(pkey), KP(table));
      } else if (table->get_partition_key() == pkey) {
        if (OB_FAIL(handle.add_table(table))) {
          LOG_WARN("failed to add table into handle", K(ret), K(pkey), K(*table));
        }
      }
    }
  }
  return ret;
}

int ObRecoveryTableData::get_table(const ObITable::TableKey& table_key, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  bool found = false;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get recovery table sstables", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get sstable get invalid argument", K(ret), K(table_key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
      ObITable* table = tables_[i];
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), K(table_key), KP(table));
      } else if (table->get_key() == table_key) {
        if (OB_FAIL(handle.set_table(table))) {
          LOG_WARN("failed to set table into handle", K(ret), K(table_key));
        }
        found = true;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!found)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("backup table do not exist", K(ret), K(table_key));
      }
    }
  }
  return ret;
}

bool ObRecoveryTableData::is_valid() const
{
  return is_inited_;
}

int ObRecoveryTableData::replay(
    const uint64_t tenant_id, const common::ObIArray<ObITable::TableKey>& table_keys, ObPGSSTableMgr& sstable_mgr)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "TableData");
  allocator_.set_attr(attr);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("recovery table data has been inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (table_keys.count() == 0) {
    // do nothing
  } else if (tables_.init(table_keys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tables init failed.", K(ret), K(tenant_id));
  } else {
    ObTableHandle handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_keys.count(); ++i) {
      const ObITable::TableKey& table_key = table_keys.at(i);
      ObITable* table = NULL;
      handle.reset();
      if (OB_FAIL(sstable_mgr.acquire_sstable(table_key, handle))) {
        LOG_WARN("failed to acquire sstable", K(ret), K(table_key));
      } else if (OB_ISNULL(table = handle.get_table())) {
        ret = OB_ERR_SYS;
        LOG_WARN("table should not be null", K(ret), K(table_key));
      } else if (OB_FAIL(tables_.push_back(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("push into tables error.", K(ret), KPC(table));
      } else {
        table->inc_ref();
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
        if (OB_NOT_NULL(tables_[i])) {
          tables_[i]->dec_ref();
        }
      }
      tables_.reset();
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObRecoveryTableData::is_below_given_snapshot(const int64_t snapshot_version, bool& is_below)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("recovery table data is not inited", K(ret));
  } else {
    is_below = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
      ObSSTable* table = NULL;
      if (OB_ISNULL(tables_[i]) || OB_UNLIKELY(!tables_[i]->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret), K(i));
      } else if (FALSE_IT(table = static_cast<ObSSTable*>(tables_[i]))) {
      } else if (table->is_trans_sstable()) {
        // skip trans sstable
      } else if (table->get_meta().max_merged_trans_version_ > snapshot_version) {
        // if max_merged_trans_version is larger than snapshot_version, we have no way to
        // recover to snapshot_version
        is_below = false;
        break;
      }
    }
  }
  return ret;
}

// ObRecoveryPointData
ObRecoveryPointData::ObRecoveryPointData()
    : is_inited_(false),
      table_map_(),
      allocator_(),
      snapshot_version_(-1),
      pg_meta_(),
      partition_store_metas_(),
      table_keys_map_()
{}

ObRecoveryPointData::~ObRecoveryPointData()
{
  reset();
}

bool ObRecoveryPointData::is_valid() const
{
  return is_inited_ && snapshot_version_ != -1 && pg_meta_.is_valid();
}

void ObRecoveryPointData::free_table_data_(ObRecoveryTableData* table_data)
{
  if (NULL != table_data) {
    table_data->~ObRecoveryTableData();
    allocator_.free(table_data);
  }
}

void ObRecoveryPointData::clear_table_map_()
{
  common::hash::ObHashMap<int64_t, ObRecoveryTableData*>::iterator iter;
  for (iter = table_map_.begin(); iter != table_map_.end(); iter++) {
    if (NULL != iter->second) {
      free_table_data_(iter->second);
    }
    iter->second = NULL;
  }
  table_map_.clear();
}

void ObRecoveryPointData::clear_table_keys_map_()
{
  common::hash::ObHashMap<int64_t, TableKeyArray>::iterator iter = table_keys_map_.begin();
  for (; iter != table_keys_map_.end(); iter++) {
    iter->second.reset();
  }
  table_keys_map_.clear();
}

void ObRecoveryPointData::reset()
{
  // why reset will be successed always? (Note cxf)
  clear_table_map_();
  clear_table_keys_map_();
  pg_meta_.reset();
  partition_store_metas_.reset();
  snapshot_version_ = -1;
  is_inited_ = false;
}

int ObRecoveryPointData::init(const int64_t snapshot_version, const ObPartitionGroupMeta& pg_meta,
    const ObIArray<ObPGPartitionStoreMeta>& partition_store_metas)
{
  int ret = OB_SUCCESS;
  lib::ObLabel table_map_lable("TableMap");
  lib::ObLabel table_keys_map_lable("TableKeysMap");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("recovery point data has been inited already", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version), K(pg_meta));
  } else if (OB_FAIL(pg_meta_.deep_copy(pg_meta))) {
    LOG_WARN("failed to deep copy pg meta", K(ret), K(pg_meta));
  } else if (OB_FAIL(partition_store_metas_.assign(partition_store_metas))) {
    LOG_WARN("failed to assign partition store metas", K(ret), K(pg_meta), K(partition_store_metas));
  } else if (OB_FAIL(table_map_.create(
                 MAX_TABLE_CNT_IN_BUCKET, table_map_lable, ObModIds::OB_HASH_NODE, get_tenant_id()))) {
    LOG_WARN("recovery point data create table map failed", K(ret));
  } else if (OB_FAIL(table_keys_map_.create(
                 MAX_TABLE_CNT_IN_BUCKET, table_keys_map_lable, ObModIds::OB_HASH_NODE, get_tenant_id()))) {
    LOG_WARN("recovery point data create table keys map failed", K(ret));
  } else {
    snapshot_version_ = snapshot_version;
    is_inited_ = true;
  }
  return ret;
}

int ObRecoveryPointData::finish_replay(ObPGSSTableMgr& sstable_mgr)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  ObRecoveryTableData* new_table = NULL;
  ObMemAttr attr(get_tenant_id(), "PointData");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get backup tables", K(ret));
  } else {
    common::hash::ObHashMap<int64_t, TableKeyArray>::iterator iter;
    for (iter = table_keys_map_.begin(); OB_SUCC(ret) && iter != table_keys_map_.end(); iter++) {
      new_table = NULL;
      if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObRecoveryTableData), attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alllocate recovery table data", K(ret));
      } else if (FALSE_IT(new_table = new (ptr) ObRecoveryTableData())) {
        // do nothing
      } else if (OB_FAIL(new_table->replay(get_tenant_id(), iter->second, sstable_mgr))) {
        LOG_WARN("replay the recovery point data failed", K(ret), K(iter->first), K(iter->second));
      } else if (OB_FAIL(table_map_.set_refactored(iter->first, new_table))) {
        LOG_WARN("add recovery point data into table map failed", K(ret), K(iter->first));
      }
    }  // for (iter = table_keys_map_...)
  }
  if (OB_FAIL(ret)) {
    clear_table_map_();
  }
  return ret;
}

int ObRecoveryPointData::get_all_tables(ObTablesHandle& handle) const
{
  int ret = OB_SUCCESS;
  ObRecoveryTableData* table_data = NULL;
  ObTablesHandle tmp_handle;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get recovery tables", K(ret));
  } else if (table_map_.size() == 0) {
    // do nothing
  } else {
    common::hash::ObHashMap<int64_t, ObRecoveryTableData*>::const_iterator iter;
    for (iter = table_map_.begin(); OB_SUCC(ret) && iter != table_map_.end(); iter++) {
      tmp_handle.reset();
      table_data = iter->second;
      if (OB_ISNULL(table_data)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table data should not be null", K(ret), K(iter->first));
      } else if (OB_FAIL(table_data->get_tables(tmp_handle))) {
        LOG_WARN("get table data failed", K(ret), K(iter->first));
      } else if (OB_FAIL(handle.add_tables(tmp_handle))) {
        LOG_WARN("add tables into handle failed", K(ret));
      } else {
        // empty
      }
    }
  }
  return ret;
}

int ObRecoveryPointData::check_table_exist(const int64_t table_id, bool& exist)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else {
    ObRecoveryTableData* const* table_data = table_map_.get(table_id);
    if (NULL == table_data) {
      exist = false;
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObRecoveryPointData::add_table_(
    const int64_t table_id, const TableKeyArray& table_keys, const ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  ObRecoveryTableData* new_table = NULL;
  ObMemAttr attr(get_tenant_id(), "PointData");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(table_id)) || OB_UNLIKELY(tables_handle.get_count() != table_keys.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(table_keys.count()), K(tables_handle.get_count()));
  } else if (tables_handle.get_count() == 0) {
    LOG_WARN("no sstables add");
  } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObRecoveryTableData), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alllocate recovery table data", K(ret));
  } else if (FALSE_IT(new_table = new (ptr) ObRecoveryTableData())) {
    // do nothing
  } else if (OB_FAIL(new_table->init(get_tenant_id(), tables_handle))) {
    LOG_WARN("failed to init recovery table data", K(ret));
    // add recovery table data into table_map_, add table keys into table_keys_map_
  } else if (OB_FAIL(table_keys_map_.set_refactored(table_id, table_keys))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to add table keys to table keys map", K(ret), K(table_id));
  } else if (OB_FAIL(table_map_.set_refactored(table_id, new_table))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to add new recovery table data to table map",
        K(ret),
        K_(snapshot_version),
        K(table_id),
        KPC(new_table));
  } else {
    LOG_INFO("succeed to add a new recovery table data", K(table_id), K(table_keys), K(tables_handle));
  }
  if (OB_FAIL(ret)) {
    if (NULL != new_table) {
      free_table_data_(new_table);
    }
  }
  return ret;
}

int ObRecoveryPointData::add_table_sstables(const int64_t table_id, const ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  ObITable* table = NULL;
  TableKeyArray table_keys;
  ObMemAttr attr(get_tenant_id(), "PointData");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else {
    // prepare table keys
    allocator_.set_attr(attr);
    if (OB_FAIL(table_keys.reserve(tables_handle.get_count()))) {
      ret = OB_ERR_SYS;
      LOG_WARN("table keys init failed", K(ret), K(table_keys));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      table = NULL;
      if (OB_ISNULL(table = tables_handle.get_table(i))) {
        ret = OB_ERR_SYS;
        LOG_WARN("table should not be null", K(ret), K(i));
      } else if (!table->get_key().is_valid() || !table->is_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("table key is not valid", K(ret), KPC(table));
      } else if (OB_FAIL(table_keys.push_back(table->get_key()))) {
        LOG_WARN("failed to push back table key", K(ret), KPC(table));
      }
    }
    // add table
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_table_(table_id, table_keys, tables_handle))) {
      LOG_ERROR("failed to add a new recovery table data", K(ret), K(table_id));
    } else {
      LOG_INFO("succeed to add a new recovery table data", K(table_id));
    }
  }
  return ret;
}

int ObRecoveryPointData::get_table_sstables(const int64_t table_id, ObTablesHandle& handle) const
{
  int ret = OB_SUCCESS;
  ObRecoveryTableData* table_data = NULL;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData not inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (OB_FAIL(table_map_.get_refactored(table_id, table_data))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("failed to get recovery table data", K(ret), K(table_id));
  } else if (OB_ISNULL(table_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recovery table data not exist", K(ret));
  } else if (OB_FAIL(table_data->get_tables(handle))) {
    LOG_WARN("failed to get table sstables", K(ret), K(table_id));
  } else {
    // empty
  }
  return ret;
}

int ObRecoveryPointData::clear_table_sstables(const int64_t table_id)
{
  int ret = OB_SUCCESS;
  ObRecoveryTableData* table_data = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData not inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (OB_FAIL(table_map_.get_refactored(table_id, table_data))) {
    LOG_WARN("failed to get recovery table data", K(ret));
  } else if (OB_ISNULL(table_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recovery table data not exist", K(ret));
  } else {
    free_table_data_(table_data);
    table_map_.erase_refactored(table_id);
  }
  return ret;
}

int ObRecoveryPointData::assign(const ObRecoveryPointData& other)
{
  int ret = OB_SUCCESS;
  lib::ObLabel table_map_lable("TableMap");
  lib::ObLabel table_keys_map_lable("TableKeysMap");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("recovery point data has been inited", K(ret));
  } else if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("recovery point data to be copied is not valid", K(ret), K(other));
  } else if (OB_FAIL(pg_meta_.deep_copy(other.pg_meta_))) {
    LOG_WARN("failed to deep copy pg_meta", K(ret), K(other));
  } else if (OB_FAIL(partition_store_metas_.assign(other.partition_store_metas_))) {
    LOG_WARN("failed to copy partition store meta", K(ret), K(other));
  } else {
    snapshot_version_ = other.get_snapshot_version();
    ObTablesHandle handle;
    if (OB_FAIL(table_map_.create(MAX_TABLE_CNT_IN_BUCKET, table_map_lable, ObModIds::OB_HASH_NODE, get_tenant_id()))) {
      LOG_WARN("recovery point data create table map failed", K(ret));
    } else if (OB_FAIL(table_keys_map_.create(
                   MAX_TABLE_CNT_IN_BUCKET, table_keys_map_lable, ObModIds::OB_HASH_NODE, get_tenant_id()))) {
      LOG_WARN("recovery point data create table keys map failed", K(ret));
    } else {
      is_inited_ = true;
      if (OB_FAIL(other.get_all_tables(handle))) {
        LOG_WARN("get recovery point data tables failed", K(ret), K(other));
      } else if (OB_FAIL(add_sstables(handle))) {
        LOG_WARN("add recovery point data tables failed", K(ret), K(other));
      }
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

int ObRecoveryPointData::get_table_keys_map(common::hash::ObHashMap<int64_t, TableKeyArray>& table_keys_map) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData is not inited", K(ret));
  } else {
    common::hash::ObHashMap<int64_t, TableKeyArray>::const_iterator curr;
    for (curr = table_keys_map_.begin(); OB_SUCC(ret) && curr != table_keys_map_.end(); curr++) {
      if (OB_FAIL(table_keys_map.set_refactored(curr->first, curr->second))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("failed to add table keys to table keys map", K(ret));
      } else {
        // empty
      }
    }
  }
  return ret;
}

int ObRecoveryPointData::deserialize_assign(const ObRecoveryPointData& other)
{
  int ret = OB_SUCCESS;
  lib::ObLabel table_map_lable("TableMap");
  lib::ObLabel table_keys_map_lable("TableKeysMap");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("recovery point data has been inited", K(ret));
  } else if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("recovery point data to be copied is not valid", K(ret), K(other));
  } else if (OB_FAIL(pg_meta_.deep_copy(other.pg_meta_))) {
    LOG_WARN("failed to deep copy pg_meta", K(ret), K(other));
  } else if (OB_FAIL(partition_store_metas_.assign(other.partition_store_metas_))) {
    LOG_WARN("failed to copy partition store meta", K(ret), K(other));
  } else {
    snapshot_version_ = other.get_snapshot_version();
    if (OB_FAIL(table_map_.create(MAX_TABLE_CNT_IN_BUCKET, table_map_lable, ObModIds::OB_HASH_NODE, get_tenant_id()))) {
      LOG_WARN("recovery point data create table map failed", K(ret));
    } else if (OB_FAIL(table_keys_map_.create(
                   MAX_TABLE_CNT_IN_BUCKET, table_keys_map_lable, ObModIds::OB_HASH_NODE, get_tenant_id()))) {
      LOG_WARN("recovery point data create table keys map failed", K(ret));
    } else if (OB_FAIL(other.get_table_keys_map(table_keys_map_))) {
      LOG_WARN("get recovery point data tables failed", K(ret), K(other));
    } else {
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

int ObRecoveryPointData::add_sstables(const ObTablesHandle& tables_handle)
{
  // Note: the sstabes of one table must be put together in tables_handle
  int ret = OB_SUCCESS;
  int64_t table_id = 0;
  ObITable* table = NULL;
  ObTablesHandle handle;
  TableKeyArray table_keys;
  ObMemAttr attr(get_tenant_id(), "PointData");
  allocator_.set_attr(attr);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData is not inited", K(ret));
  } else if (tables_handle.get_count() == 0) {
    LOG_WARN("no sstables add");
  } else if (OB_FAIL(table_keys.reserve(tables_handle.get_count()))) {
    ret = OB_ERR_SYS;
    LOG_WARN("table keys init failed", K(ret), K(table_keys));
  } else if (OB_ISNULL(table = tables_handle.get_table(0))) {
    ret = OB_ERR_SYS;
    LOG_WARN("table should not be null", K(ret), KP(table));
  } else {
    table_id = get_table_id(table);
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      table = NULL;
      if (OB_ISNULL(table = tables_handle.get_table(i))) {
        ret = OB_ERR_SYS;
        LOG_WARN("table should not be null", K(ret), K(i));
      } else if (!table->get_key().is_valid() || !table->is_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("table key is not valid", K(ret), KPC(table));
      } else if (table_id == get_table_id(table)) {
        if (OB_FAIL(table_keys.push_back(table->get_key()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to push back table key", K(ret), KPC(table));
        } else if (OB_FAIL(handle.add_table(table))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to add table to handle", K(ret), KPC(table));
        }
        // add new table
      } else if (OB_FAIL(add_table_(table_id, table_keys, handle))) {
        LOG_WARN("failed to add a new recovery table data", K(ret));
      } else {
        LOG_INFO("succeed to add a new recovery table data", K(table_id), K(table_keys));
        // if reseted, the content of map is changed?
        table_keys.reuse();
        handle.reset();
        // begin a new table
        table_id = get_table_id(table);
        i--;
      }
    }  // for
    // deal with the last one
    if (OB_SUCC(ret) && handle.get_count() != 0) {
      if (OB_FAIL(add_table_(table_id, table_keys, handle))) {
        LOG_WARN("failed to add a new recovery table data", K(ret));
      } else {
        LOG_INFO("succeed to add a new recovery table data", K(table_id), K(table_keys));
        table_keys.reset();
        handle.reset();
      }
    }
  }
  return ret;
}

int ObRecoveryPointData::serialize(char* buf, const int64_t serialize_size, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t version = OB_RECOVERY_POINT_DATA_VERSION;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData is not inited", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, serialize_size, pos, version))) {
    LOG_WARN("failed to serialize version", K(ret));
  } else if (OB_FAIL(pg_meta_.serialize(buf, serialize_size, pos))) {
    LOG_WARN("failed to serialize pg meta", K(ret));
  } else if (OB_FAIL(partition_store_metas_.serialize(buf, serialize_size, pos))) {
    LOG_WARN("failed to serialize store metas", K(ret));
  } else {
    if (OB_FAIL(serialization::encode_vi64(buf, serialize_size, pos, snapshot_version_))) {
      LOG_WARN("failed to serialize snapshot version", K(ret));
    } else if (OB_FAIL(serialization::encode_vi64(buf, serialize_size, pos, table_keys_map_.size()))) {
      LOG_WARN("failed to serialize recovery point data table cnt", K(ret));
    } else {
      common::hash::ObHashMap<int64_t, TableKeyArray>::const_iterator curr;
      for (curr = table_keys_map_.begin(); OB_SUCC(ret) && curr != table_keys_map_.end(); curr++) {
        if (OB_FAIL(serialization::encode_vi64(buf, serialize_size, pos, curr->first))) {
          LOG_WARN("failed to serialize recovery point data table cnt", K(ret));
        } else if (OB_FAIL(curr->second.serialize(buf, serialize_size, pos))) {
          LOG_WARN("failed to serialize recovery point data", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRecoveryPointData::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t data_cnt = 0;
  int64_t version = 0;
  ObPartitionGroupMeta meta;
  lib::ObLabel table_map_lable("TableMap");
  lib::ObLabel table_keys_map_lable("TableKeysMap");
  ObMemAttr attr(get_tenant_id(), "PointData");
  allocator_.set_attr(attr);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("recovery point data has been inited already", K(ret));
  } else if (OB_UNLIKELY(NULL == buf || buf_len <= 0 || pos < 0 || pos >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &version))) {
    LOG_WARN("failed to decode version", K(ret));
  } else if (version != OB_RECOVERY_POINT_DATA_VERSION) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recovery point data serialization version not match", K(ret), K(version));
  } else if (OB_FAIL(meta.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to decode meta", K(ret));
  } else if (OB_FAIL(pg_meta_.deep_copy(meta))) {
    LOG_WARN("failed to set pg_meta_", K(ret));
  } else if (OB_FAIL(partition_store_metas_.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to decode partition store metas", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &snapshot_version_))) {
    LOG_WARN("failed to decode snapshot version", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &data_cnt))) {
    LOG_WARN("failed to decode data cnt", K(ret));
  } else if (OB_FAIL(table_map_.create(
                 MAX_TABLE_CNT_IN_BUCKET, table_map_lable, ObModIds::OB_HASH_NODE, get_tenant_id()))) {
    LOG_WARN("recovery point data create table map failed", K(ret));
  } else if (OB_FAIL(table_keys_map_.create(
                 MAX_TABLE_CNT_IN_BUCKET, table_keys_map_lable, ObModIds::OB_HASH_NODE, get_tenant_id()))) {
    LOG_WARN("recovery point data create table keys map failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_cnt; ++i) {
      int64_t table_id = 0;
      TableKeyArray table_keys;
      if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &table_id))) {
        LOG_WARN("failed to decode table id", K(ret));
      } else if (OB_FAIL(table_keys.deserialize(buf, buf_len, pos))) {
        LOG_WARN("failed to decode table keys", K(ret));
      } else if (OB_FAIL(table_keys_map_.set_refactored(table_id, table_keys))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("failed to add table keys to table keys map", K(ret), K(table_id));
      } else {
        // empty
      }
    }
    if (OB_FAIL(ret)) {
      clear_table_map_();
      clear_table_keys_map_();
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int64_t ObRecoveryPointData::get_serialize_size() const
{
  int64_t serialize_size = 0;
  const int64_t version = OB_RECOVERY_POINT_DATA_VERSION;
  const int64_t data_cnt = table_keys_map_.size();
  serialize_size += serialization::encoded_length_vi64(version);
  serialize_size += pg_meta_.get_serialize_size();
  serialize_size += partition_store_metas_.get_serialize_size();
  serialize_size += serialization::encoded_length_vi64(snapshot_version_);
  serialize_size += serialization::encoded_length_vi64(data_cnt);
  common::hash::ObHashMap<int64_t, TableKeyArray>::const_iterator curr;
  for (curr = table_keys_map_.begin(); curr != table_keys_map_.end(); curr++) {
    serialize_size += serialization::encoded_length_vi64(curr->first);
    serialize_size += curr->second.get_serialize_size();
  }
  return serialize_size;
}

int64_t ObRecoveryPointData::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV(K_(pg_meta), K_(snapshot_version));
    J_ARRAY_START();
    common::hash::ObHashMap<int64_t, TableKeyArray>::const_iterator curr;
    for (curr = table_keys_map_.begin(); curr != table_keys_map_.end(); curr++) {
      J_KV(K(curr->first), "TableKeyArray", curr->second);
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

int ObRecoveryPointData::is_below_given_snapshot(const int64_t snapshot_version, bool& is_below)
{
  int ret = OB_SUCCESS;
  ObRecoveryTableData* table_data = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData is not inited", K(ret));
  } else {
    is_below = true;
    common::hash::ObHashMap<int64_t, ObRecoveryTableData*>::const_iterator iter;
    for (iter = table_map_.begin(); OB_SUCC(ret) && iter != table_map_.end(); iter++) {
      table_data = iter->second;
      if (OB_ISNULL(table_data)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table data should not be null", K(ret), K(iter->first));
      } else if (OB_FAIL(table_data->is_below_given_snapshot(snapshot_version, is_below))) {
        LOG_WARN("check below snapshot version failed", K(ret), K(iter->first));
      } else if (!is_below) {
        break;
      }
    }
  }
  return ret;
}

int ObRecoveryPointData::get_partition_store_meta(
    const ObPartitionKey& pkey, ObPGPartitionStoreMeta& partition_store_meta)
{
  int ret = OB_SUCCESS;
  bool found = false;
  partition_store_meta.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData is not inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get partition store meta get invalid argument", K(ret), K(pkey));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_store_metas_.count(); ++i) {
      const ObPGPartitionStoreMeta& tmp_partition_store_meta = partition_store_metas_.at(i);
      if (pkey == tmp_partition_store_meta.pkey_) {
        if (OB_FAIL(partition_store_meta.assign(tmp_partition_store_meta))) {
          LOG_WARN("failed to assign partition store meta", K(ret), K(pkey), K(tmp_partition_store_meta));
        }
        found = true;
        break;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not find partition", K(ret), K(pkey), K(*this));
    }
  }
  return ret;
}

int ObRecoveryPointData::get_sstable(const ObITable::TableKey& table_key, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  ObRecoveryTableData* table_data = NULL;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData is not inited", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get sstable get invalid argument", K(ret), K(table_key));
  } else if (table_map_.size() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("sstable not exist.", K(ret), K(table_key));
  } else {
    int64_t table_id = table_key.table_id_;
    if (OB_FAIL(table_map_.get_refactored(table_id, table_data))) {
      LOG_WARN("failed to get recovery table data", K(ret));
    } else if (OB_ISNULL(table_data)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("recovery table data not exist", K(ret));
    } else if (OB_FAIL(table_data->get_table(table_key, handle))) {
      LOG_WARN("failed to get table sstables", K(ret), K(table_key));
    } else {
      // empty
    }
  }
  return ret;
}

int ObRecoveryPointData::get_partition_tables(const ObPartitionKey& pkey, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObITable* table = NULL;
  ObTablesHandle tmp_handle;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryPointData is not inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get partition tables get invalid argument", K(ret), K(pkey));
  } else if (table_map_.size() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("sstable not exist.", K(ret), K(pkey));
  } else if (OB_FAIL(get_all_tables(tmp_handle))) {
    LOG_WARN("get all recovery point tables failed.", K(ret), K(pkey));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_handle.get_count(); ++i) {
      table = NULL;
      if (OB_ISNULL(table = tmp_handle.get_table(i))) {
        ret = OB_ERR_SYS;
        LOG_WARN("table should not be null", K(ret), K(i));
      } else if (!table->get_key().is_valid() || !table->is_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("table key is not valid", K(ret), KPC(table));
      } else if (table->get_partition_key() == pkey) {
        if (OB_FAIL(handle.add_table(table))) {
          LOG_WARN("failed to add table into handle", K(ret), K(pkey), K(*table));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (handle.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition do not has tables", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

// ObRecoveryData
ObRecoveryData::ObRecoveryData() : pg_key_(), recover_point_list_(), allocator_(), is_inited_(false)
{}

ObRecoveryData::~ObRecoveryData()
{
  destroy();
}

void ObRecoveryData::destroy()
{
  if (is_inited_) {
    DLIST_REMOVE_ALL_NORET(curr, recover_point_list_)
    {
      free_recovery_point(curr);
    }
    pg_key_.reset();
    is_inited_ = false;
  }
}

int ObRecoveryData::init(const ObPartitionKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRecoveryData is inited twice", K(ret), K(pg_key));
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key));
  } else {
    pg_key_ = pg_key;
    is_inited_ = true;
  }
  return ret;
}

int ObRecoveryData::create_recovery_point(const int64_t snapshot_version, const ObPartitionGroupMeta& pg_meta,
    const ObIArray<ObPGPartitionStoreMeta>& partition_store_metas, const ObTablesHandle& tables_handle,
    ObRecoveryPointData*& new_data)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObMemAttr attr(pg_key_.get_tenant_id(), "PointData");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version), K(pg_meta));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObRecoveryPointData), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alllocate back up meta data", K(ret), K(pg_key_));
  } else {
    new_data = new (buf) ObRecoveryPointData();
    if (OB_FAIL(new_data->init(snapshot_version, pg_meta, partition_store_metas))) {
      LOG_WARN("failed to init recovery point data", K(ret), K_(pg_key));
    } else if (OB_FAIL(new_data->add_sstables(tables_handle))) {
      LOG_WARN("failed to add sstables to point data", K(ret), K_(pg_key));
    } else {
      LOG_INFO("succeed to create recovery point data", KPC(new_data));
    }
  }
  if (OB_FAIL(ret) && NULL != new_data) {
    free_recovery_point(new_data);
    new_data = NULL;
  }
  return ret;
}

void ObRecoveryData::free_recovery_point(ObRecoveryPointData* new_data)
{
  if (NULL != new_data) {
    new_data->~ObRecoveryPointData();
    allocator_.free(new_data);
  }
}

int ObRecoveryData::insert_recovery_point(ObRecoveryPointData* new_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_ISNULL(new_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("recovery point is not valid", K(ret), KPC(new_data));
  } else if (OB_UNLIKELY(!new_data->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert data is not valid", K(ret), KPC(new_data));
  } else if (OB_UNLIKELY(!recover_point_list_.add_last(new_data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to add new recovery point data to list", K(ret), KPC(new_data));
  } else {
    LOG_INFO("succeed to add a new recovery point data", KPC(new_data));
  }
  return ret;
}

int ObRecoveryData::get_recovery_point(const int64_t snapshot_version, ObRecoveryPointData*& point_data)
{
  int ret = OB_SUCCESS;
  point_data = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else {
    DLIST_FOREACH(curr, recover_point_list_)
    {
      if (snapshot_version == curr->get_snapshot_version()) {
        point_data = curr;
        break;
      }
    }
  }
  return ret;
}

int ObRecoveryData::get_recovery_point_below_given_snapshot(
    const int64_t snapshot_version, ObRecoveryPointData*& point_data)
{
  int ret = OB_SUCCESS;
  bool is_below = false;
  point_data = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else {
    DLIST_FOREACH_BACKWARD_X(curr, recover_point_list_, OB_SUCC(ret))
    {
      if (OB_FAIL(curr->is_below_given_snapshot(snapshot_version, is_below))) {
        LOG_WARN("failed to check meta data is below give snapshot", K(ret), KPC(curr), K(snapshot_version));
      } else if (is_below) {
        point_data = curr;
        break;
      }
    }
  }
  return ret;
}

int ObRecoveryData::get_unneed_recovery_points(
    const ObIArray<int64_t>& versions_needs, const int64_t snapshot_gc_ts, ObIArray<ObRecoveryPointData*>& points_data)
{
  int ret = OB_SUCCESS;
  bool need_left = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else {
    DLIST_FOREACH(curr, recover_point_list_)
    {
      need_left = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < versions_needs.count(); ++i) {
        if (OB_UNLIKELY(versions_needs.at(i) <= 0)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(i), K(versions_needs));
        } else if (versions_needs.at(i) == curr->get_snapshot_version()) {
          need_left = true;
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (!need_left && curr->get_snapshot_version() <= snapshot_gc_ts) {
          if (OB_FAIL(points_data.push_back(curr))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("failed to push back into array", K(ret));
          } else {
            LOG_INFO("get unneeded recovery point", K(ret), KPC(curr));
          }
        }
      }
    }
  }
  return ret;
}

int ObRecoveryData::get_unneed_recovery_points(
    const int64_t smaller_snapshot, const int64_t larger_snapshot, ObIArray<ObRecoveryPointData*>& points_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else {
    ObRecoveryPointData* node_for_larger_snapshot = NULL;
    ObRecoveryPointData* node_for_smaller_snapshot = NULL;
    DLIST_FOREACH(curr, recover_point_list_)
    {
      bool need_recycle = false;
      bool is_below = false;
      if (NULL == node_for_larger_snapshot) {
        if (OB_FAIL(curr->is_below_given_snapshot(larger_snapshot, is_below))) {
          LOG_WARN("failed to check if point data is below give snapshot", K(ret), KPC(curr), K(larger_snapshot));
        } else if (is_below) {
          node_for_larger_snapshot = curr;
        }
      } else if (larger_snapshot > smaller_snapshot && NULL == node_for_smaller_snapshot) {
        if (OB_FAIL(curr->is_below_given_snapshot(smaller_snapshot, is_below))) {
          LOG_WARN("failed to check if point data is below give snapshot", K(ret), KPC(curr), K(smaller_snapshot));
        } else if (is_below) {
          node_for_smaller_snapshot = curr;
        } else {
          need_recycle = true;
        }
      } else {
        need_recycle = true;
      }
      if (OB_SUCC(ret) && need_recycle) {
        if (OB_FAIL(points_data.push_back(curr))) {
          LOG_WARN("failed to push back node to points data", K(ret), KPC(curr));
        }
      }
    }
  }
  return ret;
}

int ObRecoveryData::replay_remove_recovery_point(ObRecoveryPointData* del_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_ISNULL(del_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(del_data));
  } else if (OB_FAIL(remove_recovery_point(del_data))) {
    LOG_ERROR("failed to remove point data.", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObRecoveryData::remove_recovery_point(ObRecoveryPointData* del_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_ISNULL(del_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the del_data should not be null", K(ret), KPC(del_data));
  } else {
    if (OB_ISNULL(recover_point_list_.remove(del_data))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to remove point data from list", K(ret));
    } else {
      free_recovery_point(del_data);
    }
  }
  return ret;
}

int ObRecoveryData::remove_recovery_points(const ObIArray<ObRecoveryPointData*>& point_list)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* point_data = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < point_list.count(); ++i) {
      point_data = point_list.at(i);
      LOG_INFO("remove recovery point data", KPC(point_data));
      if (OB_ISNULL(point_data)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the point data to be removed should not be null", K(ret), KPC(point_data));
      } else if (OB_ISNULL(recover_point_list_.remove(point_data))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("failed to remove recovery point data from list", K(ret));
      } else {
        free_recovery_point(point_data);
      }
    }
  }
  return ret;
}

int ObRecoveryData::replay_add_recovery_point(const ObRecoveryPointData& point_data)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* new_data = NULL;
  void* buf = NULL;
  ObMemAttr attr(pg_key_.get_tenant_id(), "PointData");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("recovery data is not inited", K(ret));
  } else if (OB_UNLIKELY(!point_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("recovery point data is not valid", K(ret), K(point_data));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObRecoveryPointData), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alllocate recovery point data", K(ret), K(pg_key_));
  } else {
    new_data = new (buf) ObRecoveryPointData();
    if (OB_FAIL(new_data->deserialize_assign(point_data))) {
      LOG_WARN("failed to assign new recovery point data", K(ret), K(point_data));
    } else if (OB_UNLIKELY(!recover_point_list_.add_last(new_data))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to add new recovery point data to list", K(ret), KPC(new_data));
    }
  }
  if (OB_FAIL(ret) && NULL != new_data) {
    free_recovery_point(new_data);
  }
  return ret;
}

int ObRecoveryData::get_recovery_point_data(const int64_t snapshot_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else {
    bool found = false;
    DLIST_FOREACH_BACKWARD_X(curr, recover_point_list_, OB_SUCC(ret))
    {
      if (snapshot_version == curr->get_snapshot_version()) {
        found = true;
        if (OB_FAIL(curr->get_all_tables(handle))) {
          LOG_WARN("failed to get recovery point tables", K(ret), KPC(curr), K(snapshot_version));
        }
        break;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not find corresponding recovery point data", K(ret), K(snapshot_version), KPC(this));
    }
  }
  return ret;
}

int ObRecoveryData::get_recovery_point_table_data(
    const int64_t table_id, const int64_t snapshot_version, ObTablesHandle& handle) const
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version), K(table_id));
  } else {
    bool found = false;
    DLIST_FOREACH_BACKWARD_X(curr, recover_point_list_, OB_SUCC(ret))
    {
      if (snapshot_version == curr->get_snapshot_version()) {
        found = true;
        if (OB_FAIL(curr->get_table_sstables(table_id, handle))) {
          LOG_WARN("failed to get recovery point table sstables", K(ret), K(table_id), KPC(curr), K(snapshot_version));
        }
        break;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not find corresponding recovery point data", K(ret), K(snapshot_version), KPC(this));
    }
  }
  return ret;
}

int ObRecoveryData::serialize(ObIAllocator& allocator, char*& buf, int64_t& serialize_size)
{
  int ret = OB_SUCCESS;
  char* tmp_buf = NULL;
  int64_t pos = 0;
  ObMemAttr attr(pg_key_.get_tenant_id(), "RecoveryData");
  int64_t version = OB_RECOVERY_DATA_VERSION;
  serialize_size = get_serialize_size();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_ISNULL(tmp_buf = static_cast<char*>(allocator.alloc(serialize_size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate serialize buffer", K(ret), K(serialize_size), K_(pg_key));
  } else if (OB_FAIL(serialization::encode_vi64(tmp_buf, serialize_size, pos, version))) {
    LOG_WARN("failed to serialize version", K(ret), K_(pg_key));
  } else if (OB_FAIL(serialization::encode_vi64(tmp_buf, serialize_size, pos, recover_point_list_.get_size()))) {
    LOG_WARN("failed to serialize meta data cnt", K(ret), K_(pg_key));
  } else {
    DLIST_FOREACH(curr, recover_point_list_)
    {
      if (OB_FAIL(curr->serialize(tmp_buf, serialize_size, pos))) {
        LOG_WARN("failed to serialize backup meta data", K(ret), K_(pg_key), KPC(curr));
      }
    }
    if (OB_SUCC(ret)) {
      buf = tmp_buf;
    }
  }
  return ret;
}

int ObRecoveryData::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(pg_key_.get_tenant_id(), "PointData");
  int64_t data_cnt = 0;
  int64_t version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == buf || buf_len <= 0 || pos < 0 || pos >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &version))) {
    LOG_WARN("failed to decode version", K(ret), K_(pg_key));
  } else if (version != OB_RECOVERY_DATA_VERSION) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recovery data serialization version not match", K(ret), K(version), K_(pg_key));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &data_cnt))) {
    LOG_WARN("failed to decode data cnt", K(ret), K_(pg_key));
  } else {
    void* point_data_buf = NULL;
    ObRecoveryPointData* point_data = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < data_cnt; ++i) {
      point_data_buf = NULL;
      point_data = NULL;
      if (OB_ISNULL(point_data_buf = allocator_.alloc(sizeof(ObRecoveryPointData), attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate meta data", K(ret));
      } else {
        point_data = new (point_data_buf) ObRecoveryPointData();
        if (OB_FAIL(point_data->deserialize(buf, buf_len, pos))) {
          LOG_WARN("failed to deserialize recovery point data", K(ret), K(i));
        } else if (!recover_point_list_.add_last(point_data)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to add point data to list", K(ret), KPC(point_data));
        }
        if (OB_FAIL(ret)) {
          free_recovery_point(point_data);
        }
      }
    }
    if (OB_FAIL(ret)) {
      DLIST_REMOVE_ALL_NORET(curr, recover_point_list_)
      {
        free_recovery_point(curr);
      }
    }
  }
  return ret;
}

int ObRecoveryData::check_recovery_point_exist(const int64_t snapshot_version, bool& is_exist) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else {
    // TODO: map
    DLIST_FOREACH_BACKWARD_X(curr, recover_point_list_, OB_SUCC(ret))
    {
      if (snapshot_version == curr->get_snapshot_version()) {
        found = true;
        break;
      }
    }
    is_exist = found;
  }
  return ret;
}

int ObRecoveryData::check_recovery_point_exist(const int64_t table_id, const int64_t snapshot_version, bool& is_exist)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version), K(table_id));
  } else {
    DLIST_FOREACH_BACKWARD_X(curr, recover_point_list_, OB_SUCC(ret))
    {
      if (snapshot_version == curr->get_snapshot_version()) {
        found = true;
        if (OB_FAIL(curr->check_table_exist(table_id, is_exist))) {
          LOG_WARN("check table exist failed", K(ret), K(table_id), K(snapshot_version));
        }
        break;
      }
    }
  }
  if (!found) {
    is_exist = found;
  }
  return ret;
}

int ObRecoveryData::update_recovery_point(
    const int64_t table_id, const int64_t snapshot_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* point_data = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version), K(table_id));
  } else {
    DLIST_FOREACH_BACKWARD_X(curr, recover_point_list_, OB_SUCC(ret))
    {
      if (snapshot_version == curr->get_snapshot_version()) {
        point_data = curr;
        break;
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(point_data)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not find corresponding recovery point data", K(ret), K(snapshot_version), KPC(this));
    } else {
      if (OB_FAIL(point_data->clear_table_sstables(table_id))) {
        LOG_WARN("failed to clear recovery point table data", K(ret), KPC(point_data));
      } else if (OB_FAIL(point_data->add_table_sstables(table_id, handle))) {
        LOG_WARN("failed to add recovery point table data", K(ret), KPC(point_data));
      }
    }
  }
  return ret;
}

int ObRecoveryData::finish_replay(ObPGSSTableMgr& pg_sstable_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryData is not inited", K(ret));
  } else {
    DLIST_FOREACH(curr, recover_point_list_)
    {
      if (OB_FAIL(curr->finish_replay(pg_sstable_mgr))) {
        LOG_WARN("failed to finish replay", K(ret), K_(pg_key), KPC(curr));
      }
    }
    LOG_INFO("recovery data finish replay", K(ret), KPC(this));
  }
  return ret;
}

int64_t ObRecoveryData::get_serialize_size()
{
  int64_t serialize_size = 0;
  const int64_t version = OB_RECOVERY_DATA_VERSION;
  const int64_t data_cnt = recover_point_list_.get_size();
  serialize_size += serialization::encoded_length_vi64(version);
  serialize_size += serialization::encoded_length_vi64(data_cnt);
  DLIST_FOREACH_NORET(curr, recover_point_list_)
  {
    serialize_size += curr->get_serialize_size();
  }
  return serialize_size;
}

int64_t ObRecoveryData::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t i = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV(K_(pg_key), "recovery_point_data count", recover_point_list_.get_size());
    J_ARRAY_START();
    DLIST_FOREACH_NORET(curr, recover_point_list_)
    {
      J_KV(K(i), "RecoveryPointData", *curr);
      ++i;
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

ObRecoveryDataMgr::ObRecoveryDataMgr()
    : enable_write_slog_(false),
      log_seq_num_(0),
      pg_key_(),
      is_inited_(false),
      restore_point_data_(),
      backup_point_data_(),
      restore_point_start_version_(0)
{}

ObRecoveryDataMgr::~ObRecoveryDataMgr()
{
  destroy();
}

void ObRecoveryDataMgr::destroy()
{
  if (is_inited_) {
    pg_key_.reset();
    file_handle_.reset();
    enable_write_slog_ = false;
    log_seq_num_ = 0;
    is_inited_ = false;
  }
  restore_point_start_version_ = 0;
}

int ObRecoveryDataMgr::init(const ObPartitionKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRecoveryDataMgr is inited twice", K(ret), K(pg_key));
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(restore_point_data_.init(pg_key))) {
    LOG_WARN("restore point data inited failed", K(ret), K(pg_key));
  } else if (OB_FAIL(backup_point_data_.init(pg_key))) {
    LOG_WARN("backup meta data inited failed", K(ret), K(pg_key));
  } else {
    pg_key_ = pg_key;
    is_inited_ = true;
  }
  return ret;
}

int ObRecoveryDataMgr::add_recovery_point_(const ObRecoveryPointType point_type, const int64_t snapshot_version,
    const ObPartitionGroupMeta& pg_meta, const ObIArray<ObPGPartitionStoreMeta>& partition_store_metas,
    const ObTablesHandle& tables_handle, ObRecoveryData& recovery_data)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* new_data = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg meta is not valid", K(ret), K(snapshot_version), K(pg_meta));
  } else if (OB_FAIL(recovery_data.create_recovery_point(
                 snapshot_version, pg_meta, partition_store_metas, tables_handle, new_data))) {
    LOG_WARN("failed to alllocate recovery point data", K(ret));
  } else if (OB_FAIL(write_add_data_slog_(point_type, *new_data))) {
    LOG_WARN("failed to write add recovery point data slog", K(ret));
  } else if (OB_FAIL(recovery_data.insert_recovery_point(new_data))) {
    LOG_WARN("failed to add recovery point data", K(ret));
    ob_abort();
  } else {
    LOG_INFO("succeed to add a new recovery point data", KPC(new_data));
  }
  if (OB_FAIL(ret) && NULL != new_data) {
    recovery_data.free_recovery_point(new_data);
  }
  return ret;
}

int ObRecoveryDataMgr::write_add_data_slog_(const ObRecoveryPointType point_type, ObRecoveryPointData& point_data)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageFile* file = NULL;
  if (!enable_write_slog_) {
    ret = OB_ERR_SYS;
    LOG_WARN("can not write slog during replay", K(ret));
  } else if (OB_UNLIKELY(!point_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(point_data));
  } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("storage file should not be null", K(ret));
  } else if (OB_FAIL(SLOGGER.begin(OB_LOG_ADD_RECOVERY_POINT_DATA))) {
    LOG_WARN("failed to begin slog trans", K(ret));
  } else {
    ObStorageLogAttribute log_attr(pg_key_.get_tenant_id(), file->get_file_id());
    ObAddRecoveryPointDataLogEntry log_entry(point_type, point_data);
    int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_ADD_RECOVERY_POINT_DATA);
    if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, log_entry))) {
      LOG_WARN("failed to write add backup meta data slog", K(ret), K(point_data));
    }
    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        LOG_WARN("failed to abort slog trans", K(tmp_ret));
      }
    } else if (OB_FAIL(SLOGGER.commit(log_seq_num_))) {
      LOG_WARN("failed to commit slog trans", K(ret), K(point_data));
    }
  }
  return ret;
}

int ObRecoveryDataMgr::write_remove_data_slogs_(
    const ObRecoveryPointType point_type, ObIArray<ObRecoveryPointData*>& points_data)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageFile* file = NULL;
  ObRecoveryPointData* point_data = NULL;
  if (!enable_write_slog_) {
    ret = OB_ERR_SYS;
    LOG_WARN("can not write slog during restart", K(ret));
  } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("storage file should not be null", K(ret));
  } else if (OB_FAIL(SLOGGER.begin(OB_LOG_REMOVE_RECOVERY_POINT_DATA))) {
    LOG_WARN("failed to begin slog trans", K(ret), K_(pg_key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < points_data.count(); ++i) {
      point_data = points_data.at(i);
      ObStorageLogAttribute log_attr(pg_key_.get_tenant_id(), file->get_file_id());
      const int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_REMOVE_RECOVERY_POINT_DATA);
      if (OB_UNLIKELY(!point_data->is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("recovery point data is not valid", K(ret), KPC(point_data));
      } else {
        ObRemoveRecoveryPointDataLogEntry log_entry(point_type, *point_data);
        if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, log_entry))) {
          LOG_WARN("failed to write slog", K(ret), K(log_entry));
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        LOG_WARN("failed to abort slog trans", K(ret), K_(pg_key));
      }
    } else if (OB_FAIL(SLOGGER.commit(log_seq_num_))) {
      LOG_WARN("failed to commit slog trans", K(ret), K_(pg_key));
    }
  }
  return ret;
}

int ObRecoveryDataMgr::serialize(common::ObIAllocator& allocator, char*& buf, int64_t& serialize_size)
{
  int ret = OB_SUCCESS;
  buf = nullptr;
  serialize_size = 0;
  int64_t copy_size = 0;
  int64_t pos = 0;
  SerializePair restore_point_pair;
  SerializePair backup_meta_pair;
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_FAIL(restore_point_data_.serialize(allocator, restore_point_pair.buf_, restore_point_pair.size_))) {
    LOG_WARN("failed to serialize restore point data", K(ret), K_(pg_key));
  } else if (OB_FAIL(backup_point_data_.serialize(allocator, backup_meta_pair.buf_, backup_meta_pair.size_))) {
    LOG_WARN("failed to serialize restore point data", K(ret), K_(pg_key));
  } else {
    int64_t magic_size = serialization::encoded_length_vi64(MAGIC_NUM);
    int64_t seq_num_size = serialization::encoded_length_vi64(log_seq_num_);
    serialize_size += magic_size;
    serialize_size += seq_num_size;
    serialize_size += restore_point_pair.size_;
    serialize_size += backup_meta_pair.size_;
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(serialize_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate serialize buf", K(ret), K_(pg_key));
    } else if (OB_FAIL(serialization::encode_vi64(buf, serialize_size, pos, MAGIC_NUM))) {
      LOG_WARN("failed to serialize version", K(ret), K_(pg_key));
    } else if (OB_FAIL(serialization::encode_vi64(buf, serialize_size, pos, log_seq_num_))) {
      LOG_WARN("failed to serialize log seq num", K(ret), K_(pg_key));
    } else {
      copy_size = pos;
      MEMCPY(buf + copy_size, restore_point_pair.buf_, restore_point_pair.size_);
      copy_size += restore_point_pair.size_;
      MEMCPY(buf + copy_size, backup_meta_pair.buf_, backup_meta_pair.size_);
      copy_size += backup_meta_pair.size_;
    }
    if (OB_SUCC(ret)) {
      if (copy_size != serialize_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy_size should be equal to serialize_size", K(ret), K(copy_size), K(serialize_size));
      } else {
        LOG_INFO("succeed to serialize recovery data mgr", K(ret), K_(pg_key));
      }
    }
  }
  return ret;
}

int ObRecoveryDataMgr::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t magic_num = 0;
  TCWLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == buf || buf_len <= 0 || pos < 0 || pos >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf), K_(pg_key), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &magic_num))) {
    LOG_WARN("failed to decode magic num", K(ret), K_(pg_key));
  } else if (magic_num != MAGIC_NUM && magic_num != OLD_MAGIC_NUM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recovery data mgr magic num not match", K(ret), K_(pg_key), K(magic_num), LITERAL_K(MAGIC_NUM));
  } else if (OB_FAIL(MAGIC_NUM == magic_num && serialization::decode_vi64(buf, buf_len, pos, &log_seq_num_))) {
    LOG_WARN("failed to decode log seq num", K(ret), K_(pg_key));
  } else if (OB_FAIL(restore_point_data_.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to deserialize recovery data", K(ret), K_(pg_key));
  } else if (OB_FAIL(backup_point_data_.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to deserialize recovery data", K(ret), K_(pg_key));
  }
  return ret;
}

int ObRecoveryDataMgr::replay_add_recovery_point(
    const ObRecoveryPointType point_type, const ObRecoveryPointData& point_data)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!point_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(point_type), K(point_data));
  } else if (point_type == ObRecoveryPointType::RESTORE_POINT) {
    if (OB_FAIL(replay_add_restore_point_(point_data))) {
      LOG_WARN("replay add restore point failed", K(ret), K(point_data));
    }
  } else if (point_type == ObRecoveryPointType::BACKUP) {
    if (OB_FAIL(replay_add_backup_point_(point_data))) {
      LOG_WARN("replay add backup point failed", K(ret), K(point_data));
    }
  }
  return ret;
}

int ObRecoveryDataMgr::replay_remove_recovery_point(
    const ObRecoveryPointType point_type, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(point_type), K(snapshot_version));
  } else if (point_type == ObRecoveryPointType::RESTORE_POINT) {
    if (OB_FAIL(replay_remove_restore_point_(snapshot_version))) {
      LOG_WARN("replay remove restore point failed", K(ret), K(snapshot_version));
    }
  } else if (point_type == ObRecoveryPointType::BACKUP) {
    if (OB_FAIL(replay_remove_backup_point_(snapshot_version))) {
      LOG_WARN("replay remove backup point failed", K(ret), K(snapshot_version));
    }
  } else {
    LOG_WARN("unknown point type", K(ret), K(snapshot_version), K(point_type));
  }
  return ret;
}

int ObRecoveryDataMgr::enable_write_slog(ObPGSSTableMgr& pg_sstable_mgr)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else {
    if (OB_FAIL(restore_point_data_.finish_replay(pg_sstable_mgr))) {
      LOG_WARN("failed to finish replay", K(ret));
    } else if (OB_FAIL(backup_point_data_.finish_replay(pg_sstable_mgr))) {
      LOG_WARN("failed to finish replay", K(ret));
    }
    enable_write_slog_ = true;
    LOG_INFO("recovery data mgr finish replay", K(ret), KPC(this));
  }
  return ret;
}

int ObRecoveryDataMgr::add_restore_point(const int64_t snapshot_version, const ObPartitionGroupMeta& pg_meta,
    const ObIArray<ObPGPartitionStoreMeta>& partition_store_metas, const ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  TCWLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version), K(pg_meta));
  } else if (OB_FAIL(restore_point_data_.check_recovery_point_exist(snapshot_version, is_exist))) {
    LOG_WARN("check exist failed", K(ret), K(pg_meta));
  } else if (is_exist) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("the snapshot exist, skip", K(ret), K(pg_meta));
  } else if (OB_FAIL(add_recovery_point_(ObRecoveryPointType::RESTORE_POINT,
                 snapshot_version,
                 pg_meta,
                 partition_store_metas,
                 tables_handle,
                 restore_point_data_))) {
    LOG_WARN("failed to add restore point", K(ret), K(pg_meta));
  }
  return ret;
}

int ObRecoveryDataMgr::check_restore_point_exist(const int64_t snapshot_version, bool& is_exist)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else if (OB_FAIL(restore_point_data_.check_recovery_point_exist(snapshot_version, is_exist))) {
    LOG_WARN("check recovery point exist failed", K(ret), K(snapshot_version));
  }
  return ret;
}

int ObRecoveryDataMgr::get_all_points_info(ObIArray<ObRecoveryPointInfo>& points_info)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointInfo point_info;
  ObRecoveryPointData* point = NULL;
  ObRecoveryPointType type = ObRecoveryPointType::UNKNOWN_TYPE;
  points_info.reuse();

  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (restore_point_data_.get_recovery_point_cnt() <= 0 && backup_point_data_.get_recovery_point_cnt() <= 0) {
    // LOG_INFO("get_all_points_info no data");
    // do nothing
  } else {
    // restore point list
    if (restore_point_data_.get_recovery_point_cnt() > 0) {
      point = restore_point_data_.get_first();
      type = ObRecoveryPointType::RESTORE_POINT;
      while (OB_SUCC(ret) && point != NULL && point != restore_point_data_.get_header()) {
        point_info.reset();
        point_info.init(type, point);
        // LOG_INFO("get_all_points_info get restore point ", K(point_info));
        if (OB_FAIL(points_info.push_back(point_info))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("failed to push back into array", K(ret));
        }
        point = point->get_next();
      }
    }
    // backup point list
    if (backup_point_data_.get_recovery_point_cnt() > 0) {
      point = backup_point_data_.get_first();
      type = ObRecoveryPointType::BACKUP;
      while (OB_SUCC(ret) && point != NULL && point != backup_point_data_.get_header()) {
        point_info.reset();
        point_info.init(type, point);
        // LOG_INFO("get_all_points_info get backup point ", K(point_info));
        if (OB_FAIL(points_info.push_back(point_info))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("failed to push back into array", K(ret));
        }
        point = point->get_next();
      }
    }
  }
  return ret;
}

int ObRecoveryDataMgr::get_restore_point_read_tables(
    const int64_t table_id, const int64_t snapshot_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else if (OB_FAIL(restore_point_data_.get_recovery_point_table_data(table_id, snapshot_version, handle))) {
    LOG_WARN("get recovery point table data failed", K(ret), K(table_id), K(snapshot_version));
  }
  return ret;
}
// TODO: writing slog with lock will affect the read request and need to be optimized.
int ObRecoveryDataMgr::remove_unneed_restore_point(
    const ObIArray<int64_t>& versions_need_left, const int64_t snapshot_gc_ts)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);
  ObSEArray<ObRecoveryPointData*, 1> points_data;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (!enable_write_slog_) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_INFO("do not allow remove recovery point data during replay");
    }
  } else if (OB_FAIL(restore_point_data_.get_unneed_recovery_points(versions_need_left, snapshot_gc_ts, points_data))) {
    LOG_WARN("get recovery point failed", K(ret));
  } else if (points_data.empty()) {
    // do nothing
  } else if (OB_FAIL(write_remove_data_slogs_(ObRecoveryPointType::RESTORE_POINT, points_data))) {
    LOG_WARN("write remove log failed", K(ret), K(versions_need_left));
  } else if (OB_FAIL(restore_point_data_.remove_recovery_points(points_data))) {
    LOG_ERROR("failed to remove restore point from recovery_point_data", K(ret), K(points_data), K(snapshot_gc_ts));
    ob_abort();
  }
  return ret;
}

int ObRecoveryDataMgr::get_restore_point_start_version(int64_t& start_version)
{
  int ret = OB_SUCCESS;
  TCRLockGuard lock_guard(lock_);
  start_version = restore_point_start_version_;
  return ret;
}

int ObRecoveryDataMgr::set_restore_point_start_version(const int64_t start_version)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);
  restore_point_start_version_ = start_version;
  return ret;
}

int ObRecoveryDataMgr::replay_add_restore_point_(const ObRecoveryPointData& point_data)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!point_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(point_data));
  } else if (OB_FAIL(restore_point_data_.check_recovery_point_exist(point_data.get_snapshot_version(), is_exist))) {
    LOG_WARN("check restore point exist failed", K(ret));
  } else if (is_exist) {
    ret = OB_SUCCESS;
    // do nothing
  } else if (OB_FAIL(restore_point_data_.replay_add_recovery_point(point_data))) {
    LOG_WARN("replay add restore point failed", K(ret), K(point_data));
  }
  return ret;
}

int ObRecoveryDataMgr::replay_remove_restore_point_(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* data_to_remove = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else if (OB_FAIL(restore_point_data_.get_recovery_point(snapshot_version, data_to_remove))) {
    LOG_WARN("get point need to delete failed.", K(ret), K(snapshot_version));
  } else if (OB_ISNULL(data_to_remove)) {
    // do nothing
  } else if (OB_FAIL(restore_point_data_.replay_remove_recovery_point(data_to_remove))) {
    LOG_WARN("replay remove restore point failed", K(ret), K(snapshot_version));
  } else {
    // do nothing
  }
  return ret;
}

int ObRecoveryDataMgr::add_backup_point(const int64_t snapshot_version, const ObPartitionGroupMeta& pg_meta,
    const ObIArray<ObPGPartitionStoreMeta>& partition_store_metas, const ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  TCWLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version), K(pg_meta));
  } else if (OB_FAIL(backup_point_data_.check_recovery_point_exist(snapshot_version, is_exist))) {
    LOG_WARN("check exist failed", K(ret), K(pg_meta));
  } else if (is_exist) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("the snapshot exist, skip", K(ret), K(pg_meta));
  } else if (OB_FAIL(add_recovery_point_(ObRecoveryPointType::BACKUP,
                 snapshot_version,
                 pg_meta,
                 partition_store_metas,
                 tables_handle,
                 backup_point_data_))) {
    LOG_WARN("failed to add backup point", K(ret), K(pg_meta));
  }
  return ret;
}

int ObRecoveryDataMgr::get_backup_point_data(
    const int64_t snapshot_version, ObPartitionGroupMeta& pg_meta, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* point_data = NULL;
  pg_meta.reset();
  handle.reset();
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else if (OB_FAIL(backup_point_data_.get_recovery_point(snapshot_version, point_data))) {
    LOG_WARN("failed to get recovery point", K(ret), K(snapshot_version));
  } else if (OB_ISNULL(point_data)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can not find corresponding point data", K(ret), K(snapshot_version), KPC(this));
  } else if (OB_FAIL(pg_meta.deep_copy(point_data->get_pg_meta()))) {
    LOG_WARN("failed to copy pg meta", K(ret), KPC(point_data));
  } else if (OB_FAIL(point_data->get_all_tables(handle))) {
    LOG_WARN("failed to get tables", K(ret), KPC(point_data), K(snapshot_version));
  } else {
    // do nothing
  }
  return ret;
}

int ObRecoveryDataMgr::get_backup_pg_meta_data(const int64_t snapshot_version, ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* point_data = NULL;
  pg_meta.reset();
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else if (OB_FAIL(backup_point_data_.get_recovery_point(snapshot_version, point_data))) {
    LOG_WARN("failed to get recovery point", K(ret), K(snapshot_version));
  } else if (OB_ISNULL(point_data)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can not find corresponding point data", K(ret), K(snapshot_version), KPC(this));
  } else if (OB_FAIL(pg_meta.deep_copy(point_data->get_pg_meta()))) {
    LOG_WARN("failed to copy pg meta", K(ret), KPC(point_data));
  } else {
    // do nothing
  }
  return ret;
}

int ObRecoveryDataMgr::get_backup_partition_meta_data(const ObPartitionKey& pkey, const int64_t snapshot_version,
    ObPGPartitionStoreMeta& partition_store_meta, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* point_data = NULL;
  partition_store_meta.reset();
  handle.reset();
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version), K(pkey));
  } else if (OB_FAIL(backup_point_data_.get_recovery_point(snapshot_version, point_data))) {
    LOG_WARN("failed to get recovery point", K(ret), K(snapshot_version));
  } else if (OB_ISNULL(point_data)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can not find corresponding point data", K(ret), K(snapshot_version), KPC(this));
  } else if (OB_FAIL(point_data->get_partition_store_meta(pkey, partition_store_meta))) {
    LOG_WARN("failed to copy pg meta", K(ret), KPC(point_data));
  } else if (OB_FAIL(point_data->get_partition_tables(pkey, handle))) {
    LOG_WARN("failed to get tables", K(ret), KPC(point_data), K(snapshot_version));
  } else {
    // do nothing
  }
  return ret;
}

int ObRecoveryDataMgr::get_backup_sstable(
    const int64_t snapshot_version, const ObITable::TableKey& table_key, ObTableHandle& handle)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* point_data = NULL;
  handle.reset();
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0) || OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version), K(table_key));
  } else if (OB_FAIL(backup_point_data_.get_recovery_point(snapshot_version, point_data))) {
    LOG_WARN("failed to get recovery point", K(ret), K(snapshot_version));
  } else if (OB_ISNULL(point_data)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can not find corresponding point data", K(ret), K(snapshot_version), KPC(this));
  } else if (OB_FAIL(point_data->get_sstable(table_key, handle))) {
    LOG_WARN("failed to get table", K(ret), KPC(point_data), K(table_key));
  } else {
    // do nothing
  }
  return ret;
}

int ObRecoveryDataMgr::remove_unneed_backup_point(
    const ObIArray<int64_t>& versions_need_left, const int64_t snapshot_gc_ts)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);
  ObSEArray<ObRecoveryPointData*, 1> points_data;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (!enable_write_slog_) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_INFO("do not allow remove recovery point data during replay");
    }
  } else if (OB_FAIL(backup_point_data_.get_unneed_recovery_points(versions_need_left, snapshot_gc_ts, points_data))) {
    LOG_WARN("get recovery point failed", K(ret));
  } else if (points_data.empty()) {
    // do nothing
  } else if (OB_FAIL(write_remove_data_slogs_(ObRecoveryPointType::BACKUP, points_data))) {
    LOG_WARN("write remove log failed", K(ret), K(versions_need_left));
  } else if (OB_FAIL(backup_point_data_.remove_recovery_points(points_data))) {
    LOG_ERROR("failed to remove backup point from recovery_point_data", K(ret), K(points_data), K(snapshot_gc_ts));
    ob_abort();
  }
  return ret;
}

int ObRecoveryDataMgr::replay_add_backup_point_(const ObRecoveryPointData& point_data)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!point_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(point_data));
  } else if (OB_FAIL(backup_point_data_.check_recovery_point_exist(point_data.get_snapshot_version(), is_exist))) {
    LOG_WARN("check backup point exist failed", K(ret));
  } else if (is_exist) {
    ret = OB_SUCCESS;
    // do nothing
  } else if (OB_FAIL(backup_point_data_.replay_add_recovery_point(point_data))) {
    LOG_WARN("replay add backup point failed", K(ret), K(point_data));
  }
  return ret;
}

int ObRecoveryDataMgr::replay_remove_backup_point_(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* data_to_remove = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else if (OB_FAIL(backup_point_data_.get_recovery_point(snapshot_version, data_to_remove))) {
    LOG_WARN("get point need to delete failed.", K(ret), K(snapshot_version));
  } else if (OB_ISNULL(data_to_remove)) {
    // do nothing
  } else if (OB_FAIL(backup_point_data_.replay_remove_recovery_point(data_to_remove))) {
    LOG_WARN("replay remove backup point failed", K(ret), K(snapshot_version));
  } else {
    // do nothing
  }
  return ret;
}

int ObRecoveryDataMgr::check_backup_point_exist(const int64_t snapshot_version, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else if (OB_FAIL(backup_point_data_.check_recovery_point_exist(snapshot_version, is_exist))) {
    LOG_WARN("check recovery point exist failed", K(ret), K(snapshot_version));
  }
  return ret;
}

int ObRecoveryDataMgr::get_all_backup_tables(const int64_t snapshot_version, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointData* point_data = NULL;
  handle.reset();
  TCRLockGuard lock_guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRecoveryDataMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_version));
  } else if (OB_FAIL(backup_point_data_.get_recovery_point(snapshot_version, point_data))) {
    LOG_WARN("failed to get recovery point", K(ret), K(snapshot_version));
  } else if (OB_ISNULL(point_data)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can not find corresponding point data", K(ret), K(snapshot_version), KPC(this));
  } else if (OB_FAIL(point_data->get_all_tables(handle))) {
    LOG_WARN("failed to get table", K(ret), KPC(point_data), K(snapshot_version));
  } else {
    // do nothing
  }
  return ret;
}

int ObRecoveryPointInfo::init(const ObRecoveryPointType type, ObRecoveryPointData* point)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(point) || !point->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recovery point not valid.", K(ret), KPC(point));
  } else if (OB_FAIL(point->get_all_tables(tables_handle_))) {
    LOG_WARN("get all tables failed.", K(ret));
  } else {
    snapshot_version_ = point->get_snapshot_version();
    type_ = type;
  }
  return ret;
}

int ObRecoveryPointInfo::get_tables(ObTablesHandle& handle) const
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_FAIL(handle.add_tables(tables_handle_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add tabes into handle failed.", K(ret));
  }
  return ret;
}

int ObRecoveryPointInfo::assign(const ObRecoveryPointInfo& other)
{
  int ret = OB_SUCCESS;

  snapshot_version_ = other.get_snapshot_version();
  other.get_type(type_);
  other.get_tables(tables_handle_);

  return ret;
}

int ObRecoveryPointIterator::init(ObRecoveryDataMgr& data_mgr)
{
  int ret = OB_SUCCESS;
  data_mgr_ = &data_mgr;
  if (OB_FAIL(data_mgr_->get_all_points_info(points_info_))) {
    LOG_WARN("get all points info from data mgr failed.", K(ret));
  }
  return ret;
}

int ObRecoveryPointIterator::get_next(ObRecoveryPointInfo*& point_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data_mgr_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The data mgr of iterator is NULL, ", K(ret));
  } else if (array_idx_ < points_info_.count()) {
    point_info = &(points_info_.at(array_idx_));
    array_idx_++;
  } else {
    points_info_.reuse();
    array_idx_ = 0;
    ret = OB_ITER_END;
  }
  return ret;
}
