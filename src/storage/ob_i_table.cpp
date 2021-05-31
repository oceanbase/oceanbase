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
#include "share/ob_force_print_log.h"
#include "ob_i_table.h"
#include "ob_sstable.h"
#include "ob_old_sstable.h"
#include "ob_table_mgr.h"
#include "memtable/ob_memtable.h"
#include "share/ob_force_print_log.h"
#include "ob_table_store.h"
using namespace oceanbase;
using namespace storage;
using namespace common;

ObITable::TableKey::TableKey()
    : table_type_(ObITable::MAX_TABLE_TYPE),
      pkey_(),
      table_id_(common::OB_INVALID_ID),
      trans_version_range_(),
      version_(),
      log_ts_range_()
{}

ObITable::TableKey::TableKey(const ObITable::TableType& table_type, const common::ObPartitionKey& pkey,
    const uint64_t table_id, const common::ObVersionRange& trans_version_range, const common::ObVersion& version,
    const common::ObLogTsRange& log_ts_range)
    : table_type_(table_type),
      pkey_(pkey),
      table_id_(table_id),
      trans_version_range_(trans_version_range),
      version_(version),
      log_ts_range_(log_ts_range)
{}

void ObITable::TableKey::reset()
{
  table_type_ = ObITable::MAX_TABLE_TYPE;
  pkey_.reset();
  table_id_ = common::OB_INVALID_ID;
  trans_version_range_.reset();
  version_.reset();
  log_ts_range_.reset();
}

const char* ObITable::table_type_name_[TableType::MAX_TABLE_TYPE] = {
    "MEMTABLE", "MAJOR", "OLD_MINOR", "TRANS", "MINOR", "COMPLEMENT", "SPARSE_MINOR", "MINI", "RESERVED"};

bool ObITable::TableKey::is_table_log_ts_comparable() const
{
  return is_table_with_log_ts_range() && log_ts_range_.end_log_ts_ > ObTableCompater::OB_MAX_COMPAT_LOG_TS;
}

uint64_t ObITable::TableKey::hash() const
{
  uint64_t hash_value = 0;
  hash_value = common::murmurhash(&table_type_, sizeof(table_type_), hash_value);
  hash_value += pkey_.hash();
  hash_value = common::murmurhash(&table_id_, sizeof(table_id_), hash_value);
  hash_value += trans_version_range_.hash();
  hash_value = common::murmurhash(&version_, sizeof(version_), hash_value);
  if (is_table_log_ts_comparable()) {
    hash_value += log_ts_range_.hash();
  }
  return hash_value;
}

OB_SERIALIZE_MEMBER(ObITable::TableKey, table_type_, pkey_, table_id_, trans_version_range_, version_, log_ts_range_);

OB_SERIALIZE_MEMBER(ObITable, key_);

ObITable::ObITable() : key_(), ref_cnt_(0)
{}

int ObITable::init(const TableKey& table_key, const bool skip_version_range, const bool skip_log_ts_range)
{
  int ret = OB_SUCCESS;

  if (ObITable::MAX_TABLE_TYPE != key_.table_type_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret), K(key_), K(table_key));
  } else if (!table_key.is_valid(skip_version_range, skip_log_ts_range)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(table_key));
  } else {
    key_ = table_key;
    ref_cnt_ = 0;
  }

  return ret;
}

int ObITable::exist(const ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
    const common::ObIArray<share::schema::ObColDesc>& column_ids, bool& is_exist, bool& has_found)
{
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(rowkey);
  UNUSED(column_ids);
  is_exist = false;
  has_found = false;
  return common::OB_NOT_SUPPORTED;
}

int ObITable::prefix_exist(ObRowsInfo& rows_info, bool& may_exist)
{
  UNUSED(rows_info);
  may_exist = true;
  return common::OB_NOT_SUPPORTED;
}

int ObITable::exist(ObRowsInfo& rows_info, bool& is_exist, bool& has_found)
{
  UNUSED(rows_info);
  is_exist = false;
  has_found = false;
  return common::OB_NOT_SUPPORTED;
}

int64_t ObITable::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(
        KP(this), K_(key), K_(ref_cnt), "upper_trans_version", get_upper_trans_version(), "timestamp", get_timestamp());
    J_OBJ_END();
  }
  return pos;
}

ObTableHandle::ObTableHandle() : table_(NULL)
{}

ObTableHandle::~ObTableHandle()
{
  reset();
}

int ObTableHandle::get_sstable(ObSSTable*& sstable)
{
  int ret = OB_SUCCESS;
  sstable = NULL;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_sstable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not sstable", K(ret), K(table_->get_key()));
  } else {
    sstable = static_cast<ObSSTable*>(table_);
  }
  return ret;
}

int ObTableHandle::get_old_sstable(ObOldSSTable*& sstable)
{
  int ret = OB_SUCCESS;
  sstable = NULL;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_sstable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not sstable", K(ret), K(table_->get_key()));
  } else {
    sstable = static_cast<ObOldSSTable*>(table_);
  }
  return ret;
}

int ObTableHandle::get_sstable(const ObSSTable*& sstable) const
{
  int ret = OB_SUCCESS;
  sstable = NULL;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_sstable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not sstable", K(ret), K(table_->get_key()));
  } else {
    sstable = static_cast<const ObSSTable*>(table_);
  }
  return ret;
}

int ObTableHandle::get_memtable(memtable::ObMemtable*& memtable)
{
  int ret = OB_SUCCESS;
  memtable = NULL;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<memtable::ObMemtable*>(table_);
  }
  return ret;
}

bool ObTableHandle::is_valid() const
{
  bool ret = NULL != table_;

  return ret;
}

int ObTableHandle::get_memtable(const memtable::ObMemtable*& memtable) const
{
  int ret = OB_SUCCESS;
  memtable = NULL;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<const memtable::ObMemtable*>(table_);
  }
  return ret;
}

int ObTableHandle::set_table(ObITable* table)
{
  int ret = OB_SUCCESS;

  if (NULL != table_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "cannot set table twice", K(ret), K(*table_));
  } else if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(table));
  } else {
    table_ = table;
    table_->inc_ref();
  }

  return ret;
}

int ObTableHandle::assign(const ObTableHandle& other)
{
  int ret = OB_SUCCESS;

  if (NULL != table_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "cannot set table twice", K(ret), K(*table_));
  } else if (NULL == other.table_) {
    // do nothing
  } else {
    table_ = other.table_;
    table_->inc_ref();
  }

  return ret;
}

void ObTableHandle::reset()
{
  if (NULL != table_) {
    ObTableMgr::get_instance().release_table(table_);
    table_ = NULL;
  }
}

int ObTableHandle::get_sstable_schema_version(int64_t& schema_version) const
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  const ObSSTable* sstable = NULL;
  if (OB_FAIL(get_sstable(sstable))) {
    STORAGE_LOG(WARN, "fail to get sstable", K(ret));
  } else {
    schema_version = sstable->get_meta().schema_version_;
  }
  return ret;
}

ObTablesHandle::ObTablesHandle() : tables_(), protection_cnt_(0), memstore_retired_(NULL)
{}

ObTablesHandle::~ObTablesHandle()
{
  for (int64_t i = 0; i < tables_.count(); ++i) {
    ObTableMgr::get_instance().release_table(tables_.at(i));
  }
}

int ObTablesHandle::add_table(ObITable* table)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(table));
  } else if (OB_FAIL(tables_.push_back(table))) {
    STORAGE_LOG(WARN, "failed to add table", K(ret));
  } else {
    table->inc_ref();
  }
  return ret;
}

int ObTablesHandle::add_table(ObTableHandle& handle)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(add_table(handle.get_table()))) {
    LOG_WARN("Failed to add table", K(ret));
  }
  return ret;
}

int ObTablesHandle::add_tables(const ObIArray<ObITable*>& tables)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
    if (OB_FAIL(add_table(tables.at(i)))) {
      STORAGE_LOG(WARN, "fail to add tables", K(ret));
    }
  }
  return ret;
}

int ObTablesHandle::add_tables(const ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
    ObITable* table = handle.get_table(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret), K(i), K(handle));
    } else if (OB_FAIL(add_table(table))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }

  return ret;
}

int ObTablesHandle::assign(const ObTablesHandle& other)
{
  reset();
  protection_cnt_ = other.protection_cnt_;
  memstore_retired_ = other.memstore_retired_;
  return add_tables(other);
}
int ObTablesHandle::get_first_sstable(ObSSTable*& sstable) const
{
  int ret = OB_SUCCESS;
  sstable = NULL;

  if (tables_.count() < 1) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "empty tables, cannot get first sstable", K(ret));
  } else if (!tables_.at(0)->is_sstable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "first table is not sstable", K(ret), K(*this));
  } else {
    sstable = static_cast<ObSSTable*>(tables_.at(0));
  }
  return ret;
}

int ObTablesHandle::get_last_major_sstable(ObSSTable*& sstable) const
{
  int ret = OB_SUCCESS;
  sstable = NULL;

  for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
    ObITable* table = tables_.at(i);
    if (table->is_major_sstable()) {
      sstable = static_cast<ObSSTable*>(table);
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(sstable)) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObTablesHandle::get_last_memtable(memtable::ObMemtable*& memtable)
{
  int ret = OB_SUCCESS;
  const int64_t last_idx = tables_.count() - 1;
  memtable = NULL;

  if (tables_.count() < 1) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "empty tables, cannot get last memtable", K(ret));
  } else if (!tables_.at(last_idx)->is_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    memtable = static_cast<memtable::ObMemtable*>(tables_.at(last_idx));
  }
  return ret;
}

int ObTablesHandle::get_all_sstables(common::ObIArray<ObSSTable*>& sstables)
{
  int ret = OB_SUCCESS;
  sstables.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
    ObITable* table = tables_.at(i);
    if (table->is_sstable()) {
      if (OB_FAIL(sstables.push_back(static_cast<ObSSTable*>(table)))) {
        STORAGE_LOG(WARN, "failed to add sstable", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObTablesHandle::get_all_minor_sstables(common::ObIArray<ObSSTable*>& sstables)
{
  int ret = OB_SUCCESS;
  sstables.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
    ObITable* table = tables_.at(i);
    if (table->is_minor_sstable()) {
      if (OB_FAIL(sstables.push_back(static_cast<ObSSTable*>(table)))) {
        STORAGE_LOG(WARN, "failed to add minor sstable", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObTablesHandle::get_all_memtables(common::ObIArray<memtable::ObMemtable*>& memtables)
{
  int ret = OB_SUCCESS;
  memtables.reset();
  if (tables_.empty()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
      ObITable* table = tables_.at(i);
      if (table->is_memtable()) {
        if (OB_FAIL(memtables.push_back(static_cast<memtable::ObMemtable*>(table)))) {
          STORAGE_LOG(WARN, "failed to add sstable", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

void ObTablesHandle::reset()
{
  for (int64_t i = 0; i < tables_.count(); ++i) {
    ObTableMgr::get_instance().release_table(tables_.at(i));
  }
  tables_.reset();

  protection_cnt_ = 0;
  memstore_retired_ = NULL;
}

int ObTablesHandle::check_continues(const ObLogTsRange* log_ts_range)
{
  int ret = OB_SUCCESS;
  bool is_all_contain = false;

  if (tables_.empty()) {
    ret = OB_VERSION_RANGE_NOT_CONTINUES;
    LOG_WARN("tables is empty", K(ret));
  } else {
    // 1:check major sstable
    // there can only be one major or buf minor
    ObITable* last_table = nullptr;
    ObITable* table = nullptr;
    int64_t base_end_log_ts = 0;
    int64_t i = 0;
    if (tables_.count() > 0) {
      if (OB_ISNULL(table = tables_.at(i))) {
        ret = OB_ERR_SYS;
        LOG_WARN("table is NULL", KPC(table));
      } else if (table->is_major_sstable()) {
        base_end_log_ts = table->get_end_log_ts();
        i++;
      }
    }
    // 2:check minor sstable
    for (; OB_SUCC(ret) && i < tables_.count(); ++i) {
      ObITable* table = tables_.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_WARN("table is NULL", KPC(table));
      } else if (table->is_major_sstable()) {
        ret = OB_ERR_SYS;
        LOG_WARN("major sstable or buf minor should be first", K(i), K(table));
      } else if (OB_ISNULL(last_table)) {  // first table
        if (OB_NOT_NULL(log_ts_range) && table->get_start_log_ts() > log_ts_range->start_log_ts_) {
          ret = OB_LOG_ID_RANGE_NOT_CONTINUOUS;
          LOG_WARN("first minor sstable don't match the log_ts_range::start_log_ts",
              K(ret),
              KPC(log_ts_range),
              K(i),
              K(*this));
        } else if (table->get_end_log_ts() <= base_end_log_ts) {
          ret = OB_LOG_ID_RANGE_NOT_CONTINUOUS;
          LOG_WARN("Unexpected end log ts of first minor sstable", K(ret), K(base_end_log_ts), K(i), K(*this));
        }
      } else if (table->get_start_log_ts() > last_table->get_end_log_ts()) {
        ret = OB_LOG_ID_RANGE_NOT_CONTINUOUS;
        LOG_WARN("log ts range is not continuous", K(ret), K(i), K(*this));
      }
      last_table = table;
    }
  }
  return ret;
}

int ObTablesHandle::reserve(const int64_t count)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tables_.reserve(count))) {
    LOG_WARN("Failed to reserve count", K(ret), K(count));
  }
  return ret;
}

int ObTablesHandle::get_first_memtable(memtable::ObMemtable*& memtable)
{
  int ret = OB_SUCCESS;
  memtable = NULL;

  if (tables_.count() < 1) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "empty tables, cannot get last memtable", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
      if (tables_.at(i)->is_memtable()) {
        memtable = static_cast<memtable::ObMemtable*>(tables_.at(i));
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(memtable)) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }
  return ret;
}

void ObTablesHandle::set_retire_check()
{
  memstore_retired_ = NULL;
  // memtable is mostly at the end, found first memtable
  if (1 == tables_.count()) {
    if (tables_.at(0)->is_memtable()) {
      memstore_retired_ = &(static_cast<memtable::ObMemtable*>(tables_.at(0))->get_read_barrier());
    }
  } else {
    for (int64_t i = tables_.count() - 1; i >= 0; --i) {
      if (tables_.at(i)->is_memtable()) {
        // continue
      } else if (tables_.count() - 1 == i) {
        break;  // not have memtable
      } else {
        ObITable* table = tables_.at(i + 1);
        memstore_retired_ = &(static_cast<memtable::ObMemtable*>(table)->get_read_barrier());
        break;
      }
    }
  }
}

bool ObTablesHandle::has_split_source_table(const common::ObPartitionKey& pkey) const
{
  bool found = false;
  for (int64_t i = 0; i < tables_.count(); ++i) {
    ObITable* table = tables_.at(i);
    if (table->get_partition_key().get_partition_id() != pkey.get_partition_id()) {
      found = true;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        FLOG_INFO("has_split_source_table", K(pkey), "table_key", table->get_partition_key(), K(lbt()));
      }
      break;
    }
  }
  return found;
}

int64_t ObTablesHandle::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV("table_count", tables_.count());
    J_COMMA();
    J_ARRAY_START();
    for (int64_t i = 0; i < tables_.count(); ++i) {
      ObITable* table = tables_[i];
      if (NULL != table) {
        J_OBJ_START();
        J_KV(K(i), "table_key", table->get_key(), "ref", table->get_ref());
        J_OBJ_END();
      }
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

void ObTableProtector::hold(ObITable& table)
{
  table.inc_ref();
}

void ObTableProtector::release(ObITable& table)
{
  int tmp_ret = common::OB_SUCCESS;
  if (common::OB_SUCCESS != (tmp_ret = ObTableMgr::get_instance().release_table(&table))) {
    LOG_ERROR("failed to release table", K(tmp_ret), K(table));
  }
}
