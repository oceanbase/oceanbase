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
#include "storage/ob_i_table.h"
#include "share/ob_force_print_log.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tx_table/ob_tx_ctx_memtable.h"
#include "storage/tx_table/ob_tx_data_memtable.h"

using namespace oceanbase;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::memtable;
using namespace oceanbase::transaction::tablelock;

ObITable::TableKey::TableKey()
  :
    tablet_id_(),
    scn_range_(),
    column_group_idx_(0),
    table_type_(ObITable::MAX_TABLE_TYPE)
{
}


void ObITable::TableKey::reset()
{
  tablet_id_.reset();
  scn_range_.reset();
  column_group_idx_ = 0;
  table_type_ = ObITable::MAX_TABLE_TYPE;
}

const char* ObITable::table_type_name_[] =
{
  "MEMTABLE",
  "TX_DATA_MEMTABLE",
  "TX_CTX_MEMTABLE",
  "LOCK_MEMTABLE",
  "",
  "",
  "",
  "",
  "",
  "",
  "MAJOR",
  "MINOR",
  "MINI",
  "META_MAJOR",
  "DDL_DUMP",
  "REMOTE_LOGICAL_MINOR",
  "DDL_MEM",
};

uint64_t ObITable::TableKey::hash() const
{
  uint64_t hash_value = 0;
  hash_value = common::murmurhash(&table_type_, sizeof(table_type_), hash_value);
  hash_value = common::murmurhash(&column_group_idx_, sizeof(table_type_), hash_value);
  hash_value += tablet_id_.hash();
  if (is_table_with_scn_range()) {
    hash_value += scn_range_.hash();
  } else {
    hash_value += version_range_.hash();
  }
  return hash_value;
}

OB_SERIALIZE_MEMBER(
    ObITable::TableKey,
    tablet_id_,
    scn_range_,
    column_group_idx_,
    table_type_);

OB_SERIALIZE_MEMBER(ObITable, key_);

ObITable::ObITable()
  : key_(),
    ref_cnt_(0)
{
  STATIC_ASSERT(static_cast<int64_t>(TableType::MAX_TABLE_TYPE) == ARRAYSIZEOF(table_type_name_), "table_type_name is mismatch");
}

int ObITable::init(const TableKey &table_key)
{
  int ret = OB_SUCCESS;

  if (ObITable::MAX_TABLE_TYPE != key_.table_type_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret), K(key_), K(table_key));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(table_key));
  } else {
    key_ = table_key;
  }

  return ret;
}

void ObITable::reset()
{
  key_.reset();
}

int ObITable::safe_to_destroy(bool &is_safe)
{
  is_safe = true;
  return OB_SUCCESS;
}

int ObITable::exist(
    ObStoreCtx &ctx,
    const uint64_t table_id,
    const storage::ObTableReadInfo &read_info,
    const blocksstable::ObDatumRowkey &rowkey,
    bool &is_exist,
    bool &has_found)
{
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(read_info);
  UNUSED(rowkey);
  is_exist = false;
  has_found = false;
  return common::OB_NOT_SUPPORTED;
}

int ObITable::exist(
    ObRowsInfo &rows_info,
    bool &is_exist,
    bool &has_found)
{
  UNUSED(rows_info);
  is_exist = false;
  has_found = false;
  return common::OB_NOT_SUPPORTED;
}

int64_t ObITable::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(KP(this), K_(key), K_(ref_cnt), "upper_trans_version", get_upper_trans_version(),
         "timestamp", get_timestamp());
    J_OBJ_END();
  }
  return pos;
}

ObTableHandleV2::ObTableHandleV2()
  : table_(nullptr), t3m_(nullptr), allocator_(nullptr), table_type_(ObITable::TableType::MAX_TABLE_TYPE)
{
  INIT_OBJ_LEAK_DEBUG_NODE(node_, this, share::LEAK_CHECK_OBJ_TABLE_HANDLE, MTL_ID());
}

ObTableHandleV2::ObTableHandleV2(ObITable *table, ObTenantMetaMemMgr *t3m, ObITable::TableType type)
  : table_(nullptr), t3m_(nullptr), allocator_(nullptr), table_type_(type)
{
  abort_unless(OB_SUCCESS == set_table(table, t3m, table_type_));
  INIT_OBJ_LEAK_DEBUG_NODE(node_, this, share::LEAK_CHECK_OBJ_TABLE_HANDLE, MTL_ID());
}

ObTableHandleV2::~ObTableHandleV2()
{
  reset();
}

bool ObTableHandleV2::is_valid() const
{
  return nullptr != table_
      && ((nullptr != t3m_ && nullptr == allocator_) || (nullptr == t3m_ && nullptr != allocator_));
}

void ObTableHandleV2::reset()
{
  if (nullptr != table_) {
    if (OB_UNLIKELY(!is_valid())) {
      STORAGE_LOG_RET(ERROR, OB_INVALID_ERROR, "t3m or allocator is nullptr", KP_(table), KP_(t3m), KP_(allocator));
      ob_abort();
    } else {
      const int64_t ref_cnt = table_->dec_ref();
      if (0 == ref_cnt) {
        if (nullptr != t3m_) {
          t3m_->push_table_into_gc_queue(table_, table_type_);
        } else {
          table_->~ObITable();
          allocator_->free(table_);
        }
      } else if (OB_UNLIKELY(ref_cnt < 0)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "table ref cnt may be leaked", K(ref_cnt), KP(table_), K(table_type_));
      }
      table_ = nullptr;
      t3m_ = nullptr;
      allocator_ = nullptr;
    }
  }
}

int ObTableHandleV2::get_sstable(blocksstable::ObSSTable *&sstable)
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
    sstable = static_cast<blocksstable::ObSSTable *>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_sstable(const blocksstable::ObSSTable *&sstable) const
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
    sstable = static_cast<const blocksstable::ObSSTable *>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_memtable(memtable::ObIMemtable *&memtable)
{
  int ret = OB_SUCCESS;
  memtable = nullptr;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
   } else if (!table_->is_memtable()) {
     ret = OB_ENTRY_NOT_EXIST;
     STORAGE_LOG(WARN, "not memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<memtable::ObIMemtable*>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_memtable(const memtable::ObIMemtable *&memtable) const
{
  int ret = OB_SUCCESS;
  memtable = nullptr;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
   } else if (!table_->is_memtable()) {
     ret = OB_ENTRY_NOT_EXIST;
     STORAGE_LOG(WARN, "not memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<memtable::ObIMemtable*>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_data_memtable(memtable::ObMemtable *&memtable)
{
  int ret = OB_SUCCESS;
  memtable = NULL;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
   } else if (!table_->is_data_memtable()) {
     ret = OB_ERR_UNEXPECTED;
     STORAGE_LOG(WARN, "not data memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<memtable::ObMemtable*>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_data_memtable(const memtable::ObMemtable *&memtable) const
{
  int ret = OB_SUCCESS;
  memtable = NULL;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
   } else if (!table_->is_data_memtable()) {
     ret = OB_ENTRY_NOT_EXIST;
     STORAGE_LOG(WARN, "not data memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<const memtable::ObMemtable*>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_tx_data_memtable(ObTxDataMemtable *&memtable)
{
  int ret = OB_SUCCESS;
  memtable = nullptr;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_tx_data_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not tx data memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<ObTxDataMemtable*>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_tx_data_memtable(const ObTxDataMemtable *&memtable) const
{
  int ret = OB_SUCCESS;
  memtable = nullptr;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_tx_data_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not tx data memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<ObTxDataMemtable*>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_tx_ctx_memtable(ObTxCtxMemtable *&memtable)
{
  int ret = OB_SUCCESS;
  memtable = nullptr;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_tx_ctx_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not tx ctx memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<ObTxCtxMemtable*>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_tx_ctx_memtable(const ObTxCtxMemtable *&memtable) const
{
  int ret = OB_SUCCESS;
  memtable = nullptr;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_tx_ctx_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not tx ctx memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<ObTxCtxMemtable*>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_lock_memtable(ObLockMemtable *&memtable)
{
  int ret = OB_SUCCESS;
  memtable = nullptr;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_lock_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not lock memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<ObLockMemtable*>(table_);
  }
  return ret;
}

int ObTableHandleV2::get_lock_memtable(const ObLockMemtable *&memtable) const
{
  int ret = OB_SUCCESS;
  memtable = nullptr;

  if (OB_ISNULL(table_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_->is_lock_memtable()) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "not lock memtable", K(ret), K(table_->get_key()));
  } else {
    memtable = static_cast<ObLockMemtable*>(table_);
  }
  return ret;
}

ObTableHandleV2::ObTableHandleV2(const ObTableHandleV2 &other)
  : table_(nullptr), t3m_(nullptr)
{
  INIT_OBJ_LEAK_DEBUG_NODE(node_, this, share::LEAK_CHECK_OBJ_TABLE_HANDLE, MTL_ID());
  *this = other;
}

ObTableHandleV2 &ObTableHandleV2::operator= (const ObTableHandleV2 &other)
{
  if (this != &other) {
    reset();
    if (nullptr != other.table_) {
      if (OB_UNLIKELY(!other.is_valid())) {
        STORAGE_LOG_RET(ERROR, OB_INVALID_ERROR, "t3m_ is nullptr", K(other));
        ob_abort();
      } else {
        table_ = other.table_;
        other.table_->inc_ref();
        t3m_ = other.t3m_;
        allocator_ = other.allocator_;
        table_type_ = other.table_type_;
        if (OB_UNLIKELY(other.table_->get_ref() < 2)) {
          STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "The reference count of the table is unexpectedly decreased,"
              " the possible reason is that the table handle has concurrency", K(other));
        }
      }
    }
  }

  return *this;
}

int ObTableHandleV2::set_table(
    ObITable *table,
    ObTenantMetaMemMgr *t3m,
    const ObITable::TableType table_type)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(table) || OB_ISNULL(t3m) ||
      OB_UNLIKELY(!ObITable::is_table_type_valid(table_type))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(table), KP(t3m), K(table_type));
  } else {
    table_ = table;
    table_->inc_ref();
    t3m_ = t3m;
    allocator_ = nullptr;
    table_type_ = table_type;
  }
  return ret;
}

int ObTableHandleV2::set_table(
    ObITable *table,
    common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(table) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(table), KP(allocator));
  } else {
    table_ = table;
    table_->inc_ref();
    t3m_ = nullptr;
    allocator_ = allocator;
    table_type_ = ObITable::TableType::MAX_TABLE_TYPE;
  }
  return ret;
}

ObTablesHandleArray::ObTablesHandleArray()
  : meta_mem_mgr_(nullptr),
    allocator_(nullptr),
    tablet_id_(),
    tables_()
{
}

ObTablesHandleArray::~ObTablesHandleArray()
{
  reset();
}

void ObTablesHandleArray::reset()
{
  if (tables_.count() > 0) {
    for (int64_t i = 0; i < tables_.count(); ++i) {
      ObITable *table = tables_.at(i);
      if (OB_NOT_NULL(table)) {
        const int64_t ref_cnt = table->dec_ref();
        if (0 == ref_cnt) {
          if (OB_ISNULL(meta_mem_mgr_) && OB_ISNULL(allocator_)) {
            STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "[MEMORY LEAK] meta_mem_mgr is unexpected null!!!", KPC(table), K(tablet_id_), KP(meta_mem_mgr_), KP(allocator_));
          } else if (nullptr != meta_mem_mgr_) {
            meta_mem_mgr_->push_table_into_gc_queue(table, table->get_key().table_type_);
          } else {
            table->~ObITable();
            allocator_->free(table);
          }
        } else if (OB_UNLIKELY(ref_cnt < 0)) {
          LOG_ERROR_RET(OB_ERR_UNEXPECTED, "table ref cnt may be leaked", K(ref_cnt), KP(table), "table type", table->get_key().table_type_);
        }
      }
      tables_.at(i) = nullptr;
    }
    tables_.reset();
    tablet_id_.reset();
    meta_mem_mgr_ = nullptr;
    allocator_ = nullptr;
  }
}

int ObTablesHandleArray::add_table(ObITable *table, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(table), KP(allocator));
  } else if (OB_NOT_NULL(meta_mem_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tables either use an allocator or a t3m, not a mix", K(ret), KP(allocator_),
        KP(meta_mem_mgr_), KP(MTL(ObTenantMetaMemMgr *)));
  } else if (0 == tables_.count()) { // first add table, set allocator_ && tablet_id
    allocator_ = allocator;
    tablet_id_ = table->get_key().tablet_id_;
  } else if (OB_UNLIKELY(allocator_ != allocator)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tables must own the same allocator!!!", K(ret), KP(allocator_), KP(allocator));
  } else if (OB_UNLIKELY(tablet_id_ != table->get_key().tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tables must belong to the same tablet!!!", K(ret), K(tablet_id_), K(table->get_key()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tables_.push_back(table))) {
    STORAGE_LOG(WARN, "failed to add table", K(ret));
  } else {
    table->inc_ref();
  }
  return ret;
}

int ObTablesHandleArray::add_table(ObITable *table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid arguments", K(ret), KP(table));
  } else if (0 == tables_.count()) { // first add table, set meta_mem_mgr && tablet_id
    if (OB_ISNULL(meta_mem_mgr_ = MTL(ObTenantMetaMemMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to get TenantMetaMemMgr from MTL", K(ret));
    } else {
      allocator_ = nullptr;
      tablet_id_ = table->get_key().tablet_id_;
    }
  } else if (OB_NOT_NULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tables either use an allocator or a t3m, not a mix", K(ret), KP(allocator_),
        KP(meta_mem_mgr_), KP(MTL(ObTenantMetaMemMgr *)));
  } else if (OB_UNLIKELY(meta_mem_mgr_ != MTL(ObTenantMetaMemMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tables must own the same t3m!!!", K(ret), KP(meta_mem_mgr_), KP(MTL(ObTenantMetaMemMgr *)));
  } else if (OB_UNLIKELY(tablet_id_ != table->get_key().tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tables must belong to the same tablet!!!", K(ret), K(tablet_id_), K(table->get_key()));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tables_.push_back(table))) {
    STORAGE_LOG(WARN, "failed to add table", K(ret));
  } else {
    table->inc_ref();
  }
  return ret;
}

int ObTablesHandleArray::add_table(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(handle.get_allocator())) {
    if (OB_FAIL(add_table(handle.get_table(), handle.get_allocator()))) {
      STORAGE_LOG(WARN, "fail to add table", K(ret), K(handle));
    }
  } else if (OB_FAIL(add_table(handle.get_table()))) {
    STORAGE_LOG(WARN, "fail to add table", K(ret), K(handle));
  }
  return ret;
}

int ObTablesHandleArray::assign(const ObTablesHandleArray &other)
{
  int ret = OB_SUCCESS;
  reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < other.get_count(); ++i) {
    ObITable *table = other.get_table(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "table must not null", K(ret), K(other));
    } else if (OB_NOT_NULL(other.allocator_)) {
      if (OB_FAIL(add_table(table, other.allocator_))) {
        STORAGE_LOG(WARN, "fail to add table", K(ret));
      }
    } else if (OB_FAIL(add_table(table))) {
      STORAGE_LOG(WARN, "failed to add table", K(ret));
    }
  }
  return ret;
}

int ObTablesHandleArray::get_tables(common::ObIArray<ObITable *> &tables) const
{
  int ret = OB_SUCCESS;
  tables.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
    if (OB_ISNULL(tables_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get unexpected null table", K(ret), K(i), K(tables_));
    } else if (OB_FAIL(tables.push_back(tables_.at(i)))) {
      STORAGE_LOG(WARN, "failed to add table", K(ret));
    }
  }
  return ret;
}

int ObTablesHandleArray::get_first_memtable(memtable::ObIMemtable *&memtable) const
{
  int ret = OB_SUCCESS;
  memtable = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
    if (tables_.at(i)->is_memtable()) {
      memtable = static_cast<memtable::ObIMemtable*>(tables_.at(i));
      break;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(memtable)) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObTablesHandleArray::get_all_minor_sstables(common::ObIArray<ObITable *> &tables) const
{
  int ret = OB_SUCCESS;
  tables.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
    ObITable *table = tables_.at(i);
    if (table->is_minor_sstable() && OB_FAIL(tables.push_back(table))) {
      STORAGE_LOG(WARN, "failed to add minor sstable", K(ret), K(i));
    }
  }
  return ret;
}

int ObTablesHandleArray::check_continues(const share::ObScnRange *scn_range) const
{
  int ret = OB_SUCCESS;

  if (!tables_.empty()) {
    // 1:check major sstable
    // there can only be one major or meta merge
    const ObITable *last_table = nullptr;
    const ObITable *table = nullptr;
    SCN base_end_scn = SCN::min_scn();
    int64_t i = 0;
    if (OB_ISNULL(table = tables_.at(i))) {
      ret = OB_ERR_SYS;
      LOG_WARN("table is NULL", KPC(table));
    } else if (table->is_major_sstable() || table->is_meta_major_sstable()) {
      base_end_scn = table->is_meta_major_sstable() ? table->get_end_scn() : SCN::min_scn();
      i++;
    }
    // 2:check minor sstable
    for ( ; OB_SUCC(ret) && i < tables_.count(); ++i) {
      table = tables_.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_WARN("table is NULL", KPC(table));
      } else if (table->is_major_sstable() || table->is_meta_major_sstable()) {
        ret = OB_ERR_SYS;
        LOG_WARN("major sstable or meta merge should be first", K(ret), K(i), K(table));
      } else if (OB_ISNULL(last_table)) { // first table
        if (OB_NOT_NULL(scn_range)
            && table->get_start_scn() > scn_range->start_scn_) {
          ret = OB_LOG_ID_RANGE_NOT_CONTINUOUS;
          LOG_WARN("first minor sstable don't match the scn_range::start_log_ts", K(ret),
              KPC(scn_range), K(i), K(*this));
        } else if (table->get_end_scn() <= base_end_scn) {
          ret = OB_LOG_ID_RANGE_NOT_CONTINUOUS;
          LOG_WARN("Unexpected end log ts of first minor sstable", K(ret), K(base_end_scn), K(i), K(*this));
        }
      } else if (table->get_start_scn() > last_table->get_end_scn()) {
        ret = OB_LOG_ID_RANGE_NOT_CONTINUOUS;
        LOG_WARN("log ts range is not continuous", K(ret), K(i), K(*this));
      }
      last_table = table;
    }
  }
  return ret;
}

int64_t ObTablesHandleArray::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(KP(meta_mem_mgr_), KP(allocator_));
    J_COMMA();
    J_KV("tablet_id", tablet_id_);
    J_COMMA();
    J_KV("table_count", tables_.count());
    J_COMMA();
    J_ARRAY_START();
    for (int64_t i = 0; i < tables_.count(); ++i) {
      const ObITable *table = tables_.at(i);
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
