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
#include "storage/blocksstable/ob_storage_cache_suite.h"
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
    const storage::ObITableReadInfo &read_info,
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
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey,
    bool &is_exist,
    bool &has_found)
{
  UNUSED(param);
  UNUSED(context);
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
  : table_(nullptr), t3m_(nullptr), allocator_(nullptr),
    meta_handle_(), table_type_(ObITable::TableType::MAX_TABLE_TYPE)
{
}

ObTableHandleV2::~ObTableHandleV2()
{
  reset();
}

bool ObTableHandleV2::is_valid() const
{
  bool bret = false;
  if (nullptr == table_) {
  } else if (ObITable::is_memtable(table_type_)) {
    bret = (nullptr != t3m_) ^ (nullptr != allocator_);
  } else if (ObITable::is_ddl_mem_sstable(table_type_)) {
    bret = nullptr != t3m_;
  } else {
    // all other sstables
    bret = (meta_handle_.is_valid() ^ (nullptr != allocator_)) || lifetime_guaranteed_by_tablet_;
  }
  return bret;
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
        } else if (nullptr != allocator_) {
          table_->~ObITable();
          allocator_->free(table_);
        } else if (OB_UNLIKELY(!ObITable::is_sstable(table_type_))) {
          LOG_ERROR_RET(OB_ERR_UNEXPECTED, "possible table leak!!!", K(ref_cnt), KPC(table_), K(table_type_));
        }
      } else if (OB_UNLIKELY(ref_cnt < 0 && !ObITable::is_sstable(table_type_))) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "table ref cnt may be leaked", K(ref_cnt), KP(table_), K(table_type_));
      }
    }
  }
  table_ = nullptr;
  t3m_ = nullptr;
  allocator_ = nullptr;
  table_type_ = ObITable::TableType::MAX_TABLE_TYPE;
  meta_handle_.reset();
  lifetime_guaranteed_by_tablet_ = false;
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
  : table_(nullptr),
    t3m_(nullptr),
    allocator_(nullptr),
    meta_handle_(),
    table_type_(ObITable::TableType::MAX_TABLE_TYPE),
    lifetime_guaranteed_by_tablet_(false)
{
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
        meta_handle_ = other.meta_handle_;
        table_type_ = other.table_type_;
        lifetime_guaranteed_by_tablet_ = other.lifetime_guaranteed_by_tablet_;
        if (!ObITable::is_sstable(table_type_) && OB_UNLIKELY(other.table_->get_ref() < 2)) {
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
  } else if (OB_UNLIKELY(ObITable::is_sstable(table_type) && !ObITable::is_ddl_mem_sstable(table_type))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "sstable should not use this interface", K(ret), KP(table), K(table_type));
  } else {
    table_ = table;
    table_->inc_ref();
    t3m_ = t3m;
    allocator_ = nullptr;
    table_type_ = table_type;
  }
  return ret;
}

int ObTableHandleV2::set_sstable(
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
    table_type_ = table->get_key().table_type_;
  }
  return ret;
}

int ObTableHandleV2::set_sstable_with_tablet(ObITable *table)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(table)
      || OB_UNLIKELY(!ObITable::is_sstable(table->get_key().table_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(table));
  } else {
    table_ = table;
    table_->inc_ref();
    lifetime_guaranteed_by_tablet_ = true;
    table_type_ = table->get_key().table_type_;
  }
  return ret;
}

int ObTableHandleV2::set_sstable(ObITable *table, const ObStorageMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(table)
      || OB_UNLIKELY(!ObITable::is_sstable(table->get_key().table_type_))
      || OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(table), K(meta_handle));
  } else {
    table_ = table;
    table_->inc_ref();
    t3m_ = nullptr;
    allocator_ = nullptr;
    meta_handle_ = meta_handle;
    table_type_ = table->get_key().table_type_;
  }
  return ret;
}

ObTablesHandleArray::ObTablesHandleArray()
  : tablet_id_(),
    handles_array_()
{
}

ObTablesHandleArray::~ObTablesHandleArray()
{
  reset();
}

void ObTablesHandleArray::reset()
{
  tablet_id_.reset();
  handles_array_.reset();
}

int ObTablesHandleArray::add_memtable(ObITable *table)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), KP(table));
  } else if (OB_FAIL(tablet_id_check(table->get_key().get_tablet_id()))) {
    LOG_WARN("failed to check tablet id", K(ret), KPC(table));
  } else if (OB_FAIL(handle.set_table(table, MTL(ObTenantMetaMemMgr *), table->get_key().table_type_))) {
    LOG_WARN("failed to set table to handle", K(ret));
  } else if (OB_FAIL(handles_array_.push_back(handle))) {
    LOG_WARN("failed to add table handle", K(ret), K(handle));
  }
  return ret;
}

int ObTablesHandleArray::add_table(const ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_id_check(handle.get_table()->get_key().get_tablet_id()))) {
    LOG_WARN("failed  to add table handle to array", K(ret));
  } else if (OB_FAIL(handles_array_.push_back(handle))) {
    STORAGE_LOG(WARN, "failed to add sstable", K(ret), K(handle));
  }
  return ret;
}

int ObTablesHandleArray::add_sstable(ObITable *table, const ObStorageMetaHandle &meta_handle)
{
  // invalid meta_handle means table lifetime is guaranteed by tablet handle
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  if (OB_UNLIKELY(!table->is_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid table type", K(ret), KPC(table));
  } else if (OB_FAIL(tablet_id_check(table->get_key().get_tablet_id()))) {
    LOG_WARN("failed to check tablet id", K(ret), KPC(table));
  } else if (static_cast<ObSSTable *>(table)->is_loaded()) {
    if (!meta_handle.is_valid()) {
      if (OB_FAIL(table_handle.set_sstable_with_tablet(table))) {
        LOG_WARN("fail to set sstable with tablet", K(ret), KPC(table));
      }
    } else if (OB_FAIL(table_handle.set_sstable(table, meta_handle))) {
      LOG_WARN("fail to set table handle", K(ret), KPC(table));
    }
  } else {
    const ObMetaDiskAddr addr = static_cast<ObSSTable *>(table)->get_addr();
    if (OB_UNLIKELY(!addr.is_valid() || addr.is_none())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(addr));
    } else {
      // FIXME: this reload logic looks weird here, should remove after resolve dependencies
      ObStorageMetaCache &meta_cache = OB_STORE_CACHE.get_storage_meta_cache();
      ObStorageMetaKey meta_key(MTL_ID(), addr);
      ObStorageMetaHandle handle;
      ObSSTable *sstable = nullptr;
      if (OB_FAIL(meta_cache.get_meta(ObStorageMetaValue::MetaType::SSTABLE, meta_key, handle, nullptr))) {
        LOG_WARN("fail to get sstable from meta cache", K(ret), K(addr));
      } else if (OB_FAIL(handle.get_sstable(sstable))) {
        LOG_WARN("fail to get sstable", K(ret), K(handle));
      } else if (OB_FAIL(table_handle.set_sstable(sstable, handle))) {
        LOG_WARN("fail to set table handle", K(ret), KPC(table), KPC(sstable));
      }
    }
  }

  if (FAILEDx(handles_array_.push_back(table_handle))) {
    STORAGE_LOG(WARN, "failed to push back table", K(ret), KPC(table), K(table_handle));
  }
  return ret;
}

int ObTablesHandleArray::assign(const ObTablesHandleArray &other)
{
  int ret = OB_SUCCESS;
  reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < other.get_count(); ++i) {
    if (OB_FAIL(add_table(other.handles_array_.at(i)))) {
      LOG_WARN("fail to add table", K(ret), K(i), K(other));
    }
  }
  return ret;
}

int ObTablesHandleArray::get_table(const int64_t idx, ObTableHandleV2 &table_handle) const
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  if (OB_UNLIKELY(idx >= handles_array_.count() || idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), K(handles_array_.count()));
  } else {
    const ObTableHandleV2 &handle = handles_array_.at(idx);
    if (OB_UNLIKELY(!handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid table handle", K(ret), K(idx), K(handle));
    } else {
      table_handle = handle;
    }
  }
  return ret;
}

int ObTablesHandleArray::get_table(const ObITable::TableKey &table_key, ObTableHandleV2 &table_handle) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < handles_array_.count(); ++i) {
    const ObITable *table = nullptr;
    if (OB_ISNULL(table = handles_array_.at(i).get_table())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null table pointer");
    } else if (table->get_key() == table_key) {
      found = true;
      if (OB_FAIL(get_table(i, table_handle))) {
        STORAGE_LOG(WARN, "failed to get table by index", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObTablesHandleArray::get_tables(common::ObIArray<ObITable *> &tables) const
{
  int ret = OB_SUCCESS;
  tables.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < handles_array_.count(); ++i) {
    if (OB_UNLIKELY(!handles_array_.at(i).is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid table handle", K(ret), K(i), K_(handles_array));
    } else if (OB_FAIL(tables.push_back(handles_array_.at(i).table_))) {
      STORAGE_LOG(WARN, "failed to add table", K(ret));
    }
  }
  return ret;
}

int ObTablesHandleArray::get_first_memtable(memtable::ObIMemtable *&memtable) const
{
  int ret = OB_SUCCESS;
  memtable = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < handles_array_.count(); ++i) {
    if (handles_array_.at(i).get_table()->is_memtable()) {
      memtable = static_cast<memtable::ObIMemtable*>(handles_array_.at(i).table_);
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

  for (int64_t i = 0; OB_SUCC(ret) && i < handles_array_.count(); ++i) {
    ObITable *table = handles_array_.at(i).table_;
    if (table->is_minor_sstable() && OB_FAIL(tables.push_back(table))) {
      STORAGE_LOG(WARN, "failed to add minor sstable", K(ret), K(i));
    }
  }
  return ret;
}

int ObTablesHandleArray::check_continues(const share::ObScnRange *scn_range) const
{
  int ret = OB_SUCCESS;

  if (!handles_array_.empty()) {
    // 1:check major sstable
    // there can only be one major or meta merge
    const ObITable *last_table = nullptr;
    const ObITable *table = nullptr;
    SCN base_end_scn = SCN::min_scn();
    int64_t i = 0;
    if (OB_ISNULL(table = handles_array_.at(i).get_table())) {
      ret = OB_ERR_SYS;
      LOG_WARN("table is NULL", KPC(table));
    } else if (table->is_major_sstable() || table->is_meta_major_sstable()) {
      base_end_scn = table->is_meta_major_sstable() ? table->get_end_scn() : SCN::min_scn();
      i++;
    }
    // 2:check minor sstable
    for ( ; OB_SUCC(ret) && i < handles_array_.count(); ++i) {
      table = handles_array_.at(i).get_table();
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

int ObTablesHandleArray::tablet_id_check(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (0 == handles_array_.count()) {
    tablet_id_ = tablet_id;
  } else if (OB_UNLIKELY(tablet_id != tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet id check in handle array failed", K(ret), K(tablet_id), K_(tablet_id));
  }
  return ret;
}

int64_t ObTablesHandleArray::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV("tablet_id", tablet_id_);
    J_COMMA();
    J_KV("table_count", handles_array_.count());
    J_COMMA();
    J_ARRAY_START();
    for (int64_t i = 0; i < handles_array_.count(); ++i) {
      const ObITable *table = handles_array_.at(i).get_table();
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
