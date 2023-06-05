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

#include "ob_table_store_util.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/container/ob_array_iterator.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "observer/ob_server_struct.h"
#include "share/scn.h"


using namespace oceanbase;
using namespace share;
using namespace common;
using namespace storage;
using namespace blocksstable;

ObITableArray::ObITableArray()
  : meta_mem_mgr_(nullptr),
    array_(nullptr),
    allocator_(nullptr),
    count_(0),
    is_inited_(false)
{
}

ObITableArray::~ObITableArray()
{
  destroy();
}

void ObITableArray::destroy()
{
  is_inited_ = false;
  if (OB_NOT_NULL(array_)) {
    for (int64_t i = 0; i < count_; ++i) {
      reset_table(i);
    }
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(array_);
    }
    array_ = nullptr;
  }
  count_ = 0;
  if (OB_NOT_NULL(allocator_)) {
    allocator_ = nullptr;
  }
}

void ObITableArray::reset_table(const int64_t pos)
{
  if (pos < 0 || pos >= count_) { // do nothing
  } else if (OB_ISNULL(array_[pos])) {
  // maybe called by thread without tenant, such as iocallback, comment until remove tablet_handle from iocallback
  // } else if (OB_UNLIKELY(meta_mem_mgr_ != MTL(ObTenantMetaMemMgr *) || nullptr == meta_mem_mgr_)) {
  } else if (OB_ISNULL(meta_mem_mgr_) || OB_ISNULL(allocator_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "[MEMORY LEAK] TenantMetaMemMgr is unexpected not equal!!!", K(meta_mem_mgr_), KP(allocator_), KPC(array_[pos]));
  } else if (0 == array_[pos]->dec_ref()) {
    if (meta_mem_mgr_->is_used_obj_pool(allocator_)) {
      if (OB_UNLIKELY(OB_INVALID_TENANT_ID == MTL_ID()
          && array_[pos]->is_sstable()
          && reinterpret_cast<ObSSTable*>(array_[pos])->is_small_sstable())) {
        FLOG_INFO("this thread doesn't have MTL ctx, push sstable into gc queue", KP(array_[pos]), K(array_[pos]->get_key()));
        meta_mem_mgr_->push_table_into_gc_queue(array_[pos], array_[pos]->get_key().table_type_);
      } else if (array_[pos]->is_sstable() && !array_[pos]->is_ddl_mem_sstable()) {
        meta_mem_mgr_->gc_sstable(reinterpret_cast<ObSSTable*>(array_[pos]));
      } else {
        meta_mem_mgr_->push_table_into_gc_queue(array_[pos], array_[pos]->get_key().table_type_);
      }
    } else {
      array_[pos]->~ObITable();
      allocator_->free(array_[pos]);
    }
    array_[pos] = nullptr;
  } else {
    array_[pos] = nullptr;
  }
}

int ObITableArray::init(ObIAllocator &allocator, const int64_t count)
{
  int ret = OB_SUCCESS;
  ObITable **buf = nullptr;
  ObTenantMetaMemMgr *meta_mem_mgr = MTL(ObTenantMetaMemMgr *);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObITableArray cannot init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(0 >= count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(count));
  } else if (OB_ISNULL(buf = static_cast<ObITable**>(allocator.alloc(sizeof(ObITable*) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for table pointer", K(ret), K(count));
  } else if (OB_ISNULL(meta_mem_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get meta mem mgr from MTL", K(ret));
  } else {
    MEMSET(buf, 0, sizeof(ObITable*) * count);
    meta_mem_mgr_ =  meta_mem_mgr;
    allocator_ = &allocator;
    array_ = buf;
    count_ = count;
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_) && OB_NOT_NULL(buf)) {
    allocator.free(buf);
    array_ = nullptr;
  }
  return ret;
}

int ObITableArray::copy(
    ObIAllocator &allocator,
    const ObITableArray &other,
    const bool allow_empty_table)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(allocator, other.count_))) {
    LOG_WARN("failed to init ObITableArray for copying", K(ret), K(other));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_ISNULL(other[i])) {
        if (!allow_empty_table) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table must be not null", K(ret), K(i), K(other));
        }
      } else if (OB_FAIL(assign(i, other[i]))) {
        LOG_WARN("failed to add table", K(ret), K(i), K(other[i]));
      }
    }
    if (OB_FAIL(ret)) {
      destroy();
    }
  }
  return ret;
}

int ObITableArray::init_and_copy(
    ObIAllocator &allocator,
    const ObIArray<ObITable *> &tables,
    const int64_t start_pos)
{
  int ret = OB_SUCCESS;
  if (tables.empty() || start_pos < 0 || start_pos >= tables.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tables), K(MAX_SSTABLE_CNT_IN_STORAGE));
  } else if (OB_FAIL(init(allocator, tables.count() - start_pos))) {
    LOG_WARN("failed to init ObITableArray", K(ret), K(*this));
  } else {
    ObITable *table = nullptr;
    for (int64_t i = start_pos; OB_SUCC(ret) && i < tables.count(); ++i) {
      if (OB_ISNULL(table = tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must not null", K(ret), K(tables));
      } else if (OB_FAIL(assign(i - start_pos, table))) {
        LOG_WARN("failed to add table to tables", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      destroy();
    }
  }
  return ret;
}

int ObITableArray::assign(const int64_t pos, ObITable * const table)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *meta_mem_mgr = MTL(ObTenantMetaMemMgr *);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITableArray not inited", K(ret), K(*this));
  } else if (OB_UNLIKELY(pos < 0 || pos >= count_ || NULL == table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(pos), KP(table), K(*this));
  } else if (meta_mem_mgr_ != meta_mem_mgr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta mem mgr is unexpected not equal!", K(ret), K(meta_mem_mgr_), K(meta_mem_mgr));
  } else {
    array_[pos] = table;
    table->inc_ref();
  }
  return ret;
}

int ObITableArray::get_all_tables(common::ObIArray<ObITable *> &tables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
  } else if (0 >= count_) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_ISNULL(array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must not be null", K(ret), K(i), K(*this));
      } else if (OB_FAIL(tables.push_back(array_[i]))) {
        LOG_WARN("fail to push back", K(ret), K(i));
      }
    }
  }
  return ret;
}

ObITable *ObITableArray::get_boundary_table(const bool last) const
{
  ObITable *table = nullptr;
  if (IS_NOT_INIT) {
    // not inited, just return nullptr
  } else if (0 >= count_) {
    // no table, just return nullptr.
  } else if (last) {
    table = array_[count_ - 1];
  } else {
    table = array_[0];
  }
  return table;
}

int ObITableArray::get_table(const ObITable::TableKey &table_key, ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (IS_NOT_INIT) {
  } else if (0 >= count_) {
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key));
  } else {
    ObITable *table = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_ISNULL(array_[i])) {
      } else if (table_key == array_[i]->get_key()) {
        table = array_[i];
        break;
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(table)) {
      if (OB_FAIL(handle.set_table(table, meta_mem_mgr_, table->get_key().table_type_))) {
        LOG_WARN("failed to set table handle", K(ret));
      }
    }
  }
  return ret;
}

int ObITableArray::get_all_tables(storage::ObTablesHandleArray &tables) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
  } else if (0 >= count_) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_ISNULL(array_[i])) {
      } else if (OB_FAIL(tables.add_table(array_[i]))) {
        LOG_WARN("failed to add table to handle array", K(ret));
      }
    }
  }
  return ret;
}


int ObSSTableArray::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, count_))) {
    LOG_WARN("failed to encode count", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_ISNULL(array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table in array_ must not be null", K(ret), K(i));
      } else if (OB_FAIL(array_[i]->serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize table", K(ret), KPC(array_[i]));
      }
    }
  }
  return ret;
}

int64_t ObSSTableArray::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(count_);
  for (int64_t i = 0; i < count_; ++i) {
    len += array_[i]->get_serialize_size();
  }
  return len;
}

int ObSSTableArray::deserialize(
    ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;

  if (OB_UNLIKELY(NULL == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments.", KP(buf),K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &count))) {
    LOG_WARN("failed to decode count_", K(ret));
  } else if (0 >= count) {
  } else if (OB_FAIL(init(allocator, count))) {
    LOG_WARN("failed to init ObSSTableArray", K(ret), K(*this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count && pos < data_len; ++i) {
      ObTableHandleV2 table_handle;
      ObSSTable *sstable = nullptr;
      if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->acquire_sstable(table_handle, allocator))) {
        LOG_WARN("failed to acquire sstable from meta_mem_mgr", K(ret));
      } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(table_handle.get_table()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, sstable is nullptr", K(ret), K(table_handle));
      } else if (OB_FAIL(sstable->deserialize(allocator, buf, data_len, pos))) {
        LOG_WARN("failed to deserialize sstable", K(ret));
      } else if (OB_FAIL(assign(i, table_handle.get_table()))) {
        LOG_WARN("failed to assign table to ObSSTableArray", K(ret), K(i));
      }
    }
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObSSTableArray::get_min_schema_version(int64_t &min_schema_version) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableArray not inited", K(ret), K(*this));
  } else {
    min_schema_version = INT64_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      if (OB_ISNULL(array_[i])) {
        ret = OB_ERR_SYS;
        LOG_ERROR("unexpected null sstable", K(ret), K(i), K(*this));
      } else {
        min_schema_version = MIN(min_schema_version, static_cast<ObSSTable*>(array_[i])->get_meta().get_basic_meta().schema_version_);
      }
    }
  }
  return ret;
}


int ObExtendTableArray::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExtendTableArray not inited, cannot serialize", K(ret));
  } else if (OB_UNLIKELY(NULL == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(buf_len), K(pos), K(ret));
  } else {
    int64_t table_count = 0;
    for (int64_t i = 0; i < count_; ++i) {
      if (OB_NOT_NULL(array_[i])) {
        ++table_count;
      }
    }
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, table_count))) {
      LOG_WARN("failed to serialize table count", K(ret));
    } else if (0 == table_count) { // no extend tables
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
        if (OB_ISNULL(array_[i])) {
        } else if (OB_FAIL(array_[i]->serialize(buf, buf_len, pos))) {
          LOG_WARN("failed to serialize table", K(ret), KPC(array_[i]));
        }
      }
    }
  }
  return ret;
}

int64_t ObExtendTableArray::get_serialize_size() const
{
  int64_t len = 0;
  int64_t count = 0;
  for (int64_t i = 0; i < count_; ++i) {
    if (OB_NOT_NULL(array_[i])) {
      ++count;
    }
  }
  len += serialization::encoded_length_i64(count);
  for (int64_t i = 0; i < count_; ++i) {
    if (OB_NOT_NULL(array_[i])) {
      len += array_[i]->get_serialize_size();
    }
  }
  return len;
}

int ObExtendTableArray::deserialize(
    ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;

  if (OB_UNLIKELY(count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExtendTableArray should alloc memory for array before deserializing", K(ret), K(*this));
  } else if (OB_UNLIKELY(NULL == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments.", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &count))) {
    LOG_WARN("failed to decode count_", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count && pos < data_len; ++i) {
      ObTableHandleV2 table_handle;
      ObSSTable *sstable = nullptr;
      if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->acquire_sstable(table_handle, allocator))) {
        LOG_WARN("failed to acquire sstable from meta_mem_mgr", K(ret));
      } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(table_handle.get_table()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, sstable is nullptr", K(ret), K(table_handle));
      } else if (OB_FAIL(sstable->deserialize(allocator, buf, data_len, pos))) {
        LOG_WARN("failed to deserialize sstable", K(ret));
      } else if (table_handle.get_table()->is_meta_major_sstable()) {
        if (OB_FAIL(assign(ObTabletTableStore::META_MAJOR, table_handle.get_table()))) {
          LOG_WARN("failed to add meta major table", K(ret));
        }
      }
    }
  }
  return ret;
}


ObMemtableArray::ObMemtableArray()
  : meta_mem_mgr_(nullptr),
    allocator_(nullptr),
    array_(nullptr),
    is_inited_(false)
{
}

ObMemtableArray::~ObMemtableArray()
{
  destroy();
}

void ObMemtableArray::destroy()
{
  if (OB_ISNULL(allocator_) || OB_ISNULL(array_)) {
    // do nothing
  } else {
    ObITable *table = nullptr;
    for (int64_t i = 0; i < count(); ++i) {
      if (OB_NOT_NULL(table = array_->at(i))) {
        reset_table(i);
      }
    }
    array_->~ObSEArray();
    allocator_->free(array_);
  }

  meta_mem_mgr_ = nullptr;
  allocator_ = nullptr;
  array_ = nullptr;
  is_inited_ = false;
}

int ObMemtableArray::init(common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const int64_t alloc_size = sizeof(ObSEArray<ObITable*, DEFAULT_TABLE_CNT, common::ObIAllocator&>);

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMemtableArray has been inited", K(ret), KPC(this));
  } else if (OB_UNLIKELY(NULL == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(allocator));
  } else if (OB_ISNULL(meta_mem_mgr_ = MTL(ObTenantMetaMemMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get meta mem mgr from MTL", K(ret));
  } else if (FALSE_IT(allocator_ = allocator)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate mem for buf", K(ret));
  } else {
    array_ = new (buf) ObSEArray<ObITable *, DEFAULT_TABLE_CNT, common::ObIAllocator&>(OB_MALLOC_NORMAL_BLOCK_SIZE, *allocator_);
    is_inited_ = true;
  }
  return ret;
}

int ObMemtableArray::init(
    common::ObIAllocator *allocator,
    const ObMemtableArray &other)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMemtableArray has been inited", K(ret), KPC(this));
  } else if (OB_FAIL(init(allocator))) {
    LOG_WARN("failed to init ObMemtableArray", K(ret));
  } else if (meta_mem_mgr_ != other.meta_mem_mgr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta mem mgr must be same", K(ret), K(meta_mem_mgr_), K(other.meta_mem_mgr_));
  }

  ObITable *table = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.count(); ++i) {
    table = other.get_table(i);
    if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
    } else if (OB_FAIL(add_table(table))) {
      LOG_WARN("failed to add to memtables", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObMemtableArray::build(
    common::ObIArray<ObTableHandleV2> &handle_array,
    const int64_t start_pos)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMemtableArray not inited", K(ret), KPC(this));
  } else if (start_pos < 0 || start_pos >= handle_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(start_pos), K(handle_array));
  }

  ObITable *table = nullptr;
  for (int64_t i = start_pos; OB_SUCC(ret) && i < handle_array.count(); ++i) {
    memtable::ObMemtable *memtable = nullptr;
    table = handle_array.at(i).get_table();
    if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
    } else if (FALSE_IT(memtable = reinterpret_cast<memtable::ObMemtable *>(table))) {
    } else if (memtable->is_empty()) {
      FLOG_INFO("Empty memtable discarded", KPC(memtable));
    } else if (OB_FAIL(add_table(table))) {
      LOG_WARN("failed to add to memtables", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObMemtableArray::rebuild(common::ObIArray<ObTableHandleV2> &handle_array)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObMemtableArray not inited", K(ret), KPC(this), K(handle_array));
  } else {
    ObITable *last_memtable = get_table(count() - 1);
    SCN end_scn = (NULL == last_memtable) ? SCN::min_scn() : last_memtable->get_end_scn();

    for (int64_t i = 0; OB_SUCC(ret) && i < handle_array.count(); ++i) {
      memtable::ObMemtable *memtable = nullptr;
      ObITable *table = handle_array.at(i).get_table();
      if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
      } else if (FALSE_IT(memtable = static_cast<memtable::ObMemtable *>(table))) {
      } else if (memtable->is_empty()) {
        FLOG_INFO("Empty memtable discarded", KPC(memtable));
      } else if (table->get_end_scn() < end_scn) {
      } else if (table->get_end_scn() == end_scn && table == last_memtable) { //fix issue 41996395
      } else if (OB_FAIL(add_table(table))) {
        LOG_WARN("failed to add memtable to curr memtables", K(ret), KPC(this));
      }
    }
  }
  return ret;
}

int ObMemtableArray::rebuild(
    const share::SCN &clog_checkpoint_scn,
    common::ObIArray<ObTableHandleV2> &handle_array)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObMemtableArray not inited", K(ret), KPC(this), K(handle_array));
  } else {
    ObITable *last_memtable = get_table(count() - 1);

    if (NULL == last_memtable) {
      // use clog checkpoint scn to filter memtable handle array
      for (int64_t i = 0; OB_SUCC(ret) && i < handle_array.count(); ++i) {
        memtable::ObMemtable *memtable = nullptr;
        ObITable *table = handle_array.at(i).get_table();
        if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
        } else if (FALSE_IT(memtable = static_cast<memtable::ObMemtable *>(table))) {
        } else if (memtable->is_empty()) {
          FLOG_INFO("Empty memtable discarded", K(ret), KPC(memtable));
        } else if (table->get_end_scn() <= clog_checkpoint_scn) {
          FLOG_INFO("memtable end scn no greater than clog checkpoint scn, should be discarded", K(ret),
              "end_scn", table->get_end_scn(), K(clog_checkpoint_scn));
        } else if (OB_FAIL(add_table(table))) {
          LOG_WARN("failed to add memtable to curr memtables", K(ret), KPC(this));
        }
      }
    } else {
      int64_t pos = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < handle_array.count(); ++i) {
        memtable::ObMemtable *memtable = nullptr;
        ObITable *table = handle_array.at(i).get_table();
        if (OB_UNLIKELY(nullptr == table || !table->is_memtable())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table must be memtable", K(ret), K(i), KPC(table));
        } else if (FALSE_IT(memtable = static_cast<memtable::ObMemtable *>(table))) {
        } else if (-1 == pos && memtable == last_memtable) {
          pos = i;
        }

        if (OB_FAIL(ret)) {
        } else if (memtable->is_empty()) {
          FLOG_INFO("Empty memtable discarded", K(ret), KPC(memtable));
        } else if (-1 != pos && i > pos && OB_FAIL(add_table(table))) {
          LOG_WARN("failed to add memtable to curr memtables", K(ret), KPC(this));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(-1 == pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("last memtable cannot be found on memtable mgr", K(ret), KPC(last_memtable), K(handle_array));
      }
    }
  }
  return ret;
}

int ObMemtableArray::prepare_allocate()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMemtableArray not inited", K(ret), KPC(this));
  } else {
    int64_t cur_capacity = array_->get_capacity();
    int64_t cur_count = array_->count();
    if (cur_capacity > cur_count) {
    } else if (OB_FAIL(array_->reserve(cur_capacity * 2))) {
      LOG_WARN("failed to realloc memory for ObMemtableArray", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObMemtableArray::find(
    const ObITable::TableKey &table_key,
    ObTableHandleV2 &handle) const
{
  int ret = OB_SUCCESS;
  handle.reset();

  if (empty()) {
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(table_key));
  }

  ObITable *table = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < count(); ++i) {
    if (OB_ISNULL(get_table(i))) {
    } else if (table_key == get_table(i)->get_key()) {
      table = get_table(i);
      break;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(table)) {
    if (OB_FAIL(handle.set_table(table, meta_mem_mgr_, table->get_key().table_type_))) {
      LOG_WARN("failed to set table handle", K(ret));
    }
  }
  return ret;
}

int ObMemtableArray::find(
    const SCN &start_scn,
    const int64_t base_version,
    ObITable *&table,
    int64_t &mem_pos) const
{
  int ret = OB_SUCCESS;
  mem_pos = -1;
  table = nullptr;

  if (OB_UNLIKELY(empty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no memtable", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!start_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(start_scn), K(base_version));
  } else if (SCN::min_scn() == start_scn) {
    mem_pos = 0;
    table = get_table(0);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count(); ++i) {
      ObITable * memtable = get_table(i);
      if (OB_ISNULL(memtable)) {
        ret = OB_ERR_SYS;
        LOG_WARN("table must not null", K(ret), KPC(memtable), KPC(this));
      } else if (memtable->get_end_scn() == start_scn) {
        if (static_cast<memtable::ObIMemtable *>(memtable)->get_snapshot_version() > base_version) {
          mem_pos = i;
          table = memtable;
          break;
        }
      } else if (memtable->get_end_scn() > start_scn) {
        mem_pos = i;
        table = memtable;
        break;
      }
    }
  }
  return ret;
}

int ObMemtableArray::add_table(ObITable * const table)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *meta_mem_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMemtableArray not inited", K(ret), KPC(this));
  } else if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(table));
  } else if (FALSE_IT(meta_mem_mgr = MTL(ObTenantMetaMemMgr *))) {
  } else if (meta_mem_mgr_ != meta_mem_mgr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta mem mgr is unexpected not equal!", K(ret), K(meta_mem_mgr_), K(meta_mem_mgr));
  } else if (OB_FAIL(array_->push_back(table))) {
    LOG_WARN("failed to add table", K(ret));
  } else {
    table->inc_ref();
  }
  return ret;
}

void ObMemtableArray::reset_table(const int64_t pos)
{
  ObITable *table = nullptr;

  if (pos < 0 || pos >= count()) { // do nothing
  } else if (OB_ISNULL(table = get_table(pos))) {
  } else if (OB_ISNULL(meta_mem_mgr_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "[MEMORY LEAK] TenantMetaMemMgr is unexpected not equal!!!", K(meta_mem_mgr_), KPC(table));
  } else if (0 == table->dec_ref()) {
    meta_mem_mgr_->push_table_into_gc_queue(table, table->get_key().table_type_);
  }
}


/* ObTableStoreIterator Section */
ObTableStoreIterator::ObTableStoreIterator(const bool reverse_iter)
  : array_(),
    pos_(INT64_MAX),
    memstore_retired_(nullptr)
{
  step_ = reverse_iter ? -1 : 1;
}

ObTableStoreIterator::~ObTableStoreIterator()
{
  reset();
}

void ObTableStoreIterator::reset()
{
  array_.reset();
  pos_ = INT64_MAX;
  memstore_retired_ = nullptr;
}

void ObTableStoreIterator::resume()
{
  pos_ = step_ < 0 ? array_.count() - 1 : 0;
}

ObITable *ObTableStoreIterator::get_boundary_table(const bool is_last)
{
  ObITable *table = nullptr;
  if (!is_valid()) {
  } else if (is_last) {
    table = step_ > 0 ? array_.at(array_.count() - 1) : array_.at(0);
  } else {
    table = step_ < 0 ? array_.at(array_.count() - 1) : array_.at(0);
  }
  return table;
}

int ObTableStoreIterator::copy(const ObTableStoreIterator &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(other));
  } else if (OB_FAIL(array_.assign(other.array_))) {
    LOG_WARN("failed to copy array", K(ret));
  } else {
    memstore_retired_ = other.memstore_retired_;
  }
  return ret;
}

int ObTableStoreIterator::add_tables(ObMemtableArray &array, const int64_t start_pos)
{
  int ret = OB_SUCCESS;
  if (start_pos < 0 || start_pos >= array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(start_pos));
  }

  for (int64_t i = start_pos; OB_SUCC(ret) && i < array.count(); ++i) {
    ObITable *cur = array[i];
    if (OB_ISNULL(cur)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), K(i), K(cur));
    } else if (OB_FAIL(array_.push_back(cur))) {
      LOG_WARN("failed to add table to iterator", K(ret));
    }
  }
  return ret;
}

int ObTableStoreIterator::add_tables(ObITable **start, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(start) || count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(start), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObITable **cur = start + i;
      if (OB_ISNULL(*cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret), K(start), K(i), K(cur), K(count));
      } else if (OB_FAIL(array_.push_back(*cur))) {
        LOG_WARN("failed to add table to iterator", K(ret));
      }
    }
  }
  return ret;
}

int ObTableStoreIterator::add_table(ObITable *input_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_table));
  } else if (OB_FAIL(array_.push_back(input_table))) {
    LOG_WARN("failed to add table to iterator", K(ret), KP(input_table));
  }
  return ret;
}

int ObTableStoreIterator::get_next(ObITable *&table)
{
  int ret = OB_SUCCESS;
  table = nullptr;
  if (0 == array_.count()) {
    ret = OB_ITER_END;
  } else if (INT64_MAX == pos_) {
    pos_ = (-1 == step_) ? array_.count() - 1 : 0;
  } else if (pos_ >= array_.count() || pos_ < 0) {
    ret = OB_ITER_END;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(array_.at(pos_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter unexpected null table", K(ret), K(*this));
  } else {
    table = array_.at(pos_);
    pos_ += step_;
  }
  return ret;
}

int ObTableStoreIterator::set_retire_check()
{
  int ret = OB_SUCCESS;
  memstore_retired_ = nullptr;
  ObITable *first_memtable = nullptr;

  for (int64_t i = array_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_ISNULL(array_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must not null", K(ret), K(*this));
    } else if (array_.at(i)->is_data_memtable()) {
      first_memtable = array_.at(i);
    } else {
      break;
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(first_memtable)) {
    memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(first_memtable);
    memstore_retired_ = &memtable->get_read_barrier();
  }
  return ret;
}

/* ObPrintTableStoreIterator Section */
ObPrintTableStoreIterator::ObPrintTableStoreIterator(const ObTableStoreIterator &iter)
  : array_(iter.array_),
    pos_(iter.pos_),
    step_(iter.step_),
    memstore_retired_(iter.memstore_retired_)
{
}

ObPrintTableStoreIterator::~ObPrintTableStoreIterator()
{
}

int64_t ObPrintTableStoreIterator::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_NAME("ObTableStoreIterator_Pretty");
    J_COLON();
    J_KV(KP(this), K_(array), K_(pos), K_(step), K_(memstore_retired));
    J_COMMA();
    BUF_PRINTF("iter array");
    J_COLON();
    J_OBJ_START();
    if (array_.count() > 0) {
      ObCurTraceId::TraceId *trace_id = ObCurTraceId::get_trace_id();
      J_NEWLINE();
      BUF_PRINTF("[%ld] [ ", GETTID());
      BUF_PRINTO(PC(trace_id));
      BUF_PRINTF(" ] ");
      BUF_PRINTF(" %-10s %-19s %-19s %-19s %-19s %-19s %-4s %-16s \n",
          "table_type", "table_addr", "upper_trans_ver", "max_merge_ver",
          "start_scn", "end_scn", "ref", "uncommit_row");
      for (int64_t i = 0; i < array_.count(); ++i) {
        if (i > 0) {
          J_NEWLINE();
        }
        ObITable *table = array_.at(i);
        table_to_string(table, buf, buf_len, pos);
      }
    } else {
      J_EMPTY_OBJ();
    }
    J_OBJ_END();
    J_OBJ_END();
  }
  return pos;
}

void ObPrintTableStoreIterator::table_to_string(
    ObITable *table,
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

    BUF_PRINTF(" %-10s %-19p %-19lu %-19lu %-19s %-19s %-4ld %-16s ",
      table_name,
      reinterpret_cast<const void *>(table),
      table->get_upper_trans_version(),
      table->get_max_merged_trans_version(),
      to_cstring(table->get_start_scn()),
      to_cstring(table->get_end_scn()),
      table->get_ref(),
      uncommit_row);
  } else {
    BUF_PRINTF(" %-10s %-19s %-10s %-19s %-19s %-19s %-19s %-4s %-16s \n",
        "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL");
  }
}


/* ObTableStoreUtil Section */
bool ObTableStoreUtil::ObITableLogTsRangeCompare::operator()(
     const ObITable *ltable, const ObITable *rtable) const
{
  bool bret = false;
  if (OB_SUCCESS != result_code_) {
  } else if (OB_SUCCESS != (result_code_ = compare_table_by_scn_range(ltable, rtable, bret))) {
    LOG_WARN_RET(result_code_, "failed to compare table with LogTsRange", K(result_code_), KPC(ltable), KPC(rtable));
  }
  return bret;
}

bool ObTableStoreUtil::ObITableSnapshotVersionCompare::operator()(
     const ObITable *ltable, const ObITable *rtable) const
{
  bool bret = false;
  if (OB_SUCCESS != result_code_) {
  } else if (OB_SUCCESS != (result_code_ = compare_table_by_snapshot_version(ltable, rtable, bret))) {
    LOG_WARN_RET(result_code_, "failed to compare table with SnapshotVersion", K(result_code_), KPC(ltable), KPC(rtable));
  }
  return bret;
}

bool ObTableStoreUtil::ObTableHandleV2LogTsRangeCompare::operator()(
     const ObTableHandleV2 &lhandle, const ObTableHandleV2 &rhandle) const
{
  bool bret = false;
  if (OB_SUCCESS != result_code_) {
  } else {
    const ObITable *ltable = lhandle.get_table();
    const ObITable *rtable = rhandle.get_table();
    if (OB_SUCCESS != (result_code_ = compare_table_by_scn_range(ltable, rtable, bret))) {
      LOG_WARN_RET(result_code_, "failed to compare table with LogTsRange", K(result_code_), KPC(ltable), KPC(rtable));
    }
  }
  return bret;
}


bool ObTableStoreUtil::ObTableHandleV2SnapshotVersionCompare::operator()(
     const ObTableHandleV2 &lhandle, const ObTableHandleV2 &rhandle) const
{
  bool bret = false;

  if (OB_SUCCESS != result_code_) {
  } else {
    const ObITable *ltable = lhandle.get_table();
    const ObITable *rtable = rhandle.get_table();
    if (OB_SUCCESS != (result_code_ = compare_table_by_snapshot_version(ltable, rtable, bret))) {
      LOG_WARN_RET(result_code_, "failed to compare table with SnapshotVersion", K(result_code_), KPC(ltable), KPC(rtable));
    }
  }
  return bret;
}


int ObTableStoreUtil::compare_table_by_scn_range(const ObITable *ltable, const ObITable *rtable, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  if (NULL == ltable) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("left store must not null", K(ret));
  } else if (NULL == rtable) {
    bret = true;
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("right store must not null", K(ret));
  } else if (ltable->is_major_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ltable type", K(ret), KPC(ltable));
  } else if (rtable->is_major_sstable()) {
    bret = true;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid rtable type", K(ret), KPC(rtable));
  } else if (ltable->get_end_scn() == rtable->get_end_scn()) {
    bret = true;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("table end log ts shouldn't be equal", KPC(ltable), KPC(rtable));
  } else { // log ts not equal
    bret = ltable->get_end_scn() < rtable->get_end_scn();
  }
  return ret;
}

int ObTableStoreUtil::compare_table_by_snapshot_version(const ObITable *ltable, const ObITable *rtable, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  if (OB_ISNULL(ltable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("left store must not null", K(ret));
  } else if (OB_ISNULL(rtable)) {
    bret = true;
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("right store must not null", K(ret));
  } else if (OB_UNLIKELY(!ltable->is_major_sstable() || !rtable->is_major_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("left/right store must be major", K(ret));
  } else {
    bret = ltable->get_snapshot_version() < rtable->get_snapshot_version();
  }
  return ret;
}

int ObTableStoreUtil::sort_major_tables(ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> &tables)
{
  int ret = OB_SUCCESS;

  if (tables.empty()) {
    // no need sort
  } else {
    ObITableSnapshotVersionCompare comp(ret);
    std::sort(tables.begin(), tables.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort tables", K(ret), K(tables));
    }
  }
  return ret;
}

int ObTableStoreUtil::sort_minor_tables(ObArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;

  if (tables.empty()) {
    // no need sort
  } else {
    // There is an assumption: either all tables are with scn range, or none
    ObITableLogTsRangeCompare comp(ret);
    std::sort(tables.begin(), tables.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort tables", K(ret), K(tables));
    }
  }
  return ret;
}

bool ObTableStoreUtil::check_include_by_scn_range(const ObITable &a, const ObITable &b)
{
  bool bret = false;
  if (a.get_end_scn() >= b.get_end_scn() && a.get_start_scn() <= b.get_start_scn()) {
    bret = true;
  }
  return bret;
}

bool ObTableStoreUtil::check_intersect_by_scn_range(const ObITable &a, const ObITable &b)
{
  bool bret = false;
  if (!(a.get_end_scn() <= b.get_start_scn()
        || a.get_start_scn() >= b.get_end_scn())) {
    bret = true;
  }
  return bret;
}
