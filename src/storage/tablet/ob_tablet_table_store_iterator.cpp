/**
 * Copyright (c) 2022 OceanBase
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

#include "ob_tablet_table_store_iterator.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace memtable;
namespace storage
{

ObTableStoreIterator::ObTableStoreIterator(const bool reverse, const bool need_load_sstable)
  : need_load_sstable_(need_load_sstable),
    table_store_handle_(),
    sstable_handle_array_(),
    table_ptr_array_(),
    pos_(INT64_MAX),
    memstore_retired_(nullptr),
    transfer_src_table_store_handle_(nullptr),
    split_extra_table_store_handles_(),
    ddl_agg_sstable_handles_(nullptr),
    ddl_co_sstable_handle_(nullptr),
    aggregated_guard_created_(false)
{
  step_ = reverse ? -1 : 1;
  sstable_handle_array_.set_attr(ObMemAttr(MTL_ID(), "TblHdlArray"));
}

int ObTableStoreIterator::assign(const ObTableStoreIterator& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    need_load_sstable_ = other.need_load_sstable_;
    if (other.table_store_handle_.is_valid()) {
      table_store_handle_ = other.table_store_handle_;
    } else if (table_store_handle_.is_valid()) {
      table_store_handle_.reset();
    }

    if (OB_FAIL(ret)) {
    } else if (other.sstable_handle_array_.count() > 0) {
      if (OB_FAIL(sstable_handle_array_.assign(other.sstable_handle_array_))) {
        LOG_WARN("assign sstable handle array fail", K(ret));
      }
    } else if (sstable_handle_array_.count() > 0) {
      sstable_handle_array_.reset();
    }

    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(ddl_agg_sstable_handles_)) {
        ddl_agg_sstable_handles_->reset();
      }
      if (OB_NOT_NULL(other.ddl_agg_sstable_handles_)
          && other.ddl_agg_sstable_handles_->count() > 0) {
        if (OB_ISNULL(ddl_agg_sstable_handles_)
            && OB_ISNULL(ddl_agg_sstable_handles_ = OB_NEW(ObArray<ObTableHandleV2>, ObMemAttr(MTL_ID(), "inc_major_hdl")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", KR(ret));
        } else if (OB_FAIL(ddl_agg_sstable_handles_->assign(*other.ddl_agg_sstable_handles_))) {
          LOG_WARN("fail to assign", KR(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(nullptr != ddl_co_sstable_handle_)) {
        ddl_co_sstable_handle_->reset();
      }
      if (OB_UNLIKELY(nullptr != other.ddl_co_sstable_handle_)) {
        if (nullptr == ddl_co_sstable_handle_
            && OB_ISNULL(ddl_co_sstable_handle_ = OB_NEW(ObTableHandleV2, ObMemAttr(MTL_ID(), "ddl_co_hdl")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          *ddl_co_sstable_handle_ = *other.ddl_co_sstable_handle_;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(table_ptr_array_.assign(other.table_ptr_array_))) {
      LOG_WARN("assign table ptr array fail", K(ret));
    } else {
      pos_ = other.pos_;
      step_ = other.step_;
      memstore_retired_ = other.memstore_retired_;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(nullptr != other.transfer_src_table_store_handle_)) {
      if (nullptr == transfer_src_table_store_handle_) {
        void *meta_hdl_buf = ob_malloc(sizeof(ObStorageMetaHandle), ObMemAttr(MTL_ID(), "TransferMetaH"));
        if (OB_ISNULL(meta_hdl_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocator memory for handle", K(ret));
        } else {
          transfer_src_table_store_handle_ = new (meta_hdl_buf) ObStorageMetaHandle();
        }
      }
      if (OB_SUCC(ret)) {
        *transfer_src_table_store_handle_ = *(other.transfer_src_table_store_handle_);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(split_extra_table_store_handles_.assign(other.split_extra_table_store_handles_))) {
      LOG_WARN("failed to assign split extra table store handles", K(ret));
    }
  }
  return ret;
}

ObTableStoreIterator::~ObTableStoreIterator()
{
  reset();
}

void ObTableStoreIterator::reset()
{
  OB_DELETE(ObTableHandleV2, ObMemAttr(MTL_ID(), "ddl_co_hdl"), ddl_co_sstable_handle_);
  if (OB_NOT_NULL(ddl_agg_sstable_handles_)) {
    ddl_agg_sstable_handles_->reset();
    OB_DELETE(ObArray<ObTableHandleV2>, ObMemAttr(MTL_ID(), "inc_major_hdl"), ddl_agg_sstable_handles_);
  }
  table_ptr_array_.reset();
  sstable_handle_array_.reset();
  table_store_handle_.reset();

  if (nullptr != transfer_src_table_store_handle_) {
    transfer_src_table_store_handle_->~ObStorageMetaHandle();
    ob_free(transfer_src_table_store_handle_);
    transfer_src_table_store_handle_ = nullptr;
  }
  split_extra_table_store_handles_.reset();
  pos_ = INT64_MAX;
  memstore_retired_ = nullptr;
}

void ObTableStoreIterator::resume()
{
  pos_ = step_ < 0 ? table_ptr_array_.count() - 1 : 0;
}

int ObTableStoreIterator::get_next(ObITable *&table)
{
  int ret = OB_SUCCESS;
  table = nullptr;
  if (OB_FAIL(inner_move_idx_to_next())) {
  } else if (OB_FAIL(get_ith_table(pos_, table))) {
    LOG_WARN("fail to get ith table", K(ret), K(pos_));
  } else {
    pos_ += step_;
  }
  return ret;
}

int ObTableStoreIterator::get_next(ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  table_handle.reset();
  ObITable *table = nullptr;
  if (OB_UNLIKELY(nullptr != transfer_src_table_store_handle_ || !split_extra_table_store_handles_.empty())) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("doesn't support cross tablet get table handl", K(ret), KP(transfer_src_table_store_handle_), K(split_extra_table_store_handles_));
  } else if (OB_FAIL(inner_move_idx_to_next())) {
  } else {
    if (OB_FAIL(get_ith_table(pos_, table))) {
      LOG_WARN("fail to get ith table", K(ret), K(pos_));
    } else if (table->is_memtable() || table->is_ddl_mem_sstable()) {
      ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
      if (OB_FAIL(table_handle.set_table(table, t3m, table->get_key().table_type_))) {
        LOG_WARN("failed to set memtable to table handle", K(ret), KPC(table));
      }
    } else if (table->is_sstable()) {
      const int64_t hdl_idx = table_ptr_array_.at(pos_).hdl_idx_;
      const ObStorageMetaHandle &meta_handle = hdl_idx >= 0 ? sstable_handle_array_.at(hdl_idx) : table_store_handle_;
      if (!meta_handle.is_valid()) {
        // table lifetime guaranteed by tablet handle
        if (OB_FAIL(table_handle.set_sstable_with_tablet(table))) {
          LOG_WARN("failed to set sstable on tablet memory", K(ret));
        }
      } else if (OB_FAIL(table_handle.set_sstable(table, meta_handle))) {
        LOG_WARN("failed to set sstable to table handle", K(ret), KPC(table), K(meta_handle));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table type", K(ret), KPC(table));
    }
    if (OB_SUCC(ret)) {
      pos_ += step_;
    }
  }
  return ret;
}

int ObTableStoreIterator::get_boundary_table(const bool is_last, ObITable *&table)
{
  int ret = OB_SUCCESS;
  const int64_t count = table_ptr_array_.count();
  int64_t table_idx = -1;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table store iterator to get boundary table", K(ret), K_(table_ptr_array));
  } else {
    if (is_last) {
      table_idx = step_ > 0 ? (count - 1) : 0;
    } else {
      table_idx = step_ < 0 ? (count - 1) : 0;
    }
    if (OB_FAIL(get_ith_table(table_idx, table))) {
      LOG_WARN("fail to get ith table", K(ret), K(table_idx));
    }
  }
  return ret;
}

ObITable *ObTableStoreIterator::get_last_memtable()
{
  ObITable *table = nullptr;
  const int64_t count = table_ptr_array_.count();
  if (!is_valid()) {
  } else {
    const TablePtr &ptr = step_ > 0 ? table_ptr_array_.at(count - 1) : table_ptr_array_.at(0);
    if (nullptr != ptr.table_ && ptr.table_->is_memtable()) {
      table = ptr.table_;
    }
  }
  return table;
}

int ObTableStoreIterator::set_handle(const ObStorageMetaHandle &table_store_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_store_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table store handle", K(ret), K(table_store_handle));
  } else {
    table_store_handle_ = table_store_handle;
  }
  return ret;
}

int ObTableStoreIterator::alloc_split_extra_table_store_handle(ObStorageMetaHandle *&meta_handle)
{
  int ret = OB_SUCCESS;
  meta_handle = nullptr;
  if (OB_ISNULL(meta_handle = split_extra_table_store_handles_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocator memory for handle", K(ret));
  }
  return ret;
}

int ObTableStoreIterator::add_table(ObITable *table)
{
  int ret = OB_SUCCESS;
  TablePtr table_ptr;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table ptr", K(ret), KP(table));
  } else if (FALSE_IT(table_ptr.table_ = table)) {
  } else if (table->is_memtable()) {
    // lifetime guaranteed by tablet_handle_
  } else if (static_cast<ObSSTable *>(table)->is_loaded() || !need_load_sstable_ || aggregated_guard_created_) {
    // lifetime guaranteed by table_store_handle_
  } else if (OB_FAIL(get_table_ptr_with_meta_handle(static_cast<ObSSTable *>(table), table_ptr))) {
    LOG_WARN("fail to get table ptr with meta handle", K(ret), KPC(table));
  }

  if (FAILEDx(table_ptr_array_.push_back(table_ptr))) {
    LOG_WARN("fail to push table handle into array", K(ret));
  }
  return ret;
}

int ObTableStoreIterator::add_ddl_agg_table(ObTableHandleV2 &ddl_agg_sstable_handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *ddl_agg_sstable = nullptr;
  if (OB_UNLIKELY(!ddl_agg_sstable_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable handle is invalid", KR(ret), K(ddl_agg_sstable_handle));
  } else if (OB_ISNULL(ddl_agg_sstable_handles_)
             && OB_ISNULL(ddl_agg_sstable_handles_ = OB_NEW(ObArray<ObTableHandleV2>, ObMemAttr(MTL_ID(), "inc_major_hdl")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new array", KR(ret));
  } else if (OB_FAIL(ddl_agg_sstable_handles_->push_back(ddl_agg_sstable_handle))) {
    LOG_WARN("fail to push back", KR(ret), K(ddl_agg_sstable_handle));
  } else if (OB_FAIL(ddl_agg_sstable_handle.get_sstable(ddl_agg_sstable))) {
    LOG_WARN("fail to get sstable", KR(ret), K(ddl_agg_sstable_handle));
  } else if (OB_ISNULL(ddl_agg_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl agg sstable is nullptr", KR(ret));
  } else if (OB_FAIL(add_table(ddl_agg_sstable))) {
    LOG_WARN("fail to add table", KR(ret), KPC(ddl_agg_sstable));
  }
  return ret;
}

int ObTableStoreIterator::add_ddl_co_table(ObTableHandleV2 &table_handle, ObITable *co_table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_handle.is_valid() || nullptr == co_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable handle is invalid", KR(ret), K(table_handle), KP(co_table));
  } else if (OB_UNLIKELY(nullptr != ddl_co_sstable_handle_ && ddl_co_sstable_handle_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl co sstable handle set twice", K(ret), K(ddl_co_sstable_handle_));
  } else if (nullptr == ddl_co_sstable_handle_
      && OB_ISNULL(ddl_co_sstable_handle_ = OB_NEW(ObTableHandleV2, ObMemAttr(MTL_ID(), "ddl_co_hdl")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (FALSE_IT(*ddl_co_sstable_handle_ = table_handle)) {
  } else if (OB_FAIL(add_table(co_table))) {
    LOG_WARN("fail to add table", KR(ret), K(table_handle), KPC(co_table));
  }
  return ret;
}

int ObTableStoreIterator::get_table_ptr_with_meta_handle(
    const ObSSTable *table,
    TablePtr &table_ptr)
{
  int ret = OB_SUCCESS;
  ObStorageMetaHandle sstable_meta_hdl;
  ObSSTable *sstable = nullptr;

  if (OB_FAIL(ObCacheSSTableHelper::load_sstable(table->get_addr(),
      table->is_co_sstable(), sstable_meta_hdl))) {
    LOG_WARN("fail to load sstable", K(ret));
  } else if (OB_FAIL(sstable_handle_array_.push_back(sstable_meta_hdl))) {
    LOG_WARN("fail to push sstable meta handle", K(ret), K(sstable_meta_hdl));
  } else if (OB_FAIL(sstable_meta_hdl.get_sstable(sstable))) {
    LOG_WARN("fail to get sstable from meta handle", K(ret), K(sstable_meta_hdl), KPC(table));
  } else {
    table_ptr.table_ = sstable;
    table_ptr.hdl_idx_ = sstable_handle_array_.count() - 1;
  }
  return ret;
}

int ObTableStoreIterator::inner_move_idx_to_next()
{
  int ret = OB_SUCCESS;
  if (0 == table_ptr_array_.count()) {
    ret = OB_ITER_END;
  } else if (INT64_MAX == pos_) {
    pos_ = (-1 == step_) ? table_ptr_array_.count() - 1 : 0;
  } else if (pos_ >= table_ptr_array_.count() || pos_ < 0) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObTableStoreIterator::add_tables(
    const ObSSTableArray &sstable_array,
    const int64_t start_pos,
    const int64_t count,
    const bool unpack_co_table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!sstable_array.is_valid()
      || start_pos < 0
      || start_pos + count > sstable_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sstable_array), K(start_pos), K(count));
  } else {
    for (int64_t i = start_pos; OB_SUCC(ret) && i < start_pos + count; ++i) {
      if (OB_FAIL(add_table(sstable_array[i]))) {
        LOG_WARN("fail to add sstable to iterator", K(ret), K(i));
      } else if (sstable_array[i]->is_co_sstable() && unpack_co_table) {
        ObCOSSTableV2 *co_table = static_cast<ObCOSSTableV2 *>(sstable_array[i]);
        ObSSTableMetaHandle meta_handle;
        if (co_table->is_cgs_empty_co_table()) {
          // all_cg only co table, no need to call this func recursively
        } else if (OB_FAIL(co_table->get_meta(meta_handle))) {
          LOG_WARN("failed to get co meta handle", K(ret), KPC(co_table));
        } else {
          const ObSSTableArray &cg_sstables = meta_handle.get_sstable_meta().get_cg_sstables();
          if (OB_FAIL(add_cg_tables(cg_sstables, co_table->is_loaded(), meta_handle))) {
            LOG_WARN("fail to add cg table to iterator", K(ret), KPC(co_table));
          }
        }
      }
    }
  }
  return ret;
}

/*
 * cg sstable should be added carefully:
 * if cg is not loaded, its lifetime guranteed by cg meta handle and co meta handle
 * if cg is loaded, its lifetime guranteed by co meta handle
 */
int ObTableStoreIterator::add_cg_tables(
    const ObSSTableArray &cg_sstables,
    const bool is_loaded_co_table,
    const ObSSTableMetaHandle &co_meta_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cg_sstables.is_valid() || !co_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_sstables), K(co_meta_handle));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < cg_sstables.count(); ++i) {
    ObSSTable *cg_table = cg_sstables[i];
    TablePtr table_ptr;
    ObSSTableMetaHandle cg_meta_handle;

    if (OB_UNLIKELY(nullptr == (cg_table = cg_sstables[i]) || !cg_table->is_cg_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected cg table", K(ret), KPC(cg_table));
    } else if (is_loaded_co_table && cg_table->is_loaded()) {
      // lifetime guranteed by loaded co table
      table_ptr.table_ = cg_table;
    } else if (!cg_table->is_loaded()) {
      // cg table is shell, lifetime guranteed by cg meta handle
      if (OB_FAIL(get_table_ptr_with_meta_handle(cg_table, table_ptr))) {
        LOG_WARN("fail to get table ptr with meta handle", K(ret), KPC(cg_table));
      }
    } else {
      // cg table is loaded, lifetime guranteed by co meta handle
      if (OB_FAIL(sstable_handle_array_.push_back(co_meta_handle.get_storage_handle()))) {
        LOG_WARN("fail to push sstable meta handle", K(ret), KPC(cg_table));
      } else {
        table_ptr.table_ = cg_table;
        table_ptr.hdl_idx_ = sstable_handle_array_.count() - 1;
      }
    }

    if (FAILEDx(table_ptr_array_.push_back(table_ptr))) {
      LOG_WARN("fail to push table handle into array", K(ret));
    }
  }
  return ret;
}

int ObTableStoreIterator::add_tables(
    const ObMemtableArray &memtable_array,
    const int64_t start_pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!memtable_array.is_valid() || start_pos >= memtable_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(memtable_array), K(start_pos));
  } else {
    for (int64_t i = start_pos; OB_SUCC(ret) && i < memtable_array.count(); ++i) {
      if (OB_FAIL(add_table(memtable_array[i]))) {
        LOG_WARN("fail to add memtable to iterator", K(ret), K(i), K(memtable_array));
      }
    }
  }
  return ret;
}

int ObTableStoreIterator::get_ith_table(const int64_t pos, ObITable *&table)
{
  int ret = OB_SUCCESS;
  ObITable *tmp_table = nullptr;
  if (OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pos));
  } else if (OB_ISNULL(tmp_table = table_ptr_array_.at(pos).table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid table handle", K(ret), KP(tmp_table), K(pos), K(table_ptr_array_));
  } else if (tmp_table->is_memtable()) {
    table = tmp_table;
  } else if (static_cast<ObSSTable *>(tmp_table)->is_loaded() || !need_load_sstable_) {
    // table store local sstable
    table = tmp_table;
  } else {
    const int64_t hdl_idx = table_ptr_array_.at(pos).hdl_idx_;
    ObSSTable *sstable = nullptr;
    if (OB_UNLIKELY(hdl_idx < 0 || hdl_idx >= sstable_handle_array_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected handle idx for loaded sstable", K(ret), K(hdl_idx), KPC(tmp_table), KPC(this));
    } else if (OB_FAIL(sstable_handle_array_.at(hdl_idx).get_sstable(sstable))) {
      LOG_WARN("fail to get sstable value", K(ret), K(hdl_idx), K(sstable_handle_array_));
    } else if (sstable->is_co_sstable() && tmp_table->is_cg_sstable()) {
      // cg sstable's lifetime guranteed by co meta handle
      table = tmp_table;
    } else {
      table = sstable;
      table_ptr_array_.at(pos).table_ = sstable;
    }
  }
  return ret;
}

int ObTableStoreIterator::set_retire_check()
{
  int ret = OB_SUCCESS;
  memstore_retired_ = nullptr;
  ObITable *first_memtable = nullptr;

  for (int64_t i = table_ptr_array_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const TablePtr &table_ptr = table_ptr_array_.at(i);
    if (OB_UNLIKELY(!table_ptr.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid table handle", K(ret), K(table_ptr), K(*this));
    } else if (table_ptr.table_->is_memtable()) {
      first_memtable = table_ptr.table_;
    } else {
      break;
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(first_memtable)) {
    ObITabletMemtable *memtable = static_cast<ObITabletMemtable *>(first_memtable);
    memstore_retired_ = &memtable->get_read_barrier();
  }
  return ret;
}

int ObTableStoreIterator::get_unloaded_sstable(common::ObIArray<TablePtr*> &table_ptr_aggregate)
{
  int ret = OB_SUCCESS;
  // collect unloaded sstable
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ptr_array_.count(); ++i) {
    TablePtr &table_ptr = table_ptr_array_.at(i);
    if (OB_UNLIKELY(!table_ptr.table_->is_sstable())) {
      continue;
    }
    ObSSTable *table = static_cast<ObSSTable *>(table_ptr.table_);

    if (table->is_loaded() ||
        (table_ptr.hdl_idx_ != -1 && sstable_handle_array_.at(table_ptr.hdl_idx_).has_sent_io())) {
      // do not need to load sstable meta
    } else {
      // get cache meta handle
      int64_t hdl_idx = sstable_handle_array_.count();
      if (OB_FAIL(sstable_handle_array_.push_back(ObStorageMetaHandle()))) {
        LOG_WARN("fail to push sstable meta handle", K(ret));
      } else {
        // ObStorageMetaHandle::cache_handle_ will be allocated here
        ret = ObCacheSSTableHelper::load_sstable_from_cache(table->get_addr(), table->is_co_sstable(), sstable_handle_array_.at(hdl_idx));
        switch (ret) {
          case OB_SUCCESS: {
            // loaded from cache
            ObSSTable *sstable = nullptr;
            if (OB_FAIL(sstable_handle_array_.at(hdl_idx).get_sstable(sstable))) {
              LOG_WARN("fail to get sstable from meta handle", K(ret), K(sstable_handle_array_.at(hdl_idx)), KP(table));
            } else {
              table_ptr.table_ = sstable;
              table_ptr.hdl_idx_ = hdl_idx;
            }
            break;
          }
          case OB_ENTRY_NOT_EXIST: {
            // not loaded, add to aggregate array
            if (OB_FAIL(table_ptr_aggregate.push_back(&table_ptr))) {
              LOG_WARN("fail to push table handle into array", K(ret));
            } else {
              table_ptr.hdl_idx_ = hdl_idx;
            }
            break;
          }
          default: {
            LOG_WARN("fail to load sstable from cache", K(ret), K(table->get_addr()), K(table->is_valid()));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableStoreIterator::load_sstable_meta_with_aggregate_io()
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(need_load_sstable_)) {
    common::ObSEArray<TablePtr*, 4> table_ptr_aggregate;
    if (OB_FAIL(get_unloaded_sstable(table_ptr_aggregate))) {
      LOG_WARN("fail to get unloaded sstable", K(ret));
    } else if (table_ptr_aggregate.count() == 1) {
      // single sstable fast path
      TablePtr *table_ptr = table_ptr_aggregate.at(0);
      ObSSTable *table = static_cast<ObSSTable *>(table_ptr->table_);
      ObStorageMetaHandle &sstable_meta_hdl = sstable_handle_array_.at(table_ptr->hdl_idx_);
      ObStorageMetaValue::MetaType meta_type = table->is_co_sstable()
                                           ? ObStorageMetaValue::MetaType::CO_SSTABLE
                                           : ObStorageMetaValue::MetaType::SSTABLE;
      ObStorageMetaKey meta_key(MTL_ID(), table->get_addr());
      if (OB_FAIL(OB_STORE_CACHE.get_storage_meta_cache().prefetch(meta_type, meta_key, sstable_meta_hdl, nullptr))) {
        LOG_WARN("fail to prefetch meta", K(ret), K(meta_type), K(meta_key));
      }
    } else if(table_ptr_aggregate.count() > 1) {
      // mutilple sstable
      common::hash::ObHashSet<blocksstable::MacroBlockId> block_id_set;
      common::hash::ObHashSet<blocksstable::MacroBlockId>::const_iterator block_id_iter;

      if (OB_FAIL(block_id_set.create(table_ptr_aggregate.count(), "BlkIdSetBkt", "BlkIdSetNode", MTL_ID()))) {
        LOG_WARN("create block_id set for batch load sstable meta failed", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_ptr_aggregate.count(); ++i) {
          TablePtr *table_ptr = table_ptr_aggregate.at(i);
          ObSSTable *table = static_cast<ObSSTable *>(table_ptr->table_);

          ret = block_id_set.set_refactored(table->get_addr().block_id());
          if (OB_SUCCESS != ret && OB_HASH_EXIST != ret) {
            LOG_WARN("fail to set block id", K(ret), K(table->get_addr().block_id()));
          }
        }

        block_id_iter = block_id_set.begin();
        while (OB_SUCC(ret) && block_id_iter != block_id_set.end()) {
          common::ObSEArray<AggregatedInfo, 4> aggregated_infos;

          for (int64_t i = 0; OB_SUCC(ret) && i < table_ptr_aggregate.count(); ++i) {
            TablePtr *table_ptr = table_ptr_aggregate.at(i);
            ObSSTable *table = static_cast<ObSSTable *>(table_ptr->table_);
            ObStorageMetaHandle *sstable_meta_hdl = &sstable_handle_array_.at(table_ptr->hdl_idx_);
            if (table->get_addr().block_id() == block_id_iter->first) {
              if(OB_FAIL(aggregated_infos.push_back({table, sstable_meta_hdl}))) {
                LOG_WARN("fail to push aggregated info to array", K(ret));
              }
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(OB_STORE_CACHE.get_storage_meta_cache().get_meta_aggregated(aggregated_infos))) {
            LOG_WARN("fail to get meta aggregated and bypass cache", K(ret), K(aggregated_infos));
          }

          ++block_id_iter;
        }
      }
    }
  }
  return ret;
}

} // storage
} // blocksstable
