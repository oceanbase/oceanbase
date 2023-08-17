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

#include "share/ob_force_print_log.h"
#include "storage/access/ob_sstable_multi_version_row_iterator.h"
#include "storage/access/ob_sstable_row_exister.h"
#include "storage/access/ob_sstable_row_lock_checker.h"
#include "storage/access/ob_sstable_row_multi_exister.h"
#include "storage/access/ob_sstable_row_multi_getter.h"
#include "storage/access/ob_sstable_row_scanner.h"
#include "storage/access/ob_sstable_row_whole_scanner.h"
#include "storage/access/ob_rows_info.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_sstable_sec_meta_iterator.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ob_all_micro_block_range_iterator.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase
{
using namespace common::hash;
using namespace storage;
using namespace share;
namespace blocksstable
{

void ObSSTableMetaHandle::reset()
{
  handle_.reset();
  meta_ = nullptr;
}

int ObSSTableMetaHandle::get_sstable_meta(const ObSSTableMeta *&sstable_meta) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sstable_meta)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable meta handle not inited", K(ret));
  } else {
    sstable_meta = meta_;
  }
  return ret;
}

ObSSTable::ObSSTable()
  : addr_(),
    upper_trans_version_(0),
    max_merged_trans_version_(0),
    data_macro_block_count_(0),
    nested_size_(0),
    nested_offset_(0),
    contain_uncommitted_row_(false),
    valid_for_reading_(false),
    is_tmp_sstable_(false),
    filled_tx_scn_(share::SCN::min_scn()),
    meta_(nullptr)
{
#if defined(__x86_64__)
  static_assert(sizeof(ObSSTable) <= 160, "The size of ObSSTable will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
#endif
}

ObSSTable::~ObSSTable()
{
  reset();
}

int ObSSTable::init(const ObTabletCreateSSTableParam &param, common::ObArenaAllocator *allocator)
{
  int ret = OB_SUCCESS;
  bool inc_success = false;
  if (OB_UNLIKELY(valid_for_reading_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("this sstable can't be initialized", K(ret), K_(valid_for_reading));
  } else if (OB_UNLIKELY(!param.is_valid()) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), KP(allocator));
  } else if (OB_FAIL(ObITable::init(param.table_key_))) {
    LOG_WARN("fail to initialize ObITable", K(ret), "table_key", param.table_key_);
  } else if (OB_FAIL(init_sstable_meta(param, allocator))) {
    LOG_WARN("fail to initialize sstable meta", K(ret), K(param));
  } else if (FALSE_IT(addr_.set_mem_addr(0, sizeof(ObSSTable)))) {
  } else if (OB_FAIL(inc_macro_ref(inc_success))) {
    LOG_WARN("fail to add macro ref", K(ret), K(inc_success));
  } else if (FALSE_IT(meta_->macro_info_.dec_linked_block_ref_cnt())) {
  } else if (FALSE_IT(is_tmp_sstable_ = true)) {
  } else if (param.is_ready_for_read_) {
    if (OB_FAIL(check_valid_for_reading())) {
      LOG_WARN("Failed to check state", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!(SSTABLE_READY_FOR_READ == meta_->get_basic_meta().status_
                        || SSTABLE_WRITE_BUILDING == meta_->get_basic_meta().status_))) {
    if (OB_SUCCESS == ret) {
      ret = OB_STATE_NOT_MATCH;
    }
    reset();
  } else {
    FLOG_INFO("succeeded to init sstable", K(ret), KPC(this));
  }
  return ret;
}

void ObSSTable::reset()
{
  LOG_DEBUG("reset sstable.", KP(this), K(key_), K(is_tmp_sstable_));
  // dec ref first, then reset sstable meta
  if (is_tmp_sstable_) {
    ObSSTable::dec_macro_ref();
  }
  if (nullptr != meta_) {
    meta_->reset();
  }
  addr_.reset();
  upper_trans_version_ = 0;
  max_merged_trans_version_ = 0;
  data_macro_block_count_ = 0;
  nested_size_ = 0;
  nested_offset_ = 0;
  contain_uncommitted_row_ = false;
  meta_ = nullptr;
  valid_for_reading_ = false;
  is_tmp_sstable_ = false;
  filled_tx_scn_ = share::SCN::min_scn();
  ObITable::reset();
}

int ObSSTable::scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObDatumRange &key_range,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(param.tablet_id_ != key_.tablet_id_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Tablet id is not match", K(ret), K(*this), K(param));
  } else if (OB_UNLIKELY(
      !param.is_valid()
      || !context.is_valid()
      || !key_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(context), K(key_range));
  } else {
    ObStoreRowIterator *row_scanner = nullptr;
    if (context.query_flag_.is_whole_macro_scan()) {
      ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
          ObSSTableRowWholeScanner,
          row_scanner);
    } else if (is_multi_version_minor_sstable()) {
      ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
          ObSSTableMultiVersionRowScanner,
          row_scanner);
    } else {
      ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
          ObSSTableRowScanner<>,
          row_scanner);
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(row_scanner)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected error, row_scanner is nullptr", K(ret), KP(row_scanner));
      } else if (OB_FAIL(row_scanner->init(param, context, this, &key_range))) {
        LOG_WARN("Fail to open row scanner", K(ret), K(param), K(context), K(key_range), K(*this));
      }
    }

    if (OB_FAIL(ret)) {
      if (nullptr != row_scanner) {
        row_scanner->~ObStoreRowIterator();
        FREE_TABLE_STORE_ROW_IETRATOR(context, row_scanner);
        row_scanner = nullptr;
      }
    } else {
      row_iter = row_scanner;
    }
  }

  return ret;
}

int ObSSTable::get(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const ObDatumRowkey &rowkey,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(param.tablet_id_ != key_.tablet_id_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Tablet id is not match", K(ret), K(*this), K(param));
  } else if (OB_UNLIKELY(
      !param.is_valid()
      || !context.is_valid()
      || !rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(context), K(rowkey));
  } else {
    ObStoreRowIterator *row_getter = nullptr;
    if (is_multi_version_minor_sstable()
        && (context.is_multi_version_read(get_upper_trans_version())
          || contain_uncommitted_row())) {
      ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
          ObSSTableMultiVersionRowGetter,
          row_getter);
    } else {
      ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
          ObSSTableRowGetter,
          row_getter);
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(row_getter)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected error, row_getter is nullptr", K(ret), KP(row_getter));
      } else if (OB_FAIL(row_getter->init(param, context, this, &rowkey))) {
        LOG_WARN("Fail to open row scanner", K(ret), K(param), K(context), K(rowkey), K(*this));
      }
    }

    if (OB_FAIL(ret)) {
      if (nullptr != row_getter) {
        row_getter->~ObStoreRowIterator();
        FREE_TABLE_STORE_ROW_IETRATOR(context, row_getter);
        row_getter = nullptr;
      }
    } else {
      row_iter = row_getter;
    }
  }

  return ret;
}

int ObSSTable::multi_scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const common::ObIArray<ObDatumRange> &ranges,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(param.tablet_id_ != key_.tablet_id_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Tablet id is not match", K(ret), K(*this), K(param));
  } else if (OB_UNLIKELY(
      !param.is_valid()
      || !context.is_valid()
      || 0 >= ranges.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(context), K(ranges));
  } else {
    void *buf = nullptr;
    ObStoreRowIterator *row_scanner = nullptr;
    if (is_multi_version_minor_sstable()) {
      ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
          ObSSTableMultiVersionRowMultiScanner,
          row_scanner);
    } else {
      ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
          ObSSTableRowMultiScanner,
          row_scanner);
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(row_scanner)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected error, row_scanner is nullptr", K(ret), KP(row_scanner));
      } else if (OB_FAIL(row_scanner->init(param, context, this, &ranges))) {
        LOG_WARN("Fail to open row scanner", K(ret), K(param), K(context), K(ranges), K(*this));
      }
    }

    if (OB_FAIL(ret)) {
      if (nullptr != row_scanner) {
        row_scanner->~ObStoreRowIterator();
        FREE_TABLE_STORE_ROW_IETRATOR(context, row_scanner);
        row_scanner = nullptr;
      }
    } else {
      row_iter = row_scanner;
    }
  }

  return ret;
}

int ObSSTable::multi_get(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const common::ObIArray<ObDatumRowkey> &rowkeys,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(param.tablet_id_ != key_.tablet_id_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Tablet id is not match", K(ret), K(*this), K(param));
  } else if (OB_UNLIKELY(
      !param.is_valid()
      || !context.is_valid()
      || 0 >= rowkeys.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(context), K(rowkeys));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
      if (OB_UNLIKELY(!rowkeys.at(i).is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid rowkey", K(ret), K(i), K(rowkeys));
      }
    }
    if (OB_SUCC(ret)) {
      void *buf = nullptr;
      ObStoreRowIterator *row_getter = nullptr;
      if (is_multi_version_minor_sstable()
          && (context.is_multi_version_read(get_upper_trans_version())
            || contain_uncommitted_row())) {
        ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
            ObSSTableMultiVersionRowMultiGetter,
            row_getter);
      } else {
        ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
            ObSSTableRowMultiGetter,
            row_getter);
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(row_getter)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected error, row_getter is nullptr", K(ret), KP(row_getter));
        } else if (OB_FAIL(row_getter->init(param, context, this, &rowkeys))) {
          LOG_WARN("Fail to open row scanner", K(ret), K(param), K(context), K(rowkeys), K(*this));
        }
      }

      if (OB_FAIL(ret)) {
        if (nullptr != row_getter) {
          row_getter->~ObStoreRowIterator();
          FREE_TABLE_STORE_ROW_IETRATOR(context, row_getter);
          row_getter = nullptr;
        }
      } else {
        row_iter = row_getter;
      }
    }
  }

  return ret;
}


int ObSSTable::exist(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey,
    bool &is_exist,
    bool &has_found)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(!rowkey.is_valid()
      || !param.is_valid()
      || !context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(rowkey), K(param), K(context));
  } else {
    const ObDatumRow *store_row = nullptr;
    ObStoreRowIterator *iter = nullptr;
    is_exist = false;
    has_found = false;

    if (OB_FAIL(build_exist_iterator(param, rowkey, context, iter))) {
      LOG_WARN("Failed to build exist iterator", K(ret));
    } else if (OB_FAIL(iter->get_next_row(store_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get next row", K(ret));
      }
    } else if (OB_ISNULL(store_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null store row", K(ret));
    }

    if (OB_FAIL(ret)) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
      }
    } else if (!store_row->row_flag_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected row flag", K(ret), K(store_row->row_flag_));
    } else if (store_row->row_flag_.is_not_exist()) {
    } else if (store_row->row_flag_.is_delete()) {
      has_found = true;
    } else {
      is_exist = true;
      has_found = true;
    }

    if (OB_NOT_NULL(iter)) {
      ObTabletStat &stat = context.store_ctx_->tablet_stat_;
      stat.ls_id_ = context.ls_id_.id();
      stat.tablet_id_ = context.tablet_id_.id();
      stat.query_cnt_ = context.table_store_stat_.exist_row_.empty_read_cnt_ > 0;

      iter->~ObStoreRowIterator();
      context.stmt_allocator_->free(iter);
    }
  }

  return ret;
}


int ObSSTable::exist(ObRowsInfo &rows_info, bool &is_exist, bool &all_rows_found)
{
  int ret = OB_SUCCESS;
  bool may_exist = true;
  is_exist = false;
  all_rows_found = false;
  const ObDatumRowkey *sstable_endkey = nullptr;
  if (OB_UNLIKELY(!rows_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(rows_info));
  } else if (OB_UNLIKELY(rows_info.tablet_id_ != key_.tablet_id_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Tablet id not match", K(ret), K_(key), K(rows_info));
  } else if (is_empty()) {
    // Skip
  } else if (rows_info.all_rows_found()) {
    all_rows_found = true;
  } else if (OB_FAIL(get_last_rowkey(sstable_endkey))) {
    LOG_WARN("Fail to get SSTable endkey", K(ret), K_(meta));
  } else if (OB_ISNULL(sstable_endkey)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Null pointer to sstable endkey", K(ret), K_(meta));
  } else if (OB_FAIL(rows_info.check_min_rowkey_boundary(*sstable_endkey, may_exist))) {
    STORAGE_LOG(WARN, "Failed to check min rowkey boundary", K(ret), KPC(sstable_endkey), K(rows_info));
  } else if (!may_exist) {
    // Skip
   } else if (OB_FAIL(rows_info.refine_rowkeys())) {
     LOG_WARN("Failed to refine rowkeys", K(rows_info), K(ret));
  } else if (rows_info.rowkeys_.count() == 0) {
    LOG_INFO("Skip unexpected empty ext rowkeys", K(ret), K(rows_info));
    all_rows_found = true;
  } else {
    ObStoreRowIterator *iter = nullptr;
    const ObDatumRow *store_row = nullptr;

    if (OB_FAIL(build_multi_exist_iterator(rows_info, iter))) {
      LOG_WARN("Fail to build multi exist iterator", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < rows_info.rowkeys_.count(); ++i) {
      if (OB_FAIL(iter->get_next_row(store_row))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("Get next row failed", K(ret), K(rows_info), K(rows_info.rowkeys_.at(i)));
        }
      } else if (OB_ISNULL(store_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null store row", K(ret));
      } else if (!store_row->row_flag_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected row flag", K(ret), KPC(store_row));
      } else if (store_row->row_flag_.is_not_exist()) {
        // Skip
      } else if (store_row->row_flag_.is_delete()) {
        if (OB_FAIL(rows_info.clear_found_rowkey(i))) {
          LOG_WARN("Failed to clear rowkey in rowsinfo", K(ret), K(i), K(rows_info));
        }
      } else {
        is_exist = true;
        all_rows_found = true;
        rows_info.get_duplicate_rowkey() = rows_info.rowkeys_.at(i);
      }
      if (OB_SUCC(ret) && !is_exist) {
        all_rows_found = rows_info.all_rows_found();
      }
    }

    if (OB_NOT_NULL(iter)) {
      iter->~ObStoreRowIterator();
      rows_info.exist_helper_.table_access_context_.allocator_->free(iter);
      rows_info.reuse_scan_mem_allocator();
    }
  }
  return ret;
}

int ObSSTable::scan_macro_block(
    const ObDatumRange &range,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator,
    ObIMacroBlockIterator *&macro_block_iter,
    const bool is_reverse_scan,
    const bool need_record_micro_info,
    const bool need_scan_sec_meta)
{
  int ret = OB_SUCCESS;
  ObIMacroBlockIterator *iter = nullptr;
  void *buf = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else {
    if (need_scan_sec_meta) {
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObDualMacroMetaIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for macro block iter", K(ret));
      } else if (OB_ISNULL(iter = new (buf) ObDualMacroMetaIterator())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to construct macro block iter", K(ret));
      }
    } else {
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObIndexBlockMacroIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for macro block iter", K(ret));
      } else if (OB_ISNULL(iter = new (buf) ObIndexBlockMacroIterator())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to construct macro block iter", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(iter->open(
        *this, range, rowkey_read_info, allocator, is_reverse_scan, need_record_micro_info))) {
      LOG_WARN("Fail to open macro block iter", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != iter) {
      iter->~ObIMacroBlockIterator();
    }
    if (nullptr != buf) {
      allocator.free(buf);
    }
  } else {
    macro_block_iter = iter;
  }
  return ret;
}

int ObSSTable::scan_micro_block(
    const ObDatumRange &range,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator,
    ObAllMicroBlockRangeIterator *&micro_iter,
    const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  micro_iter = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObAllMicroBlockRangeIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for micro block range iter", K(ret));
  } else if (OB_ISNULL(micro_iter = new (buf) ObAllMicroBlockRangeIterator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to construct micro block range iterator", K(ret));
  } else if (OB_FAIL(micro_iter->open(*this, range, rowkey_read_info, allocator, is_reverse_scan))) {
    LOG_WARN("Fail to open micro block range iterator", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != micro_iter) {
      micro_iter->~ObAllMicroBlockRangeIterator();
    }
    if (nullptr != buf) {
      allocator.free(buf);
    }
    micro_iter = nullptr;
  }
  return ret;
}

int ObSSTable::scan_secondary_meta(
    ObIAllocator &allocator,
    const ObDatumRange &query_range,
    const ObITableReadInfo &rowkey_read_info,
    const blocksstable::ObMacroBlockMetaType meta_type,
    blocksstable::ObSSTableSecMetaIterator *&meta_iter,
    const bool is_reverse_scan,
    const int64_t sample_step) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else {
    void *buf = nullptr;
    ObSSTableSecMetaIterator *iter = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSSTableSecMetaIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory", K(ret));
    } else if (OB_ISNULL(iter = new (buf) ObSSTableSecMetaIterator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null pointer of secondary meta iterator", K(ret));
    } else if (OB_FAIL(iter->open(
        query_range, meta_type, *this, rowkey_read_info, allocator, is_reverse_scan, sample_step))) {
      LOG_WARN("Fail to open secondary meta iterator with range",
          K(ret), K(query_range), K(meta_type), K_(meta), K(is_reverse_scan), K(sample_step), KPC(this));
    } else {
      meta_iter = iter;
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(iter)) {
        iter->~ObSSTableSecMetaIterator();
      }
      if (OB_NOT_NULL(buf)) {
        allocator.free(buf);
      }
    }
  }
  return ret;
}

int ObSSTable::bf_may_contain_rowkey(const ObDatumRowkey &rowkey, bool &contain)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSTable has not been inited", K(ret));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to check bloomfilter", K(ret));
  } else {
    // pass sstable without bf macro
    contain = true;
  }
  return ret;
}


int ObSSTable::check_row_locked(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey,
    ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  ObSSTableRowLockChecker *row_checker = NULL;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_STORE_ROW_LOCK_CHECKER));
  lock_state.trans_version_ = SCN::min_scn();
  lock_state.is_locked_ = false;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("The SSTable has not been inited", K(ret), K_(key), K_(valid_for_reading), KPC_(meta));
  } else if (!is_multi_version_minor_sstable()) {
    // return false if not multi version minor sstable
  } else if (get_upper_trans_version() <= context.store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx()) {
    // there is no lock at this sstable
    if (!is_empty()) {
      // skip reference upper_trans_version of empty_sstable, which may greater than real
      // committed transaction's version
      if (OB_FAIL(lock_state.trans_version_.convert_for_tx(get_upper_trans_version()))) {
        LOG_WARN("Fail to convert_for_tx", K(get_upper_trans_version()), K_(meta), K(ret));
      }
    }
  } else if (NULL == (buf = allocator.alloc(sizeof(ObSSTableRowLockChecker)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory", K(ret));
  } else {
    row_checker = new (buf) ObSSTableRowLockChecker();

    if (OB_FAIL(row_checker->init(param, context, this, &rowkey))) {
      LOG_WARN("failed to open row locker", K(ret), K(param), K(context), K(rowkey));
    } else if (OB_FAIL(row_checker->check_row_locked(lock_state))) {
      LOG_WARN("failed to check row lock checker");
    }

    row_checker->~ObSSTableRowLockChecker();
  }

  return ret;
}



int ObSSTable::set_upper_trans_version(const int64_t upper_trans_version)
{
  int ret = OB_SUCCESS;

  const int64_t old_val = ATOMIC_LOAD(&upper_trans_version_);
  // only set once
  if (INT64_MAX == old_val && INT64_MAX != upper_trans_version) {
    const int64_t new_val = std::max(upper_trans_version, max_merged_trans_version_);
    ATOMIC_CAS(&upper_trans_version_, old_val, new_val);
  }

  LOG_INFO("succeed to set upper trans version", K(key_),
      K(upper_trans_version), K(upper_trans_version_));
  return ret;
}

int ObSSTable::set_addr(const ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta disk address", K(ret), K(addr));
  } else {
    addr_ = addr;
  }
  return ret;
}

int ObSSTable::get_frozen_schema_version(int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable is not initialized.", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_FAIL(get_meta(meta_handle))) {
    LOG_WARN("fail to get sstable meta handle", K(ret));
  } else {
    schema_version = meta_handle.get_sstable_meta().get_schema_version();
  }
  return ret;
}

int ObSSTable::get_last_rowkey(
    ObIAllocator &allocator,
    ObDatumRowkey &endkey)
{
  int ret = OB_SUCCESS;
  const ObDatumRowkey *last_rowkey;

  if (OB_FAIL(get_last_rowkey(last_rowkey))) {
    STORAGE_LOG(WARN, "Failed to get datum rowkey", K(ret));
  } else if (OB_ISNULL(last_rowkey)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null datum rowkey", K(ret));
  } else if (OB_FAIL(last_rowkey->deep_copy(endkey, allocator))) {
    LOG_WARN("Fail to copuy last rowkey", K(ret));
  }

  return ret;
}

int ObSSTable::deep_copy(ObArenaAllocator &allocator, ObSSTable *&dst) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  char *buf = nullptr;
  ObIStorageMetaObj *meta_obj = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(deep_copy_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for deep copy sstable", K(ret), K(deep_copy_size));
  } else if (OB_FAIL(deep_copy(buf, deep_copy_size, meta_obj))) {
    LOG_WARN("fail to inner deep copy sstable", K(ret));
  } else {
    dst = static_cast<ObSSTable *>(meta_obj);
  }
  return ret;
}

int ObSSTable::deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const
{
  int ret = OB_SUCCESS;
  value = nullptr;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(deep_copy_size));
#if __aarch64__
  } else if (OB_UNLIKELY(0 != (reinterpret_cast<int64_t>(buf) % AARCH64_CP_BUF_ALIGN))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deep copy buffer on aarch64 platform not alighed", K(ret), KP(buf));
#endif
  } else {
    ObSSTable *pvalue = new (buf) ObSSTable();
    int64_t pos = sizeof(ObSSTable);
    pvalue->key_ = key_;
    pvalue->addr_ = addr_;
    pvalue->upper_trans_version_ = upper_trans_version_;
    pvalue->max_merged_trans_version_ = max_merged_trans_version_;
    pvalue->data_macro_block_count_ = data_macro_block_count_;
    pvalue->nested_size_ = nested_size_;
    pvalue->nested_offset_ = nested_offset_;
    pvalue->contain_uncommitted_row_ = contain_uncommitted_row_;
    pvalue->is_tmp_sstable_ = false;
    pvalue->filled_tx_scn_ = filled_tx_scn_;
    pvalue->valid_for_reading_ = valid_for_reading_;
    if (is_loaded()) {
      if (OB_FAIL(meta_->deep_copy(buf, buf_len, pos, pvalue->meta_))) {
        LOG_WARN("fail to deep copy for tiny memory", K(ret), KP(buf), K(buf_len), K(pos), KPC(meta_));
      }
    }
    if (OB_SUCC(ret)) {
      value = static_cast<ObIStorageMetaObj *>(pvalue);
      LOG_DEBUG("succeed to deep copy sstable", K(ret), K(deep_copy_size), K(buf_len), K(pos), KPC(pvalue), KPC(this));
    }
  }
  return ret;
}

int ObSSTable::check_valid_for_reading()
{
  int ret = OB_SUCCESS;
  valid_for_reading_ = key_.is_valid()
                       && nullptr != meta_
                       && meta_->is_valid()
                       && (SSTABLE_READY_FOR_READ == meta_->get_basic_meta().status_
                           || SSTABLE_READY_FOR_REMOTE_PHYTSICAL_READ == meta_->get_basic_meta().status_
                           || SSTABLE_READY_FOR_REMOTE_LOGICAL_READ == meta_->get_basic_meta().status_);
  if (OB_UNLIKELY(!valid_for_reading_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state state", K(ret), K_(valid_for_reading), K(key_), KPC(meta_));
  }
  return ret;
}

int ObSSTable::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(get_meta(meta_handle))) {
    LOG_WARN("fail to get sstable meta", K(ret));
  } else if (OB_UNLIKELY(!is_valid()
      && SSTABLE_WRITE_BUILDING != meta_handle.get_sstable_meta().get_status())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("non-ready sstable for read can't be serialized.", K(ret), K_(valid_for_reading), K(meta_handle));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(!addr_.is_valid() || (!addr_.is_memory() && !addr_.is_block()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable's addr_ is invalid", K(ret), K(addr_));
  } else if (OB_FAIL(ObITable::serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize table key", K(ret), K(buf_len), K(pos));
  } else {
    ObSSTable::StatusForSerialize status;
    if (!addr_.is_memory()) {
      status.set_with_fixed_struct();
    } else {
      status.set_with_meta();
    }
    OB_UNIS_ENCODE(status.pack_);
    if (OB_FAIL(ret)) {
    } else if (status.with_fixed_struct() && OB_FAIL(serialize_fixed_struct(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize fix sstable struct", K(ret), K(buf_len), K(pos));
    } else if (status.with_meta() && OB_FAIL(meta_->serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize sstable meta", K(ret), K(buf_len), K(pos));
    } else {
      LOG_INFO("succeed to serialize sstable", K(status.pack_), KPC(this));
    }
  }
  return ret;
}

int ObSSTable::deserialize(common::ObArenaAllocator &allocator,
                           const char *buf,
                           const int64_t data_len,
                           int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObSSTable::StatusForSerialize status;
  int64_t orig_pos = pos;
  char *meta_buf = nullptr;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(ObITable::deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize ObITable", K(ret), K(data_len), K(pos));
  } else {
    // TODO: delete temporary compat here
    int64_t compat_temp_pos = pos;
    OB_UNIS_DECODE(status.pack_);
    if (StatusForSerialize::COMPAT_MAGIC == status.compat_magic_ && 0 == status.reserved_) {
      // serialized with new binary
    } else {
      // serialized with old binary, always deserialize sstable meta
      pos = compat_temp_pos;
      status.reset();
      status.set_with_meta();
    }
    if (status.with_fixed_struct() && OB_FAIL(deserialize_fixed_struct(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize sstable object struct", K(ret));
    } else if (status.with_meta()) {
      addr_.set_mem_addr(0, sizeof(ObSSTable));
      if (OB_ISNULL(meta_buf = static_cast<char *>(allocator.alloc(sizeof(ObSSTableMeta))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (FALSE_IT(meta_ = new (meta_buf) ObSSTableMeta())) {
      } else if (OB_FAIL(meta_->deserialize(allocator, buf, data_len, pos))) {
        LOG_WARN("fail to deserialize sstable meta", K(ret), K(key_), K(data_len), K(pos));
      } else if (OB_UNLIKELY(!meta_->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable meta is not valid", K(ret), K_(meta));
      } else if (OB_FAIL(meta_->transform_root_block_data(allocator))) {
        LOG_WARN("fail to transform root block data", K(ret));
      } else if (OB_FAIL(check_valid_for_reading())) {
        LOG_WARN("fail to check valid for reading", K(ret));
      } else {
        // for compat, remove later
        upper_trans_version_ = meta_->get_upper_trans_version();
        max_merged_trans_version_ = meta_->get_max_merged_trans_version();
        data_macro_block_count_ = meta_->get_data_macro_block_count();
        contain_uncommitted_row_ = meta_->contain_uncommitted_row();
        filled_tx_scn_ = meta_->get_filled_tx_scn();
        nested_size_ = meta_->get_macro_info().get_nested_size();
        nested_offset_ = meta_->get_macro_info().get_nested_offset();
      }
    } else {
      valid_for_reading_ = key_.is_valid();
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succeed to deserialize sstable", K(status.pack_), KPC(this));
  } else {
    pos = orig_pos;
    LOG_WARN("fail to deserialize sstable", K(ret), K(status.pack_), KPC(this));
    reset();
  }
  return ret;
}

int64_t ObSSTable::get_serialize_size() const
{
  int ret = OB_SUCCESS;
  int64_t len = 0; // invalid
  int64_t sstable_meta_serialize_size = 0;
  int64_t fixed_struct_serialize_size = 0;
  ObSSTable::StatusForSerialize status;
  if (OB_UNLIKELY(!addr_.is_valid() || (!addr_.is_memory() && !addr_.is_block()))) {
    len = -1;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sstable's addr_ is invalid", K(ret), K(addr_), KPC(this));
  } else if (!addr_.is_memory()) {
    status.set_with_fixed_struct();
    fixed_struct_serialize_size = get_sstable_fix_serialize_size();
  } else {
    status.set_with_meta();
    sstable_meta_serialize_size = meta_->get_serialize_size();
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_ADD_LEN(status.pack_);
    len += ObITable::get_serialize_size() + fixed_struct_serialize_size + sstable_meta_serialize_size;
  }
  return len;
}
int64_t ObSSTable::get_sstable_fix_serialize_payload_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      addr_,
      upper_trans_version_,
      max_merged_trans_version_,
      data_macro_block_count_,
      nested_size_,
      nested_offset_,
      contain_uncommitted_row_,
      filled_tx_scn_);
  return len;
}

int64_t ObSSTable::get_sstable_fix_serialize_size() const
{
  int64_t len = 0;
  int64_t payload_size = get_sstable_fix_serialize_payload_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      SSTABLE_VERSION,
      payload_size);
  len += payload_size;
  return len;
}

int ObSSTable::serialize_fixed_struct(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t len = get_sstable_fix_serialize_payload_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0 || pos + len > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(buf_len), K(len));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
        SSTABLE_VERSION,
        len,
        addr_,
        upper_trans_version_,
        max_merged_trans_version_,
        data_macro_block_count_,
        nested_size_,
        nested_offset_,
        contain_uncommitted_row_,
        filled_tx_scn_);
  }
  return ret;
}

int ObSSTable::deserialize_fixed_struct(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len < 0 || data_len < pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    int64_t len = 0;
    int64_t version = 0;
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(len);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(version != SSTABLE_VERSION)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("sstable version not match", K(ret), K(version));
    } else if (OB_UNLIKELY(pos + len > data_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semi deserialize buffer not enough", K(ret), K(pos), K(len), K(data_len));
    } else {
      LST_DO_CODE(OB_UNIS_DECODE,
          addr_,
          upper_trans_version_,
          max_merged_trans_version_,
          data_macro_block_count_,
          nested_size_,
          nested_offset_,
          contain_uncommitted_row_,
          filled_tx_scn_);
      if (OB_SUCC(ret)) {
        valid_for_reading_ = key_.is_valid();
      }
    }
  }
  return ret;
}

int ObSSTable::assign_meta(ObSSTableMeta *meta) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta) || OB_UNLIKELY(!meta->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(meta));
  } else if (OB_NOT_NULL(meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable meta already assigned", K(ret), KPC(this), KPC(meta));
  } else {
    meta_ = meta;
    if (OB_FAIL(check_valid_for_reading())) {
      LOG_WARN("fail to check valid for reading", K(ret), KPC(this));
    }
  }
  return ret;
}

void ObSSTable::inc_ref()
{
  if (is_tmp_sstable_) {
    ObITable::inc_ref();
  }
}

int64_t ObSSTable::dec_ref()
{
  int64_t cnt = -1;
  if (is_tmp_sstable_) {
    cnt = ObITable::dec_ref();
  }
  return cnt;
}

int64_t ObSSTable::get_ref() const
{
  int64_t cnt = 0;
  if (is_tmp_sstable_) {
    cnt = ObITable::get_ref();
  }
  return cnt;
}

bool ObSSTable::ignore_ret(const int ret)
{
  return OB_ALLOCATE_MEMORY_FAILED == ret || OB_TIMEOUT == ret || OB_DISK_HUNG == ret;
}

void ObSSTable::dec_macro_ref() const
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObMacroIdIterator iterator;
  common::ObArenaAllocator tmp_allocator(common::ObMemAttr(MTL_ID(), "CacheSST"));
  ObSafeArenaAllocator safe_allocator(tmp_allocator);
  ObSSTableMetaHandle meta_handle;

  if (OB_FAIL(dec_used_size())) {// ignore ret
    LOG_WARN("fail to dec used size of shared block", K(ret));
  }
  do {
    safe_allocator.reuse();
    ret = get_meta(meta_handle, &safe_allocator);
  } while (ignore_ret(ret));
  if (OB_FAIL(ret)) {
    LOG_ERROR("fail to get sstable meta", K(ret));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("meta handle is invalid", K(ret), K(meta_handle));
  } else {
    do {
      iterator.reset();
      ret = meta_handle.get_sstable_meta().get_macro_info().get_data_block_iter(iterator);
    } while (ignore_ret(ret));
    if (OB_FAIL(ret)) {
      LOG_ERROR("fail to get data block iterator", K(ret), KPC(this));
    } else {
      while (OB_SUCC(iterator.get_next_macro_id(macro_id))) {
        if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
          LOG_ERROR("fail to dec data block ref cnt", K(ret), K(macro_id));
        } else {
          LOG_DEBUG("barry debug decrease data ref cnt", K(macro_id), KPC(this), K(lbt()));
        }
      }
    }

    do {
      iterator.reset();
      ret = meta_handle.get_sstable_meta().get_macro_info().get_other_block_iter(iterator);
    } while (ignore_ret(ret));
    if (OB_FAIL(ret)) { // ignore ret
      LOG_ERROR("fail to get other block iterator", K(ret), KPC(this));
    } else {
      while (OB_SUCC(iterator.get_next_macro_id(macro_id))) {
        if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
          LOG_ERROR("fail to dec other block ref cnt", K(ret), K(macro_id));
        } else {
          LOG_DEBUG("barry debug decrease other ref cnt", K(macro_id), KPC(this), K(lbt()));
        }
      }
    }
    iterator.reset();
    if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_linked_block_iter(iterator))) { // ignore ret
      LOG_ERROR("fail to get linked block iterator", K(ret), KPC(this));
    } else {
      while (OB_SUCC(iterator.get_next_macro_id(macro_id))) {
        if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
          LOG_ERROR("fail to dec other block ref cnt", K(ret), K(macro_id));
        } else {
          LOG_DEBUG("barry debug decrease link ref cnt", K(macro_id), KPC(this), K(lbt()));
        }
      }
    }
  }
}

int ObSSTable::inc_macro_ref(bool &inc_success) const
{
  int ret = OB_SUCCESS;
  inc_success = false;
  MacroBlockId macro_id;
  ObMacroIdIterator iter;
  int64_t data_blk_cnt = 0;
  int64_t other_blk_cnt = 0;
  int64_t linked_blk_cnt = 0;
  common::ObArenaAllocator tmp_allocator(common::ObMemAttr(MTL_ID(), "CacheSST"));
  ObSafeArenaAllocator safe_allocator(tmp_allocator);
  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(get_meta(meta_handle, &safe_allocator))) {
    LOG_WARN("fail to get sstable meta", K(ret));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta handle is invalid", K(ret), K(meta_handle));
  } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_data_block_iter(iter))) {
    LOG_WARN("fail to get data block iterator", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!iter.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the iter is invalid", K(ret), K(iter));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_macro_id(macro_id))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next macro id", K(ret), K(macro_id));
        }
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
        LOG_ERROR("fail to inc data block ref cnt", K(ret), K(macro_id));
      } else {
        ++data_blk_cnt;
      }
      LOG_DEBUG("barry debug increase data ref cnt", K(ret), K(macro_id), KPC(this), K(lbt()));
    }
    iter.reset();
  }

  if (OB_FAIL(ret) && OB_ITER_END != ret) {
  } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_other_block_iter(iter))) {
    LOG_WARN("fail to get other block iterator", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!iter.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the iter is invalid", K(ret), K(iter));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_macro_id(macro_id))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next macro id", K(ret), K(macro_id));
        }
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
        LOG_ERROR("fail to inc other block ref cnt", K(ret), K(macro_id));
      } else {
        ++other_blk_cnt;
      }
      LOG_DEBUG("barry debug increase other ref cnt", K(ret), K(macro_id), KPC(this), K(lbt()));
    }
    iter.reset();
  }

  if (OB_FAIL(ret) && OB_ITER_END != ret) {
  } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_linked_block_iter(iter))) {
    LOG_WARN("fail to get linked block iterator", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!iter.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the iter is invalid", K(ret), K(iter));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_macro_id(macro_id))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next macro id", K(ret), K(macro_id));
        }
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
        LOG_ERROR("fail to inc linked block ref cnt", K(ret), K(macro_id));
      } else {
        ++linked_blk_cnt;
      }
      LOG_DEBUG("barry debug increase link ref cnt", K(ret), K(macro_id), KPC(this), K(lbt()));
    }
    iter.reset();
  }

  if ((OB_SUCC(ret) || OB_LIKELY(OB_ITER_END == ret)) && OB_FAIL(add_used_size())) {
    LOG_WARN("fail to add used size", K(ret));
  }

  if (OB_SUCC(ret)) {
    inc_success = true;
  } else if (meta_handle.is_valid()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_data_block_iter(iter))) {
      LOG_ERROR("fail to get data block iterator", K(ret), KPC(this));
    } else {
      for (int64_t i = data_blk_cnt; i > 0; --i) { // ignore ret
        if (OB_TMP_FAIL(iter.get_next_macro_id(macro_id))) {
          LOG_ERROR("fail to get next macro id", K(ret), K(iter));
        } else if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
          LOG_ERROR("fail to dec data block ref cnt", K(ret), K(tmp_ret), K(macro_id));
        } else {
          LOG_DEBUG("barry debug decrease data ref cnt", K(macro_id), KPC(this), K(lbt()));
        }
      }
      iter.reset();
    }
    if (OB_TMP_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_other_block_iter(iter))) { // ignore ret
      LOG_ERROR("fail to get other block iterator", K(ret), KPC(this));
    } else {
      for (int64_t i = other_blk_cnt; i > 0; --i) { // ignore ret
        if (OB_TMP_FAIL(iter.get_next_macro_id(macro_id))) {
          LOG_ERROR("fail to get next macro id", K(ret), K(iter));
        } else if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
          LOG_ERROR("fail to dec other block ref cnt", K(ret), K(tmp_ret), K(macro_id));
        } else {
          LOG_DEBUG("barry debug decrease other ref cnt", K(macro_id), KPC(this), K(lbt()));
        }
      }
      iter.reset();
    }
    if (OB_TMP_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_linked_block_iter(iter))) {
      LOG_ERROR("fail to get linked block iterator", K(ret), KPC(this));
    } else {
      for (int64_t i = linked_blk_cnt; i > 0; --i) {
        if (OB_TMP_FAIL(iter.get_next_macro_id(macro_id))) {
          LOG_ERROR("fail to get next macro id", K(ret), K(iter));
        } else if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
          LOG_ERROR("fail to dec linked block ref cnt", K(ret), K(tmp_ret), K(macro_id));
        } else {
          LOG_DEBUG("barry debug decrease link ref cnt", K(macro_id), KPC(this), K(lbt()));
        }
      }
      iter.reset();
    }
  }

  return ret;
}

int ObSSTable::add_used_size() const
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  ObMacroIdIterator id_iterator;
  MacroBlockId macro_id;
  ObSharedMacroBlockMgr *shared_block_mgr = MTL(ObSharedMacroBlockMgr*);

  if (OB_FAIL(get_meta(meta_handle))) {
    LOG_WARN("get meta handle fail", K(ret), KPC(this));
  } else if (is_small_sstable()) {
    const int64_t data_block_count = get_data_macro_block_count();
    if (data_block_count == 0) { // skip
    } else if (data_block_count != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected data block ids", K(ret), K(data_block_count));
    } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_data_block_iter(id_iterator))) {
      LOG_WARN("get id iterator fail", K(ret));
    } else if (OB_FAIL(id_iterator.get_next_macro_id(macro_id))) {
      LOG_WARN("get first id fail", K(ret));
    } else if (OB_FAIL(shared_block_mgr->add_block(macro_id, get_macro_read_size()))) {
      LOG_WARN("fail to add used size of shared block", K(ret));
    }
  }
  return ret;
}

int ObSSTable::dec_used_size() const
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  ObMacroIdIterator id_iterator;
  MacroBlockId macro_id;
  ObSharedMacroBlockMgr *shared_block_mgr = MTL(ObSharedMacroBlockMgr*);

  if (OB_FAIL(get_meta(meta_handle))) {
    LOG_WARN("get meta handle fail", K(ret), KPC(this));
  } else if (is_small_sstable()) {
    const int64_t data_block_count = get_data_macro_block_count();
    if (data_block_count == 0) { // skip
    } else if (data_block_count != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected data block ids", K(ret), K(data_block_count));
    } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_data_block_iter(id_iterator))) {
      LOG_WARN("get id iterator fail", K(ret));
    } else if (OB_FAIL(id_iterator.get_next_macro_id(macro_id))) {
      LOG_WARN("get first id fail", K(ret));
    } else if (OB_FAIL(shared_block_mgr->free_block(macro_id, get_macro_read_size()))) {
      LOG_WARN("fail to dec used size of shared block", K(ret), K(macro_id));
    }
  }
  return ret;
}


int ObSSTable::get_index_tree_root(
    blocksstable::ObMicroBlockData &index_data,
    const bool need_transform)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("The SSTable has not been inited", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(is_empty())) {
    index_data.reset();
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("SSTable is empty", K(ret));
  } else if (OB_UNLIKELY(!is_loaded())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not get index tree rot from an unloaded sstable", K(ret));
  } else if (OB_UNLIKELY(!meta_->get_root_info().get_addr().is_valid()
                      || !meta_->get_root_info().get_block_data().is_valid())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("meta isn't ready for read", K(ret), KPC(meta_));
  } else if (!need_transform || ObMicroBlockData::DDL_BLOCK_TREE == meta_->get_root_info().get_block_data().type_) {
    // do not need transform
    index_data = meta_->get_root_info().get_block_data();
  } else if (OB_NOT_NULL(meta_->get_root_info().get_block_data().get_extra_buf())) {
    // block is already transformed
    index_data = meta_->get_root_info().get_block_data();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Shouldn't happen, transform has already been done in initialize,", K(ret), KPC(this));
  }
  return ret;
}

int ObSSTable::build_exist_iterator(
    const ObTableIterParam &iter_param,
    const ObDatumRowkey &rowkey,
    ObTableAccessContext &access_context,
    ObStoreRowIterator *&iter)
{
  int ret = OB_SUCCESS;
  if (contain_uncommitted_row()) {
    if (OB_FAIL(get(iter_param, access_context, rowkey, iter))) {
      LOG_WARN("Failed to get row", K(ret), K(rowkey));
    }
  } else {
    void *buf = nullptr;
    ObSSTableRowExister*exister = nullptr;
    if (OB_ISNULL(buf = access_context.stmt_allocator_->alloc(sizeof(ObSSTableRowExister)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory, ", K(ret));
    } else {
      exister = new (buf) ObSSTableRowExister();
      if (OB_FAIL(exister->init(iter_param, access_context, this, &rowkey))) {
        LOG_WARN("Failed to init sstable row exister", K(ret), K(iter_param), K(access_context), K(rowkey));
      } else {
        iter = exister;
      }
    }
  }
  return ret;
}

int ObSSTable::build_multi_exist_iterator(ObRowsInfo &rows_info, ObStoreRowIterator *&iter)
{
  int ret = OB_SUCCESS;
  if (contain_uncommitted_row()) {
    if (OB_FAIL(multi_get(
                rows_info.exist_helper_.table_iter_param_,
                rows_info.exist_helper_.table_access_context_,
                rows_info.rowkeys_,
                iter))) {
      LOG_WARN("Fail to multi get row", K(ret), K(rows_info.rowkeys_));
    }
  } else {
    void *buf = nullptr;
    ObSSTableRowMultiExister*multi_exister = NULL;
    if (NULL == (buf = rows_info.exist_helper_.table_access_context_.allocator_->alloc(sizeof(ObSSTableRowMultiExister)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory, ", K(ret));
    } else {
      multi_exister = new (buf) ObSSTableRowMultiExister();
      if (OB_FAIL(multi_exister->init(rows_info.exist_helper_.table_iter_param_,
                                      rows_info.exist_helper_.table_access_context_,
                                      this, &rows_info.rowkeys_))) {
        LOG_WARN("Failed to init sstable row exister", K(ret), K(rows_info));
      } else {
        iter = multi_exister;
      }
    }
  }
  return ret;
}

int ObSSTable::get_last_rowkey(const ObDatumRowkey *&sstable_endkey)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData root_block;
  const ObIndexBlockDataHeader *idx_data_header = nullptr;
  if (is_empty()) {
    sstable_endkey = &ObDatumRowkey::MAX_ROWKEY;
  } else if (OB_FAIL(get_index_tree_root(root_block))) {
    LOG_WARN("Fail to get index tree root", K(ret), K(root_block));
  } else {
    if (ObMicroBlockData::DDL_BLOCK_TREE == root_block.type_) {
      ObBlockMetaTree *block_meta_tree = reinterpret_cast<ObBlockMetaTree *>(const_cast<char *>(root_block.buf_));
      if (OB_ISNULL(block_meta_tree)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, block meta tree can not be null", K(ret), K(root_block));
      } else if (OB_FAIL(block_meta_tree->get_last_rowkey(sstable_endkey))) {
        LOG_WARN("get last rowkey failed", K(ret));
      }
    } else {
      if (OB_ISNULL(idx_data_header = reinterpret_cast<const ObIndexBlockDataHeader *>(
          root_block.get_extra_buf()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null extra buf after transform", K(ret), K(root_block));
      } else if (OB_UNLIKELY(!idx_data_header->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid index data header", KP(idx_data_header));
      } else {
        sstable_endkey = &(idx_data_header->rowkey_array_[idx_data_header->row_cnt_ - 1]);
      }
    }
  }
  return ret;
}

int ObSSTable::get_meta(
    ObSSTableMetaHandle &meta_handle,
    common::ObSafeArenaAllocator *allocator) const
{
  int ret = OB_SUCCESS;
  meta_handle.reset();
  if (is_loaded()) {
    if (OB_UNLIKELY(!meta_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sstable meta pointer for in-memory sstable", K(ret), KPC(this));
    } else {
      meta_handle.meta_ = meta_;
      meta_handle.handle_.reset();
    }
  } else if (OB_UNLIKELY(!addr_.is_valid() || !addr_.is_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid addr for get sstable from cache", K(ret), KPC(this));
  } else {
    ObStorageMetaCache &meta_cache = OB_STORE_CACHE.get_storage_meta_cache();
    const ObStorageMetaValue *value = nullptr;
    const ObSSTable *sstable_ptr = nullptr;
    ObStorageMetaKey meta_key(MTL_ID(), addr_);
    const bool bypass_cache = nullptr != allocator;
    if (!bypass_cache) {
      if (OB_FAIL(meta_cache.get_meta(
          ObStorageMetaValue::MetaType::SSTABLE,
          meta_key,
          meta_handle.handle_,
          nullptr))) {
        LOG_WARN("fail to retrieve sstable meta from meta cache", K(ret), K(meta_key), KPC(this));
      }
    } else if (OB_FAIL(meta_cache.bypass_get_meta(ObStorageMetaValue::MetaType::SSTABLE,
                                                  meta_key,
                                                  *allocator,
                                                  meta_handle.handle_))) {
      LOG_WARN("fail to bypass cache get meta", K(ret), K(meta_key));
    }
    if (FAILEDx(meta_handle.handle_.get_value(value))) {
      LOG_WARN("fail to get value from meta handle", K(ret), KPC(this));
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sstable cache value", K(ret), K(value), KPC(this));
    } else if (OB_FAIL(value->get_sstable(sstable_ptr))) {
      LOG_WARN("fail to get sstable from meta cache value", K(ret), KPC(value), KPC(this));
    } else if (OB_ISNULL(sstable_ptr)
        || OB_UNLIKELY(!sstable_ptr->is_valid())
        || OB_ISNULL(sstable_ptr->meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sstable pointer", K(ret), KPC(sstable_ptr));
    } else {
      meta_handle.meta_ = sstable_ptr->meta_;
    }
  }
  return ret;
}

int ObSSTable::init_sstable_meta(
    const ObTabletCreateSSTableParam &param,
    common::ObArenaAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(meta_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sstable meta already exist", K(ret), KPC_(meta));
  } else {
    char *buf = static_cast<char *>(allocator->alloc(sizeof(ObSSTableMeta)));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (FALSE_IT(meta_ = new (buf) ObSSTableMeta())) {
    } else if (OB_FAIL(meta_->init(param, *allocator))) {
      LOG_WARN("fail to init sstable meta", K(ret));
    } else if (OB_FAIL(meta_->transform_root_block_data(*allocator))) {
      LOG_WARN("fail to transform root block data", K(ret));
    } else {
      upper_trans_version_ = meta_->get_upper_trans_version();
      max_merged_trans_version_ = meta_->get_max_merged_trans_version();
      data_macro_block_count_ = meta_->get_data_macro_block_count();
      contain_uncommitted_row_ = meta_->contain_uncommitted_row();
      nested_size_ = meta_->get_macro_info().get_nested_size();
      nested_offset_ = meta_->get_macro_info().get_nested_offset();
      filled_tx_scn_ = meta_->get_filled_tx_scn();
    }
  }
  return ret;
}


} // namespace blocksstable
} // namespace oceanbase
