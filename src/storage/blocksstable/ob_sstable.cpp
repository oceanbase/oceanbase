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

namespace oceanbase
{
using namespace common::hash;
using namespace storage;
namespace blocksstable
{

ObSSTable::ObSSTable()
  : meta_(),
    valid_for_reading_(false),
    hold_macro_ref_(false),
    allocator_(nullptr)
{
#if defined(__x86_64__)
  static_assert(sizeof(ObSSTable) <= 1024, "The size of ObSSTable will affect the meta memory manager, and the necessity of adding new fields needs to be considered.");
#endif
}

ObSSTable::~ObSSTable()
{
  reset();
}

int ObSSTable::init(const ObTabletCreateSSTableParam &param, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(valid_for_reading_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("this sstable can't be initialized", K(ret), K_(valid_for_reading));
  } else if (OB_UNLIKELY(!param.is_valid()) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), KP(allocator));
  } else if (OB_FAIL(ObITable::init(param.table_key_))) {
    LOG_WARN("fail to initialize ObITable", K(ret), "table_key", param.table_key_);
  } else if (OB_FAIL(meta_.init(param, allocator))) {
    LOG_WARN("fail to initialize sstable meta", K(ret), K(param));
  } else if (OB_FAIL(add_macro_ref())) {
    LOG_WARN("fail to add macro ref", K(ret));
  } else if (FALSE_IT(allocator_ = allocator)) {
  } else if (param.is_ready_for_read_) {
    if (OB_FAIL(check_valid_for_reading())) {
      LOG_WARN("Failed to check state", K(ret));
    }
  }

  if (OB_UNLIKELY(!(SSTABLE_READY_FOR_READ == meta_.get_basic_meta().status_
                 || SSTABLE_WRITE_BUILDING == meta_.get_basic_meta().status_))) {
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
  LOG_DEBUG("reset sstable.", KP(this), K(key_));
  // dec ref first, then reset sstable meta
  if (hold_macro_ref_) {
    dec_macro_ref();
  }
  meta_.reset();
  valid_for_reading_ = false;
  hold_macro_ref_ = false;
  allocator_ = nullptr;
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
          ObSSTableRowScanner,
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
          || meta_.contain_uncommitted_row())) {
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
            || meta_.contain_uncommitted_row())) {
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
    ObStoreCtx &ctx,
    const uint64_t table_id,
    const storage::ObTableReadInfo &full_read_info,
    const ObDatumRowkey &rowkey,
    bool &is_exist,
    bool &has_found)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator allocator(ObModIds::OB_STORE_ROW_EXISTER);
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(!rowkey.is_valid()
      || !ctx.is_valid()
      || !full_read_info.is_valid_full_read_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), K(rowkey), K(ctx), K(full_read_info));
  } else {
    ObTableIterParam iter_param;
    ObTableAccessContext access_context;
    common::ObVersionRange trans_version_range;
    common::ObQueryFlag query_flag;

    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
    query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
    query_flag.read_latest_ = ObQueryFlag::OBSF_MASK_READ_LATEST;
    iter_param.table_id_ = table_id;
    iter_param.tablet_id_ = key_.tablet_id_;
    iter_param.read_info_ = &full_read_info;
    iter_param.full_read_info_ = &full_read_info;

    const ObDatumRow *store_row = nullptr;
    ObStoreRowIterator *iter = nullptr;
    is_exist = false;
    has_found = false;

    if (OB_FAIL(access_context.init(query_flag, ctx, allocator, trans_version_range))) {
      LOG_WARN("Fail to init access context", K(ret), K_(key));
    } else if (OB_FAIL(build_exist_iterator(iter_param, rowkey, access_context, iter))) {
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
      if (lib::is_diagnose_info_enabled()) {
        iter->report_stat(access_context.table_store_stat_);
      }
      iter->~ObStoreRowIterator();
      access_context.stmt_allocator_->free(iter);
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
  const ObTableReadInfo *index_read_info
      = rows_info.exist_helper_.table_iter_param_.get_full_read_info()->get_index_read_info();
  if (OB_UNLIKELY(!rows_info.is_valid()) || OB_ISNULL(index_read_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(rows_info), KP(index_read_info));
  } else if (OB_UNLIKELY(rows_info.tablet_id_ != key_.tablet_id_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Tablet id not match", K(ret), K_(key), K(rows_info));
  } else if (meta_.is_empty()) {
    // Skip
  } else if (rows_info.all_rows_found()) {
    all_rows_found = true;
  } else if (OB_FAIL(get_last_rowkey(
      *index_read_info,
      sstable_endkey))) {
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
      if (lib::is_diagnose_info_enabled()) {
        iter->report_stat(rows_info.exist_helper_.table_access_context_.table_store_stat_);
      }
      iter->~ObStoreRowIterator();
      rows_info.exist_helper_.table_access_context_.allocator_->free(iter);
      rows_info.reuse_scan_mem_allocator();
    }
  }
  return ret;
}

int ObSSTable::scan_macro_block(
    const ObDatumRange &range,
    const ObTableReadInfo &index_read_info,
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
        *this, range, index_read_info, allocator, is_reverse_scan, need_record_micro_info))) {
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
    const ObTableReadInfo &index_read_info,
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
  } else if (OB_FAIL(micro_iter->open(*this, range, index_read_info, allocator, is_reverse_scan))) {
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
    const ObTableReadInfo &index_read_info,
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
        query_range, meta_type, *this, index_read_info, allocator, is_reverse_scan, sample_step))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("Fail to open secondary meta iterator with range",
            K(ret), K(query_range), K(meta_type), K_(meta), K(is_reverse_scan), K(sample_step));
      }
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

int ObSSTable::check_row_locked(ObStoreCtx &ctx,
                                const storage::ObTableReadInfo &full_read_info,
                                const ObDatumRowkey &rowkey,
                                ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  ObSSTableRowLockChecker *row_checker = NULL;
  const int64_t read_snapshot = ctx.mvcc_acc_ctx_.get_snapshot_version();
  ObArenaAllocator allocator(ObModIds::OB_STORE_ROW_LOCK_CHECKER);
  lock_state.trans_version_ = 0;
  lock_state.is_locked_ = false;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("The SSTable has not been inited", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(!full_read_info.is_valid_full_read_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid full read info", K(ret), K(full_read_info));
  } else if (!is_multi_version_minor_sstable()) {
    // return false if not multi version minor sstable
  } else if (get_upper_trans_version() <= read_snapshot) {
    // there is no lock at this sstable
    if (!meta_.is_empty()) {
      // skip reference upper_trans_version of empty_sstable, which may greater than real
      // committed transaction's version
      lock_state.trans_version_ = get_upper_trans_version();
    }
  } else if (NULL == (buf = allocator.alloc(sizeof(ObSSTableRowLockChecker)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory", K(ret));
  } else {
    row_checker = new (buf) ObSSTableRowLockChecker();
    ObTableIterParam iter_param;
    ObTableAccessContext access_context;
    common::ObVersionRange trans_version_range;
    common::ObQueryFlag query_flag;

    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.snapshot_version_ = read_snapshot;
    query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
    iter_param.tablet_id_ = key_.tablet_id_;
    iter_param.read_info_ = &full_read_info;
    iter_param.full_read_info_ = &full_read_info;

    if (OB_FAIL(access_context.init(query_flag, ctx, allocator,
                                    trans_version_range))) {
      LOG_WARN("failed to init access context", K(ret), K(key_));
    } else if (OB_FAIL(row_checker->init(iter_param, access_context, this, &rowkey))) {
      LOG_WARN("failed to open row locker", K(ret), K(iter_param),
               K(access_context), K(rowkey));
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
  if (OB_FAIL(meta_.get_basic_meta().set_upper_trans_version(
              MAX(upper_trans_version, get_max_merged_trans_version())))) {
    LOG_WARN("fail to set upper trans version", K(ret), K(upper_trans_version), K(key_));
  } else {
    LOG_INFO("succeed to set upper trans version", K(key_), K(upper_trans_version),
        K(meta_.get_basic_meta().upper_trans_version_));
  }
  return ret;
}

int ObSSTable::get_frozen_schema_version(int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable is not initialized.", K(ret), K_(valid_for_reading), K_(meta));
  } else {
    schema_version = meta_.get_basic_meta().schema_version_;
  }
  return ret;
}

int ObSSTable::set_status_for_read(const ObSSTableStatus status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(SSTABLE_WRITE_BUILDING != meta_.get_basic_meta().status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("sstable state is not match.", K(ret), K_(meta_.get_basic_meta().status));
  } else if (OB_UNLIKELY(SSTABLE_READY_FOR_READ                  != status
                      && SSTABLE_READY_FOR_REMOTE_PHYTSICAL_READ != status
                      && SSTABLE_READY_FOR_REMOTE_LOGICAL_READ   != status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(status));
  } else if (OB_FAIL(meta_.load_root_block_data())) {
    LOG_WARN("fail to load root block data", K(ret));
  } else {
    meta_.get_basic_meta().status_ = status;
    if (OB_FAIL(check_valid_for_reading())) {
      LOG_WARN("Failed to check state", K(ret));
    }

    if (OB_FAIL(ret)) {
      meta_.get_basic_meta().status_ = SSTABLE_WRITE_BUILDING;
    }
  }
  return ret;
}

int ObSSTable::get_last_rowkey(
    const ObTableReadInfo &index_read_info,
    ObIAllocator &allocator,
    ObStoreRowkey &endkey)
{
  int ret = OB_SUCCESS;
  const ObDatumRowkey *datum_rowkey;

  if (OB_UNLIKELY(!index_read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid index read info", K(ret), K(index_read_info));
  } else if (OB_FAIL(get_last_rowkey(index_read_info, datum_rowkey))) {
    STORAGE_LOG(WARN, "Failed to get datum rowkey", K(ret));
  } else if (OB_ISNULL(datum_rowkey)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null datum rowkey", K(ret));
  } else {
    const ObIArray<share::schema::ObColDesc> &col_descs = index_read_info.get_columns_desc();
    if (OB_FAIL(datum_rowkey->to_store_rowkey(col_descs, allocator, endkey))) {
      STORAGE_LOG(WARN, "Failed to transfer store rowkey", K(ret), KPC(datum_rowkey), K(col_descs));
    }
  }

  return ret;
}

int ObSSTable::get_last_rowkey(
    const ObTableReadInfo &index_read_info,
    ObIAllocator &allocator,
    ObDatumRowkey &endkey)
{
  int ret = OB_SUCCESS;
  const ObDatumRowkey *last_rowkey;

  if (OB_FAIL(get_last_rowkey(index_read_info, last_rowkey))) {
    STORAGE_LOG(WARN, "Failed to get datum rowkey", K(ret));
  } else if (OB_ISNULL(last_rowkey)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null datum rowkey", K(ret));
  } else if (OB_FAIL(last_rowkey->deep_copy(endkey, allocator))) {
    LOG_WARN("Fail to copuy last rowkey", K(ret));
  }

  return ret;
}

int ObSSTable::check_valid_for_reading()
{
  int ret = OB_SUCCESS;
  valid_for_reading_ = key_.is_valid()
                       && meta_.is_valid()
                       && (SSTABLE_READY_FOR_READ == meta_.get_basic_meta().status_
                           || SSTABLE_READY_FOR_REMOTE_PHYTSICAL_READ == meta_.get_basic_meta().status_
                           || SSTABLE_READY_FOR_REMOTE_LOGICAL_READ == meta_.get_basic_meta().status_);
  if (OB_UNLIKELY(!valid_for_reading_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected state state", K(ret), K_(valid_for_reading), K(key_), K(meta_));
  }
  return ret;
}

int ObSSTable::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid()) && SSTABLE_WRITE_BUILDING != meta_.get_basic_meta().status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("non-ready sstable for read can't be serialized.", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObITable::serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize table key", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("sstable meta fail to serialize.", K(ret), K(buf_len), K(pos));
  } else {
    LOG_DEBUG("succeed to serialize sstable", KPC(this));
  }
  return ret;
}

int ObSSTable::deserialize(common::ObIAllocator &allocator,
                           const char *buf,
                           const int64_t data_len,
                           int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(SSTABLE_NOT_INIT != meta_.get_basic_meta().status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("this sstable can't be deserialized", K(ret), K_(meta_.get_basic_meta().status));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), K(data_len));
  } else if (FALSE_IT(allocator_ = &allocator)) {
  } else if (OB_FAIL(ObITable::deserialize(buf, data_len, new_pos))) {
    LOG_WARN("failed to deserialize ObITable", K(ret), K(data_len), K(new_pos));
  } else if (OB_FAIL(meta_.deserialize(allocator_, buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize sstable meta", K(ret), K(key_), K(data_len), K(new_pos));
  } else if (OB_UNLIKELY(!meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable meta is not valid", K(ret), K_(meta));
  } else {
    pos = new_pos;
    LOG_DEBUG("succeed to deserialize sstable", KPC(this));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to deserialize sstable", K(ret), K(*this));
    reset();
  }
  return ret;
}

int ObSSTable::deserialize_post_work()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(hold_macro_ref_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("post work may be done", K(ret), KPC(this));
  } else if (OB_FAIL(add_macro_ref())) {
    LOG_WARN("fail to add macro ref", K(ret));
  } else if (SSTABLE_WRITE_BUILDING != meta_.get_basic_meta().status_ && OB_FAIL(check_valid_for_reading())) {
    LOG_WARN("fail to check state", K(ret));
  } else {
    LOG_DEBUG("succeed to do post work for sstable deserialize", KPC(this));
  }
  return ret;
}

int64_t ObSSTable::get_serialize_size() const
{
  return ObITable::get_serialize_size() + meta_.get_serialize_size();
}

void ObSSTable::dec_macro_ref()
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  for (; idx < meta_.get_macro_info().get_data_block_ids().count(); ++idx) {// ignore ret
    const MacroBlockId &macro_id = meta_.get_macro_info().get_data_block_ids().at(idx);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
      LOG_ERROR("fail to dec data block ref cnt", K(ret), K(macro_id), K(idx));
    }
  }
  for (idx = 0; idx < meta_.get_macro_info().get_other_block_ids().count(); ++idx) {// ignore ret
    const MacroBlockId &macro_id = meta_.get_macro_info().get_other_block_ids().at(idx);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
      LOG_ERROR("fail to dec other block ref cnt", K(ret), K(macro_id), K(idx));
    }
  }
  for (idx = 0; idx < meta_.get_macro_info().get_linked_block_ids().count(); ++idx) {// ignore ret
    const MacroBlockId &macro_id = meta_.get_macro_info().get_linked_block_ids().at(idx);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
      LOG_ERROR("fail to dec link block ref cnt", K(ret), K(macro_id), K(idx));
    }
  }
  hold_macro_ref_ = false;
}

int ObSSTable::add_macro_ref()
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t j = 0;
  int64_t k = 0;
  while (OB_SUCC(ret) && i < meta_.get_macro_info().get_data_block_ids().count()) {
    const MacroBlockId &macro_id = meta_.get_macro_info().get_data_block_ids().at(i);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
      LOG_ERROR("fail to inc data block ref cnt", K(ret), K(macro_id), K(i));
    } else {
      ++i;
    }
  }
  while (OB_SUCC(ret) && j < meta_.get_macro_info().get_other_block_ids().count()) {
    const MacroBlockId &macro_id = meta_.get_macro_info().get_other_block_ids().at(j);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
      LOG_ERROR("fail to inc other block ref cnt", K(ret), K(macro_id), K(j));
    } else {
      ++j;
    }
  }
  while (OB_SUCC(ret) && k < meta_.get_macro_info().get_linked_block_ids().count()) {
    const MacroBlockId &macro_id = meta_.get_macro_info().get_linked_block_ids().at(k);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id))) {
      LOG_ERROR("fail to inc link block ref cnt", K(ret), K(macro_id), K(k));
    } else {
      ++k;
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    int64_t idx = i - 1;
    for (; idx >= 0; --idx) {// ignore ret
      const MacroBlockId &macro_id = meta_.get_macro_info().get_data_block_ids().at(idx);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id)))) {
        LOG_ERROR("fail to dec data block ref cnt", K(ret), K(tmp_ret), K(macro_id));
      }
    }
    idx = j - 1;
    for (; idx >= 0; --idx) {// ignore ret
      const MacroBlockId &macro_id = meta_.get_macro_info().get_other_block_ids().at(idx);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id)))) {
        LOG_ERROR("fail to dec other block ref cnt", K(ret), K(tmp_ret), K(macro_id));
      }
    }
    idx = k - 1;
    for (; idx >= 0; --idx) {// ignore ret
      const MacroBlockId &macro_id = meta_.get_macro_info().get_linked_block_ids().at(idx);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id)))) {
        LOG_ERROR("fail to dec link block ref cnt", K(ret), K(tmp_ret), K(macro_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    hold_macro_ref_ = true;
  }
  return ret;
}

int ObSSTable::add_disk_ref()
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t j = 0;
  int64_t k = 0;
  while (OB_SUCC(ret) && i < meta_.get_macro_info().get_data_block_ids().count()) {
    const MacroBlockId &macro_id = meta_.get_macro_info().get_data_block_ids().at(i);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_disk_ref(macro_id))) {
      LOG_ERROR("fail to inc data block disk ref cnt", K(ret), K(macro_id), K(i));
    } else {
      ++i;
    }
  }
  while (OB_SUCC(ret) && j < meta_.get_macro_info().get_other_block_ids().count()) {
    const MacroBlockId &macro_id = meta_.get_macro_info().get_other_block_ids().at(j);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_disk_ref(macro_id))) {
      LOG_ERROR("fail to inc other block disk ref cnt", K(ret), K(macro_id), K(j));
    } else {
      ++j;
    }
  }
  while (OB_SUCC(ret) && k < meta_.get_macro_info().get_linked_block_ids().count()) {
    const MacroBlockId &macro_id = meta_.get_macro_info().get_linked_block_ids().at(k);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_disk_ref(macro_id))) {
      LOG_ERROR("fail to inc linked block disk ref cnt", K(ret), K(k), K(macro_id));
    } else {
      ++k;
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    int64_t idx = i - 1;
    for (; idx >= 0; --idx) {// ignore ret
      const MacroBlockId &macro_id = meta_.get_macro_info().get_data_block_ids().at(idx);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.dec_disk_ref(macro_id)))) {
        LOG_ERROR("fail to dec data block disk ref cnt", K(ret), K(tmp_ret), K(macro_id));
      }
    }
    idx = j - 1;
    for (; idx >= 0; --idx) {// ignore ret
      const MacroBlockId &macro_id = meta_.get_macro_info().get_other_block_ids().at(idx);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.dec_disk_ref(macro_id)))) {
        LOG_ERROR("fail to dec other block disk ref cnt", K(ret), K(tmp_ret), K(macro_id));
      }
    }
    idx = k - 1;
    for (; idx >= 0; --idx) {// ignore ret
      const MacroBlockId &macro_id = meta_.get_macro_info().get_linked_block_ids().at(idx);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.dec_disk_ref(macro_id)))) {
        LOG_ERROR("fail to dec linked block disk ref cnt", K(ret), K(tmp_ret), K(macro_id));
      }
    }
  }
  return ret;
}

int ObSSTable::dec_disk_ref()
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t j = 0;
  int64_t k = 0;
  while (OB_SUCC(ret) && i < meta_.macro_info_.data_block_ids_.count()) {
    MacroBlockId &macro_id = meta_.macro_info_.data_block_ids_.at(i);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_disk_ref(macro_id))) {
      LOG_ERROR("fail to dec data block disk ref cnt", K(ret), K(macro_id), K(i));
    } else {
      ++i;
    }
  }
  while (OB_SUCC(ret) && j < meta_.macro_info_.other_block_ids_.count()) {
    MacroBlockId &macro_id = meta_.macro_info_.other_block_ids_.at(j);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_disk_ref(macro_id))) {
      LOG_ERROR("fail to dec other block disk ref cnt", K(ret), K(macro_id), K(j));
    } else {
      ++j;
    }
  }
  while (OB_SUCC(ret) && k < meta_.macro_info_.linked_block_ids_.count()) {
    MacroBlockId &macro_id = meta_.macro_info_.linked_block_ids_.at(k);
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_disk_ref(macro_id))) {
      LOG_ERROR("fail to dec linked block disk ref cnt", K(ret), K(macro_id), K(k));
    } else {
      ++k;
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    int64_t idx = i - 1;
    for (; idx >= 0; --idx) {// ignore ret
      const MacroBlockId &macro_id = meta_.macro_info_.data_block_ids_.at(idx);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.inc_disk_ref(macro_id)))) {
        LOG_ERROR("fail to inc data block disk ref cnt", K(ret), K(tmp_ret), K(macro_id));
      }
    }
    idx = j - 1;
    for (; idx >= 0; --idx) {// ignore ret
      const MacroBlockId &macro_id = meta_.macro_info_.other_block_ids_.at(idx);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.inc_disk_ref(macro_id)))) {
        LOG_ERROR("fail to inc other block disk ref cnt", K(ret), K(tmp_ret), K(macro_id));
      }
    }
    idx = k - 1;
    for (; idx >= 0; --idx) {// ignore ret
      const MacroBlockId &macro_id = meta_.macro_info_.linked_block_ids_.at(idx);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.inc_disk_ref(macro_id)))) {
        LOG_ERROR("fail to inc other block disk ref cnt", K(ret), K(tmp_ret), K(macro_id));
      }
    }
  }
  return ret;
}

int ObSSTable::pre_transform_root_block(const ObTableReadInfo &index_read_info)
{
  return meta_.transform_root_block_data(index_read_info);
}

int ObSSTable::build_exist_iterator(
    const ObTableIterParam &iter_param,
    const ObDatumRowkey &rowkey,
    ObTableAccessContext &access_context,
    ObStoreRowIterator *&iter)
{
  int ret = OB_SUCCESS;
  if (meta_.contain_uncommitted_row()) {
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
  if (meta_.contain_uncommitted_row()) {
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

int ObSSTable::get_last_rowkey(
    const ObTableReadInfo &index_read_info,
    const ObDatumRowkey *&sstable_endkey)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData root_block;
  const ObIndexBlockDataHeader *idx_data_header = nullptr;
  if (meta_.is_empty()) {
    sstable_endkey = &ObDatumRowkey::MAX_ROWKEY;
  } else if (OB_FAIL(get_index_tree_root(index_read_info, root_block))) {
    LOG_WARN("Fail to get index tree root", K(ret), K(root_block));
  } else if (OB_ISNULL(idx_data_header = reinterpret_cast<const ObIndexBlockDataHeader *>(
      root_block.get_extra_buf()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null extra buf after transform", K(ret), K(root_block));
  } else if (OB_UNLIKELY(!idx_data_header->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid index data header", KP(idx_data_header));
  } else {
    sstable_endkey = &(idx_data_header->rowkey_array_[idx_data_header->row_cnt_ - 1]);
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
