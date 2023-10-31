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
#include "ob_sstable_row_lock_checker.h"
#include "storage/access/ob_rows_info.h"
#include "storage/tx/ob_trans_define.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {

ObSSTableRowLockChecker::ObSSTableRowLockChecker()
    : ObSSTableRowScanner(),
      base_rowkey_(nullptr),
      multi_version_range_()
{}

ObSSTableRowLockChecker::~ObSSTableRowLockChecker()
{}

void ObSSTableRowLockChecker::reset()
{
  ObSSTableRowScanner::reset();
  base_rowkey_ = nullptr;
  multi_version_range_.reset();
}

int ObSSTableRowLockChecker::inner_open(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(query_range), KP(table));
  } else {
    base_rowkey_ = static_cast<const ObDatumRowkey *>(query_range);
    if (OB_FAIL(base_rowkey_->to_multi_version_range(*access_ctx.allocator_, multi_version_range_))) {
      LOG_WARN("Failed to transfer multi version range", K(ret), KPC_(base_rowkey));
    } else if (OB_FAIL(ObSSTableRowScanner::inner_open(iter_param, access_ctx, table,
                                                       &multi_version_range_))) {
      LOG_WARN("failed to open scanner", K(ret));
    } else if (OB_NOT_NULL(block_row_store_)) {
      block_row_store_->disable();
    }
  }
  return ret;
}

int ObSSTableRowLockChecker::init_micro_scanner()
{
  int ret = OB_SUCCESS;
  if (nullptr == micro_scanner_) {
    if (nullptr == (micro_scanner_ = OB_NEWx(ObMicroBlockRowLockChecker,
                                             access_ctx_->stmt_allocator_,
                                             *access_ctx_->stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for micro block row scanner", K(ret));
    } else if (OB_FAIL(micro_scanner_->init(*iter_param_, *access_ctx_, sstable_))) {
      LOG_WARN("Fail to init micro scanner", K(ret), KP_(sstable));
    }
  } else if (OB_FAIL(micro_scanner_->switch_context(*iter_param_, *access_ctx_, sstable_))) {
    LOG_WARN("Fail to switch micro scanner", K(ret), KP_(sstable));
  }
  return ret;
}

int ObSSTableRowLockChecker::check_row_locked(
    const bool check_exist,
    const share::SCN &snapshot_version,
    ObStoreRowLockState &lock_state,
    ObRowState &row_state)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *store_row = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableRowLockChecker is not opened", K(ret));
  } else if (OB_FAIL(init_micro_scanner())) {
    LOG_WARN("Failed to init micro scanner", K(ret));
  } else {
    auto *row_lock_checker = static_cast<ObMicroBlockRowLockChecker *>(micro_scanner_);
    row_lock_checker->set_lock_state(&lock_state);
    row_lock_checker->set_row_state(&row_state);
    row_lock_checker->set_snapshot_version(snapshot_version);
    row_lock_checker->set_check_exist(check_exist);
    if (OB_FAIL(ObSSTableRowScanner::inner_get_next_row(store_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get next row", K(ret), K_(multi_version_range));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCC(ret) &&
      transaction::ObTransVersion::INVALID_TRANS_VERSION != prefetcher_.row_lock_check_version_) {
    if (OB_UNLIKELY(lock_state.trans_version_ != SCN::min_scn() || lock_state.is_locked_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected lock state", K(ret), K_(lock_state.trans_version), K_(lock_state.is_locked));
    } else if (row_state.max_trans_version_.get_val_for_tx() < prefetcher_.row_lock_check_version_
               && OB_FAIL(row_state.max_trans_version_.convert_for_tx(prefetcher_.row_lock_check_version_))) {
      LOG_WARN("failed to convert_for_tx", K(ret), K(prefetcher_.row_lock_check_version_));
    } else {/*do nothing*/}
  }
  return ret;
}

/************************* ObSSTableRowLockMultiChecker *************************/
ObSSTableRowLockMultiChecker::ObSSTableRowLockMultiChecker()
{
  type_ = ObStoreRowIterator::IteratorMultiRowLockCheck;
}

ObSSTableRowLockMultiChecker::~ObSSTableRowLockMultiChecker()
{}

int ObSSTableRowLockMultiChecker::init(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_range) || OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(query_range), KP(table));
  } else if (OB_FAIL(ObSSTableRowScanner::inner_open(iter_param, access_ctx, table, query_range))) {
    LOG_WARN("Failed to open multi scanner", K(ret));
  }
  return ret;
}

int ObSSTableRowLockMultiChecker::check_row_locked(
    const bool check_exist,
    const share::SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *store_row = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("SSTable row lock multi checker is not opened", K(ret), K_(is_opened));
  } else if (OB_FAIL(init_micro_scanner())) {
    LOG_WARN("Failed to init micro scanner", K(ret));
  } else {
    auto *row_lock_checker = static_cast<ObMicroBlockRowLockMultiChecker *>(micro_scanner_);
    row_lock_checker->set_snapshot_version(snapshot_version);
    row_lock_checker->set_check_exist(check_exist);
    if (OB_FAIL(ObSSTableRowScanner::inner_get_next_row(store_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to get next row", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSSTableRowLockMultiChecker::fetch_row(
    ObSSTableReadHandle &read_handle,
    const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (-1 == read_handle.micro_begin_idx_) {
    ret = OB_ITER_END;
  } else {
    if (-1 == prefetcher_.cur_micro_data_fetch_idx_) {
      prefetcher_.cur_micro_data_fetch_idx_ = read_handle.micro_begin_idx_;
      if (OB_FAIL(open_cur_data_block(read_handle))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Failed to open current data block", K(ret), KPC(this));
        }
      }
    }
    auto *multi_checker = static_cast<ObMicroBlockRowLockMultiChecker *>(micro_scanner_);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(multi_checker->get_next_row(store_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Failed to get next row", K(ret));
        } else if (prefetcher_.cur_micro_data_fetch_idx_ >= read_handle.micro_end_idx_) {
          multi_checker->inc_empty_read();
          ret = OB_ITER_END;
        } else if (FALSE_IT(++prefetcher_.cur_micro_data_fetch_idx_)) {
        } else if (OB_FAIL(open_cur_data_block(read_handle))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Failed to open current data block", K(ret), KPC(this));
          }
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObSSTableRowLockMultiChecker::init_micro_scanner()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == micro_scanner_)) {
    if (nullptr == (micro_scanner_ = OB_NEWx(ObMicroBlockRowLockMultiChecker,
                                              access_ctx_->stmt_allocator_,
                                              *access_ctx_->stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for micro block row scanner", K(ret));
    } else if (OB_FAIL(micro_scanner_->init(*iter_param_, *access_ctx_, sstable_))) {
      LOG_WARN("Fail to init micro scanner", K(ret), KP_(sstable));
    }
  } else if (OB_FAIL(micro_scanner_->switch_context(*iter_param_, *access_ctx_, sstable_))) {
    LOG_WARN("Fail to switch micro scanner", K(ret), KP_(sstable));
  }
  return ret;
}

int ObSSTableRowLockMultiChecker::open_cur_data_block(ObSSTableReadHandle &read_handle)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(nullptr != micro_scanner_);
  auto *micro_block_multi_checker = static_cast<ObMicroBlockRowLockMultiChecker *>(micro_scanner_);
  if (prefetcher_.cur_micro_data_fetch_idx_ < read_handle.micro_begin_idx_ ||
      prefetcher_.cur_micro_data_fetch_idx_ > read_handle.micro_end_idx_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K_(prefetcher), K(read_handle));
  } else {
    micro_block_multi_checker->inc_empty_read();
    micro_block_multi_checker->reuse();
    blocksstable::ObMicroIndexInfo &micro_info = prefetcher_.current_micro_info();
    ObMicroBlockDataHandle &micro_handle = prefetcher_.current_micro_handle();
    ObMicroBlockData block_data;
    auto *rows_info = const_cast<ObRowsInfo *>(micro_info.rows_info_);
    if (OB_FAIL(micro_handle.get_micro_block_data(&macro_block_reader_, block_data))) {
      LOG_WARN("Failed to get block data", K(ret), K(micro_handle));
    } else if (OB_FAIL(micro_block_multi_checker->open(micro_handle.macro_block_id_,
                                                       block_data,
                                                       micro_info.rowkey_begin_idx_,
                                                       micro_info.rowkey_end_idx_,
                                                       *rows_info))) {
      LOG_WARN("Failed to open micro_scanner", K(ret), K(micro_info), K(micro_handle), KPC(this));
    } else {
      ++access_ctx_->table_store_stat_.micro_access_cnt_;
    }
  }
  return ret;
}

}
}
