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
#include "storage/tx/ob_trans_define.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {

ObSSTableRowLockChecker::ObSSTableRowLockChecker()
    : ObSSTableRowScanner(),
    base_rowkey_(NULL),
    multi_version_range_()
{
  type_ = ObStoreRowIterator::IteratorRowLockCheck;
}

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
    } else if (OB_FAIL(ObSSTableRowScanner::inner_open(
                iter_param, access_ctx, table, &multi_version_range_))) {
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
  if (nullptr != micro_scanner_) {
  } else {
    if (sstable_->is_multi_version_minor_sstable()) {
      if (nullptr == (micro_scanner_ = OB_NEWx(ObMicroBlockRowLockChecker,
                                               access_ctx_->stmt_allocator_,
                                               *access_ctx_->stmt_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory for micro block row scanner", K(ret));
      } else if (OB_FAIL(micro_scanner_->init(*iter_param_, *access_ctx_, sstable_))) {
        LOG_WARN("Fail to init micro scanner", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpect to check row lock in major", K(ret), KPC_(sstable));
    }
  }
  return ret;
}

int ObSSTableRowLockChecker::check_row_locked(ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *store_row = nullptr;
  lock_state.reset();
  if (prefetcher_.cur_range_fetch_idx_ >= prefetcher_.cur_range_prefetch_idx_) {
    if (OB_UNLIKELY(!prefetcher_.is_prefetch_end_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Current fetch handle idx exceed prefetching idx", K(ret), K_(prefetcher));
    }
  } else {
    if (OB_UNLIKELY(!is_opened_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObSSTableRowLockChecker has not been opened", K(ret));
    } else if (nullptr == micro_scanner_ && OB_FAIL(init_micro_scanner())) {
      LOG_WARN("fail to init micro scanner", K(ret));
    } else {
      static_cast<ObMicroBlockRowLockChecker *>(micro_scanner_)->set_lock_state(&lock_state);
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(ObSSTableRowScanner::inner_get_next_row(store_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Fail to get next row", K(ret), K_(multi_version_range));
        }
      } else if (SCN::min_scn() != lock_state.trans_version_ || lock_state.is_locked_) {
        break;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) &&
        transaction::ObTransVersion::INVALID_TRANS_VERSION != prefetcher_.row_lock_check_version_) {
    if (OB_UNLIKELY(lock_state.trans_version_ != SCN::min_scn() || lock_state.is_locked_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected lock state", K(ret), K_(lock_state.trans_version), K_(lock_state.is_locked));
    } else if (OB_FAIL(lock_state.trans_version_.convert_for_tx(prefetcher_.row_lock_check_version_))) {
      LOG_WARN("failed to convert_for_tx", K(ret), K(prefetcher_.row_lock_check_version_));
    } else {/*do nothing*/}
  }
  return ret;
}

}
}
