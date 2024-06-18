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
#include "ob_micro_block_row_lock_checker.h"
#include "storage/access/ob_rows_info.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/tx_table/ob_tx_table.h"

namespace oceanbase {
namespace blocksstable {
using namespace share;

/************************* ObMicroBlockRowLockChecker *************************/

ObMicroBlockRowLockChecker::ObMicroBlockRowLockChecker(common::ObIAllocator &allocator)
    : ObMicroBlockRowScanner(allocator),
    check_exist_(false),
    snapshot_version_(),
    lock_state_(nullptr),
    row_state_(nullptr)
{
}

ObMicroBlockRowLockChecker::~ObMicroBlockRowLockChecker()
{
}

int ObMicroBlockRowLockChecker::inner_get_next_row(
    bool &row_lock_checked,
    int64_t &current,
    ObStoreRowLockState *&lock_state)
{
  int ret = OB_SUCCESS;
  row_lock_checked = false;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to get next row", K(ret));
    }
  } else {
    current = current_++;
    row_lock_checked = row_state_->is_lock_decided() || lock_state_->is_lock_decided();
    if (OB_UNLIKELY(row_lock_checked && !check_exist_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected lock state", K(ret), KPC_(lock_state), KPC_(row_state), K_(check_exist));
    } else if (lock_state_->is_lock_decided()) {
      tmp_lock_state_.reset();
      lock_state = &tmp_lock_state_;
    } else {
      lock_state = lock_state_;
    }
  }
  return ret;
}

int ObMicroBlockRowLockChecker::check_row(
    const bool row_lock_checked,
    const transaction::ObTransID &trans_id,
    const ObRowHeader *row_header,
    const ObStoreRowLockState &lock_state,
    bool &need_stop)
{
  int ret = OB_SUCCESS;
  ObDmlFlag inner_dml_flag = row_header->get_row_flag().get_dml_flag();
  const ObMultiVersionRowFlag multi_version_flag = row_header->get_row_multi_version_flag();
  const bool lock_is_decided = lock_state.is_lock_decided();
  bool is_ghost_row_flag = false;
  need_stop = false;
  if (!lock_is_decided) {
    // Row is aborted.
  } else if (!check_exist_) {
    need_stop = true;
  } else {
    if (!row_lock_checked) {
      if (lock_state.is_locked(trans_id) || lock_state.trans_version_ > snapshot_version_) {
        need_stop = true;
      }
    }
    if (!need_stop) {
      if (OB_FAIL(ObGhostRowUtil::is_ghost_row(multi_version_flag, is_ghost_row_flag))) {
        LOG_WARN("Failed to check ghost row", K(ret), K(inner_dml_flag), K(multi_version_flag));
      } else if (is_ghost_row_flag || DF_LOCK == inner_dml_flag) {
      } else {
        row_state_->row_dml_flag_ = inner_dml_flag;
        need_stop = true;
      }
    }
  }
  return ret;
}

void ObMicroBlockRowLockChecker::check_row_in_major_sstable(bool &need_stop)
{
  need_stop = true;
  if (check_exist_) {
    row_state_->row_dml_flag_ = blocksstable::ObDmlFlag::DF_INSERT;
  }
}

int ObMicroBlockRowLockChecker::get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(read_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null param", K(ret), KP_(read_info));
  } else {
    const ObRowHeader *row_header = nullptr;
    int64_t sql_sequence = 0;
    const int64_t rowkey_cnt = read_info_->get_schema_rowkey_count();
    memtable::ObMvccAccessCtx &ctx = context_->store_ctx_->mvcc_acc_ctx_;
    const transaction::ObTransID &read_trans_id = ctx.get_tx_id();
    const bool is_major_sstable = sstable_->is_major_sstable();
    int64_t trans_version = INT64_MAX;
    int64_t current;
    row = &row_;
    ObStoreRowLockState *lock_state = nullptr;
    bool row_lock_checked = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(inner_get_next_row(row_lock_checked, current, lock_state))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("Failed to get next row", K(ret), K_(macro_id), K(is_major_sstable));
        }
      } else if (!is_major_sstable) {
        if (OB_FAIL(reader_->get_multi_version_info(current,
                                                    rowkey_cnt,
                                                    row_header,
                                                    trans_version,
                                                    sql_sequence))) {
          LOG_WARN("failed to get multi version info", K(ret), K_(current), KPC(row_header),
                   KPC_(lock_state), K(sql_sequence), K_(macro_id));
        } else if (OB_ISNULL(row_header)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("row header is null", K(ret));
          // TODO(handora.qc): fix it
        } else if (OB_FAIL(lock_state->trans_version_.convert_for_tx(trans_version))) {
          LOG_ERROR("convert failed", K(ret), K(trans_version));
        } else if (row_header->get_row_multi_version_flag().is_uncommitted_row()) {
          ObTxTableGuards &tx_table_guards = ctx.get_tx_table_guards();
          transaction::ObTxSEQ tx_sequence = transaction::ObTxSEQ::cast_from_int(sql_sequence);
          if (!tx_table_guards.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("tx table guard is invalid", KR(ret), K(ctx));
          } else if (OB_FAIL(tx_table_guards.check_row_locked(read_trans_id,
                                                             row_header->get_trans_id(),
                                                             tx_sequence,
                                                             sstable_->get_end_scn(),
                                                             *lock_state))) {
          } else if (lock_state->is_locked_) {
            lock_state->lock_dml_flag_ = row_header->get_row_flag().get_dml_flag();
          }
        }
        STORAGE_LOG(DEBUG, "check row lock", K(ret), KPC_(range), K(read_trans_id), KPC(row_header),
                    K(sql_sequence), KPC_(lock_state));
      }
      if (OB_SUCC(ret)) {
        bool need_stop = false;
        if (is_major_sstable) {
          check_row_in_major_sstable(need_stop);
        } else if (OB_FAIL(check_row(row_lock_checked, read_trans_id, row_header, *lock_state, need_stop))) {
          LOG_WARN("Failed to check row", K(ret), K(ret), KPC_(range), K(read_trans_id), KPC(row_header),
                   K(sql_sequence), KPC(lock_state));
        }
        if (OB_SUCC(ret) && need_stop) {
          break;
        }
      }
    }
  }
  return ret;
}

/************************* ObMicroBlockRowLockMultiChecker *************************/

ObMicroBlockRowLockMultiChecker::ObMicroBlockRowLockMultiChecker(common::ObIAllocator &allocator)
    : ObMicroBlockRowLockChecker(allocator),
      reach_end_(false),
      rowkey_current_idx_(0),
      rowkey_begin_idx_(0),
      rowkey_end_idx_(0),
      empty_read_cnt_(0),
      rows_info_(nullptr)
{
}

ObMicroBlockRowLockMultiChecker::~ObMicroBlockRowLockMultiChecker()
{
}

int ObMicroBlockRowLockMultiChecker::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &block_data,
    const int64_t rowkey_begin_idx,
    const int64_t rowkey_end_idx,
    ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockRowScanner::open(macro_id, block_data, true, true))) {
    LOG_WARN("Failed to open ObIMicroBlockRowScanner", K(ret));
  } else {
    // current_, start_, last_ are all -1.
    rowkey_current_idx_ = rowkey_begin_idx;
    rowkey_begin_idx_ = rowkey_begin_idx;
    rowkey_end_idx_ = rowkey_end_idx;
    empty_read_cnt_ = 0;
    rows_info_ = &rows_info;
    reach_end_ = false;
  }
  return ret;
}

void ObMicroBlockRowLockMultiChecker::inc_empty_read()
{
  if (OB_NOT_NULL(context_) && OB_NOT_NULL(sstable_) &&
      !context_->query_flag_.is_index_back() && context_->query_flag_.is_use_bloomfilter_cache() &&
      !sstable_->is_small_sstable() && empty_read_cnt_ > 0) {
    (void) OB_STORE_CACHE.get_bf_cache().inc_empty_read(
             MTL_ID(),
             param_->table_id_,
             macro_id_,
             rows_info_->get_datum_cnt(),
             empty_read_cnt_);
  }
}

int ObMicroBlockRowLockMultiChecker::inner_get_next_row(
    bool &row_lock_checked,
    int64_t &current,
    ObStoreRowLockState *&lock_state)
{
  int ret = OB_SUCCESS;
  if (reach_end_) {
    ret = OB_ITER_END;
  } else if (OB_ITER_END == end_of_block() || rows_info_->is_row_skipped(rowkey_current_idx_ - 1)) {
    if (OB_FAIL(seek_forward())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to seek forward to next row", K(ret), K_(check_exist));
      }
    }
  }
  if (OB_SUCC(ret)) {
    row_lock_checked = rows_info_->is_row_lock_checked(rowkey_current_idx_ - 1);
    ObStoreRowLockState &my_lock_state = rows_info_->get_row_lock_state(rowkey_current_idx_ - 1);
    if (row_lock_checked) {
      if (OB_UNLIKELY(!check_exist_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected lock state", K(ret), K_(current), K_(start), K_(last), K(my_lock_state), K_(check_exist),
                 K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx), K(rows_info_->get_marked_rowkey(rowkey_current_idx_ - 1)),
                 KPC_(rows_info));
      } else {
        tmp_lock_state_.reset();
        lock_state = &tmp_lock_state_;
      }
    } else {
      lock_state = &my_lock_state;
    }
    current = current_++;
  }
  return ret;
}

int ObMicroBlockRowLockMultiChecker::check_row(
    const bool row_lock_checked,
    const transaction::ObTransID &trans_id,
    const ObRowHeader *row_header,
    const ObStoreRowLockState &lock_state,
    bool &need_stop)
{
  int ret = OB_SUCCESS;
  const ObMultiVersionRowFlag multi_version_flag = row_header->get_row_multi_version_flag();
  const bool lock_is_decided = lock_state.is_lock_decided();
  const int64_t rowkey_idx = rowkey_current_idx_ - 1;
  ObDmlFlag inner_dml_flag = row_header->get_row_flag().get_dml_flag();
  bool is_ghost_row_flag = false;
  need_stop = false;
  if (!lock_is_decided) {
    // Row is aborted.
  } else {
    if (!row_lock_checked) {
      if (lock_state.is_locked(trans_id)) {
        rows_info_->set_conflict_rowkey(rowkey_idx);
        rows_info_->set_error_code(OB_TRY_LOCK_ROW_CONFLICT);
        need_stop = true;
        LOG_DEBUG("Find lock conflict in mini/minor sstable", K(rowkey_idx), K(rows_info_->rowkeys_[rowkey_idx]),
                  K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(empty_read_cnt), K(lock_state),
                  K(inner_dml_flag), K_(current), K_(start), K_(last), K(row_lock_checked), K_(macro_id));
      } else if (lock_state.trans_version_ > snapshot_version_) {
        rows_info_->set_conflict_rowkey(rowkey_idx);
        rows_info_->set_error_code(OB_TRANSACTION_SET_VIOLATION);
        need_stop = true;
        LOG_DEBUG("Find tsv conflict in mini/minor sstable", K(rowkey_idx), K(rows_info_->rowkeys_[rowkey_idx]),
                  K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(empty_read_cnt), K(lock_state),
                  K(inner_dml_flag), K_(current), K_(start), K_(last), K(row_lock_checked), K_(macro_id));
      } else {
        rows_info_->set_row_lock_checked(rowkey_idx, check_exist_);
      }
    }
    if (!need_stop && check_exist_ && !rows_info_->is_row_skipped(rowkey_idx)) {
      if (OB_FAIL(ObGhostRowUtil::is_ghost_row(multi_version_flag, is_ghost_row_flag))) {
        LOG_WARN("Failed to check ghost row", K(ret), K(inner_dml_flag), K(multi_version_flag));
      } else if (is_ghost_row_flag || DF_LOCK == inner_dml_flag) {
      } else if (DF_DELETE != inner_dml_flag) {
        rows_info_->set_conflict_rowkey(rowkey_idx);
        rows_info_->set_error_code(OB_ERR_PRIMARY_KEY_DUPLICATE);
        need_stop = true;
        LOG_DEBUG("Find duplication in mini/minor sstable", K(rowkey_idx), K(rows_info_->rowkeys_[rowkey_idx]),
                  K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(empty_read_cnt), K(lock_state),
                  K(inner_dml_flag), K_(current), K_(start), K_(last), K(row_lock_checked), K_(macro_id));
      } else {
        rows_info_->set_row_checked(rowkey_idx);
      }
    }
  }
  return ret;
}

void ObMicroBlockRowLockMultiChecker::check_row_in_major_sstable(bool &need_stop)
{
  const int64_t rowkey_idx = rowkey_current_idx_ - 1;
  need_stop = false;
  if (check_exist_) {
    rows_info_->set_conflict_rowkey(rowkey_idx);
    rows_info_->set_error_code(OB_ERR_PRIMARY_KEY_DUPLICATE);
    need_stop = true;
    LOG_DEBUG("Find duplication in major sstable", K(rowkey_idx), K(rows_info_->rowkeys_[rowkey_idx]),
              K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(empty_read_cnt),
              K_(current), K_(start), K_(last), K_(macro_id));
  } else {
    rows_info_->set_row_checked(rowkey_idx);
  }
}

int ObMicroBlockRowLockMultiChecker::seek_forward()
{
  int ret = OB_SUCCESS;
  ObIMicroBlockReader *micro_block_reader = nullptr;
  bool need_search_duplicate_row = false;
  if (ObIMicroBlockReader::Decoder == reader_->get_type() ||
      ObIMicroBlockReader::CSDecoder == reader_->get_type()) {
    micro_block_reader = decoder_;
  } else {
    micro_block_reader = flat_reader_;
    need_search_duplicate_row = !sstable_->is_major_sstable() && !flat_reader_->single_version_rows();
  }
  const int64_t row_count = micro_block_reader->row_count();
  while (OB_SUCC(ret)) {
    if (rowkey_current_idx_ == rowkey_end_idx_) {
      reach_end_ = true;
      ret = OB_ITER_END;
    } else if (rows_info_->is_row_skipped(rowkey_current_idx_)) {
      ++rowkey_current_idx_;
    } else {
      const ObDatumRowkey &rowkey = rows_info_->get_rowkey(rowkey_current_idx_);
      const int64_t begin_idx = last_ + 1;
      int64_t row_idx = 0;
      bool is_equal = false;
      if (begin_idx >= row_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected state in ObMicroBlockRowLockMultiChecker", K(begin_idx), K(row_count), K(rowkey),
                 K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(current), K_(start), K_(last),
                 K_(macro_id));
      } else if (OB_FAIL(micro_block_reader->find_bound(rowkey, true, begin_idx, row_idx, is_equal))) {
        LOG_WARN("Failed to find bound", K(ret), K(begin_idx), K(rowkey), K_(current), K_(start), K_(last),
                 K_(rowkey_current_idx), K(need_search_duplicate_row));
      } else if (!is_equal) {
        ++rowkey_current_idx_;
        ++empty_read_cnt_;
        last_ = row_idx - 1;
      } else {
        current_ = row_idx;
        start_ = row_idx;
        if (need_search_duplicate_row && OB_FAIL(micro_block_reader->find_bound_through_linear_search(rowkey, row_idx, last_))) {
          LOG_WARN("Failed to find bound through linear search", K(ret), K(row_idx));
        } else {
          ++rowkey_current_idx_;
        }
        break;
      }
    }
  }
  return ret;
}

}
}
