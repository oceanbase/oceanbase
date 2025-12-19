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
#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/truncate_info/ob_truncate_partition_filter.h"
namespace oceanbase {
namespace blocksstable {
using namespace share;

/************************* ObMicroBlockRowLockChecker *************************/

ObMicroBlockRowLockChecker::ObMicroBlockRowLockChecker(common::ObIAllocator &allocator)
    : ObMicroBlockRowScanner(allocator),
    check_exist_(false),
    snapshot_version_(),
    lock_state_(nullptr),
    base_version_(0)
{
}

ObMicroBlockRowLockChecker::~ObMicroBlockRowLockChecker()
{
}

void ObMicroBlockRowLockChecker::inc_empty_read(ObSSTableReadHandle &read_handle)
{
  if (OB_NOT_NULL(context_) && OB_NOT_NULL(sstable_)
      && (!context_->query_flag_.is_index_back() || !sstable_->is_major_sstable())
      && context_->query_flag_.is_use_bloomfilter_cache() && !sstable_->is_small_sstable()) {
    (void)OB_STORE_CACHE.get_bf_cache().inc_empty_read(MTL_ID(),
                                                       param_->table_id_,
                                                       param_->ls_id_,
                                                       sstable_->get_key(),
                                                       macro_id_,
                                                       read_handle.get_rowkey_datum_cnt(),
                                                       &read_handle);
  }
}

int ObMicroBlockRowLockChecker::inner_get_next_row(
    int64_t &current,
    ObStoreRowLockState *&lock_state)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(end_of_block())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to get next row", K(ret));
    }
  } else {
    current = current_++;
    lock_state = lock_state_;
  }
  return ret;
}

int ObMicroBlockRowLockChecker::check_row(
    const transaction::ObTransID &trans_id,
    const ObRowHeader *row_header,
    ObStoreRowLockState &lock_state,
    bool &need_stop)
{
  int ret = OB_SUCCESS;
  const ObMultiVersionRowFlag multi_version_flag = row_header->get_row_multi_version_flag();
  const bool row_is_decided = lock_state.is_row_decided();
  bool is_ghost_row_flag = false;
  need_stop = false;

  if (!row_is_decided) {
    // Case1: Row is aborted.
    need_stop = false;
  } else if (OB_FAIL(ObGhostRowUtil::is_ghost_row(multi_version_flag,
                                                  is_ghost_row_flag))) {
    LOG_WARN("Failed to check ghost row", K(ret), K(multi_version_flag));
  } else if (is_ghost_row_flag) {
    // Case2: We encounter the ghost row which means the version node is
    // faked, and the row has already ended. So, we believe that the next
    // iteration will find OB_ITER_END.
    LOG_INFO("encounter a ghost row", K(ret), K(trans_id), K(lock_state));
    need_stop = false;
    lock_state.lock_dml_flag_ = ObDmlFlag::DF_NOT_EXIST;
  } else {
    // Case3: Row is decided, so the lock and existence state is decided
    need_stop = true;
  }

  return ret;
}

void ObMicroBlockRowLockChecker::check_row_in_major_sstable(bool &need_stop)
{
  need_stop = true;
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
    const bool is_major_sstable = sstable_->is_major_type_sstable() || sstable_->is_ddl_type_sstable() || sstable_->is_ddl_merge_sstable();
    int64_t trans_version = INT64_MAX;
    int64_t current;
    row = &row_;
    ObStoreRowLockState *lock_state = nullptr;
    bool is_filtered = false;
    while (OB_SUCC(ret)) {
      is_filtered = false;
      if (OB_FAIL(inner_get_next_row(current, lock_state))) {
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
        } else if (row_header->get_row_multi_version_flag().is_uncommitted_row()) {
          ObTxTableGuards &tx_table_guards = ctx.get_tx_table_guards();
          transaction::ObTxSEQ tx_sequence = transaction::ObTxSEQ::cast_from_int(sql_sequence);
          ObStoreRowLockState uncommited_lock_state;
          if (!tx_table_guards.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("tx table guard is invalid", KR(ret), K(ctx));
          } else if (OB_FAIL(tx_table_guards.check_row_locked(read_trans_id,
                                                              row_header->get_trans_id(),
                                                              tx_sequence,
                                                              sstable_->get_end_scn(),
                                                              uncommited_lock_state))) {
          } else if (FALSE_IT(trans_version = uncommited_lock_state.trans_version_.get_val_for_tx())) {
          } else if (OB_UNLIKELY(nullptr != context_->truncate_part_filter_) &&
                     transaction::is_effective_trans_version(trans_version) &&
                     OB_FAIL(check_truncate_part_filter(current,
                                                        trans_version,
                                                        row_header->get_row_multi_version_flag().is_ghost_row(),
                                                        is_filtered))) {
            LOG_WARN("failed to check truncate part filter", K(ret), K(current), K(trans_version), K(is_major_sstable));
          } else if (!is_filtered && FALSE_IT(check_base_version(trans_version, is_filtered))) {
          } else if (is_filtered) {
          } else if (FALSE_IT(*lock_state = uncommited_lock_state)) {
          } else if (lock_state->is_lock_decided()) {
            lock_state->lock_dml_flag_ = row_header->get_row_flag().get_dml_flag();
          }
        } else if (OB_UNLIKELY(nullptr != context_->truncate_part_filter_) &&
                   OB_FAIL(check_truncate_part_filter(current, 0/*trans_version*/, row_header->get_row_multi_version_flag().is_ghost_row(), is_filtered))) {
          LOG_WARN("failed to check truncate part filter", K(ret), K(current), K(trans_version), K(is_major_sstable));
        } else if (!is_filtered && FALSE_IT(check_base_version(trans_version, is_filtered))) {
        } else if (is_filtered) {
        } else if (OB_FAIL(lock_state->trans_version_.convert_for_tx(trans_version))) {
          LOG_ERROR("convert failed", K(ret), K(trans_version));
        } else {
          lock_state->lock_dml_flag_ = row_header->get_row_flag().get_dml_flag();
        }
      } else if (OB_UNLIKELY(nullptr != context_->truncate_part_filter_) &&
                 OB_FAIL(check_truncate_part_filter(current, 0/*trans_version*/, false, is_filtered))) {
        LOG_WARN("failed to check truncate part filter", K(ret), K(current), K(trans_version), K(is_major_sstable));
      } else if (is_filtered) {
      } else {
        lock_state->trans_version_ = sstable_->get_end_scn();
        lock_state->lock_dml_flag_ = blocksstable::ObDmlFlag::DF_INSERT;
      }
      if (OB_SUCC(ret) && !is_filtered) {
        bool need_stop = false;
        if (is_major_sstable) {
          check_row_in_major_sstable(need_stop);
        } else if (OB_FAIL(check_row(read_trans_id, row_header, *lock_state, need_stop))) {
          LOG_WARN("Failed to check row", K(ret), K(ret), KPC_(range), K(read_trans_id), KPC(row_header),
                   K(sql_sequence), KPC(lock_state));
        }
        STORAGE_LOG(DEBUG, "check row lock on sstable", K(ret), KPC_(range), K(need_stop),
                    K(read_trans_id), KPC(row_header), KPC(lock_state), K(current));
        if (OB_SUCC(ret) && need_stop) {
          break;
        }
      }
    }
  }
  return ret;
}

// TODO if current row is filtered by truncate, it's no need to scan remain data of the same rowkey
int ObMicroBlockRowLockChecker::check_truncate_part_filter(const int64_t current, const int64_t trans_version, const bool is_ghost_row, bool &fitered)
{
  int ret = OB_SUCCESS;
  fitered = false;
  if (!is_ghost_row && context_->truncate_part_filter_->is_valid_filter()) {
    const int64_t rowkey_cnt = read_info_->get_schema_rowkey_count();
    if (OB_FAIL(reader_->get_row(current, row_))) {
      LOG_WARN("failed to get row", K(ret));
    } else if (transaction::is_effective_trans_version(trans_version)) {
      row_.storage_datums_[rowkey_cnt].reuse();
      row_.storage_datums_[rowkey_cnt].set_int(trans_version);
    }
    if (FAILEDx(context_->truncate_part_filter_->filter(row_, fitered, true/*check_filter*/, !sstable_->is_major_sstable()))) {
      LOG_WARN("failed to filter truncated part", K(ret));
    } else if (OB_UNLIKELY(fitered)) {
      LOG_DEBUG("[TRUNCATE INFO] filtered by truncated main table partition", K(ret), K(current), K(trans_version), K_(row));
    } else {
      LOG_DEBUG("[TRUNCATE INFO] not filtered row", KR(ret), K(row_), KPC(context_->truncate_part_filter_), K(rowkey_cnt));
    }
  }
  return ret;
}

void ObMicroBlockRowLockChecker::check_base_version(const int64_t trans_version, bool &is_filtered)
{
  if (OB_UNLIKELY(base_version_ > 0 &&
                  transaction::is_effective_trans_version(trans_version) &&
                  trans_version <= base_version_)) {
    is_filtered = true;
  }
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

void ObMicroBlockRowLockMultiChecker::inc_empty_read(ObSSTableReadHandle &read_handle)
{
  if (OB_NOT_NULL(context_) && OB_NOT_NULL(sstable_)
      && (!context_->query_flag_.is_index_back() || !sstable_->is_major_sstable())
      && context_->query_flag_.is_use_bloomfilter_cache() && !sstable_->is_small_sstable() && empty_read_cnt_ > 0) {
    read_handle.current_rows_info_idx_ = rowkey_begin_idx_;
    (void)OB_STORE_CACHE.get_bf_cache().inc_empty_read(MTL_ID(),
                                                       param_->table_id_,
                                                       param_->ls_id_,
                                                       sstable_->get_key(),
                                                       macro_id_,
                                                       rows_info_->get_datum_cnt(),
                                                       &read_handle,
                                                       empty_read_cnt_);
  }
}

int ObMicroBlockRowLockMultiChecker::inner_get_next_row(
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
    lock_state = &(rows_info_->get_row_lock_state(rowkey_current_idx_ - 1));
    current = current_++;
  }
  return ret;
}

int ObMicroBlockRowLockMultiChecker::check_row(
    const transaction::ObTransID &trans_id,
    const ObRowHeader *row_header,
    ObStoreRowLockState &lock_state,
    bool &need_stop)
{
  int ret = OB_SUCCESS;
  const ObMultiVersionRowFlag multi_version_flag = row_header->get_row_multi_version_flag();
  const bool row_is_decided = lock_state.is_row_decided();
  const int64_t rowkey_idx = rowkey_current_idx_ - 1;
  bool is_ghost_row_flag = false;
  need_stop = false;

  if (!row_is_decided) {
    // Case1: Row is aborted.
    need_stop = false;
  } else if (OB_FAIL(ObGhostRowUtil::is_ghost_row(multi_version_flag,
                                                  is_ghost_row_flag))) {
    LOG_WARN("Failed to check ghost row", K(ret), K(multi_version_flag));
  } else if (is_ghost_row_flag) {
    // Case2: We encounter the ghost row which means the version node is
    // faked, and the row has already ended. So, we believe that the next
    // iteration will find OB_ITER_END.
    LOG_INFO("encounter a ghost row", K(ret), K(trans_id), K(lock_state));
    need_stop = false;
    lock_state.lock_dml_flag_ = ObDmlFlag::DF_NOT_EXIST;
  } else {
    if (lock_state.is_locked_not_by(trans_id)) {
      rows_info_->set_row_conflict_error(rowkey_idx, OB_TRY_LOCK_ROW_CONFLICT);
      need_stop = true;
      LOG_DEBUG("Find lock conflict in mini/minor sstable", K(rowkey_idx), K(rows_info_->rowkeys_[rowkey_idx]),
                K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(empty_read_cnt), K(lock_state),
                K_(current), K_(start), K_(last), K_(macro_id), KPC(row_header));
    } else if (lock_state.trans_version_ > snapshot_version_) {
      rows_info_->set_row_conflict_error(rowkey_idx, OB_TRANSACTION_SET_VIOLATION);
      need_stop = true;
      LOG_DEBUG("Find tsv conflict in mini/minor sstable", K(rowkey_idx), K(rows_info_->rowkeys_[rowkey_idx]),
                K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(empty_read_cnt), K(lock_state),
                K_(current), K_(start), K_(last), K_(macro_id), KPC(row_header));
    } else if (check_exist_ && DF_DELETE != row_header->get_row_flag().get_dml_flag()) {
      rows_info_->set_row_conflict_error(rowkey_idx, OB_ERR_PRIMARY_KEY_DUPLICATE);
      need_stop = !rows_info_->need_find_all_duplicate_key();;
      LOG_DEBUG("Find duplication in mini/minor sstable", K(rowkey_idx), K(rows_info_->rowkeys_[rowkey_idx]),
                K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(empty_read_cnt), K(lock_state),
                K_(current), K_(start), K_(last), K_(macro_id), KPC(row_header));
    } else {
      rows_info_->set_row_checked(rowkey_idx);
    }
  }

  LOG_DEBUG("check decided row in multi row checker of mini/minor sstable", K(rowkey_idx),
            K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx),
            K(rows_info_->rowkeys_[rowkey_idx]), K(lock_state), KPC(row_header));

  return ret;
}

void ObMicroBlockRowLockMultiChecker::check_row_in_major_sstable(bool &need_stop)
{
  const int64_t rowkey_idx = rowkey_current_idx_ - 1;
  need_stop = false;
  if (check_exist_) {
    rows_info_->set_row_conflict_error(rowkey_idx, OB_ERR_PRIMARY_KEY_DUPLICATE);
    need_stop = !rows_info_->need_find_all_duplicate_key();
    LOG_DEBUG("Find duplication in major sstable", K(rowkey_idx), K(rows_info_->rowkeys_[rowkey_idx]),
              K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(empty_read_cnt),
              K_(current), K_(start), K_(last), K_(macro_id));
  } else {
    rows_info_->set_row_checked(rowkey_idx);
  }

  LOG_DEBUG("check decided row in multi row checker of major sstable", K(rowkey_idx),
            K_(rowkey_current_idx), K_(rowkey_begin_idx), K_(rowkey_end_idx),
            K(rows_info_->rowkeys_[rowkey_idx]));
}

int ObMicroBlockRowLockMultiChecker::seek_forward()
{
  int ret = OB_SUCCESS;
  ObIMicroBlockReader *micro_block_reader = reader_;
  bool need_search_duplicate_row = false;
  if (ObIMicroBlockReader::Reader == reader_->get_type() || ObIMicroBlockReader::NewFlatReader == reader_->get_type()) {
    need_search_duplicate_row = !sstable_->is_major_type_sstable() && !static_cast<ObIMicroBlockFlatReaderBase *>(reader_)->single_version_rows();
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
