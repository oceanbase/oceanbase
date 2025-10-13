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

#include "ob_memtable_single_row_reader.h"

#include "storage/access/ob_table_access_context.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_read_row_util.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"


namespace oceanbase {

using namespace blocksstable;
using namespace concurrency_control;

namespace memtable {

int ObMemtableSingleRowReader::init(ObMemtable *memtable, const ObTableIterParam &param, ObTableAccessContext &context)
{
  int ret = OB_SUCCESS;
  char *trans_info_ptr = nullptr;
  int64_t length = ObTransStatRow::MAX_TRANS_STRING_SIZE;
  if (OB_ISNULL(read_info_ = param.get_read_info(false))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "Unexpected null read info", K(ret), K(param));
  } else if (param.need_trans_info() &&
             OB_ISNULL(trans_info_ptr = static_cast<char *>(context.stmt_allocator_->alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_FAIL(private_row_.init(*context.allocator_, read_info_->get_request_count(), trans_info_ptr))) {
    TRANS_LOG(WARN, "Failed to init datum row", K(ret), K(param.need_trans_info()));
  } else {
    TRANS_LOG(DEBUG, "scan iterator init succ", K(param.tablet_id_));
    param_ = &param;
    context_ = &context;
    memtable_ = memtable;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObMemtableSingleRowReader::reset()
{
  param_ = nullptr;
  context_ = nullptr;
  read_info_ = nullptr;
  memtable_ = nullptr;
  private_row_.reset();
  // bitmap_.reuse();
}

/**
 * @brief Init a new range to scan.
 *        ObMemtableScanIterator only call this func once,
 *        but ObMemtableMScanIterator call this func multiple times.
 *
 *        If current range is a single row key, this function would skip constructing MvccIterator
 */
int ObMemtableSingleRowReader::init_a_new_range(const ObDatumRange &new_range_to_scan)
{
  int ret = OB_SUCCESS;
  ObDatumRange real_range;
  ObMemtableKey* start_key = NULL;
  ObMemtableKey* end_key = NULL;
  const ObColDescIArray *out_cols = nullptr;

  is_range_scan_ = true;
  if (OB_ISNULL(memtable_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid memtable ptr", KR(ret));
  } else if (OB_ISNULL(out_cols = param_->get_out_col_descs())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null out_cols", KR(ret), K_(param));
  } else if (FALSE_IT(cur_range_ = new_range_to_scan)) {
  } else if (OB_FAIL(check_is_range_scan_(new_range_to_scan))) {
    STORAGE_LOG(WARN, "check is range scan failed", KR(ret), K(new_range_to_scan));
  } else if (!is_range_scan_) {
    // This range is a single rowkey or Delete-Insert table. Do not need construct a ObMvccRowIterator.
    // ObMemtable::get() function will be called instead
    // But a flag is needed to stop
    row_has_been_gotten_ = false;
  } else if (OB_FAIL(get_real_range_(new_range_to_scan, real_range))) {
    TRANS_LOG(WARN, "fail to get_real_range", K(ret), K(new_range_to_scan));
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!real_range.is_memtable_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid datum range", K(ret), K(real_range));
  } else if (OB_FAIL(ObMemtableKey::build(start_key,
                                          *out_cols,
                                          &real_range.get_start_key().get_store_rowkey(),
                                          *context_->allocator_))) {
    TRANS_LOG(WARN, "start key build fail", K(param_->table_id_), K(real_range));
  } else if (OB_FAIL(ObMemtableKey::build(end_key,
                                          *out_cols,
                                          &real_range.get_end_key().get_store_rowkey(),
                                          *context_->allocator_))) {
    TRANS_LOG(WARN, "end key build fail", K(param_->table_id_), K(real_range));
  } else {
    ObMvccEngine& mvcc_engine = memtable_->get_mvcc_engine();
    ObMvccScanRange mvcc_scan_range;
    mvcc_scan_range.border_flag_ = real_range.get_border_flag();
    mvcc_scan_range.start_key_ = start_key;
    mvcc_scan_range.end_key_ = end_key;
    row_iter_.reset();
    if (OB_FAIL(mvcc_engine.scan(context_->store_ctx_->mvcc_acc_ctx_,
                                 context_->query_flag_,
                                 mvcc_scan_range,
                                 memtable_->get_ls_id(),
                                 row_iter_))) {
      TRANS_LOG(WARN, "mvcc engine scan fail", K(ret), K(mvcc_scan_range));
    } else if (OB_FAIL(bitmap_.init(read_info_->get_request_count(), read_info_->get_schema_rowkey_count() + read_info_->get_extra_rowkey_count()))) {
      TRANS_LOG(WARN, "Failed to init bitmap ", K(ret));
    } else {
      TRANS_LOG(DEBUG, "mvcc engine scan success",
                K_(memtable), K(mvcc_scan_range), KPC(context_->store_ctx_),
                K(*start_key), K(*end_key));
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    reset();
  }
  return ret;
}

int ObMemtableSingleRowReader::check_is_range_scan_(const blocksstable::ObDatumRange &new_range_to_scan)
{
  int ret = OB_SUCCESS;
  bool is_single = false;
  if (param_->is_delete_insert_) {
    // delete-insert table must do range_scan
    is_range_scan_ = true;
  } else if (OB_FAIL(new_range_to_scan.is_memtable_single_rowkey(
              read_info_->get_schema_rowkey_count(),
              read_info_->get_datum_utils(),
              is_single))) {
    STORAGE_LOG(WARN, "check range failed", KR(ret));
  } else {
    // ObStoreRange store_range;
    // store_range.set_start_key(new_range_to_scan.get_start_key().get_store_rowkey());
    // store_range.set_end_key(new_range_to_scan.get_end_key().get_store_rowkey());
    // store_range.set_border_flag(new_range_to_scan.get_border_flag());
    // is_range_scan_ = !store_range.is_single_rowkey();

    is_range_scan_ = !is_single;
  }
  return ret;
}

int ObMemtableSingleRowReader::get_real_range_(const ObDatumRange &range, ObDatumRange &real_range)
{
  int ret = OB_SUCCESS;
  bool is_reverse_scan = context_->query_flag_.is_reverse_scan();
  if (!range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid range", K(ret), K(range));
  } else {
    real_range = range;
    if (is_reverse_scan) {
      real_range.start_key_ = range.get_end_key();
      real_range.end_key_ = range.get_start_key();
      if (range.get_border_flag().inclusive_start()) {
        real_range.border_flag_.set_inclusive_end();
      } else {
        real_range.border_flag_.unset_inclusive_end();
      }
      if (range.get_border_flag().inclusive_end()) {
        real_range.border_flag_.set_inclusive_start();
      } else {
        real_range.border_flag_.unset_inclusive_start();
      }
    }
  }
  return ret;
}

/**
 * @brief for the simple read operation, like normal scan or get.
 *
 */
int ObMemtableSingleRowReader::get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_in_next_row(private_row_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fill in next row faild", KR(ret));
    }
  } else {
    row = &private_row_;
  }
  return ret;
}

/**
 * @brief If fill_in_next_row() is called, the caller need to provide a memory space to hold the new row. But if
 * get_next_row() is called, ObMemtableSingleRowReader would use its own memory space to hold the new row
 *
 * @param[out] row caller proveded space to hold the new row
 */
int ObMemtableSingleRowReader::fill_in_next_row(ObDatumRow &next_row)
{
  int ret = OB_SUCCESS;
  if (param_->is_delete_insert_) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "delete insert table should not call this function", KR(ret), K(lbt()));
  } else {
    // NOTICE : use static to avoid constructor overhead of ObDatumRow
    static ObDatumRow unused_row;
    int64_t unused_cnt;
    if (OB_FAIL(inner_fill_in_next_row_(next_row, unused_row, unused_cnt))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fill in next row failed", KR(ret), K(next_row));
      }
    }
  }
  return ret;
}

/**
 * @brief the delete-insert read need to know both delete row and insert row.
 * NOTICE : only Delete-Insert case would fill in the second param &delete_row
 *
 */
int ObMemtableSingleRowReader::fill_in_next_delete_insert_row(ObDatumRow &next_row,
                                                              ObDatumRow &delete_row,
                                                              int64_t &acquired_row_cnt)
{
  int ret = OB_SUCCESS;
  bool got_a_new_row = false;
  while (OB_SUCC(ret) && !got_a_new_row) {
    if (OB_FAIL(inner_fill_in_next_row_(next_row, delete_row, acquired_row_cnt))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fill in next row failed", KR(ret), K(next_row));
      }
    } else if (next_row.row_flag_.is_not_exist()) {
      STORAGE_LOG(DEBUG, "meet a Insert-Delete row, try get next row");
    } else {
      got_a_new_row = true;
    }
  }
  return ret;
}

int ObMemtableSingleRowReader::inner_fill_in_next_row_(ObDatumRow &next_row,
                                                       ObDatumRow &delete_row,
                                                       int64_t &acquired_row_cnt)
{
  int ret = OB_SUCCESS;

  acquired_row_cnt = 0;
  if (is_range_scan_ || param_->is_delete_insert_) {
    const ObMemtableKey *key = nullptr;
    ObMvccValueIterator *value_iter = nullptr;
    if (OB_FAIL(get_next_value_iter_(key, value_iter))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "get next value iterator failed", KR(ret), KPC(key));
      }
    } else if (OB_FAIL(fill_in_next_row_by_value_iter_(key, value_iter, next_row, delete_row, acquired_row_cnt))) {
      STORAGE_LOG(WARN, "fill in new row by value iter failed", KR(ret), KPC(key));
    } else if (next_row.row_flag_.is_not_exist()) {
      STORAGE_LOG(DEBUG, "meet a Insert-Delete row, try get next row");
    }
    STORAGE_LOG(DEBUG, "fill in row for range scan", K(ret), K(is_range_scan_), K(cur_range_), K(next_row), K(delete_row), K(acquired_row_cnt));
  } else {
    // the range is a single row key, directly get row from memtable
    if (row_has_been_gotten_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(memtable_->get(*param_, *context_, cur_range_.get_start_key(), next_row))) {
      STORAGE_LOG(WARN, "fail to get memtable row", K(ret), K(param_), K(cur_range_));
    } else {
      // set row_has_been_gotten flag after get operation
      row_has_been_gotten_ = true;
      acquired_row_cnt = 1;
      STORAGE_LOG(DEBUG, "fill in row for single rowkey scan(get)", K(next_row), K(next_row.scan_index_));
    }
  }
  return ret;
}

int ObMemtableSingleRowReader::get_next_value_iter_(const ObMemtableKey *&key, ObMvccValueIterator *&value_iter)
{
  int ret = OB_SUCCESS;
  ObStoreRowLockState lock_state;
  if (OB_FAIL(row_iter_.get_next_row(key, value_iter, lock_state)) || nullptr == key || nullptr == value_iter) {
    if (OB_TRY_LOCK_ROW_CONFLICT == ret || OB_TRANSACTION_SET_VIOLATION == ret) {
      if (!context_->query_flag_.is_for_foreign_key_check()) {
        ret = OB_ERR_UNEXPECTED;  // to prevent retrying casued by throwing 6005
        STORAGE_LOG(
            WARN, "should not meet row conflict if it's not for foreign key check", K(ret), K(context_->query_flag_));
      } else if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
        const ObStoreRowkey *tmp_rowkey = nullptr;
        key->get_rowkey(tmp_rowkey);
        ObRowConflictHandler::post_row_read_conflict(
            context_->store_ctx_->mvcc_acc_ctx_,
            *tmp_rowkey,
            lock_state,
            context_->tablet_id_,
            context_->ls_id_,
            0,
            0 /* these two params get from mvcc_row, and for statistics, so we ignore them */,
            lock_state.trans_scn_);
      }
    } else if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "row_iter_ get_next_row fail", K(ret), KP(key), KP(value_iter));
    }
  }
  return ret;
}

int ObMemtableSingleRowReader::fill_in_next_row_by_value_iter_(const ObMemtableKey *key,
                                                               ObMvccValueIterator *value_iter,
                                                               blocksstable::ObDatumRow &next_row,
                                                               blocksstable::ObDatumRow &delete_row,
                                                               int64_t &acquired_row_cnt)
{
  int ret = OB_SUCCESS;
  TRANS_LOG(DEBUG, "chaser debug memtable next row", KPC(key), K(bitmap_.get_nop_cnt()));
  // generate trans stat datum for 4377 check
  if (param_->need_trans_info() && OB_NOT_NULL(next_row.trans_info_)) {
    concurrency_control::ObTransStatRow trans_stat_row;
    (void)value_iter->get_trans_stat_row(trans_stat_row);
    concurrency_control::build_trans_stat_datum(param_, next_row, trans_stat_row);
  }
  const ObStoreRowkey *rowkey = nullptr;
  (void)key->get_rowkey(rowkey);
  int64_t next_row_scn = 0;
  int64_t delete_row_scn = 0;

  if (param_->is_delete_insert_) {
    // delete insert read
    if (OB_FAIL(ObReadRow::iterate_delete_insert_row(*read_info_,
                                                     *rowkey,
                                                     *value_iter,
                                                     next_row,
                                                     delete_row,
                                                     bitmap_,
                                                     next_row_scn,
                                                     delete_row_scn,
                                                     acquired_row_cnt))) {
      STORAGE_LOG(WARN, "iterate delete insert row fail", K(ret), K(*rowkey), KP(value_iter));
    }
  } else {
    // normal read
    if (OB_FAIL(ObReadRow::iterate_row(*read_info_, *rowkey, *value_iter, next_row, bitmap_, next_row_scn))) {
      STORAGE_LOG(WARN, "iterate_row fail", K(ret), K(*rowkey), KP(value_iter));
    } else {
      acquired_row_cnt = 1;
    }
  }

  if (OB_SUCC(ret)) {
    if (param_->need_scn_) {
      if (next_row.row_flag_.is_exist() && OB_FAIL(fill_in_row_scn_(next_row_scn, value_iter, next_row))) {
        STORAGE_LOG(WARN, "fill in next row scn filed", KR(ret), K(next_row));
      } else if (param_->is_delete_insert_ && delete_row.row_flag_.is_exist() &&
                 OB_FAIL(fill_in_row_scn_(delete_row_scn, value_iter, delete_row))) {
        STORAGE_LOG(WARN, "fill in delete row scn filed", KR(ret), K(delete_row));
      }
    }
    next_row.scan_index_ = 0;
    STORAGE_LOG(DEBUG, "chaser debug memtable next row", K(ret), K(next_row), K(delete_row));
  }
  return ret;
}

int ObMemtableSingleRowReader::fill_in_row_scn_(const int64_t row_scn,
                                                const ObMvccValueIterator *value_iter,
                                                ObDatumRow &new_row)
{
  int ret = OB_SUCCESS;
  if (row_scn == share::SCN::max_scn().get_val_for_tx()) {
    // TODO(handora.qc): remove it as if we confirmed no problem according to row_scn
    STORAGE_LOG(INFO, "use max row scn", KPC(value_iter->get_mvcc_acc_ctx()));
  }

  int trans_idx = read_info_->get_trans_col_index();
  if (OB_UNLIKELY(trans_idx < 0 || trans_idx >= new_row.count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected trans idx", K(ret), K(new_row.count_), K(trans_idx));
  } else {
    new_row.storage_datums_[trans_idx].reuse();
    new_row.storage_datums_[trans_idx].set_int(row_scn);
    STORAGE_LOG(DEBUG, "set row scn is", K(trans_idx), K(row_scn), K_(private_row));
  }
  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
