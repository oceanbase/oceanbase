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

#define USING_LOG_PREFIX TRANS

#include "storage/memtable/ob_memtable_iterator.h"

#include "share/object/ob_obj_cast.h"
#include "common/ob_common_types.h"

#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/tx/ob_trans_define.h"
#include "ob_memtable_context.h"
#include "ob_memtable.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace transaction;
using namespace share::schema;
namespace memtable
{

/**
 * ---------------------------ObMemtableGetIterator-----------------------------
 */
ObMemtableGetIterator::ObMemtableGetIterator()
    : MAGIC_(VALID_MAGIC_NUM),
      is_inited_(false),
      rowkey_iter_(0),
      param_(NULL),
      context_(NULL),
      memtable_(NULL),
      rowkey_(NULL),
      cur_row_()
{
}

ObMemtableGetIterator::~ObMemtableGetIterator()
{
  reset();
}

/*
  get_iter.init(param, context, memtable)
  get_iter.set_rowkey()
  get_iter.get_next_row()
  // direct reuse
  get_iter.set_rowkey()
  get_iter.get_next_row()
 */
int ObMemtableGetIterator::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    ObIMemtable &memtable)
{
  int ret = OB_SUCCESS;
  char *trans_info_ptr = nullptr;
  if (is_inited_) {
    reset();
  }
  const ObITableReadInfo *read_info = param.get_read_info(context.use_fuse_row_cache_);
  if (param.need_trans_info()) {
    int64_t length = concurrency_control::ObTransStatRow::MAX_TRANS_STRING_SIZE;
    if (OB_ISNULL(trans_info_ptr = static_cast<char *>(context.stmt_allocator_->alloc(length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "fail to alloc memory", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(nullptr == read_info || !read_info->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Unexpected read info", K(ret), KPC(read_info));
  } else if (OB_FAIL(cur_row_.init(*context.allocator_, read_info->get_request_count(), trans_info_ptr))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
  } else {
    param_ = &param;
    context_ = &context;
    memtable_ = &memtable;
    rowkey_ = nullptr;
    is_inited_ = true;
  }
  return ret;
}

void ObMemtableGetIterator::set_rowkey(const ObDatumRowkey &rowkey)
{
  rowkey_ = &rowkey;
  rowkey_iter_ = 0;
}

int ObMemtableGetIterator::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    storage::ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table) || OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "table and query range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_FAIL(init(param, context, *static_cast<ObMemtable *>(table)))) {
    TRANS_LOG(WARN, "failed to init memtable get iter", K(ret));
  } else {
    set_rowkey(*static_cast<const ObDatumRowkey *>(query_range));
  }
  return ret;
}

int ObMemtableGetIterator::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(rowkey_iter_ > 0)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(memtable_->get(
                         *param_,
                         *context_,
                         *rowkey_,
                         cur_row_))) {
    TRANS_LOG(WARN, "memtable get fail",
              K(ret), "table_id", param_->table_id_, K(*rowkey_));
  } else {
    ++rowkey_iter_;
    cur_row_.scan_index_ = 0;
    row = &cur_row_;
  }
  return ret;
}

void ObMemtableGetIterator::reset()
{
  is_inited_ = false;
  rowkey_iter_ = 0;
  param_ = NULL;
  context_ = NULL;
  memtable_ = NULL;
  rowkey_ = NULL;
  cur_row_.reset();
}

/**
 * ---------------------------ObMemtableScanIterator----------------------------
 */
ObMemtableScanIterator::ObMemtableScanIterator()
    : MAGIC_(VALID_MAGIC_NUM),
      is_inited_(false),
      is_scan_start_(false),
      param_(NULL),
      context_(NULL),
      read_info_(NULL),
      memtable_(NULL),
      cur_range_(),
      row_iter_(),
      row_(),
      iter_flag_(0)
{
  GARL_ADD(&active_resource_, "scan_iter");
}

ObMemtableScanIterator::~ObMemtableScanIterator()
{
  GARL_DEL(&active_resource_);
  reset();
}

/*
  scan_iter.init(param, context)
  scan_iter.set_range(...)
  while(ITER_END != scan_iter.get_next_row());
  // direct reuse
  scan_iter.set_range(...)
  while(ITER_END != scan_iter.get_next_row());
 */
int ObMemtableScanIterator::init(
    ObIMemtable* memtable,
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context)
{
  int ret = OB_SUCCESS;
  char *trans_info_ptr = nullptr;
  if (is_inited_) {
    reset();
  }

  if (param.need_trans_info()) {
    int64_t length = concurrency_control::ObTransStatRow::MAX_TRANS_STRING_SIZE;
    if (OB_ISNULL(trans_info_ptr = static_cast<char *>(context.stmt_allocator_->alloc(length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "fail to alloc memory", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(read_info_ = param.get_read_info(false))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "Unexpected null read info", K(ret), K(param));
  } else if (OB_FAIL(row_.init(*context.allocator_,
                               read_info_->get_request_count(),
                               trans_info_ptr))) {
    TRANS_LOG(WARN, "Failed to init datum row", K(ret), K(param.need_trans_info()));
  } else {
    TRANS_LOG(DEBUG, "scan iterator init succ", K(param.table_id_));
    param_ = &param;
    context_ = &context;
    memtable_ = memtable;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMemtableScanIterator::set_range(const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    cur_range_ = range;
    is_scan_start_ = false;
  }
  return ret;
}

int ObMemtableScanIterator::prepare_scan()
{
  int ret = OB_SUCCESS;
  ObDatumRange range;
  ObMemtableKey* start_key = NULL;
  ObMemtableKey* end_key = NULL;
  const ObColDescIArray *out_cols = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (is_scan_start_) {
    // pass
  } else if (OB_ISNULL(out_cols = param_->get_out_col_descs())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null out_cols", K(ret), K_(param));
  } else if (OB_FAIL(get_real_range(cur_range_, range))) {
    TRANS_LOG(WARN, "fail to get_real_range", K(ret), K(cur_range_));
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!range.is_memtable_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid datum range", K(ret), K(range));
  } else if (OB_FAIL(ObMemtableKey::build(
              start_key, *out_cols, &range.get_start_key().get_store_rowkey(), *context_->get_range_allocator()))) {
    TRANS_LOG(WARN, "start key build fail", K(param_->table_id_), K(range));
  } else if (OB_FAIL(ObMemtableKey::build(
              end_key, *out_cols, &range.get_end_key().get_store_rowkey(), *context_->get_range_allocator()))) {
    TRANS_LOG(WARN, "end key build fail", K(param_->table_id_), K(range));
  } else {
    ObMvccEngine& mvcc_engine = ((ObMemtable*)memtable_)->get_mvcc_engine();
    ObMvccScanRange mvcc_scan_range;
    mvcc_scan_range.border_flag_ = range.get_border_flag();
    mvcc_scan_range.start_key_ = start_key;
    mvcc_scan_range.end_key_ = end_key;
    row_iter_.reset();
    if (OB_FAIL(mvcc_engine.scan(context_->store_ctx_->mvcc_acc_ctx_,
                                 context_->query_flag_,
                                 mvcc_scan_range,
                                 row_iter_))) {
      TRANS_LOG(WARN, "mvcc engine scan fail", K(ret), K(mvcc_scan_range));
    } else if (OB_FAIL(bitmap_.init(read_info_->get_request_count(), read_info_->get_schema_rowkey_count()))) {
      TRANS_LOG(WARN, "Failed to init bitmap ", K(ret));
    } else {
      iter_flag_ = 0;
      is_scan_start_ = true;
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

int ObMemtableScanIterator::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  const ObDatumRange *range = static_cast<const ObDatumRange *>(query_range);
  if (OB_ISNULL(table) || OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "table and query_range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_FAIL(init(static_cast<ObMemtable *>(table), param, context))) {
    TRANS_LOG(WARN, "init scan iterator fail", K(ret), K(range));
  } else if (OB_FAIL(set_range(*range))) {
    TRANS_LOG(WARN, "set scan range fail", K(ret), K(*range));
  }
  return ret;
}

int ObMemtableScanIterator::get_real_range(const ObDatumRange &range, ObDatumRange &real_range)
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

int ObMemtableScanIterator::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  const ObMemtableKey *key = NULL;
  ObMvccValueIterator *value_iter = NULL;
  ObStoreRowLockState lock_state;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(prepare_scan())) {
    TRANS_LOG(WARN, "prepare scan fail", K(ret));
  } else if (OB_FAIL(row_iter_.get_next_row(key, value_iter, iter_flag_, lock_state))
      || NULL == key || NULL == value_iter) {
    if (OB_TRY_LOCK_ROW_CONFLICT == ret || OB_TRANSACTION_SET_VIOLATION == ret) {
      if (!context_->query_flag_.is_for_foreign_key_check()) {
        ret = OB_ERR_UNEXPECTED;  // to prevent retrying casued by throwing 6005
        TRANS_LOG(WARN, "should not meet row conflict if it's not for foreign key check",
                  K(ret), K(context_->query_flag_));
      } else if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
        const ObStoreRowkey *tmp_rowkey = nullptr;
        key->get_rowkey(tmp_rowkey);
        ObRowConflictHandler::post_row_read_conflict(
                              context_->store_ctx_->mvcc_acc_ctx_,
                              *tmp_rowkey,
                              lock_state,
                              context_->tablet_id_,
                              context_->ls_id_,
                              0, 0 /* these two params get from mvcc_row, and for statistics, so we ignore them */,
                              lock_state.trans_scn_);
      }
    } else if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "row_iter_ get_next_row fail", K(ret), KP(key), KP(value_iter));
    }
  } else {
    TRANS_LOG(DEBUG, "chaser debug memtable next row", KPC(key), K(iter_flag_), K(bitmap_.get_nop_cnt()));
    const ObStoreRowkey *rowkey = NULL;
    int64_t row_scn = 0;
    key->get_rowkey(rowkey);

    concurrency_control::ObTransStatRow trans_stat_row;
    (void)value_iter->get_trans_stat_row(trans_stat_row);

    if (OB_FAIL(ObReadRow::iterate_row(*read_info_, *rowkey, *(context_->allocator_), *value_iter, row_, bitmap_, row_scn))) {
      TRANS_LOG(WARN, "iterate_row fail", K(ret), K(*rowkey), KP(value_iter));
    } else {
      STORAGE_LOG(DEBUG, "chaser debug memtable next row", K(row_));
      if (param_->need_scn_) {
        const ObColDescIArray *out_cols = nullptr;
        if (OB_ISNULL(out_cols = param_->get_out_col_descs())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "Unexpected null columns desc", K(ret), K_(param));
        } else {
          if (row_scn == share::SCN::max_scn().get_val_for_tx()) {
            // TODO(handora.qc): remove it as if we confirmed no problem according to row_scn
            TRANS_LOG(INFO, "use max row scn", KPC(value_iter->get_mvcc_acc_ctx()), K(trans_stat_row));
          }

          for (int64_t i = 0; i < out_cols->count(); i++) {
            if (out_cols->at(i).col_id_ == OB_HIDDEN_TRANS_VERSION_COLUMN_ID) {
              row_.storage_datums_[i].reuse();
              row_.storage_datums_[i].set_int(row_scn);
              TRANS_LOG(DEBUG, "set row scn is", K(i), K(row_scn), K_(row));
            }
          }
        }
      }

      // generate trans stat datum for 4377 check
      concurrency_control::build_trans_stat_datum(param_, row_, trans_stat_row);

      row_.scan_index_ = 0;
      row = &row_;
    }
  }
  if (OB_FAIL(ret)) {
    iter_flag_ = 0;
  }
  return ret;
}

void ObMemtableScanIterator::reset()
{
  is_inited_ = false;
  is_scan_start_ = false;
  param_ = NULL;
  context_ = NULL;
  read_info_ = NULL;
  memtable_ = NULL;
  cur_range_.reset();
  row_.reset();
  bitmap_.reuse();
  iter_flag_ = 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObMemtableMGetIterator::ObMemtableMGetIterator()
    :  MAGIC_(VALID_MAGIC_NUM),
       is_inited_(false),
       param_(NULL),
       context_(NULL),
       memtable_(NULL),
       rowkeys_(NULL),
       cols_map_(NULL),
       rowkey_iter_(0),
       cur_row_()
{
}

ObMemtableMGetIterator::~ObMemtableMGetIterator()
{
  reset();
}

int ObMemtableMGetIterator::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    storage::ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }

  char *trans_info_ptr = nullptr;
  const ObITableReadInfo *read_info = param.get_read_info();
  if (param.need_trans_info()) {
    int64_t length = concurrency_control::ObTransStatRow::MAX_TRANS_STRING_SIZE;
    if (OB_ISNULL(trans_info_ptr = static_cast<char *>(context.stmt_allocator_->alloc(length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "fail to alloc memory", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(table) || OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "table and query_range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_UNLIKELY(nullptr == read_info || !read_info->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Unexpected read info", K(ret), KPC(read_info));
  } else if (OB_FAIL(cur_row_.init(*context.allocator_, read_info->get_request_count(), trans_info_ptr))) {
    TRANS_LOG(WARN, "Failed to init datum row", K(ret));
  } else {
    const ObColDescIArray &out_cols = read_info->get_columns_desc();
    void *buf = NULL;
    if (NULL == (buf = context.allocator_->alloc(sizeof(ColumnMap)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "Fail to allocate memory, ", K(ret));
    } else {
      cols_map_ = new (buf) ColumnMap(*context.allocator_);
      if (OB_FAIL(cols_map_->init(out_cols))) {
        TRANS_LOG(WARN, "Fail to build column map, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      param_ = &param;
      context_ = &context;
      memtable_ = static_cast<ObMemtable *>(table);
      rowkeys_ = static_cast<const ObIArray<ObDatumRowkey> *>(query_range);
      rowkey_iter_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMemtableMGetIterator::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else {
    if (rowkey_iter_ >= rowkeys_->count()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(memtable_->get(
                           *param_,
                           *context_,
                           rowkeys_->at(rowkey_iter_),
                           cur_row_))) {
      TRANS_LOG(WARN, "memtable get fail",
                K(ret), "table_id", param_->table_id_, "rowkey", rowkeys_->at(rowkey_iter_));
    } else {
      cur_row_.scan_index_ = rowkey_iter_;
      ++rowkey_iter_;
      row = &cur_row_;
    }
  }
  return ret;
}

void ObMemtableMGetIterator::reset()
{
  is_inited_ = false;
  cur_row_.reset();
  param_ = NULL;
  context_ = NULL;
  memtable_ = NULL;
  rowkeys_ = NULL;
  rowkey_iter_ = 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObMemtableMScanIterator::ObMemtableMScanIterator()
    : ObMemtableScanIterator(),
      ranges_(NULL),
      cur_range_pos_(0)
{
}

ObMemtableMScanIterator::~ObMemtableMScanIterator()
{
  reset();
}

void ObMemtableMScanIterator::reset()
{
  ObMemtableScanIterator::reset();
  ranges_ = NULL;
  cur_range_pos_ = 0;
}

int ObMemtableMScanIterator::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    storage::ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }
  if (OB_ISNULL(table) || OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "table and query_range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_FAIL(ObMemtableScanIterator::init(static_cast<ObMemtable *>(table), param, context))) {
    TRANS_LOG(WARN, "memtable scan iterator init fail", K(ret));
  } else {
    ranges_ = static_cast<const ObIArray<ObDatumRange> *>(query_range);
    cur_range_pos_ = 0;
    is_inited_ = true;
    TRANS_LOG(DEBUG, "ObMemtableMScanIterator inited");
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMemtableMScanIterator::prepare_scan_range()
{
  int ret = OB_SUCCESS;
  if (!is_scan_start_) {
    ret = ObMemtableScanIterator::set_range(ranges_->at(cur_range_pos_));
  }
  return ret;
}

int ObMemtableMScanIterator::is_range_scan(bool &range_scan)
{
  int ret = OB_SUCCESS;
  if (cur_range_pos_ >= ranges_->count()) {
    ret = OB_ITER_END;
    range_scan = false;
  } else if (OB_UNLIKELY(!ranges_->at(cur_range_pos_).is_memtable_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid memtable range", K(ret), K_(cur_range_pos), K(ranges_->at(cur_range_pos_)));
  } else {
    ObStoreRange store_range;
    store_range.set_start_key(ranges_->at(cur_range_pos_).get_start_key().get_store_rowkey());
    store_range.set_end_key(ranges_->at(cur_range_pos_).get_end_key().get_store_rowkey());
    store_range.set_border_flag(ranges_->at(cur_range_pos_).get_border_flag());
    range_scan = !store_range.is_single_rowkey();
  }

  return ret;
}

int ObMemtableMScanIterator::next_range()
{
  int ret = OB_SUCCESS;
  ++cur_range_pos_;
  is_scan_start_ = false;
  return ret;
}

int ObMemtableMScanIterator::get_next_row_for_get(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMemtableMScanIterator has not been inited", K(ret));
  } else if (cur_range_pos_ >= ranges_->count()) {
    ret = OB_ITER_END; // should not happen
  } else {
    const ObDatumRange &range = ranges_->at(cur_range_pos_);
    if (OB_FAIL(memtable_->get(*param_, *context_, range.get_start_key(), row_))) {
      TRANS_LOG(WARN, "fail to get memtable row", K(ret), "table_id", param_->table_id_, "range", range);
    } else {
      row_.scan_index_ = cur_range_pos_;
      row = &row_;
      TRANS_LOG(DEBUG, "get_next_row_for_get row val", K(row_), K(row_.scan_index_), K(row_));
    }
  }

  return ret;
}

int ObMemtableMScanIterator::inner_get_next_row_for_scan(const ObDatumRow *&row)
{
  return ObMemtableScanIterator::inner_get_next_row(row);
}

int ObMemtableMScanIterator::get_next_row_for_scan(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(prepare_scan_range())) {
    TRANS_LOG(WARN, "prepare scan range fail", K(ret));
  } else if (OB_FAIL(inner_get_next_row_for_scan(row))) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "fail to get_next_row", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const_cast<ObDatumRow *>(row)->scan_index_ = cur_range_pos_;
    TRANS_LOG(DEBUG, "get_next_row_for_scan row val", K(this), K(row), K(row->row_flag_), "scan_index", cur_range_pos_);
  }

  return ret;
}

int ObMemtableMScanIterator::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMemtableMScanIterator has not been inited", K(ret));
  } else if (!context_->store_ctx_->mvcc_acc_ctx_.is_valid()) {
    TRANS_LOG(WARN, "read_ctx is invalid", KPC_(context_->store_ctx));
    ret = OB_ERR_UNEXPECTED;
  } else {
    row = NULL;
    while (OB_SUCCESS == ret && NULL == row) {
      bool range_scan = false;
      if (OB_FAIL(is_range_scan(range_scan))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "fail to check is_range_scan", K(ret));
        } else {
          TRANS_LOG(DEBUG, "ObMemtableMScanIterator reaches end");
        }
      } else {
        if (range_scan) {
          TRANS_LOG(DEBUG, "get_next_row_for_scan", K(cur_range_pos_));
          if (OB_FAIL(get_next_row_for_scan(row))) {
            row = NULL;
            if (OB_ITER_END != ret) {
              TRANS_LOG(WARN, "fail to get_next_row_for_scan", K(ret), K(cur_range_pos_));
            } else {
              ret = next_range(); // rewrite error code
            }
          }
        } else {
          TRANS_LOG(DEBUG, "get_next_row_for_get", K(cur_range_pos_));
          if (OB_FAIL(get_next_row_for_get(row))) {
            row = NULL;
            if (OB_ITER_END != ret) {
              TRANS_LOG(WARN, "fail to get_next_row_for_get", K(ret), K(cur_range_pos_));
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          } else {
            ret = next_range();
          }
        }
      }
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * ---------------------------ObMemtableMultiVersionScanIterator----------------------------
 */
ObMemtableMultiVersionScanIterator::ObMemtableMultiVersionScanIterator()
    : MAGIC_(VALID_MAGIC_NUM),
      is_inited_(false),
      read_info_(NULL),
      start_key_(NULL),
      end_key_(NULL),
      context_(NULL),
      row_iter_(),
      row_(),
      key_(NULL),
      value_iter_(NULL),
      key_first_row_(true),
      scan_state_(SCAN_END),
      trans_version_col_idx_(-1),
      sql_sequence_col_idx_(-1),
      row_checker_()
{
  GARL_ADD(&active_resource_, "scan_iter");
}

ObMemtableMultiVersionScanIterator::~ObMemtableMultiVersionScanIterator()
{
  GARL_DEL(&active_resource_);
  reset();
}

int ObMemtableMultiVersionScanIterator::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    storage::ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }

  const ObColDescIArray *rowkey_columns;
  const ObDatumRange *range = static_cast<const ObDatumRange *>(query_range);
  ObMemtable *memtable = static_cast<ObMemtable *>(table);
  if (OB_ISNULL(table) || OB_ISNULL(query_range) || !context.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "table and query range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_ISNULL(rowkey_columns = param.get_out_col_descs())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Unexpected null col descs", K(ret), K(param));
  } else if (OB_ISNULL(read_info_ = param.get_read_info(true))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Unexpected null columns info, ", K(ret), K(param));
  } else if (OB_UNLIKELY(!range->is_memtable_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid datum range", K(ret), K(range));
  } else if (OB_FAIL(ObMemtableKey::build_without_hash(
                  start_key_, *rowkey_columns, &range->get_start_key().get_store_rowkey(), *context.get_range_allocator()))) {
    TRANS_LOG(WARN, "start key build fail", K(param.table_id_), K(range->get_start_key()));
  } else if (OB_FAIL(ObMemtableKey::build_without_hash(
                         end_key_, *rowkey_columns, &range->get_end_key().get_store_rowkey(), *context.get_range_allocator()))) {
    TRANS_LOG(WARN, "end key build fail", K(param.table_id_), K(range->get_end_key()));
  } else {
    TRANS_LOG(DEBUG, "init multi version scan iterator", K(param), K(*range));
    ObMvccScanRange mvcc_scan_range;
    mvcc_scan_range.border_flag_ = range->get_border_flag();
    mvcc_scan_range.start_key_ = start_key_;
    mvcc_scan_range.end_key_ = end_key_;
    if (OB_FAIL(memtable->get_mvcc_engine().scan(context.store_ctx_->mvcc_acc_ctx_,
                                                 mvcc_scan_range,
                                                 context.trans_version_range_,
                                                 row_iter_))) {
      TRANS_LOG(WARN, "mvcc engine scan fail", K(ret), K(mvcc_scan_range));
    } else if (OB_FAIL(bitmap_.init(read_info_->get_request_count(), read_info_->get_rowkey_count()))) {
      TRANS_LOG(WARN, "init nop bitmap fail, ", K(ret));
    } else if (OB_FAIL(row_.init(*context.allocator_, read_info_->get_request_count()))) {
      TRANS_LOG(WARN, "Failed to init datum row", K(ret));
    } else {
      TRANS_LOG(INFO, "multi version scan iterator init succ", K(param.table_id_), K(range), KPC(read_info_), K(row_));
      trans_version_col_idx_ = param.get_schema_rowkey_count();
      sql_sequence_col_idx_ = param.get_schema_rowkey_count() + 1;
      context_ = &context;
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::get_tnode_stat(
    storage::ObTransNodeDMLStat &tnode_stat) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KPC(this));
  } else {
    row_iter_.get_tnode_dml_stat(tnode_stat);
  }
  return ret;
}

// The row format is as follows:
//rowkey｜opposite number of trans_version｜opposite number of Sql_seuqnce | non-rowkey columns
//
//example:
//suppose one row has 5 trans nodes with version 1, 2, 3, 4 and 5 respectively,
// and an uncommited trans node with version MAX, then the iterating order would be:
// |MAX | -20(Sql Sequence)
// |MAX | -8(Sql Sequence)
// －-result after all trans_node compaction（5 versions）
// |---version 4
// |----version 3
// |-----version 2
// |------version 1
int ObMemtableMultiVersionScanIterator::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KPC(this));
  } else {
    do {
      if (SCAN_END != scan_state_) {
        row_reset();
      }
      switch (scan_state_) {
        case SCAN_UNCOMMITTED_ROW:
          if (OB_FAIL(get_uncommitted_row(row))) {
            TRANS_LOG(WARN, "failed to get_uncommitted_row", K(ret));
          }
          break;
        case SCAN_COMPACT_ROW:
          if (OB_FAIL(get_compacted_multi_version_row(row))) {
            TRANS_LOG(WARN, "failed to get_compacted_multi_version_row", K(ret));
          }
          break;
        case SCAN_MULTI_VERSION_ROW:
          if (OB_FAIL(get_multi_version_row(row))) {
            TRANS_LOG(WARN, "failed to get_multi_version_row", K(ret));
          }
          break;
        case SCAN_END:
          if (OB_FAIL(init_next_value_iter())) {
            if (OB_ITER_END != ret) {
              TRANS_LOG(WARN, "failed to init_next_value_iter", K(ret));
            }
          }
          break;
        default:
          break;
      }
      if (OB_SUCC(ret) && OB_FAIL(switch_scan_state())) {
        TRANS_LOG(WARN, "failed to switch scan state", K(ret), K(*this));
      }
    } while (OB_SUCC(ret) && OB_ISNULL(row));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(row)) {
    if (OB_FAIL(row_checker_.check_row_flag_valid(*row, sql_sequence_col_idx_ + 1))) {
      TRANS_LOG(ERROR, "row flag is invalid", K(ret), KPC(this));
      if (OB_NOT_NULL(value_iter_)) {
        value_iter_->print_cur_status();
      }
    } else {
      if (key_first_row_) {
        row_.set_first_multi_version_row();
        key_first_row_ = false;
      }
      TRANS_LOG(TRACE, "after inner get next row", K(*row), K(scan_state_));
    }
  }
  return ret;
}

ObMemtableMultiVersionScanIterator::ObOuputRowValidateChecker::ObOuputRowValidateChecker()
  : output_last_row_flag_(false)
{
}

ObMemtableMultiVersionScanIterator::ObOuputRowValidateChecker::~ObOuputRowValidateChecker()
{
  reset();
}

void ObMemtableMultiVersionScanIterator::ObOuputRowValidateChecker::reset()
{
  output_last_row_flag_ = false;
}

int ObMemtableMultiVersionScanIterator::ObOuputRowValidateChecker::check_row_flag_valid(
    const ObDatumRow &row,
    const int64_t rowkey_cnt)
{
  int ret = OB_SUCCESS;
  if (row.mvcc_row_flag_.is_last_multi_version_row()) {
    if (output_last_row_flag_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "have output row with last flag before", K(ret), K(row));
    } else {
      output_last_row_flag_ = true;
    }
  }
  if (OB_SUCC(ret) && row.row_flag_.is_lock()) {
    if (OB_FAIL(ObLockRowChecker::check_lock_row_valid(row, rowkey_cnt, true/*is_memtable_iter_row_check*/))) {
      TRANS_LOG(WARN, "failed to check lock row valid", K(ret), K(row));
    }
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(row_iter_.get_next_row(key_, value_iter_))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "value iter init fail", K(ret), K(key_));
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::init_next_value_iter()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_row()) || OB_ISNULL(key_) || OB_ISNULL(value_iter_)) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "row_iter_ get_next_row fail", K(ret), KP(key_), KP(value_iter_));
    }
    ret = (OB_SUCCESS == ret) ? OB_ERR_UNEXPECTED : ret;
  } else {
    key_first_row_ = true;
    value_iter_->set_merge_scn(context_->merge_scn_);
    row_checker_.reset();
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::set_compacted_row_state(const bool add_shadow_row)
{
  int ret = OB_SUCCESS;
  // set the opposite number of trans_version as one column of the rowkey,
  // so as to distinguish multiversion rows, as well as ensuring
  // the most recent result of compaction is always in the front
  row_.storage_datums_[trans_version_col_idx_].reuse();
  row_.storage_datums_[sql_sequence_col_idx_].reuse();
  row_.storage_datums_[trans_version_col_idx_].set_int(-value_iter_->get_committed_max_trans_version());
  row_.row_flag_.fuse_flag(value_iter_->get_row_first_dml_flag());
  row_.mvcc_row_flag_.set_last_multi_version_row(value_iter_->is_multi_version_iter_end());
  if (add_shadow_row) {
    row_.set_shadow_row();
    row_.storage_datums_[sql_sequence_col_idx_].set_int(-INT64_MAX);
  } else {
    // sql_sequence of committed data is 0
    row_.storage_datums_[sql_sequence_col_idx_].set_int(0);
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::get_compacted_multi_version_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KPC(this));
  } else {
    const ObStoreRowkey *rowkey = NULL;
    key_->get_rowkey(rowkey);
    if (OB_FAIL(iterate_compacted_row(*rowkey, row_))) {
      TRANS_LOG(WARN, "iterate_row fail", K(ret), KP(key_), KP(value_iter_));
    } else if (OB_FAIL(set_compacted_row_state(value_iter_->has_multi_commit_trans()))) { // set state for compacted row
      TRANS_LOG(WARN, "failed to set state for compated row", K(row_));
    } else if (row_.row_flag_.is_exist()) {
      row = &row_;
    }
    if (NULL != row && OB_SUCC(ret)) {
      TRANS_LOG(DEBUG, "get_compacted_multi_version_row", K(ret), K(*rowkey), K(*row));
    }
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::get_multi_version_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KPC(this));
  } else {
    const ObStoreRowkey *rowkey = NULL;
    key_->get_rowkey(rowkey);
    if (OB_FAIL(iterate_multi_version_row(*rowkey, row_))) {
      TRANS_LOG(WARN, "iterate_row fail", K(ret), KP(key_), KP(value_iter_));
    } else if (row_.row_flag_.is_exist()) {
      row_.mvcc_row_flag_.set_last_multi_version_row(value_iter_->is_multi_version_iter_end());
      row = &row_;
    }
    if (NULL != row && OB_SUCC(ret)) {
      TRANS_LOG(DEBUG, "get_multi_version_row", K(ret), K(*rowkey), K(*row));
    }
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::get_uncommitted_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KPC(this));
  } else {
    const ObStoreRowkey *rowkey = key_->get_rowkey();
    if (OB_FAIL(iterate_uncommitted_row(
            *rowkey,
            row_))) {
      TRANS_LOG(WARN, "failed to iterate_uncommitted_row", K(ret));
      // transaction may have been commited right before our accessing,
      // so iterate uncommitted row may not read any data,
      // and flag OP_ROW_DOEST_NOT_EXIST stands for such situation
    } else if (row_.row_flag_.is_exist()) {
      row_.mvcc_row_flag_.set_last_multi_version_row(value_iter_->is_compact_iter_end());
      row_.mvcc_row_flag_.set_uncommitted_row(true);
      row = &row_;
      TRANS_LOG(TRACE, "get_uncommitted_row", K(*rowkey), K(*row));
    }
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::switch_to_committed_scan_state()
{
  int ret = OB_SUCCESS;
  if (!value_iter_->is_compact_iter_end()) { // iter compacted not finish
    scan_state_ = SCAN_COMPACT_ROW;
    if (OB_FAIL(value_iter_->init_multi_version_iter())) {
      LOG_WARN("Fail to init multi version iter", K(ret), K_(scan_state), KPC_(value_iter));
    }
  } else {
    scan_state_ = SCAN_END;
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::switch_scan_state()
{
  int ret = OB_SUCCESS;
  switch (scan_state_) {
  case SCAN_UNCOMMITTED_ROW:
    if (value_iter_->is_trans_node_iter_null()
        || row_.row_flag_.is_not_exist()) {
      ret = switch_to_committed_scan_state();
    }
    break;
  case SCAN_COMPACT_ROW:
    if (!value_iter_->is_multi_version_iter_end()) {
      scan_state_ = SCAN_MULTI_VERSION_ROW;
    } else {
      scan_state_ = SCAN_END;
    }
    break;
  case SCAN_MULTI_VERSION_ROW:
    if (value_iter_->is_multi_version_iter_end()) {
      scan_state_ = SCAN_END;
    }
    break;
  case SCAN_END:
    if (!value_iter_->is_trans_node_iter_null()) {
      scan_state_ = SCAN_UNCOMMITTED_ROW;
    } else {
      ret = switch_to_committed_scan_state();
    }
    break;
  default:
    break;
  }
  return ret;
}

void ObMemtableMultiVersionScanIterator::reset()
{
  is_inited_ = false;
  read_info_ = NULL;
  start_key_ = NULL;
  end_key_ = NULL;
  row_.reset();
  key_first_row_ = true;
  bitmap_.reuse();
  scan_state_ = SCAN_END;
  trans_version_col_idx_ = -1;
  sql_sequence_col_idx_ = -1;
  row_checker_.reset();
}

void ObMemtableMultiVersionScanIterator::row_reset()
{
  row_.row_flag_.reset();
  row_.scan_index_ = 0;
  row_.mvcc_row_flag_.reset();
  row_.trans_id_.reset();
  bitmap_.reuse();
  for (int64_t i = 0; i < row_.get_column_count(); i++) {
    row_.storage_datums_[i].set_nop();
  }
}

OB_INLINE int ObMemtableMultiVersionScanIterator::iterate_multi_version_row(
    const ObStoreRowkey &rowkey,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReadRow::iterate_row_key(rowkey, row))) {
    TRANS_LOG(WARN, "iterate_row_key fail", K(ret), K(rowkey));
  } else if (OB_FAIL(iterate_multi_version_row_value_(row))) {
    TRANS_LOG(WARN, "iterate_multi_version_row_value_ fail", K(ret));
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::iterate_compacted_row(
    const ObStoreRowkey &key,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value_iter_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "iterate row get invalid argument", K(ret), KP(value_iter_));
  } else if (OB_FAIL(ObReadRow::iterate_row_key(key, row))) {
    TRANS_LOG(WARN, "iterate_row_key fail", K(ret), K(key));
  } else if (OB_FAIL(iterate_compacted_row_value_(row))) {
    TRANS_LOG(WARN, "iterate_row_value fail", K(ret), K(key), KP(value_iter_));
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::iterate_uncommitted_row(
    const ObStoreRowkey &key,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReadRow::iterate_row_key(key, row))) {
    TRANS_LOG(WARN, "failed to iterate rowkey", K(ret), K(key));
  } else if (OB_FAIL(iterate_uncommitted_row_value_(row))) {
    TRANS_LOG(WARN, "failed to iterate_uncommitted_row_value_", K(ret), K(key));
  }
  return ret;
}

void ObMemtableMultiVersionScanIterator::set_flag_and_version_for_compacted_row(
    const ObMvccTransNode *tnode, ObDatumRow &row)
{
  const bool is_committed = reinterpret_cast<const ObMvccTransNode*>(tnode)->is_committed();
  const int64_t trans_version = is_committed
    ? reinterpret_cast<const ObMvccTransNode*>(tnode)->trans_version_.get_val_for_tx() : INT64_MAX;
  row.snapshot_version_ = std::max(trans_version, row.snapshot_version_);
  STORAGE_LOG(DEBUG, "row snapshot version", K(row.snapshot_version_));
}

int ObMemtableMultiVersionScanIterator::iterate_uncommitted_row_value_(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  ObRowReader row_reader;
  const void *tnode = NULL;
  const ObMemtableDataHeader *mtd = NULL;
  transaction::ObTxSEQ sql_seq;
  transaction::ObTxSEQ first_sql_sequence;
  int64_t trans_version = INT64_MAX;
  SCN trans_scn;
  bool same_sql_sequence_flag = true;
  if (OB_ISNULL(value_iter_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(value_iter_), K(row.trans_id_));
  } else {
    bitmap_.reuse();
    while (OB_SUCC(ret)) {
      if (first_sql_sequence.is_valid()
          && OB_FAIL(value_iter_->check_next_sql_sequence(row.trans_id_, first_sql_sequence, same_sql_sequence_flag))) {
        TRANS_LOG(WARN, "failed to check next sql sequence", K(ret), K(tnode));
      } else if (!same_sql_sequence_flag) { // different sql sequence need break
        break;
      } else if (OB_FAIL(value_iter_->get_next_uncommitted_node(tnode,
                                                                row.trans_id_,
                                                                trans_scn,
                                                                sql_seq))){
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "failed to get next uncommitted node", K(ret), K(tnode));
        }
      } else if (FALSE_IT(trans_version = trans_scn.get_val_for_tx())) {
      } else if (row.row_flag_.is_delete()) {
        continue;
      } else {
        const ObMvccTransNode *trans_node = reinterpret_cast<const ObMvccTransNode *>(tnode);
        if (OB_ISNULL(trans_node)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "trans node is null", K(ret), KP(trans_node));
        } else if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader *>(trans_node->buf_))) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "trans node value is null", K(ret), KP(trans_node), KP(mtd));
        } else {
          bool read_finished = false;
          if (OB_FAIL(row_reader.read_memtable_row(mtd->buf_, mtd->buf_len_, *read_info_, row, bitmap_, read_finished))) {
            TRANS_LOG(WARN, "Failed to read memtable row", K(ret));
          } else if (!first_sql_sequence.is_valid()) { // record sql sequence
            first_sql_sequence = sql_seq;
            row.storage_datums_[trans_version_col_idx_].reuse();
            row.storage_datums_[sql_sequence_col_idx_].reuse();
            row.storage_datums_[trans_version_col_idx_].set_int(-trans_version);
            row.storage_datums_[sql_sequence_col_idx_].set_int(-sql_seq.cast_to_int());
            row.row_flag_.set_flag(mtd->dml_flag_);
          } else {
            row.row_flag_.fuse_flag(mtd->dml_flag_);
          }
        }
      }
    } // end of while
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;

  return ret;
}

int ObMemtableMultiVersionScanIterator::iterate_compacted_row_value_(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool read_finished = false;
  const void *tnode = NULL;
  const ObMemtableDataHeader *mtd = NULL;
  row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  row.snapshot_version_ = 0;
  bitmap_.reuse();

  while (OB_SUCC(ret)) {
    if (OB_FAIL(value_iter_->get_next_node_for_compact(tnode))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "failed to get next node", K(ret), K(*this), K(value_iter_));
      }
    } else if (OB_ISNULL(tnode)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
    } else if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader *>(reinterpret_cast<const ObMvccTransNode *>(tnode)->buf_))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "transa node value is null", K(ret), KP(tnode), KP(mtd));
    } else {
      set_flag_and_version_for_compacted_row(reinterpret_cast<const ObMvccTransNode *>(tnode), row);
      if (row.row_flag_.is_not_exist()) {
        row.row_flag_.set_flag(mtd->dml_flag_);
      }
      if (OB_FAIL(row_reader_.read_memtable_row(mtd->buf_, mtd->buf_len_, *read_info_, row, bitmap_, read_finished))) {
        TRANS_LOG(WARN, "Failed to read row without", K(ret));
      } else if (ObDmlFlag::DF_INSERT == mtd->dml_flag_ || ObDmlFlag::DF_DELETE == mtd->dml_flag_ || read_finished) {
        if (bitmap_.is_empty() || ObDmlFlag::DF_DELETE == mtd->dml_flag_) {
          row.set_compacted_multi_version_row();
        }
        break;
      }
    }
  } // while
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  return ret;
}

int ObMemtableMultiVersionScanIterator::iterate_multi_version_row_value_(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  const void *tnode = NULL;
  const void *data = NULL;
  int64_t trans_version = INT64_MIN;
  int64_t compare_trans_version = INT64_MAX;
  const ObVersionRange &version_range = context_->trans_version_range_;
  const ObMemtableDataHeader *mtd = NULL;
  if (OB_ISNULL(value_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Unexpected null value iter", K(ret), K(value_iter_));
  }
  bitmap_.reuse();
  while (OB_SUCC(ret)) {
    if (OB_FAIL(value_iter_->get_next_multi_version_node(tnode))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "failed to get next node", K(ret), K(*this), K(value_iter_));
      }
    } else if (OB_ISNULL(tnode)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
    } else if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader *>(reinterpret_cast<const ObMvccTransNode *>(tnode)->buf_))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node value is null", K(ret), KP(tnode), KP(mtd));
    } else {
      if (row.row_flag_.is_not_exist()) {
        row.row_flag_.set_flag(mtd->dml_flag_);
      }
      compare_trans_version = reinterpret_cast<const ObMvccTransNode *>(tnode)->trans_version_.get_val_for_tx();
      if (compare_trans_version <= version_range.base_version_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "trans version smaller than base version", K(compare_trans_version), K(version_range.base_version_));
      }
    }

    bool read_finished = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(row_reader_.read_memtable_row(mtd->buf_, mtd->buf_len_, *read_info_, row, bitmap_, read_finished))) {
      TRANS_LOG(WARN, "Failed to read row without rowkey", K(ret));
    } else {
      if (compare_trans_version > trans_version) {
        trans_version = compare_trans_version;
        row.storage_datums_[trans_version_col_idx_].reuse();
        row.storage_datums_[sql_sequence_col_idx_].reuse();
        row.storage_datums_[trans_version_col_idx_].set_int(-trans_version);
        row.storage_datums_[sql_sequence_col_idx_].set_int(0);
        row.row_flag_.fuse_flag(mtd->dml_flag_);
      }
      if (row.row_flag_.is_lock()) {
        row.row_flag_ = mtd->dml_flag_;
      }
      if (bitmap_.is_empty() || row.row_flag_.is_delete()) {
        row.set_compacted_multi_version_row();
      }
      if (trans_version > version_range.multi_version_start_
          && value_iter_->is_cur_multi_version_row_end()) {
        break;
      }
    }
  } // while
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  return ret;
}



////////////////////////////////////////////////////////////////////////////////////////////////////

OB_INLINE int ObReadRow::iterate_row_key(const ObStoreRowkey &rowkey, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj *obj_ptr = rowkey.get_obj_ptr();
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
    if (OB_FAIL(row.storage_datums_[i].from_obj_enhance(obj_ptr[i]))) {
      TRANS_LOG(WARN, "Failed to transform obj to datum", K(ret), K(i), K(rowkey));
    } else if (OB_UNLIKELY(row.storage_datums_[i].is_nop_value())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "col in rowkey is unexpected nop", K(ret), K(i), K(rowkey));
    }
  }
  return ret;
}

OB_INLINE int ObReadRow::iterate_row_value_(
    const ObITableReadInfo &read_info,
    common::ObIAllocator &allocator,
    ObMvccValueIterator &value_iter,
    ObDatumRow &row,
    ObNopBitMap &bitmap,
    int64_t &row_scn)
{
  int ret = OB_SUCCESS;
  const void *tnode = NULL;
  const ObMemtableDataHeader *mtd = NULL;
  bool read_finished = false;
  ObRowReader reader;
  row_scn = 0;
  row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  row.snapshot_version_ = 0;
  while (OB_SUCC(ret) && OB_SUCC(value_iter.get_next_node(tnode))) {
    if (OB_ISNULL(tnode)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
    } else if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader *>(reinterpret_cast<const ObMvccTransNode *>(tnode)->buf_))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "transa node value is null", K(ret), KP(tnode), KP(mtd));
    } else {
      const bool is_committed = reinterpret_cast<const ObMvccTransNode *>(tnode)->is_committed();
      const int64_t trans_version = is_committed ? reinterpret_cast<const ObMvccTransNode *>(tnode)->trans_version_.get_val_for_tx() : INT64_MAX;
      if (row.row_flag_.is_not_exist()) {
        if (ObDmlFlag::DF_DELETE == mtd->dml_flag_) {
          row.row_flag_.set_flag(ObDmlFlag::DF_DELETE);
        } else {
          row.row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
        }
      }
      row.snapshot_version_ = std::max(trans_version, row.snapshot_version_);
      if (row.snapshot_version_ == INT64_MAX) {
        row.set_have_uncommited_row();
      }
      TRANS_LOG(DEBUG, "row snapshot version", K(row.snapshot_version_));

      if (OB_FAIL(reader.read_memtable_row(mtd->buf_, mtd->buf_len_, read_info, row, bitmap, read_finished))) {
        TRANS_LOG(WARN, "Failed to read memtable row", K(ret));
      } else if (0 == row_scn) {
        const ObMvccTransNode *tx_node = reinterpret_cast<const ObMvccTransNode *>(tnode);
        const ObTransID snapshot_tx_id = value_iter.get_snapshot_tx_id();
        const ObTransID reader_tx_id = value_iter.get_reader_tx_id();
        share::SCN row_version = tx_node->trans_version_;
        row_scn = row_version.get_val_for_tx();
        if (!row.is_have_uncommited_row() &&
            !value_iter.get_mvcc_acc_ctx()->is_standby_read_
            && !(snapshot_tx_id == tx_node->get_tx_id() || reader_tx_id == tx_node->get_tx_id())
            && row_version.is_max()) {
          TRANS_LOG(ERROR, "meet row scn with undecided value", KPC(tx_node),
                    K(is_committed), K(trans_version), K(value_iter));
        }
      }
      if (OB_SUCC(ret) &&(ObDmlFlag::DF_INSERT == mtd->dml_flag_ || read_finished)) {
        LOG_DEBUG("chaser debug iter memtable row", KPC(mtd), K(read_finished));
        break;
      }
    }
  } // while

  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  return ret;
}

int ObReadRow::iterate_row(
    const ObITableReadInfo &read_info,
    const ObStoreRowkey &key,
    common::ObIAllocator &allocator,
    ObMvccValueIterator &value_iter,
    ObDatumRow &row,
    ObNopBitMap &bitmap,
    int64_t &row_scn)
{
  int ret = OB_SUCCESS;
  bitmap.reuse();
  if (OB_FAIL(iterate_row_key(key, row))) {
    TRANS_LOG(WARN, "Failed to iterate_row_key", K(ret), K(key));
  } else if (OB_FAIL(iterate_row_value_(read_info, allocator, value_iter, row, bitmap, row_scn))) {
    TRANS_LOG(WARN, "Failed to iterate_row_value", K(ret), K(key));
  } else {
    if (!bitmap.is_empty()) {
      bitmap.set_nop_datums(row.storage_datums_);
    }
    TRANS_LOG(DEBUG, "Success to iterate memtable row", K(key), K(row), K(bitmap.get_nop_cnt()));
    // do nothing
  }
  return ret;
}

}
}
