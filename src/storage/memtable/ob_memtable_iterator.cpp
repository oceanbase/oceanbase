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
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/memtable/ob_memtable_block_row_scanner.h"
#include "storage/memtable/ob_memtable_read_row_util.h"
#include "storage/truncate_info/ob_truncate_partition_filter.h"
#include "ob_memtable.h"

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
    storage::ObIMemtable &memtable)
{
  int ret = OB_SUCCESS;
  char *trans_info_ptr = nullptr;
  if (is_inited_) {
    reset();
  }
  const ObITableReadInfo *read_info = param.get_read_info(context.use_fuse_row_cache_);
  context_ = &context;
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
    if (OB_UNLIKELY(IF_NEED_CHECK_BASE_VERSION_FILTER(context_) &&
                    OB_FAIL(context_->check_filtered_by_base_version(cur_row_)))) {
      TRANS_LOG(WARN, "check base version filter fail", K(ret));
    }
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
      param_(nullptr),
      context_(nullptr),
      mt_blk_scanner_(nullptr),
      single_row_reader_(),
      cur_range_()
{
  GARL_ADD(&active_resource_, "scan_iter");
}

ObMemtableScanIterator::~ObMemtableScanIterator()
{
  GARL_DEL(&active_resource_);
  FREE_ITER_FROM_ALLOCATOR(context_->allocator_, mt_blk_scanner_, ObMemtableBlockRowScanner);
  //reset is not necessary since there is no resource to release
  //reset();
}

int ObMemtableScanIterator::init(const ObTableIterParam &param,
                                 ObTableAccessContext &context,
                                 ObITable *table,
                                 const void *query_range)
{
  int ret = OB_SUCCESS;
  const ObDatumRange *range = static_cast<const ObDatumRange *>(query_range);
  if (OB_FAIL(base_init_(param, context, table, query_range))) {
    STORAGE_LOG(WARN, "init failed", KR(ret), K(param), K(context));
  } else {
    is_inited_ = true;
    if (OB_FAIL(set_range(*range))) {
      is_inited_ = false;
      STORAGE_LOG(WARN, "set scan range fail", K(ret), K(*range));
    }
  }
  return ret;
}

int ObMemtableScanIterator::base_init_(const ObTableIterParam &param,
                                       ObTableAccessContext &context,
                                       ObITable *table,
                                       const void *query_range)
{
  int ret = OB_SUCCESS;
  context_ = &context;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "try to init memtable scan iterator twice", KR(ret), K(param));
  } else if (OB_ISNULL(table) || OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table and query_range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_FAIL(single_row_reader_.init(static_cast<ObMemtable *>(table), param, context))) {
    STORAGE_LOG(WARN, "init scan iterator fail", K(ret));
  } else if (param.is_delete_insert_ && OB_FAIL(enable_block_scan_(param, context))) {
    STORAGE_LOG(WARN, "enable block scan for memtable scan iterator failed", KR(ret), K(param));
  } else {
    // base init finish
    param_ = &param;
  }
  return ret;
}

int ObMemtableScanIterator::enable_block_scan_(const ObTableIterParam &param, ObTableAccessContext &context)
{
  int ret = OB_SUCCESS;
  void *blk_scanner_buffer = nullptr;
  if (OB_ISNULL(blk_scanner_buffer = context.allocator_->alloc(sizeof(ObMemtableBlockRowScanner)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "allocate block scan ctx failed", KR(ret));
  } else if (FALSE_IT(mt_blk_scanner_ = new (blk_scanner_buffer) ObMemtableBlockRowScanner(*context.allocator_))) {
  } else if (OB_FAIL(mt_blk_scanner_->init(param, context, single_row_reader_))) {
    TRANS_LOG(WARN, "Failed to init datum row batch", KR(ret));
  } else {
    if (param.is_delete_insert_) {
      context.store_ctx_->mvcc_acc_ctx_.major_snapshot_ = context.trans_version_range_.base_version_;
      context.store_ctx_->mvcc_acc_ctx_.is_delete_insert_ = param.is_delete_insert_;
    }
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

// TODO :@gengli.wzy support get_next_rows for memtable
// int ObMemtableScanIterator::get_next_rows()
// {
//   int ret = OB_SUCCESS;

//   return ret;
// }

int ObMemtableScanIterator::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret), KP(this));
  } else if (!is_scan_start_) {
    // before scan each range, this flag would be reset to false
    if (OB_FAIL(single_row_reader_.init_a_new_range(cur_range_))) {
      STORAGE_LOG(WARN, "failed to init a new range for memtable scan iterator", KR(ret));
    } else {
      is_scan_start_ = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!param_->is_delete_insert_) {
    ret = single_row_reader_.get_next_row(row);
  } else {
    ret = mt_blk_scanner_->get_next_row(row);
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(IF_NEED_CHECK_BASE_VERSION_FILTER(context_) &&
                    OB_FAIL(context_->check_filtered_by_base_version(*(const_cast<ObDatumRow *>(row)))))) {
      TRANS_LOG(WARN, "check base version filter fail", K(ret));
    }
  } else if (OB_ITER_END != ret) {
    STORAGE_LOG(WARN,
                "get next row from memtable scan iteartor failed",
                KR(ret),
                K(is_scan_start_),
                K(param_->is_delete_insert_));
  }
  STORAGE_LOG(DEBUG, "finish memtable get next row", KR(ret), K(is_scan_start_), K(param_->is_delete_insert_), KPC(row));
  return ret;
}

void ObMemtableScanIterator::reset()
{
  is_inited_ = false;
  is_scan_start_ = false;
  FREE_ITER_FROM_ALLOCATOR(context_->allocator_, mt_blk_scanner_, ObMemtableBlockRowScanner);
  context_ = nullptr;
  param_ = nullptr;
  cur_range_.reset();
  single_row_reader_.reset();
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
  context_ = &context;
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
      if (OB_UNLIKELY(IF_NEED_CHECK_BASE_VERSION_FILTER(context_) &&
                      OB_FAIL(context_->check_filtered_by_base_version(cur_row_)))) {
        TRANS_LOG(WARN, "check base version filter fail", K(ret));
      }
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

  if (OB_FAIL(ObMemtableScanIterator::base_init_(param ,context, table, query_range))) {
    STORAGE_LOG(WARN, "base init failed", KR(ret));
  } else {
    ranges_ = static_cast<const ObIArray<ObDatumRange> *>(query_range);
    cur_range_pos_ = 0;
    is_inited_ = true;
    is_scan_start_ = false;
    TRANS_LOG(DEBUG, "ObMemtableMScanIterator inited");
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMemtableMScanIterator::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemtableMScanIterator has not been inited", K(ret));
  } else {
    row = NULL;
    while (OB_SUCCESS == ret && NULL == row) {
      if (cur_range_pos_ >= ranges_->count()) {
        ret = OB_ITER_END;
        STORAGE_LOG(DEBUG, "ObMemtableMScanIterator reaches end");
      } else if (!is_scan_start_ && OB_FAIL(ObMemtableScanIterator::set_range(ranges_->at(cur_range_pos_)))) {
        STORAGE_LOG(WARN, "set scan range failed", KR(ret), K(ranges_), K(cur_range_pos_));
      } else if (OB_FAIL(ObMemtableScanIterator::inner_get_next_row(row))) {
        row = NULL;
        if (OB_ITER_END == ret) {
          // current range is completely iterated
          ret = OB_SUCCESS;  // rewrite error code
          ++cur_range_pos_;
          is_scan_start_ = false;
        } else {
          STORAGE_LOG(WARN, "failed to get next row", K(ret), K(cur_range_pos_));
        }
      } else {
        const_cast<ObDatumRow *>(row)->scan_index_ = cur_range_pos_;
        if (OB_UNLIKELY(IF_NEED_CHECK_BASE_VERSION_FILTER(context_) &&
                        OB_FAIL(context_->check_filtered_by_base_version(*(const_cast<ObDatumRow *>(row)))))) {
          TRANS_LOG(WARN, "check base version filter fail", K(ret));
        }
        STORAGE_LOG(DEBUG, "get_next_row_for_scan row val", K(this), K(row), K(row->row_flag_), K(cur_range_pos_));
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
      enable_delete_insert_(false),
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
  context_ = &context;
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
                  start_key_, *rowkey_columns, &range->get_start_key().get_store_rowkey(), *context.allocator_))) {
    TRANS_LOG(WARN, "start key build fail", K(param.table_id_), K(range->get_start_key()));
  } else if (OB_FAIL(ObMemtableKey::build_without_hash(
                         end_key_, *rowkey_columns, &range->get_end_key().get_store_rowkey(), *context.allocator_))) {
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
      TRANS_LOG(TRACE, "multi version scan iterator init succ", K(param.table_id_), K(range), KPC(read_info_), K(row_));
      enable_delete_insert_ = param.is_delete_insert_;
      trans_version_col_idx_ = param.get_schema_rowkey_count();
      sql_sequence_col_idx_ = param.get_schema_rowkey_count() + 1;
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

int ObMemtableMultiVersionScanIterator::set_compacted_row_state(ObDatumRow &row, const bool add_shadow_row)
{
  int ret = OB_SUCCESS;
  // set the opposite number of trans_version as one column of the rowkey,
  // so as to distinguish multiversion rows, as well as ensuring
  // the most recent result of compaction is always in the front
  row.storage_datums_[trans_version_col_idx_].reuse();
  row.storage_datums_[sql_sequence_col_idx_].reuse();
  row.storage_datums_[trans_version_col_idx_].set_int(-value_iter_->get_committed_max_trans_version());
  row.row_flag_.fuse_flag(value_iter_->get_row_first_dml_flag());
  row.mvcc_row_flag_.set_last_multi_version_row(value_iter_->is_multi_version_iter_end());
  if (add_shadow_row) {
    if (OB_FAIL(ObShadowRowUtil::make_shadow_row(sql_sequence_col_idx_, row))) {
      LOG_WARN("failed to make shadow row", K(ret), K(row), K_(sql_sequence_col_idx));
    }
  } else {
    // sql_sequence of committed data is 0
    row.storage_datums_[sql_sequence_col_idx_].set_int(0);
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
    if (enable_delete_insert_ && !value_iter_->has_multi_commit_trans() &&
        !value_iter_->is_last_compact_node()) {
      // ouput all delete_insert rows in single trans
    } else if (!value_iter_->is_multi_version_iter_end()) {
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
  enable_delete_insert_ = false;
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
  } else if (enable_delete_insert_ && !value_iter_->has_multi_commit_trans()) {
    if (OB_FAIL(iterate_delete_insert_compacted_row_value_(row))) {
      TRANS_LOG(WARN, "iterate_delete_insert_row_value fail", K(ret), K(key), KP(value_iter_));
    }
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
  const bool is_committed = tnode->is_committed();
  const int64_t trans_version = is_committed ? tnode->trans_version_.get_val_for_tx() : INT64_MAX;
  row.snapshot_version_ = std::max(trans_version, row.snapshot_version_);
  STORAGE_LOG(DEBUG, "row snapshot version", K(row.snapshot_version_));
}

int ObMemtableMultiVersionScanIterator::iterate_uncommitted_row_value_(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  ObCompatRowReader row_reader;
  const void *tnode = NULL;
  const ObMemtableDataHeader *mtd = NULL;
  const ObRowHeader *row_header = nullptr;
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
          if (OB_FAIL(row_reader.read_memtable_row(mtd->buf_, mtd->buf_len_, *read_info_, row, bitmap_, read_finished, row_header))) {
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
  const ObRowHeader *row_header = nullptr;
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
      if (OB_FAIL(row_reader_.read_memtable_row(mtd->buf_, mtd->buf_len_, *read_info_, row, bitmap_, read_finished, row_header))) {
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

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_compacted_row_state(row, value_iter_->has_multi_commit_trans()))) { // set state for compated row
    TRANS_LOG(WARN, "failed to set state for compated row", K(row));
  }
  return ret;
}

// compact delete_insert rows which are in one transaction
// delete1 -> insert1 -> delete2 -> insert2 -> delete3 -> insert3 ==> delete1 -> insert3
// insert1 -> delete2-> insert2 -> delete3-> insert3 ==> insert3
// delete1 -> insert1 -> delete2 -> insert2 -> delete3 ==> delete1
// insert1 -> delete2 -> insert2 -> delete3 ==> delete3(need output last flag)
int ObMemtableMultiVersionScanIterator::iterate_delete_insert_compacted_row_value_(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool read_finished = false;
  const void *tnode = NULL;
  const ObMemtableDataHeader *mtd = NULL;
  const ObRowHeader *row_header = nullptr;
  row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  row.snapshot_version_ = 0;
  bitmap_.reuse();

  while (OB_SUCC(ret)) {
    bool first_delete_node = value_iter_->is_first_delete_compact_node();
    if (first_delete_node && row.row_flag_.is_insert()) {
      // delete1->insert1->delete2->insert2: cur_node(delete1), cur_row(insert2)
      break;
    } else if (OB_FAIL(value_iter_->get_next_node_for_compact(tnode))) {
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
      if (first_delete_node) {
        // delete1->insert1->delete2: cur_node(delete1), cur_row(delete2), reset cur_row and read first_delete_node
        row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
        bitmap_.reuse();
      }

      read_finished = false;
      if (OB_FAIL(row_reader_.read_memtable_row(mtd->buf_, mtd->buf_len_, *read_info_, row, bitmap_, read_finished, row_header))) {
        TRANS_LOG(WARN, "Failed to read memtable row", K(ret), K(row));
      } else if (OB_UNLIKELY(!read_finished && read_info_->get_schema_column_count() == row_header->get_column_count())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "Unexpected not empty bitmap", K(ret), K(row), K(read_finished), KPC(row_header), KPC_(read_info));
      } else {
        row.set_compacted_multi_version_row();
        if (row.row_flag_.is_not_exist()) {
          row.storage_datums_[trans_version_col_idx_].reuse();
          row.storage_datums_[sql_sequence_col_idx_].reuse();
          row.storage_datums_[trans_version_col_idx_].set_int(-value_iter_->get_committed_max_trans_version());
          row.storage_datums_[sql_sequence_col_idx_].set_int(0);
          row.row_flag_.set_flag(mtd->dml_flag_);
        }

        if (value_iter_->is_last_compact_node()) {
          row.mvcc_row_flag_.set_last_multi_version_row(value_iter_->is_multi_version_iter_end());
          break;
        }
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
  const ObRowHeader *row_header = nullptr;
  if (OB_ISNULL(value_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Unexpected null value iter", K(ret), K(value_iter_));
  }
  bitmap_.reuse();
  while (OB_SUCC(ret)) {
    bool first_delete_node = value_iter_->is_first_delete_multi_version_node();
    if (enable_delete_insert_ && first_delete_node && row.row_flag_.is_insert()) {
      // delete1->insert1->delete2->insert2: cur_node(delete1), cur_row(insert2)
      // not from update, insert after delete in one trans also need to mark as delete_insert
      break;
    } else if (OB_FAIL(value_iter_->get_next_multi_version_node(tnode))) {
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
      } else if (enable_delete_insert_ && first_delete_node) {
        // delete1->insert1->delete2: cur_node(delete1), cur_row(delete2), reset cur_row and read first_delete_node
        bitmap_.reuse();
        trans_version = INT64_MIN;
        row.row_flag_.set_flag(mtd->dml_flag_);
      }
      compare_trans_version = reinterpret_cast<const ObMvccTransNode *>(tnode)->trans_version_.get_val_for_tx();
    }

    bool read_finished = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(row_reader_.read_memtable_row(mtd->buf_, mtd->buf_len_, *read_info_, row, bitmap_, read_finished, row_header))) {
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
        // TODO: @dengzhi.ldz return empty when first row is insert and last row is delete
        break;
      }
    }
  } // while
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * ---------------------------ObMemtableSkipScanIterator----------------------------
 */
ObMemtableSkipScanIterator::ObMemtableSkipScanIterator()
    : ObMemtableScanIterator(),
      is_end_(false),
      is_skip_start_(false),
      is_false_range_(false),
      memtable_(nullptr),
      skip_scanner_(nullptr)
{
}

ObMemtableSkipScanIterator::~ObMemtableSkipScanIterator()
{
  storage::ObIndexSkipScanFactory::destroy_index_skip_scanner(skip_scanner_);
}

int ObMemtableSkipScanIterator::init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    storage::ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMemtableScanIterator::init(param, context, table, query_range))) {
    LOG_WARN("failed to init memtable scan iter", K(ret));
  } else if ((nullptr == skip_scanner_ || !skip_scanner_->is_opened()) &&
             OB_FAIL(storage::ObIndexSkipScanFactory::build_index_skip_scanner(param,
                                                                               context,
                                                                               static_cast<const ObDatumRange *>(query_range),
                                                                               skip_scanner_,
                                                                               true/*is_for_memtable*/))) {
    LOG_WARN("failed to build index skip scanner", K(ret));
  } else {
    memtable_ = static_cast<ObMemtable *>(table);
    param_ = &param;
    context_ = &context;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObMemtableSkipScanIterator::reset()
{
  ObMemtableScanIterator::reset();
  storage::ObIndexSkipScanFactory::destroy_index_skip_scanner(skip_scanner_);
  is_end_ = false;
  is_skip_start_ = false;
  is_false_range_ = false;
  memtable_ = nullptr;
  param_ = nullptr;
  context_ = nullptr;
}

void ObMemtableSkipScanIterator::reuse()
{
  is_end_ = false;
  is_skip_start_ = false;
  is_false_range_ = false;
  memtable_ = nullptr;
  param_ = nullptr;
  context_ = nullptr;
  if (nullptr != skip_scanner_) {
    skip_scanner_->reuse();
  }
  ObMemtableScanIterator::reset();
}

int ObMemtableSkipScanIterator::skip_to_range(const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  is_false_range_ = false;
  if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range", K(ret), K(range));
  } else if (OB_FAIL(check_always_false(range, is_false_range_))) {
    LOG_WARN("failed to check always false", K(ret));
  } else if  (!is_false_range_) {
    ObMemtableScanIterator::reset();
    if (OB_FAIL(init(*param_, *context_, memtable_, &range))) {
      LOG_WARN("failed to init memtable iter", K(ret));
    }
    LOG_DEBUG("[INDEX SKIP SCAN] memtable skip to range", K(ret), K(range));
  }
  return ret;
}

int ObMemtableSkipScanIterator::check_always_false(const ObDatumRange &range, bool &is_false)
{
  int ret = OB_SUCCESS;
  const ObStorageDatumUtils &datum_utils = param_->get_read_info()->get_datum_utils();
  int cmp = 0;
  if (OB_FAIL(range.get_start_key().compare(range.get_end_key(), datum_utils, cmp))) {
    LOG_WARN("failed to compare", K(ret), K(range));
  } else {
    is_false = (cmp > 0) || (0 == cmp && (range.is_left_open() || range.is_right_open()));
    if (is_false) {
      LOG_DEBUG("[INDEX SKIP SCAN] always false range", K(ret), K(range), K(range.border_flag_), K(lbt()));
    }
  }
  return ret;
}

int ObMemtableSkipScanIterator::get_next_skip_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (is_false_range_) {
    ret = OB_ITER_END;
  } else {
    ret = ObMemtableScanIterator::inner_get_next_row(row);
  }
  return ret;
}

int ObMemtableSkipScanIterator::inner_get_next_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (is_end_) {
    ret = OB_ITER_END;
    LOG_DEBUG("[INDEX SKIP SCAN] memtable is end", K(ret));
  } else {
    bool need_skip = !is_skip_start_;
    while (OB_SUCC(ret) && !is_end_) {
      if (need_skip && !skip_scanner_->is_disabled() && OB_FAIL(skip_scanner_->skip(*this, is_end_, !is_skip_start_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to skip", K(ret));
        }
      } else if (is_end_) {
        ret = OB_ITER_END;
        LOG_DEBUG("[INDEX SKIP SCAN] reachs end", K(ret), K_(is_end), KPC_(skip_scanner));
      } else if (FALSE_IT(is_skip_start_ = true)) {
      } else if (OB_FAIL(get_next_skip_row(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to inner get next row", K(ret));
        } else if (skip_scanner_->is_disabled()) {
          is_end_ = true;
          LOG_DEBUG("[INDEX SKIP SCAN] reachs end and skip is disabled", K(ret), K_(is_end), KPC_(skip_scanner));
        } else {
          ret = OB_SUCCESS;
          need_skip = true;
          LOG_DEBUG("[INDEX SKIP SCAN] not end", K(ret), K_(is_end), K_(is_skip_start));
        }
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null row", K(ret), KP(row));
      } else if (row->row_flag_.is_exist()) {
        LOG_DEBUG("[INDEX SKIP SCAN] get next row", K(ret), K_(is_end), K_(is_skip_start), KPC(row));
        break;
      } else {
        need_skip = true;
        LOG_DEBUG("[INDEX SKIP SCAN] get not exist row", K(ret), K_(is_end), K_(is_skip_start), KPC(row));
      }
    }
  }
  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
