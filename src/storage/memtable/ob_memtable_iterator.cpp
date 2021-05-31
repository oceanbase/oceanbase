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
#include "storage/transaction/ob_trans_define.h"
#include "ob_memtable_context.h"
#include "ob_memtable.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace transaction;
using namespace share::schema;
namespace memtable {

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
{}

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
    const storage::ObTableIterParam& param, storage::ObTableAccessContext& context, ObIMemtable& memtable)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  } else {
    cur_row_.row_val_.projector_ = NULL;
    cur_row_.row_val_.projector_size_ = 0;

    param_ = &param;
    context_ = &context;
    memtable_ = &memtable;
    rowkey_ = NULL;
    is_inited_ = true;
  }
  return ret;
}

void ObMemtableGetIterator::set_rowkey(const common::ObExtStoreRowkey& rowkey)
{
  rowkey_ = &rowkey;
  rowkey_iter_ = 0;
}

int ObMemtableGetIterator::init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    storage::ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::schema::ObColDesc>* out_cols = nullptr;
  if (OB_ISNULL(table) || OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "table and query range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_FAIL(init(param, context, *static_cast<ObMemtable*>(table)))) {
    TRANS_LOG(WARN, "failed to init memtable get iter", K(ret));
  } else {
    set_rowkey(*static_cast<const ObExtStoreRowkey*>(query_range));
  }
  return ret;
}

int ObMemtableGetIterator::inner_get_next_row(const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(rowkey_iter_ > 0)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(memtable_->get(*param_, *context_, *rowkey_, cur_row_))) {
    TRANS_LOG(WARN, "memtable get fail", K(ret), "table_id", param_->table_id_, K(*rowkey_));
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
      memtable_(NULL),
      cur_range_(),
      columns_(NULL),
      cols_map_(NULL),
      row_iter_(),
      row_(),
      iter_flag_(0),
      last_row_has_null_(true),
      first_row_(true),
      key_has_null_(true)
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
    ObIMemtable* memtable, const storage::ObTableIterParam& param, storage::ObTableAccessContext& context)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }

  if (NULL == (row_.row_val_.cells_ = (ObObj*)context.allocator_->alloc(sizeof(ObObj) * param.out_cols_->count()))) {
    TRANS_LOG(WARN, "arena alloc cells fail", "size", sizeof(ObObj) * param.out_cols_->count());
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    TRANS_LOG(DEBUG, "scan iterator init succ", K(param.table_id_));
    if (NULL == param.cols_id_map_) {
      void* buf = NULL;
      ColumnMap* local_map = NULL;
      if (NULL == (buf = context.allocator_->alloc(sizeof(ColumnMap)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        local_map = new (buf) ColumnMap(*context.allocator_);
        if (OB_FAIL(local_map->init(*param.out_cols_))) {
          TRANS_LOG(WARN, "Fail to build column map, ", K(ret));
        } else {
          cols_map_ = local_map;
        }
      }
    } else {
      cols_map_ = param.cols_id_map_;
    }

    if (OB_SUCC(ret)) {
      param_ = &param;
      context_ = &context;
      memtable_ = memtable;
      columns_ = param.out_cols_;
      row_.row_val_.count_ = param.out_cols_->count();
      is_inited_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMemtableScanIterator::set_range(const common::ObExtStoreRange& ext_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    cur_range_ = ext_range;
    is_scan_start_ = false;
  }
  return ret;
}

int ObMemtableScanIterator::prepare_scan()
{
  int ret = OB_SUCCESS;
  ObStoreRange range;
  ObMemtableKey* start_key = NULL;
  ObMemtableKey* end_key = NULL;
  ObTransSnapInfo snapshot_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (is_scan_start_) {
    // pass
  } else if (OB_FAIL(context_->store_ctx_->get_snapshot_info(snapshot_info))) {
    TRANS_LOG(WARN, "get snapshot info failed", K(ret));
  } else if (OB_FAIL(get_real_range(cur_range_.get_range(), range))) {
    TRANS_LOG(WARN, "fail to get_real_range", K(ret), K(cur_range_));
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObMemtableKey::build(
                 start_key, param_->table_id_, *param_->out_cols_, &range.get_start_key(), *context_->allocator_))) {
    TRANS_LOG(WARN, "start key build fail", K(param_->table_id_), K(range));
  } else if (OB_FAIL(ObMemtableKey::build(
                 end_key, param_->table_id_, *param_->out_cols_, &range.get_end_key(), *context_->allocator_))) {
    TRANS_LOG(WARN, "end key build fail", K(param_->table_id_), K(range));
  } else {
    ObMvccEngine& mvcc_engine = ((ObMemtable*)memtable_)->get_mvcc_engine();
    ObMvccScanRange mvcc_scan_range;
    mvcc_scan_range.border_flag_ = range.get_border_flag();
    mvcc_scan_range.start_key_ = start_key;
    mvcc_scan_range.end_key_ = end_key;
    row_iter_.reset();
    if (OB_FAIL(mvcc_engine.scan(
            *context_->store_ctx_->mem_ctx_, snapshot_info, context_->query_flag_, mvcc_scan_range, row_iter_))) {
      TRANS_LOG(WARN, "mvcc engine scan fail", K(ret), K(mvcc_scan_range));
    } else {
      iter_flag_ = 0;
      last_row_has_null_ = true;
      key_has_null_ = true;
      first_row_ = true;
      is_scan_start_ = true;
      TRANS_LOG(DEBUG,
          "mvcc engine scan success",
          K_(memtable),
          K(mvcc_scan_range),
          K(snapshot_info),
          K(*(context_->store_ctx_)),
          K(*start_key),
          K(*end_key));
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    reset();
  }
  return ret;
}

int ObMemtableScanIterator::init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;
  const ObExtStoreRange* range = static_cast<const ObExtStoreRange*>(query_range);
  if (OB_ISNULL(table) || OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "table and query_range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_FAIL(init(static_cast<ObMemtable*>(table), param, context))) {
    TRANS_LOG(WARN, "init scan iterator fail", K(ret), K(range));
  } else if (OB_FAIL(set_range(*range))) {
    TRANS_LOG(WARN, "set scan range fail", K(ret), K(*range));
  }
  return ret;
}

int ObMemtableScanIterator::get_real_range(const ObStoreRange& range, ObStoreRange& real_range)
{
  int ret = OB_SUCCESS;
  bool is_reverse_scan = context_->query_flag_.is_reverse_scan();
  if (!range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid range", K(ret), K(range));
  } else {
    real_range = range;
    if (is_reverse_scan) {
      real_range.get_start_key() = range.get_end_key();
      real_range.get_end_key() = range.get_start_key();
      if (range.get_border_flag().inclusive_start()) {
        real_range.get_border_flag().set_inclusive_end();
      } else {
        real_range.get_border_flag().unset_inclusive_end();
      }
      if (range.get_border_flag().inclusive_end()) {
        real_range.get_border_flag().set_inclusive_start();
      } else {
        real_range.get_border_flag().unset_inclusive_start();
      }
    }
  }

  return ret;
}

int ObMemtableScanIterator::skip_range(
    int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key)
{
  int ret = OB_SUCCESS;
  UNUSED(range_idx);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else {
    cur_range_.change_boundary(*gap_key, context_->query_flag_.is_reverse_scan(), !include_gap_key);
    if (OB_FAIL(cur_range_.to_collation_free_range_on_demand_and_cutoff_range(*context_->allocator_))) {
      TRANS_LOG(WARN, "to_collation_free_range_on_demand fail", K(ret));
    } else {
      ret = set_range(cur_range_);
    }
  }
  return ret;
}

int ObMemtableScanIterator::inner_get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  ObIMemtableCtx* ctx = NULL;
  const ObMemtableKey* key = NULL;
  ObMvccValueIterator* value_iter = NULL;
  const bool skip_compact = false;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ctx = context_->store_ctx_->mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "context is null", K(ret), KP(ctx));
  } else if (OB_FAIL(ctx->get_trans_status())) {
    TRANS_LOG(WARN, "transaction status error", K(ret), "ctx", *ctx);
  } else if (OB_FAIL(prepare_scan())) {
    TRANS_LOG(WARN, "prepare scan fail", K(ret));
  } else if (OB_FAIL(row_iter_.get_next_row(key, value_iter, iter_flag_, skip_compact)) || NULL == key ||
             NULL == value_iter) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "row_iter_ get_next_row fail", K(ret), KP(key), KP(value_iter));
    }
    ret = (OB_SUCCESS == ret) ? OB_ERR_UNEXPECTED : ret;
  } else {
    TRANS_LOG(DEBUG, "get next row", K(*key));
    ObTransSnapInfo snapshot_info;
    const ObStoreRowkey* rowkey = NULL;
    int64_t row_scn = 0;
    key->get_rowkey(rowkey);
    if (first_row_) {
      first_row_ = false;
      bitmap_.init(columns_->count(), rowkey->get_obj_cnt());
    }
    if (last_row_has_null_) {
      last_row_has_null_ = false;
      const int64_t col_cnt = columns_->count();
      for (int64_t i = rowkey->get_obj_cnt(); i < col_cnt; ++i) {
        row_.row_val_.cells_[i].copy_meta_type(columns_->at(i).col_type_);
      }
    }
    if (key_has_null_) {
      key_has_null_ = false;
      for (int64_t i = 0; i < rowkey->get_obj_cnt(); ++i) {
        row_.row_val_.cells_[i].copy_meta_type(columns_->at(i).col_type_);
      }
    }
    bitmap_.reset();
    bool is_committed = false;
    if (OB_NOT_NULL(value_iter) && OB_NOT_NULL(value_iter->get_trans_node()) &&
        value_iter->get_trans_node()->is_committed()) {
      is_committed = true;
    }
    if (OB_FAIL(context_->store_ctx_->get_snapshot_info(snapshot_info))) {
      TRANS_LOG(WARN, "get snapshot info failed", K(ret));
    } else if (OB_FAIL(ObReadRow::iterate_row(*cols_map_,
                   *columns_,
                   *rowkey,
                   value_iter,
                   row_,
                   bitmap_,
                   last_row_has_null_,
                   key_has_null_,
                   row_scn))) {
      TRANS_LOG(WARN, "iterate_row fail", K(ret), KP(key), KP(value_iter));
    } else {
      if (param_->need_scn_) {
        const ObColDescIArray* out_cols = NULL;
        if (OB_FAIL(param_->get_out_cols(false /*is_get*/, out_cols))) {
          STORAGE_LOG(WARN, "failed to get out cols", K(ret));
        } else {
          for (int64_t i = 0; i < out_cols->count(); i++) {
            if (out_cols->at(i).col_id_ == OB_HIDDEN_TRANS_VERSION_COLUMN_ID) {
              row_.row_val_.cells_[i].set_int(row_scn);
              TRANS_LOG(DEBUG, "set row scn is", K(i), K(row_scn), K_(row));
            }
          }
        }
      }

      row_.scan_index_ = 0;
      row = &row_;
      if (context_->query_flag_.iter_uncommitted_row() && !is_committed) {  // set for mark deletion
        row_.flag_ = ObActionFlag::OP_ROW_EXIST;
      }
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
  memtable_ = NULL;
  cur_range_.reset();
  columns_ = NULL;
  cols_map_ = NULL;
  row_.row_val_.cells_ = NULL;
  row_.row_val_.count_ = 0;
  iter_flag_ = 0;
  last_row_has_null_ = true;
  key_has_null_ = true;
  first_row_ = true;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObMemtableMGetIterator::ObMemtableMGetIterator()
    : MAGIC_(VALID_MAGIC_NUM),
      is_inited_(false),
      param_(NULL),
      context_(NULL),
      memtable_(NULL),
      rowkeys_(NULL),
      cols_map_(NULL),
      rowkey_iter_(0),
      cur_row_()
{}

ObMemtableMGetIterator::~ObMemtableMGetIterator()
{
  reset();
}

int ObMemtableMGetIterator::init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    storage::ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::schema::ObColDesc>* out_cols = nullptr;
  if (is_inited_) {
    reset();
  }

  if (OB_ISNULL(table) || OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "table and query_range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_FAIL(param.get_out_cols(context.use_fuse_row_cache_, out_cols))) {
    TRANS_LOG(WARN, "fail to get out cols", K(ret));
  } else if (NULL ==
             (cur_row_.row_val_.cells_ = (ObObj*)context.allocator_->alloc(sizeof(ObObj) * out_cols->count()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    const ColumnMap* param_column_map = nullptr;
    if (OB_FAIL(param.get_column_map(context.use_fuse_row_cache_, param_column_map))) {
      TRANS_LOG(WARN, "fail to get column map", K(ret));
    } else if (NULL == param_column_map) {
      void* buf = NULL;
      ColumnMap* local_map = NULL;
      if (NULL == (buf = context.allocator_->alloc(sizeof(ColumnMap)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        local_map = new (buf) ColumnMap(*context.allocator_);
        if (OB_FAIL(local_map->init(*out_cols))) {
          TRANS_LOG(WARN, "Fail to build column map, ", K(ret));
        } else {
          cols_map_ = local_map;
        }
      }
    } else {
      cols_map_ = param_column_map;
    }

    if (OB_SUCC(ret)) {
      cur_row_.row_val_.count_ = out_cols->count();
      cur_row_.row_val_.projector_ = NULL;
      cur_row_.row_val_.projector_size_ = 0;

      param_ = &param;
      context_ = &context;
      memtable_ = static_cast<ObMemtable*>(table);
      rowkeys_ = static_cast<const ObIArray<ObExtStoreRowkey>*>(query_range);
      rowkey_iter_ = 0;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObMemtableMGetIterator::inner_get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else {
    if (rowkey_iter_ >= rowkeys_->count()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(memtable_->get(*param_, *context_, rowkeys_->at(rowkey_iter_), cur_row_))) {
      TRANS_LOG(WARN, "memtable get fail", K(ret), "table_id", param_->table_id_, "rowkey", rowkeys_->at(rowkey_iter_));
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
  param_ = NULL;
  context_ = NULL;
  memtable_ = NULL;
  rowkeys_ = NULL;
  rowkey_iter_ = 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObMemtableMScanIterator::ObMemtableMScanIterator() : ObMemtableScanIterator(), ranges_(NULL), cur_range_pos_(0)
{}

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

int ObMemtableMScanIterator::init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    storage::ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }
  if (OB_ISNULL(table) || OB_ISNULL(query_range)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "table and query_range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_FAIL(ObMemtableScanIterator::init(static_cast<ObMemtable*>(table), param, context))) {
    TRANS_LOG(WARN, "memtable scan iterator init fail", K(ret));
  } else {
    ranges_ = static_cast<const ObIArray<ObExtStoreRange>*>(query_range);
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

int ObMemtableMScanIterator::is_range_scan(bool& range_scan)
{
  int ret = OB_SUCCESS;
  if (cur_range_pos_ >= ranges_->count()) {
    ret = OB_ITER_END;
    range_scan = false;
  } else {
    range_scan = !ranges_->at(cur_range_pos_).get_range().is_single_rowkey();
  }

  return ret;
}

int ObMemtableMScanIterator::skip_range(
    int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key)
{
  int ret = OB_SUCCESS;
  ObExtStoreRange* new_range = NULL;
  bool range_scan = false;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else {
    bool reverse = context_->query_flag_.is_reverse_scan();
    cur_range_pos_ = range_idx;
    if (OB_FAIL(is_range_scan(range_scan))) {
      TRANS_LOG(WARN, "is_range_scan fail", KP(this));
    } else {
      if (range_scan) {
        if (NULL == (new_range = (ObExtStoreRange*)context_->allocator_->alloc(sizeof(*new_range)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          bool need_skip_to_next_range = false;
          *new_range = ranges_->at(cur_range_pos_);
          new_range->change_boundary(*gap_key, reverse, !include_gap_key);
          if (OB_FAIL(new_range->to_collation_free_range_on_demand_and_cutoff_range(*context_->allocator_))) {
            TRANS_LOG(WARN, "to_collation_free_range_on_demand_and_cutoff_range fail", K(ret));
          } else if (OB_FAIL(ObMemtableScanIterator::set_range(*new_range))) {
            TRANS_LOG(WARN, "set_range fail", K(ret));
          } else if (OB_FAIL(ObMemtableScanIterator::prepare_scan())) {
            if (OB_ITER_END == ret) {
              need_skip_to_next_range = true;
              ret = OB_SUCCESS;
            } else {
              TRANS_LOG(WARN, "prepare_scan fail", K(ret));
            }
          }

          if (OB_SUCC(ret) && need_skip_to_next_range) {
            if (OB_FAIL(next_range())) {
              STORAGE_LOG(WARN, "fail to next range", K(ret));
            } else if (cur_range_pos_ >= ranges_->count()) {
              // do nothing
            } else if (OB_FAIL(ObMemtableScanIterator::set_range(ranges_->at(cur_range_pos_)))) {
              TRANS_LOG(WARN, "fail to set range", K(ret), K(cur_range_pos_));
            } else if (OB_FAIL(ObMemtableScanIterator::prepare_scan())) {
              STORAGE_LOG(WARN, "fail to prepare scan", K(ret));
            }
          }
        }
      } else {
        // skip to get range, triggered by array binding limit
        // gap key must equal to range start key
        const ObStoreRowkey& start_key = ranges_->at(cur_range_pos_).get_range().get_start_key();
        const bool is_equal = start_key.simple_equal(*gap_key);
        if (!is_equal) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid arguments", K(ret), K(start_key), K(*gap_key));
        }
      }
    }
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

int ObMemtableMScanIterator::get_next_row_for_get(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMemtableMScanIterator has not been inited", K(ret));
  } else if (cur_range_pos_ >= ranges_->count()) {
    ret = OB_ITER_END;  // should not happen
  } else {
    const ObExtStoreRange& range = ranges_->at(cur_range_pos_);
    if (OB_FAIL(memtable_->get(*param_, *context_, range.get_ext_start_key(), row_))) {
      TRANS_LOG(WARN, "fail to get memtable row", K(ret), "table_id", param_->table_id_, "range", range);
    } else {
      row_.is_get_ = true;
      row_.scan_index_ = cur_range_pos_;
      row = &row_;
      TRANS_LOG(DEBUG, "get_next_row_for_get row val", K(row_.row_val_), K(row_.scan_index_), K(row_.is_get_));
    }
  }

  return ret;
}

int ObMemtableMScanIterator::inner_get_next_row_for_scan(const ObStoreRow*& row)
{
  return ObMemtableScanIterator::inner_get_next_row(row);
}

int ObMemtableMScanIterator::get_next_row_for_scan(const ObStoreRow*& row)
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
    const_cast<ObStoreRow*>(row)->is_get_ = false;
    const_cast<ObStoreRow*>(row)->scan_index_ = cur_range_pos_;
    TRANS_LOG(
        DEBUG, "get_next_row_for_scan row val", K(this), K(row->row_val_), K(row->flag_), "scan_index", cur_range_pos_);
  }

  return ret;
}

int ObMemtableMScanIterator::inner_get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  ObIMemtableCtx* ctx = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMemtableMScanIterator has not been inited", K(ret));
  } else if (OB_ISNULL(ctx = context_->store_ctx_->mem_ctx_)) {
    TRANS_LOG(WARN, "context is null", KP(ctx));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx->get_trans_status())) {
    TRANS_LOG(WARN, "transaction status error", K(ret), "ctx", *ctx);
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
              ret = next_range();  // rewrite error code
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
      columns_(),
      cols_map_(NULL),
      start_key_(NULL),
      end_key_(NULL),
      context_(NULL),
      row_iter_(),
      row_(),
      key_(NULL),
      value_iter_(NULL),
      key_has_null_(true),
      bitmap_(),
      rowkey_count_(0),
      scan_state_(SCAN_END),
      trans_version_col_idx_(-1),
      sql_sequence_col_idx_(-1),
      iter_mode_(ObTableIterParam::OIM_ITER_FULL),
      row_checker_()
{
  GARL_ADD(&active_resource_, "scan_iter");
}

ObMemtableMultiVersionScanIterator::~ObMemtableMultiVersionScanIterator()
{
  GARL_DEL(&active_resource_);
  reset();
}

int ObMemtableMultiVersionScanIterator::init(const storage::ObTableIterParam& param,
    storage::ObTableAccessContext& context, storage::ObITable* table, const void* query_range)
{
  int ret = OB_SUCCESS;
  ObTransSnapInfo snapshot_info;
  if (is_inited_) {
    reset();
  }

  const ObStoreRange* range = static_cast<const ObStoreRange*>(query_range);
  ObMemtable* memtable = static_cast<ObMemtable*>(table);
  if (OB_ISNULL(table) || OB_ISNULL(query_range) || !context.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "table and query range can not be null", KP(table), KP(query_range), K(ret));
  } else if (OB_FAIL(context.store_ctx_->get_snapshot_info(snapshot_info))) {
    TRANS_LOG(WARN, "get snapshot info failed", K(ret));
  } else if (OB_FAIL(ObMultiVersionRowkeyHelpper::convert_multi_version_iter_param(param,
                 false /*is get*/,
                 columns_,
                 ObMultiVersionRowkeyHelpper::get_multi_version_rowkey_cnt(
                     ObMultiVersionRowkeyHelpper::MVRC_VERSION_AFTER_3_0)))) {
    TRANS_LOG(WARN, "fail to convert multi version columns", K(ret));
  } else if (OB_FAIL(ObMemtableKey::build_without_hash(
                 start_key_, param.table_id_, columns_, &range->get_start_key(), *context.allocator_))) {
    TRANS_LOG(WARN, "start key build fail", K(param.table_id_), K(range->get_start_key()));
  } else if (OB_FAIL(ObMemtableKey::build_without_hash(
                 end_key_, param.table_id_, columns_, &range->get_end_key(), *context.allocator_))) {
    TRANS_LOG(WARN, "end key build fail", K(param.table_id_), K(range->get_end_key()));
  } else {
    TRANS_LOG(DEBUG, "init multi version scan iterator", K(param), K(*range));
    ObMvccScanRange mvcc_scan_range;
    mvcc_scan_range.border_flag_ = range->get_border_flag();
    mvcc_scan_range.start_key_ = start_key_;
    mvcc_scan_range.end_key_ = end_key_;
    if (OB_FAIL(memtable->get_mvcc_engine().scan(*context.store_ctx_->mem_ctx_,
            snapshot_info,
            mvcc_scan_range,
            *context.store_ctx_->trans_table_guard_,
            row_iter_))) {
      TRANS_LOG(WARN, "mvcc engine scan fail", K(ret), K(mvcc_scan_range));
    } else if (OB_FAIL(init_row_cells(context.allocator_))) {
      TRANS_LOG(WARN, "arena alloc cells fail", "size", sizeof(ObObj) * columns_.count());
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      TRANS_LOG(DEBUG, "multi version scan iterator init succ", K(param.table_id_), K(range));
      if (NULL == param.cols_id_map_) {
        void* buf = NULL;
        ColumnMap* local_map = NULL;
        if (NULL == (buf = context.allocator_->alloc(sizeof(ColumnMap)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TRANS_LOG(WARN, "Fail to allocate memory, ", K(ret));
        } else {
          local_map = new (buf) ColumnMap(*context.allocator_);
          if (OB_FAIL(local_map->init(columns_))) {
            TRANS_LOG(WARN, "Fail to build column map, ", K(ret));
          } else {
            cols_map_ = local_map;
          }
        }
      } else {
        cols_map_ = param.cols_id_map_;
      }

      if (OB_SUCC(ret)) {
        int32_t col_pos = -1;
        if (OB_FAIL(cols_map_->get(OB_HIDDEN_TRANS_VERSION_COLUMN_ID, col_pos))) {
          TRANS_LOG(WARN, "failed to get hidden trans version column idx", K(ret));
        } else if (FALSE_IT(trans_version_col_idx_ = col_pos)) {
        } else if (OB_FAIL(cols_map_->get(OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID, col_pos))) {
          TRANS_LOG(WARN, "failed to get OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID", K(ret));
        } else {
          sql_sequence_col_idx_ = col_pos;
          context_ = &context;
          iter_mode_ = param.iter_mode_;
          rowkey_count_ = param.rowkey_cnt_ + ObMultiVersionRowkeyHelpper::get_multi_version_rowkey_cnt(
                                                  ObMultiVersionRowkeyHelpper::MVRC_VERSION_AFTER_3_0);
          bitmap_.init(columns_.count(), rowkey_count_);
          if (OB_FAIL(iter_row_reader_.init(context.allocator_, cols_map_, &bitmap_, columns_))) {
            TRANS_LOG(WARN, "failed to init MemtableIterRowReader", K(ret));
          } else {
            is_inited_ = true;
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::init_row_cells(ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "allocator is null", K(ret), K(allocator));
  } else if (NULL == (row_.row_val_.cells_ = (ObObj*)allocator->alloc(sizeof(ObObj) * columns_.count()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc cells fail", K(ret), "size", sizeof(ObObj) * columns_.count());
  } else {
    row_.row_val_.count_ = columns_.count();
  }
  return ret;
}

// The row format is as follows:
// rowkey | opposite number of trans_version | opposite number of Sql_seuqnce | non-rowkey columns
//
// example:
// suppose one row has 5 trans nodes with version 1, 2, 3, 4 and 5 respectively,
// and an uncommited trans node with version MAX, then the iterating order would be:
// |MAX | -20(Sql Sequence)
// |MAX | -8(Sql Sequence)
// --result after all trans_node compaction (5 versions)
// |---version 4
// |----version 3
// |-----version 2
// |------version 1
int ObMemtableMultiVersionScanIterator::inner_get_next_row(const ObStoreRow*& row)
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
    if (OB_FAIL(row_checker_.check_row_flag_valid(*row))) {
      TRANS_LOG(ERROR, "row flag is invalid", K(ret), KPC(this));
      if (OB_NOT_NULL(value_iter_)) {
        value_iter_->print_cur_status();
      }
    } else {
      TRANS_LOG(TRACE, "after inner get next row", K(rowkey_count_), K(*row), K(iter_mode_), K(scan_state_));
    }
  }
  return ret;
}

ObMemtableMultiVersionScanIterator::ObOuputRowValidateChecker::ObOuputRowValidateChecker()
    : output_compact_row_flag_(false), output_last_row_flag_(false)
{}

ObMemtableMultiVersionScanIterator::ObOuputRowValidateChecker::~ObOuputRowValidateChecker()
{
  reset();
}

void ObMemtableMultiVersionScanIterator::ObOuputRowValidateChecker::reset()
{
  output_compact_row_flag_ = false;
  output_last_row_flag_ = false;
}

int ObMemtableMultiVersionScanIterator::ObOuputRowValidateChecker::check_row_flag_valid(const storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (row.row_type_flag_.is_last_multi_version_row()) {
    if (output_last_row_flag_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "have output row with last flag before", K(ret));
    } else {
      output_last_row_flag_ = true;
    }
  } else if (row.row_type_flag_.is_compacted_multi_version_row()) {
    // Compact row without last flag
    if (output_compact_row_flag_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "second output row must be last row", K(ret));
    } else {
      output_compact_row_flag_ = true;
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
    } else if (storage::ObTableIterParam::OIM_ITER_OVERFLOW_TO_COMPLEMENT == iter_mode_ &&
               !value_iter_->contain_overflow_trans_node()) {
      continue;
    } else {
      break;
    }
  }
  if (OB_SUCC(ret) && storage::ObTableIterParam::OIM_ITER_NON_OVERFLOW_TO_MINI == iter_mode_) {
    value_iter_->jump_overflow_trans_node();
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
    if (key_has_null_) {
      for (int64_t i = 0; i < rowkey_count_; ++i) {
        row_.row_val_.cells_[i].copy_meta_type(columns_.at(i).col_type_);
      }
      key_has_null_ = false;
    }
    row_.set_first_dml(value_iter_->get_first_dml());
    value_iter_->set_merge_log_ts(context_->merge_log_ts_);

    // if the iterator is the last one, the overflow node would be outputed to complement,
    // other wise it would be outputed to mini sstable
    value_iter_->set_iter_mode(iter_mode_);
    row_checker_.reset();
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::set_compacted_row_state()
{
  int ret = OB_SUCCESS;
  // set the opposite number of trans_version as one column of the rowkey,
  // so as to distinguish multiversion rows, as well as ensuring
  // the most recent result of compaction is always in the front
  row_.row_val_.cells_[trans_version_col_idx_].set_int(-value_iter_->get_committed_max_trans_version());
  // sql_sequence of committed data is 0
  row_.row_val_.cells_[sql_sequence_col_idx_].set_int(0);
  row_.row_type_flag_.set_compacted_multi_version_row(true);
  row_.row_type_flag_.set_first_multi_version_row(true);
  row_.set_dml(value_iter_->get_compacted_row_dml());
  row_.row_type_flag_.set_last_multi_version_row(value_iter_->is_multi_version_iter_end());
  return ret;
}

int ObMemtableMultiVersionScanIterator::get_compacted_multi_version_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KPC(this));
  } else {
    const ObStoreRowkey* rowkey = NULL;
    key_->get_rowkey(rowkey);
    if (OB_FAIL(iterate_compacted_row(*rowkey, row_))) {
      TRANS_LOG(WARN, "iterate_row fail", K(ret), KP(key_), KP(value_iter_));
    } else if (OB_FAIL(set_compacted_row_state())) {  // set state for compacted row
      TRANS_LOG(WARN, "failed to set state for compated row", K(row_));
    } else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != row_.flag_) {
      row = &row_;
    }
    if (NULL != row && OB_SUCC(ret)) {
      TRANS_LOG(DEBUG, "get_compacted_multi_version_row", K(ret), K(*rowkey), K(*row));
    }
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::get_multi_version_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  row = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KPC(this));
  } else {
    const ObStoreRowkey* rowkey = NULL;
    key_->get_rowkey(rowkey);
    if (OB_FAIL(iterate_multi_version_row(*rowkey, row_))) {
      TRANS_LOG(WARN, "iterate_row fail", K(ret), KP(key_), KP(value_iter_));
    } else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != row_.flag_) {
      row_.row_type_flag_.set_compacted_multi_version_row(value_iter_->is_node_compacted());
      row_.row_type_flag_.set_last_multi_version_row(value_iter_->is_multi_version_iter_end());
      row = &row_;
    }
    if (NULL != row && OB_SUCC(ret)) {
      TRANS_LOG(DEBUG, "get_multi_version_row", K(ret), K(*rowkey), K(*row));
    }
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::get_uncommitted_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KPC(this));
  } else {
    const ObStoreRowkey* rowkey = key_->get_rowkey();
    if (OB_FAIL(iterate_uncommitted_row(*rowkey, row_))) {
      TRANS_LOG(WARN, "failed to iterate_uncommitted_row", K(ret));
      // transaction may have been commited right before our accessing,
      // so iterate uncommitted row may not read any data,
      // and flag OP_ROW_DOEST_NOT_EXIST stands for such situation
    } else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != row_.flag_) {
      row_.row_type_flag_.set_last_multi_version_row(value_iter_->is_compact_iter_end());
      row_.row_type_flag_.set_uncommitted_row(true);
      row = &row_;
      TRANS_LOG(TRACE, "get_uncommitted_row", K(*rowkey), K(*row));
    }
  }
  return ret;
}

void ObMemtableMultiVersionScanIterator::switch_to_committed_scan_state()
{
  if (!value_iter_->is_compact_iter_end()) {  // iter compacted not finish
    scan_state_ = SCAN_COMPACT_ROW;
    value_iter_->init_multi_version_iter();
  } else {
    scan_state_ = SCAN_END;
  }
}

int ObMemtableMultiVersionScanIterator::switch_scan_state()
{
  int ret = OB_SUCCESS;
  switch (scan_state_) {
    case SCAN_UNCOMMITTED_ROW:
      if (value_iter_->is_trans_node_iter_null() || ObActionFlag::OP_ROW_DOES_NOT_EXIST == row_.flag_) {
        switch_to_committed_scan_state();
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
        switch_to_committed_scan_state();
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
  start_key_ = NULL;
  end_key_ = NULL;
  row_.row_val_.cells_ = NULL;
  row_.row_val_.count_ = 0;
  key_has_null_ = true;
  bitmap_.reset();
  rowkey_count_ = 0;
  scan_state_ = SCAN_END;
  trans_id_.reset();
  trans_version_col_idx_ = -1;
  sql_sequence_col_idx_ = -1;
  iter_row_reader_.destory();
  iter_mode_ = ObTableIterParam::OIM_ITER_FULL;
  row_checker_.reset();
}

void ObMemtableMultiVersionScanIterator::row_state_reset()
{
  row_.flag_ = -1;
  row_.dml_ = T_DML_UNKNOWN;
  row_.is_get_ = false;
  row_.from_base_ = false;
  row_.row_pos_flag_.reset();
  row_.scan_index_ = 0;
  row_.row_type_flag_.reset();
  if (SCAN_UNCOMMITTED_ROW == scan_state_) {
    row_.trans_id_ptr_ = &trans_id_;
  } else {
    row_.trans_id_ptr_ = nullptr;
  }
  iter_row_reader_.reset();
}

void ObMemtableMultiVersionScanIterator::row_reset()
{
  row_state_reset();
}

OB_INLINE int ObMemtableMultiVersionScanIterator::iterate_multi_version_row(
    const ObStoreRowkey& rowkey, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReadRow::iterate_row_key(key_has_null_, rowkey, row))) {
    TRANS_LOG(WARN, "iterate_row_key fail", K(ret), K(rowkey));
  } else if (OB_FAIL(iterate_multi_version_row_value_(row))) {
    TRANS_LOG(WARN, "iterate_multi_version_row_value_ fail", K(ret));
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::iterate_compacted_row(const ObStoreRowkey& key, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value_iter_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "iterate row get invalid argument", K(ret), KP(value_iter_));
  } else if (OB_FAIL(ObReadRow::iterate_row_key(key_has_null_, key, row))) {
    TRANS_LOG(WARN, "iterate_row_key fail", K(ret), K(key));
  } else if (OB_FAIL(iterate_compacted_row_value_(row))) {
    TRANS_LOG(WARN, "iterate_row_value fail", K(ret), K(key), KP(value_iter_));
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::iterate_uncommitted_row(const ObStoreRowkey& key, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReadRow::iterate_row_key(key_has_null_, key, row))) {
    TRANS_LOG(WARN, "failed to iterate rowkey", K(ret), K(key));
  } else if (OB_FAIL(iterate_uncommitted_row_value_(row))) {
    TRANS_LOG(WARN, "failed to iterate_uncommitted_row_value_", K(ret), K(key));
  }
  return ret;
}

void ObMemtableMultiVersionScanIterator::set_flag_and_version_for_compacted_row(
    const ObMvccTransNode* tnode, storage::ObStoreRow& row)
{
  const bool is_committed = reinterpret_cast<const ObMvccTransNode*>(tnode)->is_committed();
  const int64_t trans_version =
      is_committed ? reinterpret_cast<const ObMvccTransNode*>(tnode)->trans_version_ : INT64_MAX;
  row.flag_ = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == row.flag_) ? ObActionFlag::OP_ROW_EXIST : row.flag_;
  row.snapshot_version_ = std::max(trans_version, row.snapshot_version_);
  STORAGE_LOG(DEBUG, "row snapshot version", K(row.snapshot_version_));
}

int ObMemtableMultiVersionScanIterator::iterate_uncommitted_row_value_(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  const void* tnode = NULL;
  const ObMemtableDataHeader* mtd = NULL;
  int64_t sql_seq = -1;
  int64_t first_sql_sequence = -1;
  int64_t trans_version = INT64_MAX;
  bool same_sql_sequence_flag = true;
  if (OB_ISNULL(value_iter_) || OB_ISNULL(row.trans_id_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(value_iter_), KP(row.trans_id_ptr_));
  } else {
    while (OB_SUCC(ret)) {
      if (first_sql_sequence > -1 && OB_FAIL(value_iter_->check_next_sql_sequence(
                                         *row.trans_id_ptr_, first_sql_sequence, same_sql_sequence_flag))) {
        TRANS_LOG(WARN, "failed to check next sql sequence", K(ret), K(tnode));
      } else if (!same_sql_sequence_flag) {  // different sql sequence need break
        break;
      } else if (OB_FAIL(value_iter_->get_next_uncommitted_node(tnode, *row.trans_id_ptr_, trans_version, sql_seq))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "failed to get next uncommitted node", K(ret), K(tnode));
        }
      } else {
        const ObMvccTransNode* trans_node = reinterpret_cast<const ObMvccTransNode*>(tnode);
        if (OB_ISNULL(trans_node)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "trans node is null", K(ret), KP(trans_node));
        } else if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader*>(trans_node->buf_))) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "trans node value is null", K(ret), KP(trans_node), KP(mtd));
        } else {
          row.flag_ = ObActionFlag::OP_ROW_EXIST;
          if (OB_FAIL(iter_row_reader_.set_buf(mtd->buf_, mtd->buf_len_))) {
            TRANS_LOG(WARN, "cci init fail", K(ret), KP(mtd));
          } else if (OB_FAIL(iter_row_reader_.get_memtable_row(row))) {
            TRANS_LOG(WARN, "iterate_trans_node_ fail", K(ret), KP(mtd));
          } else if (-1 == first_sql_sequence) {  // record sql sequence
            first_sql_sequence = sql_seq;
            row.row_val_.cells_[trans_version_col_idx_].set_int(-trans_version);
            row.row_val_.cells_[sql_sequence_col_idx_].set_int(-sql_seq);
            row.dml_ = mtd->dml_type_;
          }
        }
      }
    }  // end of while
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  if (OB_SUCC(ret) && ObActionFlag::OP_ROW_DOES_NOT_EXIST != row.flag_) {
    if (OB_FAIL(iter_row_reader_.set_nop_pos(row))) {
      TRANS_LOG(WARN, "failed to set nop pos for row", K(ret), K(row));
    }
  }

  return ret;
}

int ObMemtableMultiVersionScanIterator::iterate_compacted_row_value_(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  const void* tnode = NULL;
  const ObMemtableDataHeader* mtd = NULL;
  row.snapshot_version_ = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(value_iter_->get_next_node_for_compact(tnode))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "failed to get next node", K(ret), K(*this), K(value_iter_));
      }
    } else if (OB_ISNULL(tnode)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
    } else if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader*>(
                             reinterpret_cast<const ObMvccTransNode*>(tnode)->buf_))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "transa node value is null", K(ret), KP(tnode), KP(mtd));
    } else {
      set_flag_and_version_for_compacted_row(reinterpret_cast<const ObMvccTransNode*>(tnode), row);

      // FIXME if the real length of buffer can be obtained, passing it's real length
      if (OB_FAIL(iter_row_reader_.set_buf(mtd->buf_, mtd->buf_len_))) {
        TRANS_LOG(WARN, "cci init fail", K(ret), KP(mtd));
      } else if (OB_FAIL(iter_row_reader_.get_memtable_row(row))) {
        TRANS_LOG(WARN, "iterate_trans_node_ fail", K(ret), KP(mtd));
      }
      if (ObRowDml::T_DML_INSERT == mtd->dml_type_ || iter_row_reader_.is_iter_end()) {
        break;
      }
    }
  }  // while
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  if (OB_SUCC(ret) && OB_FAIL(iter_row_reader_.set_nop_pos(row))) {
    TRANS_LOG(WARN, "failed to set nop pos for row", K(ret), K(row));
  }
  return ret;
}

int ObMemtableMultiVersionScanIterator::iterate_multi_version_row_value_(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  const void* tnode = NULL;
  const void* data = NULL;
  int64_t trans_version = INT64_MIN;
  int64_t compare_trans_version = INT64_MAX;
  const ObVersionRange version_range = context_->store_ctx_->mem_ctx_->get_multi_version_range();
  const ObMemtableDataHeader* mtd = NULL;
  if (OB_ISNULL(value_iter_) || OB_ISNULL(cols_map_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "value iter or colsMap is null", K(ret), K(value_iter_), K(cols_map_));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(value_iter_->get_next_multi_version_node(tnode))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "failed to get next node", K(ret), K(*this), K(value_iter_));
      }
    } else if (OB_ISNULL(tnode)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
    } else if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader*>(
                             reinterpret_cast<const ObMvccTransNode*>(tnode)->buf_))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node value is null", K(ret), KP(tnode), KP(mtd));
    } else {
      row.flag_ = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == row.flag_) ? ObActionFlag::OP_ROW_EXIST : row.flag_;
      compare_trans_version = reinterpret_cast<const ObMvccTransNode*>(tnode)->trans_version_;
      if (compare_trans_version <= version_range.base_version_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(
            WARN, "trans version smaller than base version", K(compare_trans_version), K(version_range.base_version_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(iter_row_reader_.set_buf(mtd->buf_, mtd->buf_len_))) {
      // FIXME if the real length of buffer can be obtained, passing it's real length
      TRANS_LOG(WARN, "cci init fail", K(ret), KP(data));
    } else if (OB_FAIL(iter_row_reader_.get_memtable_row(row))) {
      TRANS_LOG(WARN, "iterate_trans_node_ fail", K(ret), KP(data));
    } else {
      if (compare_trans_version > trans_version) {
        trans_version = compare_trans_version;
        row.row_val_.cells_[trans_version_col_idx_].set_int(-trans_version);
        row.row_val_.cells_[sql_sequence_col_idx_].set_int(0);
        row.dml_ = mtd->dml_type_;
      }
      if (ObRowDml::T_DML_LOCK == row.dml_) {
        row.dml_ = mtd->dml_type_;
      }
      if (trans_version > version_range.multi_version_start_ && value_iter_->is_cur_multi_version_row_end()) {
        break;
      }
    }
  }  // while
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  if (OB_SUCC(ret) && ObActionFlag::OP_ROW_DOES_NOT_EXIST != row.flag_) {
    if (OB_FAIL(iter_row_reader_.set_nop_pos(row))) {
      TRANS_LOG(WARN, "failed to set nop pos for row", K(ret), K(row));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

inline int ObReadRow::column_cast_(const ObObjMeta& column_meta, ObObj& value)
{
  int ret = OB_SUCCESS;
  if (value.is_null() || ObExtendType == value.get_type()) {
    // value's type is extend, need not cast
    //} else if (!ob_can_static_cast(value.get_type(), column_meta.get_type())) {
    //  TRANS_LOG(WARN, "static cast not compatible",
    //      "src_type", value.get_type(),
    //      "dst_type", column_meta.get_type());
    //  ret = OB_ERR_UNEXPECTED;
  } else {
    value.set_type(column_meta.get_type());
    value.set_collation_type(column_meta.get_collation_type());
  }
  return ret;
}

OB_INLINE int ObReadRow::iterate_row_key(bool& key_has_null, const ObStoreRowkey& rowkey, ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  const ObObj* obj_ptr = rowkey.get_obj_ptr();
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
    obj_ptr[i].copy_value_to(row.row_val_.cells_[i], key_has_null);
  }
  return ret;
}

OB_INLINE int ObReadRow::iterate_row_value_(const share::schema::ColumnMap& column_index,
    const storage::ObColDescIArray& columns, ObIMvccValueIterator* value_iter, ObStoreRow& row, ObNopBitMap& bitmap,
    bool& has_null, int64_t& row_scn)
{
  int ret = OB_SUCCESS;
  row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  int64_t filled_column_count = 0;
  bool row_empty = true;
  const void* tnode = NULL;
  const ObMemtableDataHeader* mtd = NULL;
  row_scn = 0;
  ObMemtableRowReader reader;
  row.snapshot_version_ = 0;
  while (OB_SUCC(ret) && OB_SUCC(value_iter->get_next_node(tnode))) {
    if (OB_ISNULL(tnode)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
    } else if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader*>(
                             reinterpret_cast<const ObMvccTransNode*>(tnode)->buf_))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "transa node value is null", K(ret), KP(tnode), KP(mtd));
    } else {
      const bool is_committed = reinterpret_cast<const ObMvccTransNode*>(tnode)->is_committed();
      const int64_t trans_version =
          is_committed ? reinterpret_cast<const ObMvccTransNode*>(tnode)->trans_version_ : INT64_MAX;
      row.flag_ = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == row.flag_) ? ObActionFlag::OP_ROW_EXIST : row.flag_;
      row.snapshot_version_ = std::max(trans_version, row.snapshot_version_);
      STORAGE_LOG(DEBUG, "row snapshot version", K(row.snapshot_version_));
      // FIXME if the real length of buffer can be obtained, passing it's real length
      if (OB_FAIL(reader.set_buf(mtd->buf_, mtd->buf_len_))) {
        TRANS_LOG(WARN, "cci init fail", K(ret), KP(mtd));
      } else if (OB_FAIL(reader.get_memtable_row(
                     row_empty, column_index, columns, row, bitmap, filled_column_count, has_null))) {
        TRANS_LOG(WARN, "iterate_trans_node_ fail", K(ret), KP(mtd));
      } else if (0 == row_scn) {
        row_scn = reinterpret_cast<const ObMvccTransNode*>(tnode)->trans_version_;
      }
      if (ObRowDml::T_DML_INSERT == mtd->dml_type_ || filled_column_count >= row.row_val_.count_) {
        break;
      }
    }
  }  // while

  if (!bitmap.is_empty()) {
    has_null = true;
    bitmap.set_nop_obj(row.row_val_.cells_);
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  return ret;
}

int ObReadRow::exist(ObMvccValueIterator& value_iter, bool& is_exist, bool& has_found)
{
  int ret = OB_SUCCESS;
  ObCellReader cci;
  const void* tnode = NULL;
  const ObMemtableDataHeader* mtd = NULL;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj* value = NULL;
  is_exist = false;
  has_found = false;

  while (OB_SUCC(ret) && !has_found) {
    if (OB_FAIL(value_iter.get_next_node(tnode))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        TRANS_LOG(WARN, "Fail to get next trans node", K(ret));
      }
    } else if (OB_ISNULL(tnode)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
    } else if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader*>(
                             reinterpret_cast<const ObMvccTransNode*>(tnode)->buf_))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "transa node value is null", K(ret), KP(tnode), KP(mtd));
    } else if (OB_FAIL(cci.init(mtd->buf_, mtd->buf_len_, SPARSE))) {
      TRANS_LOG(WARN, "cci init fail", K(ret), KP(mtd));
    } else if (OB_FAIL(cci.next_cell())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        TRANS_LOG(WARN, "Fail to get next cell, ", K(ret));
      }
    } else if (OB_FAIL(cci.get_cell(column_id, value)) || NULL == value) {
      TRANS_LOG(WARN, "compact cell iterator get_cell fail", K(ret), K(column_id), "value_ptr", value);
      ret = (OB_SUCCESS == ret) ? OB_ERR_UNEXPECTED : ret;
    } else if (ObExtendType == value->get_type() && ObActionFlag::OP_END_FLAG == value->get_ext()) {
      is_exist = true;
      has_found = true;
    } else if (ObExtendType == value->get_type() && !value->is_min_value() && !value->is_max_value()) {
      if (ObActionFlag::OP_DEL_ROW != value->get_ext()) {
        TRANS_LOG(WARN, "invalid extend value", "column_id", column_id, "value", *value);
        ret = OB_ERR_UNEXPECTED;
      } else {
        // tranversing the list in reverse order, it's unnecessary
        // to continue looking when found a del flag
        is_exist = false;
        has_found = true;
      }
    } else {
      is_exist = true;
      has_found = true;
    }
  }
  return ret;
}

int ObReadRow::iterate_row(const share::schema::ColumnMap& column_index, const storage::ObColDescIArray& columns,
    const ObStoreRowkey& key, ObIMvccValueIterator* value_iter, storage::ObStoreRow& row, ObNopBitMap& bitmap,
    bool& has_null, bool& key_has_null, int64_t& row_scn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value_iter)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "iterate row get invalid argument", K(ret), KP(value_iter));
  } else if (OB_FAIL(iterate_row_key(key_has_null, key, row))) {
    TRANS_LOG(WARN, "iterate_row_key fail", K(ret), K(key));
  } else if (OB_FAIL(iterate_row_value_(column_index, columns, value_iter, row, bitmap, has_null, row_scn))) {
    TRANS_LOG(WARN, "iterate_row_value fail", K(ret), K(key));
  } else {
    // do nothing
  }
  return ret;
}

int ObReadRow::get_row_header(ObMvccValueIterator& value_iter, uint32_t& modify_count, uint32_t& acc_checksum)
{
  int ret = OB_SUCCESS;
  const void* tnode = NULL;

  if (OB_FAIL(value_iter.get_next_node(tnode))) {
    if (OB_ITER_END == ret) {
      // rewrite ret
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      TRANS_LOG(WARN, "Fail to get next trans node", K(ret));
    }
  } else if (NULL == tnode) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
  } else {
    modify_count = (reinterpret_cast<const ObMvccTransNode*>(tnode))->modify_count_;
    acc_checksum = (reinterpret_cast<const ObMvccTransNode*>(tnode))->acc_checksum_;
  }

  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
