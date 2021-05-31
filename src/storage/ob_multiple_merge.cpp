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

#include "lib/stat/ob_diagnose_info.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/object/ob_obj_compare.h"
#include "storage/ob_multiple_merge.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/ob_store_row_filter.h"
#include "storage/ob_partition_store.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_query_iterator_util.h"
#include "share/ob_worker.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_operator.h"

namespace oceanbase {
using namespace common;
namespace storage {

ObMultipleMerge::ObMultipleMerge()
    : padding_allocator_(ObModIds::OB_SSTABLE_GET_SCAN),
      iters_(),
      access_param_(NULL),
      access_ctx_(NULL),
      tables_handle_(),
      cur_row_(),
      filter_row_(),
      unprojected_row_(),
      next_row_(NULL),
      out_cols_projector_(NULL),
      curr_scan_index_(0),
      curr_rowkey_(),
      nop_pos_(),
      row_stat_(),
      scan_cnt_(0),
      filt_cnt_(0),
      need_padding_(false),
      need_fill_default_(false),
      need_fill_virtual_columns_(false),
      need_output_row_with_nop_(false),
      row_filter_(NULL),
      inited_(false),
      range_idx_delta_(0),
      get_table_param_(),
      relocate_cnt_(0),
      table_stat_(),
      skip_refresh_table_(false),
      read_memtable_only_(false)
{}

ObMultipleMerge::~ObMultipleMerge()
{
  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (OB_NOT_NULL(iters_.at(i))) {
      iters_.at(i)->~ObStoreRowIterator();
    }
  }
}

int ObMultipleMerge::init(
    const ObTableAccessParam& param, ObTableAccessContext& context, const ObGetTableParam& get_table_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObMultipleMerge has been inited, ", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid()) || OB_UNLIKELY(!context.is_valid()) ||
             OB_UNLIKELY(!get_table_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(param), K(context), K(get_table_param));
  } else if (OB_FAIL(alloc_row(*context.stmt_allocator_,
                 max(param.reserve_cell_cnt_, param.out_col_desc_param_.count()),
                 cur_row_))) {
    STORAGE_LOG(WARN, "Fail to allocate row, ", K(ret));
  } else if (OB_FAIL(alloc_row(*context.stmt_allocator_,
                 max(param.reserve_cell_cnt_, param.out_col_desc_param_.count()),
                 unprojected_row_))) {
    STORAGE_LOG(WARN, "Fail to allocate row, ", K(ret));
  } else if (OB_FAIL(alloc_row(*context.stmt_allocator_, param.get_max_out_col_cnt(), full_row_))) {
  } else if (NULL != param.index_back_project_ && OB_FAIL(alloc_row(*context.stmt_allocator_,
                                                      max(param.reserve_cell_cnt_, param.index_back_project_->count()),
                                                      filter_row_))) {
    STORAGE_LOG(WARN, "Fail to allocate filter row, ", K(ret));
  } else if (OB_FAIL(
                 nop_pos_.init(*context.stmt_allocator_, max(param.reserve_cell_cnt_, param.get_max_out_col_cnt())))) {
    STORAGE_LOG(WARN, "Fail to init nop pos, ", K(ret));
  } else if (!param.is_index_back_index_scan() && NULL != param.op_ &&
             (NULL == param.output_exprs_ || NULL == param.row2exprs_projector_ ||
                 OB_FAIL(param.row2exprs_projector_->init(
                     *param.output_exprs_, param.op_->get_eval_ctx(), *param.iter_param_.out_cols_project_)))) {
    if (OB_SUCCESS == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("output expr is NULL or row2exprs_projector_ is NULL", K(ret), K(param));
    } else {
      LOG_WARN("init row to expr projector failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    need_padding_ = is_pad_char_to_full_length(context.sql_mode_);
    need_fill_default_ = true;
    need_output_row_with_nop_ = true;
    need_fill_virtual_columns_ = (NULL != param.virtual_column_exprs_ && param.virtual_column_exprs_->count() > 0) ||
                                 (NULL != param.row2exprs_projector_ && param.row2exprs_projector_->has_virtual());
    iters_.reuse();
    access_param_ = &param;

    if (context.query_flag_.is_reverse_scan()) {
      // Ban fast skip for columns need padding and reverse scan
      // TODO: Support pushdown for reverse scan
      const_cast<ObTableAccessParam*>(access_param_)->enable_fast_skip_ = false;
    }
    access_ctx_ = &context;
    row_stat_.reset();
    table_stat_.reset();
    table_stat_.pkey_ = access_ctx_->pkey_;
    cur_row_.row_val_.projector_ = access_param_->index_projector_;
    cur_row_.row_val_.projector_size_ = access_param_->projector_size_;
    cur_row_.row_val_.count_ = access_param_->iter_param_.out_cols_project_->count();
    skip_refresh_table_ = false;
    read_memtable_only_ = false;
    for (int64_t i = cur_row_.row_val_.count_; i < max(param.reserve_cell_cnt_, param.out_col_desc_param_.count());
         ++i) {
      cur_row_.row_val_.cells_[i].set_nop_value();
    }
    full_row_.row_val_.count_ = param.get_max_out_col_cnt();
    unprojected_row_.row_val_.count_ = 0;
    get_table_param_ = get_table_param;
    if (OB_FAIL(prepare_read_tables())) {
      STORAGE_LOG(WARN, "fail to prepare read tables", K(ret));
    } else if (OB_FAIL(deal_with_tables(context, tables_handle_))) {
      STORAGE_LOG(WARN, "check table version failed", K(ret));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

int ObMultipleMerge::reset_tables()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMultipleMerge has not inited", K(ret));
  } else if (OB_FAIL(deal_with_tables(*access_ctx_, tables_handle_))) {
    STORAGE_LOG(WARN, "check table version failed", K(ret));
  } else if (OB_FAIL(prepare())) {
    STORAGE_LOG(WARN, "fail to prepare", K(ret));
  } else if (OB_FAIL(calc_scan_range())) {
    STORAGE_LOG(WARN, "fail to calculate scan range", K(ret));
  } else if (OB_FAIL(is_range_valid())) {
    // skip construct iters if range is not valid any more
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "failed to check is range valid", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(construct_iters())) {
    STORAGE_LOG(WARN, "fail to construct iters", K(ret));
  } else {
    curr_rowkey_.reset();
    curr_scan_index_ = 0;
  }

  return ret;
}

void ObMultipleMerge::remove_filter()
{
  row_filter_ = NULL;
}

int ObMultipleMerge::report_table_store_stat()
{
  int ret = OB_SUCCESS;
  if (lib::is_diagnose_info_enabled()) {
    for (int64_t i = 0; i < iters_.count(); ++i) {
      // ignore ret
      iters_.at(i)->report_stat();
    }
    collect_merge_stat(table_stat_);
    // report access cnt and output cnt to the main table, ignore ret
    if (OB_FAIL(ObTableStoreStatMgr::get_instance().report_stat(table_stat_))) {
      STORAGE_LOG(WARN, "report tablestat to main table fail,", K(ret));
    }
    table_stat_.reuse();
  }
  return ret;
}

int ObMultipleMerge::save_curr_rowkey()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMultipleMerge has not been inited, ", K(ret));
  } else if (unprojected_row_.is_valid()) {
    ObStoreRowkey tmp_rowkey;
    tmp_rowkey.assign(unprojected_row_.row_val_.cells_, access_param_->iter_param_.rowkey_cnt_);

    if (OB_FAIL(tmp_rowkey.deep_copy(curr_rowkey_, *access_ctx_->allocator_))) {
      STORAGE_LOG(WARN, "fail to deep copy rowkey", K(ret));
    } else {
      curr_scan_index_ = unprojected_row_.scan_index_;
      STORAGE_LOG(DEBUG, "save current rowkey", K(tmp_rowkey), K(curr_scan_index_));
    }
  } else {
    curr_rowkey_.reset();
    curr_scan_index_ = 0;
  }

  return ret;
}

int ObMultipleMerge::deal_with_tables(ObTableAccessContext& context, ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tables_handle.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(tables_handle));
  } else if (OB_UNLIKELY(0 != context.trans_version_range_.base_version_ ||
                         0 != context.store_ctx_->mem_ctx_->get_base_version())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN,
        "base version should be 0",
        K(ret),
        K(context.trans_version_range_.base_version_),
        K(context.store_ctx_->mem_ctx_->get_base_version()));
  } else if (OB_UNLIKELY(0 == tables_handle.get_count() || 1 == tables_handle.get_count())) {
    // do nothing
  } else {
    const ObIArray<ObITable*>& tables = tables_handle.get_tables();
    common::ObSEArray<ObITable*, 8> result_tables;
    ObITable* cur_table = NULL;
    int64_t memtable_cnt = 0;
    for (int64_t pos = 0; OB_SUCC(ret) && pos < tables.count(); ++pos) {
      cur_table = tables.at(pos);
      if (OB_UNLIKELY(NULL == cur_table)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "cur_table is NULL", K(ret), KP(cur_table));
      } else if (cur_table->is_sstable()) {
        if (static_cast<ObSSTable*>(cur_table)->get_macro_block_count() <= 0) {  // empty sstable
          continue;
        }
      } else {
        ++memtable_cnt;
      }
      if (OB_SUCC(ret) && OB_FAIL(result_tables.push_back(cur_table))) {
        STORAGE_LOG(WARN, "failed to push into result tables", K(ret));
      }
    }  // end of for
    if (OB_SUCC(ret)) {
      if (memtable_cnt == result_tables.count()) {  // only have memtables
        read_memtable_only_ = true;
      }
      if (result_tables.count() != tables_handle_.get_count()) {
        tables_handle_.reset();
        if (OB_FAIL(tables_handle_.add_tables(result_tables))) {
          STORAGE_LOG(WARN, "failed to push result tables into tables handle", K(ret), K(result_tables));
        }
      }
    }
  }
  return ret;
}

int ObMultipleMerge::project_row(const ObStoreRow& unprojected_row, const ObIArray<int32_t>* projector,
    const int64_t range_idx_delta, ObStoreRow& projected_row)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  projected_row.flag_ = unprojected_row.flag_;
  projected_row.row_type_flag_ = unprojected_row.row_type_flag_;
  projected_row.from_base_ = unprojected_row.from_base_;
  projected_row.scan_index_ = unprojected_row.scan_index_ + range_idx_delta;
  projected_row.snapshot_version_ = unprojected_row.snapshot_version_;
  projected_row.range_array_idx_ = unprojected_row.range_array_idx_;
  nop_pos_.reset();
  for (int64_t i = 0; i < projected_row.row_val_.count_; ++i) {
    idx = nullptr == projector ? i : projector->at(i);
    if (idx >= 0) {
      projected_row.row_val_.cells_[i] = unprojected_row.row_val_.cells_[idx];
    } else {
      projected_row.row_val_.cells_[i].set_nop_value();
    }
    if (projected_row.row_val_.cells_[i].is_nop_value()) {
      nop_pos_.nops_[nop_pos_.count_++] = static_cast<int16_t>(i);
    }
  }
  return ret;
}

int ObMultipleMerge::project2output_exprs(ObStoreRow& unprojected_row, ObStoreRow& cur_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != access_ctx_->lob_locator_helper_)) {
    if (!access_ctx_->lob_locator_helper_->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected lob locator helper", K(ret), KPC(access_ctx_->lob_locator_helper_));
    } else if (OB_FAIL(access_ctx_->lob_locator_helper_->fill_lob_locator(
                   unprojected_row.row_val_, false, *access_param_))) {
      STORAGE_LOG(WARN, "Failed to fill row with lob locator", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    nop_pos_.reset();
    // need set range_array_idx_ which is checked in ObTableScanRangeArrayRowIterator::get_next_row.
    cur_row.range_array_idx_ = unprojected_row.range_array_idx_;
    // in function multi_get_rows, the flag is used to identify whether the row exists or not
    cur_row.flag_ = unprojected_row.flag_;
    ret = access_param_->row2exprs_projector_->project(
        *access_param_->output_exprs_, unprojected_row.row_val_.cells_, nop_pos_.nops_, nop_pos_.count_);
  }
  return ret;
}

int ObMultipleMerge::get_next_row(ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMultipleMerge has not been inited, ", K(ret));
  } else {
    bool filtered = false;
    while (OB_SUCC(ret)) {
      if (access_ctx_->is_end()) {
        ret = OB_ITER_END;
      } else if (nullptr != access_ctx_->limit_param_ && access_ctx_->limit_param_->limit_ >= 0 &&
                 access_ctx_->out_cnt_ - access_ctx_->limit_param_->offset_ >= access_ctx_->limit_param_->limit_) {
        ret = OB_ITER_END;
        access_ctx_->is_end_ = !access_ctx_->is_array_binding_;
        if (access_ctx_->is_array_binding_) {
          if (OB_FAIL(skip_to_range(access_ctx_->range_array_pos_->at(access_ctx_->range_array_cursor_) + 1))) {
            if (OB_ITER_END == ret) {
              access_ctx_->is_end_ = true;
            } else {
              STORAGE_LOG(WARN, "fail to skip range", K(ret));
            }
          } else {
            if (next_row_ != nullptr && next_row_->range_array_idx_ == access_ctx_->range_array_cursor_) {
              next_row_ = nullptr;
            }
            ret = OB_ITER_END;
          }
        }
      } else if (OB_FAIL(refresh_table_on_demand())) {
        STORAGE_LOG(WARN, "fail to refresh table on demand", K(ret));
      } else if (nullptr != next_row_) {
        // reuse the unprojected_row_
      } else if (OB_FAIL(inner_get_next_row(unprojected_row_))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Fail to inner get next row, ", K(ret), KP(this));
        } else {
          skip_refresh_table_ = true;
        }
      }

      if (OB_SUCCESS == ret) {
        if (OB_FAIL(check_row_in_current_range(unprojected_row_))) {
          if (OB_ARRAY_BINDING_SWITCH_ITERATOR != ret) {
            STORAGE_LOG(WARN, "fail to check row in current range", K(ret));
          } else {
            next_row_ = &unprojected_row_;
          }
        } else {
          next_row_ = nullptr;
        }
      }

      if (OB_SUCCESS == ret) {
        bool is_filtered = false;
        bool not_using_static_engine =
            (access_param_->output_exprs_ == NULL || access_param_->is_index_back_index_scan());
        if (OB_UNLIKELY(NULL != row_filter_) && OB_FAIL(row_filter_->check(unprojected_row_, is_filtered))) {
          STORAGE_LOG(WARN, "filter row failed", K(ret), K(unprojected_row_));
        } else if (is_filtered) {
          STORAGE_LOG(DEBUG, "store row is filtered", K(unprojected_row_));
          ++filt_cnt_;
        } else if (OB_FAIL((not_using_static_engine) ? project_row(unprojected_row_,
                                                           access_param_->iter_param_.out_cols_project_,
                                                           range_idx_delta_,
                                                           cur_row_)
                                                     : project2output_exprs(unprojected_row_, cur_row_))) {
          STORAGE_LOG(WARN, "fail to project row", K(ret));
        } else if (need_fill_default_ && nop_pos_.count() > 0 && OB_FAIL(fuse_default(cur_row_.row_val_))) {
          STORAGE_LOG(WARN, "Fail to fuse default row, ", K(ret));
        } else if (!need_fill_default_ && !need_output_row_with_nop_ && nop_pos_.count() > 0) {
          // this is for sample scan on increment data, we only output one row if increment data
          // has all the column data needed by the sample scan
          continue;
        } else if (need_padding_ && OB_FAIL(pad_columns(cur_row_.row_val_))) {
          STORAGE_LOG(WARN, "Fail to padding columns, ", K(ret));
        } else if (need_fill_virtual_columns_ && OB_FAIL(fill_virtual_columns(cur_row_.row_val_))) {
          STORAGE_LOG(WARN, "Fail to fill virtual columns, ", K(ret));
          // new static type engine, obj is converted to datum, but no meta is used any more, so do not fill scale here
        } else if (not_using_static_engine && access_param_->need_fill_scale_ &&
                   OB_FAIL(fill_scale(cur_row_.row_val_))) {
          STORAGE_LOG(WARN, "Fail to fill scale", K(ret));
          // fill_lob_locator need to finish before check_filtered, because:
          // The new expression parameters in new engine is lob locator type, while the expected
          // parameters may not be lob locator, thus can do a implicit cast here.
          // If fill_lob_locator hasn't completed during computing filter condition,
          // the implicit cast can cause problem.
        } else if (not_using_static_engine && OB_UNLIKELY(nullptr != access_ctx_->lob_locator_helper_)) {
          if (!access_ctx_->lob_locator_helper_->is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "Unexpected lob locator helper", K(ret), KPC(access_ctx_->lob_locator_helper_));
          } else if (OB_FAIL(
                         access_ctx_->lob_locator_helper_->fill_lob_locator(cur_row_.row_val_, true, *access_param_))) {
            STORAGE_LOG(WARN, "Failed to fill row with lob locator", K(ret));
          }
        }
        if (OB_FAIL(ret) || is_filtered) {
        } else if (unprojected_row_.fast_filter_skipped_) {
          filtered = false;
          unprojected_row_.fast_filter_skipped_ = false;
        } else if (OB_FAIL(check_filtered(cur_row_.row_val_, filtered))) {
          STORAGE_LOG(WARN, "fail to check row filtered", K(ret));
        }

        if (OB_FAIL(ret)) {
        } else if (filtered) {
          ++filt_cnt_;
          row = NULL;
        } else {
          row = &cur_row_;
        }
        if (NULL != access_ctx_->table_scan_stat_) {
          access_ctx_->table_scan_stat_->access_row_cnt_++;
        }
      }

      // check row
      if (OB_SUCC(ret) && nullptr != row) {
        bool out = false;
        if (nullptr != access_ctx_->limit_param_) {
          if (access_ctx_->out_cnt_ < access_ctx_->limit_param_->offset_) {
            if (NULL != access_param_->op_) {
              // clear evaluated flag for next row.
              access_param_->op_->clear_evaluated_flag();
            }
            out = false;
          } else {
            out = true;
          }
        } else {
          out = true;
        }
        ++access_ctx_->out_cnt_;
        if (out) {
          break;
        }
      }
    }

    if (OB_ITER_END == ret) {
      EVENT_ADD(STORAGE_READ_ROW_COUNT, scan_cnt_);
      if (NULL != access_ctx_->table_scan_stat_) {
        access_ctx_->table_scan_stat_->access_row_cnt_ += row_stat_.filt_del_count_;
        table_stat_.access_row_cnt_ += row_stat_.filt_del_count_;
        access_ctx_->table_scan_stat_->rowkey_prefix_ = access_ctx_->access_stat_.rowkey_prefix_;
        access_ctx_->table_scan_stat_->bf_filter_cnt_ += access_ctx_->access_stat_.bf_filter_cnt_;
        access_ctx_->table_scan_stat_->bf_access_cnt_ += access_ctx_->access_stat_.bf_access_cnt_;
        access_ctx_->table_scan_stat_->empty_read_cnt_ += access_ctx_->access_stat_.empty_read_cnt_;
        access_ctx_->table_scan_stat_->fuse_row_cache_hit_cnt_ += table_stat_.fuse_row_cache_hit_cnt_;
        access_ctx_->table_scan_stat_->fuse_row_cache_miss_cnt_ += table_stat_.fuse_row_cache_miss_cnt_;
        access_ctx_->table_scan_stat_->block_cache_hit_cnt_ += access_ctx_->access_stat_.block_cache_hit_cnt_;
        access_ctx_->table_scan_stat_->block_cache_miss_cnt_ += access_ctx_->access_stat_.block_cache_miss_cnt_;
        access_ctx_->table_scan_stat_->row_cache_hit_cnt_ += access_ctx_->access_stat_.row_cache_hit_cnt_;
        access_ctx_->table_scan_stat_->row_cache_miss_cnt_ += access_ctx_->access_stat_.row_cache_miss_cnt_;
      }
      report_table_store_stat();
    }
    if (OB_SUCC(ret)) {
      if (NULL != access_ctx_->table_scan_stat_) {
        access_ctx_->table_scan_stat_->out_row_cnt_++;
      }
    }
  }
  return ret;
}

void ObMultipleMerge::reset()
{
  ObStoreRowIterator* iter = NULL;
  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (NULL != (iter = iters_.at(i))) {
      iter->~ObStoreRowIterator();
    }
  }
  padding_allocator_.reset();
  iters_.reset();
  access_param_ = NULL;
  access_ctx_ = NULL;
  tables_handle_.reset();
  nop_pos_.reset();
  row_stat_.reset();
  table_stat_.reset();
  scan_cnt_ = 0;
  filt_cnt_ = 0;
  need_padding_ = false;
  need_fill_default_ = false;
  need_fill_virtual_columns_ = false;
  need_output_row_with_nop_ = false;
  inited_ = false;
  range_idx_delta_ = 0;
  next_row_ = NULL;
  out_cols_projector_ = NULL;
  skip_refresh_table_ = false;
  read_memtable_only_ = false;
}

void ObMultipleMerge::reuse()
{
  reuse_iter_array();
  row_stat_.reset();
  table_stat_.reuse();
  range_idx_delta_ = 0;
  unprojected_row_.flag_ = -1;
  next_row_ = nullptr;
  skip_refresh_table_ = false;
  read_memtable_only_ = false;
}

void ObMultipleMerge::reuse_iter_array()
{
  ObStoreRowIterator* iter = NULL;
  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (NULL != (iter = iters_.at(i))) {
      iter->~ObStoreRowIterator();
    }
  }
  iters_.reuse();
}

int ObMultipleMerge::open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMultipleMerge has not been inited, ", K(ret));
  } else {
    ObMultipleMerge::reuse();
    scan_cnt_ = 0;
    filt_cnt_ = 0;
  }
  return ret;
}

int ObMultipleMerge::alloc_row(common::ObIAllocator& allocator, const int64_t cell_cnt, ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (OB_UNLIKELY(cell_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(cell_cnt), K(ret));
  } else if (NULL == (buf = allocator.alloc(sizeof(ObObj) * cell_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(cell_cnt), K(ret));
  } else {
    MEMSET(buf, 0, sizeof(ObObj) * cell_cnt);  // set cells to ObNullType
    row.row_val_.cells_ = (ObObj*)buf;
    row.capacity_ = cell_cnt;
    row.row_val_.count_ = cell_cnt;
    row.row_val_.projector_ = NULL;
    row.row_val_.projector_size_ = 0;
  }
  return ret;
}

int ObMultipleMerge::fuse_default(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < nop_pos_.count(); ++i) {
    if (NULL == access_param_->out_cols_param_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "The out cols param is NULL, ", K(ret));
    } else if (OB_FAIL(nop_pos_.get_nop_pos(i, pos))) {
      STORAGE_LOG(WARN, "Fail to get nop pos, ", K(ret));
    } else {
      idx = access_param_->iter_param_.out_cols_project_->at(pos);
      const ObObj& def_cell = access_param_->out_cols_param_->at(idx)->get_orig_default_value();
      if (NULL == access_param_->output_exprs_ || access_param_->is_index_back_index_scan()) {
        row.cells_[pos] = def_cell;
      } else {
        // skip virtual column which has nop default value.
        if (!def_cell.is_nop_value()) {
          sql::ObExpr* expr = access_param_->output_exprs_->at(pos);
          sql::ObDatum& datum = expr->locate_datum_for_write(access_param_->op_->get_eval_ctx());
          sql::ObEvalInfo& eval_info = expr->get_eval_info(access_param_->op_->get_eval_ctx());
          if (OB_FAIL(datum.from_obj(def_cell, expr->obj_datum_map_))) {
            LOG_WARN("convert obj to datum failed", K(ret));
          } else {
            eval_info.evaluated_ = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObMultipleMerge::pad_columns(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<int32_t>* padding_cols = access_param_->padding_cols_;
  int32_t idx = 0;
  padding_allocator_.reuse();

  for (int64_t i = 0; OB_SUCC(ret) && NULL != padding_cols && i < padding_cols->count(); ++i) {
    if (NULL == access_param_->out_cols_param_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "The out cols param is NULL, ", K(ret));
    } else {
      idx = padding_cols->at(i);
      const int64_t col_idx = access_param_->iter_param_.out_cols_project_->at(idx);
      share::schema::ObColumnParam* col_param = access_param_->out_cols_param_->at(col_idx);
      if (NULL == access_param_->output_exprs_ || access_param_->is_index_back_index_scan()) {
        if (OB_FAIL(pad_column(col_param->get_accuracy(), padding_allocator_, row.cells_[idx]))) {
          STORAGE_LOG(WARN, "Fail to pad column, ", K(*col_param), K(ret));
        }
      } else {
        sql::ObExpr* e = access_param_->output_exprs_->at(idx);
        if (e->arg_cnt_ > 0 && unprojected_row_.row_val_.cells_[col_idx].is_nop_value()) {
          // do nothing for virtual column with no data read,
          // datum is filled && padded in fill_virtual_columns().
        } else {
          if (OB_ISNULL(access_param_->op_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected access param: null op", K(ret));
          } else if (OB_FAIL(pad_column(col_param->get_accuracy(), access_param_->op_->get_eval_ctx(), e))) {
            LOG_WARN("pad column failed", K(ret), K(*col_param));
          }
        }
      }
    }
  }
  return ret;
}

int ObMultipleMerge::fill_virtual_columns(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (NULL == access_param_->op_ && NULL != access_param_->virtual_column_exprs_) {
    const ObColumnExprArray& virtual_column_exprs = *(access_param_->virtual_column_exprs_);
    const ObIColumnExpression* expression = NULL;

    for (int64_t i = 0; OB_SUCC(ret) && i < virtual_column_exprs.count(); ++i) {
      if (OB_ISNULL(expression = virtual_column_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "virtual column expr is NULL", K(virtual_column_exprs), K(i), K(ret));
      } else {
        const ObIArray<share::schema::ObColumnParam*>* out_cols_param = access_param_->out_cols_param_;
        int64_t idx = expression->get_result_index();
        if (OB_UNLIKELY(idx < 0) || OB_UNLIKELY(idx >= row.count_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Invalid column idx", K(i), K(idx), K(row), K(ret));
        } else {
          bool is_urowid_col = idx < out_cols_param->count() &&
                               out_cols_param->at(idx)->get_column_id() == OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID;
          STORAGE_LOG(DEBUG,
              "fill virtual column",
              K(idx),
              K(*expression),
              K(row.cells_[idx].is_nop_value()),
              K(is_urowid_col));
          if (row.cells_[idx].is_nop_value() || is_urowid_col) {
            if (OB_FAIL(expression->calc_and_project(*access_ctx_->expr_ctx_, row))) {
              STORAGE_LOG(WARN, "fail to calculate filter", K(i), K(ret));
            } else if (need_padding_ && row.cells_[idx].is_fixed_len_char_type()) {
              const int64_t col_idx = access_param_->iter_param_.out_cols_project_->at(idx);
              if (OB_FAIL(
                      pad_column(out_cols_param->at(col_idx)->get_accuracy(), padding_allocator_, row.cells_[idx]))) {
                STORAGE_LOG(WARN, "Fail to pad virtual column, ", K(ret));
              }
            } else {
              /*do nothing*/
            }
          } else {
            /*do nothing*/
          }
        }
      }
    }
  } else if (NULL != access_param_->op_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < nop_pos_.count(); i++) {
      int64_t pos = 0;
      if (OB_FAIL(nop_pos_.get_nop_pos(i, pos))) {
        LOG_WARN("get position failed", K(ret));
      } else {
        sql::ObExpr* expr = access_param_->output_exprs_->at(pos);
        // table scan access exprs is column reference expr, only virtual column has argument.
        if (expr->arg_cnt_ > 0) {
          ObDatum* datum = NULL;
          expr->get_eval_info(access_param_->op_->get_eval_ctx()).evaluated_ = false;
          if (OB_FAIL(expr->eval(access_param_->op_->get_eval_ctx(), datum))) {
            LOG_WARN("evaluate virtual column failed", K(ret));
          } else if (need_padding_ && expr->obj_meta_.is_fixed_len_char_type()) {
            const int64_t col_idx = access_param_->iter_param_.out_cols_project_->at(pos);
            if (OB_ISNULL(access_param_->op_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unexpected access param: null op", K(ret));
            } else if (OB_FAIL(pad_column(access_param_->out_cols_param_->at(col_idx)->get_accuracy(),
                           access_param_->op_->get_eval_ctx(),
                           expr))) {
              LOG_WARN("pad column failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMultipleMerge::check_filtered(const ObNewRow& row, bool& filtered)
{
  int ret = OB_SUCCESS;
  // check if timeout or if transaction status every 10000 rows, which should be within 10ms
  if (0 == (++scan_cnt_ % 10000) && !access_ctx_->query_flag_.is_daily_merge()) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      STORAGE_LOG(WARN, "query interrupt, ", K(ret));
    }
  }

  // check filter
  if (OB_SUCC(ret) && NULL != access_param_->filters_) {
    const ObNewRow* check_row = &row;
    if (NULL != access_param_->index_back_project_) {
      int64_t idx = 0;
      int64_t pad_idx = 0;
      check_row = &filter_row_.row_val_;
      for (int64_t i = 0; OB_SUCC(ret) && i < access_param_->index_back_project_->count(); ++i) {
        idx = access_param_->index_back_project_->at(i);
        if (idx >= 0) {
          filter_row_.row_val_.cells_[i] = row.cells_[idx];
          if (need_padding_ && filter_row_.row_val_.cells_[i].is_fixed_len_char_type()) {
            pad_idx = access_param_->iter_param_.out_cols_project_->at(idx);
            if (OB_FAIL(pad_column(access_param_->out_cols_param_->at(pad_idx)->get_accuracy(),
                    padding_allocator_,
                    filter_row_.row_val_.cells_[i]))) {
              STORAGE_LOG(WARN, "Fail to pad column, ", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql::ObPhyOperator::filter_row(
              *access_ctx_->expr_ctx_, *check_row, *access_param_->filters_, filtered))) {
        STORAGE_LOG(WARN, "fail to filter row", K(ret), K(row));
      }
    }
  }

  if (OB_SUCC(ret) && NULL != access_param_->op_filters_ && !access_param_->op_filters_->empty()) {
    // Execute filter in sql static typing engine.
    // %row is already projected to output expressions for main table scan.
    // We need to project %row to output expression for index back index scan.
    if (NULL != access_param_->index_back_project_) {
      ObObj pad_cell;
      ObObj* cell = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < access_param_->index_back_project_->count(); ++i) {
        int64_t idx = access_param_->index_back_project_->at(i);
        if (idx >= 0) {
          sql::ObExpr* expr = access_param_->output_exprs_->at(i);
          cell = &row.cells_[idx];
          if (need_padding_ && cell->is_fixed_len_char_type()) {
            int64_t pad_idx = access_param_->iter_param_.out_cols_project_->at(idx);
            pad_cell = *cell;
            cell = &pad_cell;
            if (OB_FAIL(pad_column(
                    access_param_->out_cols_param_->at(pad_idx)->get_accuracy(), padding_allocator_, *cell))) {
              STORAGE_LOG(WARN, "Fail to pad column, ", K(ret));
            }
          }
          sql::ObDatum& datum = expr->locate_datum_for_write(access_param_->op_->get_eval_ctx());
          sql::ObEvalInfo& eval_info = expr->get_eval_info(access_param_->op_->get_eval_ctx());
          if (OB_SUCC(ret)) {
            if (OB_FAIL(datum.from_obj(*cell, expr->obj_datum_map_))) {
              LOG_WARN("convert obj to datum failed", K(ret));
            } else {
              eval_info.evaluated_ = true;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool is_before_indexback = (NULL != access_param_->index_back_project_);
      if (OB_FAIL(access_param_->op_->filter_row_outside(is_before_indexback, *access_param_->op_filters_, filtered))) {
        LOG_WARN("filter row failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMultipleMerge::check_row_in_current_range(const ObStoreRow& row)
{
  int ret = OB_SUCCESS;

  int comp_ret = static_cast<int32_t>(access_ctx_->range_array_cursor_ - row.range_array_idx_);
  if (comp_ret > 0) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN,
        "switch range array when iterator does not reach end is not supported",
        K(ret),
        K(access_ctx_->range_array_cursor_),
        "curr_row_range_array_idx",
        row.range_array_idx_);
  } else if (comp_ret < 0) {
    // curr range is empty
    ret = OB_ARRAY_BINDING_SWITCH_ITERATOR;
    STORAGE_LOG(DEBUG, "get next row iter end", K(access_ctx_->range_array_cursor_), K(row.range_array_idx_), K(row));
  }
  return ret;
}

int ObMultipleMerge::switch_iterator(const int64_t range_array_idx)
{
  int ret = OB_SUCCESS;
  access_ctx_->range_array_cursor_ = range_array_idx;
  access_ctx_->out_cnt_ = 0;
  STORAGE_LOG(DEBUG, "switch iterator", K(range_array_idx));
  return ret;
}

int ObMultipleMerge::add_iterator(ObStoreRowIterator& iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iters_.push_back(&iter))) {
    STORAGE_LOG(WARN, "fail to push back iterator", K(ret));
  }
  return ret;
}

const ObTableIterParam* ObMultipleMerge::get_actual_iter_param(const ObITable* table) const
{
  UNUSED(table);
  return &access_param_->iter_param_;
}

int ObMultipleMerge::prepare_read_tables()
{
  int ret = OB_SUCCESS;
  tables_handle_.reset();
  if (OB_UNLIKELY(!get_table_param_.is_valid() || !access_param_->is_valid() || NULL == access_ctx_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(
        WARN, "ObMultipleMerge has not been inited", K(ret), K_(get_table_param), K_(access_param), KP_(access_ctx));
  } else if (OB_UNLIKELY(nullptr != get_table_param_.tables_handle_)) {
    if (OB_FAIL(tables_handle_.add_tables(*get_table_param_.tables_handle_))) {
      STORAGE_LOG(WARN, "fail to add tables", K(ret));
    }
  } else {
    ObPartitionStore& partition_store = *get_table_param_.partition_store_;
    const uint64_t main_table_id = access_param_->iter_param_.table_id_;
    if (OB_UNLIKELY(get_table_param_.frozen_version_ != -1)) {
      if (!get_table_param_.sample_info_.is_no_sample()) {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN,
            "sample query does not support frozen_version",
            K(ret),
            K_(get_table_param),
            K_(access_param),
            K(*access_ctx_));
      } else {
        if (OB_SUCC(ret)) {
          if (OB_FAIL(partition_store.get_read_frozen_tables(
                  main_table_id, get_table_param_.frozen_version_, tables_handle_, false /*reset_handle*/))) {
            STORAGE_LOG(WARN, "fail to get read frozen tables", K(ret), K_(access_param), K_(get_table_param));
          }
        }
      }
    } else if (OB_LIKELY(get_table_param_.sample_info_.is_no_sample())) {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(partition_store.get_read_tables(main_table_id,
                access_ctx_->store_ctx_->mem_ctx_->get_read_snapshot(),
                tables_handle_,
                false /*allow_not_ready*/,
                false /*need_safety_check*/,
                false /*reset handle*/))) {
          STORAGE_LOG(WARN, "fail to get read tables", K(ret));
        }
      }
    } else {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(partition_store.get_sample_read_tables(
                get_table_param_.sample_info_, main_table_id, tables_handle_, false /*reset_handle*/))) {
          STORAGE_LOG(WARN, "fail to get sample read tables", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      relocate_cnt_ = access_ctx_->store_ctx_->mem_ctx_->get_relocate_cnt();
      if (OB_UNLIKELY(nullptr != row_filter_)) {
        const ObPartitionKey& pkey = partition_store.get_partition_key();
        row_filter_ = tables_handle_.has_split_source_table(pkey) ? row_filter_ : NULL;
      }
    }
  }
  return ret;
}

int ObMultipleMerge::refresh_table_on_demand()
{
  int ret = OB_SUCCESS;
  bool need_refresh = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMultipleMerge has not been inited", K(ret));
  } else if (skip_refresh_table_) {
    STORAGE_LOG(DEBUG, "skip refresh table");
  } else if (OB_FAIL(check_need_refresh_table(need_refresh))) {
    STORAGE_LOG(WARN, "fail to check need refresh table", K(ret));
  } else if (need_refresh) {
    if (OB_FAIL(save_curr_rowkey())) {
      STORAGE_LOG(WARN, "fail to save current rowkey", K(ret));
    } else if (FALSE_IT(reuse_iter_array())) {
    } else if (OB_FAIL(prepare_read_tables())) {
      STORAGE_LOG(WARN, "fail to prepare read tables", K(ret));
    } else if (OB_FAIL(reset_tables())) {
      STORAGE_LOG(WARN, "fail to reset tables", K(ret));
    }
    STORAGE_LOG(INFO,
        "table need to be refresh",
        "table_id",
        access_param_->iter_param_.table_id_,
        K(*access_param_),
        K(curr_scan_index_));
  }
  return ret;
}

int ObMultipleMerge::check_need_refresh_table(bool& need_refresh)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMultipleMerge has not been inited", K(ret));
  } else {
    const bool relocated = NULL == access_ctx_->store_ctx_->mem_ctx_
                               ? false
                               : access_ctx_->store_ctx_->mem_ctx_->get_relocate_cnt() > relocate_cnt_;
    const bool memtable_retired = tables_handle_.check_store_expire();
    need_refresh = relocated || memtable_retired;
#ifdef ERRSIM
    ret = E(EventTable::EN_FORCE_REFRESH_TABLE) ret;
    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS;
      need_refresh = true;
    }
#endif
  }
  return ret;
}

int ObMultipleMerge::skip_to_range(const int64_t range_idx)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(range_idx);
  return ret;
}

int ObMultipleMerge::fill_scale(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_param_->col_scale_info_.count(); i++) {
    int64_t idx = access_param_->col_scale_info_.at(i).first;
    ObScale scale = access_param_->col_scale_info_.at(i).second;
    if (OB_UNLIKELY(idx < 0 || idx >= row.count_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid cell idx", K(ret), K(idx), K(row));
    } else {
      row.cells_[idx].set_scale(scale);
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
