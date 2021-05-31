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

#include "ob_table_scan_iterator.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/stat/ob_diagnose_info.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/object/ob_obj_compare.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/ob_multiple_scan_merge.h"
#include "storage/ob_index_merge.h"
#include "storage/ob_partition_store.h"
#include "storage/ob_store_row_filter.h"
#include "ob_warm_up.h"
#include <sys/time.h>
#include <sys/resource.h>
namespace oceanbase {
namespace storage {
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;

ObTableScanStoreRowIterator::ObTableScanStoreRowIterator()
    : is_inited_(false),
      single_merge_(NULL),
      get_merge_(NULL),
      scan_merge_(NULL),
      multi_scan_merge_(NULL),
      index_merge_(NULL),
      row_sample_iterator_(NULL),
      block_sample_iterator_(NULL),
      main_table_param_(),
      main_table_ctx_(),
      index_table_param_(),
      index_table_ctx_(),
      get_table_param_(),
      trans_service_(NULL),
      ctx_(),
      scan_param_(NULL),
      partition_store_(NULL),
      range_iter_(),
      main_iter_(NULL),
      row_filter_(NULL),
      block_cache_ws_(),
      is_iter_opened_(false),
      use_fuse_row_cache_(true)
{}

ObTableScanStoreRowIterator::~ObTableScanStoreRowIterator()
{
  reset();
}

void ObTableScanStoreRowIterator::reset()
{
  // revert store ctx
  if (NULL != trans_service_ && NULL != scan_param_ && NULL != scan_param_->trans_desc_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS !=
        (tmp_ret = trans_service_->revert_store_ctx(*scan_param_->trans_desc_, scan_param_->pg_key_, ctx_))) {
      STORAGE_LOG(ERROR, "fail to revert store ctx", K(tmp_ret), K(scan_param_->pg_key_));
    }
  }

  if (NULL != single_merge_) {
    single_merge_->~ObSingleMerge();
    single_merge_ = NULL;
  }
  if (NULL != get_merge_) {
    get_merge_->~ObMultipleGetMerge();
    get_merge_ = NULL;
  }
  if (NULL != scan_merge_) {
    scan_merge_->~ObMultipleScanMerge();
    scan_merge_ = NULL;
  }
  if (NULL != multi_scan_merge_) {
    multi_scan_merge_->~ObMultipleMultiScanMerge();
    multi_scan_merge_ = NULL;
  }
  if (NULL != index_merge_) {
    index_merge_->~ObIndexMerge();
    index_merge_ = NULL;
  }
  if (NULL != row_sample_iterator_) {
    row_sample_iterator_->~ObRowSampleIterator();
    row_sample_iterator_ = NULL;
  }
  if (NULL != block_sample_iterator_) {
    block_sample_iterator_->~ObBlockSampleIterator();
    block_sample_iterator_ = NULL;
  }
  if (NULL != row_filter_) {
    row_filter_->~ObIStoreRowFilter();
    row_filter_ = NULL;
  }

  main_table_param_.reset();
  main_table_ctx_.reset();
  index_table_param_.reset();
  index_table_ctx_.reset();
  get_table_param_.reset();
  trans_service_ = NULL;
  ctx_.reset();
  scan_param_ = NULL;
  partition_store_ = NULL;
  range_iter_.reset();
  main_iter_ = NULL;
  block_cache_ws_.reset();
  is_iter_opened_ = false;
  use_fuse_row_cache_ = true;
  is_inited_ = false;
}

void ObTableScanStoreRowIterator::reuse_row_iters()
{
  if (NULL != single_merge_) {
    single_merge_->reuse();
  }
  if (NULL != get_merge_) {
    get_merge_->reuse();
  }
  if (NULL != scan_merge_) {
    scan_merge_->reuse();
  }
  if (NULL != multi_scan_merge_) {
    multi_scan_merge_->reuse();
  }
  if (NULL != index_merge_) {
    index_merge_->reuse();
  }
  if (NULL != row_sample_iterator_) {
    row_sample_iterator_->reuse();
  }
  if (NULL != block_sample_iterator_) {
    block_sample_iterator_->reuse();
  }
}

int ObTableScanStoreRowIterator::prepare_table_param()
{
  int ret = OB_SUCCESS;
  if (nullptr == scan_param_ || nullptr == scan_param_->table_param_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(scan_param_));
  } else if (OB_FAIL(main_table_param_.init(*scan_param_))) {
    STORAGE_LOG(WARN, "failed to init main table param", K(ret));
  } else if (scan_param_->scan_flag_.is_index_back() && OB_FAIL(index_table_param_.init_index_back(*scan_param_))) {
    STORAGE_LOG(WARN, "failed to init index table param", K(ret));
  }
  return ret;
}

int ObTableScanStoreRowIterator::prepare_table_context(const ObIStoreRowFilter* row_filter)
{
  int ret = OB_SUCCESS;
  if (nullptr == scan_param_ || nullptr == scan_param_->table_param_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(scan_param_));
  } else {
    ObVersionRange trans_version_range;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.base_version_ = 0;
    trans_version_range.snapshot_version_ = ctx_.mem_ctx_->get_read_snapshot();
    if (OB_UNLIKELY(!trans_version_range.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "trans version range is not valid", K(ret), K(trans_version_range));
    } else if (OB_FAIL(main_table_ctx_.init(
                   *scan_param_, ctx_, block_cache_ws_, trans_version_range, row_filter, false /* is_index_back */))) {
      STORAGE_LOG(WARN, "failed to init main table ctx", K(ret));
    } else if (scan_param_->scan_flag_.is_index_back()) {
      if (OB_FAIL(index_table_ctx_.init(
              *scan_param_, ctx_, block_cache_ws_, trans_version_range, row_filter, true /* is_index_back */))) {
        STORAGE_LOG(WARN, "failed to init index table ctx", K(ret));
      } else {
        index_table_ctx_.use_fuse_row_cache_ = false;
      }
    }
    if (OB_SUCC(ret)) {
      main_table_ctx_.fuse_row_cache_hit_rate_ = scan_param_->fuse_row_cache_hit_rate_;
      main_table_ctx_.block_cache_hit_rate_ = scan_param_->block_cache_hit_rate_;
    }
  }
  return ret;
}

int ObTableScanStoreRowIterator::init_scan_iter(const bool index_back, ObMultipleMerge& iter)
{
  int ret = OB_SUCCESS;
  if (index_back) {
    ret = iter.init(index_table_param_, index_table_ctx_, get_table_param_);
  } else {
    ret = iter.init(main_table_param_, main_table_ctx_, get_table_param_);
  }
  if (!scan_param_->sample_info_.is_no_sample() && SampleInfo::SAMPLE_INCR_DATA == scan_param_->sample_info_.scope_) {
    iter.disable_fill_default();
    iter.disable_output_row_with_nop();
  }
  return ret;
}

int ObTableScanStoreRowIterator::rescan(const ObRangeArray& key_ranges, const ObPosArray& range_array_pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObTableScanStoreRowIterator has not been inited, ", K(ret));
  } else if (&scan_param_->key_ranges_ != &key_ranges || &scan_param_->range_array_pos_ != &range_array_pos) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "scan_param is not the same", K(ret), K(scan_param_), K(key_ranges), K(range_array_pos));
  } else {
    STORAGE_LOG(DEBUG, "table scan iterate rescan", K_(is_inited), K(scan_param_));
    // there's no need to reset main_table_param_ and index_table_param_
    // scan_param only reset query range fields in ObTableScan::rt_rescan()
    range_iter_.reuse();
    main_iter_ = NULL;
    reuse_row_iters();
    main_table_ctx_.reuse();
    index_table_ctx_.reuse();
    is_iter_opened_ = false;

    if (OB_FAIL(range_iter_.set_scan_param(*scan_param_))) {
      STORAGE_LOG(WARN, "set scan param to range iterator failed", K(ret));
    } else if (OB_FAIL(prepare_table_context(row_filter_))) {
      STORAGE_LOG(WARN, "fail to prepare table context", K(ret));
    } else {
      if (OB_FAIL(open_iter())) {
        STORAGE_LOG(WARN, "fail to open iter", K(ret));
      } else {
        is_iter_opened_ = true;
      }
    }
  }
  return ret;
}

int ObTableScanStoreRowIterator::init(transaction::ObTransService& trans_service, const ObStoreCtx& ctx,
    ObTableScanParam& scan_param, ObPartitionStore& partition_store)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObTableScanStoreRowIterator has been inited, ", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_valid()) || OB_UNLIKELY(!scan_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(ctx.is_valid()), K(scan_param.is_valid()));
  } else if (OB_FAIL(range_iter_.set_scan_param(scan_param))) {
    STORAGE_LOG(WARN, "Fail to set scan param, ", K(ret));
  } else {
    if (NULL != scan_param.part_filter_ && NULL != scan_param.part_mgr_) {
      void* buf = NULL;
      if (NULL == (buf = scan_param.allocator_->alloc(sizeof(ObStoreRowFilter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "alloc store row filter failed", K(ret));
      } else if (NULL == (row_filter_ = new (buf) ObStoreRowFilter())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "new store row filter failed", K(ret), KP(buf));
      } else if (OB_FAIL(row_filter_->init(scan_param.part_filter_,
                     scan_param.expr_ctx_.exec_ctx_,
                     scan_param.part_mgr_,
                     scan_param.pkey_))) {
        STORAGE_LOG(WARN, "row filter init failed", K(ret));
      } else {
        // do nothing
      }
    }

    if (OB_SUCC(ret)) {
      const uint64_t tenant_id = scan_param.pkey_.get_tenant_id();
      use_fuse_row_cache_ = true;
      ctx_ = ctx;
      scan_param_ = &scan_param;
      partition_store_ = &partition_store;
      if (OB_FAIL(block_cache_ws_.init(tenant_id))) {
        STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(prepare_table_param())) {
        STORAGE_LOG(WARN, "Fail to prepare table param, ", K(ret));
      } else if (OB_FAIL(prepare_table_context(row_filter_))) {
        STORAGE_LOG(WARN, "Fail to prepare table ctx, ", K(ret));
      } else {
        if (!is_iter_opened_) {
          if (OB_FAIL(open_iter())) {
            STORAGE_LOG(WARN, "fail to open iter", K(ret));
          } else {
            is_iter_opened_ = true;
          }
        }
        if (OB_SUCC(ret)) {
          trans_service_ = &trans_service;
          is_inited_ = true;
        }
      }
    }
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTableScanStoreRowIterator::open_iter()
{
  int ret = OB_SUCCESS;
  ObBatch batch;
  void* buf = NULL;
  if (OB_FAIL(range_iter_.get_next(batch))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get batch range", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    get_table_param_.partition_store_ = partition_store_;
    get_table_param_.frozen_version_ = scan_param_->frozen_version_;
    get_table_param_.sample_info_ = scan_param_->sample_info_;
    switch (batch.type_) {
      case ObBatch::T_GET: {
        if (NULL == single_merge_) {
          if (NULL == (buf = scan_param_->allocator_->alloc(sizeof(ObSingleMerge)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
          } else {
            single_merge_ = new (buf) ObSingleMerge();
            if (OB_FAIL(init_scan_iter(scan_param_->scan_flag_.is_index_back(), *single_merge_))) {
              STORAGE_LOG(WARN, "Fail to init single merge, ", K(ret));
            } else {
              use_fuse_row_cache_ = !single_merge_->is_read_memtable_only();
            }
          }
        }
        if (OB_SUCC(ret)) {
          main_table_ctx_.use_fuse_row_cache_ = use_fuse_row_cache_;
          if (OB_FAIL(single_merge_->open(*batch.rowkey_))) {
            STORAGE_LOG(WARN, "Fail to open multiple get merge iterator, ", K(ret));
          } else {
            main_iter_ = single_merge_;
          }
        }
        break;
      }
      case ObBatch::T_MULTI_GET: {
        if (NULL == get_merge_) {
          if (NULL == (buf = scan_param_->allocator_->alloc(sizeof(ObMultipleGetMerge)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
          } else {
            get_merge_ = new (buf) ObMultipleGetMerge();
            if (OB_FAIL(init_scan_iter(scan_param_->scan_flag_.is_index_back(), *get_merge_))) {
              STORAGE_LOG(WARN, "Fail to init get merge, ", K(ret));
            } else {
              use_fuse_row_cache_ = !get_merge_->is_read_memtable_only();
            }
          }
        }
        if (OB_SUCC(ret)) {
          main_table_ctx_.use_fuse_row_cache_ = use_fuse_row_cache_;
          if (OB_FAIL(get_merge_->open(*batch.rowkeys_))) {
            STORAGE_LOG(WARN, "Fail to open multiple get merge iterator, ", K(ret));
          } else {
            main_iter_ = get_merge_;
          }
        }
        break;
      }
      case ObBatch::T_SCAN: {
        if (NULL == scan_merge_) {
          if (OB_ISNULL(buf = scan_param_->allocator_->alloc(sizeof(ObMultipleScanMerge)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
          } else {
            scan_merge_ = new (buf) ObMultipleScanMerge();
            if (OB_FAIL(init_scan_iter(scan_param_->scan_flag_.is_index_back(), *scan_merge_))) {
              STORAGE_LOG(WARN, "Fail to init scan merge, ", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (scan_param_->sample_info_.is_block_sample()) {
            if (NULL == block_sample_iterator_) {
              if (OB_ISNULL(buf = scan_param_->allocator_->alloc(sizeof(ObBlockSampleIterator)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                STORAGE_LOG(WARN, "failed to allocate block_sample_iterator_", K(ret));
              } else {
                block_sample_iterator_ = new (buf) ObBlockSampleIterator(scan_param_->sample_info_);
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(block_sample_iterator_->open(*scan_merge_,
                      scan_param_->scan_flag_.is_index_back() ? index_table_ctx_ : main_table_ctx_,
                      *batch.range_,
                      get_table_param_,
                      scan_param_->scan_flag_.is_reverse_scan()))) {
                STORAGE_LOG(WARN, "failed to open block_sample_iterator_", K(ret));
              } else {
                main_iter_ = block_sample_iterator_;
              }
            }
          } else {
            if (OB_FAIL(scan_merge_->open(*batch.range_))) {
              STORAGE_LOG(WARN, "Fail to open multiple scan merge iterator, ", K(ret), K(*scan_param_));
            } else {
              main_iter_ = scan_merge_;
            }
          }
        }
        break;
      }
      case ObBatch::T_MULTI_SCAN: {
        if (NULL == multi_scan_merge_) {
          if (NULL == (buf = scan_param_->allocator_->alloc(sizeof(ObMultipleMultiScanMerge)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
          } else {
            multi_scan_merge_ = new (buf) ObMultipleMultiScanMerge();
            if (OB_FAIL(init_scan_iter(scan_param_->scan_flag_.is_index_back(), *multi_scan_merge_))) {
              STORAGE_LOG(WARN, "fail to init multi scan merge", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(multi_scan_merge_->open(*batch.ranges_))) {
            STORAGE_LOG(WARN, "Fail to open multiple multi_scan merge iterator, ", K(ret));
          } else {
            main_iter_ = multi_scan_merge_;
          }
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid batch type", K(ret), K(batch.type_));
      }
    }

    if (OB_SUCC(ret)) {
      if (scan_param_->sample_info_.is_row_sample()) {
        if (NULL == row_sample_iterator_) {
          if (OB_ISNULL(buf = scan_param_->allocator_->alloc(sizeof(ObRowSampleIterator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "failed to allocate memory", K(ret));
          } else {
            row_sample_iterator_ = new (buf) ObRowSampleIterator(scan_param_->sample_info_);
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(row_sample_iterator_->open(*main_iter_))) {
            STORAGE_LOG(WARN, "failed to open row_sample_iterator", K(ret));
          } else {
            main_iter_ = row_sample_iterator_;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (scan_param_->scan_flag_.is_index_back()) {
        if (NULL == index_merge_) {
          if (NULL == (buf = scan_param_->allocator_->alloc(sizeof(ObIndexMerge)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
          } else {
            index_merge_ = new (buf) ObIndexMerge();
            if (OB_FAIL(index_merge_->init(main_table_param_, index_table_param_, main_table_ctx_, get_table_param_))) {
              STORAGE_LOG(WARN, "Fail to init index merge, ", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          main_table_ctx_.use_fuse_row_cache_ = use_fuse_row_cache_;
          if (OB_FAIL(index_merge_->open(*main_iter_))) {
            STORAGE_LOG(WARN, "Fail to open index merge iterator, ", K(ret));
          } else {
            main_iter_ = index_merge_;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableScanStoreRowIterator::get_next_row(ObStoreRow*& cur_row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObTableScanStoreRowIterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(main_iter_)) {
    ret = OB_ITER_END;
  } else {
    if (OB_FAIL(main_iter_->get_next_row(cur_row))) {
      if (OB_ARRAY_BINDING_SWITCH_ITERATOR == ret) {
        ret = OB_ITER_END;
      } else if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next row", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
  } else if (OB_ITER_END != ret) {
    STORAGE_LOG(WARN,
        "Fail to get next row, ",
        K(ret),
        K(*scan_param_),
        K_(main_table_param),
        K_(index_table_param),
        KP(single_merge_),
        KP(get_merge_),
        KP(scan_merge_),
        KP(multi_scan_merge_),
        KP(index_merge_));
  }
  return ret;
}

int ObTableScanStoreRowIterator::switch_iterator(const int64_t range_array_idx)
{
  int ret = OB_SUCCESS;
  if (nullptr != index_merge_) {
    if (OB_FAIL(index_merge_->switch_iterator(range_array_idx))) {
      STORAGE_LOG(WARN, "fail to switch iterator", K(ret));
    }
  } else {
    if (nullptr != get_merge_) {
      if (OB_FAIL(get_merge_->switch_iterator(range_array_idx))) {
        STORAGE_LOG(WARN, "fail to switch iterator", K(ret));
      }
    } else if (nullptr != multi_scan_merge_) {
      if (OB_FAIL(multi_scan_merge_->switch_iterator(range_array_idx))) {
        STORAGE_LOG(WARN, "fail to switch iterator", K(ret));
      }
    } else if (nullptr != single_merge_) {
      if (OB_FAIL(single_merge_->switch_iterator(range_array_idx))) {
        STORAGE_LOG(WARN, "fail to switch iterator", K(ret));
      }
    } else if (nullptr != scan_merge_) {
      if (OB_FAIL(scan_merge_->switch_iterator(range_array_idx))) {
        STORAGE_LOG(WARN, "fail to switch iterator", K(ret));
      }
    }
    STORAGE_LOG(DEBUG, "switch iterator", K(range_array_idx));
  }
  return ret;
}

ObTableScanRangeArrayRowIterator::ObTableScanRangeArrayRowIterator()
    : is_inited_(false), row_iter_(NULL), cur_row_(NULL), curr_range_array_idx_(0), is_reverse_scan_(false)
{}

ObTableScanRangeArrayRowIterator::~ObTableScanRangeArrayRowIterator()
{}

int ObTableScanRangeArrayRowIterator::init(const bool is_reverse_scan, ObTableScanStoreRowIterator& iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTableScanRangeArrayRowIterator has been inited twice", K(ret));
  } else {
    is_reverse_scan_ = is_reverse_scan;
    row_iter_ = &iter;
    is_inited_ = true;
  }
  return ret;
}

int ObTableScanRangeArrayRowIterator::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObStoreRow* cur_row = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTableScanRangeArrayRowIterator has not been inited", K(ret));
  } else if (NULL != cur_row_) {
    cur_row = cur_row_;
  } else {
    if (OB_FAIL(row_iter_->get_next_row(cur_row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next row", K(ret));
      }
    } else if (OB_ISNULL(cur_row)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "curr row must not be NULL", K(ret));
    } else {
      cur_row_ = cur_row;
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t range_array_idx = cur_row->range_array_idx_;
    int comp_ret = static_cast<int32_t>(curr_range_array_idx_ - range_array_idx);
    comp_ret = is_reverse_scan_ ? -comp_ret : comp_ret;
    if (comp_ret > 0) {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN,
          "switch range array when iterator does not reach end is not supported",
          K(ret),
          K(curr_range_array_idx_),
          "curr_row_range_array_idx",
          range_array_idx,
          K_(is_reverse_scan));
    } else if (0 == comp_ret) {
      row = &(cur_row->row_val_);
      cur_row_ = NULL;
      STORAGE_LOG(DEBUG, "get next row", K(*row), K(*cur_row));
    } else if (comp_ret < 0) {
      // curr range is empty
      ret = OB_ITER_END;
      STORAGE_LOG(DEBUG, "get next row iter end", K(curr_range_array_idx_), K(range_array_idx));
    }
  }
  return ret;
}

void ObTableScanRangeArrayRowIterator::reset()
{
  is_inited_ = false;
  row_iter_ = NULL;
  cur_row_ = NULL;
  curr_range_array_idx_ = 0;
  is_reverse_scan_ = false;
}

int ObTableScanRangeArrayRowIterator::set_range_array_idx(const int64_t range_array_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_iter_->switch_iterator(range_array_idx))) {
    STORAGE_LOG(WARN, "fail to switch iterator", K(ret), K(range_array_idx));
  } else {
    curr_range_array_idx_ = range_array_idx;
  }
  return ret;
}
constexpr const char ObTableScanIterIterator::LABEL[];
constexpr const char ObTableScanIterator::LABEL[];

ObTableScanIterIterator::ObTableScanIterIterator()
    : ObNewIterIterator(ObNewIterIterator::ObTableScanIterIterator),
      store_row_iter_(),
      range_row_iter_(),
      range_array_cursor_(0),
      range_array_cnt_(0),
      is_reverse_scan_(false)
{}

ObTableScanIterIterator::~ObTableScanIterIterator()
{}

int ObTableScanIterIterator::get_next_range()
{
  int ret = OB_SUCCESS;
  if (range_array_cursor_ > range_array_cnt_ || range_array_cursor_ < -1) {
    ret = OB_ITER_END;
  } else {
    range_array_cursor_ = is_reverse_scan_ ? range_array_cursor_ - 1 : range_array_cursor_ + 1;
    ret = range_array_cursor_ >= range_array_cnt_ || range_array_cursor_ < 0 ? OB_ITER_END : OB_SUCCESS;
  }
  return ret;
}

int ObTableScanIterIterator::get_next_iter(common::ObNewRowIterator*& iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_range())) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next range", K(ret));
    }
  } else {
    range_row_iter_.set_range_array_idx(range_array_cursor_);
    iter = &range_row_iter_;
  }
  return ret;
}

int ObTableScanIterIterator::init(transaction::ObTransService& trans_service, const ObStoreCtx& ctx,
    ObTableScanParam& scan_param, ObPartitionStore& partition_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_row_iter_.init(trans_service, ctx, scan_param, partition_store))) {
    STORAGE_LOG(WARN, "fail to init table scan store row iterator", K(ret));
  } else if (OB_FAIL(range_row_iter_.init(scan_param.scan_flag_.is_reverse_scan(), store_row_iter_))) {
    STORAGE_LOG(WARN, "fail to init range row iterator", K(ret));
  } else {
    const int64_t range_array_cnt = scan_param.range_array_pos_.count();
    is_reverse_scan_ = scan_param.scan_flag_.is_reverse_scan();
    range_array_cnt_ = 0 == range_array_cnt ? 1 : range_array_cnt;  // zero range means whole range
    range_array_cursor_ = is_reverse_scan_ ? range_array_cnt_ : -1;
  }
  return ret;
}

int ObTableScanIterIterator::rescan(ObTableScanParam& scan_param)
{
  int ret = OB_SUCCESS;
  range_row_iter_.reset();
  if (OB_FAIL(store_row_iter_.rescan(scan_param.key_ranges_, scan_param.range_array_pos_))) {
    STORAGE_LOG(WARN, "fail to rescan", K(ret));
  } else if (OB_FAIL(range_row_iter_.init(scan_param.scan_flag_.is_reverse_scan(), store_row_iter_))) {
    STORAGE_LOG(WARN, "fail to init range row iterator", K(ret));
  } else {
    const int64_t range_array_cnt = scan_param.range_array_pos_.count();
    is_reverse_scan_ = scan_param.scan_flag_.is_reverse_scan();
    range_array_cnt_ = 0 == range_array_cnt ? 1 : range_array_cnt;  // zero range means whole range
    range_array_cursor_ = is_reverse_scan_ ? range_array_cnt_ : -1;
  }
  return ret;
}

void ObTableScanIterIterator::reset()
{
  store_row_iter_.reset();
  range_row_iter_.reset();
  range_array_cursor_ = 0;
  range_array_cnt_ = 0;
  is_reverse_scan_ = false;
}

ObTableScanIterator::ObTableScanIterator()
    : ObNewRowIterator(ObNewRowIterator::ObTableScanIterator), iter_(), row_iter_(NULL)
{}

ObTableScanIterator::~ObTableScanIterator()
{}

int ObTableScanIterator::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (NULL == row_iter_) {
    if (OB_FAIL(iter_.get_next_iter(row_iter_))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next iterator", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "table scan iterator reaches end");
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_iter_->get_next_row(row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next row", K(ret));
      } else {
        ret = OB_SUCCESS;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iter_.get_next_iter(row_iter_))) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "fail to get next iter", K(ret));
            } else {
              STORAGE_LOG(DEBUG, "table scan iterator reaches end");
            }
          } else if (OB_FAIL(row_iter_->get_next_row(row))) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "fail to get next row", K(ret));
            } else {
              STORAGE_LOG(DEBUG, "table scan row iterator reaches end");
              ret = OB_SUCCESS;
            }
          } else {
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableScanIterator::init(transaction::ObTransService& trans_service, const ObStoreCtx& ctx,
    ObTableScanParam& scan_param, ObPartitionStore& partition_store)
{
  return iter_.init(trans_service, ctx, scan_param, partition_store);
}

int ObTableScanIterator::rescan(ObTableScanParam& scan_param)
{
  row_iter_ = NULL;
  return iter_.rescan(scan_param);
}

void ObTableScanIterator::reset()
{
  iter_.reset();
  row_iter_ = NULL;
}

ObTableScanNewRowIterator::ObTableScanNewRowIterator(ObNewIterIterator& iter) : iter_(iter), row_iter_(NULL)
{}

ObTableScanNewRowIterator::~ObTableScanNewRowIterator()
{}

int ObTableScanNewRowIterator::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (NULL == row_iter_) {
    if (OB_FAIL(iter_.get_next_iter(row_iter_))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next iterator", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "table scan iterator reaches end");
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_iter_->get_next_row(row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next row", K(ret));
      } else {
        ret = OB_SUCCESS;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iter_.get_next_iter(row_iter_))) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "fail to get next iter", K(ret));
            } else {
              STORAGE_LOG(DEBUG, "table scan iterator reaches end");
            }
          } else if (OB_FAIL(row_iter_->get_next_row(row))) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "fail to get next row", K(ret));
            } else {
              STORAGE_LOG(DEBUG, "table scan row iterator reaches end");
              ret = OB_SUCCESS;
            }
          } else {
            break;
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
