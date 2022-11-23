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

#include "common/object/ob_obj_compare.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "ob_multiple_scan_merge.h"
#include "ob_table_scan_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_service.h"
#include <sys/resource.h>
#include <sys/time.h>
#include "ob_dml_param.h"
#include "storage/memtable/ob_memtable.h"
#include "ob_table_estimator.h"
#include "ob_index_sstable_estimator.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;

constexpr const char ObTableScanIterator::LABEL[];

ObTableScanIterator::ObTableScanIterator()
    : ObNewRowIterator(ObNewRowIterator::ObTableScanIterator),
      is_inited_(false),
      single_merge_(NULL),
      get_merge_(NULL),
      scan_merge_(NULL),
      multi_scan_merge_(NULL),
      row_sample_iterator_(NULL),
      block_sample_iterator_(NULL),
      main_table_param_(),
      main_table_ctx_(),
      get_table_param_(),
      ctx_guard_(),
      scan_param_(NULL),
      table_scan_range_(),
      main_iter_(NULL)
{
}

ObTableScanIterator::~ObTableScanIterator()
{
  reset();
}


void ObTableScanIterator::reset()
{
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
  if (NULL != row_sample_iterator_) {
    row_sample_iterator_->~ObRowSampleIterator();
    row_sample_iterator_ = NULL;
  }
  if (NULL != block_sample_iterator_) {
    block_sample_iterator_->~ObBlockSampleIterator();
    block_sample_iterator_ = NULL;
  }
  main_table_param_.reset();
  main_table_ctx_.reset();
  get_table_param_.reset();

  ctx_guard_.reset();
  scan_param_ = NULL;
  table_scan_range_.reset();
  main_iter_ = NULL;
  is_inited_ = false;
}

void ObTableScanIterator::reuse_row_iters()
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
  if (NULL != row_sample_iterator_) {
    row_sample_iterator_->reuse();
  }
  if (NULL != block_sample_iterator_) {
    block_sample_iterator_->reuse();
  }
}

int ObTableScanIterator::prepare_table_param(const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (nullptr == scan_param_ || nullptr == scan_param_->table_param_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(scan_param_));
  } else if (OB_FAIL(main_table_param_.init(*scan_param_, tablet_handle))) {
    STORAGE_LOG(WARN, "failed to init main table param", K(ret));
  }
  return ret;
}

int ObTableScanIterator::prepare_table_context()
{
  int ret = OB_SUCCESS;
  if (nullptr == scan_param_ || nullptr == scan_param_->table_param_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(scan_param_));
  } else {
    ObVersionRange trans_version_range;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.base_version_ = 0;
    trans_version_range.snapshot_version_ = ctx_guard_.get_store_ctx().mvcc_acc_ctx_.get_snapshot_version();
    if (OB_UNLIKELY(!trans_version_range.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "trans version range is not valid", K(ret), K(trans_version_range));
    } else if (OB_FAIL(main_table_ctx_.init(*scan_param_, ctx_guard_.get_store_ctx(), trans_version_range))) {
      STORAGE_LOG(WARN, "failed to init main table ctx", K(ret));
    }
  }
  return ret;
}

int ObTableScanIterator::switch_scan_param(ObMultipleMerge &iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iter.switch_param(main_table_param_, main_table_ctx_, get_table_param_))) {
    STORAGE_LOG(WARN, "Failed to switch pararmeter", K(ret), K(main_table_param_));
  } else if (!scan_param_->sample_info_.is_no_sample()
      && SampleInfo::SAMPLE_INCR_DATA == scan_param_->sample_info_.scope_) {
    iter.disable_fill_default();
    iter.disable_output_row_with_nop();
  }
  return ret;
}

void ObTableScanIterator::reuse()
{
  table_scan_range_.reset();
  main_iter_ = NULL;
  reuse_row_iters();
  main_table_ctx_.reuse();
}

void ObTableScanIterator::reset_for_switch()
{
  reuse();
  main_table_param_.reset();
  get_table_param_.reset();
  ctx_guard_.reset();
  scan_param_ = NULL;
}

int ObTableScanIterator::rescan(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObTableScanStoreRowIterator has not been inited, ", K(ret));
  } else if (&scan_param_->key_ranges_ != &scan_param.key_ranges_
              || &scan_param_->range_array_pos_ != &scan_param.range_array_pos_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "scan_param is not the same", K(ret), K(scan_param_), K(&scan_param));
  } else {
    STORAGE_LOG(DEBUG, "table scan iterate rescan", K_(is_inited), K(scan_param_));
    // there's no need to reset main_table_param_
    // scan_param only reset query range fields in ObTableScan::rt_rescan()
    if (OB_FAIL(table_scan_range_.init(*scan_param_))) {
      STORAGE_LOG(WARN, "Failed to init table scan range", K(ret));
    } else if (OB_FAIL(prepare_table_context())) {
      STORAGE_LOG(WARN, "fail to prepare table context", K(ret));
    } else if (OB_FAIL(open_iter())) {
      STORAGE_LOG(WARN, "fail to open iter", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "Success to rescan ObTableScanIterator", K(scan_param.key_ranges_));
    }
  }
  return ret;
}

int ObTableScanIterator::init(ObTableScanParam &scan_param, const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &store_ctx = ctx_guard_.get_store_ctx();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObTableScanIterator has been inited, ", K(ret), K(*this));
  } else if (OB_UNLIKELY(!store_ctx.is_valid())
          || OB_UNLIKELY(!scan_param.is_valid())
          || OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init table scan iter", K(ret), K(store_ctx), K(scan_param),
        K(tablet_handle));
  } else if (OB_FAIL(table_scan_range_.init(scan_param))) {
    STORAGE_LOG(WARN, "Failed to init table scan range", K(ret), K(scan_param));
  } else {
    scan_param_ = &scan_param;
    get_table_param_.tablet_iter_.tablet_handle_ = tablet_handle;
    if (OB_FAIL(prepare_table_param(tablet_handle))) {
      STORAGE_LOG(WARN, "Fail to prepare table param, ", K(ret));
    } else if (OB_FAIL(prepare_table_context())) {
      STORAGE_LOG(WARN, "Fail to prepare table ctx, ", K(ret));
    } else if (OB_FAIL(open_iter())) {
      STORAGE_LOG(WARN, "fail to open iter", K(ret), K(*this));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableScanIterator::switch_param(ObTableScanParam &scan_param, const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &store_ctx = ctx_guard_.get_store_ctx();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K(*this));
  } else if (OB_UNLIKELY(!store_ctx.is_valid())
          || OB_UNLIKELY(!scan_param.is_valid()
          || OB_UNLIKELY(!tablet_handle.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(store_ctx), K(scan_param), K(tablet_handle));
  } else if (OB_FAIL(table_scan_range_.init(scan_param))) {
    STORAGE_LOG(WARN, "Failed to init table scan range", K(ret), K(scan_param));
  } else {
    scan_param_ = &scan_param;
    get_table_param_.tablet_iter_.tablet_handle_ = tablet_handle;
    if (OB_FAIL(prepare_table_param(tablet_handle))) {
      STORAGE_LOG(WARN, "Fail to prepare table param, ", K(ret));
    } else if (OB_FAIL(prepare_table_context())) {
      STORAGE_LOG(WARN, "Fail to prepare table ctx, ", K(ret));
    } else if (OB_FAIL(switch_param_for_iter())) {
      STORAGE_LOG(WARN, "Failed to switch param for iter", K(ret), K(*this));
    } else if (OB_FAIL(open_iter())) {
      STORAGE_LOG(WARN, "fail to open iter", K(ret), K(*this));
    } else {
      is_inited_ = true;
    }
  }
  STORAGE_LOG(TRACE, "switch param", K(ret), K(scan_param));
  return ret;
}

int ObTableScanIterator::switch_param_for_iter()
{
  int ret = OB_SUCCESS;
  get_table_param_.frozen_version_ = scan_param_->frozen_version_;
  get_table_param_.sample_info_ = scan_param_->sample_info_;
  if (NULL != single_merge_) {
    if (OB_FAIL(switch_scan_param(*single_merge_))) {
      STORAGE_LOG(WARN, "Fail to switch param for single merge, ", K(ret), KP_(single_merge), KPC_(single_merge));
    }
  }
  if (OB_SUCC(ret) && NULL != get_merge_) {
    if (OB_FAIL(switch_scan_param(*get_merge_))) {
      STORAGE_LOG(WARN, "Fail to switch param for get merge, ", K(ret), KP_(get_merge), KPC_(get_merge));
    }
  }
  if (OB_SUCC(ret) && NULL != scan_merge_) {
    if (OB_FAIL(switch_scan_param(*scan_merge_))) {
      STORAGE_LOG(WARN, "Fail to switch param for scan merge, ", K(ret), KP_(scan_merge), KPC_(scan_merge));
    }
  }
  if (OB_SUCC(ret) && NULL != multi_scan_merge_) {
    if (OB_FAIL(switch_scan_param(*multi_scan_merge_))) {
      STORAGE_LOG(WARN, "Fail to switch param for multi scan merge, ", K(ret), KP_(multi_scan_merge), KPC_(multi_scan_merge));
    }
  }
  return ret;
}

template<typename T>
int ObTableScanIterator::init_scan_iter(T *&iter)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_ISNULL(buf = scan_param_->allocator_->alloc(sizeof(T)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory", K(ret));
  } else {
    iter = new (buf) T();
    if (OB_FAIL(iter->init(main_table_param_, main_table_ctx_, get_table_param_))) {
      STORAGE_LOG(WARN, "Failed to init multiple merge", K(ret));
    } else if (!scan_param_->sample_info_.is_no_sample()
        && SampleInfo::SAMPLE_INCR_DATA == scan_param_->sample_info_.scope_) {
      iter->disable_fill_default();
      iter->disable_output_row_with_nop();
    }
    if (OB_FAIL(ret)) {
      iter->~T();
      scan_param_->allocator_->free(iter);
      iter = nullptr;
    }
  }

  return ret;
}


#define INIT_AND_OPEN_ITER(ITER_PTR, RANGE, USE_FUSE_CACHE)              \
do {                                                                     \
  if (nullptr == ITER_PTR && OB_FAIL(init_scan_iter(ITER_PTR))) {        \
    STORAGE_LOG(WARN, "Failed to init single merge", K(ret));            \
  } else {                                                               \
    main_table_ctx_.use_fuse_row_cache_ = USE_FUSE_CACHE;                \
    if (OB_FAIL(ITER_PTR->open(RANGE))) {                                \
      STORAGE_LOG(WARN, "Fail to open multiple merge iterator", K(ret)); \
    } else {                                                             \
      main_iter_ = ITER_PTR;                                             \
    }                                                                    \
  }                                                                      \
} while(0)

int ObTableScanIterator::open_iter()
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_UNLIKELY(!table_scan_range_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error for invalid table scan range", K(ret), K(table_scan_range_));
  } else if (table_scan_range_.is_empty()) {
    //ret = OB_ITER_END;
  } else {
    bool retire_to_row_sample = false;
    get_table_param_.frozen_version_ = scan_param_->frozen_version_;
    get_table_param_.sample_info_ = scan_param_->sample_info_;
    if (table_scan_range_.is_get()) {
      if (table_scan_range_.get_rowkeys().count() == 1) {
        INIT_AND_OPEN_ITER(single_merge_, table_scan_range_.get_rowkeys().at(0), true);
        if (OB_SUCC(ret)) {
          main_table_ctx_.use_fuse_row_cache_ = !single_merge_->is_read_memtable_only();
        }
      } else {
        INIT_AND_OPEN_ITER(get_merge_, table_scan_range_.get_rowkeys(), false);
      }
    } else {
      if (table_scan_range_.get_ranges().count() == 1) {
        if (scan_param_->sample_info_.is_block_sample()) {
          if (nullptr == scan_merge_ && OB_FAIL(init_scan_iter(scan_merge_))) {
            STORAGE_LOG(WARN, "Failed to init scanmerge", K(ret));
          } else if (OB_FAIL(get_table_param_.tablet_iter_.tablet_handle_.get_obj()->get_read_tables(
                        main_table_ctx_.store_ctx_->mvcc_acc_ctx_.get_snapshot_version(),
                        get_table_param_.tablet_iter_,
                        false /*allow_not_ready*/ ))) {
            STORAGE_LOG(WARN, "Fail to read tables", K(ret));
          } else if (OB_FAIL(can_retire_to_row_sample(retire_to_row_sample))) {
            STORAGE_LOG(WARN, "Fail to try to retire to row sample", K(ret));
          } else if (!retire_to_row_sample) {
            if (nullptr == block_sample_iterator_) {
              if (OB_ISNULL(buf = scan_param_->allocator_->alloc(sizeof(ObBlockSampleIterator)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                STORAGE_LOG(WARN, "Fail to allocate memory", K(ret));
              } else {
                block_sample_iterator_ = new (buf) ObBlockSampleIterator (scan_param_->sample_info_);
              }
            }

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(block_sample_iterator_->open(*scan_merge_,
                                                            main_table_ctx_,
                                                            table_scan_range_.get_ranges().at(0),
                                                            get_table_param_,
                                                            scan_param_->scan_flag_.is_reverse_scan()))) {
                  STORAGE_LOG(WARN, "failed to open block_sample_iterator_", K(ret));
            } else {
              main_iter_ = block_sample_iterator_;
              main_table_ctx_.use_fuse_row_cache_ = false;
            }
          }
        } else {
          INIT_AND_OPEN_ITER(scan_merge_, table_scan_range_.get_ranges().at(0), false);
        }
      } else {
        INIT_AND_OPEN_ITER(multi_scan_merge_, table_scan_range_.get_ranges(), false);
      }
    }

    if (OB_SUCC(ret)) {
      if (scan_param_->sample_info_.is_row_sample() || retire_to_row_sample) {
        if (!retire_to_row_sample) {
        } else if (OB_ISNULL(scan_merge_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null scan merge", K(ret), KP(scan_merge_));
        } else if (OB_FAIL(scan_merge_->open(table_scan_range_.get_ranges().at(0)))) {
          STORAGE_LOG(WARN, "Fail to open scan merge iterator", K(ret));
        } else {
          main_table_ctx_.use_fuse_row_cache_ = false;
          main_iter_ = scan_merge_;
        }
        if (OB_FAIL(ret)) {
        } else if (nullptr == row_sample_iterator_) {
          if (OB_ISNULL(buf = scan_param_->allocator_->alloc(sizeof(ObRowSampleIterator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory", K(ret));
          } else {
            row_sample_iterator_ = new (buf) ObRowSampleIterator (scan_param_->sample_info_);
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(row_sample_iterator_->open(*main_iter_))) {
          STORAGE_LOG(WARN, "failed to open row_sample_iterator", K(ret));
        } else {
          main_iter_ = row_sample_iterator_;
        }
      }
    }
    if (OB_SUCC(ret)) {
      table_scan_range_.set_empty();
    }
  }
  STORAGE_LOG(DEBUG, "chaser debug open iter", K(ret), K(table_scan_range_));
  return ret;
}

#undef INIT_AND_OPEN_ITER

int ObTableScanIterator::can_retire_to_row_sample(bool &retire)
{
  int ret = OB_SUCCESS;

  retire = false;
  if (get_table_param_.is_valid()) {
    int64_t memtable_row_count = 0;
    int64_t sstable_row_count = 0;
    common::ObSEArray<ObITable*, 4> memtables;
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      if (OB_FAIL(get_table_param_.tablet_iter_.table_iter_.get_next(table))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "Fail to get next table iter", K(ret), K(get_table_param_.tablet_iter_.table_iter_));
        }
      } else if (table->is_memtable()) {
        memtables.push_back(table);
      } else if (table->is_sstable()) {
        sstable_row_count += static_cast<ObSSTable*>(table)->get_meta().get_basic_meta().row_count_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(get_table_param_.tablet_iter_.table_iter_.resume())) {
    } else if (memtables.count() > 0) {
      ObPartitionEst batch_est;
      ObSEArray<ObEstRowCountRecord, MAX_SSTABLE_CNT_IN_STORAGE> est_records;
      ObTableEstimateBaseInput base_input(scan_param_->scan_flag_, memtables.at(0)->get_key().tablet_id_.id(), memtables, get_table_param_.tablet_iter_.tablet_handle_);
      if (OB_FAIL(ObTableEstimator::estimate_row_count_for_scan(base_input, table_scan_range_.get_ranges(), batch_est, est_records))) {
        STORAGE_LOG(WARN, "Failed to estimate row count for scan", K(ret), KPC(scan_param_), K(table_scan_range_));
      } else {
        retire = sstable_row_count < batch_est.physical_row_count_;
      }
    }
  }

  return ret;
}

int ObTableScanIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObDatumRow *store_row = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObTableScanStoreRowIterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(main_iter_)) {
    ret = OB_ITER_END;
  } else {
    if (scan_param_->op_ != nullptr) {
      scan_param_->op_->clear_datum_eval_flag();
    }
    if (OB_FAIL(main_iter_->get_next_row(store_row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Fail to get next row, ", K(ret), KPC_(scan_param), K_(main_table_param),
            KP(single_merge_), KP(get_merge_), KP(scan_merge_), KP(multi_scan_merge_));
      }
    } else {
      row = &(store_row->get_new_row());
    }
  }
  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(check_txn_status_if_read_uncommitted_())) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObTableScanIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObTableScanStoreRowIterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(main_iter_)) {
    ret = OB_ITER_END;
  } else {
    if (scan_param_->op_ != nullptr) {
      scan_param_->op_->clear_datum_eval_flag();
    }
    if (OB_FAIL(main_iter_->get_next_rows(count, capacity))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Fail to get next row, ", K(ret), K(*scan_param_), K_(main_table_param),
            KP(single_merge_), KP(get_merge_), KP(scan_merge_), KP(multi_scan_merge_));
      }
    }
  }
  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(check_txn_status_if_read_uncommitted_())) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObTableScanIterator::check_txn_status_if_read_uncommitted_()
{
  int ret = OB_SUCCESS;
  auto &acc_ctx = ctx_guard_.get_store_ctx().mvcc_acc_ctx_;
  auto &snapshot = acc_ctx.snapshot_;
  if (snapshot.tx_id_.is_valid() && acc_ctx.mem_ctx_) {
    if (acc_ctx.mem_ctx_->is_tx_rollbacked()) {
      // The txn has been killed during normal processing. So we return
      // OB_TRANS_KILLED to prompt this abnormal state.
      ret = OB_TRANS_KILLED;
      STORAGE_LOG(WARN, "txn termianted when table scan", K(ret), K(acc_ctx));
    }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
