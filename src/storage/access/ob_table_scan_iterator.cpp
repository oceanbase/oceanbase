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

#include <sys/resource.h>
#include <sys/time.h>
#include "common/object/ob_obj_compare.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/access/ob_sample_iter_helper.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tablet/ob_tablet.h"

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
      current_iter_type_(T_INVALID_ITER_TYPE),
      single_merge_(NULL),
      get_merge_(NULL),
      scan_merge_(NULL),
      multi_scan_merge_(NULL),
      skip_scan_merge_(NULL),
      memtable_row_sample_iterator_(NULL),
      row_sample_iterator_(NULL),
      block_sample_iterator_(NULL),
      // i_sample_iter_(NULL),
      main_table_param_(),
      main_table_ctx_(),
      get_table_param_(),
      ctx_guard_(),
      scan_param_(NULL),
      table_scan_range_(),
      main_iter_(NULL),
      sample_ranges_(),
      cached_iter_node_(NULL),
      cached_iter_(NULL)
{
}

ObTableScanIterator::~ObTableScanIterator()
{
  reset();
}


void ObTableScanIterator::reset()
{
  reset_scan_iter(single_merge_);
  reset_scan_iter(get_merge_);
  reset_scan_iter(scan_merge_);
  reset_scan_iter(multi_scan_merge_);
  reset_scan_iter(skip_scan_merge_);
  reset_scan_iter(memtable_row_sample_iterator_);
  reset_scan_iter(block_sample_iterator_);
  // reset_scan_iter(i_sample_iter_);
  if (nullptr != cached_iter_node_) {
    ObGlobalIteratorPool *iter_pool = MTL(ObGlobalIteratorPool*);
    iter_pool->release(cached_iter_node_);
  }

  main_table_param_.reset();
  main_table_ctx_.reset();
  get_table_param_.reset();

  ctx_guard_.reset();
  scan_param_ = NULL;
  table_scan_range_.reset();
  main_iter_ = NULL;
  sample_ranges_.reset();
  cached_iter_ = NULL;
  current_iter_type_ = T_INVALID_ITER_TYPE;
  is_inited_ = false;
}

template<typename T>
void ObTableScanIterator::reset_scan_iter(T *&iter)
{
  if (nullptr != iter) {
    if (nullptr == cached_iter_node_) {
      iter->~T();
    }
    iter = NULL;
  }
}

void ObTableScanIterator::reuse_row_iters()
{
#define REUSE_SCAN_ITER(iter) \
  if (NULL != iter) {         \
    iter->reuse();            \
  }                           \

  REUSE_SCAN_ITER(single_merge_);
  REUSE_SCAN_ITER(get_merge_);
  REUSE_SCAN_ITER(scan_merge_);
  REUSE_SCAN_ITER(multi_scan_merge_);
  REUSE_SCAN_ITER(skip_scan_merge_);
  REUSE_SCAN_ITER(memtable_row_sample_iterator_);
  REUSE_SCAN_ITER(block_sample_iterator_);
  // REUSE_SCAN_ITER(i_sample_iter_);

#undef REUSE_SCAN_ITER
}

int ObTableScanIterator::prepare_table_param(const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObTablet *tablet = tablet_handle.get_obj();
  if (OB_ISNULL(scan_param_)
      || OB_ISNULL(scan_param_->table_param_)
      || OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(scan_param_), K(tablet_handle));
  } else if (OB_FAIL(main_table_param_.init(*scan_param_, &tablet_handle))) {
    STORAGE_LOG(WARN, "failed to init main table param", K(ret));
  } else if (nullptr != cached_iter_node_) {
    main_table_param_.set_use_global_iter_pool();
    main_table_param_.iter_param_.set_use_stmt_iter_pool();
    STORAGE_LOG(TRACE, "use global iter pool", K(main_table_param_));
  }
  return ret;
}

bool ObTableScanIterator::can_use_global_iter_pool(const ObQRIterType iter_type) const
{
  bool use_pool = false;
  if (main_table_param_.iter_param_.tablet_id_.is_inner_tablet()) {
  } else if (scan_param_->use_index_skip_scan() ||
             !scan_param_->sample_info_.is_no_sample() ||
             main_table_param_.iter_param_.is_use_column_store() ||
             main_table_param_.iter_param_.enable_pd_aggregate() ||
             main_table_param_.iter_param_.enable_pd_group_by() ||
             main_table_param_.iter_param_.has_lob_column_out_) {
  } else {
    const int64_t table_cnt = get_table_param_.tablet_iter_.table_iter()->count();
    const int64_t col_cnt = MAX(scan_param_->table_param_->get_read_info().get_schema_column_count(),
                                get_table_param_.tablet_iter_.get_tablet()->get_rowkey_read_info().get_schema_column_count());
    ObGlobalIteratorPool *iter_pool = MTL(ObGlobalIteratorPool*);
    if (OB_NOT_NULL(iter_pool)) {
       use_pool = iter_pool->can_use_iter_pool(table_cnt, col_cnt, iter_type);
    }
  }
  return use_pool;
}

int ObTableScanIterator::prepare_cached_iter_node()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != cached_iter_node_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected not null cached iter node", K(ret), KP(cached_iter_node_));
  } else if (can_use_global_iter_pool(current_iter_type_)) {
    ObGlobalIteratorPool *iter_pool = MTL(ObGlobalIteratorPool*);
    if (OB_FAIL(iter_pool->get(current_iter_type_, cached_iter_node_))) {
      STORAGE_LOG(WARN, "Failed to get from iter pool", K(ret));
    } else if (nullptr != cached_iter_node_) {
      main_table_param_.set_use_global_iter_pool();
      main_table_param_.iter_param_.set_use_stmt_iter_pool();
      STORAGE_LOG(TRACE, "use global iter pool", K(current_iter_type_), K(main_table_param_));
    }
  }
  return ret;
}

void ObTableScanIterator::try_release_cached_iter_node(const ObQRIterType rescan_iter_type)
{
  if (nullptr != cached_iter_node_) {
    const int64_t table_cnt = get_table_param_.tablet_iter_.table_iter()->count();
    const int64_t col_cnt = get_table_param_.tablet_iter_.get_tablet()->get_rowkey_read_info().get_request_count();
    bool use_pool = current_iter_type_ == rescan_iter_type &&
                    MTL(ObGlobalIteratorPool*)->can_use_iter_pool(table_cnt, col_cnt, rescan_iter_type);
    if (!use_pool) {
      STORAGE_LOG(INFO, "iter type/table cnt/col cnt is changed in rescan, disable global cache", KPC(cached_iter_node_),
        K(table_cnt), K(col_cnt), K(current_iter_type_), K(rescan_iter_type), KP(cached_iter_),
        KP(single_merge_), KP(get_merge_), KP(scan_merge_), KP(multi_scan_merge_));
      main_table_param_.diable_use_global_iter_pool();
      main_table_ctx_.reset_cached_iter_node();
      MTL(ObGlobalIteratorPool*)->release(cached_iter_node_);
      cached_iter_node_ = nullptr;
      current_iter_type_ = T_INVALID_ITER_TYPE;
      if (nullptr != cached_iter_) {
        *cached_iter_ = nullptr;
      }
    }
  }
}

int ObTableScanIterator::prepare_table_context()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->table_param_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(scan_param_));
  } else {
    ObVersionRange trans_version_range;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.base_version_ = 0;
    trans_version_range.snapshot_version_ = ctx_guard_.get_store_ctx().mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
    if (OB_UNLIKELY(!trans_version_range.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "trans version range is not valid", K(ret), K(trans_version_range));
    } else if (OB_FAIL(main_table_ctx_.init(*scan_param_, ctx_guard_.get_store_ctx(), trans_version_range, cached_iter_node_))) {
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
  sample_ranges_.reuse();
}

void ObTableScanIterator::reset_for_switch()
{
  reuse();
  main_table_param_.reset();
  main_table_ctx_.reuse();
  get_table_param_.reset();
  ctx_guard_.reset();
  sample_ranges_.reset();
  scan_param_ = NULL;
}

int ObTableScanIterator::rescan(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  ACTIVE_GLOBAL_ITERATOR_GUARD(ret, cached_iter_node_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObTableScanStoreRowIterator has not been inited, ", K(ret));
  } else if (&scan_param_->key_ranges_ != &scan_param.key_ranges_
              || &scan_param_->range_array_pos_ != &scan_param.range_array_pos_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "scan_param is not the same", K(ret), K(scan_param_), K(&scan_param));
  } else {
    STORAGE_LOG(DEBUG, "table scan iterate rescan", K_(is_inited), K(scan_param_));
    // there's no need to reset main_table_param_ and table_ctx
    // scan_param only reset query range fields in ObTableScan::rt_rescan()
    ObQRIterType rescan_iter_type = T_INVALID_ITER_TYPE;
    if (OB_FAIL(main_table_ctx_.rescan_reuse(scan_param))) {
      STORAGE_LOG(WARN, "Failed to rescan reuse", K(ret));
    } else if (OB_FAIL(table_scan_range_.init(*scan_param_))) {
      STORAGE_LOG(WARN, "Failed to init table scan range", K(ret));
    } else if (OB_FAIL(rescan_for_iter())) {
      STORAGE_LOG(WARN, "Failed to switch param for iter", K(ret), K(*this));
    } else if (OB_FAIL(table_scan_range_.get_query_iter_type(rescan_iter_type))) {
      STORAGE_LOG(WARN, "Failed to get query iter type", K(ret));
    } else if (FALSE_IT(try_release_cached_iter_node(rescan_iter_type))) {
    } else if (OB_FAIL(open_iter())) {
      STORAGE_LOG(WARN, "fail to open iter", K(ret), KPC(cached_iter_node_));
    } else {
      STORAGE_LOG(DEBUG, "Success to rescan ObTableScanIterator", K(scan_param.key_ranges_));
    }
  }
  return ret;
}

int ObTableScanIterator::init(ObTableScanParam &scan_param, const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ACTIVE_GLOBAL_ITERATOR_GUARD(ret, cached_iter_node_);
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
  } else if (OB_FAIL(table_scan_range_.get_query_iter_type(current_iter_type_))) {
    STORAGE_LOG(WARN, "Failed to get query iter type", K(ret));
  } else {
    scan_param_ = &scan_param;
    if (OB_FAIL(get_table_param_.tablet_iter_.set_tablet_handle(tablet_handle))) {
      STORAGE_LOG(WARN, "Fail to set tablet handle to iter", K(ret));
    } else if (OB_FAIL(prepare_table_param(tablet_handle))) {
      STORAGE_LOG(WARN, "Fail to prepare table param, ", K(ret));
    } else if (OB_FAIL(prepare_cached_iter_node())) {
      STORAGE_LOG(WARN, "Fail to prepare cached iter node", K(ret));
    } else if (OB_FAIL(prepare_table_context())) {
      STORAGE_LOG(WARN, "Fail to prepare table ctx, ", K(ret));
    } else if (OB_FAIL(open_iter())) {
      STORAGE_LOG(WARN, "fail to open iter", K(ret), KPC(cached_iter_node_), K(*this));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableScanIterator::switch_param(ObTableScanParam &scan_param, const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ACTIVE_GLOBAL_ITERATOR_GUARD(ret, cached_iter_node_);
  ObStoreCtx &store_ctx = ctx_guard_.get_store_ctx();
  ObQRIterType rescan_iter_type = T_INVALID_ITER_TYPE;
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
  } else if (OB_FAIL(table_scan_range_.get_query_iter_type(rescan_iter_type))) {
    STORAGE_LOG(WARN, "Failed to get query iter type", K(ret));
  } else {
    scan_param_ = &scan_param;
    if (OB_FAIL(get_table_param_.tablet_iter_.set_tablet_handle(tablet_handle))) {
      STORAGE_LOG(WARN, "Fail to set tablet handle to iter", K(ret));
    } else if (FALSE_IT(try_release_cached_iter_node(rescan_iter_type))) {
    } else if (OB_FAIL(prepare_table_param(tablet_handle))) {
      STORAGE_LOG(WARN, "Fail to prepare table param, ", K(ret));
    } else if (OB_FAIL(prepare_table_context())) {
      STORAGE_LOG(WARN, "Fail to prepare table ctx, ", K(ret));
    } else if (OB_FAIL(switch_param_for_iter())) {
      STORAGE_LOG(WARN, "Failed to switch param for iter", K(ret), K(*this));
    } else if (OB_FAIL(open_iter())) {
      STORAGE_LOG(WARN, "fail to open iter", K(ret), KPC(cached_iter_node_), K(*this));
    } else {
      is_inited_ = true;
    }
  }
  STORAGE_LOG(TRACE, "switch param", K(ret), K(scan_param));
  return ret;
}

int ObTableScanIterator::rescan_for_iter()
{
#define RESET_NOT_REFRESHED_ITER(refreshed_iter, iter) \
  if (refreshed_iter != (void*)iter) {                 \
    reset_scan_iter(iter);                             \
  }                                                    \

  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == get_table_param_.refreshed_merge_)) {
    // do nothing
  } else {
    RESET_NOT_REFRESHED_ITER(get_table_param_.refreshed_merge_, single_merge_);
    RESET_NOT_REFRESHED_ITER(get_table_param_.refreshed_merge_, get_merge_);
    RESET_NOT_REFRESHED_ITER(get_table_param_.refreshed_merge_, scan_merge_);
    RESET_NOT_REFRESHED_ITER(get_table_param_.refreshed_merge_, multi_scan_merge_);
    RESET_NOT_REFRESHED_ITER(get_table_param_.refreshed_merge_, skip_scan_merge_);
    RESET_NOT_REFRESHED_ITER(get_table_param_.refreshed_merge_, memtable_row_sample_iterator_);
    RESET_NOT_REFRESHED_ITER(get_table_param_.refreshed_merge_, block_sample_iterator_);
    get_table_param_.refreshed_merge_ = nullptr;
  }
#undef RESET_NOT_REFRESHED_ITER
  return ret;
}

int ObTableScanIterator::switch_param_for_iter()
{
#define SWITCH_PARAM_FOR_ITER(iter, ret)                                        \
  if (OB_SUCC(ret) && NULL != iter) {                                           \
    if (OB_FAIL(switch_scan_param(*iter))) {                                    \
      STORAGE_LOG(WARN, "Fail to switch param, ", K(ret), KP(iter), KPC(iter)); \
    }                                                                           \
  }                                                                             \

  int ret = OB_SUCCESS;
  get_table_param_.frozen_version_ = scan_param_->frozen_version_;
  get_table_param_.sample_info_ = scan_param_->sample_info_;
  SWITCH_PARAM_FOR_ITER(single_merge_, ret);
  SWITCH_PARAM_FOR_ITER(get_merge_, ret);
  SWITCH_PARAM_FOR_ITER(scan_merge_, ret);
  SWITCH_PARAM_FOR_ITER(multi_scan_merge_, ret);
  SWITCH_PARAM_FOR_ITER(skip_scan_merge_, ret);
#undef SWITCH_PARAM_FOR_ITER
  return ret;
}

template<typename T>
int ObTableScanIterator::init_scan_iter(T *&iter)
{
  int ret = OB_SUCCESS;
  ObQueryRowIterator *cached_iter = nullptr == cached_iter_node_ ? nullptr : cached_iter_node_->get_iter();
  if (OB_NOT_NULL(cached_iter)) {
    if (OB_UNLIKELY(cached_iter->get_type() != current_iter_type_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected cached iter type", K(ret), K(cached_iter->get_type()), K(current_iter_type_));
    } else {
      iter = static_cast<T*>(cached_iter);
      cached_iter_ = reinterpret_cast<ObQueryRowIterator**>(&iter);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (nullptr == iter) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = main_table_ctx_.get_long_life_allocator()->alloc(sizeof(T)))) {
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
        main_table_ctx_.get_long_life_allocator()->free(iter);
        iter = nullptr;
      } else if (nullptr != cached_iter_node_) {
        cached_iter_node_->set_iter(iter);
        cached_iter_ = reinterpret_cast<ObQueryRowIterator**>(&iter);
      }
    }
  } else if (OB_FAIL(iter->switch_table(main_table_param_, main_table_ctx_, get_table_param_))) {
    STORAGE_LOG(WARN, "Failed to switch table", K(ret), K(main_table_param_));
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

#define INIT_AND_OPEN_SKIP_SCAN_ITER(ITER_PTR, RANGE, SUFFIX_RANGE, USE_FUSE_CACHE) \
do {                                                                                \
  STORAGE_LOG(TRACE, "skip scan", K(main_table_param_), K(RANGE), K(SUFFIX_RANGE)); \
  if (nullptr == ITER_PTR && OB_FAIL(init_scan_iter(ITER_PTR))) {                   \
    STORAGE_LOG(WARN, "Failed to init single merge", K(ret));                       \
  } else {                                                                          \
    main_table_ctx_.use_fuse_row_cache_ = USE_FUSE_CACHE;                           \
    if (OB_FAIL(ITER_PTR->open(RANGE, SUFFIX_RANGE))) {                             \
      STORAGE_LOG(WARN, "Fail to open multiple merge iterator", K(ret));            \
    } else {                                                                        \
      main_iter_ = ITER_PTR;                                                        \
    }                                                                               \
  }                                                                                 \
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
    get_table_param_.frozen_version_ = scan_param_->frozen_version_;
    get_table_param_.sample_info_ = scan_param_->sample_info_;
    if (table_scan_range_.is_get()) {
      if (OB_FAIL(init_and_open_get_merge_iter_())) {
        STORAGE_LOG(WARN, "init and open get merge iterator failed", KR(ret));
      }
    } else if (table_scan_range_.is_scan()) {
      if (OB_FAIL(init_and_open_scan_merge_iter_())) {
        STORAGE_LOG(WARN, "init and open scan merge iterator failed", KR(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "invalid table scan range", KR(ret), K(table_scan_range_));
    }

    if (OB_SUCC(ret)) {
      table_scan_range_.set_empty();
    }
  }
  STORAGE_LOG(DEBUG, "chaser debug open iter", K(ret), K(table_scan_range_));

  return ret;
}

int ObTableScanIterator::init_and_open_get_merge_iter_()
{
  int ret = OB_SUCCESS;
  if (table_scan_range_.get_rowkeys().count() == 1) {
    INIT_AND_OPEN_ITER(single_merge_, table_scan_range_.get_rowkeys().at(0), true);
    if (OB_SUCC(ret)) {
      main_table_ctx_.use_fuse_row_cache_ = !single_merge_->is_read_memtable_only();
    }
  } else {
    INIT_AND_OPEN_ITER(get_merge_, table_scan_range_.get_rowkeys(), false);
  }
  return ret;
}

int ObTableScanIterator::sort_sample_ranges()
{
  int ret = OB_SUCCESS;
  const ObStorageDatumUtils &datum_utils = scan_param_->table_param_->get_read_info().get_datum_utils();
  if (OB_UNLIKELY(!datum_utils.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error for invalid datum utils", K(ret), KPC(scan_param_->table_param_));
  } else if (sample_ranges_.count() > 1 && scan_param_->scan_flag_.is_ordered_scan()) {
    ObDatumComparor<ObDatumRange> comparor(datum_utils, ret, scan_param_->scan_flag_.is_reverse_scan());
    lib::ob_sort(sample_ranges_.begin(), sample_ranges_.end(), comparor);
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "Failed to sort datum ranges", K(ret), K_(sample_ranges));
    }
  }
  return ret;
}

int ObTableScanIterator::init_and_open_scan_merge_iter_()
{
  int ret = OB_SUCCESS;

  if (table_scan_range_.get_ranges().count() == 1) {
    if (scan_param_->sample_info_.is_row_sample() || scan_param_->sample_info_.is_block_sample()) {
      bool need_scan_multiple_range = false;
      STORAGE_LOG(INFO, "start init sample iterator", K(scan_param_->sample_info_));
      ObGetSampleIterHelper sample_iter_helper(table_scan_range_, main_table_ctx_, *scan_param_, get_table_param_);
      if (OB_FAIL(sample_iter_helper.check_scan_range_count(need_scan_multiple_range, sample_ranges_))) {
        STORAGE_LOG(WARN, "check scan range count failed", KR(ret), KPC(scan_param_));
      } else if (OB_FAIL(sort_sample_ranges())) {
        STORAGE_LOG(WARN, "failed to sort sample ranges", K(ret));
      } else if (need_scan_multiple_range) {
        // this branch means the sample is row(memtable row) sample
        if (!scan_param_->sample_info_.is_row_sample()) {
          main_table_param_.iter_param_.disable_blockscan();
        }
        INIT_AND_OPEN_ITER(multi_scan_merge_, sample_ranges_, false);
        if (OB_FAIL(ret)) {
        } else if (scan_param_->sample_info_.is_row_sample()) {
          // Row sample is scan, do not need extra iterator.
        } else {
          if (OB_FAIL(
                  sample_iter_helper.get_sample_iter(memtable_row_sample_iterator_, main_iter_, multi_scan_merge_))) {
            STORAGE_LOG(WARN, "get sample iter failed", KR(ret), K(scan_param_));
          } else {
            STORAGE_LOG(
                INFO, "finish init memtable row sample iter", KP(memtable_row_sample_iterator_), KP(main_iter_));
          }
        }
      } else {
        // this branch means the sample is block sample
        // TODO : @yuanzhe block sample uses a different initialization logic and different open interface
        if (nullptr == scan_merge_ && OB_FAIL(init_scan_iter(scan_merge_))) {
          STORAGE_LOG(WARN, "Failed to init scanmerge", K(ret));
        } else if (OB_FAIL(sample_iter_helper.get_sample_iter(block_sample_iterator_, main_iter_, scan_merge_))) {
          STORAGE_LOG(WARN, "get sample iter failed", KR(ret), K(scan_param_));
        } else {
          STORAGE_LOG(INFO, "finish init block row sample iter", KP(block_sample_iterator_), KP(main_iter_));
        }
      }
    } else if (scan_param_->use_index_skip_scan()) {
      INIT_AND_OPEN_SKIP_SCAN_ITER(
          skip_scan_merge_, table_scan_range_.get_ranges().at(0), table_scan_range_.get_suffix_ranges().at(0), false);
    } else {
      INIT_AND_OPEN_ITER(scan_merge_, table_scan_range_.get_ranges().at(0), false);
    }
  } else if (scan_param_->use_index_skip_scan()) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "multiple ranges are not supported in index skip scan now");
  } else {
    INIT_AND_OPEN_ITER(multi_scan_merge_, table_scan_range_.get_ranges(), false);
  }

  return ret;
}

#undef INIT_AND_OPEN_ITER
#undef INIT_AND_OPEN_SKIP_SCAN_ITER

int ObTableScanIterator::get_next_row(ObNewRow *&row)
{
  return OB_NOT_IMPLEMENT;
}

int ObTableScanIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  ACTIVE_GLOBAL_ITERATOR_GUARD(ret, cached_iter_node_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObTableScanStoreRowIterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(main_iter_)) {
    ret = OB_ITER_END;
  } else {
    if (scan_param_->op_ != nullptr) {
      scan_param_->op_->clear_datum_eval_flag();
      scan_param_->op_->reset_trans_info_datum();
    }
    if (OB_FAIL(main_iter_->get_next_row(row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Fail to get next row, ", K(ret), KPC_(scan_param), K_(main_table_param),
            KP(single_merge_), KP(get_merge_), KP(scan_merge_), KP(multi_scan_merge_),
            KP(skip_scan_merge_), KPC(cached_iter_node_));
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

int ObTableScanIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  ACTIVE_GLOBAL_ITERATOR_GUARD(ret, cached_iter_node_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObTableScanStoreRowIterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(main_iter_)) {
    ret = OB_ITER_END;
  } else {
    if (scan_param_->op_ != nullptr) {
      scan_param_->op_->clear_datum_eval_flag();
      scan_param_->op_->reset_trans_info_datum();
    }

    if (OB_FAIL(main_iter_->get_next_rows(count, capacity))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Fail to get next row, ", K(ret), K(*scan_param_), K_(main_table_param),
            KP(single_merge_), KP(get_merge_), KP(scan_merge_), KP(multi_scan_merge_),
            KP(skip_scan_merge_), KPC(cached_iter_node_));
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

int ObTableScanIterator::check_ls_offline_after_read()
{
  int ret = OB_SUCCESS;

  memtable::ObMvccAccessCtx &acc_ctx = ctx_guard_.get_store_ctx().mvcc_acc_ctx_;

  if (acc_ctx.tx_table_guards_.check_ls_offline()) {
    ret = OB_LS_OFFLINE;
    STORAGE_LOG(WARN, "ls offline during the read operation", K(ret), K(acc_ctx.snapshot_));
  }
  return ret;
}

int ObTableScanIterator::check_txn_status_if_read_uncommitted_()
{
  int ret = OB_SUCCESS;
  memtable::ObMvccAccessCtx &acc_ctx = ctx_guard_.get_store_ctx().mvcc_acc_ctx_;
  if (acc_ctx.snapshot_.tx_id_.is_valid() && acc_ctx.mem_ctx_) {
    if (acc_ctx.mem_ctx_->is_tx_rollbacked()) {
      if (acc_ctx.mem_ctx_->is_for_replay()) {
        // goes here means the txn was killed due to LS's GC etc,
        // return NOT_MASTER
        ret = OB_NOT_MASTER;
      } else {
        // The txn has been killed during normal processing. So we return
        // OB_TRANS_KILLED to prompt this abnormal state.
        ret = OB_TRANS_KILLED;
        STORAGE_LOG(WARN, "txn has terminated", K(ret), "tx_id", acc_ctx.snapshot_.tx_id_);
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
