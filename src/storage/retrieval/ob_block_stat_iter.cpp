/**
 * Copyright (c) 2025 OceanBase
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

#include "storage/tx_storage/ob_ls_service.h"
#include "ob_block_stat_iter.h"

namespace oceanbase
{
namespace storage
{

ObBlockStatScanParam::ObBlockStatScanParam()
  : stat_cols_(nullptr),
    stat_projectors_(nullptr),
    scan_param_(nullptr),
    scan_single_major_only_(false),
    scan_max_sstable_block_granule_(false),
    force_scan_whole_range_(false)
{
}

void ObBlockStatScanParam::reset()
{
  stat_cols_ = nullptr;
  stat_projectors_ = nullptr;
  scan_param_ = nullptr;
  scan_single_major_only_ = false;
  scan_max_sstable_block_granule_ = false;
  force_scan_whole_range_ = false;
}

bool ObBlockStatScanParam::is_valid() const
{
  const bool ptr_valid = nullptr != stat_cols_ && nullptr != stat_projectors_ && nullptr != scan_param_;
  const bool stat_meta_valie = stat_cols_->count() == stat_projectors_->count();
  const bool range_valid = (!force_scan_whole_range_ || scan_single_major_only_);
  return ptr_valid && stat_meta_valie && range_valid;
}

int ObBlockStatScanParam::init(
    const ObIArray<ObSkipIndexColMeta> &stat_cols,
    const ObIArray<uint32_t> &stat_projectors,
    ObTableScanParam &scan_param,
    bool scan_single_major_only,
    bool scan_max_sstable_block_granule,
    bool force_scan_whole_range)
{
  int ret = OB_SUCCESS;
  stat_cols_ = &stat_cols;
  stat_projectors_ = &stat_projectors;
  scan_param_ = &scan_param;
  scan_single_major_only_ = scan_single_major_only;
  scan_max_sstable_block_granule_ = scan_max_sstable_block_granule;
  force_scan_whole_range_ = force_scan_whole_range;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(stat_cols), K(stat_projectors), K(scan_param),
        K(scan_single_major_only), K(scan_max_sstable_block_granule));
  }
  return ret;
}

int ObBlockStatIterator::SSTableIter::next()
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(idx_scanner_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx scanner is null", K(ret));
  } else if (OB_FAIL(idx_scanner_->get_next(idx_row_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from idx scanner", K(ret));
    } else {
      iter_end_ = true;
      idx_row_ = nullptr;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObBlockStatIterator::MemTableIter::next()
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    ret = OB_ITER_END;
    LOG_WARN("memtable already iter end", K(ret));
  } else if (OB_ISNULL(memtable_scanner_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable scanner is null", K(ret));
  } else if (OB_FAIL(memtable_scanner_->get_next_row(row_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from memtable scanner", K(ret));
    } else {
      iter_end_ = true;
      row_ = nullptr;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObBlockStatIterator::ObBlockStatKeyCmp::init(const blocksstable::ObStorageDatumUtils &datum_utils, const int64_t cmp_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(datum_utils));
  } else {
    datum_utils_ = &datum_utils;
    cmp_cnt_ = cmp_cnt;
    is_inited_ = true;
  }
  return ret;
}

int ObBlockStatIterator::ObBlockStatKeyCmp::cmp(const ObBlockStatIterator::ObBlockStatKeyItem &l,
    const ObBlockStatIterator::ObBlockStatKeyItem &r, int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == datum_utils_ || cmp_cnt_ <= 0 || nullptr == l.endkey_ || nullptr == r.endkey_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(datum_utils_), KP(l.endkey_), KP(r.endkey_));
  } else if (OB_UNLIKELY(l.endkey_->get_datum_cnt() < cmp_cnt_ || r.endkey_->get_datum_cnt() < cmp_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("endkey datum count less than cmp_cnt", K(ret),
             K(l.endkey_->get_datum_cnt()), K(r.endkey_->get_datum_cnt()), K(cmp_cnt_));
  } else {
    ObDatumRowkey l_key;
    ObDatumRowkey r_key;
    int temp_cmp_ret = 0;
    if (OB_FAIL(l_key.assign(l.endkey_->datums_, cmp_cnt_))) {
      STORAGE_LOG(WARN, "failed to assign rowkey", K(ret), KPC(l.endkey_));
    } else if (OB_FAIL(r_key.assign(r.endkey_->datums_, cmp_cnt_))) {
      STORAGE_LOG(WARN, "failed to assign rowkey", K(ret), KPC(r.endkey_));
    } else if (OB_FAIL(l_key.compare(r_key, *datum_utils_, temp_cmp_ret))) {
      STORAGE_LOG(WARN, "failed to compare rowkey", K(ret), KPC(l.endkey_), KPC(r.endkey_), KPC(datum_utils_));
    } else {
      cmp_ret = temp_cmp_ret;
    }
  }
  return ret;
}

ObBlockStatIterator::ObBlockStatIterator()
  : scan_param_(nullptr),
    allocator_(ObMemAttr(MTL_ID(), "BlkStatIter")),
    merged_endkey_allocator_(ObMemAttr(MTL_ID(), "BlkStatKeyIter")),
    stat_collector_(),
    scan_range_(),
    get_table_param_(),
    ctx_guard_(),
    main_table_param_(),
    main_table_ctx_(),
    sstable_idx_scan_param_(),
    scan_tables_(),
    memtable_iters_(),
    sstable_iters_(),
    key_cmp_(),
    merge_heap_(nullptr),
    iter_idxs_(),
    rowkey_read_info_(nullptr),
    curr_endkey_(nullptr),
    curr_merged_endkey_(),
    curr_scan_range_(),
    curr_scan_start_key_(),
    iter_allocator_(nullptr),
    is_baseline_merged_endkey_(false),
    iter_end_(false),
    is_inited_(false)
{
}

void ObBlockStatIterator::reset()
{
  is_inited_ = false;
  iter_end_ = false;
  is_baseline_merged_endkey_ = false;
  curr_scan_start_key_.reset();
  curr_scan_range_.reset();
  curr_endkey_ = nullptr;
  curr_merged_endkey_.reset();
  rowkey_read_info_ = nullptr;
  reset_iters();
  scan_tables_.reset();
  // sstable_idx_scan_param_.reset();
  main_table_ctx_.reset();
  main_table_param_.reset();
  ctx_guard_.reset();
  get_table_param_.reset();
  scan_range_.reset();
  stat_collector_.reset();
  key_cmp_.reset();
  release_merge_heap();
  iter_idxs_.reset();
  iter_allocator_ = nullptr;
  allocator_.reset();
  merged_endkey_allocator_.reset();
  scan_param_ = nullptr;
}

int ObBlockStatIterator::init(const ObTabletHandle &tablet_handle, ObBlockStatScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &store_ctx = ctx_guard_.get_store_ctx();
  ObTableScanParam *table_scan_param = scan_param.get_scan_param();
  const ObSSTableIndexScanParam::ScanLevel scan_level = scan_param.is_scan_max_sstable_block_granule()
      ? ObSSTableIndexScanParam::ScanLevel::ROOT
      : ObSSTableIndexScanParam::ScanLevel::LEAF;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!scan_param.is_valid() || !tablet_handle.is_valid() || !store_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(scan_param), K(tablet_handle), K(store_ctx));
  } else if (FALSE_IT(scan_param_ = &scan_param)) {
  } else if (OB_FAIL(init_scan_range(tablet_handle, scan_param))) {
    LOG_WARN("failed to init scan range", K(ret));
  } else if (OB_FAIL(get_table_param_.tablet_iter_.set_tablet_handle(tablet_handle))) {
    LOG_WARN("failed to set tablet handle to iter", K(ret));
  } else if (OB_FAIL(init_memtable_access_param(tablet_handle, *table_scan_param))) {
    LOG_WARN("failed to init memtable access param", K(ret));
  } else if (OB_FAIL(sstable_idx_scan_param_.init(
      *scan_param.get_stat_cols(),
      tablet_handle.get_obj()->get_rowkey_read_info(),
      scan_level,
      table_scan_param->scan_flag_))) {
    LOG_WARN("failed to init sstable index scan param", K(ret));
  } else if (!scan_param_->is_scan_single_major_only() && OB_FAIL(stat_collector_.init(
      *scan_param.get_stat_cols(),
      *scan_param.get_stat_projectors(),
      table_scan_param->table_param_->get_read_info().get_columns_desc(),
      *iter_allocator_))) {
    LOG_WARN("failed to init stat collector", K(ret));
  } else if (OB_FAIL(prepare_scan_tables())) {
    LOG_WARN("failed to prepare scan tables", K(ret));
  } else if (OB_FAIL(construct_iters())) {
    LOG_WARN("failed to init iters", K(ret));
  } else if (use_merged_range() && OB_FAIL(build_merge_heap(&tablet_handle.get_obj()->get_rowkey_read_info()))) {
    LOG_WARN("failed to build merge heap", K(ret));
  } else {
    rowkey_read_info_ = &tablet_handle.get_obj()->get_rowkey_read_info();
    iter_end_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObBlockStatIterator::get_next(const ObDatumRow *&agg_row, const ObDatumRowkey *&endkey)
{
  int ret = OB_SUCCESS;
  stat_collector_.reuse();
  bool beyond_range = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("block stat iterator is not initialized", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(refresh_scan_table_on_demand())) {
    LOG_WARN("failed to refresh scan table on demand", K(ret));
  } else if (OB_FAIL(next_range(beyond_range))) {
    LOG_WARN("failed to iterate next baseline range", K(ret));
  } else if (beyond_range && is_all_iter_end()) {
    iter_end_ = true;
    ret = OB_ITER_END;
  } else if (scan_param_->is_scan_single_major_only()) {
    endkey = get_baseline_block_iter().get_curr_index_row()->endkey_;
    agg_row = &(get_baseline_block_iter().get_curr_index_row()->skip_index_row_);
  } else if (OB_FAIL(collect_sstable_idx_rows(beyond_range))) {
    LOG_WARN("failed to collect sstable idx rows", K(ret));
  } else if (OB_FAIL(collect_memtable_scan_rows(beyond_range))) {
    LOG_WARN("failed to collect memtable scan rows", K(ret));
  } else if (OB_FAIL(stat_collector_.get_result_row(agg_row))) {
    LOG_WARN("failed to get agg row", K(ret));
  } else {
    endkey = curr_endkey_;
  }
  return ret;
}

int ObBlockStatIterator::advance_to(const ObDatumRowkey &advance_key, const bool inclusive)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("block stat iterator is not initialized", K(ret));
  } else if (iter_end_) {
    // skip
  } else if (OB_FAIL(refresh_scan_table_on_demand())) {
    LOG_WARN("failed to refresh scan table on demand", K(ret));
  } else if (OB_FAIL(advance_sstable_iters(advance_key, inclusive))) {
    LOG_WARN("failed to advance sstable iters", K(ret));
  } else if (OB_FAIL(advance_memtable_iters(advance_key, inclusive))) {
    LOG_WARN("failed to advance memtable iters", K(ret));
  } else if (is_all_iter_end()) {
    iter_end_ = true;
  } else {
    // set curr_endkey_ after advance to new key
    curr_endkey_ = &advance_key;
  }
  return ret;
}


int ObBlockStatIterator::init_scan_range(const ObTabletHandle &tablet_handle, ObBlockStatScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  bool is_tablet_splitting = false;
  if (scan_param.force_scan_whole_range()) {
    curr_scan_range_.set_whole_range();
  } else if (OB_FAIL(ObTabletSplitMdsHelper::get_is_spliting(*tablet_handle.get_obj(), is_tablet_splitting))) {
    LOG_WARN("failed to get is tablet splitting", K(ret));
  } else if (OB_UNLIKELY(is_tablet_splitting)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("splitting tablet not supported for block stat iterator", K(ret));
  } else if (OB_FAIL(scan_range_.init(*scan_param.get_scan_param(), *tablet_handle.get_obj(), is_tablet_splitting))) {
    LOG_WARN("failed to init scan range", K(ret));
  } else if (OB_UNLIKELY(scan_range_.get_ranges().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty scan range", K(ret), K_(scan_range), K(scan_param));
  } else {
    curr_scan_range_ = scan_range_.get_ranges().at(0);
  }
  return ret;
}

int ObBlockStatIterator::init_memtable_access_param(
    const ObTabletHandle &tablet_handle,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(scan_param.is_mview_query())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("mview query not supported for block stat iterator", K(ret));
  } else if (OB_FAIL(main_table_param_.init(scan_param, &tablet_handle))) {
    LOG_WARN("failed to init main table param", K(ret));
  } else {
    ObVersionRange trans_version_range;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.base_version_ = 0;
    trans_version_range.snapshot_version_ = ctx_guard_.get_store_ctx().mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
    if (OB_FAIL(main_table_ctx_.init(
        scan_param, ctx_guard_.get_store_ctx(), trans_version_range, nullptr /*cached_iter_node*/))) {
      LOG_WARN("failed to init main table ctx", K(ret));
    } else {
      iter_allocator_ = main_table_ctx_.get_long_life_allocator();
    }
  }
  return ret;
}

int ObBlockStatIterator::refresh_scan_table_on_demand()
{
  int ret = OB_SUCCESS;
  const bool need_refresh = get_table_param_.tablet_iter_.table_iter()->check_store_expire();
  if (OB_UNLIKELY(need_refresh)) {
    scan_tables_.reuse();
    if (nullptr != curr_endkey_ && OB_FAIL(shrink_scan_range(*curr_endkey_))) {
      LOG_WARN("failed to shrink scan range", K(ret));
    } else if (FALSE_IT(reset_iters())) {
    } else if (OB_FAIL(refresh_tablet_iter())) {
      LOG_WARN("failed to refresh tablet iter", K(ret));
    } else if (OB_FAIL(prepare_scan_tables())) {
      LOG_WARN("failed to prepare scan tables", K(ret));
    } else if (OB_FAIL(construct_iters())) {
      LOG_WARN("failed to construct iters", K(ret));
    } else if (use_merged_range()) {
      iter_idxs_.reuse();
      if (OB_FAIL(release_merge_heap())) {
        LOG_WARN("failed to release merge heap", K(ret));
      } else if (OB_FAIL(build_merge_heap(rowkey_read_info_))) {
        LOG_WARN("failed to build merge heap", K(ret));
      }
    }
  }
  return ret;
}

int ObBlockStatIterator::refresh_tablet_iter()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!get_table_param_.tablet_iter_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet iter is invalid", K(ret), K(get_table_param_.tablet_iter_));
  } else {
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    rowkey_read_info_ = nullptr;
    main_table_param_.iter_param_.rowkey_read_info_ = nullptr;
    const int64_t remain_timeout = THIS_WORKER.get_timeout_remain();
    const share::ObLSID &ls_id = main_table_ctx_.ls_id_;
    const common::ObTabletID &tablet_id = get_table_param_.tablet_iter_.get_tablet()->get_tablet_meta().tablet_id_;
    const int64_t snapshot_version = main_table_ctx_.store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
    if (OB_UNLIKELY(remain_timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("timeout", K(ret), K(ls_id), K(tablet_id), K(remain_timeout));
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr to ls", K(ret), K(ls_handle));
    } else if (OB_FAIL(ls->get_tablet_svr()->get_read_tables(
        tablet_id,
        remain_timeout,
        snapshot_version,
        snapshot_version,
        get_table_param_.tablet_iter_,
        false/*allow_not_ready*/,
        false/*need_split_src_table*/,
        false/*need_split_dst_table*/))) {
      LOG_WARN("failed to refresh tablet iterator", K(ret), K(ls_id), K_(get_table_param));
    } else {
      rowkey_read_info_ = &get_table_param_.tablet_iter_.get_tablet_handle().get_obj()->get_rowkey_read_info();
      main_table_param_.iter_param_.rowkey_read_info_ = rowkey_read_info_;
    }
  }

  return ret;
}

int ObBlockStatIterator::prepare_scan_tables()
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator *table_store_iter = get_table_param_.tablet_iter_.table_iter();
  if (OB_UNLIKELY(0 != scan_tables_.count() || !main_table_param_.is_valid() || !main_table_ctx_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status before prepare  scan tables", K(ret), K(scan_tables_.count()),
        K_(main_table_param), K_(main_table_ctx));
  } else if (OB_UNLIKELY(main_table_param_.iter_param_.is_tablet_spliting())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("splitting tablet not supported for block stat iterator", K(ret));
  } else {
    const bool query_with_frozen_version = get_table_param_.frozen_version_ != -1;
    const int64_t query_version = query_with_frozen_version
        ? get_table_param_.frozen_version_
        : main_table_ctx_.store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
    const bool major_sstable_only = query_with_frozen_version;
    if (OB_FAIL(get_table_param_.tablet_iter_.refresh_read_tables_from_tablet(
        query_version,
        false/*allow_not_ready*/,
        major_sstable_only,
        false/*need_split_src_table*/,
        false/*need_split_dst_table*/))) {
      LOG_WARN("failed to get read tables from tablet", K(ret), K(query_version),
          K(major_sstable_only), K(get_table_param_), K_(main_table_param), K_(main_table_ctx));
    }
  }

  while (OB_SUCC(ret)) {
    ObITable *table_ptr = nullptr;
    if (OB_FAIL(table_store_iter->get_next(table_ptr))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next table", K(ret));
      }
    } else if (OB_ISNULL(table_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must not be null", K(ret), K(table_store_iter));
    } else if (OB_UNLIKELY(table_ptr->is_major_sstable() && table_ptr->get_snapshot_version() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected major sstable", K(ret), KPC(table_ptr));
    } else if (OB_UNLIKELY(0 == scan_tables_.count() && !table_ptr->is_major_sstable())){
      ret = OB_NOT_SUPPORTED;
      LOG_INFO("block stat iterator not supported for tablet without major sstable", K(ret), KPC(table_ptr));
    } else if (OB_FAIL(scan_tables_.push_back(table_ptr))) {
      LOG_WARN("failed to push back table", K(ret), K(*table_ptr));
    } else if (scan_param_->is_scan_single_major_only() ) {
      ret = OB_ITER_END;
    }
  }

  if (OB_LIKELY(OB_ITER_END == ret)) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(scan_tables_.count() > common::MAX_TABLE_CNT_IN_STORAGE)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table cnt for scan", K(ret), K(scan_tables_.count()),
        K(table_store_iter), K(scan_tables_));
    }
  }

  LOG_DEBUG("prepare scan tables", K(ret), K_(scan_tables), K_(get_table_param));

  return ret;
}

int ObBlockStatIterator::construct_iters()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 != memtable_iters_.count() || 0 != sstable_iters_.count() || nullptr == iter_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status before construct iters", K(ret),
        K(memtable_iters_.count()), K(sstable_iters_.count()), KP_(iter_allocator));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < scan_tables_.count(); ++i) {
    ObITable *table = scan_tables_.at(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table must not be null", K(ret), K(i), K(scan_tables_));
    } else if (table->is_memtable()) {
      ObStoreRowIterator *iter = nullptr;
      if (OB_FAIL(table->scan(main_table_param_.iter_param_, main_table_ctx_, curr_scan_range_, iter))) {
        LOG_WARN("failed to scan memtable", K(ret), KPC(table));
      } else if (OB_FAIL(memtable_iters_.push_back(MemTableIter(iter)))) {
        LOG_WARN("failed to push back memtable iter", K(ret), KPC(table));
      }
    } else if (table->is_sstable()) {
      ObSSTableIndexScanner *idx_scanner = nullptr;
      ObSSTable *sstable = static_cast<ObSSTable *>(table);
      if (OB_FAIL(sstable->scan_index(curr_scan_range_, sstable_idx_scan_param_, *iter_allocator_, idx_scanner))) {
        LOG_WARN("failed to scan sstable index", K(ret), KPC(table));
      } else if (OB_FAIL(sstable_iters_.push_back(SSTableIter(idx_scanner)))) {
        LOG_WARN("failed to push back sstable iter", K(ret), KPC(table));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table type", K(ret), KPC(table));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(scan_tables_.count() != memtable_iters_.count() + sstable_iters_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter count", K(ret), K(scan_tables_.count()),
        K(memtable_iters_.count()), K(sstable_iters_.count()));
  } else {
    // iter startup
    for (int64_t i = 0; OB_SUCC(ret) && i < memtable_iters_.count(); ++i) {
      MemTableIter &iter = memtable_iters_.at(i);
      if (OB_FAIL(iter.next())) {
        LOG_WARN("failed to get next row from memtable iter", K(ret), K(i), K(iter));
      }
    }
    const int64_t start_iter_idx = use_merged_range() ? 0 : 1;
    for (int64_t i = start_iter_idx; OB_SUCC(ret) && i < sstable_iters_.count(); ++i) {
      SSTableIter &iter = sstable_iters_.at(i);
      if (OB_FAIL(iter.next())) {
        LOG_WARN("failed to get next row from sstable iter", K(ret), K(i), K(iter));
      }
    }
  }

  return ret;
}

void ObBlockStatIterator::reset_iters()
{
  for (int64_t i = 0; i < memtable_iters_.count(); ++i) {
    memtable_iters_.at(i).reset(iter_allocator_);
  }
  memtable_iters_.reset();
  for (int64_t i = 0; i < sstable_iters_.count(); ++i) {
    sstable_iters_.at(i).reset(iter_allocator_);
  }
  sstable_iters_.reset();
}

int ObBlockStatIterator::release_merge_heap()
{
  int ret = OB_SUCCESS;
  if (nullptr != merge_heap_) {
    merge_heap_->~OBSMergeHeap();
    if (nullptr != iter_allocator_) {
      iter_allocator_->free(merge_heap_);
    }
    merge_heap_ = nullptr;
  }
  return ret;
}

int ObBlockStatIterator::build_merge_heap(const ObITableReadInfo *rowkey_read_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != merge_heap_ || nullptr == iter_allocator_ || nullptr == rowkey_read_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status before build merge heap", K(ret), KP_(merge_heap), KP_(iter_allocator), KP(rowkey_read_info));
  } else if (OB_UNLIKELY(sstable_iters_.count() < MIN_SSTABLE_CNT_USE_MERGED_RANGE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge heap not supported for single sstable", K(ret), K(sstable_iters_.count()));
  } else if (!key_cmp_.is_valid() && OB_FAIL(key_cmp_.init(rowkey_read_info->get_datum_utils(), rowkey_read_info->get_schema_column_count()))) {
    LOG_WARN("failed to init key cmp", K(ret));
  } else if (sstable_iters_.count() <= ObBSSimpleMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
    if (OB_ISNULL(merge_heap_ = OB_NEWx(ObBSSimpleMerger, iter_allocator_, key_cmp_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate simple merger", K(ret));
    }
  } else if (OB_ISNULL(merge_heap_ = OB_NEWx(ObBSLoserTree, iter_allocator_, key_cmp_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate loser tree", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(merge_heap_->init(sstable_iters_.count(), sstable_iters_.count(), *iter_allocator_))) {
    LOG_WARN("failed to init merge heap", K(ret));
  } else if (OB_FAIL(merge_heap_->open(sstable_iters_.count()))) {
    LOG_WARN("failed to open merge heap", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_iters_.count(); ++i) {
      SSTableIter &iter = sstable_iters_.at(i);
      if (iter.is_iter_end()) {
      } else if (OB_UNLIKELY(nullptr == iter.get_curr_index_row() || nullptr == iter.get_curr_index_row()->endkey_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr to sstable iter row", K(ret), K(i), K(iter));
      } else if (OB_FAIL(merge_heap_->push(ObBlockStatKeyItem(i, iter.get_curr_index_row()->endkey_)))) {
        LOG_WARN("failed to push endkey to merge heap", K(ret), K(i), K(iter));
      } else {
        LOG_TRACE("build merge heap, push endkey to merge heap", K(ret), K(i), K(iter), KP(this), KPC(iter.get_curr_index_row()->endkey_));
      }
    }
  }
  if (OB_SUCC(ret) && !merge_heap_->empty()) {
    if (OB_FAIL(merge_heap_->rebuild())) {
      LOG_WARN("failed to rebuild merge heap", K(ret));
    }
  }
  return ret;
}

int ObBlockStatIterator::fill_merge_heap()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < iter_idxs_.count(); ++i) {
    const int64_t iter_idx = iter_idxs_.at(i);
    SSTableIter *iter = nullptr;
    if (OB_UNLIKELY(iter_idx < 0 || iter_idx >= sstable_iters_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter idx", K(ret), K(iter_idx), K(sstable_iters_.count()));
    } else {
      iter = &sstable_iters_.at(iter_idx);
    }
    if (OB_FAIL(ret)) {
    } else if (iter->is_iter_end()) {
    } else if (OB_UNLIKELY(nullptr == iter->get_curr_index_row() || nullptr == iter->get_curr_index_row()->endkey_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to sstable iter row", K(ret), K(iter_idx), K(iter));
    } else if (OB_FAIL(merge_heap_->push(ObBlockStatKeyItem(iter_idx, iter->get_curr_index_row()->endkey_)))) {
      LOG_WARN("failed to push endkey to merge heap", K(ret), K(iter_idx), K(iter));
    } else {
      LOG_TRACE("fill merge heap", K(ret), K(iter_idx), K(iter), KP(this), K(iter_idxs_), KPC(iter->get_curr_index_row()->endkey_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(iter_idxs_.reuse())) {
  } else if (!merge_heap_->empty()) {
    if (OB_FAIL(merge_heap_->rebuild())) {
      LOG_WARN("failed to rebuild merge heap", K(ret));
    }
  }
  return ret;
}

int ObBlockStatIterator::next_range(bool &beyond_range)
{
  int ret = OB_SUCCESS;
  if (use_merged_range()) {
    ret = next_merged_range(beyond_range);
  } else {
    ret = next_baseline_range(beyond_range);
  }
  return ret;
}

int ObBlockStatIterator::next_baseline_range(bool &beyond_range)
{
  int ret = OB_SUCCESS;
  SSTableIter &iter = get_baseline_block_iter();
  beyond_range = false;
  if (OB_FAIL(iter.next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from baseline block iter", K(ret), K_(sstable_iters));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (iter.is_iter_end()) {
    curr_endkey_ = &curr_scan_range_.get_end_key();
    beyond_range = true;
  } else if (scan_param_->is_scan_single_major_only()) {
    // skip
  } else if (OB_FAIL(stat_collector_.collect_agg_row(iter.get_curr_index_row()->skip_index_row_))) {
    LOG_WARN("failed to collect agg row", K(ret), K(iter));
  } else {
    curr_endkey_ = iter.get_curr_index_row()->endkey_;
  }
  return ret;
}

int ObBlockStatIterator::next_merged_range(bool &beyond_range)
{
  int ret = OB_SUCCESS;
  is_baseline_merged_endkey_ = false;
  if (OB_UNLIKELY(nullptr == merge_heap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge heap is not initialized", K(ret));
  } else if (OB_FAIL(fill_merge_heap())) {
    LOG_WARN("failed to fill merge heap", K(ret));
  } else if (merge_heap_->empty() && is_all_sstable_iters_end()) {
    curr_endkey_ = &curr_scan_range_.get_end_key();
    beyond_range = true;
  } else if (OB_UNLIKELY(merge_heap_->empty() && !is_all_sstable_iters_end())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge heap is empty but not all sstable iters end", K(ret));
  } else {
    const ObBlockStatKeyItem *item = nullptr;
    bool has_same_endkey = false;
    bool first_row = true;
    bool exceeds_current_token = false;
    while (OB_SUCC(ret) && !merge_heap_->empty() && (first_row || has_same_endkey || exceeds_current_token)) {
      first_row = false;
      has_same_endkey = !merge_heap_->is_unique_champion();
      merged_endkey_allocator_.reuse();
      if (OB_FAIL(merge_heap_->top(item))) {
        LOG_WARN("failed to get top item from merge heap", K(ret));
      } else if (OB_UNLIKELY(nullptr == item || nullptr == item->endkey_
          || item->iter_idx_ < 0 || item->iter_idx_ >= sstable_iters_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected iter idx", K(ret), KPC(item), K(sstable_iters_.count()));
      } else if (!exceeds_current_token) {
        int cmp_ret = 0;
        const ObStorageDatum &endkey_token = item->endkey_->get_datum(0);
        ObStorageDatum current_token;
        if (OB_UNLIKELY(nullptr == scan_param_ || nullptr == scan_param_->get_scan_param() ||
                        scan_param_->get_scan_param()->key_ranges_.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected scan param or empty key ranges", K(ret), KPC(scan_param_));
        } else if (OB_FAIL(current_token.from_obj(scan_param_->get_scan_param()->key_ranges_.at(0).end_key_.get_obj_ptr()[0]))) {
          LOG_WARN("fail to convert to datum", K(ret), K(scan_param_->get_scan_param()->key_ranges_));
        } else if (OB_FAIL(rowkey_read_info_->get_datum_utils().get_cmp_funcs().at(0).compare(endkey_token, current_token, cmp_ret))) {
          LOG_WARN("fail to compare token", K(ret), K(endkey_token), K(current_token));
        } else {
          exceeds_current_token = cmp_ret > 0;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(item->endkey_->deep_copy(curr_merged_endkey_, merged_endkey_allocator_))) {
        LOG_WARN("failed to deep copy endkey", K(ret), KPC(item->endkey_));
      } else {
        curr_endkey_ = &curr_merged_endkey_;
        SSTableIter &iter = sstable_iters_.at(item->iter_idx_);
        is_baseline_merged_endkey_ = is_baseline_merged_endkey_ || 0 == item->iter_idx_;
        const ObSSTableIndexRow *idx_row = iter.get_curr_index_row();
        if (OB_UNLIKELY(nullptr == idx_row || idx_row->endkey_ != item->endkey_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null idx row or endkey", K(ret), K(item->iter_idx_), KPC(idx_row),
              KP(curr_endkey_));
        } else if (OB_FAIL(stat_collector_.collect_agg_row(idx_row->skip_index_row_))) {
          LOG_WARN("failed to collect agg row", K(ret), K(item->iter_idx_), K(iter));
        } else if (OB_FAIL(iter_idxs_.push_back(item->iter_idx_))) {
          LOG_WARN("failed to push iter idx to array", K(ret), K(item->iter_idx_), K(iter_idxs_));
        } else if (OB_FAIL(merge_heap_->pop())) {
          LOG_WARN("failed to pop item from merge heap", K(ret));
        } else if (OB_FAIL(iter.next())) {
          LOG_WARN("failed to get next row from sstable iter", K(ret), K(item->iter_idx_), K(iter));
        } else {
          LOG_TRACE("next merged range", KP(this), KPC(item), K(iter), K(merge_heap_->count()), K(iter_idxs_), KPC(curr_endkey_), K(lbt()));
        }
      }
    }
  }
  return ret;
}

int ObBlockStatIterator::collect_sstable_idx_rows(const bool drain_all_iters)
{
  int ret = OB_SUCCESS;
  const int64_t start_iter_idx = use_merged_range() && !is_baseline_merged_endkey_ ? 0 : 1;
  is_baseline_merged_endkey_ = false;
  for (int64_t i = start_iter_idx; OB_SUCC(ret) && i < sstable_iters_.count(); ++i) {
    SSTableIter &iter = sstable_iters_.at(i);
    bool rowkey_in_range = true;
    while (OB_SUCC(ret) && !iter.is_iter_end() && rowkey_in_range) {
      const ObSSTableIndexRow *idx_row = iter.get_curr_index_row();
      if (OB_ISNULL(idx_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr to sstable iter row", K(ret));
      } else if (!drain_all_iters && OB_FAIL(check_rowkey_in_range(*idx_row->endkey_, rowkey_in_range))) {
        LOG_WARN("failed to check rowkey in range", K(ret), K(i), K(iter));
      } else if (!rowkey_in_range) {
        // skip
      } else if (OB_FAIL(stat_collector_.collect_agg_row(idx_row->skip_index_row_))) {
        LOG_WARN("failed to collect agg row", K(ret), K(i), K(iter));
      } else if (OB_FAIL(iter.next())) {
        LOG_WARN("failed to get next row from sstable iter", K(ret), K(i), K(iter));
      }
    }

    // collect last range contains current endkey to ensure loose agg semantic
    if (OB_FAIL(ret) || iter.is_iter_end()) {
      // skip
    } else if (OB_FAIL(stat_collector_.collect_agg_row(iter.get_curr_index_row()->skip_index_row_))) {
      LOG_WARN("failed to collect agg row", K(ret), K(i), K(iter));
    }
  }

  return ret;
}

int ObBlockStatIterator::collect_memtable_scan_rows(const bool drain_all_iters)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < memtable_iters_.count(); ++i) {
    MemTableIter &iter = memtable_iters_.at(i);
    bool rowkey_in_range = true;
    while (OB_SUCC(ret) && !iter.is_iter_end() && rowkey_in_range) {
      const ObDatumRow *row = iter.get_curr_row();
      if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr to memtable iter row", K(ret));
      } else if (!drain_all_iters && OB_FAIL(check_rowkey_in_range(*row, rowkey_in_range))) {
        LOG_WARN("failed to check rowkey in range", K(ret), K(i), K(iter));
      } else if (!rowkey_in_range) {
        // skip
      } else if (OB_FAIL(stat_collector_.collect_data_row(*row))) {
        LOG_WARN("failed to collect data row", K(ret), KPC(row));
      } else if (OB_FAIL(iter.next())) {
        LOG_WARN("failed to get next row from memtable iter", K(ret), K(i), K(iter));
      }
    }
  }

  return ret;
}

int ObBlockStatIterator::advance_sstable_iters(const ObDatumRowkey &advance_key, const bool inclusive)
{
  int ret = OB_SUCCESS;
  bool iter_advanced = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_iters_.count(); ++i) {
    SSTableIter &iter = sstable_iters_.at(i);
    const ObSSTableIndexRow *idx_row = iter.get_curr_index_row();
    bool advance_key_in_curr_range = false;
    if (nullptr != idx_row) {
      const ObDatumRowkey *endkey = idx_row->endkey_;
      int cmp_ret = 0;
      if (OB_FAIL(endkey->compare(advance_key, rowkey_read_info_->get_datum_utils(), cmp_ret, false))) {
        LOG_WARN("failed to compare rowkey", K(ret), K(endkey), K(advance_key));
      } else if (cmp_ret > 0 || (cmp_ret == 0 && inclusive)) {
        advance_key_in_curr_range = true;
      } else {
        // need do advance
      }
    }

    if (advance_key_in_curr_range) {
      // skip
    } else if (iter.is_iter_end()) {
      // skip
    } else if (OB_FAIL(iter.advance_to(advance_key, inclusive))) {
      LOG_WARN("failed to advance to key", K(ret), K(i), K(iter));
    } else if (FALSE_IT(iter_advanced = true)) {
    } else if ((0 != i || use_merged_range()) && OB_FAIL(iter.next())) {
      LOG_WARN("failed move forward iter for non-baseline sstable", K(ret), K(i), K(iter));
    }
  }
  if (OB_SUCC(ret) && iter_advanced && use_merged_range()) {
    iter_idxs_.reuse();
    merge_heap_->reuse();
    if (OB_FAIL(merge_heap_->open(sstable_iters_.count()))) {
      LOG_WARN("failed to open merge heap", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_iters_.count(); ++i) {
      if (!sstable_iters_.at(i).is_iter_end() && OB_FAIL(iter_idxs_.push_back(i))) {
        LOG_WARN("failed to push iter idx to array", K(ret), K(iter_idxs_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_merge_heap())) {
      LOG_WARN("failed to fill merge heap", K(ret));
    }
  }
  return ret;
}

int ObBlockStatIterator::advance_memtable_iters(const ObDatumRowkey &advance_key, const bool inclusive)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < memtable_iters_.count(); ++i) {
    MemTableIter &iter = memtable_iters_.at(i);
    bool advance_finished = false;
    while (OB_SUCC(ret) && !iter.is_iter_end() && !advance_finished) {
      const ObDatumRow *row = iter.get_curr_row();
      int cmp_ret = 0;
      if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr to memtable iter row", K(ret));
      } else {
        ObDatumRowkey curr_rowkey(row->storage_datums_, rowkey_read_info_->get_rowkey_count());
        if (OB_FAIL(curr_rowkey.compare(advance_key, rowkey_read_info_->get_datum_utils(), cmp_ret, false))) {
          LOG_WARN("failed to compare rowkey", K(ret), K(curr_rowkey), K(advance_key));
        } else if (cmp_ret > 0 || (cmp_ret == 0 && inclusive)) {
          advance_finished = true;
        } else if (OB_FAIL(iter.next())) {
          LOG_WARN("failed to get next row from memtable iter", K(ret), K(i), K(iter));
        }
      }
    }
  }
  return ret;
}

int ObBlockStatIterator::check_rowkey_in_range(const ObDatumRow &row, bool &rowkey_in_range) const
{
  // Since block boundary is based on major sstable, which guarantees that no duplicate rowkey between micro blocks.
  // And memtable iter does not project multi-version rowkey columns for now, we should use schema rowkey for compare.
  ObDatumRowkey rowkey(row.storage_datums_, rowkey_read_info_->get_schema_rowkey_count());
  return check_rowkey_in_range(rowkey, rowkey_in_range);
}

int ObBlockStatIterator::check_rowkey_in_range(const ObDatumRowkey &rowkey, bool &rowkey_in_range) const
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_ISNULL(curr_endkey_) || OB_ISNULL(rowkey_read_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptrs", K(ret), KP_(curr_endkey), KP_(rowkey_read_info));
  } else if (OB_FAIL(rowkey.compare(*curr_endkey_, rowkey_read_info_->get_datum_utils(), cmp_ret, false))) {
    LOG_WARN("failed to compare rowkey", K(ret), K(rowkey), K(*curr_endkey_));
  } else {
    rowkey_in_range = (cmp_ret <= 0);
  }
  return ret;
}

int ObBlockStatIterator::shrink_scan_range(const ObDatumRowkey &start_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_key.deep_copy(curr_scan_start_key_, allocator_))) {
    LOG_WARN("failed to deep copy start key", K(ret), K(start_key));
  } else if (OB_FAIL(curr_scan_start_key_.prepare_memtable_readable(rowkey_read_info_->get_columns_desc(), allocator_))) {
    LOG_WARN("failed to prepare memtable readable", K(ret), K(curr_scan_start_key_));
  } else {
    curr_scan_range_.start_key_ = curr_scan_start_key_;
  }
  return ret;
}

bool ObBlockStatIterator::is_all_iter_end() const
{
  bool all_iter_end = true;
  for (int64_t i = 0; i < memtable_iters_.count() && all_iter_end; ++i) {
    if (!memtable_iters_.at(i).is_iter_end()) {
      all_iter_end = false;
    }
  }
  all_iter_end = all_iter_end && is_all_sstable_iters_end();
  return all_iter_end;
}

bool ObBlockStatIterator::is_all_sstable_iters_end() const
{
  bool all_sstable_iters_end = true;
  for (int64_t i = 0; i < sstable_iters_.count() && all_sstable_iters_end; ++i) {
    if (!sstable_iters_.at(i).is_iter_end()) {
      all_sstable_iters_end = false;
    }
  }
  return all_sstable_iters_end;
}

bool ObBlockStatIterator::use_merged_range() const
{
  return nullptr != scan_param_ &&
         !scan_param_->is_scan_single_major_only() &&
         sstable_iters_.count() >= MIN_SSTABLE_CNT_USE_MERGED_RANGE;
}

} // namespace storage
} // namespace oceanbase
