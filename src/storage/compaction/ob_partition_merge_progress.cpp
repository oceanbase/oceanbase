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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_partition_merge_progress.h"
#include "ob_tenant_compaction_progress.h"
#include "storage/column_store/ob_co_merge_dag.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace blocksstable;

namespace compaction
{

ObPartitionMergeProgress::ObPartitionMergeProgress(common::ObIAllocator &allocator)
  : allocator_(allocator),
    ctx_(nullptr),
    merge_dag_(nullptr),
    scanned_row_cnt_arr_(nullptr),
    concurrent_cnt_(0),
    estimated_total_row_cnt_(0),
    estimated_total_size_(0),
    avg_row_length_(0),
    latest_update_ts_(ObTimeUtility::fast_current_time()),
    estimated_finish_time_(0),
    pre_scanned_row_cnt_(0),
    start_cg_idx_(0),
    end_cg_idx_(0),
    is_updating_(false),
    is_empty_merge_(false),
    is_inited_(false)
{
}

ObPartitionMergeProgress::~ObPartitionMergeProgress()
{
  reset();
}

void ObPartitionMergeProgress::reset()
{
  is_inited_ = false;
  ctx_ = nullptr;
  merge_dag_ = nullptr;
  if (OB_NOT_NULL(scanned_row_cnt_arr_)) {
    allocator_.free(scanned_row_cnt_arr_);
    scanned_row_cnt_arr_ = nullptr;
  }
  estimated_total_row_cnt_ = 0;
  estimated_total_size_ = 0;
  avg_row_length_ = 0;
  latest_update_ts_ = 0;
  estimated_finish_time_ = 0;
  pre_scanned_row_cnt_ = 0;
  start_cg_idx_ = 0;
  end_cg_idx_ = 0;
  concurrent_cnt_ = 0;
  is_updating_ = false;
}


int64_t ObPartitionMergeProgress::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(is_inited), K_(is_empty_merge), KP_(merge_dag), KP_(scanned_row_cnt_arr),
        K_(concurrent_cnt), K_(estimated_total_row_cnt),
        K_(latest_update_ts), K_(estimated_finish_time), K_(start_cg_idx), K_(end_cg_idx));
    J_OBJ_END();
  }
  return pos;
}

int ObPartitionMergeProgress::init(ObBasicTabletMergeCtx *ctx,
    ObTabletMergeDag *merge_dag,
    const int64_t start_cg_idx,
    const int64_t end_cg_idx)
{
  int ret = OB_SUCCESS;
  int64_t *buf = NULL;
  int64_t concurrent_cnt = 0;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartitionMergeProgress inited twice", K(ret));
  } else if (OB_UNLIKELY(NULL == ctx
      || NULL == merge_dag
      || 0 == (concurrent_cnt = ctx->get_concurrent_cnt()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), KPC(ctx), K(merge_dag), K(concurrent_cnt));
  } else if (OB_ISNULL(buf = static_cast<int64_t *>(allocator_.alloc(sizeof(int64_t) * concurrent_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for unit_cnt_arr_", K(ret), K(concurrent_cnt));
  } else {
    // for parallel merge, [0, concurrent_cnt) stores row count
    MEMSET(buf, 0, sizeof(int64_t) * concurrent_cnt);
    scanned_row_cnt_arr_ = buf;

    concurrent_cnt_ = concurrent_cnt;
    ctx_ = ctx;
    merge_dag_ = merge_dag;
    start_cg_idx_ = start_cg_idx;
    end_cg_idx_ = end_cg_idx;

    if (OB_FAIL(inner_init_estimated_vals())) {
      LOG_WARN("failed to init estimated vals", K(ret), KPC(ctx));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPartitionMergeProgress::estimate_memtables(
    const ObIArray<storage::ObITable*> &tables)
{
  int ret = OB_SUCCESS;

  for (int64_t i = tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObITable *table = tables.at(i);
    const memtable::ObMemtable *memtable = nullptr;

    if (OB_UNLIKELY(nullptr == table || !table->is_data_memtable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), K(i), K(tables.at(i)));
    } else {
      memtable = static_cast<const memtable::ObMemtable *>(table);
      const ObMtStat &mt_stat = memtable->get_mt_stat();
      estimated_total_row_cnt_ += mt_stat.insert_row_count_ + mt_stat.update_row_count_ + mt_stat.delete_row_count_;
      estimated_total_size_ += static_cast<ObMemtable *>(tables.at(i))->get_occupied_size();
    }
  }
  return ret;
}

int ObPartitionMergeProgress::estimate_sstables(
    const ObIArray<storage::ObITable*> &tables)
{
  int ret = OB_SUCCESS;
  int64_t total_macro_block_cnt = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
    const ObITable *table = tables.at(i);
    const ObSSTable *sstable = nullptr;

    if (OB_UNLIKELY(NULL == table || !table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), K(i), KPC(table), K(tables));
    } else if (table->is_empty()) {
      // do nothing
    } else if (FALSE_IT(sstable = static_cast<const ObSSTable *>(table))) {
    } else {
      if (sstable->is_major_sstable() && 0 == start_cg_idx_) {
        /* the major sstable size has been accumulated in ObTenantCompactionProgressMgr::init_progress
          * but for column store table:
          * total data_size = (minor/mini + co_row_store_cg major) * batch execute dag number */
      } else {
        estimated_total_size_ += sstable->get_occupy_size();
      }

      if (0 == i) {
        total_macro_block_cnt += sstable->get_total_macro_block_count();
      } else {
        total_macro_block_cnt += sstable->get_total_macro_block_count() - sstable->get_total_use_old_macro_block_count();
      }
      estimated_total_row_cnt_ += sstable->get_row_count();
    }
  }

  if (OB_SUCC(ret) && total_macro_block_cnt > 0) {
    estimated_total_row_cnt_ = (0 == estimated_total_row_cnt_)
                             ? DEFAULT_ROW_CNT_PER_MACRO_BLOCK * total_macro_block_cnt
                             : estimated_total_row_cnt_;
    estimated_total_size_ = (0 == estimated_total_size_)
                          ? OB_DEFAULT_MACRO_BLOCK_SIZE * total_macro_block_cnt
                          : estimated_total_size_;
  }
  return ret;
}

int ObPartitionMergeProgress::inner_init_estimated_vals()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> tables;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartitionMergeProgress has inited", K(ret), KPC(this));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret), KP_(ctx));
  } else if (OB_UNLIKELY(ctx_->get_tables_handle().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty tables handle", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ctx_->get_tables_handle().get_tables(tables))) {
    LOG_WARN("failed to get tables", K(ret), K(tables));
  } else if (is_mini_merge(ctx_->get_merge_type())) {
    if (OB_FAIL(estimate_memtables(tables))) {
      LOG_WARN("fail to estimate memtables", K(ret), K(tables));
    }
  } else if (OB_FAIL(estimate_sstables(tables))) {
    LOG_WARN("failed to estimate sstables", K(ret), K(tables));
  }

  if (OB_FAIL(ret)) {
  } else if (0 == estimated_total_size_ || 0 == estimated_total_row_cnt_) {
    estimated_finish_time_ = ObCompactionProgress::EXTRA_TIME + UPDATE_INTERVAL;
    is_empty_merge_ = true;
  } else {
    avg_row_length_ = estimated_total_size_ * 1.0 / estimated_total_row_cnt_;
    update_estimated_finish_time(0/*cur scanned row cnt*/);
    LOG_TRACE("success to estimate initial vals", K(ret), "param", ctx_->static_param_, KPC(this));
  }
  return ret;
}

int ObPartitionMergeProgress::update_merge_progress(
    const int64_t idx,
    const int64_t scanned_row_cnt)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMergeProgress not inited", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= concurrent_cnt_ || scanned_row_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(idx), K(concurrent_cnt_), K(scanned_row_cnt));
  } else if (is_empty_merge_) {
    // do nothing
  } else if (scanned_row_cnt <= scanned_row_cnt_arr_[idx]) {
    // do nothing
  } else if (FALSE_IT(scanned_row_cnt_arr_[idx] = scanned_row_cnt)) {
  } else if (REACH_THREAD_TIME_INTERVAL(UPDATE_INTERVAL)) {
    if (!ATOMIC_CAS(&is_updating_, false, true)) {
      latest_update_ts_ = ObTimeUtility::fast_current_time(); // only used for diagnose

      // 1. get total scanned row cnt
      int64_t total_scanned_row_cnt = 0;
      for (int64_t i = 0; i < concurrent_cnt_; ++i) {
        total_scanned_row_cnt += scanned_row_cnt_arr_[i];
      }

      // 2. the estimated total row count is too small, enlarge it and recalculate the avg row length
      if (total_scanned_row_cnt >= estimated_total_row_cnt_) {
        estimated_total_row_cnt_ += MAX(total_scanned_row_cnt / DEFAULT_INCREMENT_ROW_FACTOR, 1);
        avg_row_length_ = estimated_total_size_ * 1.0 / estimated_total_row_cnt_;
      }

      // 3. update estimated finish time by using new estimated vals
      update_estimated_finish_time(total_scanned_row_cnt);
      (void) inner_update_progress_mgr(total_scanned_row_cnt);

      // 5. record cur scanned row cnt for the next update
      pre_scanned_row_cnt_ = total_scanned_row_cnt;

      LOG_DEBUG("update merge progress", KPC(this),
                "param", ctx_->static_param_, K(total_scanned_row_cnt));

      ATOMIC_STORE(&is_updating_, false);
    }
  }
  return ret;
}

int ObPartitionMergeProgress::update_merge_info(ObSSTableMergeHistory &merge_history)
{
  int ret = OB_SUCCESS;
  if (concurrent_cnt_ > 1) {
    for (int i = 0; i < concurrent_cnt_; ++i) {
      merge_history.running_info_.parallel_merge_info_.info_[ObParalleMergeInfo::SCAN_UNITS].add(scanned_row_cnt_arr_[i]);
    }
  }
  return ret;
}

void ObPartitionMergeProgress::update_estimated_finish_time(const int64_t cur_scanned_row_cnt)
{
  int tmp_ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::fast_current_time();
  int64_t start_time = current_time;

  if (0 == cur_scanned_row_cnt) { // init merge progress
    int64_t spend_time = estimated_total_size_ / common::OB_DEFAULT_MACRO_BLOCK_SIZE * ObCompactionProgress::MERGE_SPEED
        + ObCompactionProgress::EXTRA_TIME;
    estimated_finish_time_ = spend_time + start_time + UPDATE_INTERVAL;
  } else {
    start_time = merge_dag_->get_start_time();
    int64_t delta_row_cnt = estimated_total_row_cnt_ - cur_scanned_row_cnt;
    int64_t rest_time = MAX(1, delta_row_cnt) * (current_time - start_time) / cur_scanned_row_cnt;
    estimated_finish_time_ = MAX(estimated_finish_time_, current_time + rest_time + UPDATE_INTERVAL);
  }
  if (estimated_finish_time_ - start_time >= MAX_ESTIMATE_SPEND_TIME) {
    if (REACH_THREAD_TIME_INTERVAL(PRINT_ESTIMATE_WARN_INTERVAL)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN_RET(tmp_ret, "estimated finish time is too large", K(tmp_ret), KPC(this), K(start_time), K(current_time));
    }
    estimated_finish_time_ = start_time + MAX_ESTIMATE_SPEND_TIME;
  }
}

int ObPartitionMergeProgress::get_progress_info(ObCompactionProgress &input_progress)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    // do nothing
  } else if (is_empty_merge_) {
    input_progress.data_size_ = 1;
    input_progress.unfinished_data_size_ = 0;
    input_progress.estimated_finish_time_ = estimated_finish_time_;
  } else {
    int64_t estimate_unfinished_data_size = estimated_total_size_ - pre_scanned_row_cnt_ * avg_row_length_;
    input_progress.data_size_ = estimated_total_size_;
    input_progress.unfinished_data_size_ = MAX(0, estimated_total_size_ - pre_scanned_row_cnt_ * avg_row_length_);
    input_progress.estimated_finish_time_ = estimated_finish_time_;
  }
  return ret;
}

int ObPartitionMergeProgress::diagnose_progress(ObDiagnoseTabletCompProgress &input_progress)
{
  int ret = OB_SUCCESS;
  int64_t update_threshold = UPDATE_INTERVAL * NORMAL_UPDATE_PARAM;
#ifdef ERRSIM
  update_threshold = 100;
#endif
  if (ObTimeUtility::fast_current_time() - latest_update_ts_ > update_threshold) {
    input_progress.is_suspect_abormal_ = true;
  }

  input_progress.latest_update_ts_ = latest_update_ts_;
  return ret;
}

/*
 * ObPartitionMajorMergeProgress implement
 * */
int ObPartitionMajorMergeProgress::inner_update_progress_mgr(const int64_t total_scanned_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t scan_data_size_delta = (total_scanned_row_cnt - pre_scanned_row_cnt_) * avg_row_length_;
  const bool is_first_update = pre_scanned_row_cnt_ == 0;

  if (OB_FAIL(MTL(ObTenantCompactionProgressMgr*)->update_progress(
          ctx_->get_merge_version(),
          is_first_update ? estimated_total_size_ : 0,
          scan_data_size_delta,
          estimated_finish_time_,
          false/*finish_flag*/))) {
    LOG_WARN("failed to update tenant compaction progress", K(ret),
             K(scan_data_size_delta), K(is_first_update), KPC(this));
  }
  return ret;
}

int ObPartitionMajorMergeProgress::finish_progress(
  const int64_t merge_version,
  ObCompactionTimeGuard *time_guard,
  const bool is_co_merge)
{
  int ret = OB_SUCCESS;
  estimated_finish_time_ = ObTimeUtility::fast_current_time();
  if (OB_FAIL(MTL(ObTenantCompactionProgressMgr*)->update_progress(merge_version,
                                                                   0 == pre_scanned_row_cnt_ ? estimated_total_size_ : 0, // estimate_occupy_size_delta
                                                                   estimated_total_size_ - pre_scanned_row_cnt_ * avg_row_length_,// scanned_data_size_delta
                                                                   estimated_finish_time_,
                                                                   true/*finish_flag*/,
                                                                   time_guard,
                                                                   is_co_merge))) {
    LOG_WARN("failed to update progress mgr", K(ret), K(merge_version), K(is_co_merge), KPC(this));
  }
  return ret;
}

int ObPartitionMajorMergeProgress::finish_merge_progress()
{
  int ret = OB_SUCCESS;
  ObTabletMergeCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionMajorMergeProgress not inited", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx has unexpected type", K(ret), KPC_(ctx));
  } else if (FALSE_IT(ctx = static_cast<ObTabletMergeCtx *>(ctx_))) {
  } else if (OB_FAIL(finish_progress(ctx->get_merge_version(),
                                     &ctx->info_collector_.time_guard_,
                                     false/*is_co_merge*/))) {
    LOG_WARN("failed to update progress", K(ret), KPC(this));
  } else if (OB_FAIL(MTL(ObTenantCompactionProgressMgr*)->update_compression_ratio(
      ctx->get_merge_version(),
      ctx->get_merge_info().get_merge_history()))) {
    LOG_WARN("failed to update progress", K(ret));
  } else {
    LOG_DEBUG("finish() success to update progress", K(ret),
              "param", ctx->get_dag_param(), KPC(this));
  }
  return ret;
}

int ObCOMajorMergeProgress::finish_merge_progress()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMajorMergeProgress not inited", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(merge_dag_) || typeid(*merge_dag_) != typeid(ObCOMergeBatchExeDag))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_dag has unexpected type", K(ret), KPC_(merge_dag));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ctx", K(ret), KPC_(ctx));
  } else if (typeid(*ctx_) != typeid(ObCOTabletMergeCtx)) {
    if (!GCTX.is_shared_storage_mode()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx has unexpected type", K(ret), KPC_(ctx));
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (typeid(*ctx_) != typeid(ObCOTabletOutputMergeCtx)
            && typeid(*ctx_) != typeid(ObCOTabletValidateMergeCtx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx has unexpected type", K(ret), KPC_(ctx));
#endif
    }
  }

  if (OB_SUCC(ret)) {
    ObCOMergeBatchExeDag *merge_dag = static_cast<ObCOMergeBatchExeDag*>(merge_dag_);
    ObCOTabletMergeCtx *ctx = static_cast<ObCOTabletMergeCtx*>(ctx_);
    if (OB_FAIL(finish_progress(ctx->get_merge_version(),
                                &merge_dag->get_time_guard(),
                                true/*co_merge*/))) {
      LOG_WARN("failed to update progress", K(ret), KPC(this));
    } else {
      for (int64_t i = start_cg_idx_; OB_SUCC(ret) && i < end_cg_idx_; ++i) {
        if (OB_UNLIKELY(OB_ISNULL(ctx->cg_merge_info_array_) || OB_ISNULL(ctx->cg_merge_info_array_[i]))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("merge_info is unexpected null", K(ret), KPC(ctx));
        } else if (OB_FAIL(MTL(ObTenantCompactionProgressMgr*)->update_compression_ratio(
          ctx->get_merge_version(),
          ctx->cg_merge_info_array_[i]->get_merge_history()))) {
          LOG_WARN("failed to update progress", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("finish merge progress", "param", ctx_->static_param_, KPC(this));
  }
  return ret;
}

} //compaction
} //oceanbase
