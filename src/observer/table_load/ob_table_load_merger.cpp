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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_merger.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "storage/direct_load/ob_direct_load_fast_heap_table.h"
#include "storage/direct_load/ob_direct_load_multi_map.h"
#include "storage/direct_load/ob_direct_load_range_splitter.h"
#include "storage/blocksstable/ob_sstable.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::hash;
using namespace lib;
using namespace storage;
using namespace blocksstable;
using namespace table;

class ObTableLoadMerger::MergeTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  MergeTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx, ObTableLoadMerger *merger)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), merger_(merger)
  {
    ctx_->inc_ref_count();
  }
  virtual ~MergeTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, merge_time_us);
    int ret = OB_SUCCESS;
    ObDirectLoadPartitionMergeTask *merge_task = nullptr;
    while (OB_SUCC(ret)) {
      merge_task = nullptr;
      if (OB_FAIL(merger_->get_next_merge_task(merge_task))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next compactor task", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(merge_task->process())) {
        LOG_WARN("fail to process merge task", KR(ret));
      }
      if (nullptr != merge_task) {
        merger_->handle_merge_task_finish(merge_task);
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadMerger *const merger_;
};

class ObTableLoadMerger::MergeTaskCallback : public ObITableLoadTaskCallback
{
public:
  MergeTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadMerger *merger)
    : ctx_(ctx), merger_(merger)
  {
    ctx_->inc_ref_count();
  }
  virtual ~MergeTaskCallback()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(merger_->handle_merge_thread_finish(ret_code))) {
      LOG_WARN("fail to handle merge thread finish", KR(ret));
    }
    if (OB_FAIL(ret)) {
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }
private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadMerger *const merger_;
};

class ObTableLoadMerger::RescanTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  RescanTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx, ObTableLoadMerger *merger)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), merger_(merger)
  {
    ctx_->inc_ref_count();
  }
  virtual ~RescanTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    ObDirectLoadPartitionRescanTask *rescan_task = nullptr;
    while (OB_SUCC(ret)) {
      rescan_task = nullptr;
      if (OB_FAIL(merger_->get_next_rescan_task(rescan_task))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next rescan task", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(rescan_task->process())) {
        LOG_WARN("fail to process rescan task", KR(ret));
      }
      if (nullptr != rescan_task) {
        merger_->handle_rescan_task_finish(rescan_task);
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadMerger *const merger_;
};

class ObTableLoadMerger::RescanTaskCallback : public ObITableLoadTaskCallback
{
public:
  RescanTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadMerger *merger)
    : ctx_(ctx), merger_(merger)
  {
    ctx_->inc_ref_count();
  }
  virtual ~RescanTaskCallback()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(merger_->handle_rescan_thread_finish(ret_code))) {
      LOG_WARN("fail to handle rescan thread finish", KR(ret));
    }
    if (OB_FAIL(ret)) {
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }
private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadMerger *const merger_;
};

class ObTableLoadMerger::DelLobTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  DelLobTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx, ObTableLoadMerger *merger)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), merger_(merger)
  {
    ctx_->inc_ref_count();
  }
  virtual ~DelLobTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    ObDirectLoadPartitionDelLobTask *del_lob_task = nullptr;
    while (OB_SUCC(ret)) {
      del_lob_task = nullptr;
      if (OB_FAIL(merger_->get_next_del_lob_task(del_lob_task))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next del lob task", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(del_lob_task->process())) {
        LOG_WARN("fail to process del lob task", KR(ret));
      }
      if (nullptr != del_lob_task) {
        merger_->handle_del_lob_task_finish(del_lob_task);
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadMerger *const merger_;
};

class ObTableLoadMerger::DelLobTaskCallback : public ObITableLoadTaskCallback
{
public:
  DelLobTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadMerger *merger)
    : ctx_(ctx), merger_(merger)
  {
    ctx_->inc_ref_count();
  }
  virtual ~DelLobTaskCallback()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(merger_->handle_del_lob_thread_finish(ret_code))) {
      LOG_WARN("fail to handle del lob thread finish", KR(ret));
    }
    if (OB_FAIL(ret)) {
      ctx_->store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }
private:
  ObTableLoadTableCtx *const ctx_;
  ObTableLoadMerger *const merger_;
};

/**
 * ObTableLoadMerger
 */

ObTableLoadMerger::ObTableLoadMerger(ObTableLoadStoreCtx *store_ctx)
  : store_ctx_(store_ctx),
    param_(store_ctx->ctx_->param_),
    running_thread_count_(0),
    has_error_(false),
    is_stop_(false),
    is_inited_(false)
{
}

ObTableLoadMerger::~ObTableLoadMerger()
{
  abort_unless(merging_list_.is_empty());
  abort_unless(rescan_list_.is_empty());
}

int ObTableLoadMerger::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadMerger init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(table_compact_config_.init(store_ctx_, *this))) {
      LOG_WARN("fail to init table_compact_config_", KR(ret));
    } else if (OB_FAIL(lob_id_compact_config_.init(*this))) {
      LOG_WARN("fail to init lob id compact config", KR(ret));
    } else if (OB_FAIL(table_compact_ctx_.init(store_ctx_, &table_compact_config_))) {
      LOG_WARN("fail to init table compact ctx", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadMerger::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadMerger not init", KR(ret), KP(this));
  } else {
    if (store_ctx_->is_fast_heap_table_) {
      if (OB_FAIL(build_merge_ctx())) {
        LOG_WARN("fail to build merge ctx", KR(ret));
      } else if (OB_FAIL(start_merge())) {
        LOG_WARN("fail to start merge", KR(ret));
      }
    } else {
      if (OB_FAIL(table_compact_ctx_.start())) {
        LOG_WARN("fail to start compact table", KR(ret));
      }
    }
  }
  return ret;
}

void ObTableLoadMerger::stop()
{
  LOG_WARN_RET(OB_SUCCESS, "LOAD MERGE STOP");
  is_stop_ = true;
  // 停止table合并
  table_compact_ctx_.stop();
  lob_id_compact_ctx_.stop();
  // 遍历合并中的任务队列, 调用stop
  ObMutexGuard guard(mutex_);
  ObDirectLoadPartitionMergeTask *merge_task = nullptr;
  ObDirectLoadPartitionRescanTask *rescan_task = nullptr;
  ObDirectLoadPartitionDelLobTask *del_lob_task = nullptr;
  DLIST_FOREACH_NORET(merge_task, merging_list_) {
    merge_task->stop();
  }
  DLIST_FOREACH_NORET(rescan_task, rescan_list_) {
    rescan_task->stop();
  }
  DLIST_FOREACH_NORET(del_lob_task, del_lob_list_) {
    del_lob_task->stop();
  }
}

int ObTableLoadMerger::handle_table_compact_success()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_stop_)) {
  } else {
    LOG_INFO("table compact success");
    if (OB_FAIL(build_merge_ctx())) {
      LOG_WARN("fail to build merge ctx", KR(ret));
    } else if (OB_FAIL(start_merge())) {
      LOG_WARN("fail to start merge", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMerger::handle_lob_id_compact_success()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_stop_)) {
  } else {
    bool need_del_lob = true;
    if (OB_FAIL(build_del_lob_ctx(need_del_lob))) {
      LOG_WARN("fail to build del lob ctx", KR(ret));
    } else if (need_del_lob && OB_FAIL(start_del_lob())) {
      LOG_WARN("fail to start del lob", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMerger::build_merge_ctx()
{
  int ret = OB_SUCCESS;
  ObDirectLoadMergeParam merge_param;
  merge_param.table_id_ = param_.table_id_;
  merge_param.lob_meta_table_id_ = store_ctx_->ctx_->schema_.lob_meta_table_id_;
  merge_param.target_table_id_ = store_ctx_->ctx_->ddl_param_.dest_table_id_;
  merge_param.rowkey_column_num_ = store_ctx_->ctx_->schema_.rowkey_column_count_;
  merge_param.store_column_count_ = store_ctx_->ctx_->schema_.store_column_count_;
  merge_param.fill_cg_thread_cnt_ = param_.session_count_;
  merge_param.lob_column_idxs_ = &(store_ctx_->ctx_->schema_.lob_column_idxs_);
  merge_param.table_data_desc_ = store_ctx_->table_data_desc_;
  merge_param.datum_utils_ = &(store_ctx_->ctx_->schema_.datum_utils_);
  merge_param.lob_column_idxs_ = &(store_ctx_->ctx_->schema_.lob_column_idxs_);
  merge_param.col_descs_ = &(store_ctx_->ctx_->schema_.column_descs_);
  merge_param.lob_id_table_data_desc_ = store_ctx_->lob_id_table_data_desc_;
  merge_param.lob_meta_datum_utils_ = &(store_ctx_->ctx_->schema_.lob_meta_datum_utils_);
  merge_param.lob_meta_col_descs_ = &(store_ctx_->ctx_->schema_.lob_meta_column_descs_);
  merge_param.is_heap_table_ = store_ctx_->ctx_->schema_.is_heap_table_;
  merge_param.is_fast_heap_table_ = store_ctx_->is_fast_heap_table_;
  merge_param.is_incremental_ = ObDirectLoadMethod::is_incremental(param_.method_);
  merge_param.insert_mode_ = param_.insert_mode_;
  merge_param.insert_table_ctx_ = store_ctx_->insert_table_ctx_;
  merge_param.dml_row_handler_ = store_ctx_->error_row_handler_;
  merge_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
  merge_param.trans_param_ = store_ctx_->trans_param_;
  if (OB_FAIL(merge_ctx_.init(store_ctx_->ctx_, merge_param, store_ctx_->ls_partition_ids_))) {
    LOG_WARN("fail to init merge ctx", KR(ret));
  } else if (store_ctx_->is_multiple_mode_) {
    const ObIArray<ObDirectLoadTabletMergeCtx *> &tablet_merge_ctxs =
      merge_ctx_.get_tablet_merge_ctxs();
    ObArray<ObIDirectLoadPartitionTable *> empty_table_array;
    ObIArray<ObIDirectLoadPartitionTable *> *table_array = nullptr;
    if (table_compact_ctx_.result_.tablet_result_map_.size() > 0) {
      abort_unless(1 == table_compact_ctx_.result_.tablet_result_map_.size());
      ObTableLoadTableCompactResult::TabletResultMap::Iterator iter(
        table_compact_ctx_.result_.tablet_result_map_);
      ObTableLoadTableCompactTabletResult *tablet_result = nullptr;
      if (OB_ISNULL(tablet_result = iter.next(tablet_result))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null tablet result", KR(ret));
      } else {
        table_array = &tablet_result->table_array_;
      }
      if (OB_NOT_NULL(tablet_result)) {
        table_compact_ctx_.result_.tablet_result_map_.revert(tablet_result);
      }
    } else {
      table_array = &empty_table_array;
    }
    if (!merge_param.is_heap_table_ && !table_array->empty()) {
      // for optimize split range is too slow
      ObArray<ObDirectLoadMultipleSSTable *> multiple_sstable_array;
      ObDirectLoadMultipleMergeRangeSplitter range_splitter;
      multiple_sstable_array.set_tenant_id(MTL_ID());
      for (int64_t i = 0; OB_SUCC(ret) && i < table_array->count(); ++i) {
        ObDirectLoadMultipleSSTable *sstable = nullptr;
        if (OB_ISNULL(sstable = static_cast<ObDirectLoadMultipleSSTable *>(table_array->at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table", KR(ret), K(i), K(table_array));
        } else if (OB_FAIL(multiple_sstable_array.push_back(sstable))) {
          LOG_WARN("fail to push back sstable", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(range_splitter.init(multiple_sstable_array, merge_param.table_data_desc_,
                                        merge_param.datum_utils_, *merge_param.col_descs_))) {
          LOG_WARN("fail to init range splitter", KR(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctxs.count(); ++i) {
        ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctxs.at(i);
        if (OB_FAIL(tablet_merge_ctx->build_merge_task_for_multiple_pk_table(
              multiple_sstable_array, range_splitter, param_.session_count_))) {
          LOG_WARN("fail to build merge task for multiple pk table", KR(ret));
        }
      }
   } else if (merge_param.is_heap_table_ && !table_array->empty() &&
               tablet_merge_ctxs.count() > param_.session_count_ * 2) {
      // for optimize the super multi-partition heap table space serious enlargement
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctxs.count(); ++i) {
        ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctxs.at(i);
        if (OB_FAIL(
              tablet_merge_ctx->build_aggregate_merge_task_for_multiple_heap_table(*table_array))) {
          LOG_WARN("fail to build aggregate merge task for multiple heap table", KR(ret));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctxs.count(); ++i) {
        ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctxs.at(i);
        if (OB_FAIL(tablet_merge_ctx->build_merge_task(
              *table_array, store_ctx_->ctx_->schema_.column_descs_, param_.session_count_,
              store_ctx_->is_multiple_mode_))) {
          LOG_WARN("fail to build merge task", KR(ret));
        }
      }
    }
  } else {
    ObArray<ObIDirectLoadPartitionTable *> empty_table_array;
    const ObIArray<ObDirectLoadTabletMergeCtx *> &tablet_merge_ctxs =
      merge_ctx_.get_tablet_merge_ctxs();
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctxs.count(); ++i) {
      ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctxs.at(i);
      ObTableLoadTableCompactTabletResult *tablet_result = nullptr;
      ObIArray<ObIDirectLoadPartitionTable *> *table_array = nullptr;
      if (OB_FAIL(table_compact_ctx_.result_.tablet_result_map_.get(
            tablet_merge_ctx->get_tablet_id(), tablet_result))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to get tablet result", KR(ret));
        } else {
          ret = OB_SUCCESS;
          table_array = &empty_table_array;
        }
      } else {
        table_array = &tablet_result->table_array_;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tablet_merge_ctx->build_merge_task(
              *table_array, store_ctx_->ctx_->schema_.column_descs_, param_.session_count_,
              store_ctx_->is_multiple_mode_))) {
          LOG_WARN("fail to build merge task", KR(ret));
        }
      }
      if (OB_NOT_NULL(tablet_result)) {
        table_compact_ctx_.result_.tablet_result_map_.revert(tablet_result);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(merge_task_iter_.init(&merge_ctx_))) {
      LOG_WARN("fail to init merge task iter", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMerger::build_rescan_ctx()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObDirectLoadTabletMergeCtx *> &tablet_merge_ctxs =
    merge_ctx_.get_tablet_merge_ctxs();
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctxs.count(); ++i) {
    ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctxs.at(i);
    if (OB_FAIL(tablet_merge_ctx->build_rescan_task(param_.session_count_))) {
      LOG_WARN("fail to build rescan task", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(rescan_task_iter_.init(&merge_ctx_))) {
      LOG_WARN("fail to build rescan task", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadMerger::build_del_lob_ctx(bool &need_del_lob)
{
  int ret = OB_SUCCESS;
  need_del_lob = true;
  const ObIArray<ObDirectLoadTabletMergeCtx *> &tablet_merge_ctxs =
    merge_ctx_.get_tablet_merge_ctxs();
  ObArray<ObIDirectLoadPartitionTable *> empty_table_array;
  ObIArray<ObIDirectLoadPartitionTable *> *table_array = nullptr;
  if (lob_id_compact_ctx_.result_.tablet_result_map_.size() > 0) {
    abort_unless(1 == lob_id_compact_ctx_.result_.tablet_result_map_.size());
    ObTableLoadTableCompactResult::TabletResultMap::Iterator iter(
      lob_id_compact_ctx_.result_.tablet_result_map_);
    ObTableLoadTableCompactTabletResult *tablet_result = nullptr;
    if (OB_ISNULL(tablet_result = iter.next(tablet_result))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null tablet result", KR(ret));
    } else {
      table_array = &tablet_result->table_array_;
    }
    if (OB_NOT_NULL(tablet_result)) {
      lob_id_compact_ctx_.result_.tablet_result_map_.revert(tablet_result);
    }
  } else {
    table_array = &empty_table_array;
  }
  if (OB_SUCC(ret)) {
    if (table_array->empty()) {
      FLOG_INFO("LOAD NO NEED DEL LOB");
      need_del_lob = false;
      if (OB_FAIL(store_ctx_->set_status_merged())) {
        LOG_WARN("fail to set status", K(ret));
      }
    } else {
      ObArray<ObDirectLoadMultipleSSTable *> multiple_sstable_array;
      ObDirectLoadMultipleMergeRangeSplitter range_splitter;
      multiple_sstable_array.set_tenant_id(MTL_ID());
      for (int64_t i = 0; OB_SUCC(ret) && i < table_array->count(); ++i) {
        ObDirectLoadMultipleSSTable *sstable = nullptr;
        if (OB_ISNULL(sstable = static_cast<ObDirectLoadMultipleSSTable *>(table_array->at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table", KR(ret), K(i), K(table_array));
        } else if (OB_FAIL(multiple_sstable_array.push_back(sstable))) {
          LOG_WARN("fail to push back sstable", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(range_splitter.init(multiple_sstable_array, store_ctx_->lob_id_table_data_desc_,
                                        &(store_ctx_->ctx_->schema_.lob_meta_datum_utils_),
                                        store_ctx_->ctx_->schema_.lob_meta_column_descs_))) {
          LOG_WARN("fail to init range splitter", KR(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctxs.count(); ++i) {
        ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctxs.at(i);
        if (OB_FAIL(tablet_merge_ctx->build_del_lob_task(
              multiple_sstable_array, range_splitter, param_.session_count_))) {
          LOG_WARN("fail to build merge task for multiple pk table", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(del_lob_task_iter_.init(&merge_ctx_))) {
          LOG_WARN("fail to build del lob task", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadMerger::start_merge()
{
  int ret = OB_SUCCESS;
  const int64_t thread_count = store_ctx_->task_scheduler_->get_thread_count();
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  if (OB_UNLIKELY(0 != running_thread_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected running thread count not zero", KR(ret), K(running_thread_count_));
  } else {
    running_thread_count_ = thread_count;
  }
  for (int32_t thread_idx = 0; OB_SUCC(ret) && thread_idx < thread_count; ++thread_idx) {
    ObTableLoadTask *task = nullptr;
    // 1. 分配task
    if (OB_FAIL(ctx->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 2. 设置processor
    else if (OB_FAIL(task->set_processor<MergeTaskProcessor>(ctx, this))) {
      LOG_WARN("fail to set merge task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<MergeTaskCallback>(ctx, this))) {
      LOG_WARN("fail to set merge task callback", KR(ret));
    }
    // 4. 把task放入调度器
    else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(thread_idx, task))) {
      LOG_WARN("fail to add task", KR(ret), K(thread_idx), KPC(task));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != task) {
        ctx->free_task(task);
      }
    }
  }
  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
  return ret;
}

int ObTableLoadMerger::start_rescan()
{
  int ret = OB_SUCCESS;
  const int64_t thread_count = store_ctx_->task_scheduler_->get_thread_count();
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  if (OB_UNLIKELY(0 != running_thread_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected running thread count not zero", KR(ret), K(running_thread_count_));
  } else {
    running_thread_count_ = thread_count;
  }
  for (int32_t thread_idx = 0; OB_SUCC(ret) && thread_idx < thread_count; ++thread_idx) {
    ObTableLoadTask *task = nullptr;
    // 1. 分配task
    if (OB_FAIL(ctx->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 2. 设置processor
    else if (OB_FAIL(task->set_processor<RescanTaskProcessor>(ctx, this))) {
      LOG_WARN("fail to set merge task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<RescanTaskCallback>(ctx, this))) {
      LOG_WARN("fail to set merge task callback", KR(ret));
    }
    // 4. 把task放入调度器
    else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(thread_idx, task))) {
      LOG_WARN("fail to add task", KR(ret), K(thread_idx), KPC(task));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != task) {
        ctx->free_task(task);
      }
    }
  }
  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
  return ret;
}

int ObTableLoadMerger::start_del_lob()
{
  int ret = OB_SUCCESS;
  const int64_t thread_count = store_ctx_->task_scheduler_->get_thread_count();
  ObTableLoadTableCtx *ctx = store_ctx_->ctx_;
  if (OB_UNLIKELY(0 != running_thread_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected running thread count not zero", KR(ret), K(running_thread_count_));
  } else {
    running_thread_count_ = thread_count;
  }
  for (int32_t thread_idx = 0; OB_SUCC(ret) && thread_idx < thread_count; ++thread_idx) {
    ObTableLoadTask *task = nullptr;
    // 1. 分配task
    if (OB_FAIL(ctx->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 2. 设置processor
    else if (OB_FAIL(task->set_processor<DelLobTaskProcessor>(ctx, this))) {
      LOG_WARN("fail to set del lob task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<DelLobTaskCallback>(ctx, this))) {
      LOG_WARN("fail to set del lob task callback", KR(ret));
    }
    // 4. 把task放入调度器
    else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(thread_idx, task))) {
      LOG_WARN("fail to add task", KR(ret), K(thread_idx), KPC(task));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != task) {
        ctx->free_task(task);
      }
    }
  }
  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
  return ret;
}

int ObTableLoadMerger::get_next_merge_task(ObDirectLoadPartitionMergeTask *&merge_task)
{
  int ret = OB_SUCCESS;
  merge_task = nullptr;
  if (OB_UNLIKELY(is_stop_ || has_error_)) {
    ret = OB_ITER_END;
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(merge_task_iter_.get_next_task(merge_task))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next merger", KR(ret));
      }
    } else {
      abort_unless(merging_list_.add_last(merge_task));
    }
  }
  return ret;
}

int ObTableLoadMerger::get_next_rescan_task(ObDirectLoadPartitionRescanTask *&rescan_task)
{
  int ret = OB_SUCCESS;
  rescan_task = nullptr;
  if (OB_UNLIKELY(is_stop_ || has_error_)) {
    ret = OB_ITER_END;
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(rescan_task_iter_.get_next_task(rescan_task))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next task", KR(ret));
      }
    } else {
      abort_unless(rescan_list_.add_last(rescan_task));
    }
  }
  return ret;
}

int ObTableLoadMerger::get_next_del_lob_task(ObDirectLoadPartitionDelLobTask *&del_lob_task)
{
  int ret = OB_SUCCESS;
  del_lob_task = nullptr;
  if (OB_UNLIKELY(is_stop_ || has_error_)) {
    ret = OB_ITER_END;
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(del_lob_task_iter_.get_next_task(del_lob_task))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next task", KR(ret));
      }
    } else {
      abort_unless(del_lob_list_.add_last(del_lob_task));
    }
  }
  return ret;
}

void ObTableLoadMerger::handle_merge_task_finish(ObDirectLoadPartitionMergeTask *&merge_task)
{
  ObMutexGuard guard(mutex_);
  abort_unless(OB_NOT_NULL(merging_list_.remove(merge_task)));
}

void ObTableLoadMerger::handle_rescan_task_finish(ObDirectLoadPartitionRescanTask *&rescan_task)
{
  ObMutexGuard guard(mutex_);
  abort_unless(OB_NOT_NULL(rescan_list_.remove(rescan_task)));
}

void ObTableLoadMerger::handle_del_lob_task_finish(ObDirectLoadPartitionDelLobTask *&del_lob_task)
{
  ObMutexGuard guard(mutex_);
  abort_unless(OB_NOT_NULL(del_lob_list_.remove(del_lob_task)));
}

int ObTableLoadMerger::handle_merge_thread_finish(int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_code)) {
    has_error_ = true;
  }
  const int64_t running_thread_count = ATOMIC_SAF(&running_thread_count_, 1);
  if (0 == running_thread_count) {
    if (OB_UNLIKELY(is_stop_ || has_error_)) {
    } else {
      FLOG_INFO("LOAD MERGE COMPLETED");
      // release tmpfile
      // TODO(suzhi.yt) release all tables and merge tasks
      table_compact_ctx_.result_.release_all_table_data();
      if (store_ctx_->insert_table_ctx_->need_rescan()) {
        if (OB_FAIL(build_rescan_ctx())) {
          LOG_WARN("fail to build rescan ctx", KR(ret));
        } else if (OB_FAIL(start_rescan())) {
          LOG_WARN("fail to start rescan", KR(ret));
        }
      } else if (store_ctx_->insert_table_ctx_->need_del_lob()) {
        if (OB_FAIL(merge_ctx_.close_table_builder())) {
          LOG_WARN("fail to close table builder", KR(ret));
        } else if (OB_FAIL(lob_id_compact_ctx_.init(store_ctx_, &lob_id_compact_config_))) {
          LOG_WARN("fail to init lob compact", KR(ret));
        } else if (OB_FAIL(lob_id_compact_ctx_.start())) {
          LOG_WARN("fail to start lob compact", KR(ret));
        }
      } else {
        if (OB_FAIL(store_ctx_->set_status_merged())) {
          LOG_WARN("fail to set store status merged", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadMerger::handle_rescan_thread_finish(const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_code)) {
    has_error_ = true;
  }
  const int64_t running_thread_count = ATOMIC_SAF(&running_thread_count_, 1);
  if (0 == running_thread_count) {
    if (OB_UNLIKELY(is_stop_ || has_error_)) {
    } else {
      FLOG_INFO("LOAD RESCAN COMPLETED");
      if (OB_FAIL(store_ctx_->set_status_merged())) {
        LOG_WARN("fail to set store status merged", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadMerger::handle_del_lob_thread_finish(int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_code)) {
    has_error_ = true;
  }
  const int64_t running_thread_count = ATOMIC_SAF(&running_thread_count_, 1);
  if (0 == running_thread_count) {
    if (OB_UNLIKELY(is_stop_ || has_error_)) {
    } else {
      FLOG_INFO("LOAD DEL LOB COMPLETED");
      lob_id_compact_ctx_.result_.release_all_table_data();
      if (OB_FAIL(store_ctx_->set_status_merged())) {
        LOG_WARN("fail to set store status merged", KR(ret));
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
