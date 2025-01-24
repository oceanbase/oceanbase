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

#include "observer/table_load/ob_table_load_pre_sorter.h"
#include "storage/direct_load/ob_direct_load_mem_sample.h"
#include "storage/direct_load/ob_direct_load_range_splitter.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/table_load/ob_table_load_merger.h"
#include "src/observer/table_load/ob_table_load_store_table_ctx.h"
namespace oceanbase
{
namespace observer
{
using namespace storage;
using namespace common;
ObTableLoadPreSorter::ObTableLoadPreSorter(ObTableLoadTableCtx *ctx,
                                           ObTableLoadStoreCtx *store_ctx)
  : finish_task_count_(0),
    finish_write_task_count_(0),
    running_write_task_count_(0),
    chunks_manager_(nullptr),
    ctx_(ctx),
    store_ctx_(store_ctx),
    other_task_count_(0),
    dump_task_count_(0),
    sample_task_scheduler_(nullptr),
    parallel_merge_cb_(this),
    all_trans_finished_(false),
    is_inited_(false),
    is_start_(false)
{
}

ObTableLoadPreSorter::~ObTableLoadPreSorter()
{
  mem_ctx_.has_error_ = true; //avoid the stuck of sampling thread
  if (OB_NOT_NULL(sample_task_scheduler_)) {
    sample_task_scheduler_->stop();
    sample_task_scheduler_->wait();
    sample_task_scheduler_->~ObITableLoadTaskScheduler();
    ob_free(sample_task_scheduler_);
    sample_task_scheduler_ = nullptr;
  }
  if (nullptr != chunks_manager_) {
    chunks_manager_->~ObTableLoadMemChunkManager();
    ob_free(chunks_manager_);
    chunks_manager_ = nullptr;
  }
  mem_ctx_.reset();
}

int ObTableLoadPreSorter::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPreSorter init twice", KR(ret), KP(this));
  } else if (OB_FAIL(init_task_count())) {
    LOG_WARN("fail to init task count", KR(ret), K(dump_task_count_), K(other_task_count_));
  } else if (OB_FAIL(init_mem_ctx())) {
    LOG_WARN("fail to init mem ctx", KR(ret));
  } else if (OB_FAIL(parallel_merge_ctx_.init(store_ctx_, store_ctx_->data_store_table_ctx_, mem_ctx_.table_data_desc_))) {
    LOG_WARN("fail to init parallel_merge_ctx_", KR(ret));
  } else if (OB_FAIL(init_sample_task_scheduler())) {
    LOG_WARN("fail to init sample task scheduler", KR(ret));
  } else if (OB_FAIL(init_chunks_manager())) {
    LOG_WARN("fail to init chunks manager", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadPreSorter::init_task_count()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPreSorter init twice", KR(ret), KP(this));
  } else {
    // dump task count 80%, other task count 20%
    int32_t tmp_dump_task_count = ctx_->param_.session_count_  * 4 / 5;
    dump_task_count_ = tmp_dump_task_count >= 1 ? tmp_dump_task_count : 1;
    other_task_count_ = ctx_->param_.session_count_ - dump_task_count_;
    if (other_task_count_ <= 0) {
      other_task_count_ = 1;
    }
  }
  return ret;
}

int ObTableLoadPreSorter::init_mem_ctx()
{
  int ret = OB_SUCCESS;
  mem_ctx_.table_data_desc_ = store_ctx_->data_store_table_ctx_->table_data_desc_;
  mem_ctx_.datum_utils_ = &(ctx_->schema_.datum_utils_);
  mem_ctx_.need_sort_ = ctx_->param_.need_sort_;
  mem_ctx_.column_count_ = (store_ctx_->ctx_->schema_.is_heap_table_
                              ? store_ctx_->ctx_->schema_.store_column_count_ - 1
                              : store_ctx_->ctx_->schema_.store_column_count_);
  mem_ctx_.mem_dump_task_count_ = dump_task_count_;
  mem_ctx_.dml_row_handler_ = store_ctx_->data_store_table_ctx_->row_handler_;
  mem_ctx_.dup_action_ = ctx_->param_.dup_action_;
  mem_ctx_.file_mgr_ = store_ctx_->tmp_file_mgr_;
  if (OB_FAIL(mem_ctx_.init())) {
    LOG_WARN("fail to init mem ctx", KR(ret));
  }
  return ret;
}

int ObTableLoadPreSorter::init_sample_task_scheduler()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sample_task_scheduler_ = OB_NEW(ObTableLoadTaskThreadPoolScheduler,
                                                ObMemAttr(MTL_ID(), "TLD_PreSorter"),
                                                1 /*thread_count*/,
                                                ctx_->param_.table_id_,
                                                "TLD_PreSorter",
                                                ctx_->session_info_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new sample task scheduler", KR(ret));
  } else if (OB_FAIL(sample_task_scheduler_->init())) {
    LOG_WARN("fail to init sample task scheduler", KR(ret));
  } else if (OB_FAIL(sample_task_scheduler_->start())) {
    LOG_WARN("fail to start sample task scheduler", KR(ret));
  }
  return ret;
}

int ObTableLoadPreSorter::init_chunks_manager()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(chunks_manager_ = OB_NEW(ObTableLoadMemChunkManager,
                                        ObMemAttr(MTL_ID(), "TLD_PreSorter")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new chunks_manager_", KR(ret));
  } else if (OB_FAIL(chunks_manager_->init(store_ctx_, &mem_ctx_))) {
    LOG_WARN("fail to init chunks manager", KR(ret));
  }
  return ret;
}

void ObTableLoadPreSorter::stop()
{
  set_has_error();
  if (OB_NOT_NULL(sample_task_scheduler_)) {
    sample_task_scheduler_->stop();
  }
}

int ObTableLoadPreSorter::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPreSorter not init", KR(ret));
  } else if (OB_FAIL(start_sample())) {
    LOG_WARN("fail to start sample", KR(ret));
  } else if (OB_FAIL(start_dump())) {
    LOG_WARN("fail to start dump", KR(ret));
  } else {
    is_start_ = true;
  }
  return ret;
}

int ObTableLoadPreSorter::start_sample()
{
  int ret = OB_SUCCESS;
  ObTableLoadTask *task = nullptr;
  if (OB_FAIL(ctx_->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  } else if (OB_FAIL(task->set_processor<PreSortSampleTaskProcessor>(ctx_, &mem_ctx_))) {
    LOG_WARN("fail to set pre sort sample task processor", KR(ret));
  } else if (OB_FAIL(task->set_callback<PreSortTaskCallback>(ctx_, store_ctx_, this))) {
    LOG_WARN("fail to set pre sort sample task callback", KR(ret));
  } else if (OB_FAIL(sample_task_scheduler_->add_task(0, task))) {
    LOG_WARN("fail to add task", KR(ret), KPC(task));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(task)) {
      ctx_->free_task(task);
    }
  }
  return ret;
}

int ObTableLoadPreSorter::start_dump()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < dump_task_count_; ++i) {
    ObTableLoadTask *task = nullptr;
    int32_t dump_task_session_id = other_task_count_ + i;
    if (OB_FAIL(ctx_->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(task->set_processor<PreSortDumpTaskProcessor>(ctx_, &mem_ctx_))) {
      LOG_WARN("fail to set compactor task callback", KR(ret));
    } else if (OB_FAIL(task->set_callback<PreSortTaskCallback>(ctx_, store_ctx_, this))) {
      LOG_WARN("fail to set pre sort dump task callback", KR(ret));
    } else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(dump_task_session_id, task))) {
      LOG_WARN("fail to add task", KR(ret), KPC(task));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(task)) {
        ctx_->free_task(task);
      }
    }
  } // for
  return ret;
}

int ObTableLoadPreSorter::set_all_trans_finished()
{
  int ret = OB_SUCCESS;
  if (all_trans_finished_ || mem_ctx_.all_trans_finished_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument of all trans finished", KR(ret));
  } else {
    all_trans_finished_ = true;
    mem_ctx_.all_trans_finished_ = true;
  }
  return ret;
}

int ObTableLoadPreSorter::handle_pre_sort_task_finish(int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret_code)) {
    int64_t task_to_wait = dump_task_count_ + 1;
    const int64_t finish_task_count = ATOMIC_AAF(&finish_task_count_, 1);
    if (task_to_wait == finish_task_count) {
      // all tasks have done
      if (OB_LIKELY(!(mem_ctx_.has_error_)) && OB_FAIL(prepare_parallel_merge())) {
        LOG_WARN("fail to prepare_parallel_merge", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadPreSorter::prepare_parallel_merge()
{
  int ret = OB_SUCCESS;
  ObTableLoadTask *task = nullptr;
  if (OB_FAIL(ctx_->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  } else if (OB_FAIL(task->set_processor<PreSortParallelMergeTaskProcessor>(ctx_, this))) {
    LOG_WARN("fail to set pre sort parallel merge task processor", KR(ret));
  } else if (OB_FAIL(task->set_callback<PreSortParallelMergeTaskCallback>(ctx_, store_ctx_))) {
    LOG_WARN("fail to set pre sort parallel merge task callback", KR(ret));
  } else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(0, task))) {
    LOG_WARN("fail to add task", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(task)) {
      ctx_->free_task(task);
    }
  }
  return ret;
}

int ObTableLoadPreSorter::add_table_to_parallel_merge_ctx()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < mem_ctx_.tables_.count(); i ++) {
    ObIDirectLoadPartitionTable *table = mem_ctx_.tables_.at(i);
    ObDirectLoadMultipleSSTable *sstable = nullptr;
    sstable = static_cast<ObDirectLoadMultipleSSTable *>(table);
    if (OB_ISNULL(sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", KR(ret), K(i), KPC(table));
    } else if (OB_FAIL(parallel_merge_ctx_.add_tablet_sstable(sstable))) {
      LOG_WARN("fail to add tablet sstable", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    mem_ctx_.reset();
  }
  return ret;
}

int ObTableLoadPreSorter::start_parallel_merge()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parallel_merge_ctx_.start(&parallel_merge_cb_))) {
    LOG_WARN("fail to start parallel merge", KR(ret));
  }
  return ret;
}

int ObTableLoadPreSorter::ParallelMergeCb::on_success()
{
  return pre_sorter_->handle_parallel_merge_success();
}

int ObTableLoadPreSorter::handle_parallel_merge_success()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_parallel_merge_result())) {
    LOG_WARN("fail to build result", KR(ret));
  } else if (OB_FAIL(handle_pre_sort_success())) {
    LOG_WARN("fail to handle pre sort success", KR(ret));
  }
  return ret;
}

int ObTableLoadPreSorter::build_parallel_merge_result()
{
  int ret = OB_SUCCESS;
  ObTableLoadMerger *merger = store_ctx_->data_store_table_ctx_->merger_;
  const ObTableLoadParallelMergeCtx::TabletCtxMap &tablet_ctx_map =
    parallel_merge_ctx_.get_tablet_ctx_map();
  if (OB_ISNULL(merger)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("merger should not be nullptr", KR(ret));
  } else {
    ObTableLoadTableCompactResult &result = merger->table_compact_ctx_.result_;
    for (ObTableLoadParallelMergeCtx::TabletCtxIterator tablet_ctx_iter = tablet_ctx_map.begin();
        OB_SUCC(ret) && tablet_ctx_iter != tablet_ctx_map.end(); ++tablet_ctx_iter) {
      ObTableLoadParallelMergeTabletCtx *tablet_ctx = tablet_ctx_iter->second;
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ctx->sstables_.size(); ++i) {
        ObDirectLoadMultipleSSTable *sstable = tablet_ctx->sstables_.at(i);
        ObDirectLoadMultipleSSTable *copied_sstable = nullptr;
        if (OB_ISNULL(copied_sstable = OB_NEWx(ObDirectLoadMultipleSSTable,
                        (&result.allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObDirectLoadMultipleSSTable", KR(ret));
        } else if (OB_FAIL(copied_sstable->copy(*sstable))) {
          LOG_WARN("fail to copy sstable", KR(ret));
        } else if (OB_FAIL(result.add_table(copied_sstable))) {
          LOG_WARN("fail to add table", KR(ret));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != copied_sstable) {
            copied_sstable->~ObDirectLoadMultipleSSTable();
            result.allocator_.free(copied_sstable);
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadPreSorter::handle_pre_sort_success()
{
  int ret = OB_SUCCESS;
  ObTableLoadTask *task = nullptr;
  if (OB_FAIL(ctx_->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  } else if (OB_FAIL(task->set_processor<PreSortFinishTaskProcessor>(ctx_, this))) {
    LOG_WARN("fail to set pre sort parallel merge task processor", KR(ret));
  } else if (OB_FAIL(task->set_callback<PreSortFinishTaskCallback>(ctx_, store_ctx_))) {
    LOG_WARN("fail to set pre sort parallel merge task callback", KR(ret));
  } else if (OB_FAIL(store_ctx_->task_scheduler_->add_task(0, task))) {
    LOG_WARN("fail to add task", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(task)) {
      ctx_->free_task(task);
    }
  }
  return ret;
}

int ObTableLoadPreSorter::finish()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_merge_ctx())) {
    LOG_WARN("fail to build merge ctx", KR(ret));
  } else if (OB_FAIL(start_merge())) {
    LOG_WARN("fail to start merge", KR(ret));
  }
  return ret;
}

int ObTableLoadPreSorter::build_merge_param(ObDirectLoadMergeParam& merge_param)
{
  int ret = OB_SUCCESS;
  merge_param.table_id_ = ctx_->param_.table_id_;
  merge_param.lob_meta_table_id_ = ctx_->schema_.lob_meta_table_id_;
  merge_param.target_table_id_ = ctx_->ddl_param_.dest_table_id_;
  merge_param.rowkey_column_num_ = ctx_->schema_.rowkey_column_count_;
  merge_param.store_column_count_ = ctx_->schema_.store_column_count_;
  merge_param.fill_cg_thread_cnt_ = ctx_->param_.session_count_;
  merge_param.lob_column_idxs_ = &(ctx_->schema_.lob_column_idxs_);
  merge_param.table_data_desc_ = store_ctx_->data_store_table_ctx_->table_data_desc_;
  merge_param.datum_utils_ = &(ctx_->schema_.datum_utils_);
  merge_param.col_descs_ = &(ctx_->schema_.column_descs_);
  merge_param.lob_id_table_data_desc_ = store_ctx_->data_store_table_ctx_->lob_id_table_data_desc_;
  merge_param.lob_meta_datum_utils_ = &(ctx_->schema_.lob_meta_datum_utils_);
  merge_param.lob_meta_col_descs_ = &(ctx_->schema_.lob_meta_column_descs_);
  merge_param.is_heap_table_ = ctx_->schema_.is_heap_table_;
  merge_param.is_fast_heap_table_ = store_ctx_->data_store_table_ctx_->is_fast_heap_table_;
  merge_param.is_incremental_ = ObDirectLoadMethod::is_incremental(ctx_->param_.method_);
  merge_param.insert_mode_ = ctx_->param_.insert_mode_;
  merge_param.insert_table_ctx_ = store_ctx_->data_store_table_ctx_->insert_table_ctx_;
  merge_param.dml_row_handler_ = store_ctx_->data_store_table_ctx_->row_handler_;
  merge_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
  merge_param.trans_param_ = store_ctx_->trans_param_;
  merge_param.merge_with_conflict_check_ =
    !store_ctx_->data_store_table_ctx_->is_index_table_ &&
    ObDirectLoadMethod::is_incremental(ctx_->param_.method_) &&
    (ctx_->param_.insert_mode_ == ObDirectLoadInsertMode::NORMAL ||
     (ctx_->param_.insert_mode_ == ObDirectLoadInsertMode::INC_REPLACE &&
      (store_ctx_->data_store_table_ctx_->schema_->lob_column_idxs_.count() > 0 ||
       store_ctx_->data_store_table_ctx_->schema_->index_table_count_ > 0)));
  return ret;
}

int ObTableLoadPreSorter::build_merge_ctx_for_multiple_mode(ObDirectLoadMergeParam& merge_param,
                                                            ObTableLoadMerger *merger,
                                                            ObTableLoadTableCompactResult &result)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObDirectLoadTabletMergeCtx *> &tablet_merge_ctxs =
    merger->merge_ctx_.get_tablet_merge_ctxs();
  ObArray<ObIDirectLoadPartitionTable *> empty_table_array;
  ObIArray<ObIDirectLoadPartitionTable *> *table_array = nullptr;
  if (result.tablet_result_map_.size() > 0) {
    abort_unless(1 == result.tablet_result_map_.size());
    ObTableLoadTableCompactResult::TabletResultMap::Iterator iter(result.tablet_result_map_);
    ObTableLoadTableCompactTabletResult *tablet_result = nullptr;
    if (OB_ISNULL(tablet_result = iter.next(tablet_result))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null tablet result", KR(ret));
    } else {
      table_array = &tablet_result->table_array_;
    }
    if (OB_NOT_NULL(tablet_result)) {
      result.tablet_result_map_.revert(tablet_result);
    }
  } else {
    table_array = &empty_table_array;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!merge_param.is_heap_table_ && !table_array->empty()) {
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
            multiple_sstable_array, range_splitter, ctx_->param_.session_count_))) {
        LOG_WARN("fail to build merge task for multiple pk table", KR(ret));
      }
    }
  } else if (merge_param.is_heap_table_ && !table_array->empty() &&
            tablet_merge_ctxs.count() > ctx_->param_.session_count_ * 2) {
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
            *table_array, ctx_->schema_.column_descs_, ctx_->param_.session_count_,
            store_ctx_->data_store_table_ctx_->is_multiple_mode_))) {
        LOG_WARN("fail to build merge task", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadPreSorter::build_merge_ctx_for_non_multiple_mode(ObDirectLoadMergeParam& merge_param,
                                                                ObTableLoadMerger *merger,
                                                                ObTableLoadTableCompactResult &result)
{
  int ret = OB_SUCCESS;
  ObArray<ObIDirectLoadPartitionTable *> empty_table_array;
  const ObIArray<ObDirectLoadTabletMergeCtx *> &tablet_merge_ctxs =
    merger->merge_ctx_.get_tablet_merge_ctxs();
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_merge_ctxs.count(); ++i) {
    ObDirectLoadTabletMergeCtx *tablet_merge_ctx = tablet_merge_ctxs.at(i);
    ObTableLoadTableCompactTabletResult *tablet_result = nullptr;
    ObIArray<ObIDirectLoadPartitionTable *> *table_array = nullptr;
    if (OB_FAIL(result.tablet_result_map_.get(
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
            *table_array, ctx_->schema_.column_descs_, ctx_->param_.session_count_,
            store_ctx_->data_store_table_ctx_->is_multiple_mode_))) {
        LOG_WARN("fail to build merge task", KR(ret));
      }
    }
    if (OB_NOT_NULL(tablet_result)) {
      result.tablet_result_map_.revert(tablet_result);
    }
  }
  return ret;
}

int ObTableLoadPreSorter::build_merge_ctx()
{
  int ret = OB_SUCCESS;
  ObDirectLoadMergeParam merge_param;
  ObTableLoadMerger *merger = store_ctx_->data_store_table_ctx_->merger_;
  if (OB_ISNULL(merger)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("merger shoult not be nullptr", KR(ret));
  } else if (OB_FAIL(build_merge_param(merge_param))) {
    LOG_WARN("fail to build merge param", KR(ret));
  } else {
    ObTableLoadTableCompactResult &result = merger->table_compact_ctx_.result_;
    if (OB_FAIL(merger->merge_ctx_.init(ctx_, merge_param, store_ctx_->data_store_table_ctx_->ls_partition_ids_))) {
      LOG_WARN("fail to init merge ctx", KR(ret));
    } else if (store_ctx_->data_store_table_ctx_->is_multiple_mode_) {
      if (OB_FAIL(build_merge_ctx_for_multiple_mode(merge_param, merger, result))) {
        LOG_WARN("fail to build merge ctx for multiple mode", KR(ret));
      }
    } else {
      if (OB_FAIL(build_merge_ctx_for_non_multiple_mode(merge_param, merger, result))) {
        LOG_WARN("fail to build merge ctx for non multiple mode", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(merger->merge_task_iter_.init(&(merger->merge_ctx_)))) {
      LOG_WARN("fail to init merge task iter", KR(ret));
    }
  }
  return ret;
}
int ObTableLoadPreSorter::start_merge()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_ctx_->data_store_table_ctx_->merger_->start_merge())) {
    LOG_WARN("fail to start merge", KR(ret));
  }
  return ret;
}

class ObTableLoadPreSorter::PreSortSampleTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  PreSortSampleTaskProcessor(ObTableLoadTask &task,
                             ObTableLoadTableCtx *ctx,
                             ObDirectLoadMemContext *mem_ctx)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      mem_ctx_(mem_ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~PreSortSampleTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    storage::ObDirectLoadMemSample sample(ctx_, mem_ctx_);
    if (OB_FAIL(sample.do_pre_sort_sample())) {
      LOG_WARN("fail to do sample", KR(ret));
    }
    return ret;
  }
private:
  ObTableLoadTableCtx *ctx_;
  ObDirectLoadMemContext *mem_ctx_;
};

class ObTableLoadPreSorter::PreSortDumpTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  PreSortDumpTaskProcessor(ObTableLoadTask &task,
                           ObTableLoadTableCtx *ctx,
                           ObDirectLoadMemContext *mem_ctx)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      mem_ctx_(mem_ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~PreSortDumpTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    void *tmp = nullptr;
    while (OB_SUCC(ret) && !(mem_ctx_->has_error_)) {
      if (OB_FAIL(mem_ctx_->mem_dump_queue_.pop(tmp))) {
        LOG_WARN("fail to pop", KR(ret));
      } else {
        if (OB_NOT_NULL(tmp)) {
          ObDirectLoadMemDump *mem_dump = static_cast<ObDirectLoadMemDump *>(tmp);
          if (OB_FAIL(mem_dump->do_dump())) {
            LOG_WARN("fail to do dump", KR(ret));
          }
          mem_dump->~ObDirectLoadMemDump();
          ob_free(mem_dump);
        } else {
          if (OB_FAIL(mem_ctx_->mem_dump_queue_.push(nullptr))) {
            LOG_WARN("fail to push nullptr", KR(ret));
          } else {
            break;
          }
        }
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx *ctx_;
  ObDirectLoadMemContext *mem_ctx_;
};

class ObTableLoadPreSorter::PreSortTaskCallback : public ObITableLoadTaskCallback
{
public:
  PreSortTaskCallback(ObTableLoadTableCtx *ctx,
                      ObTableLoadStoreCtx *store_ctx,
                      ObTableLoadPreSorter *pre_sorter)
    : ctx_(ctx),
      store_ctx_(store_ctx),
      pre_sorter_(pre_sorter)
  {
    ctx_->inc_ref_count();
  }
  virtual ~PreSortTaskCallback()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = ret_code;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(pre_sorter_->handle_pre_sort_task_finish(ret_code))) {
        LOG_WARN("fail to handle_pre_sort_task_finish", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      pre_sorter_->set_has_error();
      store_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }
private:
  ObTableLoadTableCtx *ctx_;
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadPreSorter *pre_sorter_;
};

class ObTableLoadPreSorter::PreSortParallelMergeTaskProcessor : public ObITableLoadTaskProcessor {
public:
  PreSortParallelMergeTaskProcessor(ObTableLoadTask &task,
                                    ObTableLoadTableCtx *ctx,
                                    ObTableLoadPreSorter *pre_sorter)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      pre_sorter_(pre_sorter)
  {
    ctx_->inc_ref_count();
  }
  virtual ~PreSortParallelMergeTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(pre_sorter_->add_table_to_parallel_merge_ctx())) {
      LOG_WARN("fail to build pre sort result", KR(ret));
    } else if (OB_FAIL(pre_sorter_->start_parallel_merge())) {
      LOG_WARN("fail to start parallel merge", KR(ret));
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadPreSorter * const pre_sorter_;
};

class ObTableLoadPreSorter::PreSortParallelMergeTaskCallback : public ObITableLoadTaskCallback
{
public:
  PreSortParallelMergeTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadStoreCtx *store_ctx)
    : ctx_(ctx),
      store_ctx_(store_ctx)
    {
      ctx_->inc_ref_count();
    }
    virtual ~PreSortParallelMergeTaskCallback()
    {
      ObTableLoadService::put_ctx(ctx_);
    }
    void callback(int ret_code, ObTableLoadTask *task) override
    {
      int ret = ret_code;
      if (OB_FAIL(ret)) {
        store_ctx_->set_status_error(ret);
      }
      ctx_->free_task(task);
      OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
    }
private:
    ObTableLoadTableCtx * const ctx_;
    ObTableLoadStoreCtx * store_ctx_;
};

class ObTableLoadPreSorter::PreSortFinishTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  PreSortFinishTaskProcessor(ObTableLoadTask &task,
                             ObTableLoadTableCtx *ctx,
                             ObTableLoadPreSorter *pre_sorter)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      pre_sorter_(pre_sorter)
  {
    ctx_->inc_ref_count();
  }
  virtual ~PreSortFinishTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret) && !pre_sorter_->all_trans_finished_) {
      //wait all trans finished
      usleep(10000);
    }
    if (OB_FAIL(pre_sorter_->finish())) {
      LOG_WARN("fail to finish", KR(ret));
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadPreSorter * const pre_sorter_;
};

class ObTableLoadPreSorter::PreSortFinishTaskCallback : public ObITableLoadTaskCallback
{
public:
  PreSortFinishTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadStoreCtx *store_ctx)
    : ctx_(ctx),
      store_ctx_(store_ctx)
    {
      ctx_->inc_ref_count();
    }
    virtual ~PreSortFinishTaskCallback()
    {
      ObTableLoadService::put_ctx(ctx_);
    }
    void callback(int ret_code, ObTableLoadTask *task) override
    {
      int ret = ret_code;
      if (OB_FAIL(ret)) {
        store_ctx_->set_status_error(ret);
      }
      ctx_->free_task(task);
      OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
    }
private:
    ObTableLoadTableCtx * const ctx_;
    ObTableLoadStoreCtx *store_ctx_;
};
} /* namespace observer */
} /* namespace oceanbase */